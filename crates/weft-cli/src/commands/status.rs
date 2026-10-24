//! `weft status`: discover the cwd project, compute current source
//! hashes, hit the dispatcher's `/projects/{id}/status` aggregator
//! with those hashes as query params (drives drift detection).
//! Print a human-readable summary or, with `--json`, emit the full
//! status payload as a single JSON line for consumption by the
//! VS Code extension's action bar.

use anyhow::{Context, Result};
use weft_core::projects::{ProjectDrift, ProjectStatusResponse, ProjectTransition};

use super::Ctx;

pub async fn run(ctx: Ctx) -> Result<()> {
    let project = ctx.project()?;
    let project_id = project.id().to_string();

    // Compute desired hashes from current source. The dispatcher
    // compares them against the build each trigger and infra copy runs:
    // the code (binary hash), each trigger's cut and each infra place's
    // own infra, to say which are behind.
    let weft_root = weft_compiler::build::resolve_weft_root()
        .map_err(|e| anyhow::anyhow!("resolve weft repo root: {e}"))?;
    // Both hashes are scoped to the compiled project's referenced /
    // infra-closure nodes, so both need the definition + catalog. If
    // the project can't compile, leave the desired hashes unset and say
    // why: status is display-only and tolerates an in-progress project,
    // but a person reading no drift must know none was looked for.
    //
    // FULL hashes, never shortened: the dispatcher stores the full
    // `running_*_hash` values register sends and compares by string
    // equality, so a shortened desired hash would never match and every
    // project would permanently report drift. Short hashes are for
    // human log lines only.
    //
    // The definition is hashed AFTER its `@asset` refs resolve, exactly
    // as build / run / resync hash it before registering: hashing the
    // unresolved source would differ from every running hash of a
    // project with an asset in it, and the drift banner would never
    // clear. Resolving writes nothing: a file not stored yet stands for
    // the value its upload would give it.
    // Whether the program runs infra of its own, for the first deploy's
    // order (its infra first, then its triggers).
    let mut declares_infra = false;
    let (desired_binary_hash, desired_full_binary_hash, desired_definition_hash, desired_trigger_cuts, desired_infra_places) =
        match weft_compiler::hash::load_program(project) {
            Ok((mut def, catalog)) => {
                declares_infra = weft_core::project::has_infra(&def);
                let resolved = crate::commands::assets::resolve_project_assets(&ctx.client()?, &project.root, &mut def).await;
                // Both node sets: a worker built with the full catalog is as
                // current as one built from the referenced set, and the
                // dispatcher accepts either as "not drifted".
                use weft_core::builds::NodeSet;
                match resolved {
                    Ok(_) => (
                        or_warn(
                            "the worker code's hash",
                            weft_compiler::hash::compute_binary_hash(&def, project, &weft_root, &catalog, NodeSet::Referenced),
                        ),
                        or_warn(
                            "the worker code's hash with every node",
                            weft_compiler::hash::compute_binary_hash(&def, project, &weft_root, &catalog, NodeSet::Full),
                        ),
                        or_warn("the project definition's hash", weft_compiler::hash::compute_definition_hash(&def)),
                        Some(weft_core::projects::NamedHashes(weft_core::run_spec::trigger_fingerprints(&def)).encode()),
                        or_warn(
                            "each infra place's hash",
                            weft_compiler::hash::compute_infra_place_hashes(&def, &project.root, &weft_root, &catalog)
                                .map(|places| weft_core::projects::NamedHashes(places).encode()),
                        ),
                    ),
                    // An asset that cannot resolve is what a build will
                    // refuse; status stays display-only and reports no
                    // desired hashes rather than a made-up drift.
                    Err(e) => {
                        warn_no_drift("its assets do not resolve", &e);
                        (None, None, None, None, None)
                    }
                }
            }
            Err(e) => {
                warn_no_drift("the project does not compile", &e);
                (None, None, None, None, None)
            }
        };

    let query = weft_core::projects::StatusQuery {
        desired_binary_hash,
        desired_full_binary_hash,
        desired_definition_hash,
        desired_trigger_cuts,
        desired_infra_places,
    };
    let path = format!("/projects/{project_id}/status{}", query.to_query_string());

    // A project that exists on disk but was never registered is not an error:
    // it is the state every project starts in, and the answer is the
    // command that leaves it. The marked 404 is how the dispatcher says
    // "no project I know under this id" (as opposed to a missing route).
    let Some(data) = ctx.client()?.get_json_if_found(&path).await? else {
        if !ctx.json_out(&serde_json::json!({ "registered": false, "projectId": project_id }))? {
            // A program with infra of its own has it started before its
            // triggers are switched on: a trigger reads its infra.
            let first = if declares_infra {
                format!("`{}` to start its infra, then `{}` to enable its triggers", ctx.weft("infra start"), ctx.weft("activate"))
            } else {
                format!("`{}` to enable its triggers", ctx.weft("activate"))
            };
            println!(
                "project: {} ({project_id})\n  not registered with the dispatcher yet: use `{}` to run it, or {first}",
                project.manifest.package.name,
                ctx.weft("run"),
            );
        }
        return Ok(());
    };

    let data: ProjectStatusResponse = serde_json::from_value(data)
        .context("read the project's status (the dispatcher and this CLI disagree on its shape; upgrade one of them)")?;

    // One JSON object on stdout; the extension reads it.
    if ctx.json_out(&data)? {
        return Ok(());
    }

    println!("project: {} ({project_id})", data.name);
    if let Some(line) = maintenance_line(&ctx).await {
        println!("  {line}");
    }
    // Half taken down: what follows is what is left.
    if data.removing {
        println!("  WARNING: {}", weft_core::projects::PROJECT_BEING_REMOVED);
    }
    println!("  registration: {}", data.status);
    match &data.address {
        Some(weft_core::projects::ProjectAddress::Serving { url }) => println!("  address: {url} (its routes, at the root)"),
        Some(weft_core::projects::ProjectAddress::Unavailable { why }) => println!("  address: unavailable: {why}"),
        None => {}
    }
    // The transition axis: only worth a line while in flight.
    match data.transition {
        ProjectTransition::None => {}
        ProjectTransition::Resyncing => {
            println!("  resync: under way (`{}` stops it as the triggers come back)", ctx.weft("cancel-activate"))
        }
        building => println!("  build: {building} (cancel with `{}`)", ctx.weft("cancel-build")),
    }
    for build in &data.builds {
        match &build.log_url {
            Some(log) => println!("    {} building as {}; its log: {log}", build.image, build.build),
            None => println!("    {} building as {}", build.image, build.build),
        }
    }
    println!("  listener: {}", if data.listener_running { "running" } else { "stopped" });
    // Orphaned live infra: never silent (the never-lose-track rule).
    if data.orphaned_infra {
        println!(
            "  WARNING: live infra exists whose node the source does not declare (removed from it, \
             or inside a part that does not compile); it keeps running (and consuming resources) \
             until stopped/terminated via the infra verbs"
        );
    }

    // One entry per infra node the program declares, started or not, so
    // an empty list really means no node declares `requiresInfra`.
    if data.infra.is_empty() && !data.built {
        println!("  infra: (this install has not built the program yet, so it does not know its nodes)");
    } else if data.infra.is_empty() {
        println!("  infra: (no nodes declare requiresInfra)");
    } else {
        println!("  infra:");
        for entry in &data.infra {
            // `node` is the node's place, spelled the way the source
            // reads it (`one.db`): the key and the label are one.
            let node = &entry.node;
            match entry.status.as_str() {
                weft_core::infra::wire::INFRA_NOT_STARTED => {
                    println!("    {node}: not started (`{}` brings it up)", ctx.weft("infra start"))
                }
                weft_core::infra::wire::INFRA_PER_INSTANCE => {
                    let copies = entry.instance_copy_count.unwrap_or(0);
                    let counted = if copies == 1 { "1 instance has a copy".to_string() } else { format!("{copies} instances have a copy") };
                    println!("    {node}: one copy per instance ({counted}, listed under instance infra)");
                }
                st => {
                    let behind = if entry.behind { ", behind the source" } else { "" };
                    println!("    {node}: {st}{behind} ({})", entry.endpoint_url.as_deref().unwrap_or("-"));
                    if let Some(progress) = &entry.progress {
                        println!("      {}", progress.describe(progress.as_of_unix));
                    }
                }
            }
            // What stopping it takes down with it: the triggers whose cut
            // contains it.
            if !entry.takes_down.is_empty() {
                println!("      taking it down takes down {}", weft_core::activation::OwnerTriggers::list_words(&entry.takes_down));
            }
        }
    }

    // Every trigger of the program, whether it is on and behind, and every
    // instance's copy of one that exists once per instance.
    if !data.triggers.is_empty() {
        println!("  triggers:");
        for entry in &data.triggers {
            for line in trigger_lines(entry) {
                println!("    {line}");
            }
        }
    }
    // Instances' own copies of the `@per_instance` infra nodes.
    if !data.instance_infra.is_empty() {
        println!("  instance infra:");
        for entry in &data.instance_infra {
            let behind = if entry.behind { ", behind the source" } else { "" };
            println!("    {} (instance {}): {}{behind}", entry.node, entry.instance, entry.status);
            if let Some(progress) = &entry.progress {
                println!("      {}", progress.describe(progress.as_of_unix));
            }
        }
    }

    let runs = &data.runs;
    // `weft tree` lists the program's own runs; the ones that set it up
    // (arming triggers, starting infra) are counted apart so the two agree.
    match runs.setup {
        0 => println!("  runs: {} runs", runs.total),
        setup => println!("  runs: {} runs (and {setup} that set the program up: its triggers, its infra)", runs.total),
    }
    if let (Some(run_id), Some(status)) = (&runs.last_run_id, &runs.last_status) {
        match runs.last_completed_at {
            Some(ts) => {
                let age = crate::progress::now_unix().saturating_sub(ts);
                println!("    last: {run_id} ({status}, completed {age}s ago)");
            }
            None => println!("    last: {run_id} ({status}, in flight)"),
        }
    }
    print_drift(&ctx, &data.drift);
    // One of weft's own calls for this project keeps failing (waking the
    // supervisor, handing a run to the workers): why infra or runs do
    // not move.
    if !data.unanswered.is_empty() {
        println!("  waiting on weft:");
        for entry in &data.unanswered {
            println!("    {}", entry.describe());
        }
    }
    // What each trigger started lately: the one trace a run kept
    // unrecorded leaves when it goes well.
    if !data.trigger_runs.is_empty() {
        println!("  runs started (the last minute or two):");
        for entry in &data.trigger_runs {
            match entry.failed {
                0 => println!("    {}: {}", entry.node, entry.started),
                failed => println!("    {}: {} ({failed} failed)", entry.node, entry.started),
            }
        }
    }
    // A public entry turning callers away, so the author knows a limit
    // is acting and which one (each is a setting on the trigger).
    if !data.limited.is_empty() {
        println!("  refused calls (the last minute or two):");
        for entry in &data.limited {
            println!("    {}: {} by {}", entry.node, entry.refused, entry.limit);
        }
    }
    // The same verb list the editor's action bar offers, so a terminal
    // reader sees what the project accepts right now (and that `resync`
    // is on the table when the listeners lag behind the code).
    if !data.available_actions.is_empty() {
        println!("  actions: {}", data.available_actions.join(", "));
    }

    Ok(())
}

/// Say on stderr (stdout may be the one JSON line the editor reads) that
/// drift cannot be looked for, and why.
/// The line `weft status` adds while the install is in maintenance, read
/// off `GET /install/maintenance`; `None` when it is not. One that could
/// not be read is said on stderr, the rest of the status still printed.
async fn maintenance_line(ctx: &Ctx) -> Option<String> {
    let read = async { serde_json::from_value::<weft_core::maintenance::MaintenanceState>(ctx.client()?.get_json("/install/maintenance").await?).map_err(anyhow::Error::from) };
    match read.await {
        Ok(state) => state.since_unix.map(|since| {
            let since = chrono::DateTime::from_timestamp(since, 0).map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string()).unwrap_or_else(|| since.to_string());
            format!("maintenance: the install is in maintenance since {since}; nothing is turned on until `{}`", ctx.weft("maintenance off"))
        }),
        Err(e) => {
            eprintln!("warning: could not read whether the install is in maintenance ({e:#})");
            None
        }
    }
}

fn warn_no_drift(why: &str, e: &anyhow::Error) {
    eprintln!("warning: {why}, so whether the running program is behind the source is not checked ({e:#})");
}

/// One desired hash, or `None` with a warning naming what could not be
/// worked out.
fn or_warn(what: &str, hash: Result<String>) -> Option<String> {
    hash.map_err(|e| warn_no_drift(&format!("{what} cannot be worked out"), &e)).ok()
}


/// One trigger as `weft status` lists it: the shared one on one line, one
/// that exists once per instance as how many instances have it on, then
/// each instance's copy.
fn trigger_lines(entry: &weft_core::projects::TriggerEntry) -> Vec<String> {
    use weft_core::projects::{TriggerCopy, TriggerState};
    let gone = if entry.gone { " (the program has it no more; `weft deactivate --trigger` takes it down)" } else { "" };
    let reads = match entry.depends_on.as_slice() {
        [] => String::new(),
        infra => format!("; reads {}", infra.join(", ")),
    };
    let copy_words = |copy: &TriggerCopy| -> String {
        let state = match copy.state {
            TriggerState::Off => {
                let door = match copy.mode {
                    weft_core::activation::ActivationMode::Down(door) => format!("off, its door set to {}", door.as_str()),
                    _ => "off".to_string(),
                };
                match copy.down_with {
                    None => door,
                    Some(weft_core::projects::DownWith::Health) => format!(
                        "{door}, taken down by the health check (its infra's health rules bring it back once that infra is \
                         healthy; with rules that do not, `weft activate` does)"
                    ),
                    Some(weft_core::projects::DownWith::Infra) => {
                        format!("{door}, taken down with its infra (it comes back once that infra runs again)")
                    }
                    Some(weft_core::projects::DownWith::Maintenance) => {
                        format!("{door}, taken down by the install's maintenance (`weft maintenance off` brings it back)")
                    }
                }
            }
            TriggerState::TurningOn => "turning on".to_string(),
            TriggerState::On => "on".to_string(),
            TriggerState::Behind => "on, behind the source (`weft resync` brings it up to date)".to_string(),
        };
        let held_back = copy.held_back.as_deref().map(|why| format!(", held back: {why}")).unwrap_or_default();
        let held = if copy.held.is_empty() { String::new() } else { format!(", holding {}", copy.held.describe()) };
        let version = copy.version.as_deref().map(|v| format!(", version {}", super::versions::short(v))).unwrap_or_default();
        let waiting = copy.waiting.as_ref().map(waiting_words).unwrap_or_default();
        format!("{state}{held_back}{held}{version}{waiting}")
    };
    let mut lines = Vec::new();
    match &entry.shared {
        Some(copy) => lines.push(format!("{}: {}{reads}{gone}", entry.node, copy_words(copy))),
        None => {
            let on = entry.copies.iter().filter(|c| matches!(c.state, TriggerState::On | TriggerState::Behind)).count();
            lines.push(format!("{}: once per instance, {on} of {} on{reads}{gone}", entry.node, entry.copies.len()));
        }
    }
    for copy in &entry.copies {
        let instance = copy.instance.as_ref().map(|i| i.as_str()).unwrap_or("-");
        lines.push(format!("  instance {instance}: {}", copy_words(copy)));
    }
    lines
}

/// "; 2 fires wait until ...", for fires waiting on something no clock
/// brings: what they wait on hands them over again.
fn waiting_words(waiting: &weft_core::program::WaitingFires) -> String {
    use weft_core::door_fire::UnclockedWait;
    let (fires, wait) = if waiting.fires == 1 { ("1 fire".to_string(), "waits") } else { (format!("{} fires", waiting.fires), "wait") };
    let until = match waiting.on {
        UnclockedWait::Instance => format!("{wait} until the instance's values change"),
        UnclockedWait::Infra => format!("{wait} until the infra the runs read is running again"),
        UnclockedWait::Picks => format!("{wait} until the install picks the connections the runs need"),
        UnclockedWait::Failure => "stopped after failing to start a run, and holds the trigger's queue; `weft activate`, a new \
                                   build or a change of the worker settings tries again"
            .to_string(),
    };
    format!("; {fires} {until}: {}", waiting.reason)
}

/// Every drift bit the dispatcher set, each with the verb that clears
/// it. Silent when nothing drifted. A trigger behind is listed with the
/// trigger.
fn print_drift(ctx: &Ctx, drift: &ProjectDrift) {
    let lines = [
        (drift.infra_drift, format!("infra: a copy is behind the source; `{}` puts the change live", ctx.weft("infra upgrade"))),
        (drift.binary_drift, format!("binary: worker code has changed; the next run or `{}` rebuilds the image", ctx.weft("build"))),
        (drift.definition_drift, "definition: project shape has changed; the next run picks it up".to_string()),
    ];
    if lines.iter().all(|(set, _)| !set) {
        return;
    }
    println!("  drift:");
    for (_, line) in lines.iter().filter(|(set, _)| *set) {
        println!("    {line}");
    }
}

#[cfg(test)]
mod tests {
    use weft_core::activation::ActivationMode;
    use weft_core::instance::InstanceId;
    use weft_core::projects::{HeldWork, TriggerCopy, TriggerEntry, TriggerState};

    fn copy(instance: Option<&str>, state: TriggerState, mode: ActivationMode) -> TriggerCopy {
        TriggerCopy { instance: instance.map(|i| InstanceId::new(i).unwrap()), state, mode, down_with: None, held_back: None, held: HeldWork::default(), waiting: None, version: None }
    }

    /// A shared trigger is one line with what it reads; one that exists
    /// once per instance says how many are on, then each instance's.
    #[test]
    fn a_trigger_reads_as_whether_it_is_on_and_behind() {
        let door = TriggerEntry {
            node: "door".into(),
            per_instance: false,
            gone: false,
            shared: Some(TriggerCopy { held: HeldWork { arrivals: 2, paused: 0 }, ..copy(None, TriggerState::Off, ActivationMode::Down(weft_core::Door::Hide)) }),
            copies: Vec::new(),
            depends_on: vec!["db".into()],
        };
        let lines = super::trigger_lines(&door);
        assert!(lines[0].starts_with("door: off, its door set to hide, holding 2 ") && lines[0].ends_with("; reads db"), "{lines:?}");
        let inbox = TriggerEntry {
            node: "inbox".into(),
            per_instance: true,
            gone: false,
            shared: None,
            copies: vec![copy(Some("ada"), TriggerState::Behind, ActivationMode::Active), copy(Some("bob"), TriggerState::Off, ActivationMode::Registered)],
            depends_on: Vec::new(),
        };
        let lines = super::trigger_lines(&inbox);
        assert_eq!(lines[0], "inbox: once per instance, 1 of 2 on");
        assert!(lines[1].starts_with("  instance ada: on, behind the source"), "{lines:?}");
        assert_eq!(lines[2], "  instance bob: off");
    }

    /// A trigger the health check took down says so, and what brings it
    /// back; one taken down with its infra says what brings it back too.
    #[test]
    fn a_trigger_down_by_the_health_check_says_so() {
        let entry = |down_with| TriggerEntry {
            node: "door".into(),
            per_instance: false,
            gone: false,
            shared: Some(TriggerCopy { down_with, ..copy(None, TriggerState::Off, ActivationMode::Down(weft_core::Door::Take)) }),
            copies: Vec::new(),
            depends_on: vec!["db".into()],
        };
        let health = super::trigger_lines(&entry(Some(weft_core::projects::DownWith::Health)));
        assert!(health[0].contains("taken down by the health check") && health[0].contains("`weft activate`"), "{health:?}");
        let infra = super::trigger_lines(&entry(Some(weft_core::projects::DownWith::Infra)));
        assert!(infra[0].contains("taken down with its infra"), "{infra:?}");
        let maintenance = super::trigger_lines(&entry(Some(weft_core::projects::DownWith::Maintenance)));
        assert!(maintenance[0].contains("`weft maintenance off` brings it back"), "{maintenance:?}");
        assert!(!super::trigger_lines(&entry(None))[0].contains("taken down"));
        let mut held_back = entry(Some(weft_core::projects::DownWith::Health));
        held_back.shared.as_mut().unwrap().held_back = Some("triggers on another build. `weft resync --all` moves them".into());
        let lines = super::trigger_lines(&held_back);
        assert!(lines[0].contains("held back: triggers on another build") && lines[0].contains("`weft resync --all`"), "{lines:?}");
    }
}
