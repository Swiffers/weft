//! `weft maintenance [status] | on [--running wait|cancel] | off | check`:
//! everything on the install switched off while weft itself is updated
//! (`weft_core::maintenance`).
//!
//! `on` takes down every trigger that is on and stops every infra copy that
//! runs, then (under `--running wait`, the default) waits until the runs
//! going have finished and the copies are stopped. `off` builds each of
//! those projects again on this weft, starts those copies on that build,
//! then brings those triggers back, followed to its end.
//! `check` is what `./setup.sh` and the GCP install workflow run before
//! replacing anything: it fails while something is on.

use std::time::Duration;

use anyhow::Context;
use weft_core::maintenance::{LiftAnswer, LiftReport, MaintenanceOn, MaintenanceOnRequest, MaintenanceState};
use weft_core::take_down::Running;

use super::Ctx;

pub enum MaintenanceAction {
    Status,
    On { running: Option<Running> },
    Off,
    /// With `key_stdin`, the operator key comes on stdin (CI); `update`
    /// is the update's own step, as the refusal names it.
    Check { key_stdin: bool, update: String },
}

/// How long `on` waits between two looks at what is still going down.
const LOOK_EVERY: Duration = Duration::from_secs(3);

pub async fn run(ctx: Ctx, action: MaintenanceAction) -> anyhow::Result<()> {
    match action {
        MaintenanceAction::Status => status(&ctx).await,
        MaintenanceAction::On { running } => on(&ctx, running).await,
        MaintenanceAction::Off => off(&ctx).await,
        MaintenanceAction::Check { key_stdin, update } => check(&ctx, key_stdin, &update).await,
    }
}

async fn read_state(client: &crate::client::DispatcherClient) -> anyhow::Result<MaintenanceState> {
    serde_json::from_value(client.get_json("/install/maintenance").await?)
        .context("read the install's maintenance (the install and this CLI disagree on its shape; upgrade one of them)")
}

/// `weft maintenance` / `weft maintenance status`.
async fn status(ctx: &Ctx) -> anyhow::Result<()> {
    let state = read_state(&ctx.client()?).await?;
    if ctx.json_out(&state)? {
        return Ok(());
    }
    match state.since_unix {
        Some(since) => {
            println!("in maintenance since {}; `{}` lifts it", when(since), ctx.weft("maintenance off"));
            if state.left.is_empty() {
                println!("  it brings nothing back: nothing was on when it began");
            }
            for project in &state.left {
                println!("  {} ({}): brings back {}", project.name, project.id, left_words(&project.copies, &project.triggers));
            }
        }
        None => {
            println!("not in maintenance");
            // What a project could not bring back when the last one was
            // lifted stays down, and is that project's own to bring back.
            for project in &state.left {
                println!(
                    "  {} ({}): still down since the last maintenance: {}; fix what kept it down, then `{}` in its folder \
                     brings it back",
                    project.name,
                    project.id,
                    left_words(&project.copies, &project.triggers),
                    ctx.weft("resync")
                );
            }
        }
    }
    let on = state.going.words();
    println!("on now: {}", if on.is_empty() { "nothing" } else { &on });
    Ok(())
}

/// `weft maintenance on`.
async fn on(ctx: &Ctx, running: Option<Running>) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let request = MaintenanceOnRequest { running };
    let answer: MaintenanceOn = serde_json::from_value(client.post_json("/install/maintenance", &serde_json::to_value(&request)?).await?)
        .context("read what `weft maintenance on` did (the install and this CLI disagree on its shape; upgrade one of them)")?;
    if !ctx.json() {
        println!("in maintenance since {}", when(answer.since_unix));
        for project in &answer.projects {
            let mut did = Vec::new();
            if !project.triggers.is_empty() {
                did.push(format!("took down {}", weft_core::activation::OwnerTriggers::list_words(&project.triggers)));
            }
            if !project.copies.is_empty() {
                did.push(format!("stopped infra {}", copy_list(&project.copies)));
            }
            if project.runs_cancelled > 0 {
                did.push(format!("cancelled {} run(s)", project.runs_cancelled));
            }
            if !did.is_empty() {
                println!("  {} ({}): {}", project.name, project.id, did.join("; "));
            }
            for refused in &project.refused {
                eprintln!("  {} ({}): still on: {refused}", project.name, project.id);
            }
        }
    }
    // What goes down on its own (a run finishing, a copy stopping) is
    // waited for, however long it takes: the update needs it over.
    // Ctrl+C stops the waiting, never what it waits for.
    if request.running() == Running::Wait {
        let mut said = String::new();
        loop {
            let state = read_state(&client).await?;
            if !state.going.still_going_down() {
                break;
            }
            let going = state.going.words();
            if going != said {
                let cancel = if state.going.runs > 0 {
                    format!("; `{}` cancels the runs", ctx.weft("maintenance on --running cancel"))
                } else {
                    String::new()
                };
                eprintln!("waiting for what is still going down: {going} (Ctrl+C stops waiting{cancel})");
                said = going;
            }
            tokio::time::sleep(LOOK_EVERY).await;
        }
    }
    ctx.json_out(&answer)?;
    check_left(ctx, &answer)?;
    if !ctx.json() {
        println!("the install is in maintenance: update it now, then `{}` brings back what this took down", ctx.weft("maintenance off"));
    }
    Ok(())
}

/// Fail when `on` left something on, which another `on` takes down once
/// what held it is over.
fn check_left(ctx: &Ctx, answer: &MaintenanceOn) -> anyhow::Result<()> {
    if answer.left_something_on() {
        anyhow::bail!(
            "the install is in maintenance, but some of it is still on (said above); once that is over, run `{}` again (it adds what is on and clears nothing)",
            ctx.weft("maintenance on")
        );
    }
    Ok(())
}

/// `weft maintenance off`.
async fn off(ctx: &Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let answer: LiftAnswer = serde_json::from_value(client.post_json("/install/maintenance/off", &serde_json::json!({})).await?)
        .context("read the lift's answer (the install and this CLI disagree on its shape; upgrade one of them)")?;
    let task = match answer {
        LiftAnswer::NotOn => {
            if !ctx.json_out(&LiftReport { projects: Vec::new() })? {
                println!("the install is not in maintenance; nothing to bring back");
            }
            return Ok(());
        }
        LiftAnswer::UnderWay { task } => task,
    };
    eprintln!("building each project again on this weft, then starting the infra the maintenance stopped, then bringing back its triggers");
    let follow = format!("/install/maintenance/follow/{task}");
    let report: LiftReport = serde_json::from_value(super::follow::follow_at(&client, &follow, |message| eprintln!("{message}")).await?)
        .context("read the lift's report (the install and this CLI disagree on its shape; upgrade one of them)")?;
    if ctx.json_out(&report)? {
        return Ok(());
    }
    let mut down = Vec::new();
    for project in &report.projects {
        let (did, failed) = lifted_lines(project);
        if !did.is_empty() {
            println!("  {} ({}): {}", project.name, project.id, did.join("; "));
        }
        for failed in &failed {
            eprintln!("  {} ({}): did not come back: {failed}", project.name, project.id);
        }
        if !failed.is_empty() {
            down.push(project.name.as_str());
        }
    }
    println!("the maintenance is lifted");
    if !down.is_empty() {
        eprintln!(
            "{} stayed down, without holding anything else: fix what is said above in its folder, then `{}` there brings \
             back what the update took down (or `{}` what you want left off)",
            down.join(", "),
            ctx.weft("resync"),
            ctx.weft("deactivate")
        );
    }
    Ok(())
}

/// What a lift did in one project, as lines: what came back (and what it
/// had nothing to do for), then what did not, each with why.
pub(crate) fn lifted_lines(project: &weft_core::maintenance::ProjectLifted) -> (Vec<String>, Vec<String>) {
    let mut did = Vec::new();
    if project.rebuilt {
        did.push("built again on this weft".to_string());
    }
    if !project.started.is_empty() {
        did.push(format!("started infra {}", copy_list(&project.started)));
    }
    if !project.turned_on.is_empty() {
        did.push(format!("turned on {}", weft_core::activation::OwnerTriggers::list_words(&project.turned_on)));
    }
    did.extend(project.skipped.iter().map(|skipped| format!("skipped {skipped}")));
    (did, project.failed.clone())
}

/// `weft maintenance check`: fail while something is on, so an update
/// never replaces a weft that is running something. An install that is not
/// set up or not running has nothing on; one whose weft predates the
/// maintenance cannot be asked, and is let through, saying so.
async fn check(ctx: &Ctx, key_stdin: bool, update: &str) -> anyhow::Result<()> {
    match ctx.install_access() {
        Ok(_) => {}
        // This machine's install with no address yet: never started.
        Err(e) if ctx.on().is_none() => {
            println!("this machine's install is not set up ({e:#}); nothing to check");
            return Ok(());
        }
        Err(e) => return Err(e),
    }
    let client = ctx.client_keyed(key_stdin)?;
    let state = match client.get_json("/install/maintenance").await {
        Ok(state) => serde_json::from_value::<MaintenanceState>(state)
            .context("read the install's maintenance (the install and this CLI disagree on its shape; upgrade one of them)")?,
        Err(e) => {
            if let Some(unreachable) = e.chain().find_map(|cause| cause.downcast_ref::<crate::client::InstallUnreachable>()) {
                println!("{} is not running; nothing to check", unreachable.named());
                return Ok(());
            }
            if crate::client::refusal(&e).is_some_and(|refused| predates_maintenance(refused.status())) {
                println!("the weft running this install predates `weft maintenance`, so whether something is on cannot be checked; going ahead");
                return Ok(());
            }
            return Err(e);
        }
    };
    // Asked by raw address (the cloud workflow), the person still acts on
    // the install through a project's target.
    let spell = |verb: &str| match ctx.dispatcher.is_some() && ctx.on().is_none() {
        true => format!("weft {verb} --on <target> (from a project that names it)"),
        false => ctx.weft(verb),
    };
    if let Some(why) = weft_core::maintenance::update_refusal(&state, spell, update) {
        anyhow::bail!("{why}");
    }
    if !ctx.json_out(&state)? {
        println!("nothing is on; the update can go ahead");
    }
    Ok(())
}

/// Whether `status`, a weft's answer to `GET /install/maintenance`, says
/// that weft predates `weft maintenance`, so it has no such route. One
/// without the route answers 404, or 405: until the maintenance came, the
/// dispatcher's catch-all public route (`/{*mount_path}`, which takes a
/// trigger's fire) served POST alone and answered any other method on any
/// path it did not know with 405.
fn predates_maintenance(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
}

/// A unix time as a person reads it.
fn when(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0).map(|at| at.format("%Y-%m-%d %H:%M UTC").to_string()).unwrap_or_else(|| unix.to_string())
}

fn copy_list(copies: &[weft_core::infra::wire::CopyRef]) -> String {
    copies
        .iter()
        .map(|copy| match &copy.instance {
            Some(instance) => format!("'{}' of instance '{instance}'", copy.node),
            None => format!("'{}'", copy.node),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// What a lift brings back in one project, as a person reads it.
fn left_words(copies: &[weft_core::infra::wire::CopyRef], triggers: &[weft_core::activation::OwnerTriggers]) -> String {
    let mut parts = Vec::new();
    if !copies.is_empty() {
        parts.push(format!("infra {}", copy_list(copies)));
    }
    if !triggers.is_empty() {
        parts.push(format!("triggers {}", weft_core::activation::OwnerTriggers::list_words(triggers)));
    }
    parts.join(", then ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::activation::OwnerTriggers;
    use weft_core::infra::wire::CopyRef;
    use weft_core::instance::{InstanceId, Owner};

    #[test]
    fn a_weft_without_the_route_predates_the_maintenance() {
        assert!(predates_maintenance(reqwest::StatusCode::NOT_FOUND));
        assert!(predates_maintenance(reqwest::StatusCode::METHOD_NOT_ALLOWED), "an older weft's catch-all answers a GET with 405");
        for status in [reqwest::StatusCode::UNAUTHORIZED, reqwest::StatusCode::FORBIDDEN, reqwest::StatusCode::INTERNAL_SERVER_ERROR, reqwest::StatusCode::CONFLICT] {
            assert!(!predates_maintenance(status), "{status}");
        }
    }

    #[test]
    fn what_a_lift_brings_back_reads_infra_first() {
        let copies = [CopyRef { node: "db".into(), instance: None }, CopyRef { node: "cache".into(), instance: Some(InstanceId::new("ada").unwrap()) }];
        let triggers = [OwnerTriggers { owner: Owner::Shared, triggers: vec!["door".into()] }];
        let words = left_words(&copies, &triggers);
        assert!(words.starts_with("infra 'db', 'cache' of instance 'ada', then triggers"), "{words}");
        assert!(words.contains("'door'"), "{words}");
        assert_eq!(left_words(&[], &triggers), format!("triggers {}", OwnerTriggers::list_words(&triggers)));
    }
}
