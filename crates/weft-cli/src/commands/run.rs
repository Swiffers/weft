//! `weft run`: compile + register the cwd project, record the code as a
//! version, start a run under it, stream logs until completion (or
//! `--detach`). A run it follows to its end sets its exit code the way
//! `weft wait` does (`wait::Outcome`): 0 when it completed, non-zero when
//! it failed or was cancelled. A run it only starts exits 0.
//!
//! A run has no entry point of its own: the dispatcher kicks every root
//! of the graph (a node no wire feeds) and pulses run whatever they
//! reach. The run is shaped by a spec (`weft_core::run_spec`), read
//! from `examples/<name>.json` or built from the flags (`--from`,
//! `--emit`, `--target`, `--before`, `--group`, `--fire`); `--save <name>` writes
//! the flags as a spec. `--seed` reuses from head's run (or from the run
//! `--seed=<run>` names) every node whose slice of the program did not
//! change. The whole catalog is compiled
//! by default; `--referenced` builds only the types used by this graph.

use anyhow::{bail, Context};
use weft_core::run_spec::{Refusal, RunSpec};
use weft_core::versions::RunStarted;

use super::versions::{spec_from_flags, RunFlags};
use super::Ctx;
use crate::progress::ActionVerb;

/// Everything `weft run` takes. One struct so the clap arm and the
/// verb never disagree on the list.
#[derive(Debug, Default, Clone)]
pub struct RunArgs {
    pub spec: Option<String>,
    pub detach: bool,
    pub node_set: Option<weft_core::builds::NodeSet>,
    /// `--seed`: `Some("")` for head's run, `Some(run)` for the run named.
    pub seed: Option<String>,
    pub seed_until: Vec<String>,
    pub seed_before: Vec<String>,
    pub root: bool,
    pub flags: RunFlags,
    pub save: Option<String>,
    /// `--check`: only say whether the run could start ([`RunCheck`]).
    pub check: bool,
}

/// What `weft run --check --json` prints: whether the run could start,
/// and when not, the refusal the run itself would give. The editor reads
/// it to disable its Run button with the reason.
// SYNC: RunCheck <-> packages/weft-graph/src/protocol.ts RunCheck
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunCheck {
    pub ok: bool,
    /// Why it could not start; absent when it could.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// The infra copies the run would read, the ones the dispatcher's gate
    /// refuses the run for when one is not running
    /// (`weft_core::infra::run_gate::copies_read` over the program the
    /// run is cut to). The check cannot see which are running; whoever
    /// reads it holds them against the install's status. Absent when it
    /// reads none, or when the run could not start.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub infra_read: Vec<InfraCopyRead>,
    /// With more than one target, each target as a run of it alone would
    /// stand. A run of several targets is one run, refused whole when one
    /// of them cannot go; this says which of them stops it and why, and
    /// which could run alone. Absent with one target.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub targets: Vec<TargetCheck>,
}

/// One infra copy a run reads: the node, spelled the way the program
/// reads it (`one.db` for the `db` an include site `one` reaches), and
/// whose copy (absent for the shared one).
// SYNC: InfraCopyRead <-> packages/weft-graph/src/protocol.ts InfraCopyRead
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct InfraCopyRead {
    pub node: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance: Option<weft_core::instance::InstanceId>,
}

/// One target of a checked run, as a run of it alone would stand.
// SYNC: TargetCheck <-> packages/weft-graph/src/protocol.ts TargetCheck
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetCheck {
    pub target: String,
    /// The triggers an input it needs only comes from: a run does not
    /// fire them, so that input gets nothing.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unfired_triggers: Vec<String>,
    /// Every other reason it cannot start, one per line.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// The infra copies it reads, as [`RunCheck::infra_read`] says them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub infra_read: Vec<InfraCopyRead>,
}

impl RunCheck {
    /// The check of a run of `definition` shaped by `spec`.
    fn of(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> Self {
        let targets = target_checks(definition, spec, args);
        match infra_read(definition, spec, args) {
            Ok(infra_read) => Self { ok: true, refusal: None, infra_read, targets },
            Err(why) => Self { targets, ..Self::refused(&why) },
        }
    }

    /// A check stopped before the run could be looked at.
    fn refused(why: &anyhow::Error) -> Self {
        Self { ok: false, refusal: Some(format!("{why:#}")), infra_read: Vec::new(), targets: Vec::new() }
    }
}

/// `weft run --check`: compile the program and resolve the run the flags
/// (or the example) describe, the way a run is refused before it starts,
/// and say whether it could. Nothing is built, registered or uploaded;
/// whatever stops the check (the program not compiling, a value that
/// cannot be read) is the refusal, so `--json` always prints its one
/// line.
async fn check(ctx: &Ctx, args: &RunArgs) -> anyhow::Result<std::process::ExitCode> {
    let checked = match prepare_check(ctx, args).await {
        Ok((definition, spec)) => RunCheck::of(&definition, spec.as_ref(), args),
        Err(why) => RunCheck::refused(&why),
    };
    if ctx.json_out(&checked)? {
        return Ok(std::process::ExitCode::SUCCESS);
    }
    match &checked.refusal {
        None => {
            println!("the run can start");
            if !checked.infra_read.is_empty() {
                let copies: Vec<String> = checked
                    .infra_read
                    .iter()
                    .map(|copy| match &copy.instance {
                        Some(instance) => format!("{} (instance '{instance}')", copy.node),
                        None => copy.node.clone(),
                    })
                    .collect();
                println!("while this infra runs, which it reads: {}", copies.join(", "));
            }
            Ok(std::process::ExitCode::SUCCESS)
        }
        Some(refusal) => {
            eprintln!("{refusal}");
            Ok(std::process::ExitCode::FAILURE)
        }
    }
}

/// [`check`]'s preparation: the program and the spec a run started now
/// would read. A program that does not compile cannot run either; its
/// diagnostics go to stderr, never into the one line a `--json` reader
/// parses.
async fn prepare_check(ctx: &Ctx, args: &RunArgs) -> anyhow::Result<(weft_core::ProjectDefinition, Option<RunSpec>)> {
    seed_bounds_need_a_seed(args)?;
    // One snapshot, as a run takes it: the example and the program it
    // reads are the ones a run started now would.
    let snapshot = super::snapshot::Snapshot::take(&ctx.project()?.root)?;
    let mut spec = spec_for(ctx, args, &snapshot.then_disk())?;
    let compiled = super::ensure::compile_snapshot(&crate::progress::Progress::new(ActionVerb::Run, false), snapshot)?;
    if let Some(spec) = &mut spec {
        let (client, _, _) = super::resolve_project(ctx)?;
        super::assets::resolve_run_values(&client, &compiled.files(), spec, false).await?;
    }
    Ok((compiled.definition.clone(), spec))
}

/// The infra copies a run of `definition` shaped by `spec` reads, once
/// the run is held to everything [`validate_run`] refuses.
fn infra_read(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> anyhow::Result<Vec<InfraCopyRead>> {
    let verdict = judge_run(definition, spec, args);
    if let Some(refusal) = verdict.refusal() {
        bail!("{refusal}");
    }
    copies_read(definition, &verdict, spec).map_err(anyhow::Error::msg)
}

/// The infra copies the run [`judge_run`] judged reads: the same places
/// the dispatcher's infra gate looks at, over the same cut. A copy no
/// status could settle (a per-instance place, and no instance named) is
/// the refusal, in the dispatcher's own words.
///
/// The cut is made with nothing saved for baked outputs: the dispatcher
/// only counts what a RUNNING copy saved, so an infra node it leaves out
/// for its saved values is running anyway, and naming it here greys
/// nothing.
fn copies_read(definition: &weft_core::ProjectDefinition, verdict: &Verdict, spec: Option<&RunSpec>) -> Result<Vec<InfraCopyRead>, String> {
    let program = verdict.resolved.as_ref().map_or(definition, |resolved| &resolved.program);
    let instance = spec.and_then(|spec| spec.instance.as_ref());
    let wanted = weft_core::infra::run_gate::copies_read(program, None, instance);
    let unnamed: Vec<_> = wanted.iter().filter(|(_, copy)| copy.is_none()).cloned().collect();
    if !unnamed.is_empty() {
        return Err(weft_core::infra::run_gate::infra_not_running(&weft_core::infra::run_gate::missing_copies(&unnamed, &[])));
    }
    Ok(wanted
        .into_iter()
        .filter_map(|(node, copy)| copy.map(|copy| InfraCopyRead { node, instance: copy.cloned() }))
        .collect())
}

/// Each target of a run of several, as a run of it alone would stand
/// ([`TargetCheck`]); none for a run of one target or none.
fn target_checks(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> Vec<TargetCheck> {
    let Some(spec) = spec.filter(|spec| spec.target.len() > 1) else {
        return Vec::new();
    };
    spec.target
        .iter()
        .map(|target| {
            let alone = RunSpec { target: vec![target.clone()], ..spec.clone() };
            let verdict = judge_run(definition, Some(&alone), args);
            let unfired_triggers: Vec<String> = verdict
                .unfed
                .iter()
                .filter_map(|unfed| unfed.trigger.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            let mut reasons: Vec<String> = verdict
                .unfed
                .iter()
                .filter(|unfed| unfed.trigger.is_none())
                .map(|unfed| unfed.message.clone())
                .chain(verdict.refused.iter().cloned())
                .collect();
            // A cut that could not be made has no infra to name.
            let infra_read = match verdict.resolved {
                None => Vec::new(),
                Some(_) => copies_read(definition, &verdict, Some(&alone)).unwrap_or_else(|why| {
                    // The instance refusal above already says it.
                    if reasons.is_empty() {
                        reasons.push(why);
                    }
                    Vec::new()
                }),
            };
            TargetCheck {
                target: target.clone(),
                unfired_triggers,
                refusal: (!reasons.is_empty()).then(|| reasons.join("\n")),
                infra_read,
            }
        })
        .collect()
}

pub async fn run(ctx: Ctx, args: RunArgs) -> anyhow::Result<std::process::ExitCode> {
    if args.check {
        return check(&ctx, &args).await;
    }
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Run, |progress| async move {
        run_inner(&ctx_inner, &progress, args).await
    })
    .await
}

/// Load saved parameters, apply explicit edits, and omit accepted output
/// evidence from the run request. Graph validation sees the final edit.
/// A saved example is read from the command's snapshot (`files`), so the
/// run uses the example its version holds.
fn spec_for(ctx: &Ctx, args: &RunArgs, files: &super::snapshot::SnapshotFirst<'_>) -> anyhow::Result<Option<RunSpec>> {
    match (&args.spec, args.flags.is_empty()) {
        (Some(name), _) => {
            let spec = super::versions::read_spec_from(ctx.project()?, name, &|path| files.read_to_string(path))?;
            Ok(Some(super::versions::apply_run_flags(&spec.without_expected(), &args.flags)?))
        }
        (None, true) => Ok(args.save.as_ref().map(RunSpec::whole)),
        (None, false) => {
            let name = args.save.clone().unwrap_or_else(|| "one-off".to_string());
            Ok(Some(spec_from_flags(&name, &args.flags)?))
        }
    }
}

/// `--seed-until` and `--seed-before` bound a seed: without `--seed`
/// they are refused before anything is read or saved.
fn seed_bounds_need_a_seed(args: &RunArgs) -> anyhow::Result<()> {
    if (!args.seed_until.is_empty() || !args.seed_before.is_empty()) && args.seed.is_none() {
        bail!("--seed-until and --seed-before need --seed");
    }
    Ok(())
}

async fn run_inner(ctx: &Ctx, progress: &crate::progress::Progress, args: RunArgs) -> anyhow::Result<std::process::ExitCode> {
    seed_bounds_need_a_seed(&args)?;
    let seed_run = match args.seed.as_deref() {
        Some(run) if !run.is_empty() => Some(super::resolve_run_id(ctx, run).await?.parse().context("a resolved run id")?),
        _ => None,
    };
    // The snapshot comes first: the example this run reads, and the one it
    // saves, are the version's.
    let mut snapshot = super::snapshot::Snapshot::take(&ctx.project()?.root)?;
    let mut spec = spec_for(ctx, &args, &snapshot.then_disk())?;
    if let (Some(name), Some(spec)) = (&args.save, &mut spec) {
        spec.name = name.clone();
    }
    if let Some(name) = &args.save {
        let spec = spec.as_ref().expect("saving constructs a run spec");
        // Asked of the disk, which is what the write below would replace.
        if super::versions::read_spec_if_present(ctx.project()?, name)?.is_some_and(|existing| existing.is_frozen()) {
            bail!("example '{name}' contains accepted results; save parameters under another name, or freeze an accepted new run to replace it");
        }
        let (path, text) = super::versions::write_spec(ctx.project()?, &RunSpec { name: name.clone(), ..spec.clone() })?;
        snapshot.record(&path, text.into_bytes())?;
        if !ctx.json() {
            println!("saved {}", path.display());
        }
    }
    let compiled = super::ensure::compile_snapshot(progress, snapshot)?;
    // A `@file(...)` / `@asset(...)` in a handed value becomes the value
    // it stands for before anything checks it, so the port-type check
    // below judges the file, never the marker text. What was saved above
    // keeps the markers, so an example reads its file afresh every run.
    if let Some(spec) = &mut spec {
        let (client, _, _) = super::resolve_project(ctx)?;
        super::assets::resolve_run_values(&client, &compiled.files(), spec, true).await?;
    }
    validate_run(&compiled.definition, spec.as_ref(), &args)?;
    let node_set = args.node_set.unwrap_or(weft_core::builds::NodeSet::Full);
    let definition = compiled.definition.clone();
    let handle = super::ensure::build_compiled(ctx, progress, node_set, compiled).await?;
    if !ctx.json() {
        println!("registered {} ({})", handle.name, handle.id);
    }
    let example = args.save.as_ref().or(args.spec.as_ref()).and_then(|_| spec.as_ref().map(|spec| spec.name.clone()));
    let body = weft_core::versions::VersionRunRequest {
        manifest: handle.manifest.clone(),
        definition_hash: handle.definition_hash().to_string(),
        binary_hash: handle.binary_hash().to_string(),
        seed: args.seed.is_some(),
        seed_run,
        seed_until: args.seed_until.clone(),
        seed_before: args.seed_before.clone(),
        root: args.root,
        spec: spec.clone(),
        example,
    };
    let path = format!("/projects/{}/versions/runs", handle.id);
    progress.dispatcher_call_start(&path);
    let started = start_run(&handle.client, &path, &body).await?;
    let run_id = started.run_id;
    progress.dispatcher_call_done(serde_json::json!({ "runId": run_id, "projectId": handle.id }));

    let summary = summary_line(&started, spec.as_ref());
    if !ctx.json() {
        println!("started run {run_id} on version {}", super::versions::short(&started.version));
    }
    // After the line that says the run started, because that is what
    // they are about ("this run started, but head moved ..."). One
    // shape for a warning whichever verb produced it: the `Warning` phase.
    for w in &started.warnings {
        progress.warn(w);
    }
    handle.warn_broken(progress);
    // The summary is the completion's own line: `Progress` prints it in
    // human mode, so printing it above as well said it twice.
    progress.complete(&summary);

    // --json implies --detach: the extension uses SSE for run
    // events, so keeping the CLI alive to follow logs would just hold
    // the action-bar state machine in `cli_running` while the run is
    // actually `run_running`.
    if args.detach || ctx.json() {
        return Ok(std::process::ExitCode::SUCCESS);
    }
    let outcome = super::follow::follow_run_id(&handle.client, ctx.on(), &run_id.to_string(), Some(&definition)).await?;
    Ok(outcome.exit_code())
}

/// What stands between a run and its start, sorted by what a reader does
/// with it ([`judge_run`]).
#[derive(Default)]
struct Verdict {
    /// The spec's resolution: `None` when there is no spec (the run is the
    /// whole program) or when it could not be resolved (`refused` says why).
    resolved: Option<weft_core::run_spec::Resolved>,
    /// The inputs something in the run needs that get nothing.
    unfed: Vec<weft_core::run_spec::Unfed>,
    /// Every other reason it cannot start.
    refused: Vec<String>,
}

impl Verdict {
    /// Everything that stops the run, as the run itself refuses it.
    fn refusal(&self) -> Option<String> {
        let reasons: Vec<&str> = self.unfed.iter().map(|unfed| unfed.message.as_str()).chain(self.refused.iter().map(String::as_str)).collect();
        (!reasons.is_empty()).then(|| format!("the run cannot start:\n{}", reasons.join("\n")))
    }
}

/// Judge a run the way the dispatcher refuses one before it starts.
fn judge_run(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> Verdict {
    let mut verdict = Verdict::default();
    match spec {
        Some(spec) => match weft_core::run_spec::resolve_spec(spec, definition, &Default::default()) {
            Err(refusal) => verdict.refused.extend(refusal.errors),
            Ok(resolved) => {
                // A seeded run can feed a crossing from history, which only
                // the dispatcher reads; it refuses an unfed input after that.
                // Which instance the run is for it refuses either way.
                if args.seed.is_none() {
                    verdict.unfed = weft_core::run_spec::unfed(definition, &resolved, spec);
                }
                if let Err(refusal) = weft_core::run_spec::refuse_instanceless(&resolved.program, spec.instance.as_ref()) {
                    verdict.refused.extend(refusal.errors);
                }
                verdict.resolved = Some(resolved);
            }
        },
        None => {
            if let Err(refusal) = weft_core::run_spec::refuse_instanceless(definition, None) {
                verdict.refused.extend(refusal.errors);
            }
        }
    }
    if !args.seed_until.is_empty() || !args.seed_before.is_empty() {
        if let Err(why) = weft_core::project::cut::walk(&weft_core::project::graph::ProjectGraph::new(definition), &weft_core::project::cut::SelectionBounds {
            target: args.seed_until.clone(), before: args.seed_before.clone(), ..Default::default()
        }) {
            verdict.refused.push(why);
        }
    }
    verdict
}

/// Refuse what the dispatcher would refuse before the run starts, and
/// hand back the spec's resolution (`None` when there is no spec: the run
/// is the whole program).
fn validate_run(definition: &weft_core::ProjectDefinition, spec: Option<&RunSpec>, args: &RunArgs) -> anyhow::Result<Option<weft_core::run_spec::Resolved>> {
    let verdict = judge_run(definition, spec, args);
    if let Some(refusal) = verdict.refusal() {
        bail!("{refusal}");
    }
    Ok(verdict.resolved)
}

/// Start the run, turning the resolver's 422 into its lines: every
/// missing input at once, each naming the three ways to satisfy it.
pub async fn start_run(
    client: &crate::client::DispatcherClient,
    path: &str,
    body: &weft_core::versions::VersionRunRequest,
) -> anyhow::Result<RunStarted> {
    let (status, text) = client.post_json_status(path, &serde_json::to_value(body)?).await.context("start run")?;
    match status {
        200..=299 => serde_json::from_str(&text).context("parse run response"),
        422 => {
            let refusal: Refusal = serde_json::from_str(&text).context("parse the refusal")?;
            bail!("the run cannot start:\n{refusal}")
        }
        _ => bail!("{}", crate::client::refusal_text(&text).unwrap_or_else(|| format!("dispatcher returned {status}"))),
    }
}

/// The one line a run ends its start with: what was inherited and
/// what runs, or what the spec covered.
pub fn summary_line(started: &RunStarted, spec: Option<&RunSpec>) -> String {
    let mut parts: Vec<String> = Vec::new();
    match &started.seed {
        Some(seed) => {
            parts.push(format!("{} nodes inherited from {}", started.inherited.len(), super::versions::short(&seed.to_string())));
            parts.push(format!("{} ran ({})", started.ran.len(), started.ran.join(", ")));
        }
        None => parts.push(format!("{} nodes", started.ran.len())),
    }
    if let Some(spec) = spec {
        let mut what = vec![format!("spec {}", spec.name)];
        match spec.from.values().map(|ports| ports.len()).sum::<usize>() + spec.group.as_ref().map(|(_, ports)| ports.len()).unwrap_or(0) {
            0 => {}
            1 => what.push("1 handed value".to_string()),
            n => what.push(format!("{n} handed values")),
        }
        if let Some((node, _)) = &spec.fire {
            what.push(format!("fired {node}"));
        }
        parts.insert(0, what.join(", "));
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(seed: Option<&str>, inherited: &[&str], ran: &[&str]) -> RunStarted {
        RunStarted {
            run_id: uuid::Uuid::nil(),
            version: "v".into(),
            seed: seed.map(|s| format!("{s}-0000-0000-0000-000000000000").parse().expect("a uuid")),
            inherited: inherited.iter().map(|s| s.to_string()).collect(),
            ran: ran.iter().map(|s| s.to_string()).collect(),
            warnings: vec![],
        }
    }

    /// `source` compiled as `main.weft`, with `files` beside it.
    fn compiled(source: &str, files: &[(&str, &str)]) -> weft_core::ProjectDefinition {
        let dir = tempfile::tempdir().unwrap();
        for (name, text) in files {
            std::fs::write(dir.path().join(name), text).unwrap();
        }
        let catalog = weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap();
        let (definition, diagnostics) = weft_compiler::compile_strict(
            source,
            uuid::Uuid::new_v4(),
            weft_compiler::CompileFs::disk(dir.path()),
            &catalog,
            weft_compiler::validate::ValidationMode::Structural,
            None,
        );
        assert!(
            !diagnostics.iter().any(|d| matches!(d.severity, weft_compiler::Severity::Error)),
            "compiles: {}",
            weft_compiler::render_diagnostics(&diagnostics)
        );
        definition
    }

    fn checked(definition: &weft_core::ProjectDefinition, targets: &[&str]) -> serde_json::Value {
        let spec = RunSpec { target: targets.iter().map(|t| t.to_string()).collect(), ..RunSpec::whole("check") };
        serde_json::to_value(super::RunCheck::of(definition, Some(&spec), &RunArgs::default())).unwrap()
    }

    #[test]
    fn a_check_prints_ok_or_the_refusal() {
        let refused = serde_json::to_value(super::RunCheck::refused(&anyhow::anyhow!("needs `door`"))).unwrap();
        assert_eq!(refused, serde_json::json!({ "ok": false, "refusal": "needs `door`" }));
        let definition = compiled("lone = Text { value: \"hi\" }\nshow = Debug\nshow.data = lone.value\n", &[]);
        assert_eq!(checked(&definition, &["show"]), serde_json::json!({ "ok": true }));
    }

    /// The check names the infra a run reads the way the dispatcher's gate
    /// does: an infra node wired straight into the target, never started
    /// or not (the check knows nothing of status), and through an include,
    /// whether the target reads out of the include site or is the site
    /// itself, spelled by its site, and only the site the run reaches.
    #[test]
    fn a_check_names_the_infra_a_run_reads() {
        let direct = compiled("postgresDatabase1 = PostgresDatabase\ndebug1 = Debug\ndebug1.data = postgresDatabase1.access\n", &[]);
        assert_eq!(checked(&direct, &["debug1"]), serde_json::json!({ "ok": true, "infraRead": [{ "node": "postgresDatabase1" }] }));

        let store = "Group() -> (conn: Access) {\n  db = PostgresDatabase\n  self.conn = db.access\n}\n";
        let included = compiled(
            "s = @include(\"store.weft\")\nt = @include(\"store.weft\")\nshow = Debug\nshow.data = s.conn\nlone = Text { value: \"hi\" }\n",
            &[("store.weft", store)],
        );
        let reads = |targets: &[&str]| checked(&included, targets)["infraRead"].clone();
        assert_eq!(reads(&["show"]), serde_json::json!([{ "node": "s.db" }]));
        assert_eq!(reads(&["s"]), serde_json::json!([{ "node": "s.db" }]));
        assert_eq!(reads(&["lone"]), serde_json::Value::Null);

        // Infra declared in the including file, read inside a group.
        let grouped = compiled(
            "pg = PostgresDatabase\nwrap = Group(conn: Access) -> () {\n  inner = Debug\n  inner.data = self.conn\n}\nwrap.conn = pg.access\n",
            &[],
        );
        assert_eq!(checked(&grouped, &["wrap.inner"])["infraRead"], serde_json::json!([{ "node": "pg" }]));
    }

    /// A run of several targets is one run, refused whole; the check also
    /// says each target alone: the one behind a trigger names that trigger,
    /// the one behind infra names the infra it reads, the free one nothing.
    #[test]
    fn a_check_of_several_targets_says_each_alone() {
        let definition = compiled(
            "door = Cron { cron: \"0 0 * * * *\" }\nreply = Format(p: String) { template: \"{{p}}\", p: door.scheduledTime }\n\
             pg = PostgresDatabase\ndebug = Debug\ndebug.data = pg.access\nlone = Text { value: \"hi\" }\n",
            &[],
        );
        let all = checked(&definition, &["reply", "debug", "lone"]);
        assert_eq!(all["ok"], false);
        assert_eq!(
            all["targets"],
            serde_json::json!([
                { "target": "reply", "unfiredTriggers": ["door"] },
                { "target": "debug", "infraRead": [{ "node": "pg" }] },
                { "target": "lone" },
            ])
        );
        // Without the trigger's side, the run of the rest goes, reading pg.
        let rest = checked(&definition, &["debug", "lone"]);
        assert_eq!(rest["ok"], true);
        assert_eq!(rest["infraRead"], serde_json::json!([{ "node": "pg" }]));
        assert_eq!(rest["targets"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn the_summary_line_says_what_was_inherited_and_what_ran() {
        let s = started(Some("3f2a1111"), &["a", "b"], &["classify", "reply"]);
        assert_eq!(summary_line(&s, None), "2 nodes inherited from 3f2a1111, 2 ran (classify, reply)");
        let s = started(None, &[], &["a", "b", "c"]);
        assert_eq!(summary_line(&s, None), "3 nodes");
        let mut spec = RunSpec::whole("angry-customer");
        spec.from.entry("x".into()).or_default().insert("p".into(), serde_json::json!(1));
        spec.fire = Some(("inbound".into(), serde_json::json!({})));
        assert_eq!(summary_line(&s, Some(&spec)), "spec angry-customer, 1 handed value, fired inbound, 3 nodes");
    }
}
