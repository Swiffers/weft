//! Run state read endpoints. Writers journal events directly
//! to Postgres from the worker. What's left here is: cancel
//! (control), delete (cleanup), and the reader endpoints the CLI
//! and VS Code extension hit over HTTP: logs, replay,
//! list_runs, get.

use std::collections::HashMap;
use std::sync::Arc;

use weft_core::ProjectDefinition;

use axum::{extract::{Path, Query, State}, http::StatusCode, Json};
use serde::Deserialize;

use weft_core::run::CancelCause;
use weft_core::RunId;

use crate::authenticator::{authorize_run, authorize_project, CallerTenant};
use crate::store::RunListQuery;
use weft_core::program::{DeletedRun, RunDetail, RunPage, ParkedWait, ResolvedRun};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// How many ambiguous matches an error names before it stops. Enough to
/// recognise the run you meant, few enough that the instruction after
/// them is still on screen.
const AMBIGUOUS_PREFIX_SHOWN: usize = 5;

/// The one run of the caller's whose run starts with `prefix`
/// (a full uuid resolves to itself). 404 when nothing matches, 409
/// when the prefix is short enough to match several, naming them.
pub async fn resolve_run_id(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(prefix): Path<String>,
) -> Result<Json<ResolvedRun>, (StatusCode, String)> {
    let prefix = prefix.to_ascii_lowercase();
    if prefix.len() < 4 || !prefix.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("'{prefix}' is not the start of a run id: give at least four hex characters"),
        ));
    }
    let matches = state
        .runs
        .run_ids_with_prefix(caller.0.as_str(), &prefix)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("run_ids_with_prefix: {e}")))?;
    match matches.as_slice() {
        [] => Err((StatusCode::NOT_FOUND, format!("no run starts with '{prefix}'"))),
        [one] => Ok(Json(ResolvedRun { run_id: *one })),
        // A count and a few short ids, never the whole list: an empty or
        // one-character prefix matches everything the project ever ran,
        // and printing a hundred full uuids buries the one sentence that
        // says what to do about it.
        several => Err((
            StatusCode::CONFLICT,
            format!(
                "'{prefix}' starts {} runs ({}{}); give more characters",
                several.len(),
                several
                    .iter()
                    .take(AMBIGUOUS_PREFIX_SHOWN)
                    .map(|c| c.to_string()[..8].to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                if several.len() > AMBIGUOUS_PREFIX_SHOWN { ", ..." } else { "" },
            ),
        )),
    }
}

pub async fn cancel(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let run_id: RunId = run_id_str
        .parse()
        .map_err(|e: uuid::Error| (StatusCode::BAD_REQUEST, e.to_string()))?;
    authorize_run(&*state.runs, &caller.0, run_id).await?;
    // A run that already ended has nothing to cancel: said so, with
    // its status, instead of a silent no-op the caller would wait on
    // forever (the editor's Stop once sat on "Cancelling..." for a run
    // that had finished an hour before).
    if let Some(summary) = state
        .runs
        .run_summary(run_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("run summary: {e}")))?
    {
        if let Some(refusal) = already_ended(summary.status) {
            return Err((StatusCode::CONFLICT, format!("run {run_id} already ended ({refusal})")));
        }
    }
    cancel_run_id(&state, run_id, &CancelCause::User).await.map_err(|e| {
        tracing::error!(target: "weft_dispatcher::cancel", run_id = %run_id, error = %e, "cancel_run_id failed");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// A `500` for a handler that answers a bare status, with why it failed in
/// the log: the answer carries no body to say it in.
fn failed(what: &'static str) -> impl FnOnce(anyhow::Error) -> StatusCode {
    move |e| {
        tracing::error!(target: "weft_dispatcher::api", error = %format!("{e:#}"), "could not {what}");
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

/// The status word to answer a cancel of a run that is over with, or
/// `None` while the run can still be cancelled.
fn already_ended(status: weft_core::program::RunStatus) -> Option<&'static str> {
    use weft_core::program::RunStatus;
    match status {
        RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled => Some(status.as_str()),
        RunStatus::Running | RunStatus::WaitingForInput => None,
    }
}

/// Cancel every run in `targets`, each with its own cause, attempting
/// ALL of them before reporting. One failing run must not strand the
/// ones after it (they would stay live with their wakes registered), and
/// a failure must not disappear either: if any cancel failed, the
/// result is an error naming every failed run and why, so a task
/// built on this is recorded failed with the real errors, never
/// completed with a count nobody reads. Returns the runs cancelled.
pub async fn cancel_run_ids(
    state: &DispatcherState,
    targets: &[(RunId, &CancelCause)],
) -> anyhow::Result<Vec<RunId>> {
    let mut cancelled = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for (run_id, cause) in targets {
        match cancel_run_id(state, *run_id, cause).await {
            Ok(()) => cancelled.push(*run_id),
            Err(e) => failures.push(format!("{run_id}: {e:#}")),
        }
    }
    if !failures.is_empty() {
        // Name both sides: whoever repairs this by hand needs to know
        // which runs are terminal now and which are still live.
        let succeeded: Vec<String> = cancelled.iter().map(|c| c.to_string()).collect();
        anyhow::bail!(
            "{} of {} cancel(s) failed: {}; cancelled: [{}]",
            failures.len(),
            failures.len() + cancelled.len(),
            failures.join("; "),
            succeeded.join(", ")
        );
    }
    Ok(cancelled)
}

/// Cancel a single run, for `cause`. THE cancel: every caller
/// (`weft stop`, the sweeps, a sibling run's `stop_tagged`, a drain at its
/// deadline) goes through here and says why.
///
/// The durable part is ONE transaction (`RunStore::cancel_run`),
/// under the run's row lock: its wake signals go, and then
///
///   - a run a worker drives gets its `cancel_requested` set and the
///     cancel announced (`weft_task_store::runs::CANCEL_CHANNEL`): the
///     broker pushes it down the worker's line, the worker fires the
///     run's flag and writes its ending with this cause, and a worker
///     that claims the run later reads the cancel with its claim;
///   - a run nobody drives (parked, queued) is ended here, its per-node
///     cancels, its open charges booked unknown and its
///     `RunCancelled` written at `last_seq + 1`
///     ([`cancel_terminal_events`]).
///
/// Either all of it lands or none does, so a database failure mid-cancel
/// leaves the run exactly as it was and the next attempt succeeds. After
/// the commit the listener forgets the stripped signals (the rows are
/// already gone, so a late fire finds nothing), and the live view paints
/// the ending for whoever watches.
pub async fn cancel_run_id(
    state: &DispatcherState,
    run_id: RunId,
    cause: &CancelCause,
) -> anyhow::Result<()> {
    tracing::info!(
        target: "weft_dispatcher::cancel",
        run_id = %run_id,
        %cause,
        "cancel_run_id start"
    );
    // The per-node cancels come off the fold, which needs the run's
    // program; a run with none (never started, a node self-test)
    // has no nodes to cancel, and a program that cannot be found is no
    // reason to leave the run running: the terminal lands anyway.
    let program = program_for_cancel(state, run_id).await?;
    let write = state.runs.cancel_run(run_id, program.as_deref(), cause).await?;
    state.listener.unregister_many(&write.removed).await;
    tracing::info!(
        target: "weft_dispatcher::cancel",
        run_id = %run_id,
        signals_removed = write.removed.len(),
        asked_its_worker = write.requested,
        node_cancellations = ?write.node_cancellations,
        "cancel committed"
    );
    Ok(())
}

/// THE definition of a dispatcher-side cancel of a run nobody drives: the
/// ordered events that end it, `NodeCancelled` per open node, an unknown
/// `CostReported` per charge its record leaves open
/// (`weft_journal::unpriced_charges_closed`), then `RunCancelled`
/// (always the last). Pure, so every cancel writer
/// writes IDENTICAL rows. They are written in one row under the run's row
/// lock, so the ending never lands without its per-node cancels.
pub fn cancel_terminal_events(
    run_id: RunId,
    events: &[weft_journal::RunEvent],
    program: Option<&ProjectDefinition>,
    cause: &CancelCause,
    now: u64,
) -> anyhow::Result<Vec<weft_journal::RunEvent>> {
    use weft_journal::RunEvent;
    let reason = cause.to_string();
    let mut writes = Vec::new();
    match program {
        Some(program) => {
            // The runs map says which nodes are still non-terminal.
            // A row the fold could not apply was logged at `error!`
            // level and is the inspector's `/replay` to show; the
            // records that did fold are the ones to flip.
            let snapshot = weft_journal::fold_to_snapshot(
                run_id,
                Arc::new(program.clone()),
                events,
            );
            for (node_id, firings) in &snapshot.firings {
                for e in firings {
                    if e.status.is_terminal() {
                        continue;
                    }
                    writes.push(RunEvent::NodeCancelled {
                        run_id,
                        node_id: node_id.clone(),
                        frames: e.frames.clone(),
                        reason: reason.clone(),
                        at_unix: now,
                    });
                }
            }
        }
        // No program: a run that never ran a node body (a node
        // self-test) has nothing per node to flip. A run whose
        // program is gone (its project was removed) or cannot be read
        // still ends, on the terminal alone: the stop must land, and
        // the run's status is read off the terminal. Its node records
        // stay as they were (the run view paints them from a
        // fold this run no longer has), which is what a run without a
        // program looks like everywhere else.
        None => {
            if events.iter().any(|e| matches!(e, RunEvent::NodeStarted { .. })) {
                tracing::warn!(
                    target: "weft_dispatcher::cancel",
                    %run_id,
                    "cancelling without the run's program: the terminal is written, the \
                     per-node cancels cannot be derived"
                );
            }
        }
    }
    // A charge its record leaves open spent money no drive is left to
    // price: booked unknown before the ending.
    let why = format!("the run was cancelled before the job was read back: {reason}");
    writes.extend(weft_journal::unpriced_charges_closed(run_id, events, &why, now));
    writes.push(RunEvent::RunCancelled { run_id, reason, cause: Some(cause.clone()), at_unix: now });
    Ok(writes)
}

/// The program a cancel folds with to derive its per-node cancels, or
/// `None` when the run has none to fold with: no program at all, a
/// removed project, or a program that cannot be read (logged; the
/// stop still lands on the terminal). `Err` only for the database.
pub async fn program_for_cancel(
    state: &crate::state::DispatcherState,
    run_id: RunId,
) -> anyhow::Result<Option<Arc<ProjectDefinition>>> {
    let lookup = crate::projection::run_program(state, run_id).await?;
    // Both unpaintable states are worth saying out loud here, and
    // `unpaintable` is the one place that knows which they are: an
    // unreadable row and a retired program both end with the terminal
    // written and no per-node cancels, and a person reading the log
    // needs to know which of the two they are looking at.
    if let Some((site, reason)) = lookup.unpaintable() {
        tracing::warn!(
            target: "weft_dispatcher::cancel",
            %run_id, %reason, ?site,
            "cancelling a run whose program cannot be folded: the terminal is written, the \
             per-node cancels cannot be derived"
        );
    }
    Ok(lookup.program())
}

/// The terminal outcome recorded for a run, if any: its run row's
/// `outcome`, stamped by the write that records its ending. `None` means the run is
/// still in flight. Used both for cancel-dedup and as the source of
/// truth when the in-RAM event bus drops events (broadcast `Lagged`).
pub(crate) async fn terminal_outcome(
    pool: &sqlx::PgPool,
    run_id: RunId,
) -> anyhow::Result<Option<TerminalOutcome>> {
    let outcome: Option<Option<String>> = sqlx::query_scalar("SELECT outcome FROM run WHERE run_id = $1")
        .bind(run_id)
        .fetch_optional(pool)
        .await?;
    Ok(outcome.flatten().map(|outcome| match outcome.as_str() {
        "completed" => TerminalOutcome::Completed,
        "cancelled" => TerminalOutcome::Cancelled,
        _ => TerminalOutcome::Failed,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalOutcome {
    Completed,
    Failed,
    Cancelled,
}

pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id_str): Path<String>,
) -> Result<Json<RunDetail>, StatusCode> {
    let run_id: RunId = run_id_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    authorize_run(&*state.runs, &caller.0, run_id)
        .await
        .map_err(|(s, _)| s)?;
    // Direct point-lookup by run (authorization above already proved the
    // caller owns it), so a run older than any list window still
    // resolves instead of 404ing.
    let summary = state
        .runs
        .run_summary(run_id)
        .await
        .map_err(failed("read the run's summary"))?
        .ok_or(StatusCode::NOT_FOUND)?;
    // The waits ride with the status they produced: a run reads as
    // `waiting_for_input` because these rows exist, and they exist
    // before the journal's `NodeSuspended` lands (the node registers
    // its wait, then returns), so a client that learns the run is
    // parked learns from the same read what it is parked on.
    let waiting = if summary.status == weft_core::program::RunStatus::WaitingForInput {
        parked_waits(&state, run_id).await.map_err(failed("read the run's waits"))?
    } else {
        Vec::new()
    };
    Ok(Json(RunDetail { summary, waiting }))
}

/// The waits of `run_id`, from the resume-signal rows that make it
/// `waiting_for_input`, in registration order.
async fn parked_waits(state: &DispatcherState, run_id: RunId) -> anyhow::Result<Vec<ParkedWait>> {
    let signals = state.registrations.signal_list_for_run_id(run_id).await?;
    waits_of(&signals, run_id)
}

fn waits_of(signals: &[crate::store::SignalRegistration], run_id: RunId) -> anyhow::Result<Vec<ParkedWait>> {
    signals
        .iter()
        .filter(|s| s.is_resume && s.run_id == Some(run_id))
        .map(|s| {
            let spec: weft_core::primitive::SignalSpec = serde_json::from_str(&s.spec_json)?;
            Ok(ParkedWait { node: s.node_id.clone(), token: s.token.clone(), kind: spec.kind })
        })
        .collect()
}

/// How many log lines one read returns, and the ceiling on asking for
/// more. `RunStore::logs_for` answers with the TAIL, so the default holds the
/// end of a long run, which is where a run goes wrong. A `limit`
/// outside `1..=MAX` is refused, not quietly moved: a caller who
/// asked for more than the ceiling would otherwise read a cut log as
/// the whole one.
const DEFAULT_LOG_LINES: u32 = 1_000;
const MAX_LOG_LINES: u32 = 20_000;

#[derive(Debug, Deserialize)]
pub struct ListLogsParams {
    pub limit: Option<u32>,
}


pub async fn list_logs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id_str): Path<String>,
    Query(params): Query<ListLogsParams>,
) -> Result<Json<weft_core::program::RunLogs>, (StatusCode, String)> {
    let run_id: RunId = run_id_str
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("'{run_id_str}' is not a run id (a uuid)")))?;
    let owner = authorize_run(&*state.runs, &caller.0, run_id).await?;
    let limit = params.limit.unwrap_or(DEFAULT_LOG_LINES);
    if !(1..=MAX_LOG_LINES).contains(&limit) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("limit is {limit}; one read holds between 1 and {MAX_LOG_LINES} lines"),
        ));
    }
    // The journal's error names the run and `weft clean` when a row
    // no longer decodes; the reader gets it, not a bare 500.
    let entries = crate::projection::run_logs(&state, run_id, limit)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    // Each line's node is spelled the way the program the run was started
    // on is written, whatever its project holds now.
    let lookup = crate::projection::registered_program(&state, run_id, &owner).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the run's program: {e:#}")))?;
    let program = lookup.program();
    // Indexed once: every line is spelled through it.
    let graph = program.as_deref().map(weft_core::project::graph::ProjectGraph::new);
    let spell = |node: String, frames: &weft_core::LoopFrames| {
        let Some(program) = &graph else { return node };
        let path: Vec<String> = weft_core::frames::call_path(frames).into_iter().map(str::to_string).collect();
        weft_core::project::placed_address(program, &node, &path).unwrap_or_else(|| {
            tracing::error!(target: "weft_dispatcher::api", %run_id, %node, "a log line names a node the run's program does not have; it is shown by its id");
            node
        })
    };
    let lines = entries
        .into_iter()
        .map(|e| weft_core::program::RunLogLine {
            inherited_from: e.inherited_from,
            at_unix: e.at_unix,
            level: e.level,
            node: e.node.map(|node| spell(node, &e.frames)),
            frames: e.frames,
            message: e.message,
        })
        .collect();
    let unspelled = lookup.unpaintable().map(|(_, why)| why.to_string());
    Ok(Json(weft_core::program::RunLogs { limit, lines, unspelled }))
}

#[derive(Debug, Deserialize)]
pub struct OutputsParams {
    /// The places whose outputs a later diff puts first (`weft freeze
    /// --expect`), comma-separated, each a node or a loop spelled the way
    /// the program the run ran is written; the answer's `focus` holds
    /// them read against it.
    pub expect: Option<String>,
}

/// Complete ordered output evidence, including streams and inherited results.
pub async fn outputs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id_str): Path<String>,
    Query(params): Query<OutputsParams>,
) -> Result<Json<weft_core::run_spec::Expected>, (StatusCode, String)> {
    let run_id: RunId = run_id_str.parse().map_err(|_| (StatusCode::BAD_REQUEST, "bad run".into()))?;
    let owner = authorize_run(&*state.runs, &caller.0, run_id).await?;
    let sources = crate::projection::reconstruct_run(&state, run_id).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("read output history: {error:#}")))?;
    let fold = &sources[&run_id];
    // The run's own program is its cut, and a seeded run's cut leaves out
    // what it reuses (a run that reuses everything it reaches runs an
    // empty one), while its record holds the reused results too. So every
    // place is spelled through the registered program the cut was taken
    // from: ids are that program's own, and only it still has the loop a
    // reused `LoopIn` or `LoopOut` belongs to.
    let program = crate::projection::registered_program(&state, run_id, &owner).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the run's program: {error:#}")))?
        .or_why(run_id)
        .map_err(|why| (StatusCode::CONFLICT, why))?;
    // Indexed once: every wire and place below is spelled through it.
    let project = &weft_core::project::graph::ProjectGraph::new(&program);
    // A loop's `LoopIn` is the compiler's: what it hands the body is the
    // wire of the node that fed the loop, so its wire says nothing a
    // person's wire does not, and its name (the loop's) would collide
    // with theirs. Its `LoopOut` is different: the list it gathers over
    // the iterations exists nowhere else, so it stays, as the loop's own
    // output (`doubler.results`).
    let boundary = |id: &str| weft_core::project::graph::GraphView::node(project, id)
        .is_some_and(|n| n.group_boundary.is_some() && n.node_type != weft_core::project::boundary_types::LOOP_OUT);
    let wires = fold.output_wires()
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .iter().filter(|wire| !boundary(&wire.node))
        .map(|wire| weft_core::run_spec::ExpectedWire::spell(project, wire)).collect();
    // Every place the run covers, spelled the way the wires are: what its
    // cut runs and what its seed reused (a node in an included file once
    // per site that reaches it, a loop once).
    let nodes = weft_core::project::places::every_place(fold.project()).into_iter()
        .chain(fold.snapshot().inherited_origins.keys().cloned())
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect::<std::collections::BTreeSet<_>>().into_iter().collect::<Vec<String>>();
    let mut focus = std::collections::BTreeSet::new();
    for spelled in weft_core::comma_list::comma_list(params.expect.iter()) {
        let spelling = match named_in_run(Ok::<_, &String>(project), run_id, &spelled)?.1 {
            weft_core::project::places::Named::Node(place) => weft_core::project::address_of(project, &place.id, &place.path),
            weft_core::project::places::Named::Group(place) if weft_core::project::graph::GraphView::is_loop(project, &place.id) => {
                weft_core::project::group_address(project, &place.id, &place.path)
            }
            weft_core::project::places::Named::Group(_) => {
                return Err((StatusCode::BAD_REQUEST, format!("'{spelled}' is a group, which has no outputs of its own; name a node, or a loop")));
            }
        };
        if !nodes.contains(&spelling) {
            return Err((StatusCode::BAD_REQUEST, format!("'{spelled}' did not run in run {run_id}, so it has no outputs there")));
        }
        focus.insert(spelling);
    }
    Ok(Json(weft_core::run_spec::Expected { wires, nodes, focus: focus.into_iter().collect() }))
}

/// Where one node of a run stands (`weft_core::program::NodeState`), the
/// same answer a program's `GetNodeState` reads. The node is spelled the
/// way `--from` takes it (`one.render`).
pub async fn node_state(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((run_id_str, node)): Path<(String, String)>,
) -> Result<Json<weft_core::program::NodeState>, (StatusCode, String)> {
    let run_id: RunId = run_id_str.parse().map_err(|_| (StatusCode::BAD_REQUEST, format!("'{run_id_str}' is not a run id")))?;
    let project_id = authorize_run(&*state.runs, &caller.0, run_id).await?.project_id;
    Ok(Json(crate::node_state::read(&state, project_id, run_id, &node).await?))
}

/// Replay journaled history through the same projection as live events.
/// Terminals come from the journal, never a synthesized second completion;
/// a run parked now ends its replay with `RunParked`, the one event about
/// where it stands rather than what it did.
pub async fn replay(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id_str): Path<String>,
) -> Result<Json<Vec<crate::events::LiveEvent>>, StatusCode> {
    let run_id: RunId = run_id_str.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    // Resolve + tenant-gate in the ONE place that owns "who owns this
    // run": a lookup failure is 500, an unknown or cross-tenant
    // run is 404, and the resolved project rides back for the
    // replay's event attribution.
    let project_id =
        authorize_run(&*state.runs, &caller.0, run_id).await.map_err(|(s, _)| s)?.project_id;
    replay_events(&state, run_id, project_id).await.map(Json)
}

/// The rows [`replay`] answers, for a run the caller was authorized for.
async fn replay_events(
    state: &DispatcherState,
    run_id: RunId,
    project_id: uuid::Uuid,
) -> Result<Vec<crate::events::LiveEvent>, StatusCode> {
    // The full record, folded over the run's program through the SAME
    // projection the live view runs (`crate::live_view`), so replay and
    // live cannot drift; bus and caller events ride along.
    // Lossy read: the inspector renders what exists, and every row
    // that no longer decodes lands below as its own JournalCorruption
    // entry, naming `weft clean`, instead of taking the response down.
    let (raw_events, unreadable_rows) =
        state.runs.events_log_lossy(run_id).await.map_err(|e| {
            tracing::error!(
                target: "weft_dispatcher::api",
                %run_id, error = %e,
                "replay: reading the journal failed"
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    // Only the database itself fails the read; a program that cannot
    // be found paints the run as unreadable, the way the live bridge
    // does for the same journal, and names `weft clean` below.
    // Everything the reader must be TOLD about this run, with the site
    // that places it on screen.
    let mut corruptions: Vec<(weft_core::primitive::CorruptionSite, String)> = unreadable_rows
        .into_iter()
        .map(|reason| (weft_core::primitive::CorruptionSite::UndecodableRow, reason))
        .collect();
    let found = crate::projection::run_program(state, run_id).await.map_err(|e| {
        tracing::error!(
            target: "weft_dispatcher::api",
            %run_id, error = %e,
            "replay: looking up the run's program failed"
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let program = {
        match found.unpaintable() {
            // The reason travels with the run, at its own site: a row
            // that no longer decodes is one missing value, a missing
            // program is every value at once and belongs beside the run
            // rather than inside a node.
            Some((site, reason)) => {
                corruptions.push((site, reason.to_string()));
                found.without_reason()
            }
            None => found,
        }
    };
    let rows: Vec<weft_journal::RunEvent> = raw_events.iter().map(|r| r.event.clone()).collect();
    let inheritance = match crate::projection::run_inheritance(state, &rows, &program).await {
        Ok(chain) => chain,
        Err(reason) => {
            corruptions.push((weft_core::primitive::CorruptionSite::UndecodableRow, reason));
            weft_journal::SeedChain::default()
        }
    };
    let mut projector = crate::projection::RunProjector::new(run_id, program, project_id)
        .with_inheritance(inheritance);
    let mut out: Vec<crate::events::LiveEvent> = Vec::new();
    for record in raw_events {
        let projected = projector.project(&record.event);
        out.extend(
            crate::events::IdentifiedEvent { event_id: record.event_id, event: () }
                .project(|()| projected),
        );
    }
    // A run parked now says so after its history: parking writes no
    // record row, and a reader that takes the history before the live
    // events (`weft wait --waiting-for-input`) learns here that it waits.
    let summary = state.runs.run_summary(run_id).await.map_err(|e| {
        tracing::error!(target: "weft_dispatcher::api", %run_id, error = %e, "replay: reading the run's status failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if summary.is_some_and(|summary| summary.status == weft_core::program::RunStatus::WaitingForInput) {
        out.push(crate::events::IdentifiedEvent::transient(DispatcherEvent::RunParked { run_id, project_id }));
    }
    // A row the fold could not apply is painted as its corruption, at
    // the row, by the projector; the rows that never decoded follow.
    for (site, reason) in corruptions {
        out.push(crate::events::IdentifiedEvent::transient(DispatcherEvent::JournalCorruption {
            run_id,
            project_id,
            site,
            reason,
        }));
    }
    Ok(out)
}

/// What `GET /runs/{id}/events` keeps: the rows about one node or group
/// (`node`, spelled the way the program the run ran is written), of one
/// kind, inside some loop iterations (`iteration`, `3` or `3.0`). All
/// optional.
#[derive(Debug, Deserialize)]
pub struct EventsParams {
    pub node: Option<String>,
    pub kind: Option<String>,
    pub iteration: Option<String>,
}

/// A run's events the way a person reads them (`weft events`): the
/// replay's rows, kept by `params` and spelled, both against the program
/// the run was started on, never whatever program its project holds now.
/// So a node renamed or moved since is found by the name it had in that
/// run, and its rows print under that name. A name that program does not
/// have is refused naming the run. When the program cannot be read, the
/// rows name their nodes by id and say why, and only a node filter, which
/// cannot be read without it, is refused.
pub async fn events(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id_str): Path<String>,
    Query(params): Query<EventsParams>,
) -> Result<Json<weft_core::program::RunEvents>, (StatusCode, String)> {
    let run_id: RunId = run_id_str.parse().map_err(|_| (StatusCode::BAD_REQUEST, format!("'{run_id_str}' is not a run id")))?;
    let owner = authorize_run(&*state.runs, &caller.0, run_id).await?;
    let lookup = crate::projection::registered_program(&state, run_id, &owner).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the run's program: {e:#}")))?;
    let rows = replay_events(&state, run_id, owner.project_id).await.map_err(|status| (status, "read the run's events".to_string()))?;
    events_answer(run_id, &lookup, &rows, params).map(Json)
}

/// [`events`] once the run's program and rows are read: pure, so how a
/// name is read against the run's own program is pinned without a
/// database.
fn events_answer(
    run_id: RunId,
    lookup: &crate::projection::ProgramLookup,
    rows: &[crate::events::LiveEvent],
    params: EventsParams,
) -> Result<weft_core::program::RunEvents, (StatusCode, String)> {
    use weft_core::live_event::spelled::{parse_iteration, spell_row, EventsFilter};
    let program = lookup.or_why(run_id);
    // Indexed once for the request: every row is spelled through it, so a
    // lookup per row is constant time, not a scan of the program.
    let graph = program.as_deref().map(weft_core::project::graph::ProjectGraph::new);
    let iteration = params.iteration.as_deref().map(parse_iteration).transpose()
        .map_err(|why| (StatusCode::BAD_REQUEST, why))?.unwrap_or_default();
    let filter = match &params.node {
        Some(spelled) => {
            let (graph, named) = named_in_run(graph.as_ref().map_err(|why| *why), run_id, spelled)?;
            EventsFilter::at(graph, named, params.kind, iteration)
        }
        None => EventsFilter::anywhere(params.kind, iteration),
    }
    .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let mut events = Vec::new();
    for row in rows {
        let wire = serde_json::to_value(row).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("an event: {e}")))?;
        if !filter.keeps(&row.event, &wire) { continue; }
        match &graph {
            Ok(graph) => events.extend(spell_row(wire, graph)),
            Err(_) => events.push(wire),
        }
    }
    let unspelled = lookup.unpaintable().map(|(_, why)| why.to_string());
    Ok(weft_core::program::RunEvents { total: rows.len(), events, unspelled })
}

/// What `spelled` names in the program `run_id` was started on
/// (`weft_core::project::places::named`): a 400 naming the run for a name
/// that program does not have (the name a person has now may not be the
/// one it had then), a 409 when the program cannot be read. Answers with
/// the program it was read in.
fn named_in_run<'g, G: weft_core::project::graph::GraphView>(
    program: Result<&'g G, &String>,
    run_id: RunId,
    spelled: &str,
) -> Result<(&'g G, weft_core::project::places::Named), (StatusCode, String)> {
    let program = program.map_err(|why| {
        (StatusCode::CONFLICT, format!("'{spelled}' cannot be read: names are read against the program run {run_id} ran, and {why}"))
    })?;
    weft_core::project::places::named(program, spelled).map(|named| (program, named))
        .map_err(|why| (StatusCode::BAD_REQUEST, format!("in the program run {run_id} ran: {why}")))
}

/// Query params for the runs list: page size + offset, and optional
/// project + start-time-range filters. All optional; `limit` defaults to a page
/// and is capped so a single request can never pull an unbounded slice.
#[derive(Debug, Deserialize)]
pub struct ListRunsParams {
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub project_id: Option<uuid::Uuid>,
    /// Inclusive lower bound on start time (unix seconds).
    pub started_after: Option<u64>,
    /// Exclusive upper bound on start time (unix seconds).
    pub started_before: Option<u64>,
    /// Only runs of this phase (`fire`, `trigger_setup`, `infra_setup`).
    pub phase: Option<weft_core::context::Phase>,
    /// Only runs started by this entry node.
    pub entry_node: Option<String>,
    /// Only runs standing this way ([`weft_core::program::RunStatus`]);
    /// any other word is a 400 before anything is read.
    pub status: Option<weft_core::program::RunStatus>,
    /// Only runs for this instance.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// Only runs carrying this tag.
    pub tag: Option<String>,
    /// Only finished runs in which this node fired, once their search
    /// entry is built (`crate::search_index`).
    pub node: Option<String>,
    /// Only finished runs whose recorded values carry every word of this,
    /// once their search entry is built (`crate::search_index`).
    pub search: Option<String>,
    /// The page after the run that started at `before_started` with id
    /// `before_run` (the last run of the page before): a walk that
    /// hands each page's last run back here reaches every run once,
    /// whatever starts or ends meanwhile, which `offset` cannot promise.
    pub before_started: Option<u64>,
    pub before_run: Option<RunId>,
}

const DEFAULT_PAGE: u32 = 50;
const MAX_PAGE: u32 = weft_core::program::MAX_RUNS_PAGE;

pub async fn list_runs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Query(params): Query<ListRunsParams>,
) -> Result<Json<RunPage>, StatusCode> {
    let below = match (params.before_started, params.before_run) {
        (Some(started), Some(run_id)) => Some((started, run_id)),
        (None, None) => None,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let query = RunListQuery {
        limit: params.limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE),
        offset: params.offset.unwrap_or(0),
        project_id: params.project_id,
        started_after: params.started_after,
        started_before: params.started_before,
        phase: params.phase,
        entry_node: params.entry_node,
        status: params.status,
        instance: params.instance,
        tag: params.tag,
        node: params.node.filter(|n| !n.trim().is_empty()),
        search: params.search.filter(|s| !s.trim().is_empty()),
        below,
    };
    let page = state
        .runs
        .list_runs(caller.0.as_str(), &query)
        .await
        .map_err(failed("list runs"))?;
    Ok(Json(page))
}

/// Return the most recent run for a project, or 404 if
/// the project has none. Used by `weft logs` (no-arg form) to
/// find the run to dump logs for.
pub async fn latest_for_project(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<weft_core::program::RunSummary>, StatusCode> {
    authorize_project(&state, &caller.0, id)
        .await
        .map_err(|(s, _)| s)?;
    // Ask SQL for exactly the newest run of this project (project-filtered,
    // limit 1), rather than scanning a fixed window and filtering in memory.
    let query = RunListQuery {
        limit: 1,
        offset: 0,
        project_id: Some(id),
        started_after: None,
        started_before: None,
        phase: None,
        entry_node: None,
        status: None,
        instance: None,
        tag: None,
        node: None,
        search: None,
        below: None,
    };
    let page = state
        .runs
        .list_runs(caller.0.as_str(), &query)
        .await
        .map_err(failed("list runs"))?;
    page.runs.into_iter().next().map(Json).ok_or(StatusCode::NOT_FOUND)
}

#[derive(Debug, Deserialize)]
pub struct WakeParams {
    /// Which loop iteration's wait (`3`, or `3.0` inside an inner loop),
    /// for a node waiting in several.
    pub iteration: Option<String>,
}

/// The one of `waits` (the waits of `run_id` at the place `spelled`, a
/// place inside `loops` loops) a wake means: the only one, or the only one
/// in the loop iterations `wanted`, read off `iterations` (each wait's
/// iterations by token, from the run's record,
/// [`crate::node_state::wait_iterations`]; `None` when the record was not
/// read, because nothing asked which). Anything but exactly one is
/// refused: none names what there is, several list each by its
/// iteration, so the next wake can name one.
fn pick_wait(
    waits: &[ParkedWait],
    iterations: Option<&HashMap<String, Vec<u32>>>,
    wanted: Option<&[u32]>,
    loops: usize,
    spelled: &str,
    run_id: RunId,
) -> Result<ParkedWait, (StatusCode, String)> {
    if waits.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("'{spelled}' is not waiting on anything in {run_id}; `weft events {run_id} --node {spelled}` shows what it did"),
        ));
    }
    let of = |w: &ParkedWait| iterations.and_then(|iterations| iterations.get(&w.token));
    let chosen: Vec<&ParkedWait> = waits
        .iter()
        .filter(|w| wanted.is_none_or(|wanted| of(w).is_some_and(|at| at.starts_with(wanted))))
        .collect();
    let listed = |waits: &[&ParkedWait]| {
        waits
            .iter()
            .map(|w| of(w).map_or_else(|| format!("wait {} (not on the run's record yet)", w.token), |at| iteration_text(at)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match chosen.as_slice() {
        [one] => Ok((*one).clone()),
        [] => Err((
            StatusCode::NOT_FOUND,
            format!(
                "'{spelled}' is not waiting in iteration {} in {run_id}; it waits in {}",
                iteration_text(wanted.unwrap_or_default()),
                listed(&waits.iter().collect::<Vec<_>>())
            ),
        )),
        several if loops == 0 => Err((
            StatusCode::CONFLICT,
            format!("'{spelled}' holds {} waits in {run_id}, outside every loop, and nothing tells them apart", several.len()),
        )),
        several => Err((
            StatusCode::CONFLICT,
            format!(
                "'{spelled}' waits {} times in {run_id}, in iterations {}; say which with `--iteration`",
                several.len(),
                listed(several)
            ),
        )),
    }
}

/// Loop iterations as `--iteration` takes them (`3.0`).
fn iteration_text(iteration: &[u32]) -> String {
    iteration.iter().map(u32::to_string).collect::<Vec<_>>().join(".")
}

/// `POST /runs/{run_id}/wake/{node}`: resolve a wait now, instead
/// of waiting for whatever it waits for.
///
/// This side decides whether the wake may happen: the caller's tenancy,
/// and that the node really has a wait parked on this run. The node is
/// read against the program the run was started on, so a node moved since
/// is named the way it was in that run. A node inside a loop can wait in
/// several iterations at once, and then `iteration` (`3`, or `3.0` inside
/// an inner loop) says which; without it the wake is refused, listing
/// them. What the wait then wakes WITH is the signal kind's own shape, so
/// the listener holding it is asked, and it answers with nothing for every
/// kind that has no truthful stand-in (a form is waiting for an answer;
/// there is no inventing one). Those are refused naming the kind, and the
/// fire goes through the same lifecycle gate every other fire passes.
///
/// Nothing here knows which kinds can be woken. That is why: a tier
/// that minted one kind's payload would have to be edited for the next.
pub async fn wake(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((run_id_str, node)): Path<(String, String)>,
    Query(params): Query<WakeParams>,
) -> Result<StatusCode, (StatusCode, String)> {
    let run_id: RunId = run_id_str.parse().map_err(|_| (StatusCode::BAD_REQUEST, "bad run".to_string()))?;
    let owner = authorize_run(&*state.runs, &caller.0, run_id).await?;
    let internal = |what: &'static str| move |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let program = crate::projection::registered_program(&state, run_id, &owner).await.map_err(internal("read the run's program"))?
        .or_why(run_id);
    // Bounded, like every other caller string this surface echoes: the
    // node name is a raw path segment, so an enormous one would come
    // straight back in the body.
    let node = weft_core::truncate_user_string(&node, 256);
    let (_, weft_core::project::places::Named::Node(place)) = named_in_run(program.as_deref(), run_id, &node)? else {
        return Err((StatusCode::BAD_REQUEST, format!("'{node}' is a group, and only a node waits; name the node inside it that waits")));
    };
    let program = program.map_err(|why| (StatusCode::CONFLICT, why))?;
    // A wait is registered under the place spelled the way the program
    // writes it, which is what the name was just read into.
    let spelled = weft_core::project::address_of(&*program, &place.id, &place.path);
    let iteration = params.iteration.as_deref().map(weft_core::live_event::spelled::parse_iteration).transpose()
        .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let loops = weft_core::live_event::spelled::check_iteration(
        &*program,
        &weft_core::project::places::Named::Node(place.clone()),
        iteration.as_deref().unwrap_or_default(),
    )
    .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let waits: Vec<ParkedWait> = parked_waits(&state, run_id).await.map_err(internal("signals"))?
        .into_iter().filter(|w| w.node == spelled).collect();
    // Which iteration each wait sits in is read off the run's record, and
    // only when something asks which: one wait, with no iteration named,
    // is the one.
    let iterations = if waits.len() > 1 || iteration.is_some() {
        let mut folds = crate::projection::reconstruct_run(&state, run_id).await.map_err(internal("the run's record"))?;
        let fold = folds.remove(&run_id).ok_or_else(|| internal("the run's record")(anyhow::anyhow!("its own fold is missing")))?;
        Some(crate::node_state::wait_iterations(&fold.current_snapshot(), &place))
    } else {
        None
    };
    let wait = pick_wait(&waits, iterations.as_ref(), iteration.as_deref(), loops, &spelled, run_id)?;
    // WHETHER a wake may happen is this side's question, and it has been
    // answered above: the project is live, the node really is waiting,
    // the caller may touch it. WHAT it wakes with is the signal kind's,
    // so the listener is asked. A kind that cannot be woken by hand
    // answers with nothing, and the refusal below names the kind without
    // this tier ever knowing one.
    let payload = state
        .listener
        .wake_by_hand(&wait.token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("ask what it wakes with: {e:#}")))?;
    let Some(payload) = payload else {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "'{}' is waiting on a {} signal, which expects a value; answer it (its form, in the browser extension or through its signal URL) instead of waking it",
                wait.node,
                wait.kind
            ),
        ));
    };
    crate::api::signal::fire_registered_signal(&state, &wait.token, payload).await
}

pub async fn clean(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    axum::Json(body): axum::Json<weft_core::program::CleanRequest>,
) -> Result<axum::Json<weft_core::program::CleanOutcome>, (StatusCode, String)> {
    if let Some(project) = body.project {
        authorize_project(&state, &caller.0, project).await?;
    }
    clean_runs(&state, &caller.0, body.project, &body.filter, body.running, None)
        .await
        .map(axum::Json)
}

/// Delete every run `filter` reaches, in `project` (or every project of
/// `tenant`). A run still going is never deleted from under itself:
/// `cancel` stops it (its rows go with the next clean), `leave` leaves it
/// running. `asked_by` is never touched: a program cleaning runs is not
/// among them. Each deleted project's bare versions are swept as its runs
/// go, the way `weft clean <run_id>` sweeps them.
pub(crate) async fn clean_runs(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project: Option<uuid::Uuid>,
    filter: &weft_core::program::RunFilter,
    running: weft_core::program::StillGoing,
    asked_by: Option<RunId>,
) -> Result<weft_core::program::CleanOutcome, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let cancel_going = running == weft_core::program::StillGoing::Cancel;
    let query = run_query(project, filter, MAX_PAGE, crate::lease::now_unix() as u64);
    let mut outcome = weft_core::program::CleanOutcome::default();
    // The pages walk back in time from a bound fixed now (a run that
    // starts while this cleans is never reached), each page starting
    // strictly after the last run the one before it listed. Whatever the
    // walk deletes, cancels or leaves behind itself, the next page is
    // exactly the runs it has not reached yet.
    let mut below = None;
    let mut swept_projects: std::collections::BTreeSet<uuid::Uuid> = Default::default();
    loop {
        let page = state
            .runs
            .list_runs(
                tenant.as_str(),
                &RunListQuery { below, ..query.clone() },
            )
            .await
            .map_err(|e| internal("list runs", e))?;
        let Some(last) = page.runs.last().map(|run| (run.started_at, run.run_id)) else {
            break;
        };
        below = Some(last);
        for run in page.runs {
            if Some(run.run_id) == asked_by {
                continue;
            }
            if run.status == weft_core::program::RunStatus::Running {
                if cancel_going {
                    cancel_run_id(state, run.run_id, &weft_core::run::CancelCause::User)
                        .await
                        .map_err(|e| internal("cancel", e))?;
                    outcome.cancelled += 1;
                } else {
                    outcome.left_running += 1;
                }
                continue;
            }
            if filter.status.is_some_and(|status| !status.reaches(run.status)) {
                continue;
            }
            let project_id = clean_run(state, tenant, run.run_id)
                .await
                .map_err(|e| (e.status(), format!("delete run {}: {}", run.run_id, e.message())))?;
            swept_projects.insert(project_id);
            outcome.deleted += 1;
        }
    }
    for project_id in swept_projects {
        if let Err((status, message)) = crate::api::versions::sweep_bare_versions(state, project_id).await {
            tracing::warn!(
                target: "weft_dispatcher::versions",
                %project_id, %status, %message,
                "the runs are deleted; the versions they may have left bare could not be swept"
            );
        }
    }
    Ok(outcome)
}

/// THE reading of a [`weft_core::program::RunFilter`] as a run store query,
/// shared by every door that takes one (clean, list, count), so they
/// reach the same runs. Its upper start bound is fixed at `now`: a run
/// that starts after the question is never part of the answer, and
/// `older_than_secs` keeps runs started at least that long ago.
pub(crate) fn run_query(
    project: Option<uuid::Uuid>,
    filter: &weft_core::program::RunFilter,
    limit: u32,
    now: u64,
) -> RunListQuery {
    RunListQuery {
        limit,
        offset: 0,
        project_id: project,
        started_after: None,
        started_before: Some(now.saturating_sub(filter.older_than_secs.unwrap_or(0)) + 1),
        phase: None,
        entry_node: filter.node.clone(),
        status: filter.status,
        instance: filter.instance.clone(),
        tag: filter.tag.clone(),
        node: None,
        search: None,
        below: None,
    }
}

/// The newest `limit` runs `filter` reaches in `project`, with how many it
/// reaches in all; a run parked on a wait reads `waiting_for_input`, as
/// in every other listing.
pub(crate) async fn list_project_runs(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project: uuid::Uuid,
    filter: &weft_core::program::RunFilter,
    limit: u32,
) -> Result<RunPage, (StatusCode, String)> {
    if !(1..=MAX_PAGE).contains(&limit) {
        return Err((StatusCode::BAD_REQUEST, format!("a runs listing takes 1 to {MAX_PAGE} runs, not {limit}")));
    }
    let query = run_query(Some(project), filter, limit, crate::lease::now_unix() as u64);
    let page = state
        .runs
        .list_runs(tenant.as_str(), &query)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list runs: {e:#}")))?;
    Ok(page)
}

/// How many runs `filter` reaches in `project`.
pub(crate) async fn count_runs(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project: uuid::Uuid,
    filter: &weft_core::program::RunFilter,
) -> Result<u64, (StatusCode, String)> {
    let query = run_query(Some(project), filter, 0, crate::lease::now_unix() as u64);
    let page = state
        .runs
        .list_runs(tenant.as_str(), &query)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("count runs: {e:#}")))?;
    Ok(page.total)
}

/// Remove one run, and sweep the version its removal left bare.
///
/// The sweep is HERE and not in whoever asked. Deleting the last run of
/// a version is what makes that version bare, and a bare version shows
/// in `weft tree` with nothing under it and a status of `unknown`, for
/// ever, with no verb that reaches it. The CLI used to do the sweep
/// itself, which left the editor's own delete not doing it at all, and
/// the CLI could only sweep the project it was standing in, which for
/// `weft clean <run_id>` (a run can be cleaned from anywhere) was
/// frequently the wrong one.
///
/// Answers the project and what was swept, so a caller can say so.
pub async fn delete_run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id_str): Path<String>,
) -> Result<axum::Json<DeletedRun>, crate::api::storage::ApiError> {
    let run_id: RunId = run_id_str.parse().map_err(|_| (StatusCode::BAD_REQUEST, format!("'{run_id_str}' is not a run id")))?;
    let project_id = clean_run(&state, &caller.0, run_id).await?;
    // The run IS deleted, which is what was asked for, so a sweep that
    // cannot run is said out loud and does not fail the delete: the
    // reaper and the next `weft clean` both reach the same rows.
    let swept = crate::api::versions::sweep_bare_versions(&state, project_id).await.unwrap_or_else(|(status, message)| {
        tracing::warn!(
            target: "weft_dispatcher::versions",
            %run_id, project_id = %project_id, %status, %message,
            "the run is deleted; the version it may have left bare could not be swept"
        );
        Vec::new()
    });
    Ok(axum::Json(DeletedRun { project: project_id, swept }))
}

/// THE removal of one run (`weft clean <run_id>`, and each run a
/// prune drops): its storage folder, then its journal, its tags, its
/// resume tokens (on the process that served them too), and its row in
/// the version tree, together; then the word to every client.
pub(crate) async fn clean_run(
    state: &DispatcherState,
    caller: &crate::tenant::TenantId,
    run_id: RunId,
) -> Result<uuid::Uuid, crate::api::storage::ApiError> {
    // The gate already read the owning row; keep it rather than asking
    // again. Its tenant is the one the storage prefix was WRITTEN
    // under, so the wipe below addresses the same bytes the run
    // created even for a project that has since been removed (asking
    // the project store for the tenant would fail exactly there).
    let owner = authorize_run(&*state.runs, caller, run_id).await?;
    // Wipe the run's storage folder (kept survivors included:
    // `weft clean <run_id>` IS the explicit removal verb for them)
    // BEFORE the journal rows go, while the run's row still exists.
    // A spent run's storage address dies with its journal history;
    // every failure below aborts so a retry can still wipe, never
    // orphaning the prefix. The store's own answer goes back as it is:
    // what it left and why, and whether waiting is enough.
    crate::storage::wipe_prefix(state, &format!("{}/run/{run_id}/", owner.tenant))
        .await
        .map_err(|e| {
            tracing::error!(
                target: "weft_dispatcher::storage",
                %run_id, error = %e,
                "could not wipe run storage; aborting clean so a retry can"
            );
            crate::api::storage::storage_err(e)
        })?;
    // Head stops pointing at the run first: a failure after it leaves
    // the run whole and reachable, and the same command retries.
    state.versions.forget_head_run(run_id).await.map_err(|e| {
        tracing::error!(
            target: "weft_dispatcher::versions",
            %run_id, error = %e,
            "could not move head off this run; nothing was deleted, retry"
        );
        (StatusCode::INTERNAL_SERVER_ERROR, "could not move head off this run; nothing was deleted, retry".to_string())
    })?;
    let removed = state
        .runs
        .delete_run(run_id)
        .await
        .map_err(|e| (failed("delete the run")(e), "could not delete the run's journal; retry".to_string()))?;
    // The questions the run was parked on went with its rows, and the
    // listener holding each one still serves it until told: a
    // client with a signal token could list and answer a form
    // belonging to a run that no longer exists. Same call as a cancel.
    state.listener.unregister_many(&removed).await;
    // Every window learns the run is gone the way it learns a cancel.
    // NOTIFY, not the journal: the journal is what was just erased.
    state
        .events
        .publish(DispatcherEvent::RunDeleted { run_id, project_id: owner.project_id })
        .await;
    Ok(owner.project_id)
}

#[cfg(test)]
mod waits_tests {
    use super::*;

    fn signal(token: &str, run_id: Option<RunId>, node: &str, is_resume: bool, kind: &str) -> crate::store::SignalRegistration {
        crate::store::SignalRegistration {
            instance: None,
            activation_trigger: None,
            source_version: None,
            setup_run_id: None,
            program: None,
            token: token.into(),
            tenant_id: "t".into(),
            project_id: uuid::Uuid::from_u128(0x100),
            run_id,
            node_id: node.into(),
            is_resume,
            spec_json: serde_json::json!({ "kind": kind }).to_string(),
            access_id: None,
            consumer_kind: None,
            tags: vec![],
            port_snapshot: None,
            consumer_payload: None,
            surface_kind: "public_entry".into(),
            mount_path: None,
            mount_methods: Vec::new(),
            auth_kind: "none".into(),
            auth_config: None,
            kind_state: serde_json::Value::Object(Default::default()),
            kind_state_seq: 0,
            holds: false,
            generation: 0,
        }
    }

    /// A run's waits are its own resume signals, in order; entry
    /// signals and another run's waits are not.
    #[test]
    fn a_runs_waits_are_its_resume_signals() {
        let (mine, other) = (RunId::new_v4(), RunId::new_v4());
        let signals = vec![
            signal("e", None, "tick", false, "timer"),
            signal("a", Some(mine), "hold", true, "timer"),
            signal("b", Some(other), "review", true, "form"),
            signal("c", Some(mine), "review", true, "form"),
        ];
        let waits = waits_of(&signals, mine).unwrap();
        assert_eq!(
            waits,
            vec![
                ParkedWait { node: "hold".into(), token: "a".into(), kind: "timer".into() },
                ParkedWait { node: "review".into(), token: "c".into(), kind: "form".into() },
            ]
        );
    }

    /// A wait inside an included file was registered under its place,
    /// spelled (`sweep.key`), and goes out exactly as the row holds it:
    /// the same file under another site is another wait with another
    /// spelling, and nothing here has to tell them apart.
    #[test]
    fn a_wait_inside_an_included_file_goes_out_as_its_place() {
        let mine = RunId::new_v4();
        let signals = vec![
            signal("a", Some(mine), "sweep.key", true, "timer"),
            signal("b", Some(mine), "again.key", true, "timer"),
        ];
        let waits = waits_of(&signals, mine).unwrap();
        let out: Vec<&str> = waits.iter().map(|w| w.node.as_str()).collect();
        assert_eq!(out, vec!["sweep.key", "again.key"]);
        let wire = serde_json::to_value(&waits[0]).unwrap();
        assert_eq!(wire["node"], "sweep.key");
    }

    /// A wake picks one of several waits at one node by the iteration the
    /// run's record holds each in: the only one without asking, the one
    /// named (or the only one inside the outer iteration named), and
    /// several unnamed refused listing their iterations.
    #[test]
    fn a_wake_picks_its_wait_by_the_iteration_on_the_record() {
        let mine = RunId::new_v4();
        let waits = waits_of(&[signal("t0", Some(mine), "lp.ask", true, "timer"), signal("t1", Some(mine), "lp.ask", true, "timer")], mine).unwrap();
        let iterations: HashMap<String, Vec<u32>> = [("t0".to_string(), vec![0, 0]), ("t1".to_string(), vec![1, 2])].into();
        let pick = |waits: &[ParkedWait], wanted: Option<&[u32]>| pick_wait(waits, Some(&iterations), wanted, 2, "lp.ask", mine);
        assert_eq!(pick(&waits, Some(&[1, 2])).unwrap().token, "t1");
        assert_eq!(pick(&waits, Some(&[1])).unwrap().token, "t1");
        let (status, why) = pick(&waits, Some(&[3])).unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(why.contains("it waits in 0.0, 1.2"), "{why}");
        let (status, why) = pick(&waits, None).unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(why.contains("in iterations 0.0, 1.2; say which with `--iteration`"), "{why}");
        assert_eq!(pick_wait(&waits[..1], None, None, 2, "lp.ask", mine).unwrap().token, "t0", "one wait needs no record");
        assert_eq!(pick(&[], None).unwrap_err().0, StatusCode::NOT_FOUND);
        // A wait the record does not hold yet is listed by its token, and
        // no iteration picks it.
        let (_, why) = pick_wait(&waits, Some(&HashMap::new()), None, 2, "lp.ask", mine).unwrap_err();
        assert!(why.contains("wait t0 (not on the run's record yet)"), "{why}");
    }

    /// `--iteration` is checked against the place before any wait is
    /// read, by the same check `weft events --iteration` passes: refused
    /// at a node in no loop, and with more numbers than loops around it.
    #[test]
    fn a_wake_refuses_an_iteration_the_place_cannot_have() {
        use weft_core::live_event::spelled::check_iteration;
        use weft_core::project::places::named;
        let node = |id: &str, scope: &[&str]| serde_json::json!({
            "id": id, "nodeType": "T", "label": null, "config": {},
            "position": {"x": 0, "y": 0}, "inputs": [], "outputs": [],
            "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false
        });
        let p: ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [node("plain", &[]), node("lp.ask", &["lp"])],
            "edges": [],
            "groups": [{"id": "lp", "kind": "loop", "loopConfig": {}, "nodeIds": ["lp.ask"]}]
        }))
        .unwrap();
        let check = |spelled: &str, iteration: &[u32]| check_iteration(&p, &named(&p, spelled).unwrap(), iteration);
        assert!(check("plain", &[0]).unwrap_err().contains("is in no loop"));
        assert!(check("lp.ask", &[1, 0]).unwrap_err().contains("at most 1 number,"));
        assert_eq!(check("lp.ask", &[1]), Ok(1));
        assert_eq!(check("plain", &[]), Ok(0));
    }
}

#[cfg(test)]
mod cancel_tests {
    use super::*;
    use weft_journal::RunEvent;

    fn program() -> ProjectDefinition {
        serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": "wait", "nodeType": "Wait", "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": []
        }))
        .expect("program")
    }

    fn open_firing(run_id: RunId) -> Vec<RunEvent> {
        vec![
            RunEvent::NodeKicked { run_id, node_id: "wait".into(), frames: vec![], firing: true, payload: None, port_snapshot: None, at_unix: 0 },
            RunEvent::NodeStarted { run_id, node_id: "wait".into(), frames: vec![], at_unix: 1 },
        ]
    }

    /// With the program, every open firing gets its `NodeCancelled`
    /// before the terminal; without one (a removed or unreadable
    /// program, a node self-test) the terminal still lands, alone.
    #[test]
    fn cancel_writes_per_node_rows_with_the_program_and_the_terminal_without() {
        let run_id = RunId::new_v4();
        let program = program();
        let with = cancel_terminal_events(run_id, &open_firing(run_id), Some(&program), &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = with.iter().map(RunEvent::kind_str).collect();
        assert_eq!(kinds, vec!["node_cancelled", "run_cancelled"]);
        assert!(matches!(&with[0], RunEvent::NodeCancelled { node_id, .. } if node_id == "wait"));
        let without = cancel_terminal_events(run_id, &open_firing(run_id), None, &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = without.iter().map(RunEvent::kind_str).collect();
        assert_eq!(kinds, vec!["run_cancelled"]);
        assert!(matches!(&without[0], RunEvent::RunCancelled { cause: Some(CancelCause::User), .. }));
    }

    /// A charge the record leaves open is booked unknown, closing it,
    /// after the per-node cancels and before the terminal.
    #[test]
    fn cancel_books_each_open_charge_unknown_before_the_terminal() {
        let run_id = RunId::new_v4();
        let mut record = open_firing(run_id);
        record.push(RunEvent::ChargeOpened {
            run_id,
            node_id: "wait".into(),
            frames: vec![],
            service: "jobs".into(),
            charge: "job-1".into(),
            scratch: serde_json::json!({}),
            origin: weft_core::CredentialOwner::Platform,
            at_unix: 2,
        });
        let writes = cancel_terminal_events(run_id, &record, Some(&program()), &CancelCause::User, 9).unwrap();
        let kinds: Vec<&str> = writes.iter().map(RunEvent::kind_str).collect();
        assert_eq!(kinds, vec!["node_cancelled", "cost_reported", "run_cancelled"]);
        assert!(matches!(&writes[1], RunEvent::CostReported { charge: Some(charge), amount_usd: None, .. } if charge == "job-1"));
    }
}

#[cfg(test)]
mod wake_tier_boundary {
    /// This module's own source, read at compile time. The test module
    /// is cut off first, since the words to look for would otherwise be
    /// found in the looking.
    fn code() -> &'static str {
        include_str!("run.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("a split always yields a first piece")
    }

    /// Waking a parked wait must name no signal kind and mint no kind's
    /// payload. Which kinds can be woken, and with what, is the
    /// listener's answer (`/wake_by_hand`), because it is the tier that
    /// owns the kinds.
    ///
    /// A source check, because putting it back is SILENT: a second
    /// wakeable kind would simply be refused with "that expects a
    /// value", which reads like a deliberate rule rather than a tier
    /// that was never told.
    #[test]
    fn waking_names_no_kind_and_mints_no_payload() {
        for needle in ["Timer", "\"timer\"", "scheduledTime", "actualTime"] {
            assert!(
                !code().contains(needle),
                "the wake handler must not know a signal kind: found `{needle}`. \
                 What a wait wakes with is the kind's own shape and comes from \
                 the listener; minting one here means the next wakeable kind is \
                 quietly unwakeable."
            );
        }
    }
}

#[cfg(test)]
mod events_tests {
    use super::*;
    use crate::projection::ProgramLookup;

    const RUN: RunId = RunId::from_u128(9);

    /// The file `setup` included at the site `setup`, its node `insights`
    /// inside the group `stages`, or (`moved`) one group deeper, inside
    /// `stages.brains`: the same program before and after a person moved
    /// the node.
    fn program(moved: bool) -> Arc<ProjectDefinition> {
        let (insights, groups) = if moved {
            ("@src:setup.stages.brains.insights", serde_json::json!([
                {"id": "@src:setup", "kind": "body", "nodeIds": []},
                {"id": "@src:setup.stages", "kind": "group", "nodeIds": [], "parentGroupId": "@src:setup"},
                {"id": "@src:setup.stages.brains", "kind": "group", "nodeIds": ["@src:setup.stages.brains.insights"], "parentGroupId": "@src:setup.stages"},
                {"id": "setup", "kind": "call", "body": "@src:setup", "nodeIds": []}
            ]))
        } else {
            ("@src:setup.stages.insights", serde_json::json!([
                {"id": "@src:setup", "kind": "body", "nodeIds": []},
                {"id": "@src:setup.stages", "kind": "group", "nodeIds": ["@src:setup.stages.insights"], "parentGroupId": "@src:setup"},
                {"id": "setup", "kind": "call", "body": "@src:setup", "nodeIds": []}
            ]))
        };
        let scope: Vec<&str> = if moved { vec!["@src:setup", "@src:setup.stages", "@src:setup.stages.brains"] } else { vec!["@src:setup", "@src:setup.stages"] };
        Arc::new(serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": [{
                "id": insights, "nodeType": "T", "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 }, "inputs": [], "outputs": [],
                "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": [],
            "groups": groups,
        })).unwrap())
    }

    /// What the run recorded, under the program it ran (`moved` false): its
    /// start, and `insights` completing under the site `setup`.
    fn rows() -> Vec<crate::events::LiveEvent> {
        let project_id = uuid::Uuid::nil();
        [
            DispatcherEvent::RunCompleted { run_id: RUN, project_id, outputs: serde_json::Value::Null, at_unix: 1 },
            serde_json::from_value(serde_json::json!({
                "kind": "node_completed", "runId": RUN, "projectId": project_id, "node": "@src:setup.stages.insights",
                "frames": [{"site": "setup"}], "output": {"text": "hi"}, "atUnix": 1
            })).unwrap(),
        ].into_iter().map(crate::events::IdentifiedEvent::transient).collect()
    }

    fn by_node(node: &str) -> EventsParams {
        EventsParams { node: Some(node.into()), kind: None, iteration: None }
    }

    /// A node moved since a run is found on that run by the name it had
    /// there, and its rows read under that name: the name is read against
    /// the program the run ran, never the one the project holds now.
    #[test]
    fn a_node_moved_since_is_found_by_its_old_name_on_the_old_run() {
        let ran = ProgramLookup::Found(program(false));
        let answer = events_answer(RUN, &ran, &rows(), by_node("setup.stages.insights")).unwrap();
        assert_eq!(answer.total, 2);
        assert_eq!(answer.events.len(), 1, "{:?}", answer.events);
        assert_eq!(answer.events[0]["node"], "setup.stages.insights");
        // The name it has now is no name of that run's program, and the
        // refusal says whose program it was read against.
        let (status, why) = events_answer(RUN, &ran, &rows(), by_node("setup.stages.brains.insights")).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(why.contains(&format!("the program run {RUN} ran")) && why.contains("unknown node"), "{why}");
        // Read against the program as it is now, the old name is what
        // misses: the reading this route never does.
        let now = ProgramLookup::Found(program(true));
        assert!(events_answer(RUN, &now, &rows(), by_node("setup.stages.insights")).is_err());
    }

    /// A program that cannot be read leaves the rows by id, saying why, and
    /// refuses only a node filter, which cannot be read without it.
    #[test]
    fn an_unreadable_program_shows_ids_and_refuses_a_node_filter() {
        let gone = ProgramLookup::ProgramGone("the code this run ran is no longer recorded".into());
        let all = events_answer(RUN, &gone, &rows(), EventsParams { node: None, kind: None, iteration: None }).unwrap();
        assert_eq!(all.events.len(), 2);
        assert_eq!(all.events[1]["node"], "@src:setup.stages.insights");
        assert!(all.unspelled.as_deref().is_some_and(|why| why.contains("no longer recorded")));
        let (status, _) = events_answer(RUN, &gone, &rows(), by_node("setup.stages.insights")).unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
    }

    /// An iteration counts from the loops around the node named, so one
    /// asked for with no node, or at a node in no loop, is refused rather
    /// than matching every loop at once.
    #[test]
    fn an_iteration_needs_a_node_in_a_loop() {
        let ran = ProgramLookup::Found(program(false));
        let anywhere = EventsParams { node: None, kind: None, iteration: Some("3".into()) };
        let (status, why) = events_answer(RUN, &ran, &rows(), anywhere).unwrap_err();
        assert!(status == StatusCode::BAD_REQUEST && why.contains("needs --node"), "{why}");
        let outside = EventsParams { iteration: Some("3".into()), ..by_node("setup.stages.insights") };
        let (status, why) = events_answer(RUN, &ran, &rows(), outside).unwrap_err();
        assert!(status == StatusCode::BAD_REQUEST && why.contains("is in no loop"), "{why}");
    }
}
