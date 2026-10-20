//! Background reapers that sweep stale rows. Every copy of the
//! dispatcher runs all of these, and each sweep runs under its own
//! install-wide advisory lock (`lease::REAPER_DOMAIN`, keyed by the
//! reaper's name), so one copy sweeps at a time and a sibling that finds
//! the lock held skips that turn: N copies cost one sweep, not N. The
//! writes stay idempotent underneath (delete-by-key is a no-op the second
//! time, task reclaim is conditional), so a sweep that overlaps a request
//! doing the same work is harmless.
//!
//! Each reaper is a `DrainLoop` that sleeps until a write it cares about
//! is announced, and on a machine also looks on a slow safety tick. A
//! machine's fronts are the one sweep on a timer alone: nothing announces
//! a container that went down, and only a machine has them. What lapses
//! with nobody announcing it is `crate::lapses`'s.

use std::time::Duration;

use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// The channel a queued terminate sweep announces on (no payload: the
/// reaper reads the whole queue), from the `storage_sweep_notify_on_insert`
/// trigger in `storage::GROUP`.
pub const STORAGE_SWEEP_CHANNEL: &str = "weft_storage_sweep";

const NOTHING: &[WakeOn] = &[];
pub(crate) static ON_PARKED_FIRE: &[WakeOn] = &[WakeOn::any(weft_task_store::parked_fires::PARKED_FIRE_CHANNEL)];
pub(crate) static ON_STORAGE_SWEEP: &[WakeOn] = &[WakeOn::any(STORAGE_SWEEP_CHANNEL)];
/// A trigger's activation changing status, whether it takes work, or until
/// when: a trigger taken down with `doorForMinutes` has a window to end.
pub(crate) static ON_DOOR_WINDOW: &[WakeOn] = &[WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL)];
/// An infra copy coming, going or changing status, or an infra command
/// ending: a copy running again (or a take-down called off) brings back the
/// triggers that went down with it, and the events that waited on it. An
/// activation changing status or a held signal going: a
/// trigger refused because another claim held it comes back once that
/// claim ends.
pub(crate) static ON_INFRA_RETURNS: &[WakeOn] = &[
    WakeOn::any(crate::held::INFRA_STATUS_CHANNEL),
    WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL),
    // A take-down command ending (a cancel, a failure) with its copies
    // still up brings back what went down with them.
    WakeOn::any(weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL),
];

/// Safety tick of the reapers that are woken by a write: 60 seconds in
/// real time, at this install's pace (`weft_core::time_scale`).
fn woken_reaper_safety() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(60))
}

/// Every reaper, as the loops the dispatcher runs.
pub fn drain_loops(state: &DispatcherState) -> Vec<DrainLoop> {
    let mut loops = vec![
        // Queued events whose trigger takes them, when their clock comes
        // due: a newly parked event wakes it at once; otherwise it sleeps
        // until the earliest head is due, which a head stamped elsewhere
        // books (`crate::parked_drain::book`).
        woken(state, ON_PARKED_FIRE, crate::parked_drain::PARKED_FIRES, "hand queued trigger events to their runs", |s| async move {
            crate::parked_drain::drain_due(&s).await
        }),
        // Storage plane: the durable terminate sweep (un-kept run files of
        // a terminated run). The queue deletes a run's row only after
        // the broker confirms the sweep; a broker that keeps failing fails
        // the pass, and the next run ending queued for a sweep tries every
        // row again. The kept-file expiry sweep is the broker's own loop
        // (it owns the bucket + metadata).
        woken(state, ON_STORAGE_SWEEP, "storage_sweep", "clear the files of ended runs", crate::storage::process_sweep_queue),
        // A door's window ending: its triggers stop listening and taking
        // work. Sleeps until the next one ends.
        woken(state, ON_DOOR_WINDOW, DOOR_WINDOWS, DOOR_WINDOWS_LABEL, close_door_windows),
        // An infra copy running again: the triggers its stop took down
        // with it come back, and the events that waited on it go.
        woken(state, ON_INFRA_RETURNS, "infra_returns", "bring back triggers whose infra returned", crate::api::infra::bring_back_triggers_whose_infra_returned),
    ];
    // A machine's fronts are its own containers: one that went down is put
    // back, on a free port when another program took its own (and the
    // project's address follows). A cloud's front is the platform's to keep.
    if state.project_ports.is_some() {
        loops.push(timed(state, Duration::from_secs(30), "fronts", "put the projects' fronts back", |s| async move { crate::front::serve_all(&s, crate::front::Say::Changes).await }));
    }
    loops
}

/// The door-windows loop, by name.
pub(crate) const DOOR_WINDOWS: &str = "door_windows";

/// Close every door whose window has passed ([`close_door_windows_over`],
/// the listener letting go of its signals through
/// `crate::listener::let_go_of_stopped`); a project left with nothing
/// taking work lets go of its front. Then sleep until the next window
/// ends, or until an activation changes.
async fn close_door_windows(state: DispatcherState) -> anyhow::Result<DrainStep> {
    let let_go = |signals: Vec<crate::journal::SignalRegistration>| {
        let state = state.clone();
        async move { crate::listener::let_go_of_stopped(&state.pg_pool, &state.listener, &signals).await }
    };
    for project_id in close_door_windows_over(&state.pg_pool, let_go).await? {
        // Nothing of the project may take work any more: its front goes.
        crate::front::let_go_if_idle(&state, project_id).await?;
    }
    let now = crate::lease::now_unix();
    Ok(match crate::activation_store::next_door_window_end(&state.pg_pool).await? {
        None => DrainStep::Done,
        Some(deadline) => DrainStep::RetryIn(Duration::from_secs((deadline + 1 - now).max(1) as u64)),
    })
}

/// Close every door whose window has passed: `let_go` has the listener
/// let go of its signals and answers the ones it kept (with why), then its
/// activation stops taking work
/// (`crate::activation_store::close_door_windows`), whatever the listener
/// answered: a fire that still arrives is refused by the activation, and a
/// holder hears it stop taking work (`crate::holders::HELD_SIGNALS_CHANNEL`).
/// A signal the listener kept is marked on its own row
/// (`crate::listener::note_let_go`) and the listener is told again at each
/// pass until it lets go (or its trigger takes work again); while any is
/// marked, its project's `weft status` names it (`Callee::LetGo`). Answers the
/// projects whose windows ended.
pub async fn close_door_windows_over<F, Fut>(pool: &sqlx::PgPool, let_go: F) -> anyhow::Result<Vec<uuid::Uuid>>
where
    F: Fn(Vec<crate::journal::SignalRegistration>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Vec<(String, String)>>>,
{
    let again = crate::journal::postgres::signals_kept_after_let_go(pool).await?;
    if !again.is_empty() {
        let kept = let_go(again.clone()).await?;
        crate::listener::note_let_go(pool, &again, &kept).await?;
    }
    let mut by_project: std::collections::BTreeMap<uuid::Uuid, Vec<weft_core::activation::ActivationKey>> = Default::default();
    for (project_id, key) in crate::activation_store::door_windows_over(pool).await? {
        by_project.entry(project_id).or_default().push(key);
    }
    let mut ended_in = Vec::new();
    for (project_id, keys) in by_project {
        let signals = crate::journal::postgres::activation_signals(pool, project_id, &keys).await?;
        let kept = let_go(signals.clone()).await?;
        crate::listener::note_let_go(pool, &signals, &kept).await?;
        let ended = crate::activation_store::close_door_windows(pool, project_id, &keys).await?;
        if ended > 0 {
            tracing::info!(
                target: "weft_dispatcher::reaper",
                %project_id, triggers = ended,
                "a door's window ended: its triggers stopped listening and take no more work"
            );
            ended_in.push(project_id);
        }
    }
    let kept: Vec<(uuid::Uuid, String, String)> =
        sqlx::query_as("SELECT project_id, node_id, let_go_failed FROM signal WHERE let_go_failed IS NOT NULL ORDER BY project_id, node_id")
            .fetch_all(pool)
            .await?;
    let mut by_project: std::collections::BTreeMap<uuid::Uuid, Vec<String>> = Default::default();
    for (project, node, why) in kept {
        by_project.entry(project).or_default().push(format!("node {node}: {why}"));
    }
    let failing: Vec<uuid::Uuid> = by_project.keys().copied().collect();
    weft_task_store::unanswered::still_failing(pool, weft_task_store::unanswered::PerProject::LetGo, &failing).await?;
    for (project, named) in by_project {
        tracing::error!(
            target: "weft_dispatcher::reaper",
            %project, signals = ?named,
            "the listener kept serving signals whose door closed; their activations take no work, so what they pick up \
             is refused"
        );
        weft_task_store::unanswered::failed(pool, weft_task_store::unanswered::Callee::LetGo(project), None, &named.join("; ")).await?.log();
    }
    Ok(ended_in)
}

/// What the door-windows loop does, in a person's words.
const DOOR_WINDOWS_LABEL: &str = "close the doors whose window passed";

/// A sweep that runs on its interval alone (a machine's only). `interval`
/// is given in real time and runs at this install's pace
/// (`weft_core::time_scale`).
fn timed<F, Fut>(state: &DispatcherState, interval: Duration, name: &'static str, label: &'static str, sweep: F) -> DrainLoop
where
    F: Fn(DispatcherState) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let state = state.clone();
    DrainLoop::new(name, label, NOTHING, weft_core::time_scale::scaled(interval), move || {
        let state = state.clone();
        let sweep = sweep.clone();
        async move {
            sweep_alone(&state, name, || sweep(state.clone())).await?;
            Ok(DrainStep::Done)
        }
    })
}

/// A sweep that runs when one of `wake_on` is announced, and on a slow
/// safety tick. The body's step says whether to look again early.
pub(crate) fn woken<F, Fut>(state: &DispatcherState, wake_on: &'static [WakeOn], name: &'static str, label: &'static str, sweep: F) -> DrainLoop
where
    F: Fn(DispatcherState) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = anyhow::Result<DrainStep>> + Send + 'static,
{
    let state = state.clone();
    DrainLoop::new(name, label, wake_on, woken_reaper_safety(), move || {
        let state = state.clone();
        let sweep = sweep.clone();
        async move {
            // A sibling holding the lock is sweeping right now, but it may
            // have read before the write this wake is for, so look again
            // shortly (`LOCK_HELD_RETRY`).
            Ok(sweep_alone(&state, name, || sweep(state.clone()))
                .await?
                .unwrap_or(DrainStep::RetryIn(weft_task_store::drain::LOCK_HELD_RETRY)))
        }
    })
}

/// Run one sweep while holding the reaper's install-wide lock, or skip
/// it (`None`) while a sibling replica holds it. The lock is held on the
/// lock pool: every sweep starts at once when the process boots, and with
/// the locks on the work pool, more sweeps than it has connections each
/// held one and waited for another, until every request of the process
/// timed out.
async fn sweep_alone<T, F, Fut>(state: &DispatcherState, name: &str, sweep: F) -> anyhow::Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    crate::lease::with_advisory_lock(
        &state.lock_pool,
        crate::lease::advisory_key(crate::lease::REAPER_DOMAIN, name),
        sweep,
    )
    .await
}
