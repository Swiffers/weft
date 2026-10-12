//! In-memory map of the signals whose connection this process holds.
//!
//! Only a signal that holds a connection (`BetweenFires::Holds`), in a
//! process that holds them (`crate::hold`), has an entry: it binds a token
//! to its resolved spec plus the task running its loop, and stopping it
//! tears the task down. Every other signal is read from its durable row
//! per call (see [`held`]).

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::task::JoinHandle;
use serde_json::Value;
use weft_core::primitive::{SignalRouting, SignalSpec};
use weft_core::signal::listener_protocol::StartMode;

/// Which transport currently serves a signal's held connection,
/// recorded on its [`ServingState`] when the serving task decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    Socket,
    Webhook,
    Unservable(String),
}

/// The LIVE serving state of one signal in this process: what its background
/// task is doing right now and the transport it decided on. Written
/// by the kind's serving task, read by the node's display. An empty
/// status with no transport means the kind reports nothing live.
#[derive(Debug, Default)]
pub struct ServingState {
    pub status: String,
    pub transport: Option<Transport>,
}

#[derive(Clone)]
pub struct RegisteredSignal {
    pub spec: SignalSpec,
    pub node_id: String,
    /// Tenant this signal belongs to. The listener holds signals from
    /// many tenants; the fire path reads this to stamp the correct
    /// tenant on the enqueued `FireSignal` task.
    pub tenant_id: String,
    /// True iff this is a mid-execution resume (HumanQuery, etc).
    /// Used by `process()` to decide which `ProcessTarget` to return
    /// for dual-use kinds like Form.
    pub is_resume: bool,
    /// Execution of the suspended execution to resume. Set iff
    /// `is_resume`. Echoed back into `ProcessTarget::Resume`.
    pub execution_id: Option<weft_core::ExecutionId>,
    /// Background task for kinds that hold a connection
    /// (`BetweenFires::Holds`). Dropping the handle via `.abort()`
    /// cancels the loop. `None` for every other kind.
    pub task: Option<Arc<TaskGuard>>,
    /// The signal's durable kind state as read for this call, for a kind
    /// read from its row per call (a poll's cursor and failure streak,
    /// which its display reads). `None` for a kind that holds a
    /// connection: its task owns its state while it runs, so a copy taken
    /// at registration would only go stale here.
    pub kind_state: Option<Value>,
    /// Routing+auth metadata computed by the kind impl at register
    /// time (or reconstructed from the durable row at rehydrate).
    /// The dispatcher copies this onto the signal row; the kind's
    /// `live` reads it to show what mount_path / auth_kind the signal
    /// is using. Always set: both register and rehydrate paths
    /// populate it, so downstream readers don't need to handle a
    /// None case.
    pub routing: SignalRouting,
    /// The live serving state (see [`ServingState`]), created at
    /// registration and shared with the kind's background task so
    /// status updates land where the kind's `live` reads them. Dies
    /// with the entry.
    pub serving: Arc<Mutex<ServingState>>,
    /// Why the signal was given up on for good (`signal.held_failed`),
    /// shown on its node until it is registered again.
    pub gave_up: Option<String>,
}

/// Wrapper so dropping a `RegisteredSignal` aborts its loop
/// exactly once, even when cloned.
pub struct TaskGuard(JoinHandle<()>);

impl TaskGuard {
    pub fn new(handle: JoinHandle<()>) -> Self {
        Self(handle)
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
pub struct Registry {
    inner: DashMap<String, RegisteredSignal>,
    /// One guard per token being brought up, so two callers bringing the
    /// same row up (a look racing a registration's take) run one after the
    /// other and the second finds it up. Local to the process like the
    /// entries: a held connection runs only in the one process that claims
    /// it. An entry lives only while someone holds its guard.
    bringing: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Held rows that could not come up, by token. Each has exactly one
    /// retry loop running ([`hold`]), the entry's `owner`; the node's
    /// display says the signal is down and why.
    down: DashMap<String, Down>,
    /// Source of [`Down::owner`] values.
    down_loops: std::sync::atomic::AtomicU64,
    /// Held signals given up on here whose give-up could not be written,
    /// by token. Each stays this process's claim, so its row is never
    /// taken back and brought up only to give up again, and the holder's
    /// next look writes the give-up again (`crate::hold`).
    unrecorded: DashMap<String, GaveUp>,
    /// Retry loops alive now, so a test can see one loop per token.
    retry_loops_running: Arc<std::sync::atomic::AtomicUsize>,
}

/// Counts one retry loop as running for as long as it lives.
struct RunningLoop(Arc<std::sync::atomic::AtomicUsize>);

impl RunningLoop {
    fn start(count: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for RunningLoop {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A down row: the latest reason, and which retry loop owns it. A loop
/// runs only while the entry is its own, so an entry cleared and marked
/// again while a loop sleeps gets a new loop and the old one ends: one
/// loop per token, never two.
struct Down {
    reason: String,
    owner: u64,
    /// Whether its row holds a connection, so that a holder counts it
    /// among its claims (`crate::hold`).
    holds: bool,
}

/// A give-up still to be written: the registration it gives up on, and
/// why.
#[derive(Debug, Clone)]
pub struct GaveUp {
    pub generation: i64,
    pub reason: String,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, token: String, signal: RegisteredSignal) {
        self.inner.insert(token, signal);
    }

    pub fn get(&self, token: &str) -> Option<RegisteredSignal> {
        self.inner.get(token).map(|r| r.clone())
    }

    pub fn remove(&self, token: &str) -> Option<RegisteredSignal> {
        self.inner.remove(token).map(|(_, v)| v)
    }

    pub fn list(&self) -> Vec<(String, RegisteredSignal)> {
        self.inner
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// Why a held row is down, while it is.
    pub fn down_reason(&self, token: &str) -> Option<String> {
        self.down.get(token).map(|r| r.reason.clone())
    }

    /// Every row down now that holds a connection: a holder's claims.
    pub fn down_connections(&self) -> Vec<String> {
        self.down.iter().filter(|e| e.value().holds).map(|e| e.key().clone()).collect()
    }

    /// Stop counting `token` as down: it came up, or its row is gone.
    /// Its retry loop sees this at its next turn and ends. When it was
    /// down, whether its row holds a connection.
    pub fn clear_down(&self, token: &str) -> Option<bool> {
        self.down.remove(token).map(|(_, down)| down.holds)
    }

    /// Every held signal given up on here whose give-up is still to be
    /// written.
    pub fn unrecorded(&self) -> Vec<(String, GaveUp)> {
        self.unrecorded.iter().map(|e| (e.key().clone(), e.value().clone())).collect()
    }

    /// The give-up still to be written for `token`, while there is one.
    pub fn unrecorded_of(&self, token: &str) -> Option<GaveUp> {
        self.unrecorded.get(token).map(|g| g.clone())
    }

    pub(crate) fn keep_unrecorded(&self, token: &str, gave_up: GaveUp) {
        self.unrecorded.insert(token.to_string(), gave_up);
    }

    /// `token`'s give-up was written, or the signal is no longer this
    /// process's to give up on (it ended, or changed hands).
    pub(crate) fn drop_unrecorded(&self, token: &str) {
        self.unrecorded.remove(token);
    }

    /// Whether the retry loop `owner` still owns `token`'s down entry.
    fn owns_down(&self, token: &str, owner: u64) -> bool {
        self.down.get(token).is_some_and(|d| d.owner == owner)
    }

    /// How many retry loops are running now, across every token.
    pub fn retry_loops_running(&self) -> usize {
        self.retry_loops_running.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn bring_up_guard(&self, token: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.bringing.entry(token.to_string()).or_default().clone()
    }

    /// Drop the token's guard once nobody else holds a copy. Counted
    /// under the map's lock, so a caller that just took a copy keeps it.
    pub(crate) fn release_bring_up_guard(&self, token: &str) {
        self.bringing.remove_if(token, |_, guard| Arc::strong_count(guard) == 1);
    }
}

/// A signal as it stands now: this process's registry entry for one it
/// holds, otherwise read fresh from its durable row. `None` when no held
/// row has that token (the signal is gone, or its activation parked).
///
/// Every endpoint that names a signal goes through here, so any copy of
/// the listener answers for any signal. The row is the truth the
/// dispatcher routes by, so the routing comes back from its columns,
/// never recomputed from the kind. A signal some holder holds elsewhere
/// is read from its row too, with what its holder last said it is doing;
/// nothing here ever brings one up (its holder's claim does, see
/// `crate::hold`). Nothing read from a row is cached: an unregister
/// reaches one copy of the listener, and a sibling that had cached the
/// signal would keep answering `/process` for it.
pub async fn held(state: &crate::ListenerState, token: &str) -> anyhow::Result<Option<RegisteredSignal>> {
    held_in(&state.registry, &state.signals, token).await
}

/// [`held`], from the registry and the signal rows themselves.
pub async fn held_in(registry: &Registry, signals: &weft_broker_client::BrokerSignalClient, token: &str) -> anyhow::Result<Option<RegisteredSignal>> {
    if let Some(sig) = registry.get(token) {
        return Ok(Some(sig));
    }
    let Some(row) = signals.get_held(token).await? else {
        return Ok(None);
    };
    let spec: SignalSpec = serde_json::from_str(&row.spec_json)
        .map_err(|e| anyhow::anyhow!("malformed spec_json for signal {}: {e}", row.token))?;
    let routing = row.to_routing().map_err(|e| anyhow::anyhow!("to_routing for signal {}: {e}", row.token))?;
    let serving = match row.holds {
        true => crate::hold::from_wire(row.serving.as_ref()),
        false => ServingState::default(),
    };
    Ok(Some(RegisteredSignal {
        spec,
        node_id: row.node_id.clone(),
        tenant_id: row.tenant_id.clone(),
        is_resume: row.is_resume,
        execution_id: row.execution_id,
        task: None,
        // A held connection's task owns its state while it runs.
        kind_state: (!row.holds).then(|| row.kind_state.clone()),
        routing,
        serving: Arc::new(Mutex::new(serving)),
        gave_up: row.held_failed.clone(),
    }))
}

/// Whether `row`'s kind decides that it holds a connection, `None` when
/// its spec, kind or state cannot be read (bringing it up says why).
fn decided_holds(row: &weft_broker_client::protocol::SignalRowWire) -> Option<bool> {
    let spec: SignalSpec = serde_json::from_str(&row.spec_json).ok()?;
    let handler = crate::kinds::lookup(&spec.kind)?;
    Some(handler.between_fires(&spec, &row.kind_state).ok()? == crate::kinds::BetweenFires::Holds)
}

/// How long the retry of a down row waits before each attempt: about a
/// minute in all, then it is given up on.
const DOWN_RETRIES: [std::time::Duration; 3] =
    [std::time::Duration::from_secs(5), std::time::Duration::from_secs(15), std::time::Duration::from_secs(40)];
/// How often a row just brought up is read again before the answer
/// counts as unknown, and the wait before the first re-read (doubled
/// after each).
const REREAD_ATTEMPTS: u32 = 5;
const REREAD_FIRST: std::time::Duration = std::time::Duration::from_millis(200);

/// Bring one held row up: the one path every bring-up of a row takes (the
/// dispatcher's `/start`, a rehydrate at boot or activation, a holder
/// taking a held one, the retry of a down row).
///
/// Single-flight per token: a second caller waits for the first and, on
/// a restore, finds the connection up and leaves it (a restore that
/// replaced it would stop the first caller's task mid-start).
///
/// A row is a snapshot: it can be deleted after it was read, and the
/// dispatcher's unregister can pass before the task below is in the
/// registry, which would leave that task running forever for a row
/// nobody holds. So a held connection's row is read again once its task
/// is registered, and a row gone by then is let go. A re-read that keeps
/// failing leaves the task running: the row was held when it was read,
/// and an unregister that comes later still finds the registry entry.
///
/// A restore or put-back that fails marks the row down: it is logged,
/// shown on the node's display, and retried from the row as it reads a few
/// times over about a minute, then given up on (written once on the row,
/// which its node shows) until it is registered again. A
/// `StartMode::New` failure is the dispatcher's to handle (it answers the
/// registration with it), so it is only returned.
pub async fn hold(
    state: &crate::ListenerState,
    row: weft_broker_client::protocol::SignalRowWire,
    mode: StartMode,
) -> anyhow::Result<()> {
    let token = row.token.clone();
    let guard = state.registry.bring_up_guard(&token);
    let result = {
        let _held = guard.lock().await;
        hold_once(state, row.clone(), mode).await
    };
    drop(guard);
    state.registry.release_bring_up_guard(&token);
    if let Err(e) = &result {
        if mode.retried() {
            mark_down(state, row, format!("{e:#}"));
        }
    }
    result
}

async fn hold_once(
    state: &crate::ListenerState,
    row: weft_broker_client::protocol::SignalRowWire,
    mode: StartMode,
) -> anyhow::Result<()> {
    let token = row.token.clone();
    if mode == StartMode::Restore && state.registry.get(&token).is_some() {
        return Ok(());
    }
    if let Err(e) = crate::kinds::bring_up(state, row, mode).await {
        // A put-back that cannot come up must not leave the replacement
        // it was undoing running: the row no longer says that spec. The
        // entry stops (its task and its outside teardown), so the retry
        // loop's restore finds nothing running and brings the put-back
        // row up.
        if mode == StartMode::PutBack {
            crate::kinds::stop_held(state, &token).await;
        }
        return Err(e);
    }
    state.registry.clear_down(&token);
    if state.registry.get(&token).is_none() {
        // Not a held connection: nothing of it stays here to let go.
        return Ok(());
    }
    let mut wait = REREAD_FIRST;
    for attempt in 1..=REREAD_ATTEMPTS {
        match state.signals.get_held(&token).await {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {
                crate::kinds::forget(state, &token);
                tracing::info!(target: "weft_listener", token = %token, "a signal's row went while it came up; its task is stopped");
                return Ok(());
            }
            Err(e) if attempt == REREAD_ATTEMPTS => {
                tracing::warn!(
                    target: "weft_listener", token = %token, error = %format!("{e:#}"),
                    "a held signal came up, but its row could not be read again; it keeps running"
                );
            }
            Err(_) => {
                tokio::time::sleep(wait).await;
                wait *= 2;
            }
        }
    }
    Ok(())
}

/// Bring `row` up where it runs (the dispatcher's `/start`, the retry of
/// a down row): a held connection, in a process that holds, through a take
/// of its claim, so it never runs here without one; in any other process
/// a holder takes it at its next look (the dispatcher sizes the holders
/// from the rows), and says on the node's display how it is. Every other
/// row comes up here ([`hold`]).
pub async fn start(
    state: &crate::ListenerState,
    row: weft_broker_client::protocol::SignalRowWire,
    mode: StartMode,
) -> anyhow::Result<()> {
    match (row.holds, state.config.holds_here) {
        (true, true) => crate::hold::take_now(state, std::slice::from_ref(&row.token), mode).await,
        (true, false) => Ok(()),
        (false, _) => hold(state, row, mode).await,
    }
}

/// Count `row`'s signal as down with `reason`, starting its retry loop
/// unless one already owns the entry (then only the reason is updated, and
/// that loop stays the owner).
fn mark_down(state: &crate::ListenerState, row: weft_broker_client::protocol::SignalRowWire, reason: String) {
    tracing::error!(target: "weft_listener", token = %row.token, reason = %reason, "a signal is down; retrying it");
    match state.registry.down.entry(row.token.clone()) {
        dashmap::Entry::Occupied(mut e) => {
            e.get_mut().reason = reason;
        }
        dashmap::Entry::Vacant(e) => {
            let owner = state.registry.down_loops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            e.insert(Down { reason, owner, holds: row.holds });
            let running = RunningLoop::start(&state.registry.retry_loops_running);
            tokio::spawn(retry_down(state.clone(), row, owner, running));
        }
    }
}

/// Bring a down row back ([`DOWN_RETRIES`]), each try from the row as it
/// reads then, until it is up or no longer this loop's to bring up: the
/// down entry is not this loop's (`owner`: something else brought it up,
/// or forgot it, or it was cleared and marked again, which started the
/// loop that owns it now), or the row is gone, registered again, or given
/// up on (another process may have unregistered it, which this one never
/// hears). One that never comes up is given up on: written once on its
/// row, which its node shows, until it is registered again.
async fn retry_down(state: crate::ListenerState, row: weft_broker_client::protocol::SignalRowWire, owner: u64, _running: RunningLoop) {
    let token = row.token.clone();
    for wait in DOWN_RETRIES {
        tokio::time::sleep(weft_core::time_scale::scaled(wait)).await;
        if !state.registry.owns_down(&token, owner) {
            return;
        }
        let row = match state.signals.get_held(&token).await {
            Ok(Some(now)) if now.generation == row.generation && now.held_failed.is_none() => now,
            Ok(_) => {
                state.registry.down.remove_if(&token, |_, d| d.owner == owner);
                tracing::info!(target: "weft_listener", token = %token, "a down signal is gone, registered again or given up on; no longer retrying it");
                return;
            }
            Err(e) => {
                tracing::warn!(target: "weft_listener", token = %token, error = %format!("{e:#}"), "a down signal's row could not be read; trying again");
                continue;
            }
        };
        match start(&state, row.clone(), StartMode::Restore).await {
            // A held connection is up once this process took it.
            Ok(()) if !row.holds || state.registry.get(&token).is_some() => {
                tracing::info!(target: "weft_listener", token = %token, "a down signal is back up");
                return;
            }
            Ok(()) => {
                state.registry.down.remove_if(&token, |_, d| d.owner == owner);
                tracing::info!(target: "weft_listener", token = %token, "a down signal is another holder's now, or gone; no longer retrying it here");
                return;
            }
            // A failure to come up is also recorded by `hold`, which
            // shows it on the node.
            Err(e) => tracing::warn!(target: "weft_listener", token = %token, error = %format!("{e:#}"), "a down signal could not be brought back"),
        }
    }
    let Some(reason) = state.registry.down.get(&token).filter(|d| d.owner == owner).map(|d| d.reason.clone()) else { return };
    let why = format!("it could not come up: {reason}");
    // A held connection's give-up that is not written stays its holder's
    // claim, and its next look writes it (`crate::hold`); nothing else
    // writes another signal's, so it is tried again here a few times.
    let mut waits = DOWN_RETRIES.iter().filter(|_| !row.holds);
    loop {
        let Err(e) = crate::kinds::give_up(&state, &token, row.generation, &why).await else { return };
        let Some(wait) = waits.next() else {
            tracing::error!(target: "weft_listener", token = %token, error = %format!("{e:#}"), "a signal given up on could not say so on its row; it stays so until it is brought up again");
            return;
        };
        tracing::warn!(target: "weft_listener", token = %token, error = %format!("{e:#}"), "a signal given up on could not say so on its row yet; trying again");
        tokio::time::sleep(weft_core::time_scale::scaled(*wait)).await;
    }
}

/// Reconcile the signals of `project` (every project when `None`) with the
/// durable `signal` table. Idempotent. Every row is brought up ([`hold`]):
/// a signal that wakes gets its next wake set (setting the same wake twice
/// is one wake), and a signal the outside calls in to needs nothing. A
/// signal that holds a connection is a holder's: a process that holds
/// (a local install's one) takes them now, and a listener that scales to
/// zero leaves them to the holders, which take them at their next look.
///
/// Called at a local install's boot over every project and by the
/// dispatcher's activate flow (`POST /rehydrate`) over the activating
/// project, after the activation's rows are written.
///
/// Every row that can come up does: one that cannot (a malformed spec, a
/// kind that refuses) does not stop the rest, so one bad row never keeps
/// the others down (at boot, every project's). It is marked down, which
/// logs it, shows it on its node, and retries it a few times before giving
/// it up. A row given up on waits to be registered again. The answer
/// also names each row that stayed down: the
/// activate fails on its own project's, the boot carries on. Rows whose
/// token is in `skip` are never brought up: they are on their way out
/// (see `RehydrateRequest`). Failing to read the rows at all is an error.
pub async fn rehydrate(
    state: &crate::ListenerState,
    project: Option<uuid::Uuid>,
    skip: &[String],
) -> anyhow::Result<Vec<String>> {
    let mut failed = Vec::new();
    let mut held = Vec::new();
    for mut row in state.signals.list_held(project).await? {
        // A row given up on waits to be registered again.
        if skip.contains(&row.token) || row.held_failed.is_some() || state.registry.get(&row.token).is_some() {
            continue;
        }
        // The row says what its kind decided when it was registered; a
        // kind that decides differently now (another weft) is followed,
        // and the row told, so the holders and their count agree.
        if let Some(holds) = decided_holds(&row) {
            if holds != row.holds {
                if let Err(e) = state.signals.set_holds(&row.token, holds).await {
                    failed.push(format!("signal {}: record whether it holds a connection: {e:#}", row.token));
                    continue;
                }
                row.holds = holds;
            }
        }
        if row.holds {
            held.push(row.token);
            continue;
        }
        let token = row.token.clone();
        if let Err(e) = hold(state, row, StartMode::Restore).await {
            failed.push(format!("signal {token}: {e:#}"));
        }
    }
    if state.config.holds_here && !held.is_empty() {
        if let Err(e) = crate::hold::take_now(state, &held, StartMode::Restore).await {
            failed.push(format!("{e:#}"));
        }
    }
    Ok(failed)
}
