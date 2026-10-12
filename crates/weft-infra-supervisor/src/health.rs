//! Health loop. Asks the host how every unit of the owned projects is
//! doing, evaluates each project's HealthProtocols, and emits
//! `infra_event` rows via the broker when a node's status changes.
//!
//! Two concerns:
//!   1. Per-unit windowed health-state transitions (flaky after N
//!      seconds not ready, recovered after N seconds ready). Drives
//!      `infra_event` flaky / recovered + the unit's status.
//!   2. HealthProtocols evaluation: ordered (condition → action)
//!      rules. First match fires; while a project's protocol is in
//!      flight, subsequent matches queue (next look re-checks once
//!      the current one settles).
//!
//! Everything a look decides lives in the database, never in this
//! process: a unit's latch and the window it is in on its row
//! (`UnitRuntime`), what each protocol acted on and how its failures stand
//! in `infra_health_protocol` (`HealthProtocolState`). So any process
//! carries on where the last look left off, and the broker knows when
//! the next look is due (a window ending, a protocol's retry) and books
//! it for a supervisor that scales to zero.
//!
//! When it looks: in one process that lives as long as the install (a
//! local one), every `health_interval`, every owned project. A supervisor
//! that scales to zero looks once per pass: when told to (a machine
//! running a project's infra says how its unit stands changed, or the
//! cloud says a machine went away, or a person asks), and when the broker
//! booked it. What this supervisor owns is followed as the ownership loop
//! changes it: a claim runs a tick right away.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use uuid::Uuid;
use weft_broker_client::protocol::{HealthProtocolState, InfraCopy, ProjectTrouble};
use weft_core::truncate_user_string;

use crate::health_engine::{
    all_units_healthy, evaluate_protocols, observe_unit, rearm, record_fire, NodeEdgeEvent, NodeHealthState, NodeObservation,
    ProtocolEvalInputs,
};
use crate::protocol::{self, HealthProtocol, ProtocolAction, UnitView};
use crate::SupervisorState;

/// What the health loop keeps in this process: the actions it is running,
/// and what its looks came to, for the next ownership sync to report.
#[derive(Default)]
pub struct HealthRegistry {
    /// Projects with a protocol's action in flight. While one is, the
    /// supervisor doesn't fire any protocol for that project (avoids
    /// action storms when health flaps mid-action).
    in_flight: HashSet<Uuid>,
    /// The steps of each project this process could not get through
    /// since its last look: that look does not report it settled while
    /// any is left.
    troubled: HashMap<Uuid, HashSet<Step>>,
    /// Projects this process looked at, whatever the look came to, not
    /// yet reported by an ownership sync: the look took their failed
    /// pass's retry.
    looked: BTreeSet<Uuid>,
    /// Projects whose look went through with nothing given up, not yet
    /// reported by an ownership sync.
    settled: BTreeSet<Uuid>,
    /// What this process could not get through, by project, not yet
    /// reported by an ownership sync.
    troubles: Troubles,
    /// The steps of a pass that cover every project it holds and could not
    /// be got through (listing the host, reading what it owns), each with
    /// its latest error, not yet reported by an ownership sync.
    all_troubled: BTreeMap<Step, String>,
}

/// The step of a pass over a project that could not be got through.
/// Each keeps its own latest error, so one that comes right (a command's
/// ending written by its rerun) clears only itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum Step {
    /// The project's health look.
    Look,
    /// The sweep of its gone copies.
    Sweep,
    /// One of its health protocols gave up.
    Protocol,
    /// How this command ended could not be written.
    End(i64),
}

impl Step {
    /// A step that failed is tried again on a clock; a protocol that gave
    /// up is not.
    fn retried(self) -> bool {
        self != Step::Protocol
    }
}

/// The latest error of each step that could not be got through, by
/// project.
type Troubles = BTreeMap<Uuid, BTreeMap<Step, String>>;

/// What an ownership sync reports of this process's looks
/// (`SupervisorSyncOwnershipRequest`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Report {
    pub(crate) looked: Vec<Uuid>,
    pub(crate) settled: Vec<Uuid>,
    troubles: Troubles,
    all_troubled: BTreeMap<Step, String>,
}

impl Report {
    /// Why a step over every project failed, each step's error once
    /// (`SupervisorSyncOwnershipRequest::all_failed`).
    pub(crate) fn all_failed(&self) -> Option<String> {
        (!self.all_troubled.is_empty()).then(|| self.all_troubled.values().map(String::as_str).collect::<Vec<_>>().join("; "))
    }

    /// The projects a step of failed, each once with every step's error.
    pub(crate) fn failed(&self) -> Vec<ProjectTrouble> {
        self.troubles(true)
    }

    /// The projects only a health protocol gave up on.
    pub(crate) fn gave_up(&self) -> Vec<ProjectTrouble> {
        self.troubles(false)
    }

    fn troubles(&self, retried: bool) -> Vec<ProjectTrouble> {
        self.troubles
            .iter()
            .filter(|(_, steps)| steps.keys().any(|s| s.retried()) == retried)
            .map(|(project, steps)| ProjectTrouble {
                project_id: *project,
                error: steps.values().map(String::as_str).collect::<Vec<_>>().join("; "),
            })
            .collect()
    }
}

impl HealthRegistry {
    /// True if a protocol action is currently in flight for the
    /// project. Exposed for introspection (e.g. asserting a hung
    /// action's timeout freed the slot).
    pub fn is_in_flight(&self, project_id: Uuid) -> bool {
        self.in_flight.contains(&project_id)
    }

    /// This process could not get through `step` of `project`, with
    /// `error`: its next look does not report it settled, and the next
    /// sync reports the step's latest error.
    pub(crate) fn troubled(&mut self, project: Uuid, step: Step, error: &str) {
        self.troubled.entry(project).or_default().insert(step);
        self.settled.remove(&project);
        self.troubles.entry(project).or_default().insert(step, error.to_string());
    }

    /// `step` of `project` came right after all (a command's rerun wrote
    /// how it ended): neither its look nor the next sync counts it.
    pub(crate) fn untroubled(&mut self, project: Uuid, step: Step) {
        if self.troubled.get_mut(&project).is_some_and(|steps| steps.remove(&step) && steps.is_empty()) {
            self.troubled.remove(&project);
        }
        if self.troubles.get_mut(&project).is_some_and(|steps| steps.remove(&step).is_some() && steps.is_empty()) {
            self.troubles.remove(&project);
        }
    }

    /// A step of a pass that covers every project it holds (`step`)
    /// could not be got through, with `error`: no project is reported
    /// settled, and the next sync reports the step's latest error as a
    /// failure of every project the pass held.
    pub(crate) fn all_troubled(&mut self, step: Step, error: &str) {
        self.all_troubled.insert(step, error.to_string());
    }

    /// What looks came to since the last call, to report. A project that
    /// failed is reported failed only: its retry looks again at all of it.
    /// With a step over every project failed, none is reported settled.
    pub(crate) fn take_report(&mut self) -> Report {
        let all_troubled = std::mem::take(&mut self.all_troubled);
        let settled = std::mem::take(&mut self.settled);
        Report {
            looked: std::mem::take(&mut self.looked).into_iter().collect(),
            settled: match all_troubled.is_empty() {
                true => settled.into_iter().collect(),
                false => Vec::new(),
            },
            troubles: std::mem::take(&mut self.troubles),
            all_troubled,
        }
    }

    /// Report `report` at the next sync again: this one did not go
    /// through. What happened since takes precedence over it: a step
    /// that failed again since keeps its newer error, and a project
    /// troubled since is not reported settled.
    pub(crate) fn report_again(&mut self, report: Report) {
        for (step, error) in report.all_troubled {
            self.all_troubled.entry(step).or_insert(error);
        }
        self.looked.extend(report.looked);
        self.settled.extend(report.settled.iter().filter(|p| !self.troubles.contains_key(p)));
        for (project, steps) in report.troubles {
            if self.settled.contains(&project) {
                continue;
            }
            let kept = self.troubles.entry(project).or_default();
            for (step, error) in steps {
                kept.entry(step).or_insert(error);
            }
        }
    }
}

pub async fn run_loop(
    state: SupervisorState,
    mut changes: tokio::sync::mpsc::UnboundedReceiver<crate::ownership::OwnershipChange>,
) -> Result<()> {
    loop {
        if let Err(e) = tick(&state).await {
            tracing::warn!(error = %e, "health tick failed");
        }
        let next_tick = state.clock.sleep(state.health_interval);
        tokio::pin!(next_tick);
        loop {
            tokio::select! {
                biased;
                change = changes.recv() => {
                    let change = change.ok_or_else(|| {
                        anyhow::anyhow!("the ownership loop is gone; the health loop cannot follow what this supervisor owns")
                    })?;
                    if let Err(e) = on_ownership_change(&state, &change).await {
                        tracing::warn!(error = %e, "health tick after a claim failed");
                    }
                }
                _ = &mut next_tick => break,
            }
        }
    }
}

/// Follow one ownership change: a claim runs a tick now rather than at
/// the next interval. Exposed so integration tests can step it.
pub async fn on_ownership_change(state: &SupervisorState, change: &crate::ownership::OwnershipChange) -> Result<()> {
    if !change.claimed.is_empty() {
        tick(state).await?;
    }
    Ok(())
}

/// One tick of the health loop: look at every owned project. A project
/// whose look fails is reported failed (`crate::failed`), and a later
/// pass looks again. Exposed (rather than only running inside `run_loop`) so
/// integration tests can step the loop one tick at a time.
pub async fn tick(state: &SupervisorState) -> Result<()> {
    let projects = crate::patiently(state, || state.broker.owned_projects(&state.replica)).await?;
    for project in &projects {
        match tick_project(state, project).await {
            Ok(look) => {
                let mut registry = state.health.lock().await;
                registry.looked.insert(project.project_id);
                let troubled = registry.troubled.remove(&project.project_id).is_some();
                if look == Look::Settled && !troubled {
                    registry.settled.insert(project.project_id);
                    registry.troubles.remove(&project.project_id);
                }
            }
            Err(e) => look_failed(state, project.project_id, &format!("could not look at its health: {e:#}")).await,
        }
    }
    Ok(())
}

/// The look at `project` failed with `error`: it counts as looked at
/// (the look took its failed pass's retry) and as failed, so the next
/// sync sets the next retry until it gives up (`crate::failed`).
async fn look_failed(state: &SupervisorState, project: Uuid, error: &str) {
    let mut registry = state.health.lock().await;
    registry.looked.insert(project);
    registry.troubled.remove(&project);
    drop(registry);
    crate::failed(state, project, Step::Look, error).await;
}

/// What one look at a project came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Look {
    /// Nothing is given up.
    Settled,
    /// A protocol's action gave up, now or before.
    GaveUp,
}

/// Look at one project's health: each unit's latch from what the host
/// reports, then its protocols.
async fn tick_project(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
) -> Result<Look> {
    // Stand down, copy by copy, while a user infra action runs on it:
    // an uncompleted supervisor command (apply / stop / terminate) that
    // reaches a copy means the lifecycle handler owns that copy's status
    // right now. Health must NOT look at it (units going down
    // during a stop are the action, not a fault), or its autonomous
    // reconcile would race and clobber the action's transition. Every
    // other copy keeps its health.
    // The first read waits out a broker that cannot answer right now.
    let commands = crate::patiently(state, || state.broker.infra_commands_in_flight(project.project_id)).await?;
    let nodes = state.broker.infra_nodes(project.project_id).await?;

    // How the host sees every unit of the project, by `(copy_id,
    // unit)`. Health is PER-UNIT: one infra node runs N units, each with
    // independent health, so a flaky sidecar doesn't drag a healthy
    // primary into "node flaky" (and can be remediated on its own). The
    // copy, not the node, is the key: a node that exists once
    // per instance runs one copy for each, and one
    // instance's broken copy says nothing about another's.
    let seen: HashMap<(String, String), weft_platform_traits::UnitRunState> = state
        .host
        .observe(&project.tenant_id, project.project_id)
        .await?
        .into_iter()
        .map(|o| ((o.copy_id, o.unit), o.state))
        .collect();

    // ONLY units whose status implies "should be running right now"
    // ({running, flaky}). Skip the rest:
    //   - `stopped` / `stopping`: user intentionally scaled to 0;
    //     0/0 ready is the desired state, not flaky.
    //   - `terminating`: resources being deleted; transient.
    //   - `provisioning`: apply is in progress; the apply executor
    //     is the source of truth until it writes Running.
    //   - `failed`: apply errored; the failure stage carries the
    //     diagnosis, health-flaky would just clobber it.
    // Also skipped: a copy a command in flight reaches (above).
    //
    // A unit the host does not report yet is UNKNOWN: no decision, no
    // view, so no protocol reads it as broken (a copy just applied can
    // reach this look before the host reports it). The unit roster
    // comes from the row's `units` map.
    //
    // The state-machine math lives in `health_engine`; this loop is
    // just the I/O harness around it. A unit's latch or window that
    // moved is written back with its status, in one write, which also
    // reconciles a status another writer drifted (the latch is the
    // single source of truth for the row's status).
    let now = state.clock.now_unix();
    let mut units: Vec<UnitView> = Vec::new();
    for n in &nodes {
        if commands.iter().any(|c| c.reaches(&n.node_id, n.instance.as_ref())) {
            continue;
        }
        for (unit, unit_rt) in n.units.iter().filter(|(_, rt)| rt.status.expects_running_units()) {
            let reading = seen.get(&(n.copy_id.clone(), unit.clone()));
            let observed = reading.map(|s| NodeObservation { ready: *s == weft_platform_traits::UnitRunState::Ready });
            let prior = NodeHealthState::of(unit_rt);
            let Some(decision) = observe_unit(
                prior.clone(),
                observed,
                n.applied_at_unix,
                now,
                Duration::from_secs(unit_rt.flaky_after_seconds as u64),
                Duration::from_secs(unit_rt.recovery_after_seconds as u64),
            ) else {
                continue;
            };
            units.push(UnitView {
                node_id: n.node_id.clone(),
                instance: n.instance.clone(),
                unit: unit.clone(),
                ready: observed.is_some_and(|o| o.ready),
                flaky: decision.next.declared_flaky,
            });
            if let Some(edge) = &decision.event {
                // Why it is not ready, in the host's words, for the
                // flaky event.
                let infra_event = match edge {
                    NodeEdgeEvent::BecameFlaky => {
                        let why = match reading {
                            Some(weft_platform_traits::UnitRunState::NotReady { why })
                            | Some(weft_platform_traits::UnitRunState::Failed { why }) => why.clone(),
                            Some(weft_platform_traits::UnitRunState::Starting { step }) => format!("still starting ({step})"),
                            Some(weft_platform_traits::UnitRunState::Stopped) => "stopped outside weft".into(),
                            Some(weft_platform_traits::UnitRunState::Ready) => "ready".into(),
                            None => "the host no longer reports it".into(),
                        };
                        weft_broker_client::protocol::InfraEvent::Flaky(weft_broker_client::protocol::FlakyPayload {
                            reason: format!("unit '{unit}' is not ready: {why}"),
                        })
                    }
                    NodeEdgeEvent::Recovered => weft_broker_client::protocol::InfraEvent::Recovered,
                };
                state.broker.event_record(project.project_id, Some(&n.node_id), n.instance.as_ref(), infra_event).await?;
            }
            if unit_rt.status != decision.desired_status || prior != decision.next {
                // Fenced on ownership, on no command reaching the copy and
                // on the unit still being in the roster: a stale outcome
                // means one of those moved since this look read them, and
                // the write is dropped.
                let outcome = state
                    .broker
                    .set_unit_health(
                        &state.replica,
                        project.project_id,
                        &n.node_id,
                        n.instance.as_ref(),
                        unit,
                        decision.desired_status,
                        decision.next.not_ready_since_unix,
                        decision.next.ready_since_unix,
                    )
                    .await?;
                if !outcome.is_applied() {
                    tracing::debug!(
                        project_id = %project.project_id,
                        node_id = %n.node_id,
                        unit = %unit,
                        ?outcome,
                        "the unit's health write was not applied (row removed, unit left the roster, a command is in flight, or ownership moved); skipping"
                    );
                }
            }
        }
    }

    // HealthProtocol evaluation. The pure decision (which protocol
    // fires, if any) lives in `health_engine::evaluate_protocols`.
    // This block just gathers the inputs, calls the pure fn, then
    // dispatches the matched action.
    let read = state.broker.health_protocols(project.project_id).await?;
    let protocols: protocol::HealthProtocols = match read.protocols {
        Some(v) => match serde_json::from_value::<protocol::HealthProtocols>(v.clone()) {
            Ok(p) => p,
            Err(e) => {
                // The user's protocol config is broken (unreadable). Emit a
                // `protocol_config_error` event so the action bar shows it,
                // then SKIP this look's evaluation rather than fall back to
                // the defaults, which would override the user's intent; the
                // next look re-reads the column. serde error strings can
                // balloon on deeply nested protocol shapes, so the message
                // is bounded below the 7800-byte Postgres NOTIFY cap.
                state
                    .broker
                    .event_record(
                        project.project_id,
                        None,
                        None,
                        weft_broker_client::protocol::InfraEvent::ProtocolConfigError(
                            weft_broker_client::protocol::ProtocolConfigErrorPayload {
                                error: truncate_user_string(&e.to_string(), 4096),
                            },
                        ),
                    )
                    .await?;
                tracing::warn!(
                    project_id = %project.project_id,
                    error = %e,
                    "health_protocols_json malformed; skipping protocol eval until fixed"
                );
                return Ok(Look::Settled);
            }
        },
        None => protocol::default_protocols(),
    };
    let inputs = ProtocolEvalInputs {
        units,
        project_status: project.status,
        health_parked: project.health_parked,
    };
    let mut rows: HashMap<String, HealthProtocolState> = read.state.into_iter().map(|s| (s.protocol.clone(), s)).collect();
    let before = rows.clone();
    let look = act(state, project, &protocols, &inputs, &mut rows, now).await;
    if rows != before {
        let mut protocols: Vec<HealthProtocolState> = rows.into_values().collect();
        protocols.sort_by(|a, b| a.protocol.cmp(&b.protocol));
        let req = weft_broker_client::protocol::SupervisorSetHealthStateRequest {
            replica: state.replica.clone(),
            project_id: project.project_id,
            protocols,
        };
        if !state.broker.set_health_state(&req).await?.is_applied() {
            tracing::info!(project_id = %project.project_id, "the project's lease moved; its protocols' state is the new owner's");
        }
    }
    Ok(look)
}

/// Re-arm what the protocols did against this look, fire the first one
/// with something to do, and keep how it went in `rows` (the project's
/// [`HealthProtocolState`]s, which the caller writes back when they
/// changed).
async fn act(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
    protocols: &protocol::HealthProtocols,
    inputs: &ProtocolEvalInputs,
    rows: &mut HashMap<String, HealthProtocolState>,
    now: i64,
) -> Look {
    // A protocol that gave up tries again once what it looked at changed
    // (a machine said so), or every unit is healthy; one a person's
    // command or look touched since comes back from the broker armed
    // already.
    let seen = serde_json::to_value(&inputs.units).expect("a unit view serializes");
    let healthy = all_units_healthy(&inputs.units);
    for row in rows.values_mut() {
        if healthy || row.gave_up_seen.as_ref().is_some_and(|s| *s != seen) {
            *row = HealthProtocolState { acted: row.acted.take(), ..unarmed(&row.protocol) };
        }
    }
    // What the protocols fired, re-armed copy by copy.
    let mut fired: HashMap<String, BTreeSet<InfraCopy>> =
        rows.values().filter_map(|r| r.acted.clone().map(|acted| (r.protocol.clone(), acted))).collect();
    rearm(&mut fired, protocols, inputs);
    for row in rows.values_mut() {
        row.acted = fired.remove(&row.protocol);
    }
    rows.retain(|_, r| *r != unarmed(&r.protocol));

    let gave_up = |rows: &HashMap<String, HealthProtocolState>| {
        if rows.values().any(|r| r.gave_up_at_unix.is_some()) {
            Look::GaveUp
        } else {
            Look::Settled
        }
    };
    let in_flight = state.health.lock().await.in_flight.contains(&project.project_id);
    let fired: HashMap<String, BTreeSet<InfraCopy>> =
        rows.values().filter_map(|r| r.acted.clone().map(|acted| (r.protocol.clone(), acted))).collect();
    let Some(matched) = evaluate_protocols(protocols, &fired, in_flight, inputs) else {
        return gave_up(rows);
    };
    let proto = matched.protocol.clone();
    let scope = matched.scope;
    // Backing off, or given up: the protocol waits (the broker books its
    // retry).
    if let Some(row) = rows.get(&proto.name) {
        if row.gave_up_at_unix.is_some() || row.next_retry_at_unix.is_some_and(|at| now < at) {
            return gave_up(rows);
        }
    }
    {
        let mut registry = state.health.lock().await;
        if !registry.in_flight.insert(project.project_id) {
            return gave_up(rows);
        }
    }
    tracing::info!(project_id = %project.project_id, protocol = %proto.name, "HealthProtocol matched; firing action");
    // Bound the action by the protocol's timeout (always set:
    // `timeout_seconds` is non-optional with a safe default, so
    // "unbounded" can't be expressed). A hung broker/host call would
    // otherwise pin `in_flight` forever, wedging this project's health
    // monitoring; the timeout maps it to a loud failure.
    let secs = proto.timeout_seconds;
    let result = match tokio::time::timeout(Duration::from_secs(secs as u64), run_action(state, project, &proto, &scope)).await {
        Ok(r) => r,
        Err(_elapsed) => Err(anyhow::anyhow!("HealthProtocol action timed out after {secs}s (broker/host call hung)")),
    };
    state.health.lock().await.in_flight.remove(&project.project_id);
    let row = rows.entry(proto.name.clone()).or_insert_with(|| unarmed(&proto.name));
    match result {
        Ok(()) => {
            // Only a success latches: a failed action never took effect.
            let mut acted = HashMap::from([(proto.name.clone(), row.acted.take().unwrap_or_default())]);
            record_fire(&mut acted, &proto, &scope);
            *row = HealthProtocolState { acted: acted.remove(&proto.name), ..unarmed(&proto.name) };
        }
        Err(e) => {
            let error = format!("{e:#}");
            row.consecutive_failures += 1;
            row.last_error = Some(error.clone());
            match weft_broker_client::protocol::supervisor_retry_after(row.consecutive_failures) {
                None => {
                    row.next_retry_at_unix = None;
                    row.gave_up_at_unix = Some(now);
                    row.gave_up_seen = Some(seen);
                    crate::give_up(state, project.project_id, &format!("its health protocol '{}' failed {} times: {error}", proto.name, row.consecutive_failures)).await;
                }
                Some(delay) => {
                    let delay = delay.as_secs() as i64;
                    row.next_retry_at_unix = Some(now + delay);
                    tracing::warn!(project_id = %project.project_id, protocol = %proto.name, %error, retry_in_secs = delay, "HealthProtocol action failed; it tries again");
                }
            }
        }
    }
    gave_up(rows)
}

/// A protocol's state with nothing fired and nothing failed.
fn unarmed(protocol: &str) -> HealthProtocolState {
    HealthProtocolState {
        protocol: protocol.to_string(),
        acted: None,
        consecutive_failures: 0,
        next_retry_at_unix: None,
        gave_up_at_unix: None,
        gave_up_seen: None,
        last_error: None,
    }
}

/// What `plan_action` decided. The I/O wrapper turns each variant
/// into the corresponding broker/host call. The split lets us unit-test
/// the decision (which verb, which payload, which copies) without
/// needing fake broker + host clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActionPlan {
    /// Emit a `notify` infra_event for UI / observability (the
    /// target of `ProtocolAction::Notify`). NOT used for trigger-state
    /// changes; those go through `EnqueueLifecycle`. Purely a
    /// "flag a channel"-style notification, no lifecycle effect.
    Notify {
        payload: weft_broker_client::protocol::NotifyPayload,
    },
    /// Enqueue a dispatcher-targeted lifecycle command. The
    /// dispatcher's claim loop picks it up and runs the
    /// deactivate/activate flow (the supervisor has no signal-table
    /// access of its own). Verb-and-payload coherence is checked
    /// at compile time by the typed `LifecycleSpec`.
    EnqueueLifecycle {
        spec: weft_broker_client::protocol::LifecycleSpec,
    },
    /// Restart `unit` in the copies of the node owned by whoever owns a
    /// broken copy (the protocol's scope). Each copy's id is
    /// resolved from the broker's `infra_nodes` list ahead of dispatch.
    RestartUnit { copies: Vec<HostCopy>, unit: String },
    /// The action references a node with no copy at all in the
    /// project's `infra_nodes`. Logged via tracing; otherwise no-op.
    NodeMissing { node_id: String },
    /// The node has copies, but none belongs to the owners of the broken
    /// copies (`owners`: `None` for the shared copy): the action names a
    /// node on the other side (a shared node for a broken instance's copy,
    /// or the reverse). Logged via tracing; otherwise no-op.
    NoCopyForOwners {
        node_id: String,
        owners: Vec<Option<weft_core::instance::InstanceId>>,
    },
}

/// One copy of a node and the id the host runs it under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostCopy {
    pub copy: weft_broker_client::protocol::InfraCopy,
    pub copy_id: String,
}

/// Pure: given the matched protocol, the broken copies it aims at (its
/// match's scope), and the broker's current `infra_nodes` snapshot,
/// decide what side effect to perform. No I/O.
pub(crate) fn plan_action(
    proto: &HealthProtocol,
    scope: &std::collections::BTreeSet<weft_broker_client::protocol::InfraCopy>,
    infra_nodes: &[weft_broker_client::protocol::SupervisorInfraNode],
) -> ActionPlan {
    use weft_broker_client::protocol::{
        DeactivateSpec, DeactivationMode, LifecycleSpec, RestoreReaders, RunningPolicy, TakeDownReach, TakeDownReaders,
    };
    // A take-down of the triggers reading the broken copies. The
    // dispatcher's claim loop runs it; lifecycle commands are the
    // channel (the supervisor has no signal-table access).
    // A condition naming no infra says nothing about which copies are
    // broken: the take-down reaches the whole project.
    let reach = if crate::protocol::names_infra(&proto.when) {
        TakeDownReach::ReadersOf { broken: scope.iter().cloned().collect() }
    } else {
        TakeDownReach::Project
    };
    let take_down = |spec: DeactivateSpec| ActionPlan::EnqueueLifecycle {
        spec: LifecycleSpec::Deactivate(TakeDownReaders { spec, reach: reach.clone() }),
    };
    match &proto.action {
        ProtocolAction::Notify { channel } => ActionPlan::Notify {
            payload: weft_broker_client::protocol::NotifyPayload {
                protocol: proto.name.clone(),
                channel: channel.clone(),
            },
        },
        ProtocolAction::ParkTriggers => take_down(DeactivateSpec {
            mode: DeactivationMode::Park,
            grace_minutes: 15,
            running_policy: RunningPolicy::Wait,
            // Autonomous park: the server default cap.
            drain_timeout_secs: None,
        }),
        ProtocolAction::HibernateTriggers { grace_minutes } => take_down(DeactivateSpec {
            mode: DeactivationMode::Hibernate,
            grace_minutes: *grace_minutes,
            running_policy: RunningPolicy::Wait,
            drain_timeout_secs: None,
        }),
        ProtocolAction::WipeTriggers => take_down(DeactivateSpec {
            mode: DeactivationMode::Wipe,
            grace_minutes: 0,
            running_policy: RunningPolicy::Cancel,
            drain_timeout_secs: None,
        }),
        ProtocolAction::AutoRecover => ActionPlan::EnqueueLifecycle {
            spec: LifecycleSpec::Reactivate(RestoreReaders { still_broken: scope.iter().cloned().collect() }),
        },
        ProtocolAction::RestartUnit { node_id, unit } => {
            copies_of(infra_nodes, node_id, scope, |copies| ActionPlan::RestartUnit { copies, unit: unit.clone() })
        }
    }
}

/// The plan `act` makes over each copy of `node_id`
/// owned by the owner of one of the broken copies in `scope` (the shared
/// copy for a broken shared one, ada's for a broken copy of ada's); when
/// there is none, why: no copy of the node at all, or none of those
/// owners'.
fn copies_of(
    infra_nodes: &[weft_broker_client::protocol::SupervisorInfraNode],
    node_id: &str,
    scope: &std::collections::BTreeSet<weft_broker_client::protocol::InfraCopy>,
    act: impl FnOnce(Vec<HostCopy>) -> ActionPlan,
) -> ActionPlan {
    let copies: Vec<_> = infra_nodes.iter().filter(|n| n.node_id == node_id).collect();
    if copies.is_empty() {
        return ActionPlan::NodeMissing { node_id: node_id.to_string() };
    }
    let targets: Vec<HostCopy> = copies
        .iter()
        .filter(|n| scope.iter().any(|broken| broken.instance == n.instance))
        .map(|n| HostCopy {
            copy: weft_broker_client::protocol::InfraCopy { node_id: n.node_id.clone(), instance: n.instance.clone() },
            copy_id: n.copy_id.clone(),
        })
        .collect();
    if targets.is_empty() {
        let owners: std::collections::BTreeSet<_> = scope.iter().map(|broken| broken.instance.clone()).collect();
        return ActionPlan::NoCopyForOwners { node_id: node_id.to_string(), owners: owners.into_iter().collect() };
    }
    act(targets)
}

async fn run_action(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
    proto: &HealthProtocol,
    scope: &std::collections::BTreeSet<weft_broker_client::protocol::InfraCopy>,
) -> Result<()> {
    // For RestartUnit we need the current infra_nodes list to resolve
    // node_id → copy_id. EnqueueLifecycle / Notify don't
    // need it; pay the broker round-trip up front to keep the
    // planner pure regardless.
    let nodes = state.broker.infra_nodes(project.project_id).await?;
    let plan = plan_action(proto, scope, &nodes);
    match plan {
        ActionPlan::Notify { payload } => {
            state
                .broker
                .event_record(
                    project.project_id,
                    None,
                    None,
                    weft_broker_client::protocol::InfraEvent::Notify(payload),
                )
                .await?;
        }
        ActionPlan::EnqueueLifecycle { spec } => {
            state.broker.enqueue_lifecycle(project.project_id, spec).await?;
        }
        ActionPlan::RestartUnit { copies, unit } => {
            // Every copy is attempted; any left unrestarted fails the
            // action, which releases the protocol's latch so it retries
            // next tick (a restart is idempotent).
            let mut failed = Vec::new();
            for HostCopy { copy, copy_id } in copies {
                let node = weft_core::infra::NodeRef {
                    tenant: project.tenant_id.clone(),
                    project: project.project_id,
                    node: copy.node_id.clone(),
                    copy_id,
                };
                if let Err(e) = state.host.restart_unit(&node, &unit).await {
                    failed.push(format!("{}: {e:#}", copy.node_id));
                }
            }
            if !failed.is_empty() {
                anyhow::bail!(
                    "protocol '{}' restarted unit '{unit}' in only some copies; not done: {}",
                    proto.name,
                    failed.join("; ")
                );
            }
        }
        ActionPlan::NodeMissing { node_id } => {
            tracing::warn!(
                project_id = %project.project_id,
                protocol = %proto.name,
                missing_node = %node_id,
                "HealthProtocol action references node that's not in infra_nodes; skipping",
            );
        }
        ActionPlan::NoCopyForOwners { node_id, owners } => {
            let owners: Vec<String> =
                owners.iter().map(|o| o.as_ref().map_or_else(|| "shared".to_string(), |m| m.to_string())).collect();
            tracing::warn!(
                project_id = %project.project_id,
                protocol = %proto.name,
                node = %node_id,
                owners = %owners.join(", "),
                "HealthProtocol action: no copy of {node_id} belongs to the broken owners; skipping",
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{HealthCondition, HealthProtocol, ProtocolAction};
    use weft_broker_client::protocol::{InfraCopy, SupervisorInfraNode};

    /// A project is reported once per sync with each step's latest error:
    /// failed when any step failed (tried again), given up when only a
    /// protocol gave up. A report that did not go through is reported
    /// again, but never over what happened since: a newer error of a
    /// step, a look that settled the project, or a trouble that keeps a
    /// project it reported settled from being settled.
    #[test]
    fn a_report_carries_each_project_once_and_what_happened_since_wins() {
        let (a, b, c, d) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4));
        let mut registry = HealthRegistry::default();
        registry.troubled(a, Step::Protocol, "the protocol gave up");
        registry.troubled(a, Step::Sweep, "the sweep failed");
        registry.troubled(b, Step::Protocol, "the protocol gave up");
        registry.settled.extend([c, d]);
        let report = registry.take_report();
        let trouble = |project, error: &str| ProjectTrouble { project_id: project, error: error.into() };
        assert_eq!(report.failed(), vec![trouble(a, "the sweep failed; the protocol gave up")]);
        assert_eq!(report.gave_up(), vec![trouble(b, "the protocol gave up")]);
        assert_eq!(report.settled, vec![c, d]);

        registry.troubled(a, Step::Sweep, "the sweep failed again");
        registry.settled.insert(b);
        // `d` fails, and its look clears what it failed since that look
        // without settling it: its trouble is still to be reported.
        registry.troubled(d, Step::Sweep, "the sweep failed");
        registry.troubled.clear();
        registry.report_again(report);
        let again = registry.take_report();
        assert_eq!(
            again.failed(),
            vec![trouble(a, "the sweep failed again; the protocol gave up"), trouble(d, "the sweep failed")],
            "the newer error stands"
        );
        assert!(again.gave_up().is_empty(), "a project settled since is not reported given up");
        assert_eq!(again.settled, vec![b, c], "a project troubled since is not reported settled");
    }

    /// A command's ending that a rerun wrote clears only that trouble: the
    /// project's look settles it when nothing else failed, and the other
    /// steps' errors stay reported.
    #[test]
    fn a_written_ending_clears_only_its_own_trouble() {
        let (a, b) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let mut registry = HealthRegistry::default();
        registry.troubled(a, Step::End(7), "its command 7 ran, and how it ended could not be written");
        registry.troubled(b, Step::End(8), "its command 8 ran, and how it ended could not be written");
        registry.troubled(b, Step::Sweep, "the sweep failed");
        registry.untroubled(a, Step::End(7));
        registry.untroubled(b, Step::End(8));
        assert!(!registry.troubled.contains_key(&a), "the look of `a` may settle it");
        assert!(registry.troubled.contains_key(&b), "`b` still failed its sweep");
        let report = registry.take_report();
        assert_eq!(report.failed(), vec![ProjectTrouble { project_id: b, error: "the sweep failed".into() }]);
    }

    fn proto(action: ProtocolAction) -> HealthProtocol {
        HealthProtocol {
            name: "p".to_string(),
            when: HealthCondition::NodeNotReady { node_id: "*".into(), unit: "*".into() },
            action,
            timeout_seconds: 1800,
        }
    }

    fn node(node_id: &str, copy_id: &str) -> SupervisorInfraNode {
        SupervisorInfraNode {
            node_id: node_id.to_string(),
            copy_id: copy_id.to_string(),
            status: weft_broker_client::protocol::InfraNodeStatus::Running,
            applied_spec_hash: None,
            applied_at_unix: None,
            addresses: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            instance: None,
        }
    }

    #[test]
    fn plan_notify_returns_notify_with_channel() {
        let p = proto(ProtocolAction::Notify {
            channel: "ops".into(),
        });
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::Notify { payload } => {
                assert_eq!(payload.channel, "ops");
                assert_eq!(payload.protocol, "p");
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    fn no_scope() -> std::collections::BTreeSet<InfraCopy> {
        Default::default()
    }

    /// The park carries what is broken, so the dispatcher parks only
    /// the triggers that read it: here ada's copy of `svc`.
    #[test]
    fn plan_park_triggers_enqueues_deactivate_park_aimed_at_the_broken_copies() {
        use weft_broker_client::protocol::{
            DeactivationMode, LifecycleSpec, RunningPolicy,
        };
        let p = proto(ProtocolAction::ParkTriggers);
        let ada_svc = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("ada").unwrap()) };
        match plan_action(&p, &std::collections::BTreeSet::from([ada_svc.clone()]), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.spec.mode, DeactivationMode::Park);
                assert_eq!(d.spec.running_policy, RunningPolicy::Wait);
                assert_eq!(d.reach, weft_broker_client::protocol::TakeDownReach::ReadersOf { broken: vec![ada_svc] });
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    /// A take-down whose condition names no infra reaches the whole
    /// project, as it did before copies had owners.
    #[test]
    fn plan_park_on_a_condition_naming_no_infra_reaches_the_project() {
        use weft_broker_client::protocol::{LifecycleSpec, ProjectStatus, TakeDownReach};
        let p = HealthProtocol {
            when: HealthCondition::ProjectStatusEq { status: ProjectStatus::Active },
            ..proto(ProtocolAction::ParkTriggers)
        };
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::EnqueueLifecycle { spec: LifecycleSpec::Deactivate(d) } => {
                assert_eq!(d.reach, TakeDownReach::Project)
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_hibernate_triggers_carries_grace() {
        use weft_broker_client::protocol::{
            DeactivationMode, LifecycleSpec, RunningPolicy,
        };
        let p = proto(ProtocolAction::HibernateTriggers { grace_minutes: 42 });
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.spec.mode, DeactivationMode::Hibernate);
                assert_eq!(d.spec.grace_minutes, 42);
                assert_eq!(d.spec.running_policy, RunningPolicy::Wait);
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_wipe_triggers_enqueues_deactivate_wipe_cancel() {
        use weft_broker_client::protocol::{
            DeactivationMode, LifecycleSpec, RunningPolicy,
        };
        let p = proto(ProtocolAction::WipeTriggers);
        match plan_action(&p, &no_scope(), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Deactivate(d),
            } => {
                assert_eq!(d.spec.mode, DeactivationMode::Wipe);
                assert_eq!(d.spec.running_policy, RunningPolicy::Cancel);
            }
            other => panic!("wrong plan: {other:?}"),
        }
    }

    #[test]
    fn plan_auto_recover_enqueues_reactivate() {
        use weft_broker_client::protocol::LifecycleSpec;
        let p = proto(ProtocolAction::AutoRecover);
        let shared = InfraCopy { node_id: "svc".into(), instance: None };
        match plan_action(&p, &std::collections::BTreeSet::from([shared.clone()]), &[]) {
            ActionPlan::EnqueueLifecycle {
                spec: LifecycleSpec::Reactivate(restore),
            } => assert_eq!(restore.still_broken, vec![shared]),
            other => panic!("wrong plan: {other:?}"),
        }
    }

    fn shared_copy(node_id: &str, copy_id: &str) -> HostCopy {
        HostCopy { copy: InfraCopy { node_id: node_id.into(), instance: None }, copy_id: copy_id.into() }
    }

    fn shared_broken(node_id: &str) -> std::collections::BTreeSet<InfraCopy> {
        std::collections::BTreeSet::from([InfraCopy { node_id: node_id.into(), instance: None }])
    }

    /// A restart reaches the copies owned by whoever owns a broken copy:
    /// ada's broken `n1` restarts ada's `n1` only, never the shared one
    /// or bob's; a broken shared `db` restarts the shared `n1`.
    #[test]
    fn plan_restart_reaches_only_the_broken_owners_copy() {
        let p = proto(ProtocolAction::RestartUnit { node_id: "n1".into(), unit: "main".into() });
        let ada_id = weft_core::instance::InstanceId::new("ada").unwrap();
        let mut ada = node("n1", "inst-ada");
        ada.instance = Some(ada_id.clone());
        let mut bob = node("n1", "inst-bob");
        bob.instance = Some(weft_core::instance::InstanceId::new("bob").unwrap());
        let nodes = vec![node("n1", "inst-shared"), ada, bob, node("n2", "inst-other")];
        let ada_broken = std::collections::BTreeSet::from([InfraCopy { node_id: "n1".into(), instance: Some(ada_id.clone()) }]);
        assert_eq!(
            plan_action(&p, &ada_broken, &nodes),
            ActionPlan::RestartUnit {
                copies: vec![HostCopy {
                    copy: InfraCopy { node_id: "n1".into(), instance: Some(ada_id) },
                    copy_id: "inst-ada".into(),
                }],
                unit: "main".into(),
            }
        );
        assert_eq!(
            plan_action(&p, &shared_broken("db"), &nodes),
            ActionPlan::RestartUnit { copies: vec![shared_copy("n1", "inst-shared")], unit: "main".into() }
        );
    }

    #[test]
    fn plan_names_the_owners_when_the_node_is_on_the_other_side() {
        let p = proto(ProtocolAction::RestartUnit { node_id: "n1".into(), unit: "main".into() });
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        let broken = std::collections::BTreeSet::from([InfraCopy { node_id: "db".into(), instance: Some(ada.clone()) }]);
        assert_eq!(
            plan_action(&p, &broken, &[node("n1", "inst-shared")]),
            ActionPlan::NoCopyForOwners { node_id: "n1".into(), owners: vec![Some(ada)] }
        );
    }

    #[test]
    fn plan_restart_node_missing_when_no_match() {
        let p = proto(ProtocolAction::RestartUnit { node_id: "ghost".into(), unit: "main".into() });
        assert_eq!(
            plan_action(&p, &shared_broken("ghost"), &[node("n1", "inst-abc")]),
            ActionPlan::NodeMissing { node_id: "ghost".into() }
        );
    }

    #[test]
    fn default_protocols_two_stage_park_then_recover() {
        let p = protocol::default_protocols();
        assert_eq!(p.protocols.len(), 2);
        assert!(matches!(p.protocols[0].action, ProtocolAction::ParkTriggers));
        assert!(matches!(p.protocols[1].action, ProtocolAction::AutoRecover));
    }
}
