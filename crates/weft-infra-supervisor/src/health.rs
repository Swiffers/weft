//! Health loop. Asks the host how every unit of the owned projects is
//! doing, runs each copy's health rules on it, and emits `infra_event`
//! rows via the broker when a node's status changes.
//!
//! Two concerns:
//!   1. Per-unit windowed health-state transitions (flaky after N
//!      seconds not ready, recovered after N seconds ready). Drives
//!      `infra_event` flaky / recovered + the unit's status.
//!   2. Health rules: each copy runs its node's rules
//!      (`weft_core::infra::health`), ordered (condition, action) pairs
//!      read on that copy's own units; its first rule that holds and has
//!      not fired is its pick, and one action per look serves every copy
//!      whose pick asks for it. The action is a command the dispatcher
//!      runs; its rules count as fired while it runs, and the look after
//!      it ends (the dispatcher asks for one as it does) reads what it
//!      did, or counts its failure (or its running past the rule's
//!      timeout) against the rules and tries again.
//!
//! Everything a look decides lives in the database, never in this
//! process: a unit's latch and the window it is in on its row
//! (`UnitRuntime`), whether each rule fired on each copy and how its
//! failures stand in `infra_health_rule` (`HealthRuleState`). So any
//! process carries on where the last look left off, and the broker knows
//! when the next look is due (a window ending, a rule's retry, a queued
//! command's deadline) and books
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
use weft_broker_client::protocol::{
    HealthCommandEnding, HealthRuleState, InfraCopy, LifecycleOutcome, ProjectTrouble, QueuedHealthCommand,
};
use weft_core::infra::health::{HealthAction, UnitReading};

use crate::health_engine::{
    next_fire, observe_unit, rearm, CopyView, NodeEdgeEvent, NodeHealthState, NodeObservation, RuleInputs, RuleOn,
};
use crate::SupervisorState;

/// What the health loop keeps in this process: the actions it is running,
/// and what its looks came to, for the next ownership sync to report.
#[derive(Default)]
pub struct HealthRegistry {
    /// Projects whose health action this process is queueing right now.
    /// While one is, it fires no other rule for that project (avoids
    /// action storms when health flaps mid-action); once queued, the
    /// command's rules wait on it through their state
    /// (`HealthRuleState::queued`).
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
    /// One of its health rules gave up.
    Rule,
    /// How this command ended could not be written.
    End(i64),
}

impl Step {
    /// A step that failed is tried again on a clock; a rule that gave up
    /// is not.
    fn retried(self) -> bool {
        self != Step::Rule
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

    /// The projects only a health rule gave up on.
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
    /// True if a health rule's action is currently in flight for the
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
            tracing::warn!(target: "weft_infra_supervisor::health", error = %e, "health tick failed");
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
                        tracing::warn!(target: "weft_infra_supervisor::health", error = %e, "health tick after a claim failed");
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
    /// A health rule's action gave up, now or before.
    GaveUp,
}

/// Look at one project's health: each unit's latch from what the host
/// reports, then each copy's health rules.
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
    // reading, so no rule reads it as broken (a copy just applied can
    // reach this look before the host reports it). The unit roster
    // comes from the row's `units` map.
    //
    // The state-machine math lives in `health_engine`; this loop is
    // just the I/O harness around it. A unit's latch or window that
    // moved is written back with its status, in one write, which also
    // reconciles a status another writer drifted (the latch is the
    // single source of truth for the row's status).
    let now = state.clock.now_unix();
    let mut copies: Vec<CopyView> = Vec::new();
    for n in &nodes {
        if commands.iter().any(|c| c.reaches(&n.node_id, n.instance.as_ref())) {
            continue;
        }
        let mut units: Vec<UnitReading> = Vec::new();
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
            units.push(UnitReading {
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
                        target: "weft_infra_supervisor::health",
                        project_id = %project.project_id,
                        node_id = %n.node_id,
                        unit = %unit,
                        ?outcome,
                        "the unit's health write was not applied (row removed, unit left the roster, a command is in flight, or ownership moved); skipping"
                    );
                }
            }
        }
        // A copy the host reported none of is neither broken nor healthy:
        // no rule reads it.
        if !units.is_empty() {
            copies.push(CopyView {
                copy: InfraCopy { node_id: n.node_id.clone(), instance: n.instance.clone() },
                rules: weft_core::infra::health::rules_of(n.health_rules.as_deref()),
                units,
            });
        }
    }

    // The health rules: the pure decision (which rule fires on which
    // copies, if any) lives in `health_engine::next_fire`. This gathers
    // the inputs and the rules' state, fires, and writes the state back
    // when it moved.
    let inputs = RuleInputs {
        copies,
        listening: project.status == weft_broker_client::protocol::ProjectStatus::Active,
        taken_down_by_health: project.health_down,
    };
    let read = state.broker.health_state(project.project_id).await?;
    let mut rows: BTreeMap<RuleOn, HealthRuleState> =
        read.rules.into_iter().map(|s| (RuleOn { copy: s.copy.clone(), rule: s.rule.clone() }, s)).collect();
    let before = rows.clone();
    let look = act(state, project, &inputs, &mut rows, &read.ended, now).await;
    if rows != before {
        let req = weft_broker_client::protocol::SupervisorSetHealthStateRequest {
            replica: state.replica.clone(),
            project_id: project.project_id,
            rules: rows.into_values().collect(),
        };
        if !state.broker.set_health_state(&req).await?.is_applied() {
            tracing::info!(target: "weft_infra_supervisor::health", project_id = %project.project_id, "the project's lease moved; its health rules' state is the new owner's");
        }
    }
    Ok(look)
}

/// Settle the commands the rules queued (`ended`: how the ones that ended
/// did), re-arm the rules against this look, fire what has something to
/// do, and keep how it went in `rows` (the project's
/// [`HealthRuleState`]s, which the caller writes back when they changed).
async fn act(
    state: &SupervisorState,
    project: &weft_broker_client::protocol::SupervisorProject,
    inputs: &RuleInputs,
    rows: &mut BTreeMap<RuleOn, HealthRuleState>,
    ended: &[HealthCommandEnding],
    now: i64,
) -> Look {
    let seen_of = |copy: &InfraCopy| serde_json::to_value(inputs.units_of(copy)).expect("a unit reading serializes");
    let mut given_up = Vec::new();
    // The commands the rules queued. One that ended is done with; one
    // that failed, or that has not ended by its deadline, is a failure of
    // its rule, tried again like any failed action. The dispatcher asks
    // for a look as each one ends, so this look is usually that one.
    let mut landed: BTreeSet<RuleOn> = BTreeSet::new();
    for (on, row) in rows.iter_mut() {
        let Some(queued) = row.queued else { continue };
        let error = match ended.iter().find(|e| e.command == queued.command) {
            Some(HealthCommandEnding { outcome: LifecycleOutcome::Failed, message, .. }) => {
                format!("its command {} failed: {}", queued.command, message.as_deref().unwrap_or("no reason given"))
            }
            Some(HealthCommandEnding { outcome, .. }) => {
                row.queued = None;
                row.consecutive_failures = 0;
                row.last_error = None;
                let took_down = inputs.rule_on(on).is_some_and(|(rule, _)| rule.action == HealthAction::TakeDownTriggers);
                if *outcome == LifecycleOutcome::Succeeded && took_down {
                    row.took_down = true;
                    landed.insert(on.clone());
                }
                continue;
            }
            None if now >= queued.deadline_unix => {
                format!("its command {} did not end within the rule's timeout", queued.command)
            }
            None => continue,
        };
        row.queued = None;
        row.fired = false;
        given_up.extend(failed(project.project_id, on, row, &error, seen_of(&on.copy), now));
    }
    // A rule that no longer holds for its copy (or whose copy this look
    // did not see) starts its next episode afresh; one that gave up while
    // it holds tries again once what it looked at (its copy's units)
    // changed (a machine said so). One a person's command or look
    // touched since comes back from the broker armed already.
    for (on, row) in rows.iter_mut() {
        let holds = inputs.rule_on(on).is_some_and(|(_, holds)| holds);
        if !holds || row.gave_up_seen.as_ref().is_some_and(|s| *s != seen_of(&on.copy)) {
            *row = HealthRuleState { fired: row.fired, queued: row.queued, took_down: row.took_down, ..unarmed(on) };
        }
    }
    // What fired, re-armed rule by rule, copy by copy.
    let mut fired: BTreeSet<RuleOn> = rows.iter().filter(|(_, r)| r.fired).map(|(on, _)| on.clone()).collect();
    rearm(&mut fired, inputs);
    // What a take-down took down stays its until its copy came back by
    // its node's own rule, or nothing the health check took down is down
    // any more (a person brought it back). A take-down seen landing in
    // this very look keeps its mark: the project was read before the
    // command's ending, so it may not show what the take-down did yet.
    for (on, row) in rows.iter_mut() {
        row.fired = fired.contains(on);
        if row.took_down && (inputs.brought_back(&on.copy) || (!inputs.taken_down_by_health && !landed.contains(on))) {
            row.took_down = false;
        }
    }
    rows.retain(|on, r| *r != unarmed(on));
    if !given_up.is_empty() {
        crate::give_up(state, project.project_id, &given_up.join("; ")).await;
    }

    let gave_up = |rows: &BTreeMap<RuleOn, HealthRuleState>| {
        if rows.values().any(|r| r.gave_up_at_unix.is_some()) {
            Look::GaveUp
        } else {
            Look::Settled
        }
    };
    if state.health.lock().await.in_flight.contains(&project.project_id) {
        return gave_up(rows);
    }
    // Backing off, given up (the broker books its retry), or its command
    // still running: that rule waits on its copy.
    let waiting = |on: &RuleOn| {
        rows.get(on).is_some_and(|r| {
            r.gave_up_at_unix.is_some() || r.queued.is_some() || r.next_retry_at_unix.is_some_and(|at| now < at)
        })
    };
    let Some(fire) = next_fire(inputs, &fired, waiting) else {
        return gave_up(rows);
    };
    {
        let mut registry = state.health.lock().await;
        if !registry.in_flight.insert(project.project_id) {
            return gave_up(rows);
        }
    }
    let names = fire.rules.iter().map(|(on, _)| format!("'{}' on {}", on.rule, copy_name(&on.copy))).collect::<Vec<_>>().join(", ");
    tracing::info!(target: "weft_infra_supervisor::health", project_id = %project.project_id, rules = %names, "health rules hold; firing their action");
    // A copy whose take-down took something down, or is queued, keeps
    // its readers down when a bring-back runs.
    let took_down: BTreeSet<InfraCopy> = rows
        .iter()
        .filter(|(on, r)| {
            r.took_down
                || (r.queued.is_some() && inputs.rule_on(on).is_some_and(|(rule, _)| rule.action == HealthAction::TakeDownTriggers))
        })
        .map(|(on, _)| on.copy.clone())
        .collect();
    // Bound the queueing by its rules' timeout. A hung broker call would
    // otherwise pin `in_flight` forever, wedging this project's health
    // monitoring; the timeout maps it to a loud failure.
    let timeout = fire.timeout();
    let action = state.broker.enqueue_lifecycle(project.project_id, fire.plan(inputs, &took_down));
    let result = match tokio::time::timeout(timeout, action).await {
        Ok(r) => r,
        Err(_elapsed) => Err(anyhow::anyhow!("the health rules' action timed out after {}s (broker call hung)", timeout.as_secs())),
    };
    state.health.lock().await.in_flight.remove(&project.project_id);
    let mut given_up = Vec::new();
    match result {
        Ok(command) => {
            // Queued: the rule counts as fired while its command runs. A
            // look that finds the command failed, or past the rule's
            // timeout from now, takes that back and tries again.
            for (on, rule) in &fire.rules {
                let row = rows.entry(on.clone()).or_insert_with(|| unarmed(on));
                let deadline_unix = now.saturating_add(i64::from(rule.timeout_seconds.get()));
                *row = HealthRuleState {
                    fired: true,
                    queued: Some(QueuedHealthCommand { command, deadline_unix }),
                    took_down: row.took_down,
                    consecutive_failures: row.consecutive_failures,
                    last_error: row.last_error.clone(),
                    ..unarmed(on)
                };
            }
        }
        Err(e) => {
            let error = format!("{e:#}");
            for (on, _) in &fire.rules {
                let row = rows.entry(on.clone()).or_insert_with(|| unarmed(on));
                given_up.extend(failed(project.project_id, on, row, &error, seen_of(&on.copy), now));
            }
        }
    }
    if !given_up.is_empty() {
        crate::give_up(state, project.project_id, &given_up.join("; ")).await;
    }
    gave_up(rows)
}

/// Count one failure of `on`'s action (`row`, its state) with `error`: it
/// tries again after its backoff, or, at the last failure, gives up on
/// what its copy's units looked like (`seen`), and says so (for
/// `crate::give_up`).
fn failed(project: Uuid, on: &RuleOn, row: &mut HealthRuleState, error: &str, seen: serde_json::Value, now: i64) -> Option<String> {
    row.consecutive_failures += 1;
    row.last_error = Some(error.to_string());
    match weft_broker_client::protocol::supervisor_retry_after(row.consecutive_failures) {
        None => {
            row.next_retry_at_unix = None;
            row.gave_up_at_unix = Some(now);
            row.gave_up_seen = Some(seen);
            Some(format!(
                "its health rule '{}' on {} failed {} times: {error}",
                on.rule,
                copy_name(&on.copy),
                row.consecutive_failures
            ))
        }
        Some(delay) => {
            let delay = delay.as_secs() as i64;
            row.next_retry_at_unix = Some(now + delay);
            tracing::warn!(target: "weft_infra_supervisor::health", project_id = %project, rule = %on.rule, node_id = %on.copy.node_id, %error, retry_in_secs = delay, "a health rule's action failed; it tries again");
            None
        }
    }
}

/// A copy as a person reads it: `'db'`, or `'db' of instance 'ada'`.
fn copy_name(copy: &InfraCopy) -> String {
    match &copy.instance {
        None => format!("'{}'", copy.node_id),
        Some(instance) => format!("'{}' of instance '{instance}'", copy.node_id),
    }
}

/// A rule's state on a copy with nothing fired and nothing failed.
fn unarmed(on: &RuleOn) -> HealthRuleState {
    HealthRuleState {
        copy: on.copy.clone(),
        rule: on.rule.clone(),
        fired: false,
        queued: None,
        took_down: false,
        consecutive_failures: 0,
        next_retry_at_unix: None,
        gave_up_at_unix: None,
        gave_up_seen: None,
        last_error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A project is reported once per sync with each step's latest error:
    /// failed when any step failed (tried again), given up when only a
    /// rule gave up. A report that did not go through is reported
    /// again, but never over what happened since: a newer error of a
    /// step, a look that settled the project, or a trouble that keeps a
    /// project it reported settled from being settled.
    #[test]
    fn a_report_carries_each_project_once_and_what_happened_since_wins() {
        let (a, b, c, d) = (Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4));
        let mut registry = HealthRegistry::default();
        registry.troubled(a, Step::Rule, "the rule gave up");
        registry.troubled(a, Step::Sweep, "the sweep failed");
        registry.troubled(b, Step::Rule, "the rule gave up");
        registry.settled.extend([c, d]);
        let report = registry.take_report();
        let trouble = |project, error: &str| ProjectTrouble { project_id: project, error: error.into() };
        assert_eq!(report.failed(), vec![trouble(a, "the sweep failed; the rule gave up")]);
        assert_eq!(report.gave_up(), vec![trouble(b, "the rule gave up")]);
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
            vec![trouble(a, "the sweep failed again; the rule gave up"), trouble(d, "the sweep failed")],
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
}
