//! Pure state machine driving the supervisor's flaky/recovered
//! detection and which health rules fire.
//!
//! Split off from `health.rs` so the windowed transitions can be
//! tested deterministically without spinning a real supervisor +
//! broker + host + tokio runtime. The shape:
//!
//!   inputs           pure fn                outputs
//!   ─────────  ───────────────────────  ───────────────
//!   the unit's row (latch, window) +
//!   unit ready or not      ──> evaluate_node_health  ──> NodeDecision
//!   now (unix seconds)
//!
//!   rules fired so far     +
//!   each seen copy's units
//!   and its node's rules   ──> next_fire             ──> Option<Fire>
//!
//! Tests poke values straight into these functions, no fakes
//! required. The I/O glue in `health.rs` is then a thin caller that
//! asks the host how each unit is doing, calls these, dispatches the
//! resulting events.

use std::collections::BTreeSet;
use std::time::Duration;

use weft_broker_client::protocol::{InfraCopy, LifecycleSpec, RestoreReaders, TakeDownReaders};
use weft_core::infra::health::{CopyReading, HealthAction, HealthRule, UnitReading};

/// Default windows. Public so callers can override per-test or
/// per-instance without forking the engine.
pub const FLAKY_AFTER: Duration = Duration::from_secs(30);
pub const RECOVERY_AFTER: Duration = Duration::from_secs(30);

/// One unit's health latch and the window it is in, as its row carries it
/// (`UnitRuntime`): the row is the only place it lives, so any process
/// that looks next carries on from where the last look left it, and the
/// broker knows from it when a window ends (the supervisor's next pass).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NodeHealthState {
    /// Latched: true after we declared flaky, false after we
    /// declared recovered. Flips via the windowed transitions. The row's
    /// `flaky` status is this latch.
    pub declared_flaky: bool,
    /// Since when (unix seconds) a unit not declared flaky has been seen
    /// not ready.
    pub not_ready_since_unix: Option<i64>,
    /// Since when (unix seconds) a unit declared flaky has been seen ready
    /// again.
    pub ready_since_unix: Option<i64>,
}

impl NodeHealthState {
    /// The latch a unit's row carries. The caller never evaluates a unit
    /// outside {Running, Flaky}.
    pub fn of(unit: &weft_broker_client::protocol::UnitRuntime) -> Self {
        Self {
            declared_flaky: unit.status == weft_broker_client::protocol::InfraNodeStatus::Flaky,
            not_ready_since_unix: unit.not_ready_since_unix,
            ready_since_unix: unit.ready_since_unix,
        }
    }

    /// Whether this latch has seen the unit's workload: broken, or in a
    /// window.
    fn has_seen(&self) -> bool {
        self.declared_flaky || self.not_ready_since_unix.is_some() || self.ready_since_unix.is_some()
    }
}

/// Pure: one unit's decision from what this look saw of it (`None`: the
/// host does not report the unit). A unit the host does not report is
/// UNKNOWN (`None` back) while nothing says it should be there: a copy
/// applied less than `flaky_after` ago (`applied_at_unix`) may not be
/// reported yet. One seen broken or in a window before, or whose copy was
/// applied longer ago than that (its apply waited for it to be ready),
/// reads as not ready.
pub fn observe_unit(
    prior: NodeHealthState,
    observed: Option<NodeObservation>,
    applied_at_unix: Option<i64>,
    now_unix: i64,
    flaky_after: Duration,
    recovery_after: Duration,
) -> Option<NodeDecision> {
    let settled = applied_at_unix.is_some_and(|at| now_unix.saturating_sub(at) >= secs(flaky_after));
    let observation = match observed {
        Some(observation) => observation,
        None if prior.has_seen() || settled => NodeObservation { ready: false },
        None => return None,
    };
    Some(evaluate_node_health(prior, observation, now_unix, flaky_after, recovery_after))
}

/// What `evaluate_node_health` decided this tick.
///
/// Three pieces:
///   - `next`: the new latch and window, written to the unit's row when
///     they differ from what it carries.
///   - `desired_status`: what the row SHOULD say, derived from the
///     latch. The I/O layer compares this to the observed row
///     and writes only when they differ. Sibling writes (e.g.
///     `set_applied` flipping the row to Running while we think
///     it's Flaky) drift on a single tick; this carries the
///     "what's correct" answer so reconciliation falls out for
///     free in both directions.
///   - `event`: the event-of-record for this tick. `Some` only on
///     edges (`Flaky` / `Recovered`); `None` means "no edge, just
///     reconcile if the row drifted."
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDecision {
    pub next: NodeHealthState,
    pub desired_status: weft_broker_client::protocol::InfraNodeStatus,
    pub event: Option<NodeEdgeEvent>,
}

/// Edge events the supervisor publishes to `infra_event` on
/// transitions. Distinct from `NodeDecision.desired_status` which
/// the row-status reconciliation reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeEdgeEvent {
    BecameFlaky,
    Recovered,
}

/// What the caller observed for one unit this tick.
#[derive(Debug, Clone, Copy)]
pub struct NodeObservation {
    /// The host reports the unit running and every readiness check
    /// passing.
    pub ready: bool,
}

/// Pure: given the unit's latch + this tick's observation + now (unix
/// seconds), compute the next latch and whether anything happened.
///
/// A unit not declared flaky turns flaky once it has been seen not ready
/// for `flaky_after`, counted from the first look that saw it not ready;
/// a flaky one recovers once it has been seen ready for `recovery_after`,
/// counted from the first look that saw it ready again. Any reading the
/// other way restarts the window. A unit a look runs on is `running` or
/// `flaky`, which its apply wrote only once it was ready.
pub fn evaluate_node_health(
    prior: NodeHealthState,
    observation: NodeObservation,
    now_unix: i64,
    flaky_after: Duration,
    recovery_after: Duration,
) -> NodeDecision {
    use weft_broker_client::protocol::InfraNodeStatus;
    // The window this reading runs: since when the unit has read against
    // its latch, and how long that must last for the latch to flip.
    let (since, window) = match (prior.declared_flaky, observation.ready) {
        (true, true) => (prior.ready_since_unix, recovery_after),
        (false, false) => (prior.not_ready_since_unix, flaky_after),
        // Reading as latched: no window runs.
        _ => (None, Duration::ZERO),
    };
    let reads_against = prior.declared_flaky == observation.ready;
    let since = since.unwrap_or(now_unix);
    let flips = reads_against && now_unix.saturating_sub(since) >= secs(window);
    let declared_flaky = prior.declared_flaky != flips;
    let next = match (flips, reads_against, declared_flaky) {
        (false, true, true) => NodeHealthState { declared_flaky, not_ready_since_unix: None, ready_since_unix: Some(since) },
        (false, true, false) => NodeHealthState { declared_flaky, not_ready_since_unix: Some(since), ready_since_unix: None },
        _ => NodeHealthState { declared_flaky, not_ready_since_unix: None, ready_since_unix: None },
    };
    let event = flips.then_some(if declared_flaky { NodeEdgeEvent::BecameFlaky } else { NodeEdgeEvent::Recovered });

    // Desired status derived from the LATCH, not from the
    // instantaneous observation. The latch only flips on a window
    // expiry; a single bad reading on a tick doesn't drag
    // the row to Flaky, and a single good reading doesn't drag
    // it back to Running. This is what the windows are FOR.
    let desired_status = if next.declared_flaky {
        InfraNodeStatus::Flaky
    } else {
        InfraNodeStatus::Running
    };

    NodeDecision { next, desired_status, event }
}

/// Pure: the restart this look saw of a unit (`seen`, the host's latest
/// one) that its row does not hold yet (`recorded`), to write onto the
/// unit once; `None` when there is none or the row holds it already. A
/// restart is told from the next by when it stopped, so a cause the host
/// reads differently at a later look (its memory of why is gone) does not
/// rewrite it, and nothing is written at the looks in between.
pub fn restart_to_record(
    seen: Option<&weft_platform_traits::UnitRestart>,
    recorded: Option<&weft_core::infra::LastRestart>,
) -> Option<weft_core::infra::LastRestart> {
    let seen = seen?;
    if recorded.is_some_and(|r| r.at_unix == seen.stopped_at_unix) {
        return None;
    }
    Some(weft_core::infra::LastRestart { at_unix: seen.stopped_at_unix, cause: seen.cause.clone() })
}

fn secs(d: Duration) -> i64 {
    i64::try_from(d.as_secs()).unwrap_or(i64::MAX)
}

/// One copy as this look saw it: whose copy, the rules its node runs
/// (`weft_core::infra::health::rules_of`), and its units the host
/// reported that are expected to run now.
#[derive(Debug, Clone)]
pub struct CopyView {
    pub copy: InfraCopy,
    pub rules: Vec<HealthRule>,
    pub units: Vec<UnitReading>,
}

/// What the rules are evaluated on: every copy with a unit the host
/// reported (a copy it has not reported is neither broken nor healthy,
/// so no rule reads it), and the project's listening.
#[derive(Debug, Clone)]
pub struct RuleInputs {
    pub copies: Vec<CopyView>,
    /// The project's listening reads `active`.
    pub listening: bool,
    /// Something the health check took down is still down.
    pub taken_down_by_health: bool,
}

impl RuleInputs {
    fn reading<'a>(&self, copy: &'a CopyView) -> CopyReading<'a> {
        CopyReading { units: &copy.units, listening: self.listening, taken_down_by_health: self.taken_down_by_health }
    }

    /// The units this look saw of `copy`: none when it saw none.
    pub fn units_of(&self, copy: &InfraCopy) -> &[UnitReading] {
        self.copies.iter().find(|c| c.copy == *copy).map(|c| c.units.as_slice()).unwrap_or_default()
    }

    /// The rule `on` names, as its copy's node runs it, and whether it
    /// holds for the copy now. `None` when this look did not see the copy,
    /// or its node no longer runs a rule of that name.
    pub fn rule_on(&self, on: &RuleOn) -> Option<(&HealthRule, bool)> {
        let copy = self.copies.iter().find(|c| c.copy == on.copy)?;
        let rule = copy.rules.iter().find(|r| r.name == on.rule)?;
        Some((rule, rule.when.holds(&self.reading(copy))))
    }

    /// Whether a bring-back rule of `copy`'s node holds for it now: its
    /// node's own word that the copy came back.
    pub fn brought_back(&self, copy: &InfraCopy) -> bool {
        self.copies.iter().find(|c| c.copy == *copy).is_some_and(|c| {
            let reading = self.reading(c);
            c.rules.iter().any(|rule| rule.action == HealthAction::BringBack && rule.when.holds(&reading))
        })
    }
}

/// One rule on one copy: what its state is kept by. Two nodes' rules of
/// the same name, or one rule on two copies, are two of these.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuleOn {
    pub copy: InfraCopy,
    pub rule: String,
}

/// Pure: each copy's first rule, in its node's order, that holds for it
/// and has not fired for it (`fired`). A copy none holds for has none.
fn picks<'a>(inputs: &'a RuleInputs, fired: &BTreeSet<RuleOn>) -> Vec<(RuleOn, &'a HealthRule)> {
    inputs
        .copies
        .iter()
        .filter_map(|copy| {
            let reading = inputs.reading(copy);
            copy.rules
                .iter()
                .filter(|rule| rule.when.holds(&reading))
                .map(|rule| (RuleOn { copy: copy.copy.clone(), rule: rule.name.clone() }, rule))
                .find(|(on, _)| !fired.contains(on))
        })
        .collect()
}

/// What one look does: one action, for the rules (each on its copy) that
/// asked for it.
#[derive(Debug, Clone)]
pub struct Fire<'a> {
    pub action: HealthAction,
    pub rules: Vec<(RuleOn, &'a HealthRule)>,
}

impl Fire<'_> {
    /// How long queueing the action may take: the longest of its rules'
    /// timeouts.
    pub fn timeout(&self) -> Duration {
        let secs = self.rules.iter().map(|(_, rule)| rule.timeout_seconds.get()).max().unwrap_or_default();
        Duration::from_secs(u64::from(secs))
    }

    /// The lifecycle command the action enqueues for the dispatcher (the
    /// supervisor has no trigger rows of its own):
    ///
    /// - a take-down aims at exactly the copies whose rule fired: the
    ///   dispatcher takes down the live triggers whose cut contains one;
    /// - a bring-back names the copies still broken ([`still_broken`],
    ///   `took_down` being the copies whose take-down took something down
    ///   or is queued): the dispatcher brings back what the health check
    ///   took down whose cut contains none of them.
    pub fn plan(&self, inputs: &RuleInputs, took_down: &BTreeSet<InfraCopy>) -> LifecycleSpec {
        match self.action {
            HealthAction::TakeDownTriggers => {
                let broken: BTreeSet<InfraCopy> = self.rules.iter().map(|(on, _)| on.copy.clone()).collect();
                LifecycleSpec::Deactivate(TakeDownReaders { broken: broken.into_iter().collect() })
            }
            HealthAction::BringBack => LifecycleSpec::Reactivate(RestoreReaders {
                still_broken: still_broken(inputs, took_down).into_iter().collect(),
            }),
        }
    }
}

/// Pure: the action this look fires, or `None`. Every copy's pick
/// ([`picks`]) whose rule is not `waiting` (backing off after a failure,
/// or given up) joins it: the take-downs first, as one action over every
/// copy whose take-down holds; else the bring-backs, as one.
pub fn next_fire<'a>(inputs: &'a RuleInputs, fired: &BTreeSet<RuleOn>, waiting: impl Fn(&RuleOn) -> bool) -> Option<Fire<'a>> {
    let picks: Vec<(RuleOn, &HealthRule)> = picks(inputs, fired).into_iter().filter(|(on, _)| !waiting(on)).collect();
    [HealthAction::TakeDownTriggers, HealthAction::BringBack].into_iter().find_map(|action| {
        let rules: Vec<(RuleOn, &HealthRule)> = picks.iter().filter(|(_, rule)| rule.action == action).cloned().collect();
        (!rules.is_empty()).then_some(Fire { action, rules })
    })
}

/// Pure: the seen copies a bring-back keeps the readers of down: those
/// no bring-back rule of their node holds for, when either their node
/// runs one (its own rules say the copy has not come back) or the copy is
/// in `took_down` (a take-down of it took something down, or is queued,
/// and nothing brought the copy back since). So a copy whose node runs no
/// bring-back rule keeps down what its own take-down took down, until a
/// person brings it back, and never holds back a trigger another copy's
/// rule took down: however it is doing, nothing asked it to.
pub fn still_broken(inputs: &RuleInputs, took_down: &BTreeSet<InfraCopy>) -> BTreeSet<InfraCopy> {
    inputs
        .copies
        .iter()
        .filter(|copy| {
            let has_bring_back = copy.rules.iter().any(|rule| rule.action == HealthAction::BringBack);
            (has_bring_back || took_down.contains(&copy.copy)) && !inputs.brought_back(&copy.copy)
        })
        .map(|copy| copy.copy.clone())
        .collect()
}

/// Pure: forget every fired rule that no longer holds for its copy (its
/// copy unseen, or the rule gone from its node, included), so the copy's
/// next episode fires it again.
pub fn rearm(fired: &mut BTreeSet<RuleOn>, inputs: &RuleInputs) {
    fired.retain(|on| inputs.rule_on(on).is_some_and(|(_, holds)| holds));
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::protocol::InfraNodeStatus as Status;
    use weft_core::infra::health::{built_in_rules, HealthCondition, DEFAULT_RULE_TIMEOUT_SECONDS};

    const T: i64 = 1_000_000;

    fn obs(ready: bool) -> NodeObservation {
        NodeObservation { ready }
    }

    fn eval(prior: NodeHealthState, ready: bool, at: i64) -> NodeDecision {
        evaluate_node_health(prior, obs(ready), at, FLAKY_AFTER, RECOVERY_AFTER)
    }

    fn flaky() -> NodeHealthState {
        NodeHealthState { declared_flaky: true, ..NodeHealthState::default() }
    }

    // ---------- the latch read from the row ----------

    /// The row's status is the latch and its stamps are the window, so a
    /// process that never looked at the unit carries on where the last
    /// look left it.
    #[test]
    fn the_latch_is_read_from_the_row() {
        let unit = weft_broker_client::protocol::UnitRuntime {
            status: Status::Flaky,
            stop_behavior: weft_core::StopBehavior::Stop,
            flaky_after_seconds: 30,
            recovery_after_seconds: 30,
            image_refs: Default::default(),
            not_ready_since_unix: None,
            ready_since_unix: Some(T),
            last_restart: None,
        };
        assert_eq!(
            NodeHealthState::of(&unit),
            NodeHealthState { declared_flaky: true, not_ready_since_unix: None, ready_since_unix: Some(T) }
        );
        // Ready since T: recovered once the window from T has passed,
        // whoever looks.
        assert_eq!(eval(NodeHealthState::of(&unit), true, T + 29).event, None);
        assert_eq!(eval(NodeHealthState::of(&unit), true, T + 30).event, Some(NodeEdgeEvent::Recovered));
    }

    // ---------- no edge ----------

    #[test]
    fn ready_and_running_needs_no_window() {
        let result = eval(NodeHealthState::default(), true, T);
        assert!(result.event.is_none());
        assert_eq!(result.desired_status, Status::Running);
        assert_eq!(result.next, NodeHealthState::default());
    }

    /// The first look that sees it not ready starts the window; a look
    /// inside it keeps the window's start; a ready look ends it.
    #[test]
    fn briefly_not_ready_is_not_flaky() {
        let r1 = eval(NodeHealthState::default(), false, T);
        assert!(r1.event.is_none());
        assert_eq!(r1.desired_status, Status::Running);
        assert_eq!(r1.next.not_ready_since_unix, Some(T));
        let r2 = eval(r1.next, false, T + 5);
        assert_eq!(r2.next.not_ready_since_unix, Some(T), "the window keeps its start");
        let r3 = eval(r2.next, true, T + 10);
        assert_eq!(r3.next, NodeHealthState::default(), "ready again ends the window");
    }

    // ---------- BecameFlaky edge ----------

    #[test]
    fn not_ready_past_flaky_window_becomes_flaky() {
        let state = NodeHealthState { not_ready_since_unix: Some(T), ..NodeHealthState::default() };
        let result = eval(state, false, T + 31);
        assert_eq!(result.event, Some(NodeEdgeEvent::BecameFlaky));
        assert_eq!(result.next, flaky());
        assert_eq!(result.desired_status, Status::Flaky);
    }

    #[test]
    fn already_flaky_does_not_re_fire_event_but_desired_stays_flaky() {
        let result = eval(flaky(), false, T + 41);
        // No edge event : we're already past the transition.
        assert!(result.event.is_none());
        // But desired_status is Flaky (latch is still set). This is
        // what drives the I/O-layer reconciliation: even on
        // NoChange ticks, the row should stay (or return to) Flaky.
        assert_eq!(result.desired_status, Status::Flaky);
        assert_eq!(result.next, flaky());
    }

    // ---------- Recovered edge ----------

    #[test]
    fn flaky_then_ready_past_recovery_window_recovers() {
        let state = NodeHealthState { ready_since_unix: Some(T), ..flaky() };
        let result = eval(state, true, T + 40);
        assert_eq!(result.event, Some(NodeEdgeEvent::Recovered));
        assert_eq!(result.next, NodeHealthState::default());
        assert_eq!(result.desired_status, Status::Running);
    }

    /// A flaky unit briefly ready stays flaky, and a reading not ready
    /// again restarts its recovery window.
    #[test]
    fn flaky_then_briefly_ready_does_not_recover() {
        let r1 = eval(flaky(), true, T);
        assert!(r1.event.is_none());
        assert_eq!(r1.next.ready_since_unix, Some(T));
        // Desired status stays Flaky: row should NOT flip back
        // until the full recovery window elapses.
        assert_eq!(r1.desired_status, Status::Flaky);
        let r2 = eval(r1.next, false, T + 5);
        assert_eq!(r2.next, flaky(), "not ready again: the window starts over");
    }

    #[test]
    fn recovery_window_zero_means_first_ready_recovers() {
        let result = evaluate_node_health(flaky(), obs(true), T, FLAKY_AFTER, Duration::from_secs(0));
        assert_eq!(result.event, Some(NodeEdgeEvent::Recovered));
    }

    // ---------- full lifecycle ----------

    #[test]
    fn full_lifecycle_ready_flaky_recovered() {
        let mut s = NodeHealthState::default();
        // ready at T, not ready from T+5: flaky once that is 30 s old.
        for (ready, at, edge) in [
            (true, T, None),
            (false, T + 5, None),
            (false, T + 34, None),
            (false, T + 35, Some(NodeEdgeEvent::BecameFlaky)),
            (true, T + 45, None),
            (true, T + 75, Some(NodeEdgeEvent::Recovered)),
        ] {
            let r = eval(s, ready, at);
            assert_eq!(r.event, edge, "at T+{}", at - T);
            s = r.next;
        }
    }

    // ---------- restart_to_record: once per restart ----------

    /// A restart is written the first time a look sees it, and never at
    /// the looks after, even when the host no longer knows why it
    /// happened; the next one is written again.
    #[test]
    fn a_restart_is_recorded_once() {
        use weft_core::infra::{LastRestart, RestartCause};
        let seen = |at: i64, cause: RestartCause| weft_platform_traits::UnitRestart { container: "pg".into(), stopped_at_unix: at, cause, count: 1 };
        let oom = LastRestart { at_unix: T, cause: RestartCause::OutOfMemory };
        assert_eq!(restart_to_record(None, None), None, "no restart");
        assert_eq!(restart_to_record(None, Some(&oom)), None, "a restart the host no longer reports stays as recorded");
        assert_eq!(restart_to_record(Some(&seen(T, RestartCause::OutOfMemory)), None), Some(oom.clone()), "first seen");
        assert_eq!(restart_to_record(Some(&seen(T, RestartCause::OutOfMemory)), Some(&oom)), None, "seen again");
        assert_eq!(restart_to_record(Some(&seen(T, RestartCause::Unknown)), Some(&oom)), None, "the same restart, its cause forgotten");
        assert_eq!(
            restart_to_record(Some(&seen(T + 60, RestartCause::Exited { code: 1 })), Some(&oom)),
            Some(LastRestart { at_unix: T + 60, cause: RestartCause::Exited { code: 1 } }),
            "the next one"
        );
    }

    // ---------- observe_unit: unknown vs vanished ----------

    /// A unit the host does not report is unknown while nothing says it
    /// should be there: no decision. Once it was seen in a window or
    /// broken, or its copy was applied longer ago than its flaky window,
    /// its absence reads as not ready.
    #[test]
    fn an_unseen_unit_is_unknown_and_a_vanished_one_is_not_ready() {
        let observe = |prior: NodeHealthState, applied: Option<i64>, at: i64| {
            observe_unit(prior, None, applied, at, FLAKY_AFTER, RECOVERY_AFTER)
        };
        assert_eq!(observe(NodeHealthState::default(), None, T), None, "never applied");
        assert_eq!(observe(NodeHealthState::default(), Some(T), T + 29), None, "applied inside its window");
        let gone = observe(NodeHealthState::default(), Some(T), T + 30).expect("applied long enough ago");
        assert_eq!(gone.next.not_ready_since_unix, Some(T + 30));
        let later = observe(gone.next, None, T + 60).expect("in a window");
        assert_eq!(later.event, Some(NodeEdgeEvent::BecameFlaky));
        let still = observe(flaky(), None, T).expect("broken is seen");
        assert_eq!(still.desired_status, Status::Flaky);
    }

    // ---------- the rules ----------

    const TAKE_DOWN: &str = "take-down-while-flaky";
    const BRING_BACK: &str = "bring-back-once-healthy";

    fn shared(node: &str) -> InfraCopy {
        InfraCopy { node_id: node.into(), instance: None }
    }

    fn of(node: &str, instance: &str) -> InfraCopy {
        InfraCopy { node_id: node.into(), instance: Some(weft_core::instance::InstanceId::new(instance).unwrap()) }
    }

    fn unit(name: &str, ready: bool, flaky: bool) -> UnitReading {
        UnitReading { unit: name.into(), ready, flaky }
    }

    /// A copy running the built-in rules, its one unit named after its
    /// node.
    fn copy(copy: InfraCopy, ready: bool, flaky: bool) -> CopyView {
        let units = vec![unit(&copy.node_id, ready, flaky)];
        CopyView { copy, rules: built_in_rules(), units }
    }

    fn inputs(copies: Vec<CopyView>) -> RuleInputs {
        RuleInputs { copies, listening: true, taken_down_by_health: false }
    }

    fn on(copy: &InfraCopy, rule: &str) -> RuleOn {
        RuleOn { copy: copy.clone(), rule: rule.into() }
    }

    fn fire(inputs: &RuleInputs, fired: &BTreeSet<RuleOn>) -> Option<(HealthAction, Vec<RuleOn>)> {
        next_fire(inputs, fired, |_| false).map(|f| (f.action, f.rules.into_iter().map(|(on, _)| on).collect()))
    }

    fn rule(name: &str, when: HealthCondition, action: HealthAction) -> HealthRule {
        HealthRule { name: name.into(), when, action, timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS }
    }

    /// The built-in take-down fires on a unit's latch, never on one
    /// reading, and aims at exactly the copy it holds for: ada's broken
    /// copy of `svc`, never bob's healthy one or the shared `db`.
    #[test]
    fn a_take_down_waits_for_the_latch_and_aims_at_its_copy() {
        let ada_svc = of("svc", "ada");
        let one_bad_reading = inputs(vec![copy(ada_svc.clone(), false, false), copy(of("svc", "bob"), true, false), copy(shared("db"), true, false)]);
        assert!(fire(&one_bad_reading, &BTreeSet::new()).is_none());
        let latched = inputs(vec![copy(ada_svc.clone(), false, true), copy(of("svc", "bob"), true, false), copy(shared("db"), true, false)]);
        let f = next_fire(&latched, &BTreeSet::new(), |_| false).expect("take down");
        assert_eq!(f.action, HealthAction::TakeDownTriggers);
        assert_eq!(f.plan(&latched, &BTreeSet::new()), LifecycleSpec::Deactivate(TakeDownReaders { broken: vec![ada_svc] }));
        let quiet = RuleInputs { listening: false, ..latched };
        assert!(fire(&quiet, &BTreeSet::new()).is_none(), "nothing listens: nothing to take down");
    }

    /// Copies that break in the same look go down in one action; a copy
    /// already taken down is not again, and a second copy breaking later
    /// is taken down alone.
    #[test]
    fn a_take_down_acts_once_per_copy() {
        let (ada, bob) = (of("svc", "ada"), of("svc", "bob"));
        let both = inputs(vec![copy(ada.clone(), false, true), copy(bob.clone(), false, true)]);
        assert_eq!(fire(&both, &BTreeSet::new()), Some((HealthAction::TakeDownTriggers, vec![on(&ada, TAKE_DOWN), on(&bob, TAKE_DOWN)])));
        let fired = BTreeSet::from([on(&ada, TAKE_DOWN)]);
        assert_eq!(fire(&both, &fired), Some((HealthAction::TakeDownTriggers, vec![on(&bob, TAKE_DOWN)])));
        let all = BTreeSet::from([on(&ada, TAKE_DOWN), on(&bob, TAKE_DOWN)]);
        assert!(fire(&both, &all).is_none());
    }

    /// Each copy runs its own node's rules over its own units: a node
    /// whose rule reads only its `primary` unit is not taken down for a
    /// flaky `sidecar`, while a node on the built-in rules is taken down
    /// for any of its units; two nodes' rules of one name are two states.
    #[test]
    fn each_copy_runs_its_own_nodes_rules() {
        let primary_only = vec![rule(
            TAKE_DOWN,
            HealthCondition::UnitFlaky { unit: Some("primary".into()) },
            HealthAction::TakeDownTriggers,
        )];
        let picky = CopyView { copy: shared("picky"), rules: primary_only, units: vec![unit("primary", true, false), unit("sidecar", false, true)] };
        let plain = CopyView { copy: shared("plain"), rules: built_in_rules(), units: vec![unit("primary", true, false), unit("sidecar", false, true)] };
        let look = inputs(vec![picky, plain]);
        assert_eq!(fire(&look, &BTreeSet::new()), Some((HealthAction::TakeDownTriggers, vec![on(&shared("plain"), TAKE_DOWN)])));
        let fired = BTreeSet::from([on(&shared("plain"), TAKE_DOWN)]);
        assert!(fire(&look, &fired).is_none(), "the other node's rule of that name is its own");
    }

    /// A node that runs no rules is never acted on, however broken.
    #[test]
    fn a_node_with_no_rules_is_left_alone() {
        let look = inputs(vec![CopyView { copy: shared("db"), rules: Vec::new(), units: vec![unit("db", false, true)] }]);
        assert!(fire(&look, &BTreeSet::new()).is_none());
    }

    /// The built-in bring-back fires only while the health check took
    /// something down, for every healthy copy at once, and names the
    /// copies still broken: here ada's, so her readers stay down.
    #[test]
    fn a_bring_back_names_the_copies_still_broken() {
        let (ada, bob) = (of("svc", "ada"), of("svc", "bob"));
        let nothing_down = inputs(vec![copy(ada.clone(), true, false), copy(bob.clone(), true, false)]);
        assert!(fire(&nothing_down, &BTreeSet::new()).is_none(), "nothing the health check took is down");

        let look = RuleInputs { taken_down_by_health: true, listening: false, ..inputs(vec![copy(ada.clone(), false, true), copy(bob.clone(), true, false), copy(shared("db"), true, false)]) };
        let f = next_fire(&look, &BTreeSet::new(), |_| false).expect("bring back");
        assert_eq!(f.action, HealthAction::BringBack);
        assert_eq!(f.rules.iter().map(|(on, _)| on.clone()).collect::<Vec<_>>(), vec![on(&bob, BRING_BACK), on(&shared("db"), BRING_BACK)]);
        assert_eq!(f.plan(&look, &BTreeSet::new()), LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![ada.clone()] }));

        // Fired for bob and db: it fires again once ada heals, for her.
        let fired = BTreeSet::from([on(&shared("db"), BRING_BACK), on(&bob, BRING_BACK)]);
        assert!(fire(&look, &fired).is_none());
        let healed = RuleInputs { copies: vec![copy(ada.clone(), true, false), copy(bob, true, false), copy(shared("db"), true, false)], ..look };
        assert_eq!(fire(&healed, &fired), Some((HealthAction::BringBack, vec![on(&ada, BRING_BACK)])));
        assert!(still_broken(&healed, &BTreeSet::new()).is_empty());
    }

    /// A take-down goes before a bring-back in the same look: the
    /// bring-back then still names the newly broken copy.
    #[test]
    fn a_take_down_goes_first() {
        let look = RuleInputs { taken_down_by_health: true, ..inputs(vec![copy(shared("db"), false, true), copy(shared("svc"), true, false)]) };
        assert_eq!(fire(&look, &BTreeSet::new()).map(|(action, _)| action), Some(HealthAction::TakeDownTriggers));
    }

    /// A copy whose node runs no bring-back rule holds down only what its
    /// own take-down took down (`took_down`): healthy or not, it never
    /// holds back a trigger another copy's rule took down. Here `db`
    /// (the built-in rules) recovered, `cache` runs no rules at all and
    /// `keep` only a take-down: neither holds anything down until a
    /// take-down of it took something down, and then only a person
    /// brings that back.
    #[test]
    fn a_copy_with_no_bring_back_holds_down_only_what_it_took_down() {
        let no_rules = CopyView { copy: shared("cache"), rules: Vec::new(), units: vec![unit("cache", false, true)] };
        let keep_down = CopyView {
            copy: shared("keep"),
            rules: vec![rule(TAKE_DOWN, HealthCondition::UnitFlaky { unit: None }, HealthAction::TakeDownTriggers)],
            units: vec![unit("keep", true, false)],
        };
        let look = RuleInputs { taken_down_by_health: true, ..inputs(vec![copy(shared("db"), true, false), no_rules, keep_down]) };
        let f = next_fire(&look, &BTreeSet::new(), |_| false).expect("db brings back");
        assert_eq!(f.rules.iter().map(|(on, _)| on.clone()).collect::<Vec<_>>(), vec![on(&shared("db"), BRING_BACK)]);
        assert_eq!(f.plan(&look, &BTreeSet::new()), LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![] }));
        let took_down = BTreeSet::from([shared("keep"), shared("db")]);
        assert_eq!(
            f.plan(&look, &took_down),
            LifecycleSpec::Reactivate(RestoreReaders { still_broken: vec![shared("keep")] }),
            "db came back by its own rule; keep has no way back but a person"
        );
    }

    /// A rule backing off or given up on a copy keeps that copy out of the
    /// action (and does not fall through to its next rule); the others
    /// still act.
    #[test]
    fn a_waiting_rule_holds_back_only_its_copy() {
        let (ada, bob) = (of("svc", "ada"), of("svc", "bob"));
        let both = inputs(vec![copy(ada.clone(), false, true), copy(bob.clone(), false, true)]);
        let f = next_fire(&both, &BTreeSet::new(), |o| *o == on(&ada, TAKE_DOWN)).expect("bob's goes on");
        assert_eq!(f.rules.into_iter().map(|(on, _)| on).collect::<Vec<_>>(), vec![on(&bob, TAKE_DOWN)]);
        assert!(next_fire(&both, &BTreeSet::new(), |_| true).is_none());
    }

    /// The action's timeout is the longest of its rules'.
    #[test]
    fn an_action_takes_as_long_as_its_slowest_rule() {
        let quick = rule("quick", HealthCondition::UnitFlaky { unit: None }, HealthAction::TakeDownTriggers);
        let slow = HealthRule { timeout_seconds: std::num::NonZeroU32::new(4000).unwrap(), ..quick.clone() };
        let look = inputs(vec![
            CopyView { copy: shared("a"), rules: vec![quick], units: vec![unit("a", false, true)] },
            CopyView { copy: shared("b"), rules: vec![slow], units: vec![unit("b", false, true)] },
        ]);
        assert_eq!(next_fire(&look, &BTreeSet::new(), |_| false).unwrap().timeout(), Duration::from_secs(4000));
    }

    /// Re-arming is per rule per copy: a take-down whose copy healed is
    /// forgotten (its next breakage fires), one still broken is kept; a
    /// bring-back is forgotten once nothing is down; an unseen copy's, or
    /// a rule its node no longer runs, is forgotten.
    #[test]
    fn rearm_forgets_what_no_longer_holds() {
        let (ada, bob) = (of("svc", "ada"), of("svc", "bob"));
        let mut fired = BTreeSet::from([
            on(&ada, TAKE_DOWN),
            on(&bob, TAKE_DOWN),
            on(&shared("db"), BRING_BACK),
            on(&shared("gone"), TAKE_DOWN),
            on(&bob, "renamed-since"),
        ]);
        let now = RuleInputs {
            taken_down_by_health: true,
            ..inputs(vec![copy(ada.clone(), true, false), copy(bob.clone(), false, true), copy(shared("db"), true, false)])
        };
        rearm(&mut fired, &now);
        assert_eq!(fired, BTreeSet::from([on(&bob, TAKE_DOWN), on(&shared("db"), BRING_BACK)]));
        let nothing_down = RuleInputs { taken_down_by_health: false, ..now };
        rearm(&mut fired, &nothing_down);
        assert_eq!(fired, BTreeSet::from([on(&bob, TAKE_DOWN)]));
    }
}
