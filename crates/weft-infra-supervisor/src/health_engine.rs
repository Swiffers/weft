//! Pure state machine driving the supervisor's flaky/recovered
//! detection and HealthProtocol matching.
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
//!   prior in_flight flag   +
//!   prior fired scopes     +
//!   seen units             ──> evaluate_protocols    ──> Option<ProtocolMatch>
//!   protocol list
//!
//! Tests poke values straight into these functions, no fakes
//! required. The I/O glue in `health.rs` is then a thin caller that
//! asks the host how each unit is doing, calls these, dispatches the
//! resulting events.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use weft_broker_client::protocol::InfraCopy;

use crate::protocol::{
    broken_copies, evaluate_condition, names_infra, still_broken_copies, ConditionContext, HealthProtocol, HealthProtocols,
    ProtocolAction, UnitView,
};

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

fn secs(d: Duration) -> i64 {
    i64::try_from(d.as_secs()).unwrap_or(i64::MAX)
}

/// What the protocols are evaluated on: every seen unit of every copy
/// expected to run now, and the project's listening.
#[derive(Debug, Clone)]
pub struct ProtocolEvalInputs {
    pub units: Vec<UnitView>,
    pub project_status: weft_broker_client::protocol::ProjectStatus,
    /// Is something the health loop took down still down? Gates
    /// `HealthCondition::HealthParked` (auto-recover).
    pub health_parked: bool,
}

impl ProtocolEvalInputs {
    fn context(&self) -> ConditionContext<'_> {
        ConditionContext { units: &self.units, project_status: self.project_status, health_parked: self.health_parked }
    }
}

/// What `evaluate_protocols` decided. None = no protocol fires this
/// tick. Some = the named protocol matched and the caller should
/// dispatch its action.
#[derive(Debug, Clone)]
pub struct ProtocolMatch<'a> {
    pub protocol: &'a HealthProtocol,
    /// Where the action lands, see [`protocol_scope`]: the copies it
    /// acts on, the copies still broken for an auto-recover, empty for a
    /// notification or a take-down of the whole project.
    pub scope: BTreeSet<InfraCopy>,
}

/// Pure: where `proto` holds this look. `None` when it does not hold
/// at all.
///
/// - An action on broken copies: the copies its condition finds broken
///   ([`broken_copies`], `None` rather than an empty set: such an action
///   with no copy has nothing to aim at); when the condition names no
///   infra, [`ProtocolAction::unowned_scope`] (the named node's shared
///   copy for a restart, the empty set, the whole project, for
///   a take-down).
/// - An auto-recover: the copies still broken ([`still_broken_copies`]),
///   when the condition holds with those set aside. Empty when nothing
///   is, and every reader the loop took comes back.
/// - A notification: the empty set.
pub fn protocol_scope(proto: &HealthProtocol, ctx: &ConditionContext<'_>) -> Option<BTreeSet<InfraCopy>> {
    if matches!(proto.action, ProtocolAction::AutoRecover) {
        let broken = still_broken_copies(&proto.when, ctx);
        let rest: Vec<UnitView> = ctx.units.iter().filter(|u| !broken.contains(&u.copy())).cloned().collect();
        return evaluate_condition(&proto.when, &ConditionContext { units: &rest, ..ctx.clone() }).then_some(broken);
    }
    if !evaluate_condition(&proto.when, ctx) {
        return None;
    }
    if !proto.action.aims_at_broken_copies() {
        return Some(BTreeSet::new());
    }
    if !names_infra(&proto.when) {
        return Some(proto.action.unowned_scope());
    }
    let broken = broken_copies(&proto.when, ctx);
    (!broken.is_empty()).then_some(broken)
}

/// Pure: what a protocol that already fired (`acted`, see
/// [`record_fire`]) still has to do where it holds now (`scope`), or
/// `None` when it has done it all.
///
/// - An auto-recover fires again once a copy that was broken when it
///   last fired, or broke since ([`rearm`]), is broken no longer, with
///   the copies still broken now.
/// - An action on copies acts only on the copies it has not acted on:
///   a second copy breaking after the first is restarted or taken
///   down alone, never the first one again.
/// - An action on no copy (a notification, a take-down of the whole
///   project) fired once for the episode.
fn still_to_do(proto: &HealthProtocol, acted: &BTreeSet<InfraCopy>, scope: BTreeSet<InfraCopy>) -> Option<BTreeSet<InfraCopy>> {
    if matches!(proto.action, ProtocolAction::AutoRecover) {
        return (!acted.is_subset(&scope)).then_some(scope);
    }
    let fresh: BTreeSet<InfraCopy> = scope.difference(acted).cloned().collect();
    (!fresh.is_empty()).then_some(fresh)
}

/// Pure: given the protocol list, what each already-fired protocol
/// acted on and still holds for (`fired`: name to the copies its action
/// aimed at, see [`rearm`]), the in-flight flag, and the inputs computed
/// from this tick, return the FIRST protocol that has something to do,
/// aimed at only that ([`still_to_do`]).
///
/// Skips:
///   - if the project has any protocol in-flight (returns None);
///   - protocols that do not hold ([`protocol_scope`]);
///   - a protocol already fired with nothing left to do.
pub fn evaluate_protocols<'a>(
    protocols: &'a HealthProtocols,
    fired: &HashMap<String, BTreeSet<InfraCopy>>,
    in_flight: bool,
    inputs: &ProtocolEvalInputs,
) -> Option<ProtocolMatch<'a>> {
    if in_flight {
        return None;
    }
    let ctx = inputs.context();
    for proto in &protocols.protocols {
        let Some(scope) = protocol_scope(proto, &ctx) else {
            continue;
        };
        let scope = match fired.get(&proto.name) {
            None => scope,
            Some(acted) => match still_to_do(proto, acted, scope) {
                Some(scope) => scope,
                None => continue,
            },
        };
        return Some(ProtocolMatch { protocol: proto, scope });
    }
    None
}

/// Pure: note that `proto` fired on `scope`. An action on copies adds
/// them to what it acted on; an auto-recover remembers the copies still
/// broken when it fired, so it fires again when one of them heals.
pub fn record_fire(fired: &mut HashMap<String, BTreeSet<InfraCopy>>, proto: &HealthProtocol, scope: &BTreeSet<InfraCopy>) {
    let acted = fired.entry(proto.name.clone()).or_default();
    if matches!(proto.action, ProtocolAction::AutoRecover) {
        *acted = scope.clone();
    } else {
        acted.extend(scope.iter().cloned());
    }
}

/// Pure: is every seen unit of the project healthy now: fully ready,
/// and none declared flaky?
pub fn all_units_healthy(units: &[UnitView]) -> bool {
    units.iter().all(|u| u.ready && !u.flaky)
}

/// Pure: re-arm what the protocols fired, copy by copy.
///
/// - An action on copies keeps, of those, only the ones it still holds
///   for ([`protocol_scope`]): a copy that healed is armed again, so its
///   next episode fires, while every other copy stays as it was.
/// - An auto-recover is forgotten once it no longer holds (nothing the
///   loop took is still down), and otherwise also counts every copy
///   broken now, so a copy that breaks after it fired is restored when
///   it heals.
/// - A notification re-arms once every seen unit of the project is
///   healthy; a take-down of the whole project once it no longer holds.
/// - A protocol no longer configured is forgotten.
pub fn rearm(fired: &mut HashMap<String, BTreeSet<InfraCopy>>, protocols: &HealthProtocols, inputs: &ProtocolEvalInputs) {
    let ctx = inputs.context();
    let healthy = all_units_healthy(&inputs.units);
    fired.retain(|name, acted| {
        let Some(proto) = protocols.protocols.iter().find(|p| p.name == *name) else {
            return false;
        };
        let holds = protocol_scope(proto, &ctx);
        match &proto.action {
            ProtocolAction::AutoRecover => match holds {
                Some(broken) => {
                    acted.extend(broken);
                    true
                }
                None => false,
            },
            ProtocolAction::Notify { .. } => !healthy,
            _ if acted.is_empty() => holds.is_some(),
            _ => {
                let holds = holds.unwrap_or_default();
                acted.retain(|copy| holds.contains(copy));
                !acted.is_empty()
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{HealthCondition, ProtocolAction};
    use weft_broker_client::protocol::InfraNodeStatus as Status;

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

    // ---------- evaluate_protocols ----------

    fn proto(name: &str, when: HealthCondition, action: ProtocolAction) -> HealthProtocol {
        HealthProtocol {
            name: name.to_string(),
            when,
            action,
            timeout_seconds: 1800,
        }
    }

    fn flaky_when() -> HealthCondition {
        HealthCondition::NodeNotReady { node_id: "*".into(), unit: "*".into() }
    }

    fn view(node: &str, instance: Option<&str>, ready: bool, flaky: bool) -> UnitView {
        UnitView {
            node_id: node.into(),
            instance: instance.map(|m| weft_core::instance::InstanceId::new(m).unwrap()),
            unit: node.into(),
            ready,
            flaky,
        }
    }

    fn inputs(units: Vec<UnitView>) -> ProtocolEvalInputs {
        ProtocolEvalInputs {
            units,
            project_status: weft_broker_client::protocol::ProjectStatus::Active,
            health_parked: false,
        }
    }

    fn inputs_with_ready(node: &str, ready: bool) -> ProtocolEvalInputs {
        inputs(vec![view(node, None, ready, false)])
    }

    fn fired(name: &str, scope: BTreeSet<InfraCopy>) -> HashMap<String, BTreeSet<InfraCopy>> {
        HashMap::from([(name.to_string(), scope)])
    }

    #[test]
    fn protocols_match_first_in_list_order() {
        let p = HealthProtocols {
            protocols: vec![
                proto("first", flaky_when(), ProtocolAction::AutoRecover),
                proto(
                    "second",
                    flaky_when(),
                    ProtocolAction::Notify {
                        channel: "ops".into(),
                    },
                ),
            ],
        };
        let inputs = inputs_with_ready("n1", false);
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs);
        assert_eq!(m.expect("match").protocol.name, "first");
    }

    #[test]
    fn protocols_skip_already_fired() {
        let p = HealthProtocols {
            protocols: vec![
                proto("first", flaky_when(), ProtocolAction::ParkTriggers),
                proto(
                    "second",
                    flaky_when(),
                    ProtocolAction::Notify {
                        channel: "ops".into(),
                    },
                ),
            ],
        };
        let inputs = inputs_with_ready("n1", false);
        let n1 = BTreeSet::from([InfraCopy { node_id: "n1".into(), instance: None }]);
        let m = evaluate_protocols(&p, &fired("first", n1), false, &inputs);
        assert_eq!(m.expect("match").protocol.name, "second");
    }

    #[test]
    fn protocols_no_match_when_in_flight() {
        let p = HealthProtocols {
            protocols: vec![proto("first", flaky_when(), ProtocolAction::AutoRecover)],
        };
        let inputs = inputs_with_ready("n1", false);
        let m = evaluate_protocols(&p, &HashMap::new(), true, &inputs);
        assert!(m.is_none());
    }

    #[test]
    fn protocols_no_match_when_condition_false() {
        let p = HealthProtocols {
            protocols: vec![proto("first", flaky_when(), ProtocolAction::AutoRecover)],
        };
        // Ready: `node_not_ready` is false.
        let inputs = inputs_with_ready("n1", true);
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs);
        assert!(m.is_none());
    }

    #[test]
    fn protocols_empty_list_no_match() {
        let p = HealthProtocols {
            protocols: vec![],
        };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs(Vec::new()));
        assert!(m.is_none());
    }

    /// The default park fires on a unit's latch, never on one reading:
    /// a copy not ready that is not declared flaky parks nothing; a
    /// declared-flaky one parks, aimed at exactly its copy.
    #[test]
    fn the_default_park_waits_for_the_latch_and_aims_at_the_broken_copy() {
        let p = crate::protocol::default_protocols();
        let one_bad_reading = inputs(vec![view("svc", Some("ada"), false, false), view("db", None, true, false)]);
        assert!(evaluate_protocols(&p, &HashMap::new(), false, &one_bad_reading).is_none());

        let latched = inputs(vec![view("svc", Some("ada"), false, true), view("db", None, true, false)]);
        let m = evaluate_protocols(&p, &HashMap::new(), false, &latched).expect("park");
        assert_eq!(m.protocol.name, "park-while-infra-broken");
        let ada_svc = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("ada").unwrap()) };
        assert_eq!(m.scope, BTreeSet::from([ada_svc.clone()]));

        // Fired on ada's copy: the same breakage does not fire again, a
        // second copy breaking does, aimed at that copy alone.
        let acted = fired("park-while-infra-broken", BTreeSet::from([ada_svc.clone()]));
        assert!(evaluate_protocols(&p, &acted, false, &latched).is_none());
        let both = inputs(vec![view("svc", Some("ada"), false, true), view("db", None, false, true)]);
        let m = evaluate_protocols(&p, &acted, false, &both).expect("the new breakage parks");
        assert_eq!(m.scope, BTreeSet::from([InfraCopy { node_id: "db".into(), instance: None }]));
    }

    /// A restart fires once per copy: ada's broken copy is restarted, and
    /// when bob's breaks after it, only bob's is.
    #[test]
    fn a_protocol_acts_once_per_copy() {
        let p = HealthProtocols {
            protocols: vec![proto(
                "restart",
                HealthCondition::NodeFlaky { node_id: "svc".into(), unit: "*".into() },
                ProtocolAction::RestartUnit { node_id: "svc".into(), unit: "svc".into() },
            )],
        };
        let ada = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("ada").unwrap()) };
        let bob = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("bob").unwrap()) };
        let mut acted = HashMap::new();
        let first = inputs(vec![view("svc", Some("ada"), false, true), view("svc", Some("bob"), true, false)]);
        let m = evaluate_protocols(&p, &acted, false, &first).expect("ada's copy breaks");
        assert_eq!(m.scope, BTreeSet::from([ada.clone()]));
        record_fire(&mut acted, m.protocol, &m.scope);
        let then = inputs(vec![view("svc", Some("ada"), false, true), view("svc", Some("bob"), false, true)]);
        rearm(&mut acted, &p, &then);
        let m = evaluate_protocols(&p, &acted, false, &then).expect("bob's copy breaks after");
        assert_eq!(m.scope, BTreeSet::from([bob]), "ada's copy is not restarted again");
        record_fire(&mut acted, m.protocol, &m.scope);
        assert!(evaluate_protocols(&p, &acted, false, &then).is_none());
    }

    /// A restart on a broken shared copy aims at that shared copy.
    #[test]
    fn a_restart_on_a_broken_shared_copy_aims_at_it() {
        let p = HealthProtocols {
            protocols: vec![proto(
                "restart",
                flaky_when(),
                ProtocolAction::RestartUnit { node_id: "svc".into(), unit: "svc".into() },
            )],
        };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &inputs(vec![view("svc", None, false, false)]))
            .expect("the shared copy is not ready");
        assert_eq!(m.scope, BTreeSet::from([InfraCopy { node_id: "svc".into(), instance: None }]));
    }

    /// A protocol whose condition names no infra acts where it did
    /// before copies had owners: a restart of the named node's shared
    /// copy, a take-down on the whole project, each once while it
    /// holds, and again after it stopped holding.
    #[test]
    fn a_protocol_with_no_unit_condition_acts_as_before() {
        use weft_broker_client::protocol::ProjectStatus;
        let active = HealthCondition::ProjectStatusEq { status: ProjectStatus::Active };
        let restart = HealthProtocols {
            protocols: vec![proto(
                "restart",
                active.clone(),
                ProtocolAction::RestartUnit { node_id: "svc".into(), unit: "svc".into() },
            )],
        };
        let m = evaluate_protocols(&restart, &HashMap::new(), false, &inputs(Vec::new())).expect("fires");
        assert_eq!(m.scope, BTreeSet::from([InfraCopy { node_id: "svc".into(), instance: None }]));

        let park = HealthProtocols { protocols: vec![proto("park", active, ProtocolAction::ParkTriggers)] };
        let mut acted = HashMap::new();
        let m = evaluate_protocols(&park, &acted, false, &inputs(Vec::new())).expect("fires on the whole project");
        assert!(m.scope.is_empty());
        record_fire(&mut acted, m.protocol, &m.scope);
        rearm(&mut acted, &park, &inputs(Vec::new()));
        assert!(evaluate_protocols(&park, &acted, false, &inputs(Vec::new())).is_none(), "once while it holds");
        let inactive = ProtocolEvalInputs { project_status: ProjectStatus::Inactive, ..inputs(Vec::new()) };
        rearm(&mut acted, &park, &inactive);
        assert!(acted.is_empty(), "re-armed once it stopped holding");
    }

    /// Auto-recover reads each copy on its own and names the copies
    /// still broken: while ada's copy is flaky it fires with that copy
    /// (everything else comes back), and only when the health loop parked
    /// something.
    #[test]
    fn the_default_recover_names_the_copies_still_broken() {
        let p = crate::protocol::default_protocols();
        let ada_svc = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("ada").unwrap()) };
        let healthy_parked = ProtocolEvalInputs { health_parked: true, ..inputs(vec![view("svc", None, true, false)]) };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &healthy_parked).expect("recover");
        assert_eq!(m.protocol.name, "auto-recover-when-infra-healthy");
        assert!(m.scope.is_empty(), "nothing is still broken");

        let parked_svc = fired("park-while-infra-broken", BTreeSet::from([ada_svc.clone()]));
        let one_still_flaky = ProtocolEvalInputs {
            health_parked: true,
            ..inputs(vec![view("svc", None, true, false), view("svc", Some("ada"), false, true)])
        };
        let m = evaluate_protocols(&p, &parked_svc, false, &one_still_flaky).expect("the rest recovers");
        assert_eq!(m.protocol.name, "auto-recover-when-infra-healthy");
        assert_eq!(m.scope, BTreeSet::from([ada_svc]));

        let nothing_parked = inputs(vec![view("svc", None, true, false)]);
        assert!(evaluate_protocols(&p, &HashMap::new(), false, &nothing_parked).is_none());
    }

    /// A shared-only project whose triggers read only copies the loop
    /// cannot see: nothing is known broken, so a recovery fires with an
    /// empty still-broken set and every reader the loop took comes back.
    #[test]
    fn a_recovery_with_no_seen_copy_restores_everything() {
        let p = crate::protocol::default_protocols();
        let parked = ProtocolEvalInputs { health_parked: true, ..inputs(Vec::new()) };
        let m = evaluate_protocols(&p, &HashMap::new(), false, &parked).expect("recover");
        assert_eq!(m.protocol.name, "auto-recover-when-infra-healthy");
        assert!(m.scope.is_empty());
    }

    /// Re-arming is per copy: a take-down forgets a copy once it healed
    /// and keeps the one still broken; a recovery fires again when a copy
    /// broken at its fire, or broken since, heals, and is forgotten once
    /// nothing it took is down; a notification waits for the whole
    /// project.
    #[test]
    fn rearm_is_copy_by_copy() {
        let p = crate::protocol::default_protocols();
        let ada = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("ada").unwrap()) };
        let bob = InfraCopy { node_id: "svc".into(), instance: Some(weft_core::instance::InstanceId::new("bob").unwrap()) };
        let recover = &p.protocols[1];
        let mut acted = HashMap::from([
            ("park-while-infra-broken".to_string(), BTreeSet::from([ada.clone(), bob.clone()])),
            ("gone-from-the-config".to_string(), BTreeSet::new()),
        ]);
        // The recovery fired while ada's copy was still broken.
        record_fire(&mut acted, recover, &BTreeSet::from([ada.clone()]));
        // Ada healed, bob is still flaky, and something is still parked.
        let now = ProtocolEvalInputs {
            health_parked: true,
            ..inputs(vec![view("svc", Some("ada"), true, false), view("svc", Some("bob"), false, true)])
        };
        rearm(&mut acted, &p, &now);
        assert_eq!(acted.get("park-while-infra-broken"), Some(&BTreeSet::from([bob.clone()])));
        assert!(!acted.contains_key("gone-from-the-config"));
        // Bob broke after the recovery fired: counted, so his healing fires it.
        assert_eq!(acted.get("auto-recover-when-infra-healthy"), Some(&BTreeSet::from([ada.clone(), bob.clone()])));
        let m = evaluate_protocols(&p, &acted, false, &now).expect("ada healed since it fired");
        assert_eq!(m.scope, BTreeSet::from([bob.clone()]));
        record_fire(&mut acted, m.protocol, &m.scope);
        assert!(evaluate_protocols(&p, &acted, false, &now).is_none(), "nothing healed since");

        // Nothing is parked any more: the recovery is forgotten.
        let done = inputs(vec![view("svc", Some("ada"), true, false), view("svc", Some("bob"), true, false)]);
        rearm(&mut acted, &p, &done);
        assert!(!acted.contains_key("auto-recover-when-infra-healthy"));

        let notify = HealthProtocols { protocols: vec![proto("tell", flaky_when(), ProtocolAction::Notify { channel: "ops".into() })] };
        let mut told = fired("tell", BTreeSet::new());
        rearm(&mut told, &notify, &inputs(vec![view("svc", None, false, false)]));
        assert!(told.contains_key("tell"), "not healthy yet");
        rearm(&mut told, &notify, &inputs(vec![view("svc", None, true, false)]));
        assert!(told.is_empty());
    }

    // ---------- all_units_healthy ----------

    #[test]
    fn healthy_means_every_unit_ready_and_none_flaky() {
        assert!(all_units_healthy(&[view("a", None, true, false), view("b", None, true, false)]));
        assert!(!all_units_healthy(&[view("a", None, true, false), view("b", None, false, false)]));
        assert!(!all_units_healthy(&[view("a", None, true, true)]), "ready inside the recovery window is not healthy");
        // No expected-running unit keeps nothing armed.
        assert!(all_units_healthy(&[]));
    }
}
