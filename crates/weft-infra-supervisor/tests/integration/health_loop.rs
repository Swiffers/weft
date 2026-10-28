//! Layer-3 integration tests for the supervisor's health loop.
//!
//! These exercise the full `tick_health` orchestration against
//! in-memory fakes: seed broker + host state, advance the clock,
//! call `tick_health()`, assert on emitted events + status writes.
//!
//! Counter-cases the pure-function tests can't catch:
//!   - lock ordering inside `tick_project`,
//!   - state reset when status leaves `running` / `flaky`,
//!   - the broker call sequence (event_record before set_status),
//!   - fired-set re-arming after recovery.

use std::time::Duration;

use weft_broker_client::protocol::InfraNodeStatus as Status;
use weft_infra_supervisor::broker_ops::{BrokerCall, BrokerSupervisorOps};
use weft_infra_supervisor::testing::SupervisorTestRig;
use weft_platform_traits::clock::Clock;
use weft_platform_traits::UnitRunState;

/// A one-unit roster (unit named `unit`, at `status`, default windows).
fn unit_map(
    unit: &str,
    status: Status,
) -> std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime> {
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        unit.to_string(),
        weft_broker_client::protocol::UnitRuntime {
            status,
            stop_behavior: weft_core::StopBehavior::Stop,
            flaky_after_seconds: 30,
            recovery_after_seconds: 30,
            image_refs: Default::default(),
            not_ready_since_unix: None,
            ready_since_unix: None,
            last_restart: None,
        },
    );
    m
}

const TENANT: &str = "tenant-test";
const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const PROJ_USER_OFF: uuid::Uuid = uuid::Uuid::from_u128(0x5eaa6fdd30862fd);
const PROJ_INACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xb2cdde0277773ee);
const PROJ_ACTIVE: uuid::Uuid = uuid::Uuid::from_u128(0xb8a5f259a9ea55d);
const INACTIVE_BROKEN: uuid::Uuid = uuid::Uuid::from_u128(0x53bf4ec8cd4f847);
const ACTIVE_HEALTHY: uuid::Uuid = uuid::Uuid::from_u128(0xe78b1a9b48520b1);
const NODE: &str = "bridge";

/// A stop of every node's shared copies, in flight.
fn whole_project_command() -> weft_broker_client::protocol::InFlightCommand {
    weft_broker_client::protocol::InFlightCommand { node_id: None, copies: weft_core::instance::Copies::Shared }
}

/// Stamp `status` on this file's node as lifecycle command `id` (a `verb`
/// of every node's shared copies: a stop, or an apply, which is what a
/// start is) does: issued, stamping under its fence, then completed, so
/// the health loop is not stood down after it.
async fn stamp_as_command(rig: &SupervisorTestRig, id: i64, verb: weft_broker_client::protocol::InfraLifecycleVerb, status: Status) {
    rig.broker.enqueue_command(weft_broker_client::protocol::SupervisorCommandRow {
        id,
        project_id: PROJECT,
        node_id: None,
        verb,
        spec_json: None,
        force: false,
        copies: weft_core::instance::Copies::Shared,
        cancel_requested: false,
    });
    let stamped = rig.broker.set_status("test-replica", id, PROJECT, NODE, None, Some(NODE), status, None, None).await.unwrap();
    assert!(matches!(stamped, weft_broker_client::WriteOutcome::Applied(_)), "{stamped:?}");
    rig.broker.command_complete("test-replica", id, None, false).await.unwrap();
}

fn rig() -> SupervisorTestRig {
    let rig = SupervisorTestRig::with_tenant(TENANT);
    rig.broker.add_project(PROJECT);
    rig
}

/// What the host reports for `project` from now on: each `(node,
/// copy_id, unit, ready)`, and nothing else.
fn observe(rig: &SupervisorTestRig, project: uuid::Uuid, units: &[(&str, &str, &str, bool)]) {
    rig.host.clear_project(project);
    for (node, copy_id, unit, ready) in units {
        let copy = weft_core::infra::NodeRef {
            tenant: TENANT.into(),
            project,
            node: (*node).into(),
            copy_id: (*copy_id).into(),
        };
        let state = if *ready { UnitRunState::Ready } else { UnitRunState::NotReady { why: "probe failing".into() } };
        rig.host.set_state(&copy, unit, state);
    }
}

/// How many take-downs the health loop asked the broker to enqueue.
fn take_down_calls(rig: &SupervisorTestRig) -> usize {
    rig.broker
        .calls()
        .iter()
        .filter(|c| {
            matches!(c, BrokerCall::EnqueueLifecycle { spec: weft_broker_client::protocol::LifecycleSpec::Deactivate(_), .. })
        })
        .count()
}

// ---------- the window lives on the row ----------

/// A unit's window is written on its row at the look that starts it and
/// cleared when it ends, so any process (and the broker, which books the
/// next pass at the window's end) reads where it stands. A unit that runs
/// fine carries none and costs no write.
#[tokio::test]
async fn the_window_lives_on_the_units_row() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    let unit = |rig: &SupervisorTestRig| rig.broker.infra_node(PROJECT, NODE).unwrap().units["bridge"].clone();
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    assert!(rig.broker.status_writes().is_empty(), "running fine: nothing to write");

    let started = rig.clock.now_unix();
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    assert_eq!((unit(&rig).status, unit(&rig).not_ready_since_unix), (Status::Running, Some(started)));
    rig.advance(Duration::from_secs(10));
    observe(&rig, PROJECT, &[]);
    rig.tick_health().await.unwrap();
    assert_eq!(unit(&rig).not_ready_since_unix, Some(started), "no longer reported: still not ready, same window");

    rig.advance(Duration::from_secs(21));
    rig.tick_health().await.unwrap();
    assert_eq!((unit(&rig).status, unit(&rig).not_ready_since_unix), (Status::Flaky, None));

    let back = rig.clock.now_unix();
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    assert_eq!((unit(&rig).status, unit(&rig).ready_since_unix), (Status::Flaky, Some(back)));
    rig.advance(Duration::from_secs(30));
    rig.tick_health().await.unwrap();
    assert_eq!((unit(&rig).status, unit(&rig).ready_since_unix), (Status::Running, None));
}

// ---------- false-flaky-on-fresh-provision ----------

#[tokio::test]
async fn provisioning_node_not_yet_ready_does_not_flap_flaky() {
    // Scenario: node is `provisioning`. Supervisor's health loop
    // SHOULD skip it (the apply executor owns the status). We assert
    // no flaky event fires no matter how long the loop runs.
    let rig = rig();
    rig.broker
        .add_infra_node(PROJECT, NODE, "inst1", Status::Provisioning);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    for _ in 0..3 {
        rig.advance(Duration::from_secs(60));
        rig.tick_health().await.unwrap();
    }

    let events = rig.broker.events();
    let kinds: Vec<&str> = events.iter().map(|(_, _, _, k, _)| k.as_str()).collect();
    assert!(
        !kinds.contains(&"flaky"),
        "should not emit flaky for provisioning node"
    );
    let status = rig.broker.infra_node(PROJECT, NODE).unwrap().status;
    assert_eq!(status, Status::Provisioning);
}

// ---------- health stands down during a user infra action ----------

#[tokio::test]
async fn health_skips_project_while_infra_command_in_flight() {
    // Scenario: a node is `running` with degraded replicas (0/1) that
    // would normally flap flaky after the window. But a user infra
    // action (stop/start/terminate) is in flight for the project. The
    // health loop must STAND DOWN for the whole project: no flaky
    // event, no status write, so it can't race the user action.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    // A user infra action is running.
    rig.broker.set_infra_commands_in_flight(PROJECT, vec![whole_project_command()]);

    // Run well past the flaky window: with no gate, this would emit
    // flaky + reconcile status. With the gate, nothing happens.
    for _ in 0..3 {
        rig.advance(Duration::from_secs(60));
        rig.tick_health().await.unwrap();
    }

    assert!(rig.broker.events().is_empty(), "no health events while a command is in flight");
    // Status untouched: the lifecycle handler owns it.
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Running);
    // The loop checked the gate and did NOT proceed to set_status.
    let calls = rig.broker.calls();
    assert!(
        calls.iter().any(|c| matches!(c, BrokerCall::InfraCommandsInFlight { .. })),
        "the gate was consulted"
    );
    assert!(
        !calls.iter().any(|c| matches!(c, BrokerCall::SetStatus { .. })),
        "no autonomous status write while standing down"
    );
}

/// A command in flight on one instance's copy stands health down for that
/// copy only: the shared copy of the same node, degraded, still turns
/// flaky, while ada's copy (as degraded) is left to the command.
#[tokio::test]
async fn health_stands_down_only_for_the_copy_a_command_reaches() {
    let rig = rig();
    let ada = weft_core::instance::InstanceId::new("ada").unwrap();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, NODE, &ada, "inst-ada", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true), (NODE, "inst-ada", "bridge", true)]);
    rig.tick_health().await.unwrap();
    rig.broker.set_infra_commands_in_flight(
        PROJECT,
        vec![weft_broker_client::protocol::InFlightCommand {
            node_id: Some(NODE.to_string()),
            copies: weft_core::instance::Copies::Instance(ada.clone()),
        }],
    );
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false), (NODE, "inst-ada", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let flaky: Vec<_> = rig.broker.events().into_iter().filter(|(_, _, _, k, _)| k == "flaky").collect();
    assert_eq!(flaky.len(), 1, "{flaky:?}");
    assert_eq!(flaky[0].2, None, "the shared copy, never ada's");
    assert_eq!(rig.broker.infra_copy(PROJECT, NODE, None).unwrap().status, Status::Flaky);
    assert_eq!(rig.broker.infra_copy(PROJECT, NODE, Some(&ada)).unwrap().status, Status::Running);
    assert!(
        !rig.broker.status_writes().iter().any(|(_, _, instance, _)| instance.is_some()),
        "no status write for the copy the command owns"
    );
}

#[tokio::test]
async fn health_rearms_after_infra_command_completes() {
    // Same degraded node, but the command finishes between ticks: the
    // gate clears and health resumes (flaky fires on the next windowed
    // tick). Proves the stand-down is transient, not a permanent mute.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // A user action runs (e.g. an upgrade): health stands down. The
    // gate also clears any in-memory window, so health resumes clean.
    rig.broker.set_infra_commands_in_flight(PROJECT, vec![whole_project_command()]);
    rig.advance(Duration::from_secs(60));
    rig.tick_health().await.unwrap();
    assert!(rig.broker.events().is_empty(), "gated: no events");

    // Action completes; node is back up and healthy. Health re-arms
    // and establishes a fresh ready baseline.
    rig.broker.set_infra_commands_in_flight(PROJECT, Vec::new());
    rig.tick_health().await.unwrap();

    // Now the node genuinely degrades, post-action. Flaky must fire
    // after the window: proof the monitor is live again, not muted.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap(); // first not-ready observation
    rig.advance(Duration::from_secs(35)); // past the 30s flaky window
    rig.tick_health().await.unwrap();

    let kinds: Vec<String> = rig.broker.events().iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(kinds.contains(&"flaky".to_string()), "health re-armed after the command completed");
}

// ---------- ready→flaky→recovered cycle ----------

#[tokio::test]
async fn full_lifecycle_emits_flaky_then_recovered() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Tick 1: ready. No event.
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.events().len(), 0);

    // Replicas degrade.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    // Tick 2 (immediately): still inside flaky window, no flaky yet.
    rig.tick_health().await.unwrap();
    let event_kinds: Vec<String> =
        rig.broker.events().iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(!event_kinds.contains(&"flaky".to_string()));

    // Advance past flaky window. Tick 3: emits flaky + sets status.
    // The built-in `take-down-while-flaky` rule ALSO fires here,
    // enqueueing a `deactivate` lifecycle command via
    // `enqueue_lifecycle`. We filter for `flaky` event_record entries
    // specifically; a rule firing is observed via
    // `BrokerCall::EnqueueLifecycle` counts (see the
    // `fired_set_rearms_when_the_copy_heals` test).
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let events = rig.broker.events();
    let flaky_events: Vec<_> = events.iter().filter(|(_, _, _, k, _)| k == "flaky").collect();
    assert_eq!(flaky_events.len(), 1);
    assert_eq!(flaky_events[0].1.as_deref(), Some(NODE));
    let status = rig.broker.infra_node(PROJECT, NODE).unwrap().status;
    assert_eq!(status, Status::Flaky);

    // Replicas recover.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Tick 4 (immediately after recovery): inside recovery window,
    // no recovered yet.
    rig.tick_health().await.unwrap();
    let event_kinds: Vec<String> =
        rig.broker.events().iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(!event_kinds.contains(&"recovered".to_string()));

    // Advance past recovery window. Tick 5: emits recovered.
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let events = rig.broker.events();
    let kinds: Vec<String> = events.iter().map(|(_, _, _, k, _)| k.clone()).collect();
    assert!(kinds.contains(&"recovered".to_string()));
    let status = rig.broker.infra_node(PROJECT, NODE).unwrap().status;
    assert_eq!(status, Status::Running);
}

// ---------- per-unit health on a multi-unit node ----------

#[tokio::test]
async fn one_flaky_unit_does_not_drag_down_a_healthy_sibling() {
    // A node with two units: `primary` (healthy) and `sidecar`
    // (degrades). Per-unit health means only `sidecar` goes flaky; the
    // node rolls up to flaky (worst-of-units) but `primary` stays
    // Running. Pre-per-unit, the node summed (2 desired, 1 ready) and
    // there was no way to tell which unit was down.
    let rig = rig();
    rig.broker.add_infra_node_units(
        PROJECT,
        NODE,
        "inst1",
        &[("primary", Status::Running), ("sidecar", Status::Running)],
    );
    observe(&rig, PROJECT, &[(NODE, "inst1", "primary", true), (NODE, "inst1", "sidecar", true)]);

    // Tick 1: both ready, baseline established.
    rig.tick_health().await.unwrap();
    assert!(rig.broker.events().is_empty());

    // sidecar degrades; primary stays healthy.
    observe(&rig, PROJECT, &[(NODE, "inst1", "primary", true), (NODE, "inst1", "sidecar", false)]);
    rig.tick_health().await.unwrap(); // first not-ready observation
    rig.advance(Duration::from_secs(35)); // past the 30s window
    rig.tick_health().await.unwrap();

    // A flaky event fired, naming the sidecar unit.
    let flaky: Vec<_> = rig
        .broker
        .events()
        .into_iter()
        .filter(|(_, _, _, k, _)| k == "flaky")
        .collect();
    assert_eq!(flaky.len(), 1, "exactly one flaky edge (the sidecar)");

    // Per-unit truth: sidecar flaky, primary still running.
    let row = rig.broker.infra_node(PROJECT, NODE).unwrap();
    assert_eq!(row.units.get("sidecar").unwrap().status, Status::Flaky);
    assert_eq!(row.units.get("primary").unwrap().status, Status::Running);
    // Node rollup = worst-of-units = flaky.
    assert_eq!(row.status, Status::Flaky);
}

// ---------- state reset on stop→start ----------

#[tokio::test]
async fn stop_clears_health_state_then_start_does_not_flake() {
    // A stopped unit is not looked at, so nothing from before its stop
    // makes it flaky after its start: simulate stop→start and assert no
    // flaky event fires.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Run a few ticks to populate the health state.
    for _ in 0..2 {
        rig.advance(Duration::from_secs(10));
        rig.tick_health().await.unwrap();
    }

    // Stop: status flips to stopped. (Simulating what lifecycle's
    // stop verb would do; we just write directly here.)
    stamp_as_command(&rig, 1, weft_broker_client::protocol::InfraLifecycleVerb::Stop, Status::Stopped).await;
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);

    // Tick: should NOT evaluate health (status is stopped).
    rig.advance(Duration::from_secs(60));
    rig.tick_health().await.unwrap();

    // Start: status flips back to running, replicas come back.
    stamp_as_command(&rig, 2, weft_broker_client::protocol::InfraLifecycleVerb::Apply, Status::Running).await;
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    // Several ticks while ready. Should not emit flaky.
    for _ in 0..3 {
        rig.advance(Duration::from_secs(10));
        rig.tick_health().await.unwrap();
    }

    let events = rig.broker.events();
    let flaky_count = events.iter().filter(|(_, _, _, k, _)| k == "flaky").count();
    assert_eq!(flaky_count, 0, "no flaky event should fire on clean start");
}

// ---------- fired-set re-arm ----------

#[tokio::test]
async fn fired_set_rearms_when_the_copy_heals() {
    // After a rule fires (the built-in take-down enqueues a `deactivate`
    // lifecycle command aimed at the broken copy), the supervisor
    // remembers the copy in `fired` so it doesn't fire again on the
    // same degradation. Once that copy is no longer broken, it must
    // re-arm (`health_engine::rearm`) so its next degradation triggers
    // the rule again.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();

    // Degrade past the flaky window: the park fires (one EnqueueLifecycle).
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let enqueue_count_1 = rig
        .broker
        .calls()
        .iter()
        .filter(|c| matches!(c, BrokerCall::EnqueueLifecycle { .. }))
        .count();
    assert_eq!(enqueue_count_1, 1, "the built-in take-down should fire once");

    // Recover: ready for the whole recovery window.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    // Degrade again, past the flaky window.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let enqueue_count_2 = rig
        .broker
        .calls()
        .iter()
        .filter(|c| matches!(c, BrokerCall::EnqueueLifecycle { .. }))
        .count();
    assert_eq!(
        enqueue_count_2, 2,
        "the built-in take-down should re-fire after recovery"
    );
}

// ---------- event-then-status ordering ----------

#[tokio::test]
async fn flaky_emits_event_then_set_status() {
    // The action bar reads `infra_event` to drive the badge animation;
    // it expects the event to land before status. Asserting the call
    // order catches a future refactor that flips them.
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);

    rig.tick_health().await.unwrap();
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap(); // first not-ready observation
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let calls = rig.broker.calls();
    let mut saw_event = false;
    for c in &calls {
        match c {
            BrokerCall::EventRecord { kind, .. } if kind == "flaky" => {
                saw_event = true;
            }
            BrokerCall::SetUnitHealth { status, .. } if *status == Status::Flaky => {
                assert!(
                    saw_event,
                    "set_unit_health(flaky) must come AFTER event_record(flaky)"
                );
                return;
            }
            _ => {}
        }
    }
    panic!("never saw set_unit_health(flaky)");
}

// ---------- empty project ----------

#[tokio::test]
async fn project_with_no_nodes_no_events() {
    let rig = rig();
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.events().len(), 0);
}

// ---------- a rule's action ----------

/// The built-in take-down's name.
const TAKE_DOWN: &str = "take-down-while-flaky";

/// An Active project whose one unit is seen not ready: its flaky window
/// is running, so a look past the window (35 s on) latches it flaky and
/// the default take-down fires.
async fn rig_with_a_breaking_unit() -> SupervisorTestRig {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, NODE, "inst1", Status::Running);
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 0, "inside the window nothing fires");
    rig
}

/// Longer than the default action timeout, so a tick that hangs on the
/// action is cut by that timeout first. Under `start_paused` tokio skips
/// ahead to it, so nothing really waits.
const PAST_THE_ACTION_TIMEOUT: Duration = Duration::from_secs(3600);

#[tokio::test(start_paused = true)]
async fn hung_action_times_out_frees_inflight_and_unlatches_fired() {
    // The wedge this prevents: a health rule's action's broker call
    // hangs (a broker that never answers). Without the timeout it pins
    // `in_flight` forever and stops all future health ticks. Worse,
    // even with the timeout, if the rule stayed latched in
    // `fired` it would never retry (the project can't go healthy
    // because recovery is what would make it healthy). This drives
    // the FULL production path (`tick` -> `tick_project` -> the
    // `tokio::time::timeout(run_action)` wrap) and asserts BOTH the
    // mechanical free (`in_flight` cleared) AND the retryability
    // (`fired` un-latched).
    //
    // Under start_paused, when the tick parks on the hung enqueue (a
    // never-waking `pending()`), the only live timer is the action
    // timeout, so tokio auto-advances to it and fires it
    // deterministically: no real wait. The outer bound is a safety net
    // so a regression (timeout not firing) fails fast instead of
    // hanging the suite.
    let rig = rig_with_a_breaking_unit().await;
    rig.broker.hang_lifecycle_enqueues();
    rig.advance(Duration::from_secs(35)); // FakeClock past the flaky window

    tokio::time::timeout(PAST_THE_ACTION_TIMEOUT, rig.tick_health())
        .await
        .expect("tick_health hung: the action timeout did not fire")
        .unwrap();

    // The action started (the enqueue recorded) then hung; the
    // timeout mapped it to a failed action.
    assert_eq!(take_down_calls(&rig), 1, "the action should have started before hanging");

    // Mechanical free: slot released so the next tick can evaluate.
    assert!(!rig.state.health.lock().await.is_in_flight(PROJECT), "timed-out action must free in_flight");
    let row = rule_state(&rig).expect("the failure is kept");
    // Retryability: the failed rule is NOT latched as fired, so
    // a later tick fires it again (the wedge fix).
    assert!(!row.fired, "timed-out action must not count as fired");
    // Backoff armed: one failure recorded, so the immediate next tick
    // is skipped (no re-fire storm) until the backoff window elapses.
    assert_eq!(row.consecutive_failures, 1, "a failed action must arm the backoff");
    assert_eq!(row.next_retry_at_unix, Some(rig.clock.now_unix() + 5));
}

/// Where the built-in take-down stands on the shared copy, as the broker
/// keeps it.
fn rule_state(rig: &SupervisorTestRig) -> Option<weft_broker_client::protocol::HealthRuleState> {
    let copy = weft_broker_client::protocol::InfraCopy { node_id: NODE.into(), instance: None };
    rig.broker.health_state_rows(PROJECT).into_iter().find(|s| s.rule == TAKE_DOWN && s.copy == copy)
}

#[tokio::test(start_paused = true)]
async fn failed_action_backs_off_then_retries_after_window() {
    // After a failed action, the rule is un-latched from `fired`
    // (so it WILL retry) but gated by exponential backoff so it does
    // NOT re-fire on every poll tick. Walk: fail once (enqueue
    // call #1, backoff=1, ~5s window) -> immediate tick is skipped
    // (no call #2) -> advance the clock past the window -> next tick
    // re-fires (call #2, backoff=2).
    let rig = rig_with_a_breaking_unit().await;
    rig.broker.hang_lifecycle_enqueues();
    rig.advance(Duration::from_secs(35));

    // Tick 1: fires, hangs, times out -> failure -> backoff=1.
    tokio::time::timeout(PAST_THE_ACTION_TIMEOUT, rig.tick_health())
        .await
        .expect("tick 1 hung")
        .unwrap();
    assert_eq!(take_down_calls(&rig), 1, "first tick fires the action");
    assert_eq!(rule_state(&rig).unwrap().consecutive_failures, 1);

    // Tick 2 immediately (no clock advance): inside the backoff
    // window -> skipped, no new action.
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 1, "tick inside backoff window must NOT re-fire");

    // Advance past the first backoff window (5s). Tick 3 re-fires.
    rig.advance(Duration::from_secs(6));
    tokio::time::timeout(PAST_THE_ACTION_TIMEOUT, rig.tick_health())
        .await
        .expect("tick 3 hung")
        .unwrap();
    assert_eq!(take_down_calls(&rig), 2, "tick past backoff window re-fires");
    assert_eq!(rule_state(&rig).unwrap().consecutive_failures, 2, "second failure grows the backoff count");

    // The third failure gives up, reported once by the next ownership
    // sync (and never as a failure tried again on a clock), and no later
    // look fires it again while what it saw stays the same.
    rig.advance(Duration::from_secs(11));
    tokio::time::timeout(PAST_THE_ACTION_TIMEOUT, rig.tick_health()).await.expect("tick 4 hung").unwrap();
    assert_eq!(take_down_calls(&rig), 3);
    let row = rule_state(&rig).unwrap();
    assert!(row.gave_up_at_unix.is_some() && row.next_retry_at_unix.is_none(), "given up: {row:?}");
    rig.tick_ownership().await.unwrap();
    rig.tick_ownership().await.unwrap();
    assert_eq!(rig.broker.gave_up(), vec![PROJECT]);
    assert!(rig.broker.failed().is_empty());
    rig.advance(Duration::from_secs(5));
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 3, "given up: no retry while what it saw stays the same");

    // What it saw changing re-arms it: the unit reads ready again while
    // still latched flaky, and the take-down tries afresh.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    tokio::time::timeout(PAST_THE_ACTION_TIMEOUT, rig.tick_health()).await.expect("tick 5 hung").unwrap();
    assert_eq!(take_down_calls(&rig), 4, "re-armed by what it saw changing");
    let row = rule_state(&rig).unwrap();
    assert!(row.gave_up_at_unix.is_none() && row.consecutive_failures == 1, "a fresh start: {row:?}");

    // Every unit healthy (ready for its whole recovery window) clears
    // what failed.
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(rule_state(&rig), None, "healthy: nothing failed is kept");
}

/// The take-down the built-in rule queued, still waiting on the
/// dispatcher.
fn queued_command(rig: &SupervisorTestRig) -> i64 {
    rule_state(rig).and_then(|r| r.queued).expect("a command is queued").command
}

/// The rule counts as fired while its command runs; a command the
/// dispatcher failed is a failure of the rule, tried again after 5 s,
/// then 10 s, and given up at the third, exactly like an action that
/// could not be queued. A success clears what failed.
#[tokio::test]
async fn a_health_command_that_fails_on_the_dispatcher_is_retried_then_given_up() {
    use weft_broker_client::protocol::LifecycleOutcome;
    let rig = rig_with_a_breaking_unit().await;
    rig.broker.keep_health_commands_running();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 1);
    let row = rule_state(&rig).unwrap();
    assert!(row.fired && row.queued.is_some() && !row.took_down, "queued, not landed yet: {row:?}");
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 1, "a rule waits on its running command");

    for (failure, wait) in [(1, 5), (2, 10)] {
        rig.broker.end_health_command(queued_command(&rig), LifecycleOutcome::Failed, Some("the listener did not answer"));
        rig.tick_health().await.unwrap();
        let row = rule_state(&rig).unwrap();
        assert!(!row.fired && row.queued.is_none(), "the failure takes the firing back: {row:?}");
        assert_eq!(row.consecutive_failures, failure);
        assert_eq!(row.next_retry_at_unix, Some(rig.clock.now_unix() + wait));
        assert!(row.last_error.as_deref().is_some_and(|e| e.contains("the listener did not answer")), "{row:?}");
        assert_eq!(take_down_calls(&rig), failure as usize, "nothing again inside the backoff");
        rig.advance(Duration::from_secs(wait as u64 + 1));
        rig.tick_health().await.unwrap();
        assert_eq!(take_down_calls(&rig), failure as usize + 1, "tried again after the backoff");
    }
    rig.broker.end_health_command(queued_command(&rig), LifecycleOutcome::Failed, Some("the listener did not answer"));
    rig.tick_health().await.unwrap();
    let row = rule_state(&rig).unwrap();
    assert!(row.gave_up_at_unix.is_some() && row.next_retry_at_unix.is_none(), "the third failure gives up: {row:?}");
    rig.tick_ownership().await.unwrap();
    assert_eq!(rig.broker.gave_up(), vec![PROJECT]);
    rig.advance(Duration::from_secs(60));
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 3, "given up: no retry while what it saw stays the same");

    // A person's look re-arms it (the broker clears the give-up); this
    // time the dispatcher succeeds, and the failures are cleared.
    rig.broker.set_health_state_rows(
        PROJECT,
        vec![weft_broker_client::protocol::HealthRuleState { consecutive_failures: 0, gave_up_at_unix: None, gave_up_seen: None, last_error: None, ..row }],
    );
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 4);
    rig.broker.end_health_command(queued_command(&rig), LifecycleOutcome::Succeeded, None);
    rig.tick_health().await.unwrap();
    let row = rule_state(&rig).unwrap();
    assert!(row.fired && row.queued.is_none() && row.took_down && row.consecutive_failures == 0 && row.last_error.is_none(), "{row:?}");
}

/// A command that never ends counts as failed once the rule's timeout has
/// passed since it was queued, and is tried again.
#[tokio::test]
async fn a_health_command_past_its_timeout_counts_as_failed() {
    let rig = rig_with_a_breaking_unit().await;
    rig.broker.keep_health_commands_running();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let deadline = rule_state(&rig).and_then(|r| r.queued).unwrap().deadline_unix;
    assert_eq!(deadline, rig.clock.now_unix() + 1800, "the built-in rules' timeout");
    rig.advance(Duration::from_secs(1799));
    rig.tick_health().await.unwrap();
    assert!(rule_state(&rig).unwrap().queued.is_some(), "still inside its timeout");
    rig.advance(Duration::from_secs(1));
    rig.tick_health().await.unwrap();
    let row = rule_state(&rig).unwrap();
    assert_eq!(row.consecutive_failures, 1, "{row:?}");
    assert!(row.queued.is_none() && !row.fired);
    assert!(row.last_error.as_deref().is_some_and(|e| e.contains("did not end within")), "{row:?}");
    rig.advance(Duration::from_secs(6));
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 2, "tried again");
}

#[tokio::test]
async fn completed_action_frees_in_flight() {
    // The success side, through the FULL tick: the action does NOT
    // hang, the timeout wrapper is transparent, the action runs
    // (the take-down enqueued) and the in_flight slot frees. Together
    // with `hung_action_times_out_frees_inflight_and_unlatches_fired`
    // this covers both arms of the timeout match in `tick_project`.
    let rig = rig_with_a_breaking_unit().await;
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let in_flight = {
        let reg = rig.state.health.lock().await;
        reg.is_in_flight(PROJECT)
    };
    assert!(!in_flight, "completed action must free the in_flight slot");
    assert_eq!(take_down_calls(&rig), 1, "the action should have run");
}

// ---------- regression: set_applied lands mid-flaky ----------

/// A frozen unit the row calls `Flaky`, still at 0 ready, stays `Flaky`
/// after a command in flight on the copy stood health down: the latch is
/// the row, so there is nothing to rewrite.
#[tokio::test]
async fn frozen_flaky_unit_is_not_rewritten_running_after_latch_reset() {
    let rig = rig();
    rig.broker.add_infra_node_with(
        PROJECT,
        NODE,
        "inst1",
        Status::Flaky,
        Some("hash".into()),
        std::collections::BTreeMap::new(),
        unit_map(NODE, Status::Flaky),
    );
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", false)]);
    // A command in flight on the copy clears its latch and stands the
    // tick down for it.
    rig.broker.set_infra_commands_in_flight(PROJECT, vec![whole_project_command()]);
    rig.tick_health().await.unwrap();
    rig.broker.set_infra_commands_in_flight(PROJECT, Vec::new());

    rig.advance(Duration::from_secs(1));
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Flaky);
    assert!(
        !rig.broker.calls().iter().any(|c| matches!(c, BrokerCall::SetStatus { .. })),
        "no status rewrite for a unit the row already calls Flaky; calls={:?}",
        rig.broker.calls()
    );

    // Ready for the whole recovery window: recovered, and only then
    // written Running.
    observe(&rig, PROJECT, &[(NODE, "inst1", "bridge", true)]);
    rig.advance(Duration::from_secs(1));
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Flaky);
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(rig.broker.infra_node(PROJECT, NODE).unwrap().status, Status::Running);
}

// ---------- regression: the built-in rules ----------

/// The take-downs the rig's broker was asked to enqueue for `project`.
fn take_downs(rig: &SupervisorTestRig, project: uuid::Uuid) -> Vec<weft_broker_client::protocol::TakeDownReaders> {
    rig.broker
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            BrokerCall::EnqueueLifecycle { project_id, spec: weft_broker_client::protocol::LifecycleSpec::Deactivate(d) }
                if project_id == project =>
            {
                Some(d)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn the_built_in_take_down_aims_at_the_broken_copy() {
    // The built-in take-down: an Active project whose unit stays
    // not ready for the whole flaky window (after having been ready)
    // enqueues a park aimed at that copy, and only at it: the
    // dispatcher then parks only the triggers reading it.
    let rig = rig();
    rig.broker.add_project_with_status(PROJ_ACTIVE,
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node(PROJ_ACTIVE, NODE, "inst1", Status::Running);
    observe(&rig, PROJ_ACTIVE, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    observe(&rig, PROJ_ACTIVE, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    assert!(take_downs(&rig, PROJ_ACTIVE).is_empty(), "one bad reading takes nothing down");

    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let parks = take_downs(&rig, PROJ_ACTIVE);
    assert_eq!(parks.len(), 1, "the latched breakage takes its triggers down once");
    assert_eq!(parks[0].broken, vec![weft_broker_client::protocol::InfraCopy { node_id: NODE.into(), instance: None }]);
}

/// The incident: an instance's copy was just applied (its row says
/// Running) and the host has not reported it yet. Unknown is
/// not broken: however long that lasts, nothing parks and nothing is
/// called flaky. An instance's copy that does break parks only that copy.
#[tokio::test]
async fn a_copy_the_host_has_not_reported_never_parks_and_a_broken_one_parks_alone() {
    let rig = rig();
    let ada = weft_core::instance::InstanceId::new("ada").unwrap();
    rig.broker.add_infra_node(PROJECT, "shared", "inst-shared", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, NODE, &ada, "inst1", Status::Running);
    observe(&rig, PROJECT, &[("shared", "inst-shared", "shared", true)]);
    for _ in 0..4 {
        rig.advance(Duration::from_secs(60));
        rig.tick_health().await.unwrap();
    }
    assert!(take_downs(&rig, PROJECT).is_empty(), "an unseen copy is unknown, never broken");
    assert!(!rig.broker.events().iter().any(|(_, _, _, k, _)| k == "flaky"));

    // Ada's copy shows up ready, then breaks for the whole window.
    observe(&rig, PROJECT, &[("shared", "inst-shared", "shared", true), (NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    observe(&rig, PROJECT, &[("shared", "inst-shared", "shared", true), (NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let parks = take_downs(&rig, PROJECT);
    assert_eq!(parks.len(), 1);
    assert_eq!(
        parks[0].broken,
        vec![weft_broker_client::protocol::InfraCopy { node_id: NODE.into(), instance: Some(ada) }],
        "only ada's copy is broken; the shared one is healthy"
    );
}

/// Each copy runs the rules its own node wrote, stamped on its row: of
/// three nodes breaking at once, the one on the built-in rules is taken
/// down, the one that wrote no rules at all is left alone, and the one
/// whose rule reads only its `primary` unit is left alone while only its
/// `sidecar` is flaky. Two copies firing a rule of one name keep two
/// states.
#[tokio::test]
async fn each_copy_runs_the_rules_its_node_wrote() {
    use weft_core::infra::health::{HealthAction, HealthCondition, HealthRule, DEFAULT_RULE_TIMEOUT_SECONDS};
    let rig = rig();
    let ada = weft_core::instance::InstanceId::new("ada").unwrap();
    rig.broker.add_infra_node(PROJECT, "plain", "c-plain", Status::Running);
    rig.broker.add_instance_infra_node(PROJECT, "plain", &ada, "c-plain-ada", Status::Running);
    rig.broker.add_infra_node(PROJECT, "silent", "c-silent", Status::Running);
    rig.broker.set_health_rules(PROJECT, "silent", None, Some(Vec::new()));
    rig.broker.add_infra_node_units(PROJECT, "picky", "c-picky", &[("primary", Status::Running), ("sidecar", Status::Running)]);
    rig.broker.set_health_rules(
        PROJECT,
        "picky",
        None,
        Some(vec![HealthRule {
            name: TAKE_DOWN.into(),
            when: HealthCondition::UnitFlaky { unit: Some("primary".into()) },
            action: HealthAction::TakeDownTriggers,
            timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
        }]),
    );
    let units = |ready: bool| {
        [
            ("plain", "c-plain", "plain", ready),
            ("plain", "c-plain-ada", "plain", ready),
            ("silent", "c-silent", "silent", ready),
            ("picky", "c-picky", "primary", true),
            ("picky", "c-picky", "sidecar", ready),
        ]
    };
    observe(&rig, PROJECT, &units(true));
    rig.tick_health().await.unwrap();
    observe(&rig, PROJECT, &units(false));
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();

    let copy = |node: &str, instance: Option<&weft_core::instance::InstanceId>| weft_broker_client::protocol::InfraCopy {
        node_id: node.into(),
        instance: instance.cloned(),
    };
    let downs = take_downs(&rig, PROJECT);
    assert_eq!(downs.len(), 1, "one action for every copy that broke in the same look");
    assert_eq!(downs[0].broken, vec![copy("plain", None), copy("plain", Some(&ada))]);
    let states: Vec<_> = rig.broker.health_state_rows(PROJECT).into_iter().map(|s| (s.copy, s.rule, s.fired)).collect();
    assert_eq!(
        states,
        vec![(copy("plain", None), TAKE_DOWN.to_string(), true), (copy("plain", Some(&ada)), TAKE_DOWN.to_string(), true)]
    );
}

#[tokio::test]
async fn the_built_in_bring_back_fires_once_infra_is_healthy() {
    // The built-in bring-back: with every seen unit healthy
    // and something the health loop parked still down, reactivate.
    let rig = rig();
    rig.broker.add_project_with_status(PROJ_INACTIVE,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The health loop parked it: auto-recover is allowed to undo it.
    rig.broker.set_health_down(PROJ_INACTIVE, true);
    rig.broker.add_infra_node(PROJ_INACTIVE, NODE, "inst1", Status::Running);
    observe(&rig, PROJ_INACTIVE, &[(NODE, "inst1", "bridge", true)]);

    rig.tick_health().await.unwrap();

    let calls = rig.broker.calls();
    let reactivated = calls.iter().any(|c| matches!(
        c,
        BrokerCall::EnqueueLifecycle { project_id, spec }
            if *project_id == PROJ_INACTIVE
            && matches!(spec, weft_broker_client::protocol::LifecycleSpec::Reactivate(_))
    ));
    assert!(
        reactivated,
        "the built-in bring-back must enqueue a reactivate when infra is up and the health check took something down"
    );
}

/// `db` (the built-in rules) goes flaky and takes trigger T down; T also
/// reads `cache`, whose node runs no health rules at all. Once `db`
/// recovers, the bring-back names nothing still broken: `cache` took
/// nothing down, so it holds nothing back, and T comes back.
#[tokio::test]
async fn a_node_with_no_rules_never_holds_back_another_nodes_recovery() {
    let rig = rig();
    rig.broker.add_infra_node(PROJECT, "db", "c-db", Status::Running);
    rig.broker.add_infra_node(PROJECT, "cache", "c-cache", Status::Running);
    rig.broker.set_health_rules(PROJECT, "cache", None, Some(Vec::new()));
    let units = |db: bool| [("db", "c-db", "db", db), ("cache", "c-cache", "cache", true)];
    observe(&rig, PROJECT, &units(true));
    rig.tick_health().await.unwrap();
    observe(&rig, PROJECT, &units(false));
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let db = weft_broker_client::protocol::InfraCopy { node_id: "db".into(), instance: None };
    assert_eq!(take_downs(&rig, PROJECT).iter().map(|d| d.broken.clone()).collect::<Vec<_>>(), vec![vec![db.clone()]]);
    // The dispatcher took T down: nothing listens, and the health check
    // holds something down. Its ending asks for a look.
    rig.broker.add_project_with_status(PROJECT, weft_broker_client::protocol::ProjectStatus::Inactive);
    rig.broker.set_health_down(PROJECT, true);
    rig.tick_health().await.unwrap();

    observe(&rig, PROJECT, &units(true));
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    let bring_backs: Vec<_> = rig
        .broker
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            BrokerCall::EnqueueLifecycle { spec: weft_broker_client::protocol::LifecycleSpec::Reactivate(r), .. } => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(
        bring_backs,
        vec![weft_broker_client::protocol::RestoreReaders { still_broken: Vec::new() }],
        "db came back and cache took nothing down: T comes back"
    );
}

#[tokio::test]
async fn the_built_in_bring_back_never_undoes_a_person() {
    // Regression: the user clicked Stop infra (or Deactivate / Upgrade)
    // on a running+active project. The infra processes are still up for a
    // moment (or being brought back up by a later Start). A health tick
    // that lands then must NOT auto-reactivate, because the USER
    // deactivated (nothing is `health_down`), not the health loop.
    // Before the gate, this raced into a reactivate and left the user
    // with active triggers but no/just-stopped infra.
    let rig = rig();
    rig.broker.add_project_with_status(PROJ_USER_OFF,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    // The USER deactivated: nothing is health-parked (default).
    rig.broker.add_infra_node(PROJ_USER_OFF, NODE, "inst1", Status::Running);
    observe(&rig, PROJ_USER_OFF, &[(NODE, "inst1", "bridge", true)]);

    rig.tick_health().await.unwrap();

    let any_reactivate = rig.broker.calls().iter().any(|c| matches!(
        c,
        BrokerCall::EnqueueLifecycle { project_id, spec }
            if *project_id == PROJ_USER_OFF
            && matches!(spec, weft_broker_client::protocol::LifecycleSpec::Reactivate(_))
    ));
    assert!(
        !any_reactivate,
        "a USER-deactivated project (nothing health-parked) must NOT be auto-reactivated, \
         even when infra is healthy"
    );
}

#[tokio::test]
async fn the_built_in_rules_do_not_fire_when_the_listening_mismatches() {
    // Cross-check: an Active project with HEALTHY infra and nothing
    // health-parked must NOT enqueue a reactivate. And an Inactive
    // project with BROKEN infra must NOT enqueue a park (the
    // first-stage condition requires Active: nothing listens to park).
    let rig = rig();
    rig.broker.add_project_with_status(ACTIVE_HEALTHY,
        weft_broker_client::protocol::ProjectStatus::Active,
    );
    rig.broker.add_infra_node(ACTIVE_HEALTHY, NODE, "inst1", Status::Running);
    observe(&rig, ACTIVE_HEALTHY, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();

    rig.broker.add_project_with_status(INACTIVE_BROKEN,
        weft_broker_client::protocol::ProjectStatus::Inactive,
    );
    rig.broker.add_infra_node(INACTIVE_BROKEN, NODE, "inst1", Status::Running);
    observe(&rig, INACTIVE_BROKEN, &[(NODE, "inst1", "bridge", true)]);
    rig.tick_health().await.unwrap();
    observe(&rig, INACTIVE_BROKEN, &[(NODE, "inst1", "bridge", false)]);
    rig.tick_health().await.unwrap();
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(
        rig.broker.infra_node(INACTIVE_BROKEN, NODE).unwrap().status,
        Status::Flaky,
        "the unit did break"
    );

    let calls = rig.broker.calls();
    let any_lifecycle = calls.iter().any(|c| matches!(c, BrokerCall::EnqueueLifecycle { .. }));
    assert!(
        !any_lifecycle,
        "neither (Active+healthy) nor (Inactive+broken) should fire a built-in rule"
    );
}

// ---------- ownership ----------

/// A claimed project is looked at at once, without waiting for the
/// tick. Where its rules stand lives with the project, not in this
/// process: a take-down already made is not made again.
#[tokio::test]
async fn ownership_changes_reach_the_health_loop_at_once() {
    use weft_infra_supervisor::ownership::OwnershipChange;
    let rig = rig_with_a_breaking_unit().await;
    rig.advance(Duration::from_secs(35));
    rig.tick_health().await.unwrap();
    assert_eq!(take_down_calls(&rig), 1);
    let looks = |rig: &SupervisorTestRig| rig.broker.calls().iter().filter(|c| matches!(c, BrokerCall::OwnedProjects { .. })).count();
    let before = looks(&rig);

    rig.health_ownership_change(&OwnershipChange { claimed: vec![PROJECT], lost: vec![] }).await.unwrap();
    assert_eq!(looks(&rig), before + 1, "a claimed project is looked at without waiting for the tick");
    assert_eq!(take_down_calls(&rig), 1, "the take-down it made is kept with the project");
}

weft_core::stress_test!(
    name: a_pass_keeps_renewing_its_leases_while_a_rule_acts,
    runs: 8,
    worker_threads: 4,
    async fn body() {
        // A rule's action can take longer than a lease (a broker
        // slow to answer): the pass renews while it acts, so no sibling
        // takes the project over and fires the action a second time.
        let syncs = |rig: &SupervisorTestRig| rig.broker.calls().iter().filter(|c| matches!(c, BrokerCall::SyncOwnership { .. })).count();
        let rig = rig_with_a_breaking_unit().await;
        rig.broker.hang_lifecycle_enqueues();
        rig.advance(Duration::from_secs(35));
        let pass = tokio::spawn({
            let state = rig.state.clone();
            async move { weft_infra_supervisor::tick(&state).await }
        });
        let bounded = tokio::time::timeout(Duration::from_secs(4), async {
            while take_down_calls(&rig) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let acting = syncs(&rig);
            while syncs(&rig) < acting + 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        bounded.await.expect("the leases were not renewed while the action hangs");
        pass.abort();
    }
);
