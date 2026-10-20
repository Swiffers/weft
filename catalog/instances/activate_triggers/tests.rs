//! ActivateTriggers self-tests: every per-instance trigger of the
//! instance by default, every shared trigger when no instance is given,
//! only the named ones when `only` is wired, releasing what they held
//! unless `held` says drop.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{FakeRig, NodeTest, ReleaseHeld, WeftResult};

use super::ActivateTriggersNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("every_trigger_of_the_instance", every),
        NodeTest::fake("only_the_named_ones", only),
        NodeTest::fake("no_instance_is_the_programs_own_triggers", the_programs_own),
        NodeTest::fake("held_work_is_dropped_when_asked", dropped),
    ]
}

async fn every(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateTriggersNode, json!({ "instance": "ada" })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { scope, held } => {
            assert!(scope.triggers.is_empty());
            assert_eq!(scope.instances, weft::instance::InstanceTarget::named(vec![weft::instance::InstanceId::new("ada").unwrap()]));
            assert_eq!(*held, ReleaseHeld::Run, "what they held runs by default");
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn only(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateTriggersNode, json!({ "instance": "ada", "only": ["receive"] })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { scope, .. } => assert_eq!(scope.triggers, vec!["receive".to_string()]),
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn the_programs_own(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateTriggersNode, json!({ "only": ["inbox"] })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { scope, .. } => {
            assert_eq!(scope.triggers, vec!["inbox".to_string()]);
            assert!(scope.instances.is_shared(), "the program's own triggers");
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn dropped(rig: FakeRig) -> WeftResult<()> {
    rig.run(&ActivateTriggersNode, json!({ "held": "drop" })).await.ok()?;
    match &rig.program_calls()[0].0 {
        ProgramCall::TriggerActivate { held, .. } => assert_eq!(*held, ReleaseHeld::Drop),
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
