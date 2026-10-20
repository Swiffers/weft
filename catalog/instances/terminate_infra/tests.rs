//! TerminateInfra self-tests: the terminate is asked for that
//! instance's copy with the node's choices, or for the program's own copy
//! when no instance is given.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{Door, FakeRig, Held, NodeTest, Running, StopSelf, WeftResult};

use super::TerminateInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("terminates_the_instances_copy", terminates),
        NodeTest::fake("no_instance_terminates_the_programs_own_copy", the_programs_own),
    ]
}

async fn terminates(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &TerminateInfraNode,
        json!({ "node": "one.bridge", "instance": "ada", "door": "refuse", "held": "drop", "running": "cancel" }),
    )
    .await
    .ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraTerminate { node, instance, take_down, disks }, StopSelf::Keep) => {
            assert_eq!(node, "one.bridge");
            assert_eq!(*disks, weft::infra::TerminateDisks::KeepListed);
            assert_eq!(instance.as_ref().unwrap().as_str(), "ada");
            assert_eq!((take_down.door, take_down.held, take_down.running), (Some(Door::Refuse), Some(Held::Drop), Some(Running::Cancel)));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn the_programs_own(rig: FakeRig) -> WeftResult<()> {
    rig.run(&TerminateInfraNode, json!({ "node": "pg" })).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::InfraTerminate { node, instance, .. }, StopSelf::Keep) => {
            assert_eq!(node, "pg");
            assert!(instance.is_none(), "the program's own copy");
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}
