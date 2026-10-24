//! StopInfra self-tests: the stop is asked for that instance's copy, or
//! the program's own when no instance is given, the choices become the
//! take-down, the self choice follows the toggle, and a contradiction is
//! refused before anything is asked.

use serde_json::json;

use weft::program::ProgramCall;
use weft::{Door, FakeRig, Held, NodeTest, Running, StopSelf, WeftResult};

use super::StopInfraNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("takes_keeps_and_waits_by_default", defaults),
        NodeTest::fake("no_instance_stops_the_programs_own_copy", the_programs_own),
        NodeTest::fake("the_choices_reach_the_call", choices),
        NodeTest::fake("keep_is_no_running_answer", keep_refused),
        NodeTest::fake("no_copy_is_told_apart_from_done", no_copy),
        NodeTest::fake("a_copy_already_down_is_done", already_down),
    ]
}

async fn no_copy(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.stop", json!({ "copy": "no_copy" }));
    let out = rig.run(&StopInfraNode, json!({ "node": "bridge", "instance": "adaa" })).await.ok()?;
    assert_eq!(out.output("noCopy")?, &json!(true));
    assert!(out.output("done").is_err(), "nothing was taken down");
    Ok(())
}

async fn already_down(rig: FakeRig) -> WeftResult<()> {
    rig.answer_program_call("weft.infra.stop", json!({ "copy": "already_down" }));
    let out = rig.run(&StopInfraNode, json!({ "node": "bridge", "instance": "ada" })).await.ok()?;
    assert_eq!(out.output("done")?, &json!(true));
    assert!(out.output("noCopy").is_err());
    Ok(())
}

async fn defaults(rig: FakeRig) -> WeftResult<()> {
    rig.run(&StopInfraNode, json!({ "node": "bridge", "instance": "ada" })).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::Infra(weft::program::infra::InfraCall::Stop { node, instance, take_down, .. }), StopSelf::Keep) => {
            assert_eq!(node, "bridge");
            assert_eq!(instance.as_ref().unwrap().as_str(), "ada");
            assert_eq!((take_down.door, take_down.door_for_minutes), (Some(Door::Take), None));
            assert_eq!((take_down.held, take_down.running), (Some(Held::Keep), Some(Running::Wait)));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn the_programs_own(rig: FakeRig) -> WeftResult<()> {
    let out = rig.run(&StopInfraNode, json!({ "node": "pg" })).await.ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::Infra(weft::program::infra::InfraCall::Stop { node, instance, .. }), StopSelf::Keep) => {
            assert_eq!(node, "pg");
            assert!(instance.is_none(), "the program's own copy");
        }
        other => panic!("unexpected call {other:?}"),
    }
    assert_eq!(out.output("done")?, &json!(true));
    Ok(())
}

async fn choices(rig: FakeRig) -> WeftResult<()> {
    rig.run(
        &StopInfraNode,
        json!({ "node": "bridge", "instance": "ada", "door": "hide", "doorForMinutes": 30, "held": "drop", "running": "cancel", "includeSelf": true }),
    )
    .await
    .ok()?;
    match &rig.program_calls()[0] {
        (ProgramCall::Infra(weft::program::infra::InfraCall::Stop { take_down, .. }), StopSelf::Include) => {
            assert_eq!((take_down.door, take_down.door_for_minutes), (Some(Door::Hide), Some(30)));
            assert_eq!((take_down.held, take_down.running), (Some(Held::Drop), Some(Running::Cancel)));
        }
        other => panic!("unexpected call {other:?}"),
    }
    Ok(())
}

async fn keep_refused(rig: FakeRig) -> WeftResult<()> {
    let err = rig.run(&StopInfraNode, json!({ "node": "bridge", "running": "keep" })).await.failure()?;
    assert!(err.contains("`wait` or `cancel`"), "{err}");
    assert!(rig.program_calls().is_empty());
    Ok(())
}
