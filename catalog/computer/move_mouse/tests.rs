//! MoveMouse self-tests: the action it asks the computer for, and a refusal
//! reaching `_error`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::MoveMouseNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("asks_the_computer_and_says_what_it_did", acts),
        NodeTest::fake("a_refusal_from_the_computer_reaches_error", refused),
    ]
}

async fn acts(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let outcome = rig.run(&MoveMouseNode, json!({ "computer": computer, "args": { "x": 5, "y": 6 } })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Moved the mouse."));
    assert_eq!(asked(&rig), vec![("move".to_string(), json!({ "x": 5, "y": 6 }))], "the args go to the computer as they came");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "(5, 6000) is off the 1280x800 screen" }));
    rig.wire_output("_error");
    let outcome = rig.run(&MoveMouseNode, json!({ "computer": computer, "args": { "x": 5, "y": 6 } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("move: (5, 6000) is off the 1280x800 screen"), "{error}");
    assert!(outcome.outputs.get("result").is_none(), "a refused action says nothing on result");
    Ok(())
}
