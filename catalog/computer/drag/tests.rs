//! Drag self-tests: the action it asks the computer for, and a refusal
//! reaching `_error`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::DragNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("asks_the_computer_and_says_what_it_did", acts),
        NodeTest::fake("a_refusal_from_the_computer_reaches_error", refused),
    ]
}

async fn acts(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let outcome = rig.run(&DragNode, json!({ "computer": computer, "args": { "path": [{ "x": 1, "y": 2 }, { "x": 30, "y": 40 }], "modifiers": "shift" } })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Dragged."));
    assert_eq!(asked(&rig), vec![("drag".to_string(), json!({ "path": [{ "x": 1, "y": 2 }, { "x": 30, "y": 40 }], "modifiers": "shift" }))], "the args go to the computer as they came");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "a drag takes a path of at least two points" }));
    rig.wire_output("_error");
    let outcome = rig.run(&DragNode, json!({ "computer": computer, "args": { "path": [{ "x": 1, "y": 2 }, { "x": 30, "y": 40 }], "modifiers": "shift" } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("drag: a drag takes a path of at least two points"), "{error}");
    assert!(outcome.outputs.get("result").is_none(), "a refused action says nothing on result");
    Ok(())
}
