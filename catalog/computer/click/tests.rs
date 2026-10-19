//! Click self-tests: the action it asks the computer for, and a refusal
//! reaching `_error`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::ClickNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("asks_the_computer_and_says_what_it_did", acts),
        NodeTest::fake("a_refusal_from_the_computer_reaches_error", refused),
    ]
}

async fn acts(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let outcome = rig.run(&ClickNode, json!({ "computer": computer, "args": { "x": 120, "y": 80, "button": "right", "count": 2, "modifiers": "ctrl" } })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Clicked."));
    assert_eq!(asked(&rig), vec![("click".to_string(), json!({ "x": 120, "y": 80, "button": "right", "count": 2, "modifiers": "ctrl" }))], "the args go to the computer as they came");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "(130, 900) is off the 1280x800 screen" }));
    rig.wire_output("_error");
    let outcome = rig.run(&ClickNode, json!({ "computer": computer, "args": { "x": 120, "y": 80, "button": "right", "count": 2, "modifiers": "ctrl" } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("click: (130, 900) is off the 1280x800 screen"), "{error}");
    assert!(outcome.outputs.get("result").is_none(), "a refused action says nothing on result");
    Ok(())
}
