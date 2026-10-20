//! PressKey self-tests: the action it asks the computer for, and a refusal
//! reaching `_error`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::PressKeyNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("asks_the_computer_and_says_what_it_did", acts),
        NodeTest::fake("a_refusal_from_the_computer_reaches_error", refused),
    ]
}

async fn acts(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let outcome = rig.run(&PressKeyNode, json!({ "computer": computer, "args": { "keys": "ctrl+s", "repeat": 2 } })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Pressed."));
    assert_eq!(asked(&rig), vec![("key".to_string(), json!({ "keys": "ctrl+s", "repeat": 2 }))], "the args go to the computer as they came");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "pressing keys failed: (symbol) No such key name 'Nope'. Ignoring it." }));
    rig.wire_output("_error");
    let outcome = rig.run(&PressKeyNode, json!({ "computer": computer, "args": { "keys": "ctrl+s", "repeat": 2 } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("key: pressing keys failed: (symbol) No such key name 'Nope'. Ignoring it."), "{error}");
    assert!(outcome.outputs.get("result").is_none(), "a refused action says nothing on result");
    Ok(())
}
