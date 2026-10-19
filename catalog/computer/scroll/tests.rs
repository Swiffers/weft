//! Scroll self-tests: the action it asks the computer for, and a refusal
//! reaching `_error`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::ScrollNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("asks_the_computer_and_says_what_it_did", acts),
        NodeTest::fake("a_refusal_from_the_computer_reaches_error", refused),
    ]
}

async fn acts(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let outcome = rig.run(&ScrollNode, json!({ "computer": computer, "args": { "x": 10, "y": 20, "direction": "down", "amount": 3 } })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Scrolled."));
    assert_eq!(asked(&rig), vec![("scroll".to_string(), json!({ "x": 10, "y": 20, "direction": "down", "amount": 3 }))], "the args go to the computer as they came");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "amount is a whole number of 1 or more; got 0" }));
    rig.wire_output("_error");
    let outcome = rig.run(&ScrollNode, json!({ "computer": computer, "args": { "x": 10, "y": 20, "direction": "down", "amount": 3 } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("scroll: amount is a whole number of 1 or more; got 0"), "{error}");
    assert!(outcome.outputs.get("result").is_none(), "a refused action says nothing on result");
    Ok(())
}
