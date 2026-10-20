//! MouseButton self-tests: down and up, each said as what it did, and
//! a refusal reaching `_error`.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::MouseButtonNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("presses_the_button_down", down),
        NodeTest::fake("lets_the_button_up", up),
        NodeTest::fake("a_refusal_from_the_computer_reaches_error", refused),
        NodeTest::fake("an_action_that_is_neither_down_nor_up_is_refused_before_the_computer", sideways),
    ]
}

async fn down(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let args = json!({ "action": "down", "button": "left" });
    let outcome = rig.run(&MouseButtonNode, json!({ "computer": computer, "args": args })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Pressed the button down."));
    assert_eq!(asked(&rig), vec![("mouse".to_string(), args)]);
    Ok(())
}

async fn up(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let outcome = rig.run(&MouseButtonNode, json!({ "computer": computer, "args": { "action": "up" } })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Released the button."));
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "no mouse button 'side'; the buttons are left, middle, right, back, forward" }));
    rig.wire_output("_error");
    let outcome = rig.run(&MouseButtonNode, json!({ "computer": computer, "args": { "action": "down", "button": "side" } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("mouse: no mouse button 'side'"), "{error}");
    Ok(())
}

async fn sideways(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    let outcome = rig.run(&MouseButtonNode, json!({ "computer": computer, "args": { "action": "sideways" } })).await;
    let error = outcome.result.expect_err("neither down nor up").to_string();
    assert!(error.contains("down or up"), "{error}");
    assert!(asked(&rig).is_empty(), "nothing reaches the computer");
    Ok(())
}
