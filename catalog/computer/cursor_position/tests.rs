//! CursorPosition self-tests: the position as a model reads it and as
//! numbers, and a malformed answer failing loud.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::CursorPositionNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("says_where_the_mouse_is", reads),
        NodeTest::fake("an_answer_with_no_position_fails_loud", malformed),
    ]
}

async fn reads(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "x": 512, "y": 384 }));
    let outcome = rig.run(&CursorPositionNode, json!({ "computer": computer })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("X=512, Y=384"));
    assert_eq!(outcome.outputs["x"], json!(512.0));
    assert_eq!(outcome.outputs["y"], json!(384.0));
    assert_eq!(asked(&rig), vec![("cursor".to_string(), json!({}))]);
    Ok(())
}

async fn malformed(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "x": 512 }));
    let outcome = rig.run(&CursorPositionNode, json!({ "computer": computer })).await;
    let error = outcome.result.expect_err("half a position").to_string();
    assert!(error.contains("no y"), "{error}");
    Ok(())
}
