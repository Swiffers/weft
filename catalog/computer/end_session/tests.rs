//! EndSession self-tests: the session asked to end, the default one when
//! none is named, and the answer read back.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer};
use super::EndSessionNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("ends_the_named_session", named),
        NodeTest::fake("no_name_ends_the_default_session", default_session),
        NodeTest::fake("an_answer_that_says_nothing_fails_loud", malformed),
    ]
}

async fn named(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "ended": true }));
    let outcome = rig.run(&EndSessionNode, json!({ "computer": computer, "session": "run-1" })).await.ok()?;
    assert_eq!(outcome.outputs["ended"], json!(true));
    assert_eq!(asked(&rig), vec![("end_session".to_string(), json!({ "session": "run-1" }))]);
    Ok(())
}

async fn default_session(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "ended": false }));
    let outcome = rig.run(&EndSessionNode, json!({ "computer": computer })).await.ok()?;
    assert_eq!(outcome.outputs["ended"], json!(false), "nothing to end is no failure");
    assert_eq!(asked(&rig), vec![("end_session".to_string(), json!({ "session": "" }))]);
    Ok(())
}

async fn malformed(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({}));
    let outcome = rig.run(&EndSessionNode, json!({ "computer": computer, "session": "x" })).await;
    let error = outcome.result.expect_err("no answer").to_string();
    assert!(error.contains("without saying whether there was one"), "{error}");
    Ok(())
}
