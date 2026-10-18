//! AnthropicModel self-tests: the emitted `LlmProvider` object.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::AnthropicModelNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("emits_kind_model_and_the_wired_connection", emits_provider),
        NodeTest::fake("refuses_a_value_that_is_no_connection", refuses_a_value_that_is_no_connection),
    ]
}

async fn emits_provider(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &AnthropicModelNode,
            json!({ "access": rig.access("anthropic"), "model": "claude-sonnet-4-5" }),
        )
        .await
        .ok()?;
    let provider = &outcome.outputs["provider"];
    assert_eq!(provider["kind"], json!("anthropic"));
    assert_eq!(provider["model"], json!("claude-sonnet-4-5"));
    assert!(provider["account"].get("__weft_access__").is_some(), "carries the marker");
    Ok(())
}

/// A pasted key is not a connection: the marker an Access node emits
/// is the only value `access` takes, refused at this node rather than
/// inside the inference node that would open it.
async fn refuses_a_value_that_is_no_connection(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig
        .run(&AnthropicModelNode, json!({ "access": "sk-ant-pasted", "model": "claude-sonnet-4-5" }))
        .await;
    let err = outcome.result.expect_err("a pasted key must refuse").to_string();
    assert!(err.contains("'access'"), "{err}");
    Ok(())
}
