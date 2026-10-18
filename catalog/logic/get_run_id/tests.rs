//! GetRunId self-tests: the node emits the id of the run it fires in. The
//! rig runs the node pure, as its metadata marks it, so any call reaching
//! outside the run would fail it.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::GetRunIdNode;

pub fn tests() -> Vec<NodeTest> {
    vec![NodeTest::fake("emits_this_runs_id", emits_this_runs_id)]
}

async fn emits_this_runs_id(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&GetRunIdNode, json!({})).await.ok()?;
    let run_id = outcome.run_id.expect("the body ran with a ctx");
    assert_eq!(outcome.outputs["runId"], json!(run_id.to_string()));
    Ok(())
}
