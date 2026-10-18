//! GetNodeState: where one node of a run stands, read from that run's
//! record: its status, the raw last answer its wait saw, or its error.
//!
//! The node is passed on exactly as written. A name the program does not
//! have, a group, or a node inside a loop is refused before the program
//! runs when it is written literally (the metadata rule), and by the
//! dispatcher when it is wired.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{RunContext, RunId, Node, NodeManifest, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct GetNodeStateNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GetNodeStateNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let given: String = ctx.inputs.get("runId")?;
        let run: RunId = given.trim().parse().map_err(|_| {
            WeftError::Input(format!("runId takes a run id (the one GetRunId gives), so it cannot be '{given}'"))
        })?;
        let node: String = ctx.inputs.get("node")?;
        let state = ctx.node_state(run, node).await?;
        ctx.pulse_downstream(NodeOutput::new().set_serialized("state", &state)?).await
    }
}
