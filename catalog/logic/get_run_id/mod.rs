//! GetRunId: this run's id, so a program can store it beside its own
//! data and find the run again later.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GetRunIdNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GetRunIdNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let run_id = ctx.run_id.to_string();
        ctx.pulse_downstream(NodeOutput::new().set("runId", run_id)).await
    }
}
