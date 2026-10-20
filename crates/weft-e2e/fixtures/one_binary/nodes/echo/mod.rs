//! Echo: says its text back. The e2e rig rewrites the marked line to change
//! the node's code, which gives the project a new binary.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct EchoNode;

#[async_trait]
impl Node for EchoNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;
        let said = text; // the line the test rewrites
        ctx.pulse_downstream(NodeOutput::new().set("text", said)).await
    }
}
