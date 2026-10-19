//! Shout: upper-cases its text. The line marked below does not compile, on
//! purpose: the e2e rig proves the worker is built without this package.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ShoutNode;

#[async_trait]
impl Node for ShoutNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let text: String = ctx.inputs.get("text")?;
        // Does not compile: a String is no number.
        let loud: u32 = text.to_uppercase();
        ctx.pulse_downstream(NodeOutput::new().set("text", loud)).await
    }
}
