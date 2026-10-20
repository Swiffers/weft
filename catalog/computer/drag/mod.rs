//! Drag: press a mouse button, move along a path, release.

use async_trait::async_trait;
use serde_json::Value;

use weft::{Node, NodeManifest, RunContext, WeftResult};

use super::actions;

#[derive(NodeManifest)]
pub struct DragNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DragNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let args: Value = ctx.inputs.get("args")?;
        actions::act_and_say(&ctx, actions::DRAG, args, "Dragged.").await
    }
}
