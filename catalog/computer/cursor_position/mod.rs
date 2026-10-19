//! CursorPosition: read where the mouse is on the computer's screen.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{Node, NodeErrExt, NodeManifest, RunContext, WeftResult};

use super::actions;

#[derive(NodeManifest)]
pub struct CursorPositionNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CursorPositionNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let answer = actions::act(&ctx, actions::CURSOR, json!({})).await?;
        let x = answer["x"].as_f64().node_err(format!("the computer answered no x for the cursor: {answer}"))?;
        let y = answer["y"].as_f64().node_err(format!("the computer answered no y for the cursor: {answer}"))?;
        // The spelling Anthropic's computer tool answers its own
        // cursor_position with, which models were trained to read.
        let said = format!("X={x}, Y={y}");
        ctx.pulse_downstream(NodeOutput::new().set("result", said).set("x", x).set("y", y)).await
    }
}
