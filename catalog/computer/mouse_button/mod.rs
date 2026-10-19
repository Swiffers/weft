//! MouseButton: press or release a mouse button where the mouse is.

use async_trait::async_trait;
use serde_json::Value;

use weft::{node_bail, Node, NodeManifest, RunContext, WeftResult};

use super::actions;

#[derive(NodeManifest)]
pub struct MouseButtonNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for MouseButtonNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let args: Value = ctx.inputs.get("args")?;
        let said = match args["action"].as_str() {
            Some("down") => "Pressed the button down.",
            Some("up") => "Released the button.",
            _ => node_bail!("the mouse button's action is down or up; got {}", args["action"]),
        };
        actions::act_and_say(&ctx, actions::MOUSE, args, said).await
    }
}
