//! Screenshot: capture the computer's whole screen.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{Node, NodeErrExt, NodeManifest, RunContext, WeftResult};

use super::actions;

#[derive(NodeManifest)]
pub struct ScreenshotNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ScreenshotNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let answer = actions::act(&ctx, actions::SCREENSHOT, json!({})).await?;
        let width = answer["width"].as_u64().node_err(format!("the computer answered a screenshot with no width: {answer}"))?;
        let height = answer["height"].as_u64().node_err(format!("the computer answered a screenshot with no height: {answer}"))?;
        let image = actions::store_png(&ctx, &answer, "screenshot.png").await?;
        let said = format!("Took a screenshot of the {width}x{height} screen.");
        ctx.pulse_downstream(NodeOutput::new().set("image", image).set("result", said)).await
    }
}
