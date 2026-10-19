//! Zoom: capture one region of the computer's screen, enlarged.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Node, NodeManifest, RunContext, WeftResult};

use super::actions;

#[derive(NodeManifest)]
pub struct ZoomNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ZoomNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let args: Value = ctx.inputs.get("args")?;
        let answer = actions::act(&ctx, actions::ZOOM, args).await?;
        let image = actions::store_png(&ctx, &answer, "zoom.png").await?;
        ctx.pulse_downstream(NodeOutput::new().set("image", image).set("result", "Zoomed in on the region.")).await
    }
}
