//! EndSession: end one of the computer's shell sessions.

use async_trait::async_trait;
use serde_json::json;

use weft::node::NodeOutput;
use weft::{Node, NodeErrExt, NodeManifest, RunContext, WeftResult};

use super::actions;

#[derive(NodeManifest)]
pub struct EndSessionNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for EndSessionNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // The computer's default session is the empty name.
        let session: String = ctx.inputs.opt("session")?.unwrap_or_default();
        let answer = actions::act(&ctx, actions::END_SESSION, json!({ "session": session })).await?;
        let ended = answer["ended"].as_bool().node_err(format!("the computer answered an end of session without saying whether there was one: {answer}"))?;
        ctx.pulse_downstream(NodeOutput::new().set("ended", ended)).await
    }
}
