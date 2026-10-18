//! OpenRouterModel: the OpenRouter half of an LLM call, as one value.
//! Names the model (and OpenRouter's routing knobs) and emits it with
//! the connection wired in from an OpenRouterAccess node as one
//! `LlmProvider` object any inference node consumes. Consuming nodes
//! open the connection at call time, inside their own firing (which is
//! what keeps a runtime-supplied credential's per-firing lifecycle
//! intact).

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct OpenRouterModelNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for OpenRouterModelNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        provider::emit_model(&ctx, "openrouter").await
    }
}
