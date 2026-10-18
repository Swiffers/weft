//! OpenAIModel: the direct-OpenAI half of an LLM call, as one value.
//! Names the model and emits it with the connection wired in from an
//! OpenAIAccess node as one `LlmProvider` object any inference node
//! consumes.

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct OpenAIModelNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for OpenAIModelNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        provider::emit_model(&ctx, "openai").await
    }
}
