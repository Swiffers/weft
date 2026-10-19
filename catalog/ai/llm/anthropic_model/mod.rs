//! AnthropicModel: the native-Anthropic half of an LLM call, as one
//! value. Names the model and emits it with the connection wired in
//! from an AnthropicAccess node as one `LlmProvider` object any
//! inference node consumes. The call itself speaks Anthropic's own
//! `/v1/messages` wire (minillmlib's Anthropic dialect), so model names
//! are Anthropic's, not OpenRouter's.

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct AnthropicModelNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for AnthropicModelNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        provider::emit_model(&ctx, "anthropic").await
    }
}
