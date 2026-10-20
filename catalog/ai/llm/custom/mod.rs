//! CustomProvider: any OpenAI-compatible endpoint as an LLM provider.
//! Owns the endpoint's base URL, the model name, and (optionally) a
//! pasted-key connection, and emits them as one `LlmProvider` object
//! any inference node consumes. The connection is optional on purpose:
//! a local or otherwise unauthenticated compatible server (an Ollama,
//! a vLLM behind your own wall) takes bare calls.

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::provider;

#[derive(NodeManifest)]
pub struct CustomProviderNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for CustomProviderNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // The recipe's `connectionOptional` makes an unpicked connection
        // `None` here, and the object then carries no `account`.
        let account = ctx.inputs.access(provider::CONNECTION_INPUT)?;
        provider::emit(&ctx, "custom", account).await
    }
}
