//! LlmInference: one language-model call, buffered. The reply comes
//! out whole on `response`, as text, when the model finishes; for live
//! deltas use LlmStream instead, and for an answer parsed as JSON and
//! held to a declared type, LlmInferenceJson.
//!
//! WHO serves the call arrives on the `provider` input: one
//! `LlmProvider` object a provider node emitted (the connection was
//! picked over there, this node opens it inside its own firing). HOW
//! the model samples comes from the optional `params` input (an
//! LlmParams node's object); absent = the defaults. The shared
//! plumbing (generator, params, history assembly, the storage
//! round-trip, the call itself) lives in the package's `call.rs`.
//!
//! CONVERSATIONS live in a file: `historyFile` names a project file
//! holding a `ChatHistory` (the typed minillmlib-shaped list whose
//! media slots hold stored files). The call sends what the file holds
//! plus this call's turn, then adds the turn and the reply to the end
//! of that same file in place, and the file goes out on `historyFile`.
//! With no file in, a wired `historyFile` output starts a new one.
//! TOOLS: wired LlmTool declarations ride to the provider; the model's
//! calls come out on `toolCalls` (and are saved in the file), the graph
//! runs each tool, and a Chat Message node (role: tool) appends the
//! results to the file for the next call, which is why `prompt` is
//! optional once a file is wired.
//!
//! The call streams internally so a Stop lands mid-generation
//! (`call::complete`); this node holds no cost bookkeeping at all.

use async_trait::async_trait;

use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::call;

#[derive(NodeManifest)]
pub struct LlmInferenceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for LlmInferenceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let (llm, response) = call::complete(&ctx).await?;
        let output = call::finish(&ctx, llm, &response, call::reply_text).await?;
        ctx.pulse_downstream(output).await
    }
}
