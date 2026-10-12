//! ElevenLabsRemixVoice: generate voice PREVIEWS from a voice the
//! account can already use, changed by a description. Each preview
//! carries its audio and a generated voice id; nothing lands on the
//! account until Create Voice mints the preferred one, and the voice
//! that was remixed stays as it was.

use async_trait::async_trait;

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

use super::voice_previews::{emit_previews, line_refusal, preview_request};

#[derive(NodeManifest)]
pub struct ElevenLabsRemixVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsRemixVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let voice: String = ctx.inputs.get("voice")?;
        let description: String = ctx.inputs.get("description")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let text: Option<String> = ctx.inputs.opt::<String>("text")?.filter(|t| !t.trim().is_empty());

        if let Some(refusal) = refusal(&voice, &description, text.as_deref()) {
            return Err(WeftError::Input(refusal));
        }
        let body = preview_request(&ctx, &description, text.as_deref())?;
        // The voice id is typed text that becomes a path segment:
        // encoded, it can only ever name a voice.
        let route = format!("{}/remix", urlencoding::encode(&voice));
        emit_previews(&ctx, &account, &route, &body, &output_format, "remix the voice on ElevenLabs").await
    }
}

/// What ElevenLabs's remix route would refuse, said before anything is
/// paid for (the compiler refuses the same when it is written; this is
/// the floor for wired values), or `None` when the request is fine.
/// Lengths count characters, as ElevenLabs does.
fn refusal(voice: &str, description: &str, text: Option<&str>) -> Option<String> {
    if voice.trim().is_empty() {
        return Some("`voice` is empty: pick the voice to remix, or paste its id".into());
    }
    let described = description.chars().count();
    if !(5..=1000).contains(&described) {
        return Some(format!(
            "`description` is {described} characters, and ElevenLabs takes 5 to 1000: {}",
            if described < 5 { "say what to change in more detail" } else { "shorten it" }
        ));
    }
    line_refusal(text)
}

