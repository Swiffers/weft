//! ElevenLabsDesignVoice: generate voice PREVIEWS from a text
//! description (and, on eleven_ttv_v3, a reference recording). Each
//! preview carries its audio and a generated voice id; nothing lands on
//! the account until Create Voice mints the preferred one into a
//! durable voice.

use async_trait::async_trait;
use base64::Engine as _;
use serde_json::json;

use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

use super::flows::set_some;
use super::voice_previews::{emit_previews, line_refusal, preview_request};

#[derive(NodeManifest)]
pub struct ElevenLabsDesignVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The one model ElevenLabs lets take reference audio (and so the
/// prompt strength that weighs it against the description).
const REFERENCE_MODEL: &str = "eleven_ttv_v3";

#[async_trait]
impl Node for ElevenLabsDesignVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let description: String = ctx.inputs.get("description")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let text: Option<String> = ctx.inputs.opt::<String>("text")?.filter(|t| !t.trim().is_empty());
        let reference: Option<FileHandle> = ctx.inputs.opt("referenceAudio")?;
        let prompt_strength: Option<f64> = ctx.inputs.opt("promptStrength")?;
        let enhance: bool = ctx.inputs.get("shouldEnhance")?;

        if let Some(refusal) =
            refusal(&description, text.as_deref(), &model, reference.is_some(), prompt_strength.is_some())
        {
            return Err(WeftError::Input(refusal));
        }
        // The fields both preview routes take, then the ones only
        // designing takes.
        let mut body = preview_request(&ctx, &description, text.as_deref())?;
        body["model_id"] = json!(model);
        set_some(&mut body, "quality", ctx.inputs.opt::<f64>("quality")?);
        if enhance {
            body["should_enhance"] = json!(true);
        }
        if let Some(file) = &reference {
            let (_, bytes) = ctx.storage(StorageScope::Execution).get_bytes(file).await?;
            body["reference_audio_base64"] = json!(base64::engine::general_purpose::STANDARD.encode(bytes));
        }
        emit_previews(&ctx, &account, "design", &body, &output_format, "design the voice on ElevenLabs").await
    }
}

/// What ElevenLabs's voice design route would refuse, said before
/// anything is paid for (the compiler refuses the same when it is
/// written; this is the floor for wired values), or `None` when the
/// request is fine. Lengths count characters, as ElevenLabs does.
fn refusal(
    description: &str,
    text: Option<&str>,
    model: &str,
    reference: bool,
    prompt_strength: bool,
) -> Option<String> {
    let described = description.chars().count();
    if !(20..=1000).contains(&described) {
        return Some(format!(
            "`description` is {described} characters, and ElevenLabs takes 20 to 1000: {}",
            if described < 20 { "describe the voice in more detail" } else { "shorten it" }
        ));
    }
    if let Some(refusal) = line_refusal(text) {
        return Some(refusal);
    }
    if reference && model != REFERENCE_MODEL {
        return Some(format!(
            "ElevenLabs takes `referenceAudio` only on {REFERENCE_MODEL}, and `model` is {model}: set `model` to \
             {REFERENCE_MODEL}, or leave `referenceAudio` empty"
        ));
    }
    if prompt_strength && !reference {
        return Some(
            "`promptStrength` weighs the description against `referenceAudio`, and no `referenceAudio` is given: \
             wire one in, or leave `promptStrength` empty"
                .into(),
        );
    }
    None
}
