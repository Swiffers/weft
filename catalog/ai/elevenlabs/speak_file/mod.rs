//! ElevenLabsSpeakFile: batch text-to-speech. Text in, one audio file
//! out (the non-streaming sibling of the realtime voice loop): POST
//! the text to the voice's TTS route and store the answered audio.

use async_trait::async_trait;
use serde_json::json;

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio_with_id, API};
use super::flows::set_some;
use super::speech::{query, shared_settings, voice_settings};

#[derive(NodeManifest)]
pub struct ElevenLabsSpeakFileNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsSpeakFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let voice: String = ctx.inputs.get("voice")?;
        let text: String = ctx.inputs.get("text")?;
        let model: String = ctx.inputs.get("model")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let previous: Option<String> = ctx.inputs.opt("previousText")?;
        let next: Option<String> = ctx.inputs.opt("nextText")?;
        let japanese: bool = ctx.inputs.get("languageTextNormalization")?;

        if let Some(refusal) = refusal(
            &model,
            ctx.inputs.opt::<String>("languageCode")?.is_some(),
            previous.is_some(),
            next.is_some(),
            !ctx.inputs.list::<String>("previousRequestIds")?.is_empty(),
            !ctx.inputs.list::<String>("nextRequestIds")?.is_empty(),
        ) {
            return Err(WeftError::Input(refusal));
        }

        let mut body = json!({ "text": text, "model_id": model });
        set_some(&mut body, "voice_settings", voice_settings(&ctx)?);
        set_some(&mut body, "previous_text", previous);
        set_some(&mut body, "next_text", next);
        if japanese {
            body["apply_language_text_normalization"] = json!(true);
        }
        shared_settings(&ctx, &mut body, &output_format)?;

        let http = ctx.client(&account).await?;
        // The voice id is typed text that becomes a path segment:
        // encoded, it can only ever name a voice.
        let url = format!("{API}/text-to-speech/{}?{}", urlencoding::encode(&voice), query(&ctx, &output_format)?);
        let (extension, mime) = audio_file_type(&output_format)?;
        let filename = format!("speech.{extension}");
        let what = "turn the text into speech on ElevenLabs";
        emit_audio_with_id(&ctx, http.post(url).json(&body), what, &filename, mime, ("request-id", "requestId")).await
    }
}

/// The settings ElevenLabs's text to speech route will not take
/// together, said before anything is paid for (the compiler refuses
/// the same pairs when they are written; this is the floor for wired
/// ones), or `None` when the request is fine.
fn refusal(
    model: &str,
    language: bool,
    previous_text: bool,
    next_text: bool,
    previous_ids: bool,
    next_ids: bool,
) -> Option<String> {
    if language && model == "eleven_multilingual_v2" {
        return Some(
            "eleven_multilingual_v2 takes no `languageCode`: pick another `model`, or leave `languageCode` empty".into(),
        );
    }
    if model == "eleven_v3" && (previous_ids || next_ids) {
        return Some(
            "ElevenLabs does not take `previousRequestIds` or `nextRequestIds` on eleven_v3: pick another \
             `model`, or leave both empty"
                .into(),
        );
    }
    // ElevenLabs reads the ids and ignores the text when both are sent.
    for (text, ids, side) in [(previous_text, previous_ids, "previous"), (next_text, next_ids, "next")] {
        if text && ids {
            return Some(format!(
                "`{side}Text` and `{side}RequestIds` are both set, and ElevenLabs ignores the text when it has \
                 the ids: keep one of them"
            ));
        }
    }
    None
}
