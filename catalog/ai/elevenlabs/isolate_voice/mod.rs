//! ElevenLabsIsolateVoice: strip background noise from a recording,
//! keeping only the voice: POST the audio to the audio-isolation
//! route and store the cleaned answer under the type the answer says
//! it is.

use async_trait::async_trait;

use weft::access::client::Multipart;
use weft::node::NodeOutput;
use weft::reqwest::header::CONTENT_TYPE;
use weft::storage::{normalize_content_type, FileHandle, StorageScope};
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::{audio_file_type, input_file_format, store_audio, API};

#[derive(NodeManifest)]
pub struct ElevenLabsIsolateVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

const WHAT: &str = "isolate the voice on ElevenLabs";

/// The stored name of the cleaned audio: the recording's name with
/// `isolated_` before it and the extension of the type ElevenLabs
/// answered with. The recording's own extension never carries over
/// (it would name mp3 bytes `.wav`), and a type no format family names
/// leaves the name without one rather than guess.
fn isolated_name(recording: &str, answered_mime: &str) -> String {
    let stem = match recording.rsplit_once('.') {
        Some((stem, _)) if !stem.is_empty() => stem,
        _ => recording,
    };
    match extension_of(answered_mime) {
        Some(extension) => format!("isolated_{stem}.{extension}"),
        None => format!("isolated_{stem}"),
    }
}

/// The extension [`audio_file_type`] gives the type `mime`, found by
/// asking it for every format family it names.
fn extension_of(mime: &str) -> Option<&'static str> {
    let bare = |m: &str| m.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    let wanted = bare(mime);
    ["mp3", "pcm", "wav", "ulaw", "alaw", "opus"]
        .into_iter()
        .filter_map(|family| audio_file_type(family).ok())
        .find(|&(_, family_mime)| bare(family_mime) == wanted)
        .map(|(extension, _)| extension)
}

#[async_trait]
impl Node for ElevenLabsIsolateVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let audio: FileHandle = ctx.inputs.get("audio")?;

        let (meta, bytes) = ctx.storage(StorageScope::Run).get_bytes(&audio).await?;
        let file_format = input_file_format(&ctx, &meta.mime_type)?;
        let mut form = Multipart::form_data().file("audio", &meta.filename, &meta.mime_type, bytes);
        if let Some(file_format) = file_format {
            form = form.text("file_format", file_format);
        }
        let (content_type, body) = form.build();

        let http = ctx.client(&account).await?;
        let resp = http
            .post(format!("{API}/audio-isolation"))
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .node_err(WHAT)?;
        // The route takes no output format, and ElevenLabs's reference
        // declares its answer as audio/mpeg whatever was sent, so the
        // answer's own type is the truth about the bytes, never the
        // recording's.
        let mime = normalize_content_type(resp.headers().get(CONTENT_TYPE).and_then(|v| v.to_str().ok()));
        let stored = store_audio(&ctx, resp, WHAT, &isolated_name(&meta.filename, &mime), &mime).await?;
        ctx.pulse_downstream(NodeOutput::stored_file(stored)).await
    }
}
