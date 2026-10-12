//! ElevenLabsMusic: a text prompt, or a composition plan laid out chunk
//! by chunk, to a music track: POST it to the music route, store the
//! answered audio, and emit it with the id ElevenLabs gave the song.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::{Access, ExecutionContext, Node, NodeManifest, WeftError, WeftResult};

use super::elevenlabs::{audio_file_type, emit_audio_with_id, API};
use super::flows::set_some;

/// The output format that lets ElevenLabs pick the best one for the
/// model.
const AUTO: &str = "auto";
/// The longest prompt and the longest lyrics ElevenLabs takes, in
/// characters.
const MAX_PROMPT_CHARS: usize = 4_100;
const MAX_LYRICS_CHARS: usize = 4_000;

#[derive(NodeManifest)]
pub struct ElevenLabsMusicNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsMusicNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let output_format: String = ctx.inputs.get("outputFormat")?;
        let body = request_body(&ctx)?;
        if let Some(refusal) = refusal(&body, &output_format) {
            return Err(WeftError::Input(refusal));
        }
        let (extension, mime) = file_type(&output_format)?;

        let http = ctx.client(&account).await?;
        let url = format!("{API}/music?output_format={}", urlencoding::encode(&output_format));
        let what = "compose the music on ElevenLabs";
        let filename = format!("music.{extension}");
        emit_audio_with_id(&ctx, http.post(url).json(&body), what, &filename, mime, ("song-id", "songId")).await
    }
}

/// The JSON body from the node's inputs: every setting it was given,
/// and nothing for one left empty or off, so ElevenLabs applies its own
/// default there.
fn request_body(ctx: &ExecutionContext) -> WeftResult<Value> {
    let prompt: Option<String> = ctx.inputs.opt("prompt")?;
    let plan: Option<Value> = ctx.inputs.opt("compositionPlan")?;
    let model: String = ctx.inputs.get("model")?;
    // Whole numbers: the widgets' step holds a wired value to whole
    // numbers too.
    let length_secs: Option<u64> = ctx.inputs.opt("lengthSecs")?;
    let seed: Option<u64> = ctx.inputs.opt("seed")?;
    let mode: Option<String> = ctx.inputs.opt("generationMode")?;
    let lyrics: Option<String> = ctx.inputs.opt("lyrics")?;
    let finetune: Option<String> = ctx.inputs.opt("finetune")?;
    let strength: Option<f64> = ctx.inputs.opt("finetuneStrength")?;

    let mut body = json!({ "model_id": model });
    set_some(&mut body, "prompt", prompt);
    set_some(&mut body, "composition_plan", plan);
    set_some(&mut body, "music_length_ms", length_secs.map(|secs| secs * 1000));
    set_some(&mut body, "generation_mode", mode);
    set_some(&mut body, "lyrics_text", lyrics);
    set_some(&mut body, "seed", seed);
    set_some(&mut body, "finetune_id", finetune);
    set_some(&mut body, "finetune_strength", strength);
    for (port, field) in [
        ("forceInstrumental", "force_instrumental"),
        ("usePhoneticNames", "use_phonetic_names"),
        ("storeForInpainting", "store_for_inpainting"),
        ("signWithC2pa", "sign_with_c2pa"),
    ] {
        if ctx.inputs.get::<bool>(port)? {
            body[field] = json!(true);
        }
    }
    Ok(body)
}

/// What ElevenLabs refuses in a music request, or would take and throw
/// away, said before anything is paid for (the compiler refuses the
/// same shapes when they are written; this is the floor for wired
/// ones), or `None` when `body` can go out with `output_format`. It
/// reads the body itself, so it judges exactly what would be sent.
fn refusal(body: &Value, output_format: &str) -> Option<String> {
    let has = |field: &str| body.get(field).is_some();
    match (body.get("prompt").and_then(Value::as_str), has("composition_plan")) {
        (Some(_), true) => {
            return Some(
                "both `prompt` and `compositionPlan` are set, and ElevenLabs composes from one of them: keep one".into(),
            )
        }
        (None, false) => return Some("there is nothing to compose from: give a `prompt` or a `compositionPlan`".into()),
        (Some(prompt), false) => {
            if has("seed") {
                return Some(
                    "ElevenLabs takes a `seed` only with a `compositionPlan`: compose from a plan, or leave `seed` empty"
                        .into(),
                );
            }
            let chars = prompt.chars().count();
            if chars > MAX_PROMPT_CHARS {
                return Some(format!(
                    "`prompt` is {chars} characters, and ElevenLabs takes at most {MAX_PROMPT_CHARS}: shorten it, \
                     or lay the song out in a `compositionPlan`"
                ));
            }
        }
        (None, true) => {
            for (field, port) in [
                ("music_length_ms", "lengthSecs"),
                ("force_instrumental", "forceInstrumental"),
                ("generation_mode", "generationMode"),
            ] {
                if has(field) {
                    return Some(format!(
                        "ElevenLabs takes `{port}` only with a `prompt`, and this composes from a \
                         `compositionPlan`: leave `{port}` empty, or compose from a `prompt`"
                    ));
                }
            }
        }
    }
    if let Some(lyrics) = body.get("lyrics_text").and_then(Value::as_str) {
        if has("force_instrumental") {
            return Some(
                "`lyrics` are given and `forceInstrumental` makes sure nothing is sung: keep one of them".into(),
            );
        }
        let chars = lyrics.chars().count();
        if chars > MAX_LYRICS_CHARS {
            return Some(format!(
                "`lyrics` are {chars} characters, and ElevenLabs takes at most {MAX_LYRICS_CHARS}: shorten them"
            ));
        }
    }
    if let Some(strength) = body.get("finetune_strength").and_then(Value::as_f64) {
        if !has("finetune_id") {
            return Some(
                "`finetuneStrength` is set and no `finetune` is picked for it to act on: pick a `finetune`, or \
                 leave `finetuneStrength` empty"
                    .into(),
            );
        }
        if strength <= 0.0 {
            return Some(format!(
                "`finetuneStrength` is {strength}, and ElevenLabs takes a strength above 0: raise it, or leave \
                 `finetune` empty to compose without one"
            ));
        }
    }
    if has("sign_with_c2pa") && !(output_format == AUTO || output_format.starts_with("mp3_")) {
        return Some(format!(
            "ElevenLabs signs with C2PA only an mp3, and `outputFormat` is '{output_format}': pick an mp3 format \
             or auto, or turn `signWithC2pa` off"
        ));
    }
    None
}

/// The stored file's extension and mime. ElevenLabs answers `auto`
/// with an mp3 whatever the model (mp3_44100_128 on its v1 models,
/// mp3_48000_192 on its v2 models), so `auto` is stored as one.
fn file_type(output_format: &str) -> WeftResult<(&'static str, &'static str)> {
    audio_file_type(if output_format == AUTO { "mp3" } else { output_format })
}
