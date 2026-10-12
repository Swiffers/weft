//! What ElevenLabs's two speech routes (text to speech and text to
//! dialogue) take beyond the text and the voice settings, read from the
//! same-named inputs of the node that speaks: the language, the seed,
//! number spelling, pronunciation dictionaries, request stitching, the
//! professional voice switch, audio effects and zero retention. Both
//! nodes end on `emit_audio_with_id`, giving out the `request-id` a later
//! request stitches to.

use serde_json::{json, Map, Value};

use weft::{ExecutionContext, NodeErrExt, WeftError, WeftResult};

use super::flows::set_some;

/// The most pronunciation dictionaries one request applies, and the
/// most request ids it takes on either side (ElevenLabs's limits).
const MAX_DICTIONARIES: usize = 3;
const MAX_REQUEST_IDS: usize = 3;

/// The query a speech request goes out with: its output format, and
/// zero retention when the node turned logging off.
pub fn query(ctx: &ExecutionContext, output_format: &str) -> WeftResult<String> {
    let logging: bool = ctx.inputs.get("enableLogging")?;
    let mut query = format!("output_format={}", urlencoding::encode(output_format));
    if !logging {
        query.push_str("&enable_logging=false");
    }
    Ok(query)
}

/// Put every shared setting the node was given on `body`, refusing
/// before anything is sent what ElevenLabs would refuse.
pub fn shared_settings(ctx: &ExecutionContext, body: &mut Value, output_format: &str) -> WeftResult<()> {
    let language: Option<String> = ctx.inputs.opt("languageCode")?;
    // A whole number: the widget's step holds a wired value to whole
    // numbers too.
    let seed: Option<f64> = ctx.inputs.opt("seed")?;
    let normalization: Option<String> = ctx.inputs.opt("textNormalization")?;
    let pvc_as_ivc: bool = ctx.inputs.get("usePvcAsIvc")?;
    let dictionaries: Vec<Value> = ctx.inputs.list("pronunciationDictionaries")?;
    let previous_ids: Vec<String> = ctx.inputs.list("previousRequestIds")?;
    let next_ids: Vec<String> = ctx.inputs.list("nextRequestIds")?;
    let logging: bool = ctx.inputs.get("enableLogging")?;

    // Zero retention keeps no history, and stitching reads the history
    // of the requests it names (the compiler refuses the pair when it
    // is written; this is the floor for a wired one).
    if !logging && !(previous_ids.is_empty() && next_ids.is_empty()) {
        return Err(WeftError::Input(
            "`enableLogging` is off, and ElevenLabs cannot carry a request it keeps no history of on from or into \
             other requests: turn `enableLogging` on, or leave `previousRequestIds` and `nextRequestIds` empty"
                .into(),
        ));
    }
    set_some(body, "language_code", language);
    set_some(body, "seed", seed.map(|s| s as u64));
    set_some(body, "apply_text_normalization", normalization);
    if pvc_as_ivc {
        body["use_pvc_as_ivc"] = json!(true);
    }
    if dictionaries.len() > MAX_DICTIONARIES {
        return Err(WeftError::Input(format!(
            "`pronunciationDictionaries` lists {} dictionaries, and ElevenLabs applies at most {MAX_DICTIONARIES} to one request: \
             keep {MAX_DICTIONARIES} or fewer",
            dictionaries.len()
        )));
    }
    if !dictionaries.is_empty() {
        body["pronunciation_dictionary_locators"] =
            dictionaries.iter().map(dictionary_locator).collect::<WeftResult<Vec<_>>>()?.into();
    }
    for (port, field, ids) in
        [("previousRequestIds", "previous_request_ids", previous_ids), ("nextRequestIds", "next_request_ids", next_ids)]
    {
        if ids.len() > MAX_REQUEST_IDS {
            return Err(WeftError::Input(format!(
                "`{port}` lists {} request ids, and ElevenLabs takes at most {MAX_REQUEST_IDS}: keep the \
                 {MAX_REQUEST_IDS} closest to this request",
                ids.len()
            )));
        }
        if !ids.is_empty() {
            body[field] = json!(ids);
        }
    }
    if let Some(effects) = audio_effects(ctx, output_format)? {
        body["audio_effects"] = effects;
    }
    Ok(())
}

/// The `voice_settings` object from the node's `stability`,
/// `similarityBoost`, `style`, `speed` and `speakerBoost` inputs, or
/// `None` when none is set, so the voice keeps its own settings.
pub fn voice_settings(ctx: &ExecutionContext) -> WeftResult<Option<Value>> {
    let mut settings = Map::new();
    for (port, key) in [("stability", "stability"), ("similarityBoost", "similarity_boost"), ("style", "style"), ("speed", "speed")] {
        if let Some(v) = ctx.inputs.opt::<f64>(port)? {
            settings.insert(key.into(), json!(v));
        }
    }
    if let Some(v) = ctx.inputs.opt::<bool>("speakerBoost")? {
        settings.insert("use_speaker_boost".into(), json!(v));
    }
    Ok((!settings.is_empty()).then(|| settings.into()))
}

/// One entry of `pronunciationDictionaries` as ElevenLabs's locator: a
/// bare id applies the dictionary's latest version, `{ id, version }`
/// pins one.
fn dictionary_locator(entry: &Value) -> WeftResult<Value> {
    let (id, version) = match entry {
        Value::String(id) => (id.as_str(), None),
        Value::Object(o) => (
            o.get("id").and_then(Value::as_str).node_err("a `pronunciationDictionaries` entry has no `id`")?,
            o.get("version").and_then(Value::as_str),
        ),
        other => weft::node_bail!("a `pronunciationDictionaries` entry is neither an id nor `{{ id, version }}`: {other}"),
    };
    let mut locator = json!({ "pronunciation_dictionary_id": id });
    set_some(&mut locator, "version_id", version);
    Ok(locator)
}

/// The `audio_effects` object, or `None` when the node set no effect.
fn audio_effects(ctx: &ExecutionContext, output_format: &str) -> WeftResult<Option<Value>> {
    let filter: Option<String> = ctx.inputs.opt("filterPreset")?;
    let distance: Option<f64> = ctx.inputs.opt("distance")?;
    let environment: Option<String> = ctx.inputs.opt("environment")?;
    let send_level: Option<f64> = ctx.inputs.opt("sendLevel")?;
    let noise: Option<String> = ctx.inputs.opt("backgroundNoise")?;
    let pan: Option<f64> = ctx.inputs.opt("pan")?;

    if send_level.is_some() && environment.is_none() {
        return Err(WeftError::Input(
            "`sendLevel` sets how much of the voice feeds the reverb, but no `environment` is set to make one: \
             pick an `environment`, or leave `sendLevel` empty"
                .into(),
        ));
    }
    if let Some(refusal) = stereo_refusal(output_format, environment.is_some(), send_level, pan) {
        return Err(WeftError::Input(refusal));
    }
    let mut effects = Map::new();
    let mut put = |key: &str, value: Option<Value>| {
        if let Some(v) = value {
            effects.insert(key.into(), v);
        }
    };
    put("filter_preset_id", filter.map(Value::from));
    put("distance", distance.map(Value::from));
    put("environment_id", environment.map(Value::from));
    put("send_level", send_level.map(Value::from));
    put("background_noise_id", noise.map(Value::from));
    put("pan", pan.map(Value::from));
    Ok((!effects.is_empty()).then(|| effects.into()))
}

/// ElevenLabs renders reverb (an environment whose send level is above
/// 0, and 1 when unset) and a pan away from the centre in stereo, and
/// refuses stereo audio in its pcm, ulaw and alaw formats. The refusal, said
/// before anything is paid for, or `None` when the pair is fine.
pub fn stereo_refusal(
    output_format: &str,
    environment: bool,
    send_level: Option<f64>,
    pan: Option<f64>,
) -> Option<String> {
    let mono = ["pcm_", "ulaw_", "alaw_"].iter().any(|p| output_format.starts_with(p));
    if !mono {
        return None;
    }
    let effect = if environment && send_level.unwrap_or(1.0) > 0.0 {
        "the reverb `environment`"
    } else if pan.is_some_and(|p| p != 0.0) {
        "`pan`"
    } else {
        return None;
    };
    Some(format!(
        "{effect} makes the audio stereo, and ElevenLabs refuses stereo audio in '{output_format}': \
         pick an mp3, wav or opus `outputFormat`, or leave {effect} empty"
    ))
}
