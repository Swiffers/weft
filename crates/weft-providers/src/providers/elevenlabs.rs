//! The ElevenLabs meter.
//!
//! Routes (relative to `https://api.elevenlabs.io/v1`):
//! - `speech-to-text/realtime` is a BILLABLE SESSION: the realtime
//!   transcription WebSocket (Scribe v2 realtime). ElevenLabs prices it by
//!   the DURATION OF INPUT AUDIO ($0.39 per hour, plus $0.08/h with keyterm
//!   prompting, $0.12/h with transcript editing, and 30% of the base with
//!   entity detection), so the session observer
//!   accrues cost from the audio chunks the CALLER sends: each
//!   `input_audio_chunk` frame carries base64 PCM whose byte count, at the
//!   session's negotiated audio format, IS a duration. Frames from the
//!   provider (transcripts) cost nothing.
//! - `text-to-speech/{voice}/stream-input` is a BILLABLE SESSION priced by
//!   the characters of text the caller sends.
//! - the one-shot routes (speech, dialogue, sound, music, project
//!   dubbing, transcription, alignment) are priced from their request or their
//!   answer; `batch_route` lists them.
//! - `flows/image` and `flows/video` (Image & Video generation) are priced
//!   from their request, at the rate a hand-captured price table states for
//!   the model at the request's settings; the job's read
//!   (`flows/{image,video}/{id}`) settles whether that price was charged.
//!   The "Image & Video" section below has the rules.
//!
//! Everything else is Unknown: the install's key never travels on it, and a
//! caller's own key passes it through unmeasured, exactly like any unknown
//! route.
//!
//! The audio format (and with it bytes-per-second) is read from the
//! session URL's query string, the same parameters the provider itself
//! reads to configure the session, so the observer and the provider price
//! the same bytes the same way. A frame that does not parse as an audio
//! chunk accrues nothing (the provider ignores or refuses it too).

use std::collections::HashMap;
use std::sync::LazyLock;

use serde_json::{json, Value};

use crate::{
    CallObservation, FollowUp, MeasuredCost, ProviderMeter, RouteClass, SessionObservation,
};

pub struct ElevenLabsMeter;

pub static ELEVENLABS: ElevenLabsMeter = ElevenLabsMeter;

crate::register_meter!(ELEVENLABS);

/// The realtime transcription route, relative to the base.
const REALTIME_STT: &str = "speech-to-text/realtime";

/// Published realtime rates (Scribe v2 Realtime, on the API pricing page
/// the batch rates below cite), USD per hour of input audio: the session,
/// and what keyterm prompting and transcript editing add on top of it.
const REALTIME_STT_USD_PER_HOUR: f64 = 0.39;
const REALTIME_KEYTERMS_USD_PER_HOUR: f64 = 0.08;
const REALTIME_EDIT_USD_PER_HOUR: f64 = 0.12;
/// Entity detection, realtime and batch alike, as a fraction of the base
/// transcription rate (the pricing page: "adds a 30% premium over the base
/// transcription price").
const ENTITY_DETECTION_PREMIUM: f64 = 0.30;

/// Bytes per second of audio for a declared format, or `None` for a format
/// this meter does not know (refused at session start rather than guessed).
/// `pcm_<rate>` is 16-bit mono (2 bytes per sample); `ulaw_8000` is 1 byte
/// per sample. A rate that is not a finite positive number is refused the
/// same way: it could never price bytes honestly.
fn bytes_per_second(audio_format: &str) -> Option<f64> {
    match audio_format {
        "ulaw_8000" => Some(8_000.0),
        _ => {
            let rate: f64 = audio_format.strip_prefix("pcm_")?.parse().ok()?;
            if !rate.is_finite() || rate <= 0.0 {
                return None;
            }
            Some(rate * 2.0)
        }
    }
}

/// The session's effective rate in USD per hour of input audio, from the
/// session URL's query parameters (the add-ons are opt-in query
/// parameters). Each add-on is billed once however many values it
/// carries: a node repeats `keyterms` once per term and
/// `entity_detection` once per category.
fn usd_per_hour(query: &str) -> f64 {
    let asked = |name: &str| {
        url::form_urlencoded::parse(query.as_bytes()).any(|(k, v)| k == name && asks_for(v.as_bytes()))
    };
    let mut rate = REALTIME_STT_USD_PER_HOUR;
    if asked("keyterms") {
        rate += REALTIME_KEYTERMS_USD_PER_HOUR;
    }
    if asked("transcript_edit") {
        rate += REALTIME_EDIT_USD_PER_HOUR;
    }
    if asked("entity_detection") {
        rate += REALTIME_STT_USD_PER_HOUR * ENTITY_DETECTION_PREMIUM;
    }
    rate
}

/// Whether a transcription setting's value turns its add-on on: any value
/// but an empty one or an empty list (a form carries a list as JSON
/// text). None of these is a flag: entity detection takes a category or
/// a list of them, and a keyterm or an edit is text.
fn asks_for(value: &[u8]) -> bool {
    !matches!(value.trim_ascii(), b"" | b"[]")
}

fn query_param(query: &str, name: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

/// The number of bytes a base64 string decodes to, from its length and
/// padding alone (no allocation; the audio itself is never needed).
/// Handles padded and unpadded forms; `None` for a length no base64
/// value can have.
fn base64_decoded_len(b64: &str) -> Option<u64> {
    let padding = b64.bytes().rev().take_while(|b| *b == b'=').count() as u64;
    let significant = (b64.len() as u64).saturating_sub(padding);
    let groups = significant / 4;
    let rem = significant % 4;
    if rem == 1 {
        return None;
    }
    Some(groups * 3 + rem.saturating_sub(1))
}

// ── Batch (one-shot) routes ─────────────────────────────────────────
//
// Published API rates (elevenlabs.io/pricing/api, checked 2026-10-11,
// the same on every plan): speech (text to speech and text to
// dialogue) $0.08 per 1k characters on v4, v3 and Multilingual v2, and
// $0.04 on the low-latency models (v4 Turbo, v3 Conversational, Flash,
// Turbo); sound effects / voice changer / voice isolator $0.12 per
// minute of audio, music $0.15 per minute, Dubbing v1 (`dubbing_v1` on
// the project API) $0.50 per minute, Dubbing v2 (`dubbing_v2`) $2.20 per
// minute, batch transcription (Scribe v2 and v2 Medical, `speech-to-text`)
// $0.22 per hour of input audio with keyterm prompting $0.05 and
// transcript editing $0.07 per hour on top, forced alignment billed at
// that base rate. The legacy
// `dubbing` route is not priced: no node calls it, and a node that does
// runs on its author's own key.
const TTS_USD_PER_1K_CHARS: f64 = 0.08;
const TTS_LOW_LATENCY_USD_PER_1K_CHARS: f64 = 0.04;
const SOUND_EFFECT_USD_PER_MINUTE: f64 = 0.12;
const VOICE_CHANGER_USD_PER_MINUTE: f64 = 0.12;
const ISOLATOR_USD_PER_MINUTE: f64 = 0.12;
const MUSIC_USD_PER_MINUTE: f64 = 0.15;
const DUBBING_USD_PER_MINUTE: f64 = 0.50;
/// Dubbing v2, the project API's other model (`dubbing_v2`); `dubbing_v1`
/// is [`DUBBING_USD_PER_MINUTE`].
const DUBBING_V2_USD_PER_MINUTE: f64 = 2.20;
const ALIGN_USD_PER_HOUR: f64 = 0.22;
const STT_USD_PER_HOUR: f64 = 0.22;
const STT_KEYTERMS_USD_PER_HOUR: f64 = 0.05;
const STT_EDIT_USD_PER_HOUR: f64 = 0.07;
/// Speaker-role detection, as a fraction of the base rate (ElevenLabs's
/// API reference: "an additional 10% surcharge on base transcription
/// cost"). Entity detection is [`ENTITY_DETECTION_PREMIUM`].
const SPEAKER_ROLES_PREMIUM: f64 = 0.10;
/// The minimums the API reference states: an edit is "billed for at
/// least 10 seconds of audio", and "when more than 100 keyterms are
/// provided, a minimum billable duration of 20 seconds applies per
/// request".
const STT_EDIT_MIN_SECONDS: f64 = 10.0;
const STT_MANY_KEYTERMS: usize = 100;
const STT_MANY_KEYTERMS_MIN_SECONDS: f64 = 20.0;

/// The worst (most minutes per byte) plausible input audio: 32 kbps
/// mono mp3 = 240 KB per minute. Input-priced routes only see the
/// upload's byte count, so this converts it to a LEAN-HIGH duration
/// bound (a wav overshoots, which is the safe direction for a bound).
const INPUT_BYTES_PER_MINUTE: f64 = 240_000.0;

/// The streaming TTS websocket (`text-to-speech/{voice}/stream-input`):
/// a billable session priced per character of TEXT the caller sends,
/// at the model's batch TTS rate. Returns the voice segment when the
/// path is exactly that shape.
fn streaming_tts_voice(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("text-to-speech/")?;
    let (voice, tail) = rest.split_once('/')?;
    (tail == "stream-input" && one_segment(voice)).then_some(voice)
}

/// The per-character rate for a speech model id: the low-latency
/// models (`eleven_v4_turbo`, `eleven_v3_conversational`, Flash and
/// Turbo) at the cheaper rate, everything else at the standard one. An
/// absent id is the route's default model (Multilingual v2 for text to
/// speech, v3 for text to dialogue), both at the standard rate.
fn tts_rate_of(model: Option<&str>) -> f64 {
    let model = model.unwrap_or("");
    if model.contains("flash") || model.contains("turbo") || model.contains("conversational") {
        TTS_LOW_LATENCY_USD_PER_1K_CHARS
    } else {
        TTS_USD_PER_1K_CHARS
    }
}

/// The streaming TTS session tap: characters of text the CALLER
/// sends accrue at the model's rate; audio frames back cost nothing.
struct StreamingTtsSession {
    usd_per_1k_chars: f64,
    model: Option<String>,
    chars: u64,
}

impl StreamingTtsSession {
    /// Dollars accrued so far.
    fn accrued_usd(&self) -> f64 {
        self.chars as f64 / 1000.0 * self.usd_per_1k_chars
    }
}

impl SessionObservation for StreamingTtsSession {
    fn on_frame_to_provider(&mut self, payload: &[u8]) {
        // Only the `text` field carries billable characters; the
        // opener's lone space and the empty closer round to nothing
        // anyway, so no special-casing.
        let Ok(msg) = serde_json::from_slice::<Value>(payload) else { return };
        if let Some(text) = msg["text"].as_str() {
            self.chars += text.chars().count() as u64;
        }
    }

    fn on_frame_to_caller(&mut self, _payload: &[u8]) {}

    fn end(self: Box<Self>, interrupted: bool) -> MeasuredCost {
        MeasuredCost {
            amount_usd: Some(self.accrued_usd()),
            model: self.model.clone(),
            metadata: json!({
                "characters": self.chars,
                "usdPer1kChars": self.usd_per_1k_chars,
                "interrupted": interrupted,
            }),
        }
    }
}

/// One clean URL segment (a voice id): no traversal, no separators.
fn one_segment(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// The batch route families this meter prices. Voice management
/// (`voices/add`, listing) is free; the voice-DESIGN routes
/// (`text-to-voice/...`) stay Unknown deliberately: their credit
/// price is not published in USD, and a guessed figure would misbill,
/// so they run on the caller's own key only until priced.
enum BatchRoute {
    Tts,
    /// Text to dialogue: every line's characters at the model's speech
    /// rate.
    Dialogue,
    Sts,
    Isolation,
    SoundEffect,
    Music,
    Align,
    /// Batch transcription (`speech-to-text`): the audio duration the
    /// answer states, every channel counted, at the hourly rate the
    /// request's settings add up to.
    Stt,
    Free,
}

fn batch_route(method: &str, path: &str) -> Option<BatchRoute> {
    match method {
        "GET" => free_read(path).then_some(BatchRoute::Free),
        "POST" => billed_post(path),
        _ => None,
    }
}

/// The cost-free reads: the pickers' lists (`voices`, `models`, the
/// filter presets, environments and noise beds of
/// `audio-effects/catalog`, the `pronunciation-dictionaries` list, the
/// account's `music/finetunes`, the agents' `convai/llm/list`) and the
/// account probe (`user`).
fn free_read(path: &str) -> bool {
    matches!(
        path,
        "voices"
            | "models"
            | "user"
            | "audio-effects/catalog"
            | "pronunciation-dictionaries"
            | "music/finetunes"
            | "convai/llm/list"
    )
}

/// The billed POST routes.
fn billed_post(path: &str) -> Option<BatchRoute> {
    if let Some(rest) = path.strip_prefix("text-to-speech/") {
        let priced = match rest.split_once('/') {
            None => one_segment(rest),
            Some((voice, variant)) => one_segment(voice) && speech_variant(variant),
        };
        return priced.then_some(BatchRoute::Tts);
    }
    if path == "text-to-dialogue" || path.strip_prefix("text-to-dialogue/").is_some_and(speech_variant) {
        return Some(BatchRoute::Dialogue);
    }
    if let Some(voice) = path.strip_prefix("speech-to-speech/") {
        return one_segment(voice).then_some(BatchRoute::Sts);
    }
    match path {
        "audio-isolation" => Some(BatchRoute::Isolation),
        "sound-generation" => Some(BatchRoute::SoundEffect),
        "music" => Some(BatchRoute::Music),
        "forced-alignment" => Some(BatchRoute::Align),
        "speech-to-text" => Some(BatchRoute::Stt),
        // `voices/add` (and the text-to-voice mint) create ACCOUNT
        // ASSETS: a voice minted through a credential lands in that
        // credential's account. Unknown keeps the runtime credential
        // off these routes; a user's own key passes as always.
        _ => None,
    }
}

/// The variants a speech route serves under its base path, each priced
/// like the base: the streamed answer, the answer with timestamps, and
/// both.
fn speech_variant(variant: &str) -> bool {
    matches!(variant, "stream" | "with-timestamps" | "stream/with-timestamps")
}

/// A TTS request's price: its text's characters at the model's rate.
/// An unparseable body prices as zero characters (the provider would
/// refuse it too).
fn tts_usd(body: &[u8]) -> f64 {
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let chars = parsed["text"].as_str().map(|t| t.chars().count()).unwrap_or(0) as f64;
    chars / 1000.0 * tts_rate_of(parsed["model_id"].as_str())
}

/// A text to dialogue request's price: the characters of every line
/// (`inputs[].text`) at the model's rate. An unparseable body prices as
/// zero characters (the provider would refuse it too).
fn dialogue_usd(body: &[u8]) -> f64 {
    let parsed: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let chars: usize = parsed["inputs"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|line| line["text"].as_str())
        .map(|text| text.chars().count())
        .sum();
    chars as f64 / 1000.0 * tts_rate_of(parsed["model_id"].as_str())
}

// ── Project dubbing (`dubbing/project/...`) ──────────────────────────
//
// The create is the spend: it charges one language's dubbing up front,
// which the project's first language target then consumes, and each
// further target is charged on its own. What a language costs is the
// source's duration at the project's model rate, and neither number
// is on the create's answer reliably (the source is fetched and
// measured just after it). So the create opens a charge under the
// project id ElevenLabs answers, and the reads of that project report
// on it: a language added counts toward the charge, the project read
// states the source's duration, and the first language target read
// as finished closes it, asking for the project itself when no read
// stated the duration yet. A project given more languages after its
// first one finished is not counted past that point.

/// A project dubbing route, with the project id it names.
enum ProjectRoute<'a> {
    /// `POST dubbing/project`: opens the charge.
    Create,
    /// `POST dubbing/project/{id}/language`: one more language.
    AddLanguage(&'a str),
    /// `GET dubbing/project/{id}/language/{language}`: a target's state.
    Language(&'a str),
    /// `GET dubbing/project/{id}`: the project, its source's duration.
    Project(&'a str),
    /// `DELETE dubbing/project/{id}`: free, and how a node clears away a
    /// project nobody keeps, on the built-in connection too.
    Delete,
}

fn project_route<'a>(method: &str, path: &'a str) -> Option<ProjectRoute<'a>> {
    if method == "POST" && path == "dubbing/project" {
        return Some(ProjectRoute::Create);
    }
    let rest = path.strip_prefix("dubbing/project/")?;
    let mut parts = rest.split('/');
    let id = parts.next().filter(|id| one_segment(id))?;
    match (method, parts.next(), parts.next(), parts.next()) {
        ("GET", None, ..) => Some(ProjectRoute::Project(id)),
        ("DELETE", None, ..) => Some(ProjectRoute::Delete),
        ("POST", Some("language"), None, _) => Some(ProjectRoute::AddLanguage(id)),
        ("GET", Some("language"), Some(language), None) if one_segment(language) => Some(ProjectRoute::Language(id)),
        _ => None,
    }
}

/// The per-minute rate of a project's dubbing model, `None` for a model
/// this meter has no published rate for.
fn project_dub_rate(model: &str) -> Option<f64> {
    match model {
        "dubbing_v1" => Some(DUBBING_USD_PER_MINUTE),
        "dubbing_v2" => Some(DUBBING_V2_USD_PER_MINUTE),
        _ => None,
    }
}

/// The source's duration in seconds, from the project itself, when no
/// read reported it before the charge had to close.
async fn project_duration(project: &str, follow_up: &FollowUp<'_>) -> anyhow::Result<f64> {
    let url = format!("{}/dubbing/project/{project}", follow_up.base_url);
    let answer: Value = follow_up.http.get(&url).send().await?.error_for_status()?.json().await?;
    answer["media"]["duration_s"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("the project states no source duration yet"))
}

/// Close a project charge: every language counted, the ones the create
/// started and the ones added after it, and at least the one the create
/// prepaid, at the source's duration and the model's rate.
fn priced_project(scratch: &Value, duration_s: f64) -> MeasuredCost {
    let model = scratch["model"].as_str().unwrap_or_default().to_string();
    let started = scratch["languageIds"].as_array().map_or(0, |ids| ids.len() as u64);
    let languages = (started + scratch["languages"].as_u64().unwrap_or(0)).max(1);
    let mut metadata = scratch.clone();
    metadata["durationS"] = json!(duration_s);
    metadata["languagesCharged"] = json!(languages);
    let Some(rate) = project_dub_rate(&model) else {
        metadata["resolution"] = json!(format!("no published rate for the dubbing model '{model}'"));
        return MeasuredCost { amount_usd: None, model: Some(model), metadata };
    };
    metadata["resolution"] =
        json!(format!("{duration_s}s x {languages} language(s) x ${rate} per minute ({model})"));
    MeasuredCost { amount_usd: Some(duration_s / 60.0 * rate * languages as f64), model: Some(model), metadata }
}

/// A lean-high minutes bound for an input-priced route, from the
/// request's byte count (a multipart upload is dominated by the audio
/// bytes). Floored at one minute: these routes bill a minimum.
fn input_minutes(body: &[u8]) -> f64 {
    (body.len() as f64 / INPUT_BYTES_PER_MINUTE).max(1.0)
}

/// Bytes per second of OUTPUT audio for the request's declared
/// `output_format` (absent = the API default, mp3_44100_128), so the
/// answered bytes are a duration. The compressed families
/// (`mp3_<rate>_<kbps>`, `opus_<rate>_<kbps>`) are kbps/8*1000; `wav_<rate>`
/// is 16-bit mono PCM like `pcm_<rate>` (its 44-byte header is noise);
/// `alaw_8000` and `ulaw_8000` are one byte per sample. An unknown
/// format prices at the default's rate.
fn output_bytes_per_second(query: &str) -> f64 {
    const DEFAULT: f64 = 128_000.0 / 8.0;
    let format =
        query_param(query, "output_format").unwrap_or_else(|| "mp3_44100_128".to_string());
    let positive = |n: f64| (n > 0.0 && n.is_finite()).then_some(n);
    if let Some(rest) = format.strip_prefix("mp3_").or_else(|| format.strip_prefix("opus_")) {
        return rest.split('_').nth(1).and_then(|k| k.parse::<f64>().ok()).and_then(positive).map_or(DEFAULT, |kbps| kbps * 1000.0 / 8.0);
    }
    if let Some(rate) = format.strip_prefix("wav_") {
        return rate.parse::<f64>().ok().and_then(positive).map_or(DEFAULT, |rate| rate * 2.0);
    }
    if format == "alaw_8000" {
        return 8_000.0;
    }
    bytes_per_second(&format).unwrap_or(DEFAULT)
}

/// [`output_bytes_per_second`] on the music route, whose format defaults
/// to `auto`: ElevenLabs answers `auto` as mp3_44100_128 on `music_v1`
/// (the route's default model) and as mp3_48000_192 on the v2 models.
fn music_bytes_per_second(query: &str, request_body: &[u8]) -> f64 {
    match query_param(query, "output_format").as_deref() {
        None | Some("auto") => {
            let model = serde_json::from_slice::<Value>(request_body)
                .ok()
                .and_then(|body| body["model_id"].as_str().map(str::to_string));
            match model.as_deref() {
                None | Some("music_v1") => 128_000.0 / 8.0,
                Some(_) => 192_000.0 / 8.0,
            }
        }
        Some(_) => output_bytes_per_second(query),
    }
}

// ── Image & Video (`flows/image`, `flows/video`) ─────────────────────
//
// A generation's price is a function of its request: the model, the
// settings that model's price depends on, and how much it makes (one
// image per request, since no image model takes a count, or the video's
// seconds). ElevenLabs publishes no USD rate for these routes and no
// answer states a cost, so the rates come from `elevenlabs_prices.json`,
// the web app's pricing table captured BY HAND on 2026-10-16 for tier
// `grant_tier_2_2025_07_23` (its endpoint answers only a signed-in
// session). Refreshing it is manual: capture it again and replace the
// file. A request whose price key the table does not hold, or whose
// quantity cannot be read, is priced unknown, never at the table's
// `any` row or the nearest key.
//
// The create only queues the job, and ElevenLabs does not charge a
// failed generation, so the create opens a charge under the generation
// id it answers and the job's read settles it: the quoted price once
// the job reads `completed`, nothing once it reads `failed`.

/// The hand-captured price table, parsed once.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FlowPrices {
    tier: String,
    captured_on: String,
    /// US cents per image, by price key.
    image_cents_per_image: HashMap<String, f64>,
    /// US cents per second of video, by price key.
    video_cents_per_second: HashMap<String, f64>,
}

static FLOW_PRICES: LazyLock<FlowPrices> = LazyLock::new(|| {
    serde_json::from_str(include_str!("elevenlabs_prices.json"))
        .expect("the checked-in ElevenLabs price table parses (a test holds it to that)")
});

/// What a generation route makes.
#[derive(Clone, Copy)]
enum FlowMedia {
    Image,
    Video,
}

/// An Image & Video route.
enum FlowRoute<'a> {
    /// `POST flows/image` or `POST flows/video`: queues a generation and
    /// opens its charge.
    Create(FlowMedia),
    /// `GET flows/{image,video}/{id}`: the job's state, which settles the
    /// charge.
    Job(&'a str),
}

fn flow_route<'a>(method: &str, path: &'a str) -> Option<FlowRoute<'a>> {
    let rest = path.strip_prefix("flows/")?;
    let (noun, id) = match rest.split_once('/') {
        None => (rest, None),
        Some((noun, id)) => (noun, Some(id)),
    };
    let media = match noun {
        "image" => FlowMedia::Image,
        "video" => FlowMedia::Video,
        _ => return None,
    };
    match (method, id) {
        ("POST", None) => Some(FlowRoute::Create(media)),
        ("GET", Some(id)) if one_segment(id) => Some(FlowRoute::Job(id)),
        _ => None,
    }
}

/// A generation request's price: the table key its settings make, the
/// key's rate, and how many images or seconds it makes.
#[derive(Debug, PartialEq)]
struct FlowQuote {
    key: String,
    cents_per_unit: f64,
    units: f64,
}

impl FlowQuote {
    fn usd(&self) -> f64 {
        self.cents_per_unit * self.units / 100.0
    }
}

/// A text setting of the request, or the model's default when the
/// request leaves it out.
fn flow_text(body: &Value, field: &str, default: &str) -> Result<String, String> {
    match &body[field] {
        Value::Null => Ok(default.to_string()),
        Value::String(text) => Ok(text.clone()),
        other => Err(format!("the request's `{field}` is {other}, not text")),
    }
}

/// A yes/no setting of the request, or the model's default.
fn flow_flag(body: &Value, field: &str, default: bool) -> Result<bool, String> {
    match &body[field] {
        Value::Null => Ok(default),
        Value::Bool(flag) => Ok(*flag),
        other => Err(format!("the request's `{field}` is {other}, not true or false")),
    }
}

/// The video's length in seconds (`duration_secs`), or the model's
/// default.
fn flow_seconds(body: &Value, default: f64) -> Result<f64, String> {
    match &body["duration_secs"] {
        Value::Null => Ok(default),
        other => other
            .as_f64()
            .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
            .ok_or_else(|| format!("the request's `duration_secs` is {other}, not a length in seconds")),
    }
}

/// The price key of an image request, following the table's keys for
/// each model: the settings that move a model's price follow its id, in
/// the table's order, with the default the API reference states for a
/// setting the request leaves out. A setting the table does not price
/// (a GPT Image 2's resolution, a Seedream 5 Lite's) is not part of the
/// key.
fn image_key(model: &str, body: &Value) -> Result<String, String> {
    Ok(match model {
        "gemini-2.5-flash-image" | "bytedance-seedream-5-lite" => model.to_string(),
        "gemini-3-pro-image" | "gemini-3.1-flash-image" | "gemini-3.1-flash-lite-image" => {
            format!("{model}-{}", flow_text(body, "resolution", "1K")?)
        }
        "bytedance-seedream-5-pro" => format!("{model}-{}", flow_text(body, "resolution", "2K")?),
        "gpt-image-1" | "gpt-image-1.5" => format!(
            "{model}-{}-{}",
            flow_text(body, "quality", "medium")?,
            flow_text(body, "aspect_ratio", "1:1")?
        ),
        "gpt-image-2" => format!("{model}-{}", flow_text(body, "quality", "medium")?),
        "gpt-image-2.5-sunburst" | "gpt-image-2.5-flare" => {
            format!("{model}-{}", flow_text(body, "quality", "high")?)
        }
        _ => return Err(format!("no price rule for the image model '{model}'")),
    })
}

/// The price key of a video request and the seconds it makes, following
/// the table's keys the way [`image_key`] does.
fn video_key(model: &str, body: &Value) -> Result<(String, f64), String> {
    match model {
        // Veo: the bare id is 720p and 1080p alike, `-4K` the top
        // resolution, and `-audio` a video with sound.
        "veo-3.1-generate-001" | "veo-3.1-fast-generate-001" => {
            let resolution = flow_text(body, "resolution", "720p")?;
            let mut key = model.to_string();
            if !matches!(resolution.as_str(), "720p" | "1080p") {
                key = format!("{key}-{resolution}");
            }
            if flow_flag(body, "generate_audio", true)? {
                key.push_str("-audio");
            }
            Ok((key, flow_seconds(body, 8.0)?))
        }
        // Seedance: resolution, then aspect ratio; sound does not move the
        // price. Seedance 2.5 takes its aspect from a start frame whatever
        // the request says (the API reference: "First-frame /
        // first-and-last-frame tasks always use `auto`"), and `auto` has
        // no price, since the frame decides it.
        "bytedance-seedance-v2" | "bytedance-seedance-v2-fast" | "bytedance-seedance-v2-mini"
        | "bytedance-seedance-v2.5" => {
            let resolution = flow_text(body, "resolution", "720p")?;
            let aspect = if model == "bytedance-seedance-v2.5" && !body["start_frame"].is_null() {
                "auto".to_string()
            } else {
                flow_text(body, "aspect_ratio", "16:9")?
            };
            Ok((format!("{model}-{resolution}-{aspect}"), flow_seconds(body, 5.0)?))
        }
        "creatify-aurora" => Err(
            "a lip-sync video is as long as its audio, and the request does not state that length".to_string(),
        ),
        _ => Err(format!("no price rule for the video model '{model}'")),
    }
}

/// What a generation request costs, or why it cannot be priced.
fn flow_quote(media: FlowMedia, body: &Value) -> Result<FlowQuote, String> {
    let model = body["model_id"].as_str().ok_or("the request names no `model_id`")?;
    let (key, units, table) = match media {
        FlowMedia::Image => (image_key(model, body)?, 1.0, &FLOW_PRICES.image_cents_per_image),
        FlowMedia::Video => {
            let (key, seconds) = video_key(model, body)?;
            (key, seconds, &FLOW_PRICES.video_cents_per_second)
        }
    };
    let cents_per_unit = *table.get(&key).ok_or_else(|| {
        format!(
            "the price table (tier {}, captured {}) has no '{key}'",
            FLOW_PRICES.tier, FLOW_PRICES.captured_on
        )
    })?;
    Ok(FlowQuote { key, cents_per_unit, units })
}

/// The create's tap: the request's quote as a seed, and the generation id
/// the answer names.
fn flow_create_tap(media: FlowMedia, request_body: &[u8]) -> super::JsonBodyObservation {
    let request: Value = serde_json::from_slice(request_body).unwrap_or(Value::Null);
    let mut seed = serde_json::Map::new();
    seed.insert("model".into(), request["model_id"].clone());
    match flow_quote(media, &request) {
        Ok(quote) => {
            seed.insert("usd".into(), json!(quote.usd()));
            seed.insert("priceKey".into(), json!(quote.key));
            seed.insert("centsPerUnit".into(), json!(quote.cents_per_unit));
            seed.insert(
                match media {
                    FlowMedia::Image => "images",
                    FlowMedia::Video => "seconds",
                }
                .into(),
                json!(quote.units),
            );
        }
        Err(why) => {
            seed.insert("usd".into(), Value::Null);
            seed.insert("resolution".into(), json!(format!("price unknown: {why}")));
        }
    }
    super::JsonBodyObservation::new().seed(seed).field("generationId", "/id").note_outcome()
}

/// Settle a generation's charge from a read of its job: the quote once it
/// completed, nothing once it failed, and no verdict while it runs.
fn flow_settled(observed: &crate::ObservedCall, scratch: &Value) -> Option<MeasuredCost> {
    let mut metadata = scratch.clone();
    let amount_usd = match observed.data["status"].as_str() {
        Some("completed") => scratch["usd"].as_f64(),
        Some("failed") => {
            metadata["resolution"] =
                json!("the generation failed, and ElevenLabs does not charge a failed generation");
            Some(0.0)
        }
        _ => return None,
    };
    if let Some(fields) = metadata.as_object_mut() {
        // The amount and the model are the cost's own fields.
        fields.remove("usd");
        fields.remove("model");
    }
    Some(MeasuredCost { amount_usd, model: scratch["model"].as_str().map(str::to_string), metadata })
}

// ── Batch transcription (`speech-to-text`) ───────────────────────────
//
// Billed by the audio's duration, which only the answer knows: a
// `source_url` request uploads nothing to measure, and an upload's byte
// count is a format-dependent guess. The settings that change the
// rate travel in the request's multipart form; the duration comes from
// the answer's `audio_duration_secs`.

/// Where `needle` first starts in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    let (&first, tail) = needle.split_first()?;
    let mut from = 0;
    while let Some(offset) = haystack[from..].iter().position(|&b| b == first) {
        let at = from + offset;
        if haystack[at + 1..].starts_with(tail) {
            return Some(at);
        }
        from = at + 1;
    }
    None
}

/// What follows the first line break in `bytes`.
fn after_line(bytes: &[u8]) -> Option<&[u8]> {
    find(bytes, b"\r\n").map(|at| &bytes[at + 2..])
}

/// `bytes` split around the first `byte`.
fn split_at_byte(bytes: &[u8], byte: u8) -> Option<(&[u8], &[u8])> {
    let at = bytes.iter().position(|&b| b == byte)?;
    Some((&bytes[..at], &bytes[at + 1..]))
}

/// A multipart body's boundary, from its first line (`--<boundary>\r\n`,
/// trailing padding allowed).
fn multipart_boundary(body: &[u8]) -> Option<&[u8]> {
    let line = &body[..find(body, b"\r\n")?];
    let boundary = line.strip_prefix(b"--")?.trim_ascii_end();
    (!boundary.is_empty()).then_some(boundary)
}

/// A multipart form's named fields, in order, each with its raw content.
/// A file's bytes may hold anything, `\r\n--` included, so a part ends
/// only at the full delimiter (`\r\n--<boundary>`). Tolerant: a part
/// with no readable name is skipped, and a body that is not a form reads
/// as no fields, so a setting that does not parse reads as not set.
fn multipart_fields(body: &[u8]) -> Vec<(&str, &[u8])> {
    let mut fields = Vec::new();
    let Some(boundary) = multipart_boundary(body) else { return fields };
    let delimiter = [&b"\r\n--"[..], boundary].concat();
    let Some(mut rest) = after_line(body) else { return fields };
    while let Some(end) = find(rest, &delimiter) {
        fields.extend(form_field(&rest[..end]));
        let after = &rest[end + delimiter.len()..];
        // `--` right after a delimiter closes the form.
        if after.starts_with(b"--") {
            break;
        }
        let Some(next) = after_line(after) else { break };
        rest = next;
    }
    fields
}

/// One part's field name and content: its headers end at the first blank
/// line, and the name is `Content-Disposition`'s `name` parameter.
fn form_field(part: &[u8]) -> Option<(&str, &[u8])> {
    let end = find(part, b"\r\n\r\n")?;
    let name = part[..end].split(|&b| b == b'\n').find_map(|line| disposition_name(line.trim_ascii()))?;
    Some((name, &part[end + 4..]))
}

/// The `name` parameter of a `Content-Disposition` header line, `None` for
/// any other line. Parameters match whole, so `filename` never reads as
/// `name`.
fn disposition_name(line: &[u8]) -> Option<&str> {
    let (header, value) = split_at_byte(line, b':')?;
    if !header.trim_ascii().eq_ignore_ascii_case(b"content-disposition") {
        return None;
    }
    value.split(|&b| b == b';').find_map(|param| {
        let (key, value) = split_at_byte(param, b'=')?;
        if !key.trim_ascii().eq_ignore_ascii_case(b"name") {
            return None;
        }
        let value = value.trim_ascii();
        let unquoted = value.strip_prefix(b"\"").and_then(|v| v.strip_suffix(b"\"")).unwrap_or(value);
        std::str::from_utf8(unquoted).ok()
    })
}

/// What a transcription request asks for that changes its price, read
/// from its form.
struct TranscriptionAsk {
    model: Option<String>,
    /// How many keyterms it lists (the form repeats the field per term).
    keyterms: usize,
    edit: bool,
    entities: bool,
    speaker_roles: bool,
}

impl TranscriptionAsk {
    fn read(body: &[u8]) -> Self {
        let fields = multipart_fields(body);
        let asking = |name: &'static str| fields.iter().filter(move |(n, v)| *n == name && asks_for(v));
        Self {
            model: fields
                .iter()
                .find(|(name, _)| *name == "model_id")
                .and_then(|(_, value)| std::str::from_utf8(value).ok())
                .map(str::to_string),
            keyterms: asking("keyterms").count(),
            edit: asking("transcript_edit").next().is_some(),
            // Redaction (`entity_redaction`) adds nothing here: the
            // pricing page and the API reference state no charge for it
            // (only ElevenLabs's Python SDK docstring claims 30%), and it
            // always comes with the detection it narrows, which is priced.
            entities: asking("entity_detection").next().is_some(),
            speaker_roles: fields
                .iter()
                .any(|(name, value)| *name == "detect_speaker_roles" && value.trim_ascii().eq_ignore_ascii_case(b"true")),
        }
    }

    /// The USD for `seconds` of transcribed audio (every channel counted),
    /// with the minimums ElevenLabs bills.
    fn usd(&self, seconds: f64) -> f64 {
        let seconds =
            if self.keyterms > STT_MANY_KEYTERMS { seconds.max(STT_MANY_KEYTERMS_MIN_SECONDS) } else { seconds };
        let mut per_hour = STT_USD_PER_HOUR;
        if self.keyterms > 0 {
            per_hour += STT_KEYTERMS_USD_PER_HOUR;
        }
        if self.entities {
            per_hour += STT_USD_PER_HOUR * ENTITY_DETECTION_PREMIUM;
        }
        if self.speaker_roles {
            per_hour += STT_USD_PER_HOUR * SPEAKER_ROLES_PREMIUM;
        }
        let edit_seconds = if self.edit { seconds.max(STT_EDIT_MIN_SECONDS) } else { 0.0 };
        (seconds * per_hour + edit_seconds * STT_EDIT_USD_PER_HOUR) / 3600.0
    }
}

/// The deepest nesting [`TranscriptScan`] follows. ElevenLabs's answer
/// nests a handful of levels; a body nesting past this is not one, and
/// the bound keeps the reader's memory fixed.
const SCAN_MAX_DEPTH: usize = 32;
/// The longest key or number the scan reads; a longer one reads as
/// nothing (no key it acts on, and no number it trusts).
const SCAN_TOKEN_CAP: usize = 32;

/// The keys [`TranscriptScan`] acts on.
#[derive(Clone, Copy, PartialEq)]
enum ScanKey {
    AudioDuration,
    Transcripts,
    Words,
    ChannelIndex,
    Other,
}

/// One open container on the scan's path.
#[derive(Clone, Copy)]
enum ScanStep {
    /// An object, under the last key read, and whether that key's value
    /// has started (past its `:`), which is what tells a key from a
    /// string value.
    Object { key: ScanKey, in_value: bool },
    Array,
}

/// The token the scan is in the middle of.
#[derive(Clone, Copy, Default)]
enum Lexeme {
    #[default]
    Between,
    String { key: bool, escaped: bool },
    Number,
    /// `true`, `false` or `null`.
    Literal,
}

/// A transcription answer, read as it streams past. The answer carries
/// every word with its timing, so a long recording's answer can outgrow
/// the shared JSON tap's cap, which would leave the cost unknown; this walks
/// the JSON byte by byte, holding only the path to the current value,
/// and keeps what pricing needs: the top-level `audio_duration_secs`,
/// each channel transcript's own (`transcripts[].audio_duration_secs`),
/// and which channels the words name (`words[].channel_index`).
#[derive(Default)]
struct TranscriptScan {
    path: Vec<ScanStep>,
    lexeme: Lexeme,
    /// The key or number being read, at most [`SCAN_TOKEN_CAP`] + 1
    /// bytes.
    token: Vec<u8>,
    duration: Option<f64>,
    /// How many channel transcripts `transcripts` holds, once it opens as
    /// a list, and the durations they state.
    transcripts: Option<u64>,
    channel_seconds: f64,
    channel_durations: u64,
    /// The `channel_index` values the words name, one bit each.
    word_channels: u64,
    /// The answer's outermost value closed.
    complete: bool,
    /// The bytes stopped being JSON this reader can follow, so nothing it
    /// read counts.
    broken: bool,
}

impl TranscriptScan {
    fn feed(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if self.broken {
                return;
            }
            self.step(byte);
        }
    }

    fn step(&mut self, byte: u8) {
        match self.lexeme {
            Lexeme::String { key, escaped: true } => {
                self.lexeme = Lexeme::String { key, escaped: false };
                if key {
                    self.push_token(byte);
                }
            }
            Lexeme::String { key, escaped: false } => match byte {
                b'\\' => self.lexeme = Lexeme::String { key, escaped: true },
                b'"' => {
                    self.lexeme = Lexeme::Between;
                    if key {
                        self.read_key();
                    }
                }
                _ if key => self.push_token(byte),
                _ => {}
            },
            Lexeme::Number if matches!(byte, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') => self.push_token(byte),
            Lexeme::Number => {
                self.lexeme = Lexeme::Between;
                self.read_number();
                self.between(byte);
            }
            Lexeme::Literal if byte.is_ascii_alphabetic() => {}
            Lexeme::Literal => {
                self.lexeme = Lexeme::Between;
                self.between(byte);
            }
            Lexeme::Between => self.between(byte),
        }
    }

    /// A byte outside any string, number or literal.
    fn between(&mut self, byte: u8) {
        if byte.is_ascii_whitespace() {
            return;
        }
        if self.complete {
            self.broken = true;
            return;
        }
        match byte {
            b'{' => self.open(ScanStep::Object { key: ScanKey::Other, in_value: false }),
            b'[' => self.open(ScanStep::Array),
            b'}' => self.close(true),
            b']' => self.close(false),
            b':' => match self.path.last_mut() {
                Some(ScanStep::Object { in_value, .. }) => *in_value = true,
                _ => self.broken = true,
            },
            b',' => {
                if let Some(ScanStep::Object { key, in_value }) = self.path.last_mut() {
                    *key = ScanKey::Other;
                    *in_value = false;
                }
            }
            b'"' => {
                let key = matches!(self.path.last(), Some(ScanStep::Object { in_value: false, .. }));
                self.token.clear();
                self.lexeme = Lexeme::String { key, escaped: false };
            }
            b'-' | b'0'..=b'9' => {
                self.token.clear();
                self.token.push(byte);
                self.lexeme = Lexeme::Number;
            }
            b't' | b'f' | b'n' => self.lexeme = Lexeme::Literal,
            _ => self.broken = true,
        }
    }

    fn open(&mut self, step: ScanStep) {
        if self.path.len() == SCAN_MAX_DEPTH {
            self.broken = true;
            return;
        }
        match step {
            ScanStep::Array if self.at(&[Some(ScanKey::Transcripts)]) => self.transcripts = Some(0),
            ScanStep::Object { .. } if self.at(&[Some(ScanKey::Transcripts), None]) => {
                *self.transcripts.get_or_insert(0) += 1;
            }
            _ => {}
        }
        self.path.push(step);
    }

    fn close(&mut self, object: bool) {
        match self.path.pop() {
            Some(ScanStep::Object { .. }) if object => {}
            Some(ScanStep::Array) if !object => {}
            _ => {
                self.broken = true;
                return;
            }
        }
        self.complete = self.path.is_empty();
    }

    fn push_token(&mut self, byte: u8) {
        if self.token.len() <= SCAN_TOKEN_CAP {
            self.token.push(byte);
        }
    }

    fn read_key(&mut self) {
        let read = match self.token.as_slice() {
            b"audio_duration_secs" => ScanKey::AudioDuration,
            b"transcripts" => ScanKey::Transcripts,
            b"words" => ScanKey::Words,
            b"channel_index" => ScanKey::ChannelIndex,
            _ => ScanKey::Other,
        };
        if let Some(ScanStep::Object { key, .. }) = self.path.last_mut() {
            *key = read;
        }
    }

    fn read_number(&mut self) {
        if self.token.len() > SCAN_TOKEN_CAP {
            return;
        }
        let Some(value) = std::str::from_utf8(&self.token)
            .ok()
            .and_then(|token| token.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
        else {
            return;
        };
        if self.at(&[Some(ScanKey::AudioDuration)]) {
            self.duration = Some(value);
        } else if self.at(&[Some(ScanKey::Transcripts), None, Some(ScanKey::AudioDuration)]) {
            self.channel_seconds += value;
            self.channel_durations += 1;
        } else if self.at(&[Some(ScanKey::Words), None, Some(ScanKey::ChannelIndex)])
            && value.fract() == 0.0
            && value < 64.0
        {
            self.word_channels |= 1 << value as u64;
        }
    }

    /// Whether the current value sits exactly at `path`: `Some(key)` an
    /// object's member, `None` a list's element.
    fn at(&self, path: &[Option<ScanKey>]) -> bool {
        self.path.len() == path.len()
            && self.path.iter().zip(path).all(|(step, want)| match (step, want) {
                (ScanStep::Object { key, .. }, Some(want)) => key == want,
                (ScanStep::Array, None) => true,
                _ => false,
            })
    }

    /// The audio seconds ElevenLabs billed, every channel counted, and how
    /// many channels that is, or why the answer does not say. ElevenLabs
    /// bills each channel "independently at the full audio duration": an
    /// answer split by channel (`transcripts`) states each channel's
    /// duration on its transcript, and those add up. A single transcript
    /// states one duration, the recording's length: when its words name
    /// several channels (the `combined` style), each of them is billed at
    /// that length. Checked live on 2026-10-11: a 2.28 second stereo file
    /// answered 2.2756 s in the combined style, and 2.2756 s per channel
    /// (4.5511 s at the top) in the separate one.
    fn billed(&self) -> Result<(f64, u64), &'static str> {
        if self.broken || !self.complete {
            return Err("the answer was cut short, or is not the JSON a transcription answers");
        }
        match self.transcripts {
            Some(0) => Err("the answer holds no transcript"),
            Some(channels) if self.channel_durations == channels => Ok((self.channel_seconds, channels)),
            Some(_) => Err("a channel's transcript in the answer states no audio duration"),
            None => {
                let channels = u64::from(self.word_channels.count_ones()).max(1);
                self.duration
                    .map(|seconds| (seconds * channels as f64, channels))
                    .ok_or("the answer states no audio duration")
            }
        }
    }
}

/// How one batch call's cost resolves; minted per route by `observe`.
enum BatchPricing {
    /// Settled from the request the moment the provider accepts.
    FromRequest { usd: f64 },
    /// The answered audio's bytes, at the format's byte rate, are the
    /// billed duration.
    OutputMinutes { bytes_per_second: f64, usd_per_minute: f64 },
    /// The audio duration the answer states, at the rate the request's
    /// settings add up to.
    AnsweredDuration { ask: TranscriptionAsk, scan: TranscriptScan },
}

/// The per-call tap for the batch routes: counts the response and
/// closes into the priced figure. The
/// small JSON envelope in `end` is what `resolve` reads.
struct BatchObservation {
    pricing: BatchPricing,
    /// The model the request named (its `model_id`, in a JSON body or a
    /// transcription's form), booked with the cost as a session's is.
    model: Option<String>,
    status: u16,
    body_bytes: u64,
}

impl CallObservation for BatchObservation {
    fn on_status(&mut self, status: u16) {
        self.status = status;
    }

    fn on_chunk(&mut self, bytes: &[u8]) {
        self.body_bytes += bytes.len() as u64;
        if let BatchPricing::AnsweredDuration { scan, .. } = &mut self.pricing {
            scan.feed(bytes);
        }
    }

    fn end(self: Box<Self>, interrupted: bool) -> crate::ObservedCall {
        // `detail` is what the trail shows of how the figure was reached.
        let (usd, detail): (Option<f64>, Value) = match crate::providers::verdict_for_status(self.status) {
            // Refused before doing anything: bills nothing.
            crate::providers::StatusVerdict::KnownZero => (Some(0.0), Value::Null),
            // A 5xx is not a refusal. ElevenLabs may have generated and
            // billed the audio with the error coming from in front of
            // it, so zero would claim a real spend was free.
            crate::providers::StatusVerdict::Unknown => (None, Value::Null),
            crate::providers::StatusVerdict::Accepted => match &self.pricing {
                BatchPricing::FromRequest { usd } => (Some(*usd), Value::Null),
                // A cut output stream under-measures the audio the
                // provider generated and billed: honestly unknown.
                BatchPricing::OutputMinutes { .. } if interrupted => (None, Value::Null),
                BatchPricing::OutputMinutes { bytes_per_second, usd_per_minute } => {
                    let minutes = self.body_bytes as f64 / bytes_per_second / 60.0;
                    (Some(minutes * usd_per_minute), Value::Null)
                }
                // An answer that does not state its duration (cut short,
                // or a webhook request's early answer) leaves a billed
                // transcription honestly unknown.
                BatchPricing::AnsweredDuration { ask, scan } => match scan.billed() {
                    Ok((seconds, channels)) => {
                        (Some(ask.usd(seconds)), json!({ "audioSeconds": seconds, "channels": channels }))
                    }
                    Err(why) => (None, json!({ "resolution": why })),
                },
            },
        };
        crate::ObservedCall {
            interrupted,
            status: self.status,
            data: json!({
                "usd": usd,
                "model": self.model,
                "outputBytes": self.body_bytes,
                "interrupted": interrupted,
                "detail": detail,
            }),
        }
    }
}

struct RealtimeSttSession {
    bytes_per_second: f64,
    usd_per_hour: f64,
    model: Option<String>,
    audio_bytes: u64,
}

impl RealtimeSttSession {
    /// Dollars accrued so far.
    fn accrued_usd(&self) -> f64 {
        let audio_seconds = self.audio_bytes as f64 / self.bytes_per_second;
        audio_seconds / 3600.0 * self.usd_per_hour
    }
}

impl SessionObservation for RealtimeSttSession {
    fn on_frame_to_provider(&mut self, payload: &[u8]) {
        // Only audio chunks carry billable duration; anything else (a
        // commit control, malformed bytes) accrues nothing.
        let Ok(msg) = serde_json::from_slice::<Value>(payload) else { return };
        if msg["message_type"] != "input_audio_chunk" {
            return;
        }
        if let Some(b64) = msg["audio_base_64"].as_str() {
            if let Some(n) = base64_decoded_len(b64) {
                self.audio_bytes += n;
            }
        }
    }

    fn on_frame_to_caller(&mut self, _payload: &[u8]) {}

    fn end(self: Box<Self>, interrupted: bool) -> MeasuredCost {
        let audio_seconds = self.audio_bytes as f64 / self.bytes_per_second;
        MeasuredCost {
            amount_usd: Some(self.accrued_usd()),
            model: self.model.clone(),
            metadata: json!({
                "audioSeconds": audio_seconds,
                "usdPerHour": self.usd_per_hour,
                "interrupted": interrupted,
            }),
        }
    }
}

#[async_trait::async_trait]
impl ProviderMeter for ElevenLabsMeter {
    fn service(&self) -> &'static str {
        "elevenlabs"
    }

    fn base_url(&self) -> &'static str {
        "https://api.elevenlabs.io/v1"
    }

    fn classify(&self, method: &str, path: &str) -> RouteClass {
        if path == REALTIME_STT || streaming_tts_voice(path).is_some() {
            return RouteClass::BillableSession;
        }
        match flow_route(method, path) {
            Some(FlowRoute::Create(_)) => return RouteClass::Billable(crate::Pricing::Metered),
            Some(FlowRoute::Job(_)) => return RouteClass::Reports,
            None => {}
        }
        match project_route(method, path) {
            Some(ProjectRoute::Create) => return RouteClass::Billable(crate::Pricing::Metered),
            Some(ProjectRoute::Delete) => return RouteClass::Free,
            Some(_) => return RouteClass::Reports,
            None => {}
        }
        match batch_route(method, path) {
            Some(BatchRoute::Free) => RouteClass::Free,
            Some(_) => RouteClass::Billable(crate::Pricing::Metered),
            None => RouteClass::Unknown,
        }
    }

    fn observe(&self, path: &str, query: &str, request_body: &[u8]) -> Box<dyn CallObservation> {
        // The project routes say what a dub came to in what they answer,
        // never in what was sent. The create reports its own model; the
        // reads carry the target's state and the source's duration.
        let project_tap = || {
            super::JsonBodyObservation::new()
                .field("projectId", "/project_id")
                .field("model", "/model_id")
                .field("status", "/status")
                .field("durationS", "/media/duration_s")
                // The languages a create already started (`target_language`
                // on the create), counted with the ones added after it.
                .field("languageIds", "/language_ids")
                .note_outcome()
        };
        if project_route("POST", path).is_some() || project_route("GET", path).is_some() {
            return Box::new(project_tap());
        }
        // A generation is priced from its request when it is queued; a
        // read of its job says whether that price was charged.
        match flow_route("POST", path).or_else(|| flow_route("GET", path)) {
            Some(FlowRoute::Create(media)) => return Box::new(flow_create_tap(media, request_body)),
            Some(FlowRoute::Job(_)) => {
                return Box::new(super::JsonBodyObservation::new().field("status", "/status"));
            }
            None => {}
        }
        let pricing = match batch_route("POST", path) {
            // The whole cost is a function of the request: settled the
            // moment the provider accepts the call.
            Some(BatchRoute::Tts) => BatchPricing::FromRequest { usd: tts_usd(request_body) },
            Some(BatchRoute::Dialogue) => BatchPricing::FromRequest { usd: dialogue_usd(request_body) },
            Some(BatchRoute::Align) => BatchPricing::FromRequest {
                usd: input_minutes(request_body) / 60.0 * ALIGN_USD_PER_HOUR,
            },
            // Transcription is priced from the duration it answers, at
            // the rate its form's settings add up to.
            Some(BatchRoute::Stt) => BatchPricing::AnsweredDuration {
                ask: TranscriptionAsk::read(request_body),
                scan: TranscriptScan::default(),
            },
            // Output-priced audio: the answered bytes, at the output
            // format's byte rate, are the duration.
            Some(BatchRoute::SoundEffect) => BatchPricing::OutputMinutes {
                bytes_per_second: output_bytes_per_second(query),
                usd_per_minute: SOUND_EFFECT_USD_PER_MINUTE,
            },
            Some(BatchRoute::Music) => BatchPricing::OutputMinutes {
                bytes_per_second: music_bytes_per_second(query, request_body),
                usd_per_minute: MUSIC_USD_PER_MINUTE,
            },
            Some(BatchRoute::Sts) => BatchPricing::OutputMinutes {
                bytes_per_second: output_bytes_per_second(query),
                usd_per_minute: VOICE_CHANGER_USD_PER_MINUTE,
            },
            Some(BatchRoute::Isolation) => BatchPricing::OutputMinutes {
                bytes_per_second: output_bytes_per_second(query),
                usd_per_minute: ISOLATOR_USD_PER_MINUTE,
            },
            _ => unreachable!("observe is only minted for Billable routes"),
        };
        let model = match &pricing {
            BatchPricing::AnsweredDuration { ask, .. } => ask.model.clone(),
            _ => serde_json::from_slice::<Value>(request_body).ok().and_then(|request| request["model_id"].as_str().map(str::to_string)),
        };
        Box::new(BatchObservation { pricing, model, status: 0, body_bytes: 0 })
    }

    fn observe_session(
        &self,
        path: &str,
        query: &str,
    ) -> anyhow::Result<Box<dyn SessionObservation>> {
        if streaming_tts_voice(path).is_some() {
            return Ok(Box::new(StreamingTtsSession {
                usd_per_1k_chars: tts_rate_of(query_param(query, "model_id").as_deref()),
                model: query_param(query, "model_id"),
                chars: 0,
            }));
        }
        anyhow::ensure!(path == REALTIME_STT, "'{path}' is not a session route");
        let format =
            query_param(query, "audio_format").unwrap_or_else(|| "pcm_16000".to_string());
        let bytes_per_second = bytes_per_second(&format).ok_or_else(|| {
            anyhow::anyhow!("unknown audio_format '{format}'; this meter cannot price it")
        })?;
        Ok(Box::new(RealtimeSttSession {
            bytes_per_second,
            usd_per_hour: usd_per_hour(query),
            model: query_param(query, "model_id"),
            audio_bytes: 0,
        }))
    }

    fn opens_charge(&self, path: &str, observed: &crate::ObservedCall) -> Option<String> {
        // A create ElevenLabs refused spent nothing; `resolve` books it.
        if observed.data["accepted"] != json!(true) {
            return None;
        }
        if let Some(FlowRoute::Create(_)) = flow_route("POST", path) {
            return observed.data["generationId"].as_str().map(str::to_string);
        }
        if !matches!(project_route("POST", path), Some(ProjectRoute::Create)) {
            return None;
        }
        observed.data["projectId"].as_str().map(str::to_string)
    }

    fn charge_reported_on(&self, path: &str, _observed: &crate::ObservedCall) -> Option<String> {
        if let Some(FlowRoute::Job(id)) = flow_route("GET", path) {
            return Some(id.to_string());
        }
        match project_route("POST", path).or_else(|| project_route("GET", path))? {
            ProjectRoute::Create | ProjectRoute::Delete => None,
            ProjectRoute::AddLanguage(id) | ProjectRoute::Language(id) | ProjectRoute::Project(id) => {
                Some(id.to_string())
            }
        }
    }

    async fn fold_report(
        &self,
        path: &str,
        observed: crate::ObservedCall,
        scratch: &mut Value,
        follow_up: FollowUp<'_>,
    ) -> Option<MeasuredCost> {
        // A read ElevenLabs did not answer is no evidence about the dub:
        // the charge stays open for one that does.
        if !(200..300).contains(&observed.status) {
            return None;
        }
        if let Some(FlowRoute::Job(_)) = flow_route("GET", path) {
            return flow_settled(&observed, scratch);
        }
        if let Some(seconds) = observed.data["durationS"].as_f64() {
            scratch["durationS"] = json!(seconds);
        }
        let status = observed.data["status"].as_str().unwrap_or_default();
        match project_route("POST", path).or_else(|| project_route("GET", path))? {
            ProjectRoute::Create | ProjectRoute::Delete => None,
            ProjectRoute::AddLanguage(_) => {
                scratch["languages"] = json!(scratch["languages"].as_u64().unwrap_or(0) + 1);
                None
            }
            ProjectRoute::Project(_) | ProjectRoute::Language(_) if status == "failed" => {
                let mut metadata = scratch.clone();
                metadata["resolution"] = json!(
                    "the dub failed, and ElevenLabs does not say whether the minimum charge its \
                     project took is refunded"
                );
                Some(MeasuredCost { amount_usd: None, model: scratch["model"].as_str().map(str::to_string), metadata })
            }
            ProjectRoute::Project(_) => None,
            ProjectRoute::Language(_) if status != "completed" => None,
            ProjectRoute::Language(project) => {
                let duration = match scratch["durationS"].as_f64() {
                    Some(seconds) => Ok(seconds),
                    None => project_duration(project, &follow_up).await,
                };
                Some(match duration {
                    Ok(seconds) => priced_project(scratch, seconds),
                    Err(e) => {
                        let mut metadata = scratch.clone();
                        metadata["resolution"] = json!(format!("the source's duration could not be read: {e:#}"));
                        MeasuredCost { amount_usd: None, model: scratch["model"].as_str().map(str::to_string), metadata }
                    }
                })
            }
        }
    }

    async fn resolve(
        &self,
        path: &str,
        observed: crate::ObservedCall,
        _follow_up: FollowUp<'_>,
    ) -> MeasuredCost {
        // Only a create that opened no charge lands here from the project
        // and generation routes: one ElevenLabs refused, or one it took
        // without naming what it made, which nothing can then report on.
        let create = match (flow_route("POST", path), project_route("POST", path)) {
            (Some(FlowRoute::Create(_)), _) => Some(("the generation create", "generation")),
            (_, Some(ProjectRoute::Create)) => Some(("the dubbing project create", "project")),
            _ => None,
        };
        if let Some((what, made)) = create {
            if let Some(cost) = super::cost_from_status(observed.status, what) {
                return cost;
            }
            return MeasuredCost {
                amount_usd: None,
                model: observed.data["model"].as_str().map(str::to_string),
                metadata: json!({
                    "resolution": format!("ElevenLabs took the {made} but named no {made} id, so nothing can report on it")
                }),
            };
        }
        // The observation already computed the figure (see
        // `BatchObservation::end`); a refused call bills nothing, an
        // interrupted output-priced call is honestly unknown.
        let amount = observed.data["usd"].as_f64();
        // The amount and the model are the cost's own fields.
        let mut metadata = json!({ "outputBytes": observed.data["outputBytes"], "interrupted": observed.data["interrupted"] });
        if let Some(detail) = observed.data["detail"].as_object() {
            for (key, value) in detail {
                metadata[key] = value.clone();
            }
        }
        MeasuredCost { amount_usd: amount, model: observed.data["model"].as_str().map(str::to_string), metadata }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    fn chunk(bytes: usize) -> Vec<u8> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(vec![0u8; bytes]);
        serde_json::to_vec(&json!({
            "message_type": "input_audio_chunk",
            "audio_base_64": b64,
        }))
        .unwrap()
    }

    #[test]
    fn audio_duration_prices_the_session() {
        // 60 seconds of pcm_16000 (32000 B/s) at $0.39/h.
        let mut obs = ELEVENLABS
            .observe_session(REALTIME_STT, "model_id=scribe_v2_realtime&audio_format=pcm_16000")
            .unwrap();
        obs.on_frame_to_provider(&chunk(32_000 * 60));
        let expected = 60.0 / 3600.0 * 0.39;
        // Transcripts back cost nothing; junk frames cost nothing.
        obs.on_frame_to_caller(br#"{"message_type":"partial_transcript","text":"hi"}"#);
        obs.on_frame_to_provider(b"not json");
        let cost = obs.end(false);
        assert_eq!(cost.amount_usd, Some(expected));
        assert_eq!(cost.model.as_deref(), Some("scribe_v2_realtime"));
        assert_eq!(cost.metadata["audioSeconds"], serde_json::json!(60.0));
    }

    #[test]
    fn base64_length_math_matches_real_decoding() {
        for n in [0usize, 1, 2, 3, 4, 5, 31_999, 32_000] {
            let b64 = base64::engine::general_purpose::STANDARD.encode(vec![7u8; n]);
            assert_eq!(base64_decoded_len(&b64), Some(n as u64), "padded n={n}");
            let b64 = base64::engine::general_purpose::STANDARD_NO_PAD.encode(vec![7u8; n]);
            assert_eq!(base64_decoded_len(&b64), Some(n as u64), "unpadded n={n}");
        }
        // Degenerate inputs measure as invalid or empty, never panic.
        assert_eq!(base64_decoded_len(""), Some(0));
        assert_eq!(base64_decoded_len("="), Some(0));
        assert_eq!(base64_decoded_len("=="), Some(0));
        assert_eq!(base64_decoded_len("===="), Some(0));
        assert_eq!(base64_decoded_len("A"), None, "one significant char is not base64");
    }

    #[test]
    fn a_degenerate_audio_rate_is_refused_at_session_start() {
        for format in ["pcm_0", "pcm_-16000", "pcm_inf", "pcm_NaN"] {
            let err = ELEVENLABS
                .observe_session(REALTIME_STT, &format!("audio_format={format}"))
                .err()
                .unwrap_or_else(|| panic!("'{format}' must be refused"));
            assert!(err.to_string().contains("audio_format"), "{err:#}");
        }
    }

    #[test]
    fn addons_raise_the_rate() {
        let minute = |q: &str| {
            let mut o = ELEVENLABS.observe_session(REALTIME_STT, q).unwrap();
            o.on_frame_to_provider(&chunk(32_000 * 60));
            o.end(false).amount_usd.unwrap()
        };
        // One minute of audio, at the hourly rates.
        assert!((minute("") - 0.39 / 60.0).abs() < 1e-9);
        // Keyterms $0.08, an edit $0.12, entity detection 30% of $0.39.
        let all = minute("keyterms=alpha&keyterms=beta&transcript_edit=Write+dates+as+ISO&entity_detection=all");
        assert!((all - (0.39 + 0.08 + 0.12 + 0.117) / 60.0).abs() < 1e-9, "{all}");
    }

    /// Each realtime add-on is billed once, whatever number of values the
    /// URL repeats it with, and an empty value asks for nothing.
    #[test]
    fn each_realtime_addon_counts_once() {
        for (query, rate) in [
            ("", 0.39),
            ("model_id=scribe_v2_realtime&audio_format=pcm_16000&language_code=en&commit_strategy=vad", 0.39),
            ("keyterms=alpha&keyterms=beta&keyterms=gamma", 0.47),
            ("keyterms=", 0.39),
            ("transcript_edit=Expand+abbreviations", 0.51),
            ("transcript_edit=", 0.39),
            ("entity_detection=all", 0.507),
            // A list, the way a node sends one (repeated per category),
            // and as JSON text.
            ("entity_detection=pii&entity_detection=phi", 0.507),
            ("entity_detection=%5B%22pii%22%2C%22phi%22%5D", 0.507),
            ("entity_detection=%5B%5D", 0.39),
        ] {
            let got = usd_per_hour(query);
            assert!((got - rate).abs() < 1e-9, "'{query}': {got}");
        }
    }

    #[test]
    fn only_the_realtime_route_is_a_session() {
        assert_eq!(
            ELEVENLABS.classify("GET", REALTIME_STT),
            RouteClass::BillableSession
        );
        assert_eq!(
            ELEVENLABS.classify("POST", "speech-to-text"),
            RouteClass::Billable(crate::Pricing::Metered)
        );
        assert!(ELEVENLABS.observe_session("speech-to-text", "").is_err());
        assert_eq!(ELEVENLABS.classify("GET", "user"), RouteClass::Free);
        assert!(ELEVENLABS.observe_session("user", "").is_err());
    }

    #[test]
    fn batch_routes_classify_and_trick_paths_stay_unknown() {
        let billable = |m: &str, p: &str| {
            matches!(ELEVENLABS.classify(m, p), RouteClass::Billable(crate::Pricing::Metered))
        };
        for p in [
            "text-to-speech/v1",
            "text-to-speech/v1/stream",
            "text-to-speech/v1/with-timestamps",
            "text-to-speech/v1/stream/with-timestamps",
            "text-to-dialogue",
            "text-to-dialogue/stream",
            "text-to-dialogue/with-timestamps",
            "text-to-dialogue/stream/with-timestamps",
            "speech-to-speech/v1",
            "audio-isolation",
            "sound-generation",
            "music",
            "forced-alignment",
            "speech-to-text",
        ] {
            assert!(billable("POST", p), "POST {p} bills metered");
            assert_eq!(ELEVENLABS.classify("GET", p), RouteClass::Unknown, "GET {p}");
        }
        for p in ["voices", "models", "user", "audio-effects/catalog", "pronunciation-dictionaries", "music/finetunes", "convai/llm/list"] {
            assert_eq!(ELEVENLABS.classify("GET", p), RouteClass::Free, "GET {p}");
        }
        // The legacy dubbing route is not priced, so the install's key
        // never travels on it.
        for (m, p) in [("POST", "dubbing"), ("GET", "dubbing/d1"), ("GET", "dubbing/d1/audio/fr"), ("DELETE", "dubbing/d1")] {
            assert_eq!(ELEVENLABS.classify(m, p), RouteClass::Unknown, "{m} {p}");
        }
        // Voice creation mints an ACCOUNT asset, so the runtime
        // credential never travels there (Unknown = own key only);
        // voice DESIGN is also unpriced until a USD rate is published.
        assert_eq!(ELEVENLABS.classify("POST", "voices/add"), RouteClass::Unknown);
        assert_eq!(ELEVENLABS.classify("POST", "text-to-voice/design"), RouteClass::Unknown);
        // An image or video generation is priced from its request, and a
        // read of its job reports on it. Template runs, the generation
        // lists and the temporary assets a generation reads stay Unknown.
        assert!(billable("POST", "flows/image"));
        assert!(billable("POST", "flows/video"));
        assert_eq!(ELEVENLABS.classify("GET", "flows/image/g1"), RouteClass::Reports);
        assert_eq!(ELEVENLABS.classify("GET", "flows/video/g1"), RouteClass::Reports);
        for (m, p) in [
            ("GET", "flows/image"),
            ("GET", "flows/video"),
            ("POST", "flows/video/g1"),
            ("DELETE", "flows/video/g1"),
            ("POST", "flows/text-to-speech"),
            ("POST", "flows/templates/t1/runs"),
            ("GET", "flows/templates/t1/runs/r1"),
            ("POST", "assets"),
            ("DELETE", "assets/a1"),
        ] {
            assert_eq!(ELEVENLABS.classify(m, p), RouteClass::Unknown, "{m} {p}");
        }
        for trick in [
            "text-to-speech/../user",
            "text-to-speech/a/b",
            "text-to-speech/%2e%2e",
            "text-to-speech/v1/",
            "text-to-speech/v1/stream/extra",
            "text-to-dialogue/",
            "text-to-dialogue/../user",
            "text-to-dialoguex",
            "dubbing/d1/audio/fr/extra",
            "dubbing/../voices",
            "flows/image/",
            "flows/imagex",
            "flows/image/../user",
            "flows/video/%2e%2e",
            "flows/video/g1/extra",
            "flows/../user",
        ] {
            assert_eq!(
                ELEVENLABS.classify("POST", trick),
                RouteClass::Unknown,
                "POST {trick}"
            );
            assert_eq!(ELEVENLABS.classify("GET", trick), RouteClass::Unknown, "GET {trick}");
        }
    }

    #[test]
    fn tts_prices_the_request_characters_at_the_model_rate() {
        let body = |model: &str, text: &str| {
            serde_json::to_vec(&json!({ "text": text, "model_id": model })).unwrap()
        };
        let run = |body: &[u8]| {
            let mut obs = ELEVENLABS.observe("text-to-speech/v1", "output_format=mp3_44100_128", body);
            obs.on_status(200);
            obs.on_chunk(b"audio-bytes");
            obs.end(false)
        };
        let thousand = "x".repeat(1000);
        for (model, usd) in [
            ("eleven_multilingual_v2", 0.08),
            ("eleven_v3", 0.08),
            ("eleven_v4", 0.08),
            ("eleven_flash_v2_5", 0.04),
            ("eleven_turbo_v2_5", 0.04),
            ("eleven_v3_conversational", 0.04),
            ("eleven_v4_turbo", 0.04),
        ] {
            let observed = run(&body(model, &thousand));
            assert!((observed.data["usd"].as_f64().unwrap() - usd).abs() < 1e-9, "{model}: {observed:?}");
        }
        // A refused call bills nothing, whatever the request said.
        let mut obs =
            ELEVENLABS.observe("text-to-speech/v1", "", &body("eleven_v3", &thousand));
        obs.on_status(401);
        assert_eq!(obs.end(false).data["usd"].as_f64(), Some(0.0));
    }

    /// Text to dialogue, the route serving `eleven_v4`, prices every
    /// line's characters at the model's rate, the route's default model
    /// (v3) at the standard one.
    #[test]
    fn dialogue_prices_every_lines_characters_at_the_model_rate() {
        let run = |path: &str, body: serde_json::Value| {
            let mut obs = ELEVENLABS.observe(path, "", &serde_json::to_vec(&body).unwrap());
            obs.on_status(200);
            obs.on_chunk(b"audio-bytes");
            obs.end(false).data["usd"].as_f64().unwrap()
        };
        let lines = json!([
            { "text": "x".repeat(600), "voice_id": "a" },
            { "text": "y".repeat(400), "voice_id": "b" },
        ]);
        let v4 = run("text-to-dialogue", json!({ "inputs": lines, "model_id": "eleven_v4" }));
        assert!((v4 - 0.08).abs() < 1e-9, "{v4}");
        let default = run("text-to-dialogue/stream", json!({ "inputs": lines }));
        assert!((default - 0.08).abs() < 1e-9, "no model is v3, at the standard rate: {default}");
        let fast = run("text-to-dialogue/with-timestamps", json!({ "inputs": lines, "model_id": "eleven_v3_conversational" }));
        assert!((fast - 0.04).abs() < 1e-9, "{fast}");
        assert_eq!(run("text-to-dialogue", json!({ "nope": 1 })), 0.0, "no lines, no characters");
    }

    #[test]
    fn output_priced_routes_measure_the_answered_bytes() {
        // 60s of mp3_44100_128 (16000 B/s) of music at $0.15/min.
        let mut obs = ELEVENLABS.observe("music", "", b"{}");
        obs.on_status(200);
        obs.on_chunk(&vec![0u8; 16_000 * 60]);
        let observed = obs.end(false);
        assert!((observed.data["usd"].as_f64().unwrap() - 0.15).abs() < 1e-9);
        // An interrupted output stream is honestly unknown, never a
        // low number.
        let mut obs = ELEVENLABS.observe("sound-generation", "", b"{}");
        obs.on_status(200);
        obs.on_chunk(&vec![0u8; 16_000]);
        assert_eq!(obs.end(true).data["usd"].as_f64(), None);
    }

    #[test]
    fn streaming_tts_prices_the_sent_characters() {
        assert_eq!(
            ELEVENLABS.classify("GET", "text-to-speech/v1/stream-input"),
            RouteClass::BillableSession
        );
        assert_eq!(
            ELEVENLABS.classify("GET", "text-to-speech/../stream-input"),
            RouteClass::Unknown
        );
        let mut obs = ELEVENLABS
            .observe_session("text-to-speech/v1/stream-input", "model_id=eleven_flash_v2_5")
            .unwrap();
        obs.on_frame_to_provider(br#"{"text":" "}"#);
        let thousand = serde_json::to_vec(&json!({ "text": "x".repeat(999) })).unwrap();
        obs.on_frame_to_provider(&thousand);
        // Audio back costs nothing; junk frames cost nothing.
        obs.on_frame_to_caller(br#"{"audio":"aGk=","isFinal":false}"#);
        obs.on_frame_to_provider(b"not json");
        let cost = obs.end(false);
        assert!((cost.amount_usd.unwrap() - 0.04).abs() < 1e-9, "{cost:?}");
        assert_eq!(cost.model.as_deref(), Some("eleven_flash_v2_5"));
    }

    #[test]
    fn the_editor_lists_are_free_and_their_neighbours_stay_unknown() {
        for (m, p) in [
            ("GET", "pronunciation-dictionaries/x/y"),
            ("POST", "audio-effects/catalog"),
            ("DELETE", "pronunciation-dictionaries"),
        ] {
            assert_eq!(ELEVENLABS.classify(m, p), RouteClass::Unknown, "{m} {p}");
        }
        // A project's delete is the project route's.
        assert!(matches!(project_route("DELETE", "dubbing/project/p1"), Some(ProjectRoute::Delete)));
    }

    #[test]
    fn music_on_auto_prices_at_the_format_the_model_answers() {
        let body = |model: &str| serde_json::to_vec(&json!({ "prompt": "a tune", "model_id": model })).unwrap();
        assert!((music_bytes_per_second("output_format=auto", &body("music_v2_5")) - 24_000.0).abs() < 1e-9);
        assert!((music_bytes_per_second("", &body("music_v2")) - 24_000.0).abs() < 1e-9, "absent is auto");
        assert!((music_bytes_per_second("output_format=auto", &body("music_v1")) - 16_000.0).abs() < 1e-9);
        assert!((music_bytes_per_second("output_format=auto", b"{}") - 16_000.0).abs() < 1e-9, "the route's default model");
        assert!((music_bytes_per_second("output_format=mp3_44100_64", &body("music_v2")) - 8_000.0).abs() < 1e-9);
    }

    #[test]
    fn every_output_format_family_is_a_byte_rate() {
        for (format, rate) in [
            ("mp3_44100_128", 16_000.0),
            ("mp3_22050_32", 4_000.0),
            ("opus_48000_32", 4_000.0),
            ("opus_48000_192", 24_000.0),
            ("wav_44100", 88_200.0),
            ("pcm_16000", 32_000.0),
            ("ulaw_8000", 8_000.0),
            ("alaw_8000", 8_000.0),
            ("flac_44100", 16_000.0),
        ] {
            let got = output_bytes_per_second(&format!("output_format={format}"));
            assert!((got - rate).abs() < 1e-9, "{format}: {got}");
        }
        assert!((output_bytes_per_second("") - 16_000.0).abs() < 1e-9, "absent is the API default");
    }

    #[test]
    fn ulaw_prices_at_one_byte_per_sample() {
        let mut o = ELEVENLABS
            .observe_session(REALTIME_STT, "audio_format=ulaw_8000")
            .unwrap();
        o.on_frame_to_provider(&chunk(8_000 * 60));
        assert!((o.end(false).amount_usd.unwrap() - 0.39 / 60.0).abs() < 1e-9);
        assert!(ELEVENLABS.observe_session(REALTIME_STT, "audio_format=opus").is_err());
    }

    /// A transcription form laid out byte for byte the way weft's
    /// multipart builder frames one (`Multipart::build` in weft-core, behind
    /// a feature this crate does not turn on): each part is the boundary
    /// line, its headers, a blank line and its payload, the file first,
    /// then every setting as a text part.
    fn transcription_form(file: Option<&[u8]>, fields: &[(&str, &str)]) -> Vec<u8> {
        const BOUNDARY: &str = "weft-0123456789abcdef";
        let mut body = Vec::new();
        if let Some(bytes) = file {
            body.extend_from_slice(
                format!(
                    "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"call.mp3\"\r\n\
                     Content-Type: audio/mpeg\r\n\r\n"
                )
                .as_bytes(),
            );
            body.extend_from_slice(bytes);
            body.extend_from_slice(b"\r\n");
        }
        for (name, value) in fields {
            body.extend_from_slice(
                format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes(),
            );
        }
        body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
        body
    }

    /// Audio bytes hold anything, line breaks and `\r\n--` and the
    /// boundary's own first characters included: only the full delimiter
    /// ends a part, so every setting after the file still reads.
    #[test]
    fn the_form_reader_finds_every_field_past_a_binary_file() {
        let mut audio = vec![0u8, 255, b'\r', b'\n', b'-', b'-', 7];
        audio.extend_from_slice(b"\r\n--weft-0123\r\n--weft-0123456789abcdeX\r\n\r\n--");
        audio.extend((0u8..=255).cycle().take(4096));
        let body = transcription_form(
            Some(&audio),
            &[("model_id", "scribe_v2"), ("keyterms", "weft"), ("keyterms", "Quentin")],
        );
        let fields = multipart_fields(&body);
        let names: Vec<&str> = fields.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["file", "model_id", "keyterms", "keyterms"]);
        assert_eq!(fields[0].1, &audio[..]);
        assert_eq!(fields[1].1, b"scribe_v2");
        assert_eq!(fields[3].1, b"Quentin");
    }

    /// Whatever does not parse reads as not there, never as a panic or a
    /// field it is not.
    #[test]
    fn what_is_not_a_form_reads_as_no_fields() {
        let bodies: [&[u8]; 5] = [
            b"",
            br#"{"model_id":"scribe_v2"}"#,
            b"--\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nx\r\n----\r\n",
            b"--b\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nnever closed",
            b"--b\r\nContent-Disposition: form-data; name=\"a\"",
        ];
        for body in bodies {
            assert!(multipart_fields(body).is_empty(), "{:?}", String::from_utf8_lossy(body));
        }
        // A part with no name is skipped and its neighbours still read;
        // header names and parameters match whatever their case, and
        // `filename` is never `name`.
        let body = b"--b\r\nContent-Type: text/plain\r\n\r\nnameless\r\n\
                     --b\r\ncontent-disposition: form-data; filename=\"name.txt\"; NAME=keyterms\r\n\r\nalpha\r\n--b--\r\n";
        assert_eq!(multipart_fields(body), vec![("keyterms", &b"alpha"[..])]);
    }

    #[test]
    fn a_transcription_form_settles_its_rate() {
        let ask = TranscriptionAsk::read(&transcription_form(
            None,
            &[
                ("model_id", "scribe_v2"),
                ("source_url", "https://example.com/talk.mp3"),
                ("keyterms", "alpha"),
                ("keyterms", "beta"),
                ("keyterms", ""),
                ("entity_detection", r#"["all"]"#),
                ("diarize", "true"),
                ("detect_speaker_roles", "true"),
                ("transcript_edit", "Write dates as ISO"),
            ],
        ));
        assert_eq!(ask.model.as_deref(), Some("scribe_v2"));
        assert_eq!(ask.keyterms, 2, "an empty term is no term");
        assert!(ask.entities && ask.speaker_roles && ask.edit);
        // An hour: $0.22, keyterms $0.05, entities 30% and roles 10% of
        // $0.22, the edit $0.07.
        assert!((ask.usd(3600.0) - (0.22 + 0.05 + 0.066 + 0.022 + 0.07)).abs() < 1e-9);
        let plain = TranscriptionAsk::read(&transcription_form(
            None,
            &[("entity_detection", "[]"), ("detect_speaker_roles", "false"), ("transcript_edit", "")],
        ));
        assert!(!plain.entities && !plain.speaker_roles && !plain.edit && plain.keyterms == 0);
        assert!((plain.usd(3600.0) - 0.22).abs() < 1e-9);
    }

    /// An edit is billed for at least 10 seconds, and a request with more
    /// than 100 keyterms for at least 20.
    #[test]
    fn the_stated_minimums_hold() {
        let edit = TranscriptionAsk { model: None, keyterms: 0, edit: true, entities: false, speaker_roles: false };
        assert!((edit.usd(3.0) - (3.0 * 0.22 + 10.0 * 0.07) / 3600.0).abs() < 1e-12);
        assert!((edit.usd(60.0) - 60.0 * 0.29 / 3600.0).abs() < 1e-12);
        let many = TranscriptionAsk { model: None, keyterms: 101, edit: false, entities: false, speaker_roles: false };
        assert!((many.usd(5.0) - 20.0 * 0.27 / 3600.0).abs() < 1e-12);
        let hundred = TranscriptionAsk { keyterms: 100, ..many };
        assert!((hundred.usd(5.0) - 5.0 * 0.27 / 3600.0).abs() < 1e-12);
    }

    /// Feed an answer to a fresh scan in chunks of `size` bytes.
    fn scanned(answer: &[u8], size: usize) -> TranscriptScan {
        let mut scan = TranscriptScan::default();
        for chunk in answer.chunks(size) {
            scan.feed(chunk);
        }
        scan
    }

    /// Strings that look like structure, escapes, a duration key inside a
    /// word and one inside an export: only the answer's own top-level
    /// duration counts, however the bytes arrive.
    #[test]
    fn the_scan_reads_the_duration_past_every_word_whatever_the_chunking() {
        let answer = br#"{
            "language_code": "en",
            "text": "she said \"}]{[\" then \\ and \"audio_duration_secs\": 999",
            "words": [
                {"text": "she", "start": 0.0, "end": 0.2, "type": "word", "logprob": -0.1,
                 "characters": [{"text": "s", "start": 0.0, "end": 0.1}]},
                {"text": "}", "audio_duration_secs": 999.0, "type": "spacing", "speaker_id": null, "x": true}
            ],
            "additional_formats": [{"content": "{\"audio_duration_secs\": 5}", "is_base64_encoded": false}],
            "entities": null,
            "audio_duration_secs": 12.5,
            "edited_transcript": null
        }"#;
        serde_json::from_slice::<Value>(answer).expect("the fixture is real JSON");
        for size in [1, 2, 3, 7, answer.len()] {
            assert_eq!(scanned(answer, size).billed(), Ok((12.5, 1)), "chunks of {size}");
        }
    }

    /// An answer split by channel bills each channel at the duration its
    /// transcript states.
    #[test]
    fn a_split_answer_bills_every_channel() {
        let answer = br#"{"transcripts": [
            {"text": "hello", "channel_index": 0, "words": [{"text": "hello", "channel_index": 0}], "audio_duration_secs": 30.0},
            {"text": "hi", "channel_index": 1, "words": [], "audio_duration_secs": 30.0}
        ], "transcription_id": "t1", "audio_duration_secs": 30.0}"#;
        assert_eq!(scanned(answer, 5).billed(), Ok((60.0, 2)));
        // One channel's duration missing: unknown, never half the bill.
        let missing = br#"{"transcripts": [{"text": "a", "audio_duration_secs": 30.0}, {"text": "b", "audio_duration_secs": null}]}"#;
        assert!(scanned(missing, 5).billed().is_err());
    }

    #[test]
    fn what_the_answer_does_not_settle_is_unknown() {
        // The combined style: the recording's length, once per channel
        // its words name.
        let combined = br#"{"text": "a b", "words": [{"text": "a", "channel_index": 0}, {"text": "b", "channel_index": 1}], "audio_duration_secs": 30.0}"#;
        assert_eq!(scanned(combined, 64).billed(), Ok((60.0, 2)));
        // A mono recording sent with `use_multi_channel` answers one channel.
        let mono = br#"{"text": "a", "words": [{"text": "a", "channel_index": 0}], "audio_duration_secs": 30.0}"#;
        assert_eq!(scanned(mono, 64).billed(), Ok((30.0, 1)));
        let webhook = br#"{"message": "Request accepted. Transcription result will be sent to the webhook.", "request_id": "abc123"}"#;
        assert!(scanned(webhook, 64).billed().is_err(), "a webhook request's early answer");
        let full = br#"{"text": "a", "audio_duration_secs": 30.0}"#;
        assert!(scanned(&full[..full.len() - 1], 64).billed().is_err(), "a cut answer");
        let trailing = br#"{"audio_duration_secs": 30.0} {"audio_duration_secs": 1.0}"#;
        assert!(scanned(trailing, 64).billed().is_err(), "bytes after the answer");
        assert!(scanned(b"not json", 64).billed().is_err());
        let deep = format!("{}{}", "[".repeat(100), "]".repeat(100));
        assert!(scanned(deep.as_bytes(), 64).billed().is_err(), "nesting past the bound");
    }

    /// The whole route: priced from the duration ElevenLabs answers, a
    /// linked recording included, with the trail saying how.
    #[tokio::test]
    async fn a_transcription_is_priced_from_the_duration_it_answers() {
        let run = |form: &[u8], status: u16, answer: &str| {
            let mut obs = ELEVENLABS.observe("speech-to-text", "", form);
            obs.on_status(status);
            for chunk in answer.as_bytes().chunks(16) {
                obs.on_chunk(chunk);
            }
            obs.end(false)
        };
        let hour = r#"{"language_code": "en", "text": "...", "words": [], "audio_duration_secs": 3600.0}"#;
        // A linked recording uploads nothing, and is still the hour it answers.
        let linked = transcription_form(None, &[("model_id", "scribe_v2"), ("source_url", "https://example.com/talk.mp3")]);
        let observed = run(&linked, 200, hour);
        assert!((observed.data["usd"].as_f64().unwrap() - 0.22).abs() < 1e-9, "{observed:?}");
        assert_eq!(observed.data["model"], json!("scribe_v2"));
        let http = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let shared = weft_core::shared::Shared::new(std::time::Duration::MAX);
        let follow = FollowUp { http: &http, base_url: NOWHERE, shared: &shared };
        let cost = ELEVENLABS.resolve("speech-to-text", observed, follow).await;
        assert_eq!(cost.metadata["audioSeconds"], json!(3600.0));
        assert_eq!(cost.metadata["channels"], json!(1));
        // A three-byte upload asking for keyterms, entities and speaker
        // roles, answering an hour.
        let uploaded = transcription_form(
            Some(&[1, 2, 3]),
            &[
                ("model_id", "scribe_v2"),
                ("keyterms", "weft"),
                ("entity_detection", r#"["pii"]"#),
                ("diarize", "true"),
                ("detect_speaker_roles", "true"),
            ],
        );
        let usd = run(&uploaded, 200, hour).data["usd"].as_f64().unwrap();
        assert!((usd - (0.22 + 0.05 + 0.066 + 0.022)).abs() < 1e-9, "{usd}");
        // Refused: nothing. Accepted with no duration: unknown, with why.
        assert_eq!(run(&linked, 422, r#"{"detail": "bad"}"#).data["usd"].as_f64(), Some(0.0));
        let unstated = run(&linked, 200, r#"{"text": "hi", "words": []}"#);
        assert_eq!(unstated.data["usd"].as_f64(), None);
        assert!(unstated.data["detail"]["resolution"].is_string(), "{unstated:?}");
    }

    /// One project route's answer, as the harness observes it.
    fn answered(path: &str, status: u16, body: serde_json::Value) -> crate::ObservedCall {
        let mut obs = ELEVENLABS.observe(path, "", b"multipart-or-json-ignored");
        obs.on_status(status);
        obs.on_chunk(&serde_json::to_vec(&body).unwrap());
        obs.end(false)
    }

    /// Fold one report into a project charge, with `base` answering any
    /// follow-up the meter makes.
    async fn fold_at(path: &str, observed: crate::ObservedCall, scratch: &mut Value, base: &str) -> Option<MeasuredCost> {
        let http = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let shared = weft_core::shared::Shared::new(std::time::Duration::MAX);
        ELEVENLABS.fold_report(path, observed, scratch, FollowUp { http: &http, base_url: base, shared: &shared }).await
    }

    /// Nothing listens here, so a follow-up the meter was not meant to
    /// make fails the test's expectation loudly.
    const NOWHERE: &str = "http://127.0.0.1:9";

    #[test]
    fn project_dubbing_routes_classify_and_trick_paths_stay_unknown() {
        assert_eq!(ELEVENLABS.classify("POST", "dubbing/project"), RouteClass::Billable(crate::Pricing::Metered));
        for (m, p) in [
            ("POST", "dubbing/project/p1/language"),
            ("GET", "dubbing/project/p1/language/l1"),
            ("GET", "dubbing/project/p1"),
        ] {
            assert_eq!(ELEVENLABS.classify(m, p), RouteClass::Reports, "{m} {p}");
        }
        // Deleting a project is how a node clears away one nobody keeps,
        // on the built-in connection too; listing them is no node's.
        assert_eq!(ELEVENLABS.classify("DELETE", "dubbing/project/p1"), RouteClass::Free);
        assert_eq!(ELEVENLABS.classify("GET", "dubbing/project"), RouteClass::Unknown);
        for (m, p) in [
            ("DELETE", "dubbing/project/../voices"),
            ("GET", "dubbing/project/p1/language"),
            ("GET", "dubbing/project/p1/transcript"),
            ("GET", "dubbing/project/p1/language/l1/extra"),
            ("GET", "dubbing/project/../user"),
            ("POST", "dubbing/project/%2e%2e/language"),
        ] {
            assert_eq!(ELEVENLABS.classify(m, p), RouteClass::Unknown, "{m} {p}");
        }
    }

    /// The create opens a charge under the project id; a language added
    /// counts; the project read states the duration; the first target
    /// read as finished closes it at duration x languages x rate.
    #[tokio::test]
    async fn a_project_dub_is_priced_when_its_language_finishes() {
        let created = answered(
            "dubbing/project",
            201,
            json!({ "project_id": "p1", "status": "queued", "model_id": "dubbing_v1", "media": null }),
        );
        assert_eq!(ELEVENLABS.opens_charge("dubbing/project", &created).as_deref(), Some("p1"));
        let mut scratch = created.data;
        let added = answered("dubbing/project/p1/language", 201, json!({ "language_id": "l1", "project_id": "p1", "status": "queued" }));
        assert_eq!(ELEVENLABS.charge_reported_on("dubbing/project/p1/language", &added).as_deref(), Some("p1"));
        assert!(fold_at("dubbing/project/p1/language", added, &mut scratch, NOWHERE).await.is_none());
        let project = answered("dubbing/project/p1", 200, json!({ "project_id": "p1", "status": "ready", "media": { "duration_s": 120.0 } }));
        assert!(fold_at("dubbing/project/p1", project, &mut scratch, NOWHERE).await.is_none(), "a project read never closes it");
        let working = answered("dubbing/project/p1/language/l1", 200, json!({ "status": "processing", "project_id": "p1" }));
        assert!(fold_at("dubbing/project/p1/language/l1", working, &mut scratch, NOWHERE).await.is_none());
        let done = answered("dubbing/project/p1/language/l1", 200, json!({ "status": "completed", "project_id": "p1" }));
        let cost = fold_at("dubbing/project/p1/language/l1", done, &mut scratch, NOWHERE).await.expect("a finished language closes it");
        assert!((cost.amount_usd.unwrap() - 1.0).abs() < 1e-9, "{cost:?}");
        assert_eq!(cost.model.as_deref(), Some("dubbing_v1"));
    }

    #[test]
    fn every_language_counts_at_the_models_rate() {
        let scratch = json!({ "model": "dubbing_v2", "languages": 2 });
        let cost = priced_project(&scratch, 60.0);
        assert!((cost.amount_usd.unwrap() - 4.40).abs() < 1e-9, "{cost:?}");
        let prepaid = priced_project(&json!({ "model": "dubbing_v1" }), 60.0);
        assert!((prepaid.amount_usd.unwrap() - 0.50).abs() < 1e-9, "the prepaid language counts once: {prepaid:?}");
        let unknown = priced_project(&json!({ "model": "dubbing_v9" }), 60.0);
        assert_eq!(unknown.amount_usd, None, "no rate, no guess");
        // A create that started a language, and one more added after it:
        // two languages.
        let both = priced_project(&json!({ "model": "dubbing_v1", "languageIds": ["l1"], "languages": 1 }), 60.0);
        assert!((both.amount_usd.unwrap() - 1.0).abs() < 1e-9, "{both:?}");
    }

    #[tokio::test]
    async fn a_failed_dub_closes_as_unknown() {
        let failed = answered("dubbing/project/p1/language/l1", 200, json!({ "status": "failed", "project_id": "p1" }));
        let mut scratch = json!({ "model": "dubbing_v1", "durationS": 30.0 });
        let cost = fold_at("dubbing/project/p1/language/l1", failed, &mut scratch, NOWHERE).await;
        let cost = cost.expect("a failed dub closes its charge");
        assert_eq!(cost.amount_usd, None);
    }

    /// When no read stated the source's duration before the language
    /// finished, the meter asks the project for it.
    #[tokio::test]
    async fn a_finished_language_asks_the_project_for_its_duration() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let _ = socket.read(&mut request).await;
            let body = r#"{"project_id":"p1","status":"ready","media":{"duration_s":30.0}}"#;
            let answer = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(answer.as_bytes()).await.unwrap();
        });
        let done = answered("dubbing/project/p1/language/l1", 200, json!({ "status": "completed", "project_id": "p1" }));
        let mut scratch = json!({ "model": "dubbing_v1" });
        let cost = fold_at("dubbing/project/p1/language/l1", done, &mut scratch, &base).await;
        let cost = cost.expect("a finished language closes it");
        assert!((cost.amount_usd.unwrap() - 0.25).abs() < 1e-9, "{cost:?}");
    }

    #[tokio::test]
    async fn a_refused_project_create_spends_nothing() {
        let refused = answered("dubbing/project", 422, json!({ "detail": [{ "loc": ["body", "file"], "msg": "Field required" }] }));
        assert_eq!(ELEVENLABS.opens_charge("dubbing/project", &refused), None);
        let http = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let shared = weft_core::shared::Shared::new(std::time::Duration::MAX);
        let follow = FollowUp { http: &http, base_url: NOWHERE, shared: &shared };
        let cost = ELEVENLABS.resolve("dubbing/project", refused, follow).await;
        assert_eq!(cost.amount_usd, Some(0.0));
    }

    /// The checked-in table parses, holds every row the capture had, and
    /// says when and for which tier it was captured.
    #[test]
    fn the_price_table_parses_whole() {
        assert_eq!(FLOW_PRICES.image_cents_per_image.len(), 82);
        assert_eq!(FLOW_PRICES.video_cents_per_second.len(), 296);
        assert_eq!(FLOW_PRICES.tier, "grant_tier_2_2025_07_23");
        assert_eq!(FLOW_PRICES.captured_on, "2026-10-16");
    }

    /// The key and the quantity a request prices at, in cents per unit.
    fn quoted(media: FlowMedia, body: Value) -> Result<FlowQuote, String> {
        flow_quote(media, &body)
    }

    fn assert_quote(media: FlowMedia, body: Value, key: &str, cents_per_unit: f64, units: f64) {
        let quote = quoted(media, body.clone()).unwrap_or_else(|why| panic!("{body}: {why}"));
        assert_eq!(quote.key, key, "{body}");
        assert!((quote.cents_per_unit - cents_per_unit).abs() < 1e-9, "{body}: {quote:?}");
        assert_eq!(quote.units, units, "{body}");
    }

    /// Every image family, each setting its price moves with, and the
    /// default the API reference states when the request leaves one out.
    /// No image model takes a count, so a request is one image.
    #[test]
    fn every_image_family_builds_its_price_key() {
        let image = |body: Value, key: &str, cents: f64| assert_quote(FlowMedia::Image, body, key, cents, 1.0);
        image(json!({ "model_id": "gemini-2.5-flash-image" }), "gemini-2.5-flash-image", 4.98);
        // Gemini: the resolution, 1K when left out.
        image(json!({ "model_id": "gemini-3-pro-image" }), "gemini-3-pro-image-1K", 24.12);
        image(json!({ "model_id": "gemini-3-pro-image", "resolution": "4K" }), "gemini-3-pro-image-4K", 36.0);
        image(json!({ "model_id": "gemini-3.1-flash-image", "resolution": "2K" }), "gemini-3.1-flash-image-2K", 12.12);
        image(json!({ "model_id": "gemini-3.1-flash-image", "resolution": "512" }), "gemini-3.1-flash-image-512", 5.4);
        image(json!({ "model_id": "gemini-3.1-flash-lite-image" }), "gemini-3.1-flash-lite-image-1K", 4.08);
        // GPT Image 1 and 1.5: quality then aspect ratio, medium and 1:1
        // when left out; the background moves nothing.
        image(json!({ "model_id": "gpt-image-1", "quality": "high", "aspect_ratio": "3:2" }), "gpt-image-1-high-3:2", 31.8);
        image(json!({ "model_id": "gpt-image-1", "background": "transparent" }), "gpt-image-1-medium-1:1", 6.84);
        image(json!({ "model_id": "gpt-image-1.5", "quality": "low", "aspect_ratio": "2:3" }), "gpt-image-1.5-low-2:3", 3.36);
        // GPT Image 2 and 2.5: quality alone (medium, and high on 2.5,
        // when left out); the table does not price their resolution.
        image(json!({ "model_id": "gpt-image-2", "resolution": "4K" }), "gpt-image-2-medium", 6.48);
        image(json!({ "model_id": "gpt-image-2", "quality": "high" }), "gpt-image-2-high", 25.44);
        image(json!({ "model_id": "gpt-image-2.5-sunburst" }), "gpt-image-2.5-sunburst-high", 6.6828);
        image(json!({ "model_id": "gpt-image-2.5-flare", "quality": "max" }), "gpt-image-2.5-flare-max", 25.65);
        // Seedream: 5 Pro by resolution (2K when left out), 5 Lite flat.
        image(json!({ "model_id": "bytedance-seedream-5-pro" }), "bytedance-seedream-5-pro-2K", 16.2);
        image(json!({ "model_id": "bytedance-seedream-5-pro", "resolution": "1K" }), "bytedance-seedream-5-pro-1K", 8.1);
        image(json!({ "model_id": "bytedance-seedream-5-lite", "resolution": "3K" }), "bytedance-seedream-5-lite", 4.2);
    }

    /// Every video family, priced per second times the request's length
    /// (or the model's default length).
    #[test]
    fn every_video_family_builds_its_price_key() {
        let video = |body: Value, key: &str, cents: f64, seconds: f64| {
            assert_quote(FlowMedia::Video, body, key, cents, seconds)
        };
        // Veo: `-4K` only at the top resolution, `-audio` with sound
        // (sound on and 8 seconds when left out).
        video(json!({ "model_id": "veo-3.1-generate-001" }), "veo-3.1-generate-001-audio", 48.0, 8.0);
        video(
            json!({ "model_id": "veo-3.1-generate-001", "resolution": "1080p", "generate_audio": false, "duration_secs": 4 }),
            "veo-3.1-generate-001",
            24.0,
            4.0,
        );
        video(json!({ "model_id": "veo-3.1-generate-001", "resolution": "4K" }), "veo-3.1-generate-001-4K-audio", 72.0, 8.0);
        video(
            json!({ "model_id": "veo-3.1-fast-generate-001", "resolution": "4K", "generate_audio": false, "duration_secs": 6 }),
            "veo-3.1-fast-generate-001-4K",
            36.0,
            6.0,
        );
        // Seedance: resolution then aspect ratio (720p, 16:9 and 5
        // seconds when left out); sound moves nothing.
        video(json!({ "model_id": "bytedance-seedance-v2" }), "bytedance-seedance-v2-720p-16:9", 36.288, 5.0);
        video(
            json!({ "model_id": "bytedance-seedance-v2", "resolution": "4k", "aspect_ratio": "1:1", "generate_audio": false, "duration_secs": 15 }),
            "bytedance-seedance-v2-4k-1:1",
            186.624,
            15.0,
        );
        video(json!({ "model_id": "bytedance-seedance-v2-fast", "resolution": "480p", "aspect_ratio": "21:9" }), "bytedance-seedance-v2-fast-480p-21:9", 13.5, 5.0);
        video(json!({ "model_id": "bytedance-seedance-v2-mini", "aspect_ratio": "9:16" }), "bytedance-seedance-v2-mini-720p-9:16", 18.144, 5.0);
        video(
            json!({ "model_id": "bytedance-seedance-v2.5", "resolution": "1080p", "aspect_ratio": "4:3", "duration_secs": 30 }),
            "bytedance-seedance-v2.5-1080p-4:3",
            124.248,
            30.0,
        );
    }

    /// Veo 3.1 without sound is 24 cents a second (the usage report's
    /// $0.24/s), so an 8-second video is $1.92.
    #[test]
    fn veo_is_24_cents_a_second() {
        let quote = quoted(
            FlowMedia::Video,
            json!({ "model_id": "veo-3.1-generate-001", "generate_audio": false, "duration_secs": 8 }),
        )
        .unwrap();
        assert!((quote.cents_per_unit - 24.0).abs() < 1e-9);
        assert!((quote.usd() - 1.92).abs() < 1e-9, "{quote:?}");
    }

    /// A key the table does not hold, a model with no rule, and a length
    /// that does not read are all unknown: never the table's `any` row,
    /// never the nearest key.
    #[test]
    fn what_the_table_does_not_price_is_unknown() {
        for (media, body) in [
            // A resolution the model's rows do not have.
            (FlowMedia::Image, json!({ "model_id": "gemini-3.1-flash-lite-image", "resolution": "4K" })),
            (FlowMedia::Image, json!({ "model_id": "gpt-image-1", "aspect_ratio": "16:9" })),
            // A model the table has a row for but no request rule (and
            // the `any` row, by name).
            (FlowMedia::Image, json!({ "model_id": "flux-1-kontext" })),
            (FlowMedia::Image, json!({ "model_id": "any" })),
            (FlowMedia::Video, json!({ "model_id": "any" })),
            (FlowMedia::Image, json!({ "prompt": "no model" })),
            // `auto` leaves the aspect, and so the price, to the model.
            (FlowMedia::Video, json!({ "model_id": "bytedance-seedance-v2", "aspect_ratio": "auto" })),
            // Seedance 2.5 uses `auto` with a start frame, whatever the
            // request says.
            (
                FlowMedia::Video,
                json!({ "model_id": "bytedance-seedance-v2.5", "aspect_ratio": "16:9", "start_frame": { "type": "asset", "asset_id": "a1" } }),
            ),
            // A lip-sync video is as long as its audio.
            (FlowMedia::Video, json!({ "model_id": "creatify-aurora", "resolution": "480p" })),
            // A length or a setting that does not read.
            (FlowMedia::Video, json!({ "model_id": "veo-3.1-generate-001", "duration_secs": "8" })),
            (FlowMedia::Video, json!({ "model_id": "veo-3.1-generate-001", "duration_secs": 0 })),
            (FlowMedia::Video, json!({ "model_id": "veo-3.1-generate-001", "generate_audio": "yes" })),
            (FlowMedia::Image, json!({ "model_id": "gpt-image-2", "quality": 3 })),
        ] {
            assert!(quoted(media, body.clone()).is_err(), "{body} must be unknown");
        }
    }

    /// One generation route's answer, as the harness observes it.
    fn generation_answer(path: &str, request: Value, status: u16, answer: Value) -> crate::ObservedCall {
        let mut obs = ELEVENLABS.observe(path, "", &serde_json::to_vec(&request).unwrap());
        obs.on_status(status);
        obs.on_chunk(&serde_json::to_vec(&answer).unwrap());
        obs.end(false)
    }

    /// The create opens a charge under the generation id carrying its
    /// quote; a read of the job while it runs settles nothing, and the
    /// read that finds it completed books the quote.
    #[tokio::test]
    async fn a_generation_is_charged_its_quote_once_it_completes() {
        let request = json!({ "model_id": "veo-3.1-generate-001", "prompt": "a corgi", "generate_audio": false, "duration_secs": 8 });
        let created = generation_answer("flows/video", request, 200, json!({ "id": "g1", "status": "pending" }));
        assert_eq!(ELEVENLABS.opens_charge("flows/video", &created).as_deref(), Some("g1"));
        let mut scratch = created.data;
        let running = generation_answer("flows/video/g1", Value::Null, 200, json!({ "id": "g1", "status": "generating" }));
        assert_eq!(ELEVENLABS.charge_reported_on("flows/video/g1", &running).as_deref(), Some("g1"));
        assert!(fold_at("flows/video/g1", running, &mut scratch, NOWHERE).await.is_none());
        let done = generation_answer(
            "flows/video/g1",
            Value::Null,
            200,
            json!({ "id": "g1", "status": "completed", "content_url": "https://x.example/v.mp4", "content_mime_type": "video/mp4" }),
        );
        let cost = fold_at("flows/video/g1", done, &mut scratch, NOWHERE).await.expect("a completed job closes it");
        assert!((cost.amount_usd.unwrap() - 1.92).abs() < 1e-9, "{cost:?}");
        assert_eq!(cost.model.as_deref(), Some("veo-3.1-generate-001"));
        assert_eq!(cost.metadata["priceKey"], json!("veo-3.1-generate-001"));
        assert_eq!(cost.metadata["seconds"], json!(8.0));
    }

    /// A failed generation is not charged; one whose request the table
    /// cannot price closes unknown once it completes, saying why.
    #[tokio::test]
    async fn a_failed_generation_is_free_and_an_unpriced_one_unknown() {
        let priced = json!({ "model_id": "gemini-3.1-flash-image", "prompt": "a corgi" });
        let mut scratch = generation_answer("flows/image", priced, 200, json!({ "id": "g2", "status": "pending" })).data;
        let failed = generation_answer(
            "flows/image/g2",
            Value::Null,
            200,
            json!({ "id": "g2", "status": "failed", "failure_reason": "moderation", "error_message": "no" }),
        );
        let cost = fold_at("flows/image/g2", failed, &mut scratch, NOWHERE).await.expect("a failed job closes it");
        assert_eq!(cost.amount_usd, Some(0.0));

        let unpriced = json!({ "model_id": "gemini-3.1-flash-lite-image", "resolution": "4K", "prompt": "a corgi" });
        let mut scratch = generation_answer("flows/image", unpriced, 200, json!({ "id": "g3", "status": "pending" })).data;
        let done = generation_answer("flows/image/g3", Value::Null, 200, json!({ "id": "g3", "status": "completed" }));
        let cost = fold_at("flows/image/g3", done, &mut scratch, NOWHERE).await.expect("a completed job closes it");
        assert_eq!(cost.amount_usd, None);
        let why = cost.metadata["resolution"].as_str().unwrap();
        assert!(why.contains("gemini-3.1-flash-lite-image-4K"), "{why}");
    }

    /// A refused create opens nothing and spends nothing.
    #[tokio::test]
    async fn a_refused_generation_spends_nothing() {
        let request = json!({ "model_id": "veo-3.1-generate-001", "prompt": "a corgi", "duration_secs": 5 });
        let refused = generation_answer("flows/video", request, 422, json!({ "detail": "duration_secs must be 4, 6 or 8" }));
        assert_eq!(ELEVENLABS.opens_charge("flows/video", &refused), None);
        let http = reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build();
        let shared = weft_core::shared::Shared::new(std::time::Duration::MAX);
        let follow = FollowUp { http: &http, base_url: NOWHERE, shared: &shared };
        let cost = ELEVENLABS.resolve("flows/video", refused, follow).await;
        assert_eq!(cost.amount_usd, Some(0.0));
    }
}
