//! ElevenLabsSoundEffect self-tests: the request the node builds and the
//! stored file it emits, per output format.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeErrExt, NodeTest, WeftResult};

use super::super::elevenlabs::audio_file_type;
use super::ElevenLabsSoundEffectNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("every_offered_output_format_has_a_file_type", file_types),
        NodeTest::fake("generates_and_stores_the_effect", generates),
        NodeTest::fake("every_setting_reaches_the_request", every_setting),
        NodeTest::fake("each_format_is_stored_under_its_own_name_and_type", formats),
        NodeTest::live("one_real_short_effect", "elevenlabs", live_effect),
    ]
}

/// One real one-second effect on the default model and format (sound
/// effects are priced by the minute of audio); nothing lands on the
/// account.
async fn live_effect(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ElevenLabsSoundEffectNode,
            json!({
                "account": rig.access("elevenlabs"),
                "text": "a single soft click",
                "durationSecs": 1,
                "loop": false,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.output("mimeType")?.as_str(), Some("audio/mpeg"));
    let size = outcome.output("sizeBytes")?.as_f64().unwrap_or(0.0);
    assert!(size > 500.0, "one second of mp3 is non-trivial: {size}");
    Ok(())
}

/// Every format the metadata offers stores under a real extension and
/// mime.
fn file_types() -> WeftResult<()> {
    let metadata: Value =
        serde_json::from_str(include_str!("metadata.json")).node_err("parse the sound effect metadata")?;
    let input = metadata["inputs"]
        .as_array()
        .and_then(|inputs| inputs.iter().find(|i| i["name"] == "outputFormat"))
        .node_err("the metadata declares `outputFormat`")?;
    let options = input["widget"]["options"].as_array().node_err("`outputFormat` offers options")?;
    assert!(!options.is_empty());
    for format in options {
        audio_file_type(format.as_str().node_err("every option is a string")?)?;
    }
    Ok(())
}

const SOUND: &str = "/v1/sound-generation";

/// The node's account and a prompt, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({ "account": rig.access("elevenlabs"), "text": "rain on glass" });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn generates(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", &format!("{SOUND}?output_format=mp3_44100_128"), 200, "audio/mpeg", b"fx".to_vec());
    let outcome =
        rig.run(&ElevenLabsSoundEffectNode, inputs(&rig, json!({ "durationSecs": 4, "loop": true }))).await.ok()?;
    assert_eq!(outcome.outputs["filename"], json!("sound_effect.mp3"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/mpeg"));
    let sent = &rig.requests()[0];
    assert_eq!(sent.query.as_deref(), Some("output_format=mp3_44100_128"), "the default format is asked for by name");
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "text": "rain on glass",
            "model_id": "eleven_text_to_sound_v2",
            "loop": true,
            "duration_seconds": 4.0,
        })
    );
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    rig.respond_raw("POST", &format!("{SOUND}?output_format=opus_48000_96"), 200, "audio/ogg", b"fx".to_vec());
    let outcome = rig
        .run(
            &ElevenLabsSoundEffectNode,
            inputs(
                &rig,
                json!({
                    "durationSecs": 2.5,
                    "promptInfluence": 0.8,
                    "model": "eleven_text_to_sound_v2",
                    "outputFormat": "opus_48000_96",
                }),
            ),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["filename"], json!("sound_effect.ogg"));
    assert_eq!(outcome.outputs["mimeType"], json!("audio/ogg; codecs=opus"));
    assert_eq!(
        rig.requests()[0].body.as_ref().expect("json payload"),
        &json!({
            "text": "rain on glass",
            "model_id": "eleven_text_to_sound_v2",
            "loop": false,
            "duration_seconds": 2.5,
            "prompt_influence": 0.8,
        })
    );
    Ok(())
}

async fn formats(rig: FakeRig) -> WeftResult<()> {
    for (format, filename, mime) in [
        ("mp3_22050_32", "sound_effect.mp3", "audio/mpeg"),
        ("pcm_16000", "sound_effect.pcm", "audio/pcm"),
        ("ulaw_8000", "sound_effect.ulaw", "audio/basic"),
        ("alaw_8000", "sound_effect.alaw", "audio/PCMA"),
    ] {
        rig.respond_raw("POST", &format!("{SOUND}?output_format={format}"), 200, mime, b"fx".to_vec());
        let outcome = rig.run(&ElevenLabsSoundEffectNode, inputs(&rig, json!({ "outputFormat": format }))).await.ok()?;
        assert_eq!(outcome.outputs["filename"], json!(filename), "{format}");
        assert_eq!(outcome.outputs["mimeType"], json!(mime), "{format}");
        let sent = rig.requests();
        let query = sent.last().and_then(|r| r.query.clone());
        assert_eq!(query, Some(format!("output_format={format}")));
    }
    Ok(())
}
