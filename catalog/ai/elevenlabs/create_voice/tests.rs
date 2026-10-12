//! ElevenLabsCreateVoice self-tests: the create request, the parts of
//! the minted voice the node hands on, and the description ElevenLabs
//! would refuse, refused before anything is sent.

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult};

use super::{description_refusal, ElevenLabsCreateVoiceNode};

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("creates_and_emits_the_voice", creates),
        NodeTest::fake("a_voice_with_no_preview_link_closes_that_port", no_preview),
        NodeTest::fake("a_description_elevenlabs_would_refuse_is_refused_before_anything_is_sent", refused),
        NodeTest::basic("the_description_lengths_elevenlabs_takes", description_limits),
    ]
}

const DESCRIPTION: &str = "A calm, warm narrator with a soft voice";

/// The inputs of a plain create, plus `extra`.
fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "name": "Narrator",
        "description": DESCRIPTION,
        "previewId": "gen-1",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn creates(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/text-to-voice",
        json!({
            "voice_id": "voice-durable",
            "name": "Narrator",
            "category": "generated",
            "labels": { "language": "en", "accent": "british" },
            "preview_url": "https://storage.googleapis.com/previews/voice-durable.mp3",
        }),
    );
    let outcome = rig
        .run(&ElevenLabsCreateVoiceNode, inputs(&rig, json!({ "labels": { "language": "en", "accent": "british" } })))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["voiceId"], json!("voice-durable"));
    assert_eq!(
        outcome.outputs["previewUrl"],
        json!("https://storage.googleapis.com/previews/voice-durable.mp3")
    );
    assert_eq!(outcome.outputs["labels"], json!({ "language": "en", "accent": "british" }));

    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({
            "voice_name": "Narrator",
            "voice_description": DESCRIPTION,
            "generated_voice_id": "gen-1",
            "labels": { "accent": "british", "language": "en" },
        })
    );
    Ok(())
}

async fn no_preview(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v1/text-to-voice",
        json!({ "voice_id": "voice-durable", "labels": {}, "preview_url": null }),
    );
    let outcome = rig.run(&ElevenLabsCreateVoiceNode, inputs(&rig, json!({}))).await.ok()?;
    assert_eq!(outcome.outputs["voiceId"], json!("voice-durable"));
    assert!(!outcome.outputs.contains_key("previewUrl"), "no link answered, so the port closes");
    assert_eq!(outcome.outputs["labels"], json!({}), "the voice's labels, even when there are none");
    let body = rig.requests()[0].body.clone().expect("json payload");
    assert!(body.get("labels").is_none(), "no labels given, none sent: {body}");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.wire_output("error");
    for (description, says) in
        [("calm narrator".to_string(), "`description` is 13 characters"), ("x".repeat(1001), "`description` is 1001 characters")]
    {
        let err = rig.run(&ElevenLabsCreateVoiceNode, inputs(&rig, json!({ "description": description }))).await.failure()?;
        assert!(err.starts_with("input error"), "a mistake in the program fails the run even with `error` wired: {err}");
        assert!(err.contains(says), "{err}");
    }
    assert!(rig.requests().is_empty(), "nothing is sent, so no voice is minted");
    Ok(())
}

fn description_limits() -> WeftResult<()> {
    assert!(description_refusal(&"x".repeat(19)).is_some());
    assert!(description_refusal(&"x".repeat(20)).is_none());
    assert!(description_refusal(&"é".repeat(20)).is_none(), "characters count, not bytes");
    assert!(description_refusal(&"x".repeat(1000)).is_none());
    assert!(description_refusal(&"x".repeat(1001)).is_some());
    Ok(())
}
