//! ElevenLabsGenerateVideo self-tests: the request each setting makes,
//! the frames as references, the parked wait, the stored video, and
//! the two ways a generation fails.

use serde_json::{json, Value};

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use crate::flows::video_resolution;
use crate::testing::{completes, create_body, emitted_file, fails, inline};

use super::ElevenLabsGenerateVideoNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("the_top_resolution_is_spelled_the_models_way", top_resolution),
        NodeTest::fake("a_prompt_alone_sends_the_model_prompt_and_sound", prompt_alone),
        NodeTest::fake("frames_and_every_setting_reach_the_request", every_setting),
        NodeTest::fake("seedance_gets_its_own_spelling_of_4k", seedance_4k),
        NodeTest::fake("a_failed_generation_fails_with_elevenlabs_reason", failed),
        NodeTest::fake("a_failed_generation_comes_out_on_error_when_it_is_wired", failed_caught),
        NodeTest::fake("a_refused_create_quotes_elevenlabs_and_never_waits", refused),
        NodeTest::live("one_real_four_second_video", "elevenlabs", live_generate),
    ]
}

fn top_resolution() -> WeftResult<()> {
    assert_eq!(video_resolution("veo-3.1-generate-001", "4k"), "4K");
    assert_eq!(video_resolution("veo-3.1-fast-generate-001", "4K"), "4K");
    assert_eq!(video_resolution("bytedance-seedance-v2", "4K"), "4k");
    assert_eq!(video_resolution("bytedance-seedance-v2", "1080p"), "1080p", "only 4K has two spellings");
    assert_eq!(video_resolution("veo-3.1-generate-001", "1080p"), "1080p", "only 4K has two spellings");
    Ok(())
}

fn inputs(rig: &FakeRig, extra: Value) -> Value {
    let mut base = json!({
        "account": rig.access("elevenlabs"),
        "prompt": "a corgi surfing a small wave at sunset",
    });
    for (k, v) in extra.as_object().expect("extra inputs are an object") {
        base[k] = v.clone();
    }
    base
}

async fn prompt_alone(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "v1", "video/mp4", b"mp4-bytes");
    let outcome = rig.run(&ElevenLabsGenerateVideoNode, inputs(&rig, json!({}))).await.ok()?;

    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "veo-3.1-fast-generate-001",
            "prompt": "a corgi surfing a small wave at sunset",
            "generate_audio": true,
        }),
        "the default model and sound travel; a setting nobody gave never does"
    );
    let (bytes, filename, mime) = emitted_file(&rig, &outcome.outputs, "video")?;
    assert_eq!(bytes, b"mp4-bytes");
    assert_eq!(filename, "video.mp4");
    assert_eq!(mime, "video/mp4");

    // The wait parks on the generation, signed by the connection, and
    // looks every 10 seconds, the most ElevenLabs asks for on a video.
    let awaited = rig.awaited_signals();
    assert_eq!(awaited.len(), 1, "one wait");
    assert_eq!(awaited[0].kind, "poll_endpoint");
    assert_eq!(awaited[0].config["url"], json!("https://api.elevenlabs.io/v1/flows/video/v1"));
    assert_eq!(awaited[0].config["interval_secs"], json!(10));
    assert_eq!(awaited[0].config["carry"], json!(["status"]));
    assert!(awaited[0].access.is_some(), "each look is signed by the ElevenLabs connection");
    Ok(())
}

async fn every_setting(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "v2", "video/mp4", b"mp4");
    let first = rig.store_file("first.png", "image/png", b"first".to_vec());
    let last = rig.store_file("last.jpg", "image/jpeg", b"last".to_vec());
    rig.run(
        &ElevenLabsGenerateVideoNode,
        inputs(
            &rig,
            json!({
                "model": "veo-3.1-generate-001",
                "startFrame": first,
                "endFrame": last,
                "durationSecs": 6,
                "aspectRatio": "9:16",
                "resolution": "4k",
                "generateAudio": false,
                "negativePrompt": "people, text",
                "promptAsWritten": true,
                "seed": 7,
            }),
        ),
    )
    .await
    .ok()?;
    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "veo-3.1-generate-001",
            "prompt": "a corgi surfing a small wave at sunset",
            "generate_audio": false,
            "start_frame": inline("image/png", b"first"),
            "end_frame": inline("image/jpeg", b"last"),
            "duration_secs": 6,
            "aspect_ratio": "9:16",
            "resolution": "4K",
            "negative_prompt": "people, text",
            "enhance_prompt": false,
            "seed": 7,
        })
    );
    Ok(())
}

async fn seedance_4k(rig: FakeRig) -> WeftResult<()> {
    completes(&rig, "video", "v3", "video/mp4", b"mp4");
    rig.run(
        &ElevenLabsGenerateVideoNode,
        inputs(&rig, json!({ "model": "bytedance-seedance-v2", "resolution": "4K" })),
    )
    .await
    .ok()?;
    assert_eq!(
        create_body(&rig),
        json!({
            "model_id": "bytedance-seedance-v2",
            "prompt": "a corgi surfing a small wave at sunset",
            "generate_audio": true,
            "resolution": "4k",
        })
    );
    Ok(())
}

async fn failed(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "video", "v4", "moderated", "The prompt was rejected by content moderation. You were not charged for this generation.");
    let err = rig.run(&ElevenLabsGenerateVideoNode, inputs(&rig, json!({}))).await.failure()?;
    assert!(err.contains("moderated"), "{err}");
    assert!(err.contains("You were not charged"), "{err}");
    assert!(err.contains("v4"), "the error names the generation: {err}");
    Ok(())
}

async fn failed_caught(rig: FakeRig) -> WeftResult<()> {
    fails(&rig, "video", "v5", "timeout", "Timed out while processing.");
    rig.wire_output("_error");
    let outcome = rig.run(&ElevenLabsGenerateVideoNode, inputs(&rig, json!({}))).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("error is a string").to_string();
    assert!(error.contains("timeout"), "{error}");
    assert!(error.contains("Timed out while processing."), "{error}");
    assert!(!outcome.outputs.contains_key("video"), "a caught failure emits no video");
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/v1/flows/video",
        422,
        json!({ "detail": [{ "loc": ["body", "veo-3.1-fast-generate-001", "duration_secs"], "msg": "Input should be 4, 6 or 8", "type": "literal_error" }] }),
    );
    let err = rig
        .run(&ElevenLabsGenerateVideoNode, inputs(&rig, json!({ "durationSecs": 5 })))
        .await
        .failure()?;
    assert!(err.contains("422"), "{err}");
    assert!(err.contains("body.veo-3.1-fast-generate-001.duration_secs: Input should be 4, 6 or 8"), "{err}");
    assert!(rig.awaited_signals().is_empty(), "a refused create never parks");
    Ok(())
}

/// One real 4-second 720p video with no sound on Veo 3.1 Fast, the
/// shortest Veo makes: it bills that one video to the account's
/// credits. Nothing is left on the account: generations are listed by
/// the API but hold no storage.
async fn live_generate(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &ElevenLabsGenerateVideoNode,
            json!({
                "account": rig.access("elevenlabs"),
                "prompt": "a red ball rolls slowly across a white table",
                "model": "veo-3.1-fast-generate-001",
                "durationSecs": 4,
                "resolution": "720p",
                "generateAudio": false,
            }),
        )
        .await
        .ok()?;
    let file = weft::storage::StoredFile::from_value(outcome.output("video")?)?;
    assert!(file.mime_type.starts_with("video/"), "{}", file.mime_type);
    assert!(file.size_bytes > 10_000, "a real video is more than 10 KB: {}", file.size_bytes);
    Ok(())
}
