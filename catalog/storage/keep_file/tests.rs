//! KeepFile self-tests: the keep flag lands and the exact marker
//! passes through; a project copy is a new file with the same shape,
//! living until deleted unless `ttlDays` gives it a lifetime.

use serde_json::json;

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use super::KeepFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("keeps_and_passes_the_marker_through", keeps),
        NodeTest::fake("project_scope_copies_the_file_and_emits_the_copy", copies_into_project),
        NodeTest::fake("ttl_days_gives_the_project_copy_a_lifetime", ttl_in_project),
        NodeTest::fake("ttl_days_sets_how_long_a_kept_run_file_lives", ttl_in_run),
        NodeTest::fake("a_video_stays_a_video_whichever_scope", video_stays_video),
    ]
}

/// The output is the input's own kind (`T: File`): a Video kept or
/// copied comes out as a Video, which a port typed `Video` downstream
/// takes. Held to `Video` here as such a program would declare it.
async fn video_stays_video(rig: FakeRig) -> WeftResult<()> {
    let video = rig.store_file("clip.mp4", "video/mp4", b"\x00\x00\x00\x18ftypmp42".to_vec());
    rig.output_type("file", weft::WeftType::parse("Video").expect("parses"));
    for scope in ["run", "project"] {
        let outcome = rig.run(&KeepFileNode, json!({ "file": video.clone(), "scope": scope })).await.ok()?;
        let out = &outcome.outputs["file"];
        assert!(out.get("__weft_video__").is_some(), "{scope}: still a video: {out}");
    }
    Ok(())
}

async fn keeps(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("keeper.txt", "text/plain", b"data".to_vec());
    let outcome = rig
        .run(&KeepFileNode, json!({ "file": file.clone() }))
        .await
        .ok()?;
    assert_eq!(outcome.outputs["file"], file, "the marker passes through untouched");
    let key = StoredFile::from_value(&file)?.key;
    assert!(rig.stored_meta(&key)?.keep, "the keep flag landed on the run's file");
    Ok(())
}

/// The copy is a NEW stored file carrying the source's filename, mime
/// type, size and bytes; the source stays where it was, unkept.
async fn copies_into_project(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("upload.png", "image/png", b"PNG!".to_vec());
    let outcome = rig
        .run(&KeepFileNode, json!({ "file": file.clone(), "scope": "project" }))
        .await
        .ok()?;
    let source = StoredFile::from_value(&file)?;
    let copy = StoredFile::from_value(&outcome.outputs["file"])?;
    assert_ne!(copy.key, source.key, "the emitted reference is the copy, not the source");
    assert_eq!(copy.filename, "upload.png");
    assert_eq!(copy.mime_type, "image/png");
    assert_eq!(copy.size_bytes, 4);
    let copy_meta = rig.stored_meta(&copy.key)?;
    assert_eq!(copy_meta.size_bytes, 4, "the bytes were copied");
    assert!(!copy_meta.keep, "a project file carries no keep flag");
    assert!(!rig.stored_meta(&source.key)?.keep, "the source is left as it was");
    Ok(())
}

/// A project copy with `ttlDays` expires once idle that long; without
/// one (the previous case) it lives until deleted.
async fn ttl_in_project(rig: FakeRig) -> WeftResult<()> {
    let file = rig.store_file("upload.png", "image/png", b"PNG!".to_vec());
    let outcome = rig
        .run(&KeepFileNode, json!({ "file": file, "scope": "project", "ttlDays": 7 }))
        .await
        .ok()?;
    let copy = rig.stored_meta(&StoredFile::from_value(&outcome.outputs["file"])?.key)?;
    assert_eq!(copy.keep_ttl_secs, Some(7 * 24 * 3600), "the copy lives 7 idle days");
    assert!(!copy.keep, "the run-end flag is a run file's alone");
    let forever = rig.store_file("b.png", "image/png", b"PNG!".to_vec());
    let outcome = rig.run(&KeepFileNode, json!({ "file": forever, "scope": "project" })).await.ok()?;
    let copy = rig.stored_meta(&StoredFile::from_value(&outcome.outputs["file"])?.key)?;
    assert_eq!(copy.keep_ttl_secs, None, "no ttlDays: the project copy lives until deleted");
    Ok(())
}

/// On a run file: empty is the storage default, 0 is forever, a number
/// is that many idle days.
async fn ttl_in_run(rig: FakeRig) -> WeftResult<()> {
    for (ttl_days, expected) in [(None, Some(30 * 24 * 3600)), (Some(0), None), (Some(2), Some(2 * 24 * 3600))] {
        let file = rig.store_file("keeper.txt", "text/plain", b"data".to_vec());
        let mut inputs = json!({ "file": file.clone() });
        if let Some(days) = ttl_days {
            inputs["ttlDays"] = json!(days);
        }
        rig.run(&KeepFileNode, inputs).await.ok()?;
        let meta = rig.stored_meta(&StoredFile::from_value(&file)?.key)?;
        assert!(meta.keep);
        assert_eq!(meta.keep_ttl_secs, expected, "ttlDays {ttl_days:?}");
    }
    Ok(())
}
