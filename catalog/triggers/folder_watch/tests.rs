//! FolderWatch self-tests (fake tier only: what the folder does is the
//! listener's, tested there). Covers both trigger bodies: `setup_trigger`
//! registers the folder watch from the settings, `run` forwards a fire's
//! wake payload.

use serde_json::json;

use weft::{FakeRig, NodeTest, WeftResult};

use super::FolderWatchNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("setup_registers_the_folder_watch", setup_registers),
        NodeTest::fake("a_fire_forwards_its_wake_payload", fire_forwards),
    ]
}

async fn setup_registers(rig: FakeRig) -> WeftResult<()> {
    rig.run_setup_trigger(&FolderWatchNode, json!({ "folder": "/home/me/inbox" }))
        .await
        .ok()?;
    let registered = rig.registered_signals();
    assert_eq!(registered.len(), 1, "exactly one signal registered");
    let (spec, _) = &registered[0];
    assert_eq!(spec.kind, "folder_watch");
    assert_eq!(spec.config["folder"], "/home/me/inbox");
    assert_eq!(spec.config["recursive"], false, "only the folder itself unless asked");
    assert!(spec.config.get("pattern").is_none(), "every file");
    assert_eq!(spec.config["events"], json!(["created", "changed", "removed"]), "no events picked is every event");

    let rig = FakeRig::new();
    rig.run_setup_trigger(
        &FolderWatchNode,
        json!({ "folder": "/in", "recursive": true, "pattern": "*.pdf", "events": ["created"] }),
    )
    .await
    .ok()?;
    let config = &rig.registered_signals()[0].0.config;
    assert_eq!(config["recursive"], true);
    assert_eq!(config["pattern"], "*.pdf");
    assert_eq!(config["events"], json!(["created"]));

    // A "Fire on" list emptied by hand is not every event: it goes to the
    // kind as it is, and the kind refuses it.
    let rig = FakeRig::new();
    rig.run_setup_trigger(&FolderWatchNode, json!({ "folder": "/in", "events": [] }))
        .await
        .ok()?;
    let (spec, _) = &rig.registered_signals()[0];
    assert_eq!(spec.config["events"], json!([]));
    let err = weft::signal::validate_spec(spec).expect_err("an empty list is refused");
    assert!(err.contains("nothing would ever fire"), "{err}");
    Ok(())
}

async fn fire_forwards(rig: FakeRig) -> WeftResult<()> {
    rig.wake(json!({ "path": "/in/a.pdf", "event": "created", "sizeBytes": 12 }));
    let outcome = rig.run(&FolderWatchNode, json!({ "folder": "/in" })).await.ok()?;
    assert_eq!(outcome.outputs["path"], json!("/in/a.pdf"));
    assert_eq!(outcome.outputs["event"], json!("created"));
    assert_eq!(outcome.outputs["sizeBytes"], json!(12));

    // A removed file has no size, and its port stays empty.
    rig.wake(json!({ "path": "/in/a.pdf", "event": "removed" }));
    let outcome = rig.run(&FolderWatchNode, json!({ "folder": "/in" })).await.ok()?;
    assert_eq!(outcome.outputs["event"], json!("removed"));
    assert!(outcome.outputs.get("sizeBytes").is_none(), "{:?}", outcome.outputs);

    // A fire that lost its path is a loud error naming it.
    rig.wake(json!({ "event": "created" }));
    let err = rig
        .run(&FolderWatchNode, json!({ "folder": "/in" }))
        .await
        .result
        .expect_err("a wake missing path fails loud")
        .to_string();
    assert!(err.contains("path"), "the error names the missing field: {err}");
    Ok(())
}
