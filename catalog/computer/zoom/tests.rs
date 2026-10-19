//! Zoom self-tests: the region asked for and the image stored, and a
//! refusal reaching `_error`.

use serde_json::json;

use weft::storage::StoredFile;
use weft::{FakeRig, NodeTest, WeftResult};

use super::super::testing::{answers, asked, computer, PNG_BASE64};
use super::ZoomNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("asks_for_the_region_and_stores_the_image", zooms),
        NodeTest::fake("a_region_off_the_screen_reaches_error", refused),
    ]
}

async fn zooms(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "png": PNG_BASE64 }));
    let args = json!({ "region": [0, 0, 320, 200] });
    let outcome = rig.run(&ZoomNode, json!({ "computer": computer, "args": args })).await.ok()?;
    assert_eq!(outcome.outputs["result"], json!("Zoomed in on the region."));
    assert_eq!(StoredFile::from_value(&outcome.outputs["image"])?.mime_type, "image/png");
    assert_eq!(asked(&rig), vec![("zoom".to_string(), args)]);
    Ok(())
}

async fn refused(rig: FakeRig) -> WeftResult<()> {
    let computer = computer(&rig);
    answers(&rig, json!({ "error": "the region [0, 0, 9000, 200] is not a box inside the 1280x800 screen" }));
    rig.wire_output("_error");
    let outcome = rig.run(&ZoomNode, json!({ "computer": computer, "args": { "region": [0, 0, 9000, 200] } })).await.ok()?;
    let error = outcome.output("_error")?.as_str().expect("the error is text").to_string();
    assert!(error.contains("zoom: the region"), "{error}");
    assert!(outcome.outputs.get("image").is_none(), "a refused zoom stores nothing");
    Ok(())
}
