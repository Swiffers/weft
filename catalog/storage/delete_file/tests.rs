//! DeleteFile self-tests: the file goes, in any scope, and a file that is
//! not stored is refused naming why.

use serde_json::json;

use weft::storage::{StorageScope, StoredFile};
use weft::{FakeRig, NodeTest, WeftResult};

use super::DeleteFileNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("the_file_is_gone_whichever_scope", deletes),
        NodeTest::fake("a_file_at_an_outside_address_is_refused", url_refused),
    ]
}

async fn deletes(rig: FakeRig) -> WeftResult<()> {
    for scope in [StorageScope::Run, StorageScope::Project] {
        let file = rig.store_file_in(&scope, "old.txt", "text/plain", b"bye".to_vec());
        let key = StoredFile::from_value(&file)?.key;
        let outcome = rig.run(&DeleteFileNode, json!({ "file": file })).await.ok()?;
        assert_eq!(outcome.outputs["done"], json!(true));
        assert!(rig.stored_bytes(&key).is_err(), "{scope:?}: the file is gone");
    }
    Ok(())
}

async fn url_refused(rig: FakeRig) -> WeftResult<()> {
    let external = json!({ "__weft_image__": { "url": "https://cdn.example/a.png", "mimeType": "image/png", "filename": "a.png", "sizeBytes": 1 } });
    let err = rig.run(&DeleteFileNode, json!({ "file": external })).await.failure()?;
    assert!(err.contains("external URL"), "{err}");
    Ok(())
}
