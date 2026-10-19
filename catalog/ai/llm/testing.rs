//! Shared fake-tier plumbing for the llm package's tests: the provider
//! object a provider node would emit, a conversation file and how to
//! read one back, and a canned OpenAI-style SSE reply.
#![cfg(feature = "node-tests")]

use serde_json::{json, Value};

use weft::storage::{StorageScope, StoredFile};
use weft::{FakeRig, WeftResult};

/// An OpenRouter `LlmProvider` object on the rig's fake connection.
pub fn provider(rig: &FakeRig) -> Value {
    json!({ "kind": "openrouter", "model": "test/model", "account": rig.access("openrouter") })
}

/// A conversation as an author starts one: a project file holding it.
pub fn conversation_file(rig: &FakeRig, messages: Value) -> Value {
    rig.store_file_in(&StorageScope::Project, "chat.json", "application/json", serde_json::to_vec(&messages).expect("json"))
}

/// The conversation the file value `file` names holds now.
pub fn conversation(rig: &FakeRig, file: &Value) -> WeftResult<Vec<Value>> {
    let key = StoredFile::from_value(file)?.key;
    let written: Value = serde_json::from_slice(&rig.stored_bytes(&key)?).expect("the file holds JSON");
    Ok(written.as_array().expect("a list of messages").clone())
}

/// OpenAI-style SSE chunks ending in a finish reason and `[DONE]`.
pub fn sse(chunks: &[&str], finish: Option<&str>) -> Vec<u8> {
    let mut body = String::new();
    for c in chunks {
        body.push_str(&format!(
            "data: {}\n\n",
            json!({ "id": "gen-1", "choices": [{ "delta": { "content": c } }] })
        ));
    }
    if let Some(reason) = finish {
        body.push_str(&format!(
            "data: {}\n\n",
            json!({ "id": "gen-1", "choices": [{ "delta": {}, "finish_reason": reason }] })
        ));
    }
    body.push_str("data: [DONE]\n\n");
    body.into_bytes()
}
