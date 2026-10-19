//! Shared fake-tier plumbing for the computer package's tests: a
//! computer on the `computer` input, its answers, and what it was asked.
#![cfg(feature = "node-tests")]

use serde_json::{json, Value};

use weft::{EndpointMethod, FakeRig};

/// A computer shared by a Computer node named `machine`, as the handle
/// to put on the node's `computer` input.
pub fn computer(rig: &FakeRig) -> Value {
    rig.declare_infra("machine", "api", "http://computer.example:8000")
}

/// Queue the computer's answer to the node's next action: `result` is
/// what the action envelope carries back.
pub fn answers(rig: &FakeRig, result: Value) {
    rig.answer_infra("machine", "api", EndpointMethod::Post, "/action", json!({ "result": result }));
}

/// The actions the node asked the computer for, as (action, payload).
pub fn asked(rig: &FakeRig) -> Vec<(String, Value)> {
    rig.endpoint_calls()
        .into_iter()
        .map(|call| {
            assert_eq!((call.place.as_str(), call.path.as_str()), ("machine", "/action"), "every action goes through the envelope");
            let body = call.body.expect("an action carries a body");
            (body["action"].as_str().expect("the action's name").to_string(), body["payload"].clone())
        })
        .collect()
}

/// A tiny PNG, base64, the way the computer sends a screenshot.
pub const PNG_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
