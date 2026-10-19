//! Debug self-tests: a sink that logs what flows in.

use serde_json::json;

use weft::bus::{BusOptions, StreamEnd};
use weft::{FakeRig, NodeTest, WeftResult};

use super::DebugNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("logs_the_value_under_the_node_label", logs_value),
        NodeTest::fake("an_absent_input_logs_null", absent_logs_null),
        NodeTest::fake("a_bus_that_broke_off_is_logged_as_failed", failed_bus),
    ]
}

async fn logs_value(rig: FakeRig) -> WeftResult<()> {
    let outcome = rig.run(&DebugNode, json!({ "data": { "n": 42 } })).await.ok()?;
    assert!(outcome.outputs.is_empty(), "Debug is a terminal sink");
    let logs = rig.logs();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].1.contains(r#"{"n":42}"#), "logged the payload: {}", logs[0].1);
    Ok(())
}

async fn absent_logs_null(rig: FakeRig) -> WeftResult<()> {
    rig.run(&DebugNode, json!({})).await.ok()?;
    let logs = rig.logs();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].1.ends_with("null"), "absent input logs null: {}", logs[0].1);
    Ok(())
}

/// A bus whose producer broke off is followed to its end like any bus, and
/// the closing line says it failed and why.
async fn failed_bus(rig: FakeRig) -> WeftResult<()> {
    let (mut bus, marker) = rig.seed_bus(BusOptions::default())?;
    bus.register("llm").expect("a fresh bus accepts its first name");
    bus.send("delta", json!("hel")).expect("an open bus accepts a send");
    bus.inner_arc().close(StreamEnd::Failed { error: "the model went away".into() });
    rig.run(&DebugNode, json!({ "data": marker })).await.ok()?;
    let logs = rig.logs();
    let last = &logs.last().expect("the closing line").1;
    assert!(last.contains("bus failed after 1 message(s)") && last.contains("the model went away"), "{last}");
    Ok(())
}
