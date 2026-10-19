//! WebSearch self-tests. `fake` pins the request the node builds and
//! the result shaping against a canned Exa answer; `live` runs one
//! real single-result search through the production access path (the
//! cheap route: one small search).

use serde_json::json;

use weft::storage::{wire_weight, MAX_WIRE_VALUE_BYTES};
use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::super::wire::fit_texts;
use super::WebSearchNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::basic("fitting_texts_keeps_the_short_ones_whole", fitting_texts),
        NodeTest::fake("builds_the_search_and_shapes_the_results", canned_search),
        NodeTest::fake("settings_past_a_wire_refuse_before_searching", settings_past_the_wire),
        NodeTest::fake("texts_heavier_than_their_characters_are_cut_to_fit", heavy_texts),
        NodeTest::live("one_real_single_result_search", "exa", live_search),
    ]
}

async fn canned_search(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/search",
        json!({ "results": [
            { "title": "Weft", "url": "https://weft.dev", "publishedDate": "2026-01-01",
              "text": "a workflow language", "extra": "dropped" },
        ]}),
    );
    let outcome = rig
        .run(
            &WebSearchNode,
            json!({
                "account": rig.access("exa"),
                "query": "weft workflow language",
                "numResults": 3,
                "includeText": true,
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["count"], json!(1.0));
    assert_eq!(
        outcome.outputs["results"][0],
        json!({ "title": "Weft", "url": "https://weft.dev",
                "publishedDate": "2026-01-01", "text": "a workflow language" }),
        "results are shaped to the declared fields only"
    );

    let sent = &rig.requests()[0];
    assert_eq!(
        sent.body.as_ref().expect("json payload"),
        &json!({ "query": "weft workflow language", "type": "auto",
                 "numResults": 3, "contents": { "text": { "maxCharacters": 4000 } } }),
        "the manifest's text cap rides every text request, so no page can blow the wire"
    );
    Ok(())
}

async fn settings_past_the_wire(rig: FakeRig) -> WeftResult<()> {
    let err = rig
        .run(
            &WebSearchNode,
            json!({ "account": rig.access("exa"), "query": "q", "numResults": 100, "maxTextChars": 4000 }),
        )
        .await
        .failure()?;
    assert!(err.starts_with("input error") && err.contains("`numResults`") && err.contains("`maxTextChars`"), "{err}");
    assert!(rig.requests().is_empty(), "nothing is paid for");
    Ok(())
}

/// Ten texts of 4000 characters three bytes each weigh about 120 KB:
/// within `maxTextChars`, past the wire, so the node shortens them.
async fn heavy_texts(rig: FakeRig) -> WeftResult<()> {
    let results: Vec<_> = (0..10)
        .map(|i| json!({ "title": format!("r{i}"), "url": format!("https://ex.com/{i}"), "text": "日".repeat(4000) }))
        .collect();
    rig.respond("POST", "/search", json!({ "results": results }));
    let outcome = rig
        .run(&WebSearchNode, json!({ "account": rig.access("exa"), "query": "q" }))
        .await
        .ok()?;
    let results = outcome.output("results")?;
    assert!(wire_weight(results) <= MAX_WIRE_VALUE_BYTES, "the results fit a wire");
    assert_eq!(outcome.outputs["count"], json!(10.0), "every result is kept");
    for r in results.as_array().expect("a list") {
        let chars = r["text"].as_str().expect("text").chars().count();
        assert!(chars > 3000 && chars < 4000, "each text is shortened evenly, not dropped: {chars}");
    }
    Ok(())
}

async fn live_search(rig: LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &WebSearchNode,
            json!({
                "account": rig.access("exa"),
                "query": "rust programming language",
                "numResults": 1,
                "includeText": false,
            }),
        )
        .await
        .ok()?;
    let count = outcome.output("count")?.as_f64().unwrap_or(0.0);
    assert!(count >= 1.0, "a real search answers at least one result");
    let first = &outcome.output("results")?[0];
    assert!(
        first["url"].as_str().is_some_and(|u| u.starts_with("http")),
        "a result carries a real URL: {first}"
    );
    Ok(())
}

fn fitting_texts() -> WeftResult<()> {
    let mut items = vec![
        json!({ "url": "a", "text": "short" }),
        json!({ "url": "b", "text": "x".repeat(MAX_WIRE_VALUE_BYTES) }),
        json!({ "url": "c", "text": "y".repeat(MAX_WIRE_VALUE_BYTES) }),
    ];
    let cut = fit_texts(&mut items, "text").expect("fits once cut");
    assert_eq!(cut, vec![false, true, true]);
    assert_eq!(items[0]["text"], json!("short"), "the short text stays whole");
    let total = wire_weight(&items);
    assert!(total <= MAX_WIRE_VALUE_BYTES && total + 2 >= MAX_WIRE_VALUE_BYTES, "the room is used: {total}");
    let b = items[1]["text"].as_str().expect("text").len();
    let c = items[2]["text"].as_str().expect("text").len();
    assert!(b.abs_diff(c) <= 1, "the long ones share evenly: {b} and {c}");

    let mut small = vec![json!({ "text": "a" })];
    assert_eq!(fit_texts(&mut small, "text"), Ok(vec![false]), "a list that fits is left alone");
    Ok(())
}
