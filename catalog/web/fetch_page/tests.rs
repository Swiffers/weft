//! FetchPage self-tests: the scrape request's exact payload and the
//! markdown/title extraction, plus the loud refusal path.

use serde_json::{json, Value};

use weft::{FakeRig, NodeTest, WeftResult};

use weft::storage::{wire_weight, MAX_WIRE_VALUE_BYTES};

use super::FetchPageNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("scrapes_a_page_into_markdown", scrapes_page),
        NodeTest::fake("markdown_past_max_chars_is_cut_and_says_so", cut_at_max_chars),
        NodeTest::fake("markdown_too_heavy_for_a_wire_is_cut_to_fit", cut_to_the_wire),
        NodeTest::fake("a_refused_scrape_fails_loud", refused_scrape),
        NodeTest::fake("a_success_without_markdown_fails_loud", missing_markdown),
        NodeTest::live("one_real_scrape", "firecrawl", live_scrape),
    ]
}

async fn live_scrape(rig: weft::LiveRig) -> WeftResult<()> {
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({ "account": rig.access("firecrawl"), "url": "https://example.com" }),
        )
        .await
        .ok()?;
    let markdown = outcome.output("markdown")?.as_str().expect("markdown text");
    assert!(
        markdown.to_lowercase().contains("example"),
        "example.com scraped into markdown, got: {markdown}"
    );
    Ok(())
}

async fn scrapes_page(rig: FakeRig) -> WeftResult<()> {
    rig.respond(
        "POST",
        "/v2/scrape",
        json!({
            "success": true,
            "data": {
                "markdown": "# Hello\n\nBody.",
                "metadata": { "title": "Hello Page" }
            }
        }),
    );
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://example.com/post",
            }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["markdown"], json!("# Hello\n\nBody."));
    assert_eq!(outcome.outputs["title"], json!("Hello Page"));
    assert_eq!(outcome.outputs["truncated"], json!(false), "a short page arrives whole");

    let sent = rig.requests();
    assert_eq!(
        sent[0].body.as_ref().expect("scrape payload"),
        &json!({
            "url": "https://example.com/post",
            "formats": ["markdown"],
            "onlyMainContent": true,
        }),
        "exactly the scrape shape the meter prices (no credit-multiplying options)"
    );
    Ok(())
}

async fn missing_markdown(rig: FakeRig) -> WeftResult<()> {
    // The markdown is the node's whole product: a "success" answer
    // without it must refuse loudly, never emit an empty page.
    rig.respond(
        "POST",
        "/v2/scrape",
        json!({ "success": true, "data": { "metadata": { "title": "No Body" } } }),
    );
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://example.com",
            }),
        )
        .await;
    let err = outcome.result.expect_err("missing markdown is loud").to_string();
    assert!(err.contains("no markdown"), "{err}");
    Ok(())
}

async fn refused_scrape(rig: FakeRig) -> WeftResult<()> {
    rig.respond_status(
        "POST",
        "/v2/scrape",
        402,
        json!({ "success": false, "error": "insufficient credits" }),
    );
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({
                "account": rig.access("firecrawl"),
                "url": "https://example.com",
            }),
        )
        .await;
    let err = outcome.result.expect_err("a refusal is loud").to_string();
    assert!(err.contains("insufficient credits"), "{err}");
    Ok(())
}

fn scrape_answer(markdown: &str) -> Value {
    json!({ "success": true, "data": { "markdown": markdown, "metadata": { "title": "T" } } })
}

async fn cut_at_max_chars(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/scrape", scrape_answer("# Héllo world"));
    let outcome = rig
        .run(
            &FetchPageNode,
            json!({ "account": rig.access("firecrawl"), "url": "https://example.com", "maxChars": 5 }),
        )
        .await
        .ok()?;
    assert_eq!(outcome.outputs["markdown"], json!("# Hél"), "five characters, not five bytes");
    assert_eq!(outcome.outputs["truncated"], json!(true));
    Ok(())
}

/// Fifty thousand characters of a script three bytes each weigh about
/// 150 KB: the default `maxChars` keeps them all, so the wire is what
/// cuts, and the markdown arrives as heavy as a wire carries.
async fn cut_to_the_wire(rig: FakeRig) -> WeftResult<()> {
    rig.respond("POST", "/v2/scrape", scrape_answer(&"日".repeat(50_000)));
    let outcome = rig
        .run(&FetchPageNode, json!({ "account": rig.access("firecrawl"), "url": "https://example.com" }))
        .await
        .ok()?;
    let markdown = outcome.output("markdown")?.as_str().expect("markdown text");
    let weighs = wire_weight(markdown);
    assert!(weighs <= MAX_WIRE_VALUE_BYTES && weighs > MAX_WIRE_VALUE_BYTES - 3, "cut to fit, no shorter: {weighs}");
    assert!(markdown.chars().all(|c| c == '日'), "cut between characters");
    assert_eq!(outcome.outputs["truncated"], json!(true));
    Ok(())
}
