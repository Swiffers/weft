//! GoogleCalendarListEvents self-tests: one Google page per call, the
//! next read with Google's token, and the flattened event shape.

use serde_json::json;

use weft::{FakeRig, LiveRig, NodeTest, WeftResult};

use super::GoogleCalendarListEventsNode;

pub fn tests() -> Vec<NodeTest> {
    vec![
        NodeTest::fake("pages_and_flattens_the_events", pages_and_flattens),
        NodeTest::fake("a_page_heavier_than_a_wire_names_max_results", heavy_window),
        NodeTest::live("one_real_window_listing", "google", live_list),
    ]
}

async fn pages_and_flattens(rig: FakeRig) -> WeftResult<()> {
    let base = "/calendar/v3/calendars/primary/events?singleEvents=true&orderBy=startTime\
                &maxResults=25&timeMin=2026-08-01T00%3A00%3A00Z&timeMax=2026-08-31T00%3A00%3A00Z";
    rig.respond(
        "GET",
        base,
        json!({ "items": [{
            "id": "e1", "summary": "First",
            "start": { "dateTime": "2026-08-10T09:00:00Z" },
            "end": { "dateTime": "2026-08-10T10:00:00Z" },
            "htmlLink": "l1",
        }], "nextPageToken": "t2" }),
    );
    rig.respond(
        "GET",
        &format!("{base}&pageToken=t2"),
        json!({ "items": [{
            "id": "e2", "summary": "Second",
            "start": { "date": "2026-08-12" },
            "end": { "date": "2026-08-13" },
            "htmlLink": "l2",
        }] }),
    );
    let list = |token: &str| {
        rig.run(
            &GoogleCalendarListEventsNode,
            json!({
                "account": rig.access("google"),
                "timeMin": "2026-08-01T00:00:00Z",
                "timeMax": "2026-08-31T00:00:00Z",
                "pageToken": token,
            }),
        )
    };
    let first = list("").await.ok()?;
    assert_eq!(first.outputs["count"], json!(1.0), "one Google page per call");
    assert_eq!(first.outputs["events"][0]["start"], json!("2026-08-10T09:00:00Z"), "timed start flattened");
    assert_eq!(first.outputs["nextPageToken"], json!("t2"));
    assert_eq!(first.outputs["hasMore"], json!(true));

    let second = list("t2").await.ok()?;
    assert_eq!(second.outputs["events"][0]["start"], json!("2026-08-12"), "all-day start flattened");
    assert_eq!(second.outputs["hasMore"], json!(false));
    Ok(())
}

async fn heavy_window(rig: FakeRig) -> WeftResult<()> {
    let base = "/calendar/v3/calendars/primary/events?singleEvents=true&orderBy=startTime\
                &maxResults=25&timeMin=2026-08-01T00%3A00%3A00Z&timeMax=2026-08-31T00%3A00%3A00Z";
    let items: Vec<_> = (0..3)
        .map(|i| json!({ "id": format!("e{i}"), "description": "d".repeat(40 * 1024), "start": {}, "end": {} }))
        .collect();
    rig.respond("GET", base, json!({ "items": items }));
    let err = rig
        .run(
            &GoogleCalendarListEventsNode,
            json!({
                "account": rig.access("google"),
                "timeMin": "2026-08-01T00:00:00Z",
                "timeMax": "2026-08-31T00:00:00Z",
            }),
        )
        .await
        .failure()?;
    assert!(err.contains("at most 100 KB") && err.contains("lower `maxResults`"), "{err}");
    Ok(())
}

async fn live_list(rig: LiveRig) -> WeftResult<()> {
    // Read-only: whatever the window holds, count and events agree.
    let outcome = rig
        .run(
            &GoogleCalendarListEventsNode,
            json!({
                "account": rig.access("google"),
                "timeMin": "2000-01-01T00:00:00Z",
                "timeMax": "2000-01-08T00:00:00Z",
            }),
        )
        .await
        .ok()?;
    let events = outcome.output("events")?.as_array().expect("events list").len();
    let count = outcome.output("count")?.as_f64().expect("count");
    assert_eq!(events as f64, count, "count matches the listed events");
    Ok(())
}
