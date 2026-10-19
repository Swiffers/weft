//! GoogleCalendarListEvents: the events in a time window, expanded
//! (recurring events appear as their instances) and ordered by start.

use async_trait::async_trait;
use serde_json::{json, Value};

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GoogleCalendarListEventsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleCalendarListEventsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let calendar: String = ctx.inputs.get("calendar")?;
        let time_min: String = ctx.inputs.get("timeMin")?;
        let time_max: String = ctx.inputs.get("timeMax")?;
        let query: Option<String> = ctx.inputs.opt("query")?;
        let max_results = super::api::page_size(&ctx.inputs, "maxResults")?;

        let mut base = format!(
            "https://www.googleapis.com/calendar/v3/calendars/{}/events?singleEvents=true&orderBy=startTime&maxResults={max_results}&timeMin={}&timeMax={}",
            super::api::segment(&calendar),
            urlencoding::encode(&time_min),
            urlencoding::encode(&time_max),
        );
        if let Some(q) = query.filter(|q| !q.trim().is_empty()) {
            base.push_str(&format!("&q={}", urlencoding::encode(&q)));
        }
        let page_token: String = ctx.inputs.get_or("pageToken", String::new())?;
        let http = ctx.client(&account).await?;

        // One Google page per call: a window can hold more events than
        // any wire, and a workflow that needs all of them pages with
        // `nextPageToken` until `hasMore` is false.
        let page = super::api::list_page(
            &http,
            &base,
            "items",
            &page_token,
            event,
            "google calendar: list events",
            "maxResults",
        )
        .await?;
        let count = page.items.len() as f64;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("events", Value::Array(page.items))
                .set("count", count)
                .set("hasMore", page.next_page_token.is_some())
                .set("nextPageToken", page.next_page_token.unwrap_or_default()),
        )
        .await
    }
}

/// One event as the node emits it, its start and end flattened to the
/// time (or the date, for an all-day event).
fn event(e: &Value) -> Value {
    json!({
        "id": e["id"],
        "summary": e["summary"],
        "start": e["start"]["dateTime"].as_str().or(e["start"]["date"].as_str()),
        "end": e["end"]["dateTime"].as_str().or(e["end"]["date"].as_str()),
        "location": e["location"],
        "description": e["description"],
        "link": e["htmlLink"],
    })
}
