//! Server-Sent Events endpoints. Clients (CLI `weft follow`, the VS
//! Code extension's right sidebar) subscribe to
//! a per-project stream. The dispatcher's EventBus (in-memory
//! broadcast channel per project) is the source.

use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::{Stream, StreamExt};
use tokio::sync::broadcast;

use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::LiveEvent;
use crate::state::DispatcherState;

pub async fn project_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(project_id): Path<uuid::Uuid>,
) -> Result<Sse<impl Stream<Item = Result<Event, broadcast::error::RecvError>>>, StatusCode> {
    authorize_project(&state, &caller.0, project_id)
        .await
        .map_err(|(s, _)| s)?;
    let rx = state.events.subscribe_project(project_id).await;
    let stream = live_events(rx, None);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// What the nodes of one graph are showing, pushed as it changes: the
/// editor names the nodes it draws a display for (`infra=` for infra
/// nodes, `signals=` for triggers, each a comma-separated list of places
/// as a person spells them), and gets each one's current display at
/// once, then every change. With `run=<id>`, the stream also carries what
/// that run's waiting nodes show (`DisplaySource::Wait`). Nothing is looked
/// at once the stream closes.
pub async fn display_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(project): Path<uuid::Uuid>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, (StatusCode, String)> {
    use crate::display_feeds::{DisplayKey, DisplaySource, StateReader};
    authorize_project(&state, &caller.0, project).await?;
    let reader = std::sync::Arc::new(StateReader(state.clone()));
    let mut feeds = Vec::new();
    for (param, source) in [("infra", DisplaySource::Infra), ("signals", DisplaySource::Signal)] {
        for node in query.get(param).into_iter().flat_map(|list| list.split(',')).filter(|n| !n.is_empty()) {
            // The editor shows the program's shared copies; an instance's
            // own copy is seen through that instance's token.
            let key = DisplayKey { project, source, node: node.to_string(), instance: None };
            feeds.push(state.displays.watch(key, reader.clone()));
        }
    }
    // The run the editor follows, whose waiting nodes show what their
    // waits read: only a run of this project.
    let run = match query.get("run").filter(|run| !run.is_empty()) {
        Some(run) => {
            let run: weft_core::RunId = run.parse().map_err(|_| (StatusCode::BAD_REQUEST, format!("run={run} is not a run id")))?;
            let owner = state.runs.run_owner(run).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the run's row: {e:#}")))?;
            if owner.is_none_or(|owner| owner.project_id != project) {
                return Err((StatusCode::NOT_FOUND, format!("there is no run {run} in this project")));
            }
            Some(run)
        }
        None => None,
    };
    // A stream of nothing would end at once, and a client that
    // reconnects on an ended stream would ask again in a loop.
    if feeds.is_empty() && run.is_none() {
        return Err((StatusCode::BAD_REQUEST, "name at least one node (infra= or signals=) or a run (run=)".to_string()));
    }
    let mut streams: Vec<DisplayEvents> = feeds.into_iter().map(|feed| Box::pin(display_changes(feed)) as _).collect();
    if let Some(run) = run {
        let waits = crate::display_feeds::run_waits(reader, run).map(|display| Ok(display_event(&display)));
        streams.push(Box::pin(waits));
    }
    let stream = futures::stream::select_all(streams);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// One source of display events on a display stream: a node's feed, or a
/// run's waits.
type DisplayEvents = std::pin::Pin<Box<dyn Stream<Item = Result<Event, std::convert::Infallible>> + Send>>;

/// One node's display as the SSE event the editor reads.
fn display_event(display: &crate::display_feeds::NodeDisplay) -> Event {
    Event::default().data(serde_json::to_string(display).expect("a display serializes"))
}

/// Every display one feed shows, as SSE events, for as long as the
/// stream holds the feed.
fn display_changes(
    feed: std::sync::Arc<crate::display_feeds::Feed>,
) -> impl Stream<Item = Result<Event, std::convert::Infallible>> {
    let shown = feed.shown();
    futures::stream::unfold((feed, shown, true), |(feed, mut shown, first)| async move {
        loop {
            // The first turn sends what is there already; after that,
            // only what changes.
            if !first || shown.borrow().is_none() {
                shown.changed().await.ok()?;
            }
            let current = shown.borrow_and_update().clone();
            if let Some(display) = current {
                let message = crate::display_feeds::NodeDisplay {
                    source: feed.key().source,
                    node: feed.key().node.clone(),
                    feed: display,
                };
                return Some((Ok(display_event(&message)), (feed, shown, false)));
            }
        }
    })
}

pub async fn run_stream(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(run_id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<Event, broadcast::error::RecvError>>>, StatusCode> {
    // Run SSE: we don't index events by run alone (a run
    // belongs to a project). Resolve project_id via the run's row, then
    // subscribe to the project's bus but filter
    // for this run.
    //
    // A bad run string is 400; a run store lookup failure is 500;
    // an unknown run (no run row) is 404. The pre-Result
    // shape papered over all three with empty-string project_id,
    // which silently routed events into a phantom bucket.
    let target_run_id: uuid::Uuid = run_id
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    // Resolve + tenant-gate in the ONE place that owns "who owns this
    // run": an unknown or cross-tenant run is 404 either way,
    // and the project id rides back for the stream's attribution.
    let project_id = crate::authenticator::authorize_run(&*state.runs, &caller.0, target_run_id)
        .await
        .map_err(|(s, _)| s)?
        .project_id;

    let rx = state.events.subscribe_project(project_id).await;
    let stream = live_events(rx, Some(target_run_id));
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

fn live_events(
    rx: broadcast::Receiver<LiveEvent>,
    run_id: Option<uuid::Uuid>,
) -> impl Stream<Item = Result<Event, broadcast::error::RecvError>> {
    futures::stream::unfold(Some(rx), move |rx| async move {
        let mut rx = rx?;
        loop {
            match rx.recv().await {
                Ok(event) if run_id.is_none() || event.event.run_id() == run_id => {
                    return Some((Ok(to_sse(event)), Some(rx)));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(err) => {
                    // Dropped updates invalidate the live view. End this
                    // subscription with an error so clients show the loss
                    // instead of continuing with an incomplete history.
                    tracing::warn!(%err, ?run_id, "ending event stream after lost updates");
                    return Some((Err(err), None));
                }
            }
        }
    })
}

fn to_sse(event: LiveEvent) -> Event {
    // `DispatcherEvent` is a plain enum over `String` / typed enums /
    // `Value`. Serialization is infallible; the previous
    // `unwrap_or_else(|_| "{}".into())` was dead defensive code that
    // would silently ship a `{}`-shaped event the extension would
    // never parse. Assert the invariant so a future variant that
    // breaks it fails loud.
    let payload = serde_json::to_string(&event).expect("DispatcherEvent serializes");
    Event::default().data(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use crate::events::{DispatcherEvent, IdentifiedEvent};

    fn completed(run_id: uuid::Uuid) -> LiveEvent {
        IdentifiedEvent::transient(DispatcherEvent::RunCompleted {
            run_id, project_id: uuid::Uuid::from_u128(0x100), outputs: serde_json::json!({}), at_unix: 1,
        })
    }

    /// A display stream sends what the node shows as soon as it is
    /// known, then only what changes, named by the node the editor asked
    /// for.
    #[tokio::test(start_paused = true)]
    async fn a_display_stream_sends_the_current_display_then_changes() {
        use crate::display_feeds::{DisplayAt, DisplayFeeds, DisplayKey, DisplayReader, DisplaySource, NodeFeed, LOOK_EVERY};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Flip(AtomicUsize);
        #[async_trait::async_trait]
        impl DisplayReader for Flip {
            async fn find(&self, _key: &DisplayKey) -> Result<DisplayAt, String> {
                Ok(DisplayAt::Nowhere)
            }

            async fn read(&self, _key: &DisplayKey, _at: &DisplayAt) -> NodeFeed {
                if self.0.fetch_add(1, Ordering::SeqCst) < 3 { NodeFeed::Absent } else { NodeFeed::Ok { items: vec![] } }
            }
        }
        let feeds = DisplayFeeds::default();
        let key = DisplayKey { project: uuid::Uuid::nil(), source: DisplaySource::Signal, node: "one.door".into(), instance: None };
        let feed = feeds.watch(key, std::sync::Arc::new(Flip(AtomicUsize::new(0))));
        let stream = display_changes(feed);
        futures::pin_mut!(stream);
        let started = tokio::time::Instant::now();
        let first = stream.next().await.unwrap().unwrap();
        assert!(format!("{first:?}").contains(r#"\"state\":\"absent\""#), "{first:?}");
        let second = stream.next().await.unwrap().unwrap();
        assert!(format!("{second:?}").contains(r#"\"state\":\"ok\""#), "{second:?}");
        assert!(format!("{second:?}").contains(r#"\"node\":\"one.door\""#), "{second:?}");
        assert_eq!(started.elapsed(), LOOK_EVERY * 3, "the unchanged looks in between sent nothing");
    }

    weft_core::stress_test!(
        name: lagged_followers_fail_instead_of_silently_skipping_updates,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            for filter in [None, Some(uuid::Uuid::new_v4())] {
                let (tx, rx) = broadcast::channel(1);
                let run_id = uuid::Uuid::new_v4();
                tx.send(completed(run_id)).unwrap();
                tx.send(completed(run_id)).unwrap();
                let stream = live_events(rx, filter);
                futures::pin_mut!(stream);
                assert!(matches!(stream.next().await, Some(Err(broadcast::error::RecvError::Lagged(1)))));
                assert!(stream.next().await.is_none());
            }
        }
    );

    weft_core::stress_test!(
        name: run_stream_filters_other_run_ids_without_dropping_its_own,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let (tx, rx) = broadcast::channel(4);
            let run_id = uuid::Uuid::new_v4();
            tx.send(completed(uuid::Uuid::new_v4())).unwrap();
            tx.send(completed(run_id)).unwrap();
            drop(tx);
            let events = live_events(rx, Some(run_id)).collect::<Vec<_>>().await;
            assert_eq!(events.len(), 1);
            assert!(events[0].is_ok());
        }
    );
}
