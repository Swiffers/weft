//! Layer-3 contract tests for what a registration again carries over of
//! the row it replaces, the rule every kind shares
//! (`weft_listener::kinds::prepare_signal`): a poll's cursor, the kind
//! whose place in its source matters most. A folder watch's snapshot is in
//! `folder_watch.rs`; whether a trigger listened while it was down is
//! derived on the dispatcher's side (`listened_while_down`).

use std::sync::Arc;

use serde_json::{json, Value};

use weft_core::signal::listener_protocol::PriorRow;
use weft_core::signal::{to_spec, PollEndpoint, Timer, TimerSpec};
use weft_listener::kinds::{prepare_signal, SignalIdentity};
use weft_listener::{Holders, ListenerState};

use crate::folder_watch::{listener_state, spawn_broker, FakeDoors};

async fn state() -> ListenerState {
    listener_state(spawn_broker().await, Arc::new(FakeDoors::default()), Holders::OnThisMachine)
}

/// A delta poll of `url`, every `interval_secs`.
fn feed(url: &str, interval_secs: u64) -> PollEndpoint {
    serde_json::from_value(json!({ "url": url, "interval_secs": interval_secs, "delta": { "items": "items", "cursor_field": "id" } })).unwrap()
}

/// A row that polled a while: a cursor, and a streak of failures.
fn polled() -> Value {
    json!({ "delta": { "cursor": 7 }, "consecutive_failures": 4, "last_failure": "the endpoint answered 502" })
}

fn entry(spec: weft_core::primitive::SignalSpec) -> SignalIdentity {
    SignalIdentity { token: "t".into(), tenant_id: "tenant-a".into(), node_id: "feed".into(), is_resume: false, run_id: None, spec }
}

async fn prepared(state: &ListenerState, identity: SignalIdentity, prior: PriorRow) -> Value {
    prepare_signal(state, identity, None, Some(&prior), 1_000).await.unwrap().kind_state
}

/// A poll that listened the whole time and reads the same feed keeps its
/// cursor, so what the feed queued meanwhile is delivered; the failure
/// streak was the last registration's and starts over. A new cadence is
/// the same feed.
#[tokio::test]
async fn a_poll_that_listened_to_the_same_feed_keeps_its_cursor_and_nothing_else() {
    let state = state().await;
    let before = to_spec(feed("https://feed.test/items", 60));
    let prior = PriorRow { spec: before.clone(), kind_state: polled(), listened: true };
    assert_eq!(prepared(&state, entry(before), prior.clone()).await, json!({ "delta": { "cursor": 7 } }));
    let slower = to_spec(feed("https://feed.test/items", 600));
    assert_eq!(prepared(&state, entry(slower), prior).await, json!({ "delta": { "cursor": 7 } }), "the cadence is not the feed");
}

/// Behind a door that refused, nothing arrived as far as the trigger is
/// concerned: its poll starts unprimed, and its first poll primes it
/// silently, so nothing refused is delivered.
#[tokio::test]
async fn a_poll_behind_a_door_that_refused_starts_afresh() {
    let state = state().await;
    let spec = to_spec(feed("https://feed.test/items", 60));
    let prior = PriorRow { spec: spec.clone(), kind_state: polled(), listened: false };
    assert_eq!(prepared(&state, entry(spec), prior).await, json!({}));
}

/// A poll pointed at another feed starts there afresh: a cursor of one
/// feed would hide the other's items, or fire all of them.
#[tokio::test]
async fn a_poll_pointed_at_another_feed_starts_afresh() {
    let state = state().await;
    let before = to_spec(feed("https://feed.test/items", 60));
    let prior = PriorRow { spec: before.clone(), kind_state: polled(), listened: true };
    let other_url = to_spec(feed("https://other.test/items", 60));
    assert_eq!(prepared(&state, entry(other_url), prior.clone()).await, json!({}), "another url");
    let other_cursor = to_spec(
        serde_json::from_value::<PollEndpoint>(json!({ "url": "https://feed.test/items", "interval_secs": 60, "delta": { "items": "items", "cursor_field": "seq" } })).unwrap(),
    );
    assert_eq!(prepared(&state, entry(other_cursor), prior.clone()).await, json!({}), "another cursor field");
    let mut signed = before;
    signed.access = Some(weft_core::primitive::AccessRef { id: "conn-b".into(), service: "svc".into(), required_values: Vec::new() });
    assert_eq!(prepared(&state, entry(signed), prior.clone()).await, json!({}), "signed by another connection");
    let timer = PriorRow { spec: to_spec(Timer { spec: TimerSpec::After { duration_ms: 60_000 } }), ..prior };
    assert_eq!(prepared(&state, entry(to_spec(feed("https://feed.test/items", 60))), timer).await, json!({}), "a row of another kind");
}

/// A run's wait never carries anything over: a retry of its registration
/// starts from the moment it was asked for, as the first did.
#[tokio::test]
async fn a_wait_never_carries_its_row_over() {
    let state = state().await;
    let spec = to_spec(PollEndpoint { url: "https://jobs.test/1".into(), ..Default::default() });
    let prior = PriorRow { spec: spec.clone(), kind_state: json!({ "consecutive_failures": 2, "last_failure": "x" }), listened: true };
    let wait = SignalIdentity { is_resume: true, run_id: Some(uuid::Uuid::nil()), ..entry(spec) };
    assert_eq!(prepared(&state, wait, prior).await, json!({ "first_poll_at_ms": 1_000 }));
}

/// A kind with no place in a source starts fresh whatever the row held:
/// a timer is a new schedule from each activation.
#[tokio::test]
async fn a_kind_with_no_source_starts_fresh() {
    let state = state().await;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 60_000 } });
    let prior = PriorRow { spec: spec.clone(), kind_state: json!({ "next_fire_at_unix_ms": 5 }), listened: true };
    assert_eq!(prepared(&state, entry(spec), prior).await, json!({ "next_fire_at_unix_ms": 61_000 }));
}
