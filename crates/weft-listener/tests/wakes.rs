//! Layer-3 contract tests for the wake path: the real listener code
//! (registration, arming, `kinds::wake`, the timer's claim) against
//! hand-rolled fakes of its I/O: a fake broker holding signal rows in
//! memory (the held read and the claimed state write, the same
//! rule the SQL applies), a fake alarm recording the wakes set, and a
//! fake task store recording the fires enqueued.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use weft_core::signal::{to_spec, Timer, TimerSpec};
use weft_listener::kinds::{prepare_signal, wake, SignalIdentity, WakeBody};
use weft_listener::{ListenerConfig, ListenerState};
use weft_platform_traits::FakeAlarm;

// ---------- Fakes ----------

/// The task store: records every answer to a waiting run the listener
/// enqueued (an entry's event never becomes a task).
#[derive(Default)]
struct FakeTasks {
    answers: Mutex<Vec<weft_task_store::tasks::NewTask>>,
}

#[async_trait::async_trait]
impl weft_task_store::TaskStoreClient for FakeTasks {
    async fn enqueue_dedup(&self, spec: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
        self.answers.lock().unwrap().push(spec);
        Ok(weft_task_store::tasks::DedupOutcome::Inserted(uuid::Uuid::new_v4()))
    }
    async fn wait_for_terminal(&self, _: uuid::Uuid, _: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
        unreachable!()
    }
}

/// Signal rows by token: `(row as the broker hands it back)`. The write
/// applies the same rule as `held_signals::write_kind_state`.
type Rows = Arc<Mutex<HashMap<String, Value>>>;

/// A registration's kind state, written (moving the row one version on)
/// just before the next claim is judged: a registration landing between
/// a wake's read and its claim.
type Landing = Arc<Mutex<Option<Value>>>;

/// How many of the next `get_held` reads answer 500 (the broker
/// hiccuping), how many `get_held` reads were made, and how many of the
/// next `write_kind_state` writes answer 500, and the status every
/// connection resolve answers with (no connection is ever found by it).
#[derive(Default)]
struct GetHeld {
    failing: std::sync::atomic::AtomicUsize,
    reads: std::sync::atomic::AtomicUsize,
    failing_writes: std::sync::atomic::AtomicUsize,
    resolve_status: std::sync::atomic::AtomicU16,
}

/// The events put in their trigger's queue (`/v1/door/park_fire`), and,
/// while `down`, the queue cannot be reached either.
#[derive(Default)]
struct Queue {
    parked: Mutex<Vec<Value>>,
    down: std::sync::atomic::AtomicBool,
}

async fn spawn_broker(rows: Rows, landing: Landing, get_held: Arc<GetHeld>, queue: Arc<Queue>, asked: Arc<std::sync::atomic::AtomicUsize>) -> String {
    use axum::routing::post;
    use std::sync::atomic::Ordering;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let get_rows = rows.clone();
    let writes = get_held.clone();
    let give_up_rows = rows.clone();
    let list_rows = rows.clone();
    let hold_rows = rows.clone();
    let let_go_rows = rows.clone();
    let target_rows = rows.clone();
    let resolving = get_held.clone();
    let app = axum::Router::new()
        // A connection resolve, refused with the status the test set.
        .route(
            "/v1/access/listener-resolve",
            post(move |axum::Json(_): axum::Json<Value>| {
                let resolving = resolving.clone();
                async move {
                    let status = axum::http::StatusCode::from_u16(resolving.resolve_status.load(Ordering::SeqCst)).unwrap();
                    (status, "the connection cannot be resolved")
                }
            }),
        )
        // An entry's event goes straight to its project's worker
        // (`FakeDoors`); an answer to a waiting run is a task.
        .route(
            "/v1/signal/fire_target",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = target_rows.clone();
                async move {
                    let is_resume = rows.lock().unwrap().get(req["token"].as_str().unwrap()).is_some_and(|row| row["is_resume"] == true);
                    axum::Json(json!({ "project_id": uuid::Uuid::nil(), "is_resume": is_resume, "address": "http://worker.test" }))
                }
            }),
        )
        // An event whose worker could not be reached waits in its
        // trigger's queue.
        .route(
            "/v1/door/park_fire",
            post(move |axum::Json(req): axum::Json<Value>| {
                let queue = queue.clone();
                async move {
                    if queue.down.load(Ordering::SeqCst) {
                        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
                    }
                    queue.parked.lock().unwrap().push(req);
                    Ok(axum::Json(json!("parked")))
                }
            }),
        )
        // A holder giving up its claims, as `held_signals::let_go` does.
        .route(
            "/v1/signal/let_go",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = let_go_rows.clone();
                async move {
                    let mut rows = rows.lock().unwrap();
                    for row in rows.values_mut() {
                        if row["held_by"] == req["replica"] {
                            row["held_by"] = Value::Null;
                        }
                    }
                    axum::Json(json!({}))
                }
            }),
        )
        // A holder's look, by the same rule as `held_signals::hold`: it
        // keeps what it still claims, names what is gone as ended, and
        // takes the held rows nobody claims or that it claims and did not
        // keep (a row rewritten under it comes back in the same look),
        // never one given up on, the wanted ones first.
        .route(
            "/v1/signal/hold",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = hold_rows.clone();
                async move {
                    let mut rows = rows.lock().unwrap();
                    let replica = req["replica"].clone();
                    let holding: Vec<&str> = req["holding"].as_array().unwrap().iter().map(|h| h["token"].as_str().unwrap()).collect();
                    let kept: Vec<&str> = holding
                        .iter()
                        .copied()
                        .filter(|t| rows.get(*t).is_some_and(|r| r["holds"] == json!(true) && r["held_by"] == replica))
                        .collect();
                    let ended: Vec<&str> = holding.iter().copied().filter(|t| !rows.contains_key(*t)).collect();
                    let want: Vec<&str> = req["want"].as_array().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
                    let mut free: Vec<String> = rows
                        .iter()
                        .filter(|(t, r)| {
                            r["holds"] == json!(true)
                                && r["held_failed"].is_null()
                                && (r["held_by"].is_null() || r["held_by"] == replica)
                                && !kept.contains(&t.as_str())
                        })
                        .map(|(t, _)| t.clone())
                        .collect();
                    free.sort_by_key(|t| !want.contains(&t.as_str()));
                    let mut taken = Vec::new();
                    for token in free {
                        let row = rows.get_mut(&token).unwrap();
                        row["held_by"] = replica.clone();
                        let mut wire = row.clone();
                        wire.as_object_mut().unwrap().remove("held_by");
                        taken.push(wire);
                    }
                    axum::Json(json!({ "kept": kept, "ended": ended, "taken": taken }))
                }
            }),
        )
        .route(
            "/v1/signal/list_held",
            post(move |axum::Json(_): axum::Json<Value>| {
                let rows = list_rows.clone();
                async move {
                    let rows: Vec<Value> = rows
                        .lock()
                        .unwrap()
                        .values()
                        .cloned()
                        .map(|mut row| {
                            row.as_object_mut().unwrap().remove("held_by");
                            row
                        })
                        .collect();
                    axum::Json(json!({ "rows": rows }))
                }
            }),
        )
        // An address on a `wi-` host is an infra endpoint, reached on the
        // same port of the machine; every other is reached as it is. Each
        // ask is counted.
        .route(
            "/v1/infra/listener-address",
            post(move |axum::Json(req): axum::Json<Value>| {
                let asked = asked.clone();
                async move {
                    asked.fetch_add(1, Ordering::SeqCst);
                    let authority = req["authority"].as_str().unwrap();
                    axum::Json(match authority.strip_prefix("wi-").and_then(|a| a.split_once(':')) {
                        Some((_, port)) => json!({ "authority": format!("127.0.0.1:{port}"), "is_infra": true }),
                        None => json!({ "authority": authority, "is_infra": false }),
                    })
                }
            }),
        )
        // A signal given up on, as `held_signals::give_up` writes it: only
        // the registration it names, and its holder lets go.
        .route(
            "/v1/signal/give_up",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = give_up_rows.clone();
                async move {
                    if let Some(row) = rows.lock().unwrap().get_mut(req["token"].as_str().unwrap()) {
                        if row["generation"] == req["generation"] && row["held_failed"].is_null() {
                            row["held_failed"] = req["reason"].clone();
                            row["held_by"] = Value::Null;
                        }
                    }
                    axum::Json(json!({}))
                }
            }),
        )
        .route(
            "/v1/signal/get_held",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = get_rows.clone();
                let get_held = get_held.clone();
                async move {
                    get_held.reads.fetch_add(1, Ordering::SeqCst);
                    let failing = get_held.failing.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
                    if failing.is_ok() {
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                    let mut row = rows.lock().unwrap().get(req["token"].as_str().unwrap()).cloned();
                    if let Some(row) = row.as_mut() {
                        row.as_object_mut().unwrap().remove("held_by");
                    }
                    Ok(axum::Json(json!({ "row": row })))
                }
            }),
        )
        .route(
            "/v1/signal/write_kind_state",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = rows.clone();
                let landing = landing.clone();
                let writes = writes.clone();
                async move {
                    if writes.failing_writes.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                    let mut rows = rows.lock().unwrap();
                    let Some(row) = rows.get_mut(req["token"].as_str().unwrap()) else {
                        return Ok(axum::Json(json!({ "written": false })));
                    };
                    if let Some(registered) = landing.lock().unwrap().take() {
                        row["kind_state"] = registered;
                        row["kind_state_seq"] = json!(row["kind_state_seq"].as_i64().unwrap() + 1);
                    }
                    let at = row["kind_state_seq"].as_i64().unwrap();
                    let written = req["from_seq"].as_i64().unwrap() == at;
                    if written {
                        row["kind_state"] = req["kind_state"].clone();
                        row["kind_state_seq"] = json!(at + 1);
                    }
                    Ok(axum::Json(json!({ "written": written })))
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, weft_broker_client::line::server::with_line(app)).await.unwrap() });
    format!("http://{addr}")
}

struct Rig {
    state: ListenerState,
    tasks: Arc<FakeTasks>,
    doors: Arc<FakeDoors>,
    queue: Arc<Queue>,
    alarm: Arc<FakeAlarm>,
    rows: Rows,
    landing: Landing,
    get_held: Arc<GetHeld>,
    /// How often the listener asked where an address is.
    asked: Arc<std::sync::atomic::AtomicUsize>,
    /// The fake broker's address.
    broker: String,
}

/// Holders as on a local install: one process, never checked, told
/// nothing.
fn holders() -> weft_listener::HolderPlatform {
    weft_listener::HolderPlatform {
        news: Arc::new(weft_platform_traits::FakeHolderNews::default()),
        liveness: Arc::new(weft_platform_traits::OneProcessHolds),
    }
}

/// The listener's way to the fake broker at `broker`.
fn link(broker: &str) -> weft_broker_client::BrokerLink {
    weft_broker_client::BrokerLink::new(
        broker.to_string(),
        weft_broker_client::TokenSource::role(
            Arc::new(weft_platform_traits::FixedToken("test-token".into())),
            "test-listener",
            weft_platform_traits::CoreRole::Listener,
        ),
    )
}

async fn rig(holds_here: bool) -> Rig {
    let rows: Rows = Arc::default();
    let landing: Landing = Arc::default();
    let get_held: Arc<GetHeld> = Arc::default();
    let queue = Arc::new(Queue::default());
    let asked: Arc<std::sync::atomic::AtomicUsize> = Arc::default();
    let broker = spawn_broker(rows.clone(), landing.clone(), get_held.clone(), queue.clone(), asked.clone()).await;
    let tasks = Arc::new(FakeTasks::default());
    let doors = Arc::new(FakeDoors::default());
    let alarm = Arc::new(FakeAlarm::new());
    let state = ListenerState::new(
        ListenerConfig { replica: "test-listener".into(), holds_here, prefer_push: false },
        tasks.clone(),
        link(&broker),
        alarm.clone(),
        doors.clone(),
        holders(),
    );
    Rig { state, tasks, doors, queue, alarm, rows, landing, get_held, asked, broker }
}

fn identity(token: &str, spec: weft_core::primitive::SignalSpec) -> SignalIdentity {
    SignalIdentity {
        token: token.into(),
        tenant_id: "tenant-a".into(),
        node_id: "tick".into(),
        is_resume: false,
        execution_id: None,
        spec,
    }
}

/// The row the dispatcher would write for a registration.
fn row(token: &str, spec: &weft_core::primitive::SignalSpec, kind_state: Value, seq: i64) -> Value {
    let holds = weft_listener::kinds::lookup(&spec.kind)
        .is_some_and(|h| h.between_fires(spec, &kind_state).expect("a test row's state reads") == weft_listener::kinds::BetweenFires::Holds);
    json!({
        "token": token, "tenant_id": "tenant-a", "for_instance": null, "node_id": "tick",
        "spec_json": serde_json::to_string(spec).unwrap(), "is_resume": false, "execution_id": null,
        "surface_kind": "internal", "mount_path": null, "mount_methods": [], "auth_kind": "none",
        "auth_config": null, "kind_state": kind_state, "kind_state_seq": seq, "holds": holds, "serving": null,
        "generation": 1
    })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// A wake for `token` at `due_at_ms` that carries nothing: it reads the
/// row, as a wake set before registrations were numbered does.
fn due(token: &str, due_at_ms: i64) -> WakeBody {
    WakeBody { token: token.into(), due_at_ms, generation: None, set_again: 0, row: None, addresses: Default::default(), ended: false }
}

/// The last wake the alarm was asked to set for `token`, as the listener
/// receives it, due now.
fn last_wake(rig: &Rig, token: &str) -> WakeBody {
    let set = rig.alarm.wakes_for(&format!("signal:{token}"));
    let mut body: WakeBody = serde_json::from_value(set.last().expect("a wake was set").body.clone()).unwrap();
    body.due_at_ms = now_ms();
    body
}

/// Register a signal the way the dispatcher does: prepare it (nothing
/// starts), write its row, then bring it up. Answers the prepared state.
async fn register(rig: &Rig, token: &str, spec: &weft_core::primitive::SignalSpec, asked_at_unix_ms: i64) -> anyhow::Result<Value> {
    let prepared = prepare_signal(&rig.state, identity(token, spec.clone()), None, None, asked_at_unix_ms).await?;
    assert!(rig.alarm.wakes_for(&format!("signal:{token}")).is_empty(), "preparing sets no wake");
    let written = row(token, spec, prepared.kind_state.clone(), 1);
    rig.rows.lock().unwrap().insert(token.into(), written.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(written)?, weft_core::signal::listener_protocol::StartMode::New).await?;
    Ok(prepared.kind_state)
}

/// A rehydrate brings up every row it can: a row that cannot come up is
/// named in the answer, and the rows after it still come up.
#[tokio::test]
async fn a_rehydrate_brings_up_every_row_past_a_broken_one() {
    let rig = rig(false).await;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 60_000 } });
    let prepared = prepare_signal(&rig.state, identity("good", spec.clone()), None, None, now_ms()).await.unwrap();
    let mut broken = row("broken", &spec, json!({}), 1);
    broken["spec_json"] = json!("not json");
    {
        let mut rows = rig.rows.lock().unwrap();
        rows.insert("broken".into(), broken);
        rows.insert("good".into(), row("good", &spec, prepared.kind_state, 1));
    }

    let down = weft_listener::registry::rehydrate(&rig.state, Some(uuid::Uuid::nil()), &[]).await.unwrap();
    assert_eq!(down.len(), 1, "{down:?}");
    assert!(down[0].contains("signal broken"), "{down:?}");
    assert_eq!(rig.alarm.wakes_for("signal:good").len(), 1, "the good row came up");
}

/// A timer is armed with its pinned moment, fires once on the wake, and
/// a second delivery of the same wake (an alarm retrying, a second copy
/// of the listener) fires nothing: the claim has one winner.
#[tokio::test]
async fn a_timer_wake_fires_once_however_often_it_is_delivered() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.expect("a timer registers on a serverless listener");
    let armed = rig.alarm.wakes_for("signal:tok");
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].at_unix_ms, asked + 1_000, "the wake is the pinned moment");
    assert_eq!(armed[0].role, weft_platform_traits::CoreRole::Listener);
    let body = || due("tok", asked + 1_000);

    wake(&rig.state, body()).await.unwrap();
    wake(&rig.state, body()).await.unwrap();
    let fires = rig.doors.fired.lock().unwrap();
    assert_eq!(fires.len(), 1, "one tick, whatever the deliveries");
    assert_eq!(fires[0].token, "tok");
    assert!(fires[0].payload["scheduledTime"].is_string());
    let stored = rig.rows.lock().unwrap()["tok"].clone();
    assert_eq!(stored["kind_state"], json!({}), "a one-shot has nothing left");
    assert_eq!(stored["kind_state_seq"], 2);
    assert_eq!(rig.alarm.wakes_for("signal:tok").len(), 1, "and sets no further wake");
}

/// A wake is only ever set once its row is written, so a wake that finds
/// no row is for a signal that is gone: it does nothing and sets nothing.
#[tokio::test]
async fn a_wake_for_a_signal_that_is_gone_does_nothing() {
    let rig = rig(true).await;
    let now = now_ms();
    wake(&rig.state, due("gone", now)).await.unwrap();
    assert!(rig.alarm.wakes_for("signal:gone").is_empty());
    assert!(rig.doors.fired.lock().unwrap().is_empty());
}

/// A registration that rewrote the row (a reactivate, a new schedule)
/// moved its version, so a wake that read the row before it cannot claim
/// over it: the new schedule stands.
#[tokio::test]
async fn a_wake_read_before_a_new_registration_cannot_claim_over_it() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    let fresh = json!({ "next_fire_at_unix_ms": asked + 3_600_000 });
    *rig.landing.lock().unwrap() = Some(fresh.clone());

    wake(&rig.state, due("tok", asked + 1_000)).await.unwrap();
    let stored = rig.rows.lock().unwrap()["tok"].clone();
    assert_eq!((stored["kind_state"].clone(), stored["kind_state_seq"].clone()), (fresh, json!(2)), "the new schedule stands");
}

/// A kind that holds a connection open is prepared as held by a listener
/// that scales to zero: its row says so, and a holder takes it. Nothing
/// starts here.
#[tokio::test]
async fn a_serverless_listener_prepares_a_held_kind_for_a_holder() {
    let rig = rig(false).await;
    let spec = to_spec(weft_core::signal::SseSubscribe { url: "https://example.com/feed".into(), event_name: String::new(), filters: Vec::new() });
    let prepared = prepare_signal(&rig.state, identity("sse", spec), None, None, now_ms()).await.expect("prepared");
    assert!(prepared.holds);
    assert!(rig.state.registry.get("sse").is_none());
}

/// A tick whose worker could not be reached, nor its trigger's queue, fails
/// the wake and leaves the moment unclaimed, so the alarm's retry fires it:
/// a one-shot is never lost to a worker and a broker down for a moment.
#[tokio::test]
async fn a_tick_that_could_not_be_delivered_is_fired_by_the_retried_wake() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    let body = || due("tok", asked + 1_000);

    rig.doors.failing.store(true, std::sync::atomic::Ordering::SeqCst);
    rig.queue.down.store(true, std::sync::atomic::Ordering::SeqCst);
    wake(&rig.state, body()).await.expect_err("a lost tick fails the wake so the alarm retries");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 1, "the moment is not claimed");

    rig.doors.failing.store(false, std::sync::atomic::Ordering::SeqCst);
    rig.queue.down.store(false, std::sync::atomic::Ordering::SeqCst);
    wake(&rig.state, body()).await.unwrap();
    let fires = rig.doors.fired.lock().unwrap();
    assert_eq!(fires.len(), 1, "the retry fires the tick");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 2);
}

/// A wake that keeps failing is delivered again only as many times as the
/// alarm gives it: a failure on each earlier delivery asks for another,
/// and one on the last gives the signal up, once, saying why on its row,
/// and answers the wake as handled, so nothing is retried for as long as
/// a queue keeps a task.
#[tokio::test]
async fn a_wake_that_fails_on_its_last_delivery_gives_its_signal_up_once() {
    use weft_listener::kinds::wake_delivered;
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    rig.doors.failing.store(true, std::sync::atomic::Ordering::SeqCst);
    rig.queue.down.store(true, std::sync::atomic::Ordering::SeqCst);

    for attempt in 1..FakeAlarm::ATTEMPTS {
        wake_delivered(&rig.state, last_wake(&rig, "tok"), FakeAlarm::delivered(attempt)).await.expect_err("tried again");
        assert!(rig.rows.lock().unwrap()["tok"]["held_failed"].is_null(), "delivery {attempt} gives nothing up");
    }
    let why = wake_delivered(&rig.state, last_wake(&rig, "tok"), FakeAlarm::delivered(FakeAlarm::ATTEMPTS))
        .await
        .unwrap()
        .expect("given up on the last delivery");
    assert!(why.contains(&format!("failed each of the {} times the listener handled it", FakeAlarm::ATTEMPTS)), "{why}");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["held_failed"], json!(why), "the row says why");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 1, "the moment was never claimed");
}

/// A give-up that could not be written (the broker down on the last
/// delivery) answers a failure, so the wake comes back past the bound;
/// what the node then reads names the bound the platform gives, never
/// how far past it the deliveries went.
#[tokio::test]
async fn a_wake_given_up_past_its_last_delivery_names_the_configured_bound() {
    use weft_listener::kinds::wake_delivered;
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    register(&rig, "tok", &to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } }), asked).await.unwrap();
    rig.doors.failing.store(true, std::sync::atomic::Ordering::SeqCst);
    rig.queue.down.store(true, std::sync::atomic::Ordering::SeqCst);

    let past = FakeAlarm::ATTEMPTS + 4;
    let why = wake_delivered(&rig.state, last_wake(&rig, "tok"), FakeAlarm::delivered(past))
        .await
        .unwrap()
        .expect("given up past the last delivery");
    assert!(why.contains(&format!("failed each of the {} times the listener handled it", FakeAlarm::ATTEMPTS)), "{why}");
    assert!(!why.contains(&format!("{past} times")), "{why}");
}

/// A wake that acted (its tick went out, its moment claimed) and then
/// could not set the wake after it is delivered again like any failure;
/// given up on its last delivery, it says that is what failed, never that
/// the wake failed to act.
#[tokio::test]
async fn a_wake_that_acted_but_could_not_set_the_next_says_so_when_given_up() {
    use weft_listener::kinds::wake_delivered;
    let rig = rig(false).await;
    let spec = to_spec(Timer { spec: TimerSpec::Cron { expression: "0 * * * * *".into(), timezone: "UTC".into() } });
    register(&rig, "tok", &spec, now_ms() - 120_000).await.unwrap();
    let armed: WakeBody = serde_json::from_value(rig.alarm.wakes_for("signal:tok")[0].body.clone()).unwrap();
    assert!(armed.due_at_ms < now_ms(), "its first moment has passed");
    rig.alarm.fail_sets(true);

    wake_delivered(&rig.state, armed.clone(), FakeAlarm::delivered(1)).await.expect_err("tried again");
    assert_eq!(rig.doors.fired.lock().unwrap().len(), 1, "it acted");
    let why = wake_delivered(&rig.state, armed, FakeAlarm::delivered(FakeAlarm::ATTEMPTS))
        .await
        .unwrap()
        .expect("given up on the last delivery");
    assert!(why.contains("was handled, but the wake after it could not be set"), "{why}");
    assert!(why.contains(&format!("the last of the {} times the listener handled it", FakeAlarm::ATTEMPTS)), "{why}");
    assert!(!why.contains("failed each"), "{why}");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["held_failed"], json!(why), "the row says why");
}

/// A tick whose worker cannot be reached waits in its trigger's queue,
/// already processed, under the fire's own id: the install hands it over
/// once the worker is back, and one the worker did take is born once.
#[tokio::test]
async fn a_tick_whose_worker_is_down_waits_in_its_triggers_queue() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    rig.doors.failing.store(true, std::sync::atomic::Ordering::SeqCst);

    wake(&rig.state, due("tok", asked + 1_000)).await.unwrap();
    assert!(rig.doors.fired.lock().unwrap().is_empty());
    let parked = rig.queue.parked.lock().unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0]["token"], "tok");
    assert!(parked[0]["fire"]["payload"]["scheduledTime"].is_string(), "parked processed by its kind");
    assert!(parked[0]["fire"]["fire_id"].as_str().unwrap().parse::<uuid::Uuid>().is_ok());
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 2, "the moment is claimed: the tick is kept");
}

/// A far wake a platform delivered early (Cloud Tasks cannot schedule past
/// 30 days) touches nothing and is set again for its real moment, as a
/// booking of its own (the task being answered still holds the first
/// one's name) that reads its row when it comes due. Set again a second
/// time, it is yet another booking.
#[tokio::test]
async fn a_wake_that_arrives_before_its_moment_is_set_again_for_it() {
    let rig = rig(false).await;
    let asked = now_ms();
    let far = asked + 60 * 24 * 3_600_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: (far - asked) as u64 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    let armed = rig.alarm.wakes_for("signal:tok");
    assert_eq!(armed.len(), 1);

    let first: WakeBody = serde_json::from_value(armed[0].body.clone()).unwrap();
    wake(&rig.state, first).await.unwrap();
    assert!(rig.doors.fired.lock().unwrap().is_empty(), "nothing fires early");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 1);
    let again = rig.alarm.wakes_for("signal:tok:again-1");
    assert_eq!(again.len(), 1, "set again once");
    assert_eq!((again[0].at_unix_ms, again[0].generation), (far, armed[0].generation));
    assert_ne!(again[0].name(), armed[0].name(), "named apart from the task being answered");
    let body: WakeBody = serde_json::from_value(again[0].body.clone()).unwrap();
    assert_eq!((body.due_at_ms, body.set_again), (far, 1));
    assert!(body.row.is_none(), "it reads its row at its moment");

    wake(&rig.state, body).await.unwrap();
    let twice = rig.alarm.wakes_for("signal:tok:again-2");
    assert_eq!(twice.len(), 1, "set again a second time, apart from both");
    assert_ne!(twice[0].name(), again[0].name());
}

/// A poll down here, whose wake chain is still set (its restore could not
/// set the next wake), has that wake cancelled when it is unregistered,
/// like any other.
#[tokio::test]
async fn unregistering_a_down_poll_cancels_its_wakes() {
    let rig = rig(false).await;
    let spec = delta_poll("http://127.0.0.1:1/feed");
    register(&rig, "poll", &spec, now_ms()).await.unwrap();
    let set = rig.alarm.wakes_for("signal:poll").pop().expect("armed");

    rig.alarm.fail_sets(true);
    let written = rig.rows.lock().unwrap()["poll"].clone();
    weft_listener::registry::hold(&rig.state, serde_json::from_value(written).unwrap(), StartMode::Restore).await.expect_err("down");
    assert!(rig.state.registry.down_reason("poll").is_some());

    let removed = weft_core::signal::listener_protocol::UnregisterRequest {
        token: "poll".into(),
        tenant_id: "tenant-a".into(),
        spec,
        kind_state: rig.rows.lock().unwrap()["poll"]["kind_state"].clone(),
        generation: 1,
        reused: false,
    };
    weft_listener::kinds::unregister(&rig.state, removed).await.unwrap();
    assert!(rig.alarm.cancelled().iter().any(|w| w.name() == set.name()), "the wake still set is cancelled");
    assert!(rig.state.registry.down_reason("poll").is_none());
}

/// A poll down in a holder is no holder's claim, so the holder's look
/// leaves it to its own retry instead of stopping it.
#[tokio::test]
async fn a_holders_look_leaves_a_down_poll_retrying() {
    let rig = rig(true).await;
    register(&rig, "poll", &delta_poll("http://127.0.0.1:1/feed"), now_ms()).await.unwrap();
    rig.alarm.fail_sets(true);
    let written = rig.rows.lock().unwrap()["poll"].clone();
    weft_listener::registry::hold(&rig.state, serde_json::from_value(written).unwrap(), StartMode::Restore).await.expect_err("down");

    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    assert!(rig.state.registry.down_reason("poll").is_some(), "still down, and retried");
}

/// A feed that answers 500 while `failing` is set, and one page with a
/// single item once it is cleared.
async fn spawn_feed(failing: Arc<std::sync::atomic::AtomicBool>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/feed",
        axum::routing::get(move || {
            let failing = failing.clone();
            async move {
                if failing.load(std::sync::atomic::Ordering::SeqCst) {
                    Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                } else {
                    Ok(axum::Json(json!({ "items": [{ "id": 7 }] })))
                }
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/feed")
}

/// Another copy of the listener, as a serverless platform starts one per
/// request: nothing in memory, the same broker, task store and alarm.
fn fresh_listener(rig: &Rig) -> ListenerState {
    ListenerState::new((*rig.state.config).clone(), rig.tasks.clone(), link(&rig.broker), rig.alarm.clone(), rig.doors.clone(), holders())
}

/// What a signal's display says, as one string.
async fn shown(state: &ListenerState, token: &str) -> String {
    let sig = weft_listener::registry::held(state, token).await.unwrap().expect("the row is held");
    let live = weft_listener::kinds::compute_live(&weft_listener::kinds::LiveCtx { sig: &sig, address: None });
    serde_json::to_string(&live).unwrap()
}

/// A poll's failure streak rides its wakes, so it grows across wakes that
/// each run on a fresh listener (as serverless copies do), and the row is
/// written only as the polls start failing, as the streak reaches the
/// escalation, and as they work again; the node's display reads it there.
/// A delta poll that never succeeded stays unprimed however often its row
/// was written, so its first good poll primes silently instead of firing
/// history.
#[tokio::test]
async fn a_poll_failure_streak_rides_its_wakes_and_priming_is_explicit() {
    let failing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let feed = spawn_feed(failing.clone()).await;
    let spec = to_spec(
        serde_json::from_value::<weft_core::signal::PollEndpoint>(json!({
            "url": feed,
            "interval_secs": 60,
            "delta": { "items": "items", "cursor_field": "id" },
        }))
        .unwrap(),
    );
    let first = rig(false).await;
    register(&first, "poll", &spec, now_ms()).await.unwrap();

    // (polls in a row that failed, what the row says, its version)
    for (n, stored, seq, says) in [(1, 1, 2, "(failing)"), (2, 1, 2, "(failing)"), (3, 3, 3, "failed 3 or more in a row")] {
        let copy = fresh_listener(&first);
        let reads = first.get_held.reads.load(std::sync::atomic::Ordering::SeqCst);
        wake(&copy, last_wake(&first, "poll")).await.unwrap();
        assert_eq!(first.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), reads, "the poll read no row");
        let row = first.rows.lock().unwrap()["poll"].clone();
        assert_eq!((row["kind_state"]["consecutive_failures"].clone(), row["kind_state_seq"].clone()), (json!(stored), json!(seq)), "poll {n}: {row}");
        assert!(row["kind_state"].get("delta").is_none(), "a failed poll primes nothing");
        assert_eq!(last_wake(&first, "poll").row.unwrap().kind_state["consecutive_failures"], n, "the streak rides the wake");
        assert!(shown(&copy, "poll").await.contains(says));
    }

    failing.store(false, std::sync::atomic::Ordering::SeqCst);
    let copy = fresh_listener(&first);
    wake(&copy, last_wake(&first, "poll")).await.unwrap();
    assert!(first.doors.fired.lock().unwrap().is_empty(), "the first good poll primes, it fires nothing");
    let stored = first.rows.lock().unwrap()["poll"]["kind_state"].clone();
    assert_eq!(stored, json!({ "delta": { "cursor": 7 } }), "primed, and the streak is cleared");
    assert!(shown(&copy, "poll").await.contains("polling"));
}

// ---------- A run waiting on a poll ----------

/// A job status endpoint answering each GET with the next of `script`
/// (status code, body), then repeating the last; counts the GETs.
async fn spawn_status(script: Vec<(u16, Value)>) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = hits.clone();
    let script = Arc::new(script);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/status",
        axum::routing::get(move || {
            let hits = counted.clone();
            let script = script.clone();
            async move {
                let n = hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (code, body) = script[n.min(script.len() - 1)].clone();
                (axum::http::StatusCode::from_u16(code).unwrap(), axum::Json(body))
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/status"), hits)
}

/// The run a registered wait belongs to.
const WAITING_RUN: &str = "00000000-0000-0000-0000-0000000000e1";

/// A wait on a job: resumes on the first answer whose `status` says done.
fn job_wait(url: &str) -> weft_core::primitive::SignalSpec {
    to_spec(weft_core::signal::PollEndpoint {
        url: url.into(),
        interval_secs: 60,
        filters: vec![weft_core::signal::Predicate::regex("status", "COMPLETED|FAILED")],
        ..Default::default()
    })
}

/// Register a parked run's wait the way the dispatcher does.
async fn register_wait(rig: &Rig, token: &str, spec: &weft_core::primitive::SignalSpec, asked_at_unix_ms: i64) -> anyhow::Result<()> {
    let mut who = identity(token, spec.clone());
    who.is_resume = true;
    who.execution_id = Some(WAITING_RUN.parse().unwrap());
    let prepared = prepare_signal(&rig.state, who, None, None, asked_at_unix_ms).await?;
    let mut written = row(token, spec, prepared.kind_state, 1);
    written["is_resume"] = json!(true);
    written["execution_id"] = json!(WAITING_RUN);
    rig.rows.lock().unwrap().insert(token.into(), written.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(written)?, StartMode::New).await
}

/// A job that is already done when the run starts waiting resumes it on
/// the first wake, which is pinned at the moment the wait was asked for
/// rather than an interval later. The answer is the response itself, it
/// routes to the parked run, and nothing polls after it.
#[tokio::test]
async fn a_wait_on_a_job_already_done_resumes_at_once() {
    let rig = rig(false).await;
    let (url, hits) = spawn_status(vec![(200, json!({ "status": "COMPLETED", "url": "https://cdn/x.mp4" }))]).await;
    let asked = now_ms();
    register_wait(&rig, "wait", &job_wait(&url), asked).await.unwrap();
    let armed = rig.alarm.wakes_for("signal:wait");
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].at_unix_ms, asked, "the first poll is the moment the wait was asked for");

    wake(&rig.state, due("wait", asked)).await.unwrap();
    {
        let fires = rig.tasks.answers.lock().unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].payload["value"]["url"], "https://cdn/x.mp4");
    }
    assert_eq!(rig.alarm.wakes_for("signal:wait").len(), 1, "answered: no further poll is set");
    assert!(shown(&rig.state, "wait").await.contains("answered"));

    let outcome = weft_listener::kinds::process(&rig.state, "wait", json!({ "status": "COMPLETED" })).await.unwrap();
    assert!(
        matches!(&outcome.target, weft_core::signal::listener_protocol::ProcessTarget::Resume { execution_id } if execution_id.to_string() == WAITING_RUN),
        "the fire resumes the parked run: {:?}",
        outcome.target
    );

    // A wake that was already set when the answer went out polls nothing.
    wake(&rig.state, due("wait", now_ms())).await.unwrap();
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(rig.tasks.answers.lock().unwrap().len(), 1);
}

/// A job still running is not an answer: the filter drops it and the wait
/// keeps polling on its interval. A non-2xx answer is a failed poll,
/// counted on the row and retried. The first answer that passes the
/// filter resumes the run, once.
#[tokio::test]
async fn a_wait_polls_until_the_job_is_done_and_resumes_once() {
    let rig = rig(false).await;
    let (url, hits) = spawn_status(vec![
        (200, json!({ "status": "IN_PROGRESS" })),
        (503, json!({ "error": "busy" })),
        (200, json!({ "status": "IN_PROGRESS" })),
        (200, json!({ "status": "FAILED", "error": "nsfw" })),
    ])
    .await;
    let asked = now_ms();
    register_wait(&rig, "wait", &job_wait(&url), asked).await.unwrap();

    for _ in 0..3 {
        wake(&rig.state, due("wait", now_ms())).await.unwrap();
        assert!(rig.tasks.answers.lock().unwrap().is_empty(), "not done yet: nothing fires");
    }
    let stored = rig.rows.lock().unwrap()["wait"]["kind_state"].clone();
    assert_eq!(stored, json!({}), "the failure streak cleared on the good poll after it");
    let wakes = rig.alarm.wakes_for("signal:wait");
    assert_eq!(wakes.len(), 4, "armed, then one more per poll");
    assert!(wakes[1].at_unix_ms > asked, "after the first poll, the interval sets the pace");

    wake(&rig.state, due("wait", now_ms())).await.unwrap();
    {
        let fires = rig.tasks.answers.lock().unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].payload["value"], json!({ "status": "FAILED", "error": "nsfw" }));
    }
    assert_eq!(rig.alarm.wakes_for("signal:wait").len(), 4, "answered: no further poll is set");
    assert_eq!(rig.rows.lock().unwrap()["wait"]["kind_state"], json!({ "resumed": true }));
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
}

/// A non-2xx answer is shown on the node as a failed poll while the
/// wait goes on.
#[tokio::test]
async fn a_wait_whose_status_endpoint_fails_keeps_polling_and_says_so() {
    let rig = rig(false).await;
    let (url, _) = spawn_status(vec![(404, json!({ "error": "no such job" }))]).await;
    register_wait(&rig, "wait", &job_wait(&url), now_ms()).await.unwrap();
    wake(&rig.state, due("wait", now_ms())).await.unwrap();
    assert!(rig.doors.fired.lock().unwrap().is_empty());
    assert!(shown(&rig.state, "wait").await.contains("404"), "{}", shown(&rig.state, "wait").await);
    assert_eq!(rig.alarm.wakes_for("signal:wait").len(), 2, "it polls again");
}

/// Delta mode cannot serve a run's wait (its first poll primes
/// silently), so the registration is refused, naming why.
#[tokio::test]
async fn a_wait_in_delta_mode_is_refused() {
    let rig = rig(false).await;
    let spec = to_spec(
        serde_json::from_value::<weft_core::signal::PollEndpoint>(json!({
            "url": "https://example.com/jobs",
            "delta": { "items": "items", "cursor_field": "id" },
        }))
        .unwrap(),
    );
    let err = register_wait(&rig, "wait", &spec, now_ms()).await.expect_err("refused");
    assert!(format!("{err:#}").contains("delta"), "{err:#}");
}

/// A poll that fires nothing (its filter drops every answer), so its wakes
/// touch no row, on a grid of an hour.
fn quiet_poll(url: &str) -> weft_core::primitive::SignalSpec {
    to_spec(weft_core::signal::PollEndpoint {
        url: url.into(),
        interval_secs: 3600,
        filters: vec![weft_core::signal::Predicate::regex("status", "DONE")],
        ..Default::default()
    })
}

/// The grid point of an hour at or before now.
fn this_hour() -> i64 {
    now_ms().div_euclid(3_600_000) * 3_600_000
}

/// A poll registered again while one of its wakes is being handled (the
/// wake carries its row, so it reads nothing): the moment it sets next was
/// ended by the new registration's start, so the next one under its name
/// is the ended wake, which does nothing.
#[tokio::test]
async fn a_poll_wake_handled_while_its_signal_is_registered_again_ends_its_chain() {
    let rig = rig(false).await;
    let (url, hits) = spawn_status(vec![(200, json!({ "status": "RUNNING" }))]).await;
    let spec = quiet_poll(&url);
    register(&rig, "poll", &spec, now_ms()).await.unwrap();
    let mut in_flight = last_wake(&rig, "poll");
    in_flight.due_at_ms = this_hour();
    assert!(in_flight.row.is_some(), "a quiet poll's wake carries its row");

    // Registered again while that wake is handled: the start ends the
    // replaced registration's wakes before the wake sets its next one.
    let kind_state = rig.rows.lock().unwrap()["poll"]["kind_state"].clone();
    rig.rows.lock().unwrap().get_mut("poll").unwrap()["generation"] = json!(2);
    weft_listener::kinds::cancel_wakes(&rig.state, "poll", 1, &spec, &kind_state).await.unwrap();
    assert_eq!(wake(&rig.state, in_flight).await.unwrap(), None);

    let next = rig.alarm.wakes_for("signal:poll").pop().expect("the wake set its next one");
    assert_eq!((next.generation, next.at_unix_ms), (1, this_hour() + 3_600_000));
    let kept = rig.alarm.under(&next.name()).expect("a wake goes off under that name");
    let kept: WakeBody = serde_json::from_value(kept.body).unwrap();
    assert!(kept.ended, "the name was taken by the ended wake");
    let polled = hits.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(wake(&rig.state, kept).await.unwrap(), None);
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), polled, "the ended wake polls nothing");
}

/// A carried poll wake handled past the moment after its own sets a later
/// one, which nothing ended, so it reads its row first: registered again
/// meanwhile, it sets nothing; still its registration, it goes on.
#[tokio::test]
async fn a_late_poll_wake_reads_its_row_before_setting_the_next() {
    let rig = rig(false).await;
    let (url, _) = spawn_status(vec![(200, json!({ "status": "RUNNING" }))]).await;
    register(&rig, "poll", &quiet_poll(&url), now_ms()).await.unwrap();
    let mut late = last_wake(&rig, "poll");
    late.due_at_ms = this_hour() - 2 * 3_600_000;
    let set = rig.alarm.wakes_for("signal:poll").len();

    let reads = rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst);
    wake(&rig.state, late.clone()).await.unwrap();
    assert_eq!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), reads + 1, "a late wake reads its row");
    assert_eq!(rig.alarm.wakes_for("signal:poll").len(), set + 1, "still its registration: it goes on");

    rig.rows.lock().unwrap().get_mut("poll").unwrap()["generation"] = json!(2);
    wake(&rig.state, late).await.unwrap();
    assert_eq!(rig.alarm.wakes_for("signal:poll").len(), set + 1, "registered again: it sets nothing");
}

/// A signed-in poll whose connection the broker cannot resolve now (a
/// 503) counts a failed poll and polls again; one whose connection the
/// broker says is gone (a 404) gives the signal up, once, and its wake is
/// answered as handled, so the alarm stops delivering it.
#[tokio::test]
async fn a_poll_whose_connection_is_gone_gives_up_once_and_one_unreachable_polls_again() {
    let rig = rig(false).await;
    let mut spec = to_spec(weft_core::signal::PollEndpoint { url: "http://127.0.0.1:1/feed".into(), interval_secs: 60, ..Default::default() });
    spec.access = Some(weft_core::primitive::AccessRef { id: uuid::Uuid::nil().to_string(), service: "feeds".into(), required_values: vec![] });
    register(&rig, "poll", &spec, now_ms()).await.unwrap();

    rig.get_held.resolve_status.store(503, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(wake(&rig.state, last_wake(&rig, "poll")).await.unwrap(), None);
    assert_eq!(rig.alarm.wakes_for("signal:poll").len(), 2, "it polls again");
    assert!(rig.rows.lock().unwrap()["poll"]["held_failed"].is_null());

    rig.get_held.resolve_status.store(404, std::sync::atomic::Ordering::SeqCst);
    let why = wake(&rig.state, last_wake(&rig, "poll")).await.unwrap().expect("given up");
    assert!(why.contains("cannot be resolved"), "{why}");
    assert_eq!(rig.rows.lock().unwrap()["poll"]["held_failed"], json!(why), "the row says why");
    assert_eq!(rig.alarm.wakes_for("signal:poll").len(), 2, "nothing polls after it");
}

// ---------- Held connections: one bring-up path ----------

/// A held-connection spec whose loop only ever retries (nothing listens
/// on the port), so bringing it up needs nothing reachable.
fn sse_spec() -> weft_core::primitive::SignalSpec {
    to_spec(weft_core::signal::SseSubscribe {
        url: "http://127.0.0.1:1/events".into(),
        event_name: String::new(),
        filters: vec![],
    })
}

fn has_task(state: &ListenerState, token: &str) -> bool {
    state.registry.get(token).is_some_and(|sig| sig.task.is_some())
}

/// Two bring-ups of a held connection at once (a look racing a
/// registration's take) start it once: the second waits for the first and
/// finds it up, instead of replacing (and so stopping) the task the first
/// just started.
#[tokio::test]
async fn two_bring_ups_of_a_held_connection_start_it_once() {
    let rig = rig(true).await;
    let written = row("sse", &sse_spec(), json!({}), 1);
    rig.rows.lock().unwrap().insert("sse".into(), written.clone());
    let up = || weft_listener::registry::hold(&rig.state, serde_json::from_value(written.clone()).unwrap(), weft_core::signal::listener_protocol::StartMode::Restore);
    let (a, b) = tokio::join!(up(), up());
    a.unwrap();
    b.unwrap();
    let first = rig.state.registry.get("sse").expect("up");
    assert!(has_task(&rig.state, "sse"));
    let again = weft_listener::registry::held(&rig.state, "sse").await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&first.serving, &again.serving), "one running connection answers");
}

/// A holder takes the held signals nobody holds, keeps them while it
/// claims them, and stops one whose claim went (its row was rewritten)
/// and takes it up again fresh in that same look: no later look is
/// coming.
#[tokio::test]
async fn a_holder_takes_what_nobody_holds_and_takes_back_a_rewrite_at_once() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    let restore = weft_core::signal::listener_protocol::StartMode::Restore;
    weft_listener::hold::take_now(&rig.state, &[], restore).await.unwrap();
    assert!(has_task(&rig.state, "sse"), "taken and up");
    let first = rig.state.registry.get("sse").unwrap();
    weft_listener::hold::take_now(&rig.state, &[], restore).await.unwrap();
    assert!(Arc::ptr_eq(&first.serving, &rig.state.registry.get("sse").unwrap().serving), "kept as it runs");
    rig.rows.lock().unwrap().get_mut("sse").unwrap()["held_by"] = Value::Null;
    weft_listener::hold::take_now(&rig.state, &[], restore).await.unwrap();
    let again = rig.state.registry.get("sse").expect("taken back in the same look");
    assert!(!Arc::ptr_eq(&first.serving, &again.serving), "as a new connection");
}

/// A listener that does not hold never brings a held connection up: it
/// answers from the row, with what its holder last said.
#[tokio::test]
async fn a_listener_that_does_not_hold_answers_a_held_signal_from_its_row() {
    let rig = rig(false).await;
    let mut written = row("sse", &sse_spec(), json!({}), 1);
    written["serving"] = json!({ "status": "listening" });
    rig.rows.lock().unwrap().insert("sse".into(), written);
    let sig = weft_listener::registry::held(&rig.state, "sse").await.unwrap().expect("its row answers");
    assert!(sig.task.is_none());
    assert_eq!(sig.serving.lock().status, "listening");
    assert!(rig.state.registry.get("sse").is_none(), "nothing is brought up here");
    let down = weft_listener::registry::rehydrate(&rig.state, None, &[]).await.unwrap();
    assert!(down.is_empty() && rig.state.registry.get("sse").is_none(), "a rehydrate leaves it to the holders");
}

/// A held connection whose row cannot be read again once it is up keeps
/// running: the row was held when it was listed, and an unregister that
/// comes later still finds it in the registry.
#[tokio::test]
async fn a_held_connection_stays_up_when_its_row_cannot_be_read_again() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    rig.get_held.failing.store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
    let down = weft_listener::registry::rehydrate(&rig.state, None, &[]).await.unwrap();
    assert!(down.is_empty(), "{down:?}");
    assert!(has_task(&rig.state, "sse"), "a broker hiccup does not turn it off");
    assert!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst) > 1, "the re-read was retried");
    assert!(rig.state.registry.down_reason("sse").is_none());
}

/// A signal that could not come up is down and says so, is tried again
/// from its row as it reads then, a few times over about a minute, then
/// given up on: written once on its row, which its node shows, and no
/// longer tried, nor brought back by a rehydrate.
#[tokio::test(start_paused = true)]
async fn a_down_signal_is_tried_a_few_times_then_given_up() {
    let rig = rig(false).await;
    let mut broken = row("tok", &to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } }), json!({}), 1);
    broken["spec_json"] = json!("not json");
    rig.rows.lock().unwrap().insert("tok".into(), broken.clone());

    weft_listener::registry::hold(&rig.state, serde_json::from_value(broken).unwrap(), StartMode::Restore).await.expect_err("down");
    assert!(rig.state.registry.down_reason("tok").unwrap().contains("malformed"));
    for (n, wait) in [5, 15].into_iter().enumerate() {
        next_try(&rig, wait, n + 1).await;
        assert_eq!(rig.state.registry.retry_loops_running(), 1, "still trying after try {}", n + 1);
        assert!(rig.rows.lock().unwrap()["tok"]["held_failed"].is_null());
    }
    // The last try, then its give-up, which goes to the broker.
    tokio::task::yield_now().await;
    tokio::time::advance(weft_core::time_scale::scaled(std::time::Duration::from_secs(40)) + std::time::Duration::from_millis(1)).await;
    tokio::time::resume();
    wait_for(|| !rig.rows.lock().unwrap()["tok"]["held_failed"].is_null()).await;
    assert!(rig.rows.lock().unwrap()["tok"]["held_failed"].as_str().unwrap().contains("malformed"));
    wait_for(|| rig.state.registry.retry_loops_running() == 0).await;
    assert!(rig.state.registry.down_reason("tok").is_none());
    assert_eq!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), 3, "each try read the row first");

    let down = weft_listener::registry::rehydrate(&rig.state, None, &[]).await.unwrap();
    assert!(down.is_empty() && rig.state.registry.down_reason("tok").is_none(), "a rehydrate leaves it for a registration again");
}

/// Let a down row's retry loop, asleep on the paused clock, through its
/// `wait_secs` wait into try `n`, whose row read goes to the broker on the
/// real clock (the paused one would time the call out at once); then,
/// once the loop has handled the answer and gone back to sleep, pause the
/// clock again.
async fn next_try(rig: &Rig, wait_secs: u64, n: usize) {
    tokio::task::yield_now().await;
    tokio::time::advance(weft_core::time_scale::scaled(std::time::Duration::from_secs(wait_secs)) + std::time::Duration::from_millis(1)).await;
    tokio::time::resume();
    wait_for(|| rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst) >= n).await;
    // The answer's handling is CPU work on this one thread, done well
    // inside this pause.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    tokio::time::pause();
}

/// A down signal whose row went before its next try (unregistered through
/// another copy of the listener, which this one never hears) stops being
/// tried: nothing comes up, no wake is set, and nothing is given up on.
#[tokio::test(start_paused = true)]
async fn a_down_signal_whose_row_went_is_no_longer_tried() {
    let rig = rig(false).await;
    let mut broken = row("tok", &to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } }), json!({}), 1);
    broken["spec_json"] = json!("not json");
    weft_listener::registry::hold(&rig.state, serde_json::from_value(broken).unwrap(), StartMode::Restore).await.expect_err("down");
    assert_eq!(rig.state.registry.retry_loops_running(), 1);

    tokio::task::yield_now().await;
    tokio::time::advance(weft_core::time_scale::scaled(std::time::Duration::from_secs(5)) + std::time::Duration::from_millis(1)).await;
    tokio::time::resume();
    wait_for(|| rig.state.registry.retry_loops_running() == 0).await;
    assert!(rig.state.registry.down_reason("tok").is_none(), "no longer counted down");
    assert_eq!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), 1, "one read, which found it gone");
    assert!(rig.alarm.wakes_for("signal:tok").is_empty(), "nothing armed for a gone row");
}

use weft_core::signal::listener_protocol::StartMode;

fn sse_at(url: &str) -> weft_core::primitive::SignalSpec {
    to_spec(weft_core::signal::SseSubscribe { url: url.into(), event_name: String::new(), filters: vec![] })
}

fn unreadable_sse() -> weft_core::primitive::SignalSpec {
    let mut spec = sse_spec();
    spec.config = json!({ "url": 5 });
    spec
}

/// A held connection whose row is deleted while it comes up (the
/// unregister passed before its task was in the registry) is let go once
/// the row is read again, instead of running for a row nobody holds.
#[tokio::test]
async fn a_held_connection_whose_row_went_while_it_came_up_is_let_go() {
    let rig = rig(true).await;
    let snapshot = row("sse", &sse_spec(), json!({}), 1);
    weft_listener::registry::hold(&rig.state, serde_json::from_value(snapshot).unwrap(), StartMode::Restore).await.unwrap();
    assert!(rig.state.registry.get("sse").is_none(), "the re-read found no row, so nothing holds it");
    assert!(rig.state.registry.down_reason("sse").is_none());
}

/// A row the dispatcher puts back replaces the replacement running under
/// the same token, so the listener ends up running what the row says.
#[tokio::test]
async fn a_put_back_row_replaces_the_connection_running_under_its_token() {
    let rig = rig(true).await;
    let replacement = row("sse", &sse_at("http://127.0.0.1:1/new"), json!({}), 1);
    rig.rows.lock().unwrap().insert("sse".into(), replacement.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(replacement).unwrap(), StartMode::New).await.unwrap();
    let running = rig.state.registry.get("sse").unwrap();

    let prior = row("sse", &sse_at("http://127.0.0.1:1/old"), json!({}), 2);
    rig.rows.lock().unwrap().insert("sse".into(), prior.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(prior).unwrap(), StartMode::PutBack).await.unwrap();

    let now = rig.state.registry.get("sse").unwrap();
    assert_eq!(now.spec.config["url"], "http://127.0.0.1:1/old", "the put-back row runs");
    assert!(!Arc::ptr_eq(&running.serving, &now.serving), "the replacement's connection was replaced");
}

/// A put-back that cannot come up stops the replacement it was undoing
/// (the row no longer says that spec) and is marked down and retried.
#[tokio::test]
async fn a_put_back_that_cannot_come_up_stops_the_replacement_and_is_retried() {
    let rig = rig(true).await;
    let replacement = row("sse", &sse_spec(), json!({}), 1);
    rig.rows.lock().unwrap().insert("sse".into(), replacement.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(replacement).unwrap(), StartMode::New).await.unwrap();

    let prior = row("sse", &unreadable_sse(), json!({}), 2);
    rig.rows.lock().unwrap().insert("sse".into(), prior.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(prior).unwrap(), StartMode::PutBack)
        .await
        .expect_err("the put-back row cannot come up");
    assert!(rig.state.registry.get("sse").is_none(), "the replacement no longer runs");
    assert!(rig.state.registry.down_reason("sse").is_some(), "the put-back row is down and retried");
    assert_eq!(rig.state.registry.retry_loops_running(), 1);
}

/// A row cleared and marked down again while its retry loop sleeps gets
/// one new loop, and the old one ends at its next turn: never two loops
/// for one token.
#[tokio::test(start_paused = true)]
async fn a_row_marked_down_again_keeps_one_retry_loop() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &unreadable_sse(), json!({}), 1));
    let bring_up = || async {
        let row = rig.rows.lock().unwrap()["sse"].clone();
        weft_listener::registry::hold(&rig.state, serde_json::from_value(row).unwrap(), StartMode::Restore).await
    };
    bring_up().await.expect_err("down");
    assert_eq!(rig.state.registry.retry_loops_running(), 1);
    rig.state.registry.clear_down("sse");
    bring_up().await.expect_err("down again");

    // Past the first loop's first wait, it has seen the entry is not its own.
    let first_wait = weft_core::time_scale::scaled(std::time::Duration::from_secs(5));
    tokio::time::sleep(first_wait + std::time::Duration::from_millis(500)).await;
    assert_eq!(rig.state.registry.retry_loops_running(), 1, "one loop for the token");
    assert!(rig.state.registry.down_reason("sse").is_some());
}

// ---------- Replacing a held connection ----------

/// A held kind that records, in order, each task it starts (by the spec's
/// `n`) and each outside teardown it is asked for, under the spec's
/// `test`: the tests of this binary share one process under `cargo test`,
/// so one log per test keeps them from reading each other's entries.
struct RecordingHold;

static RECORDED: Mutex<std::collections::BTreeMap<String, Vec<String>>> = Mutex::new(std::collections::BTreeMap::new());

fn recorded(test: &str) -> Vec<String> {
    RECORDED.lock().unwrap().get(test).cloned().unwrap_or_default()
}

fn record(spec: &weft_core::primitive::SignalSpec, entry: String) {
    let test = spec.config["test"].as_str().expect("a recording spec names its test").to_string();
    RECORDED.lock().unwrap().entry(test).or_default().push(entry);
}
const RECORDING_TAG: &str = "test_recording_hold";

#[async_trait::async_trait]
impl weft_listener::kinds::KindHandler for RecordingHold {
    fn tag(&self) -> &'static str {
        RECORDING_TAG
    }

    fn between_fires(&self, _spec: &weft_core::primitive::SignalSpec, _kind_state: &serde_json::Value) -> anyhow::Result<weft_listener::kinds::BetweenFires> {
        Ok(weft_listener::kinds::BetweenFires::Holds)
    }

    fn compute_routing(&self, _spec: &weft_core::primitive::SignalSpec) -> anyhow::Result<weft_core::primitive::SignalRouting> {
        anyhow::bail!("never registered through prepare in this test")
    }

    async fn spawn_task(
        &self,
        spec: &weft_core::primitive::SignalSpec,
        _kind_state: &Value,
        _ctx: weft_listener::kinds::SpawnCtx,
    ) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
        record(spec, format!("spawn {}", spec.config["n"]));
        Ok(Some(tokio::spawn(std::future::pending())))
    }

    fn process_entry(
        &self,
        _sig: &weft_listener::registry::RegisteredSignal,
        payload: Value,
    ) -> weft_core::signal::listener_protocol::ProcessOutcome {
        weft_core::signal::listener_protocol::ProcessOutcome {
            value: payload,
            target: weft_core::signal::listener_protocol::ProcessTarget::Entry,
        }
    }

    fn render(&self, _token: &str, _sig: &weft_listener::registry::RegisteredSignal) -> anyhow::Result<Option<Value>> {
        Ok(None)
    }

    async fn on_unregister(
        &self,
        _token: &str,
        sig: &weft_listener::registry::RegisteredSignal,
        _events_broker: &Arc<weft_broker_client::BrokerEventsClient>,
    ) {
        record(&sig.spec, format!("unregister {}", sig.spec.config["n"]));
    }
}

inventory::submit!(&RecordingHold as &dyn weft_listener::kinds::KindHandler);

fn recording_spec(test: &str, n: u32) -> weft_core::primitive::SignalSpec {
    weft_core::primitive::SignalSpec {
        kind: RECORDING_TAG.into(),
        config: json!({ "n": n, "test": test }),
        consumer_kind: None,
        access: None,
        match_predicates: Vec::new(),
        limits: Default::default(),
        settings: Default::default(),
    }
}

/// Replacing a held connection tears the displaced one down (a provider
/// subscription of its spec is dropped) before the new one starts, since
/// that teardown is keyed by the token the new one reuses; forgetting it
/// later tears the new one down.
#[tokio::test]
async fn replacing_a_held_connection_tears_the_displaced_one_down_first() {
    let rig = rig(true).await;
    for n in 1..=2 {
        let written = row("rec", &recording_spec("replacing", n), json!({}), n.into());
        rig.rows.lock().unwrap().insert("rec".into(), written.clone());
        weft_listener::registry::hold(&rig.state, serde_json::from_value(written).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
            .await
            .unwrap();
    }
    assert_eq!(recorded("replacing"), ["spawn 1", "unregister 1", "spawn 2"]);

    weft_listener::kinds::forget(&rig.state, "rec");
    wait_for(|| recorded("replacing").len() == 4).await;
    assert_eq!(recorded("replacing")[3], "unregister 2");
    assert!(rig.state.registry.get("rec").is_none());
}

/// Forgetting a held connection and bringing the same token straight
/// back up tears the old one down before the new one starts: the
/// detached teardown holds the token's bring-up guard, so it cannot drop
/// the new subscription.
#[tokio::test]
async fn a_bring_up_right_after_forget_waits_for_the_old_teardown() {
    let rig = rig(true).await;
    let first = row("rec", &recording_spec("bring_up", 1), json!({}), 1);
    rig.rows.lock().unwrap().insert("rec".into(), first.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(first).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
        .await
        .unwrap();

    weft_listener::kinds::forget(&rig.state, "rec");
    let second = row("rec", &recording_spec("bring_up", 2), json!({}), 2);
    rig.rows.lock().unwrap().insert("rec".into(), second.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(second).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
        .await
        .unwrap();
    assert_eq!(recorded("bring_up"), ["spawn 1", "unregister 1", "spawn 2"]);
    assert!(rig.state.registry.get("rec").is_some(), "the new connection runs");
}

/// A held signal registered again in a process that already holds it is
/// taken again and comes up as its row now reads, the displaced one torn
/// down first.
#[tokio::test]
async fn a_held_signal_registered_again_here_comes_up_anew() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("rec".into(), row("rec", &recording_spec("again", 1), json!({}), 1));
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    assert_eq!(recorded("again"), ["spawn 1"]);

    rig.rows.lock().unwrap().get_mut("rec").unwrap()["spec_json"] = json!(serde_json::to_string(&recording_spec("again", 2)).unwrap());
    weft_listener::hold::take_now(&rig.state, &["rec".to_string()], StartMode::New).await.unwrap();
    assert_eq!(recorded("again"), ["spawn 1", "unregister 1", "spawn 2"]);
    assert_eq!(rig.state.registry.get("rec").unwrap().spec.config["n"], 2);
}

/// A held signal that went to another holder stops here and leaves what
/// it arranged outside to that holder; one whose row is gone stops and
/// tears that down.
#[tokio::test]
async fn a_lost_claim_stops_here_and_an_ended_signal_tears_down() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("rec".into(), row("rec", &recording_spec("lost", 1), json!({}), 1));
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();

    rig.rows.lock().unwrap().get_mut("rec").unwrap()["held_by"] = json!("another-holder");
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    assert!(rig.state.registry.get("rec").is_none(), "stopped here");

    rig.rows.lock().unwrap().get_mut("rec").unwrap()["held_by"] = Value::Null;
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    assert_eq!(recorded("lost"), ["spawn 1", "spawn 1"], "no teardown for a lost claim");

    rig.rows.lock().unwrap().remove("rec");
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    wait_for(|| recorded("lost").len() == 3).await;
    assert_eq!(recorded("lost")[2], "unregister 1");
    assert!(rig.state.registry.get("rec").is_none());
}

/// A holder told to stop finishes its look, stops its connections before
/// it gives up their claims, and leaves what they arranged outside to the
/// next holder.
#[tokio::test]
async fn letting_go_stops_the_connections_first_and_tears_nothing_down() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("rec".into(), row("rec", &recording_spec("let_go", 1), json!({}), 1));
    weft_listener::hold::run(rig.state.clone(), None, std::future::ready(())).await;
    assert!(rig.state.registry.get("rec").is_none());
    assert!(rig.rows.lock().unwrap()["rec"]["held_by"].is_null(), "the claim is given up");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(recorded("let_go"), ["spawn 1"]);
}

/// A wake left from a row that woke, for a row registered again to be held,
/// ends quietly instead of failing every retry of the alarm.
#[tokio::test]
async fn a_wake_for_a_signal_that_no_longer_wakes_ends_quietly() {
    let rig = rig(false).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    wake(&rig.state, due("sse", now_ms())).await.unwrap();
    assert!(rig.doors.fired.lock().unwrap().is_empty());
    assert!(rig.alarm.wakes_for("signal:sse").is_empty());
}

async fn wait_for(mut done: impl FnMut() -> bool) {
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the detached teardown never ran");
}

// ---------- The `/wake` door ----------

/// Serve the listener's real router on a local port; answers its base URL.
async fn serve(state: ListenerState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, weft_listener::router(state)).await.unwrap() });
    format!("http://{addr}")
}

/// Every log line written while the guard lives, as text.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A body that can never be a wake is answered 200 (so a retrying queue
/// like Cloud Tasks stops), says it was dropped and why, and is logged
/// as an error carrying the body.
#[tokio::test]
async fn a_wake_body_that_can_never_be_read_is_dropped_with_a_success_and_logged() {
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt().with_ansi(false).with_writer(move || writer.clone()).finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let rig = rig(false).await;
    let base = serve(rig.state.clone()).await;
    let http = reqwest::Client::new();
    for body in ["not json at all", r#"{"at_unix_ms": 1, "body": {"tok": "x"}}"#] {
        let answer = http.post(format!("{base}/wake")).body(body).send().await.unwrap();
        assert_eq!(answer.status(), 200, "{body}");
        let said: Value = answer.json().await.unwrap();
        assert_eq!(said["dropped"], true, "{said}");
        assert!(said["reason"].as_str().unwrap().contains("not a wake call"), "{said}");
    }
    let written = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(written.contains("ERROR") && written.contains("not json at all"), "{written}");
    assert!(rig.doors.fired.lock().unwrap().is_empty());
}

/// Post `call` to the listener's `/wake` at `base` as the alarm's
/// delivery that had `answered` answered ones before it, and no others.
async fn post_wake(base: &str, call: &Value, answered: u32) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}/wake"))
        .header(FakeAlarm::ANSWERED_HEADER, answered)
        .header(FakeAlarm::TRIED_HEADER, answered)
        .json(call)
        .send()
        .await
        .unwrap()
}

/// A readable wake whose processing fails answers 424, so the alarm tries
/// it again and counts the try (a 5xx it would not count); on its last
/// delivery it gives its signal up and answers 200 "dropped". A call that
/// does not say which delivery it is did not come from the alarm, and is
/// refused with a 400 naming the header.
#[tokio::test]
async fn a_readable_wake_that_fails_answers_424_for_a_retry_until_its_last_delivery() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    register(&rig, "tok", &to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } }), asked).await.unwrap();
    rig.doors.failing.store(true, std::sync::atomic::Ordering::SeqCst);
    rig.queue.down.store(true, std::sync::atomic::Ordering::SeqCst);
    let base = serve(rig.state.clone()).await;
    let call = json!({ "at_unix_ms": now_ms(), "body": { "token": "tok", "due_at_ms": asked + 1_000, "generation": 1 } });

    assert_eq!(post_wake(&base, &call, 0).await.status(), weft_platform_traits::alarm::WAKE_FAILED_STATUS);
    let answer = post_wake(&base, &call, FakeAlarm::ATTEMPTS - 1).await;
    assert_eq!(answer.status(), 200);
    let said: Value = answer.json().await.unwrap();
    assert_eq!(said["dropped"], true, "{said}");
    assert!(rig.rows.lock().unwrap()["tok"]["held_failed"].is_string(), "the row says why");

    let unsaid = reqwest::Client::new().post(format!("{base}/wake")).json(&call).send().await.unwrap();
    assert_eq!(unsaid.status(), 400);
    assert!(unsaid.text().await.unwrap().contains(FakeAlarm::ANSWERED_HEADER));
}

/// A wake whose row can never be acted on (a state its kind cannot read)
/// gives the signal up, written once on its row, and is answered 200
/// "dropped", so the alarm stops; a wake for a registration the row no
/// longer is does nothing.
#[tokio::test]
async fn a_wake_that_can_never_be_handled_gives_its_signal_up_and_is_dropped() {
    let rig = rig(false).await;
    let spec = job_wait("http://127.0.0.1:1/status");
    rig.rows.lock().unwrap().insert("poll".into(), row("poll", &spec, json!({ "not_a_poll_state": true }), 1));
    let base = serve(rig.state.clone()).await;
    let call = |generation: i64| json!({ "at_unix_ms": now_ms(), "body": { "token": "poll", "due_at_ms": now_ms(), "generation": generation } });

    let answer = post_wake(&base, &call(7), 0).await;
    assert_eq!(answer.status(), 204, "another registration's wake does nothing");
    assert!(rig.rows.lock().unwrap()["poll"]["held_failed"].is_null());

    let answer = post_wake(&base, &call(1), 0).await;
    assert_eq!(answer.status(), 200);
    let said: Value = answer.json().await.unwrap();
    assert_eq!(said["dropped"], true, "{said}");
    let given_up = rig.rows.lock().unwrap()["poll"]["held_failed"].clone();
    assert!(given_up.as_str().unwrap().contains("poll_endpoint state"), "{given_up}");
    assert!(rig.alarm.wakes_for("signal:poll").is_empty(), "nothing more is set");
    let live = reqwest::Client::new().post(format!("{base}/live")).json(&json!({ "token": "poll" })).send().await.unwrap();
    let shown = live.text().await.unwrap();
    assert!(shown.contains("stopped:"), "its node says why: {shown}");
}

/// A feed that answers the same page every time.
async fn spawn_steady_feed() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route("/feed", axum::routing::get(|| async { axum::Json(json!({ "items": [{ "id": 7 }] })) }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/feed")
}

fn delta_poll(url: &str) -> weft_core::primitive::SignalSpec {
    to_spec(serde_json::from_value::<weft_core::signal::PollEndpoint>(json!({ "url": url, "interval_secs": 60, "delta": { "items": "items", "cursor_field": "id" } })).unwrap())
}

/// A poll that finds nothing new, handled by a listener that just
/// started, asks the broker nothing: its row and where its address is
/// ride its wake, and it writes nothing.
#[tokio::test]
async fn an_empty_poll_with_its_state_in_the_wake_makes_no_broker_call() {
    let rig = rig(false).await;
    register(&rig, "poll", &delta_poll(&spawn_steady_feed().await), now_ms()).await.unwrap();
    wake(&rig.state, last_wake(&rig, "poll")).await.unwrap();
    assert_eq!(rig.rows.lock().unwrap()["poll"]["kind_state"], json!({ "delta": { "cursor": 7 } }), "the first poll primed");
    let (reads, asked, seq) = (
        rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst),
        rig.asked.load(std::sync::atomic::Ordering::SeqCst),
        rig.rows.lock().unwrap()["poll"]["kind_state_seq"].clone(),
    );

    let copy = fresh_listener(&rig);
    wake(&copy, last_wake(&rig, "poll")).await.unwrap();
    assert_eq!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), reads, "no row read");
    assert_eq!(rig.asked.load(std::sync::atomic::Ordering::SeqCst), asked, "no address asked");
    assert_eq!(rig.rows.lock().unwrap()["poll"]["kind_state_seq"], seq, "nothing written");
    assert!(rig.doors.fired.lock().unwrap().is_empty());
    assert_eq!(rig.alarm.wakes_for("signal:poll").len(), 3, "and the next poll is set");
}

/// A poll wake that wrote its row and then failed is delivered again
/// (`again`) carrying the row as it was before the write. That delivery
/// reads the row instead, so it does not lose its claim to its own
/// earlier write, and it sets the next wake.
async fn a_poll_wake_delivered_again_after_it_wrote(again: weft_platform_traits::Delivery) {
    use weft_listener::kinds::wake_delivered;
    let feed = spawn_feed(Arc::new(std::sync::atomic::AtomicBool::new(true))).await;
    let rig = rig(false).await;
    register(&rig, "poll", &delta_poll(&feed), now_ms()).await.unwrap();
    let carried = last_wake(&rig, "poll");
    assert!(carried.row.is_some(), "the wake carries its row");

    rig.alarm.fail_sets(true);
    wake_delivered(&rig.state, carried.clone(), FakeAlarm::delivered(1)).await.expect_err("the next wake could not be set");
    assert_eq!(rig.rows.lock().unwrap()["poll"]["kind_state_seq"], 2, "the failing poll was written");

    rig.alarm.fail_sets(false);
    let (set, reads) = (rig.alarm.wakes_for("signal:poll").len(), rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(wake_delivered(&rig.state, carried, again).await.unwrap(), None);
    assert_eq!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), reads + 1, "it read the row");
    assert_eq!(rig.alarm.wakes_for("signal:poll").len(), set + 1, "and set the next wake");
}

/// The listener answered the failure, so the next delivery is its second
/// try.
#[tokio::test]
async fn a_poll_wake_delivered_again_after_an_answered_failure_reads_its_row_and_sets_the_next() {
    a_poll_wake_delivered_again_after_it_wrote(FakeAlarm::delivered(2)).await;
}

/// The listener answered nothing (it died mid-wake, or ran past the
/// alarm's deadline), so the next delivery is still its first try, and
/// still a redelivery.
#[tokio::test]
async fn a_poll_wake_delivered_again_after_an_unanswered_try_reads_its_row_and_sets_the_next() {
    a_poll_wake_delivered_again_after_it_wrote(FakeAlarm::after_unanswered(1)).await;
}

/// A poll whose state is too big to ride its wake leaves it on the row,
/// which the wake reads.
#[tokio::test]
async fn a_poll_state_too_big_for_its_wake_stays_on_the_row() {
    let rig = rig(false).await;
    let big = json!({ "delta": { "cursor": "x".repeat(70_000) } });
    let written = row("poll", &delta_poll("http://127.0.0.1:1/feed"), big, 1);
    rig.rows.lock().unwrap().insert("poll".into(), written.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(written).unwrap(), StartMode::New).await.unwrap();
    let body = last_wake(&rig, "poll");
    assert!(body.row.is_none(), "too big to carry");
    assert_eq!(body.generation, Some(1), "it still names its registration");
}

/// A poll whose change could not be written does not carry a state the
/// row does not have: its next wake reads the row, and writes the change
/// then.
///
/// The feed answers 500 at once. An address nothing listens on would
/// not do: where the host drops the connect instead of refusing it (WSL
/// in mirrored mode), each poll waits out the client's 10 s connect
/// bound, and a wake handled past the next minute reads its row once
/// more (`on_time_or_still_registered`), so the read count is off.
#[tokio::test]
async fn a_poll_whose_write_failed_reads_its_row_next() {
    let rig = rig(false).await;
    let feed = spawn_feed(Arc::new(std::sync::atomic::AtomicBool::new(true))).await;
    register(&rig, "poll", &delta_poll(&feed), now_ms()).await.unwrap();
    let seq = rig.rows.lock().unwrap()["poll"]["kind_state_seq"].clone();

    rig.get_held.failing_writes.store(1, std::sync::atomic::Ordering::SeqCst);
    wake(&rig.state, last_wake(&rig, "poll")).await.unwrap();
    assert_eq!(rig.rows.lock().unwrap()["poll"]["kind_state_seq"], seq, "the first failure could not be written");
    let next = last_wake(&rig, "poll");
    assert!(next.row.is_none(), "the next wake reads the row");

    let reads = rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst);
    wake(&rig.state, next).await.unwrap();
    assert_eq!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), reads + 1, "it read the row");
    assert_eq!(rig.rows.lock().unwrap()["poll"]["kind_state"]["consecutive_failures"], 1, "and wrote the failure it found");
    assert!(last_wake(&rig, "poll").row.is_some(), "written, the state rides the wake again");
}

/// A poll registered again carries its new spec in its new wakes, and the
/// start cancels the wakes the row it replaced had set; an unregister
/// cancels the new one.
#[tokio::test]
async fn a_poll_registered_again_carries_its_new_spec_and_its_old_wakes_are_cancelled() {
    let rig = rig(false).await;
    let (old, new) = (delta_poll("http://127.0.0.1:1/old"), delta_poll("http://127.0.0.1:1/new"));
    register(&rig, "poll", &old, now_ms()).await.unwrap();
    let first = rig.alarm.wakes_for("signal:poll").pop().unwrap();

    let mut again = row("poll", &new, json!({}), 2);
    again["generation"] = json!(2);
    rig.rows.lock().unwrap().insert("poll".into(), again);
    let base = serve(rig.state.clone()).await;
    let start = json!({ "token": "poll", "mode": "new", "replaced": { "spec": old, "kind_state": {}, "generation": 1 } });
    let answer = reqwest::Client::new().post(format!("{base}/start")).json(&start).send().await.unwrap();
    assert_eq!(answer.status(), 204, "{}", answer.text().await.unwrap());

    assert!(rig.alarm.cancelled().iter().any(|w| w.name() == first.name()), "the old registration's wake is cancelled");
    let carried = last_wake(&rig, "poll");
    assert_eq!(carried.generation, Some(2));
    assert!(carried.row.unwrap().spec_json.contains("/new"), "the new wake polls the new spec");

    let latest = rig.alarm.wakes_for("signal:poll").pop().unwrap();
    let removed = weft_core::signal::listener_protocol::UnregisterRequest {
        token: "poll".into(),
        tenant_id: "tenant-a".into(),
        spec: new,
        kind_state: json!({}),
        generation: 2,
        reused: false,
    };
    weft_listener::kinds::unregister(&rig.state, removed).await.unwrap();
    assert!(rig.alarm.cancelled().iter().any(|w| w.name() == latest.name()), "an unregister cancels its wake");
}

/// A held connection to an address that is no infra endpoint asks where it
/// is once, and its reconnects ask the broker nothing; one to an infra
/// endpoint asks again after each failed connect. What it says changes
/// once as it drops, not at every attempt.
#[tokio::test]
async fn reconnects_ask_the_broker_only_where_an_infra_endpoint_is() {
    // A port nothing listens on, which refuses at once.
    let closed = {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        taken.local_addr().unwrap().port()
    };
    let reconnect = |url: String| async move {
        let rig = rig(true).await;
        let written = row("sse", &sse_at(&url), json!({}), 1);
        rig.rows.lock().unwrap().insert("sse".into(), written.clone());
        weft_listener::registry::hold(&rig.state, serde_json::from_value(written).unwrap(), StartMode::New).await.unwrap();
        let reads = rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let said = rig.state.registry.get("sse").unwrap().serving.lock().status.clone();
        assert!(said.starts_with("reconnecting since"), "{said}");
        // Four tries: at once, then after 1, 2 and 4 seconds.
        tokio::time::sleep(std::time::Duration::from_millis(7_500)).await;
        assert_eq!(rig.state.registry.get("sse").unwrap().serving.lock().status, said, "said once, not per attempt");
        assert_eq!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst), reads, "{url}: no row read");
        rig.asked.load(std::sync::atomic::Ordering::SeqCst)
    };
    let (public, infra) =
        tokio::join!(reconnect(format!("http://127.0.0.1:{closed}/events")), reconnect(format!("http://wi-feed:{closed}/events")));
    assert_eq!(public, 1, "asked once, whatever the reconnects");
    assert!(infra >= 3, "asked again after each failed connect: {infra}");
}

/// The project's worker, as the listener hands it events: records each
/// one, and, while `failing`, cannot be reached.
#[derive(Default)]
struct FakeDoors {
    fired: Mutex<Vec<weft_core::door_fire::DoorFire>>,
    failing: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl weft_listener::fire_sink::WorkerDoors for FakeDoors {
    async fn fire(&self, _: uuid::Uuid, address: &str, fire: &weft_core::door_fire::DoorFire) -> anyhow::Result<weft_core::door_fire::Fired> {
        if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("no worker answers at {address}");
        }
        self.fired.lock().unwrap().push(fire.clone());
        Ok(weft_core::door_fire::Fired::Started { execution_id: weft_core::door_fire::run_of_fire(fire.fire_id) })
    }
}
