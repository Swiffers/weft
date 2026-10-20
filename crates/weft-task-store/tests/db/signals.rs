//! Layer-3 tests for the signals that wake waiters, against a REAL
//! Postgres: what matters is that a notification goes out exactly when a
//! write commits (never for one that rolls back), that the triggers fire
//! on exactly the writes someone waits on, and that a waiter hears about
//! a lost connection. A faked store could tell none of that.

use crate::support;

use std::time::Duration;

use serde_json::json;
use sqlx::PgPool;

use weft_task_store::pg_signal::{Heard, Subscription};
use weft_task_store::tasks::{self, TASK_READY_CHANNEL};

use support::{claimed_task, setup, signals};

/// Long enough for a notification that was sent to arrive on a loaded
/// machine, so "nothing arrived" means nothing was sent.
const QUIET: Duration = Duration::from_millis(700);

fn task(dedup: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: weft_task_store::TaskKind::RegisterSignal,
        project_id: None,
        dedup_key: Some(dedup.to_string()),
        run_id: None,
        tenant_id: "tenant-1".to_string(),
        payload: json!({}),
    }
}

/// Everything heard until the line goes quiet.
async fn drain(subscription: &mut Subscription) -> Vec<Heard> {
    let mut heard = Vec::new();
    while let Ok(next) = tokio::time::timeout(QUIET, subscription.next()).await {
        heard.push(next.expect("the watch is running"));
    }
    heard
}

fn on(heard: &[Heard], channel: &str) -> Vec<String> {
    heard
        .iter()
        .filter_map(|h| match h {
            Heard::Signal { channel: c, payload } if *c == channel => Some(payload.to_string()),
            _ => None,
        })
        .collect()
}

#[sqlx::test]
async fn a_committed_notification_is_heard_and_a_rolled_back_one_never_is(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_notify($1, 'rolled back')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_notify($1, 'committed')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec!["committed".to_string()]);
}

/// A dropped listening connection loses whatever was sent meanwhile, so
/// every waiter is told it is lost, and to look again once it is back;
/// whether it listens can be read in between.
#[sqlx::test]
async fn a_lost_connection_tells_every_waiter_to_recheck(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();
    let killed: Vec<(bool,)> = sqlx::query_as(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND application_name = $1",
    )
    .bind(weft_task_store::pg_signal::WATCH_APPLICATION_NAME)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(killed.len(), 1, "exactly one listening connection per watch");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    assert!(!heard.listening(), "not listening while it is lost");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Recheck);
    assert!(heard.listening(), "listening again");
}

/// Cut every listening connection of the test database, as a database
/// going to sleep does.
async fn cut_the_listener(pool: &PgPool) {
    let killed: Vec<(bool,)> = sqlx::query_as(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = current_database() AND application_name = $1",
    )
    .bind(weft_task_store::pg_signal::WATCH_APPLICATION_NAME)
    .fetch_all(pool)
    .await
    .unwrap();
    assert_eq!(killed.len(), 1, "exactly one listening connection per watch");
}

/// A process that scales to zero listens only while it has work: a
/// connection that drops while nothing is busy (the database going to
/// sleep) stays down, so listening does not wake the database straight
/// back up, until work arrives, which waits for it to listen again and is
/// told it resumed; a drop while something is busy reconnects at once.
#[sqlx::test]
async fn a_quiet_watch_listens_again_only_when_work_arrives(pool: PgPool) {
    setup(&pool).await;
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(
        &pool.connect_options(),
        support::CHANNELS,
        weft_task_store::pg_signal::Listening::WhileBusy,
    )
    .await
    .unwrap();
    let mut heard = watch.subscribe();
    // Past the moment just after work, when a drop still counts as busy.
    tokio::time::sleep(Duration::from_secs(6)).await;
    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    assert!(tokio::time::timeout(Duration::from_secs(3), heard.next()).await.is_err(), "nothing while quiet");
    assert!(!heard.listening());

    let busy = watch.busy().await;
    assert!(heard.listening(), "work waits for the watch to listen");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Resumed);

    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Recheck, "busy: listening again at once");
    drop(busy);
}

/// A TCP proxy in front of the test database that counts the connections
/// made through it, and refuses them (closes at once) while `refusing`.
struct CountingProxy {
    port: u16,
    accepted: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    refusing: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

async fn counting_proxy(pool: &PgPool) -> CountingProxy {
    use std::sync::atomic::Ordering;
    let options = pool.connect_options();
    let target = format!("{}:{}", options.get_host(), options.get_port());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = CountingProxy { port, accepted: Default::default(), refusing: Default::default() };
    let (accepted, refusing) = (proxy.accepted.clone(), proxy.refusing.clone());
    tokio::spawn(async move {
        loop {
            let (mut inbound, _) = listener.accept().await.unwrap();
            accepted.fetch_add(1, Ordering::SeqCst);
            if refusing.load(Ordering::SeqCst) {
                continue;
            }
            let target = target.clone();
            tokio::spawn(async move {
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    proxy
}

/// A watch listening through `proxy`.
async fn watch_through(pool: &PgPool, proxy: &CountingProxy) -> std::sync::Arc<weft_task_store::pg_signal::PgSignalWatch> {
    let options = (*pool.connect_options()).clone().host("127.0.0.1").port(proxy.port);
    weft_task_store::pg_signal::PgSignalWatch::start(&options, support::CHANNELS, weft_task_store::pg_signal::Listening::WhileBusy).await.unwrap()
}

/// A connection the database closes while nothing is busy is not made
/// again behind weft's back: the listener does not reconnect on its own,
/// so a sleeping database is not woken by it.
#[sqlx::test]
async fn a_quiet_watch_opens_no_connection_when_its_own_drops(pool: PgPool) {
    use std::sync::atomic::Ordering;
    setup(&pool).await;
    let proxy = counting_proxy(&pool).await;
    let watch = watch_through(&pool, &proxy).await;
    let mut heard = watch.subscribe();
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_secs(6)).await;
    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 1, "no connection made while nothing is busy");
}

/// A watch that cannot listen again while something is busy keeps trying,
/// and stops once nothing is busy any more, so an unreachable or sleeping
/// database is not tried every second for nothing; work arriving later
/// makes it listen again, and, since the connection was lost while this
/// process was busy writing, every waiter rechecks.
#[sqlx::test]
async fn a_watch_stops_retrying_once_its_work_ends(pool: PgPool) {
    use std::sync::atomic::Ordering;
    setup(&pool).await;
    let proxy = counting_proxy(&pool).await;
    let watch = watch_through(&pool, &proxy).await;
    let mut heard = watch.subscribe();
    let busy = watch.busy().await;
    proxy.refusing.store(true, Ordering::SeqCst);
    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(proxy.accepted.load(Ordering::SeqCst) > 2, "busy: trying again");
    drop(busy);
    tokio::time::sleep(Duration::from_secs(8)).await;
    let tried = proxy.accepted.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), tried, "nothing busy: no more tries");
    proxy.refusing.store(false, Ordering::SeqCst);
    let _busy = watch.busy().await;
    assert!(heard.listening(), "work makes it listen again");
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Recheck, "its own writes while it could not listen went unheard");
}

/// Work that went ahead while a quiet watch could not listen again wrote
/// unheard, so the watch, once it listens, has every waiter recheck.
#[sqlx::test]
async fn work_that_went_ahead_unheard_makes_the_watch_recheck(pool: PgPool) {
    use std::sync::atomic::Ordering;
    setup(&pool).await;
    let proxy = counting_proxy(&pool).await;
    let watch = watch_through(&pool, &proxy).await;
    let mut heard = watch.subscribe();
    tokio::time::sleep(Duration::from_secs(6)).await;
    proxy.refusing.store(true, Ordering::SeqCst);
    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    let busy = watch.busy().await;
    assert!(!heard.listening(), "the work went ahead without it");
    proxy.refusing.store(false, Ordering::SeqCst);
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Recheck);
    drop(busy);
}

/// A task announces itself when it becomes claimable, and only then:
/// claims, heartbeats and completions are the hot path and stay silent,
/// while a requeue is loud.
#[sqlx::test]
async fn a_task_is_announced_exactly_when_it_becomes_claimable(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();

    let work = tasks::enqueue(&pool, task("d")).await.unwrap();
    assert!(!on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "new work wakes the pickers");

    assert_eq!(claimed_task(&pool, "disp-1").await.unwrap().expect("claimed").id, work);
    tasks::heartbeat(&pool, work, "disp-1").await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "claim and heartbeat are silent");

    assert!(tasks::surrender(&pool, work, "disp-1").await.unwrap());
    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec![String::new()]);

    assert_eq!(claimed_task(&pool, "disp-1").await.unwrap().expect("claimed").id, work);
    tasks::complete(&pool, work, "disp-1", json!(1)).await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "completing is silent");
}

/// What a write leaves in the outbox goes out once it commits and its
/// writer pokes the flusher, each announcement once however many times it
/// was left; what a rolled-back write left never does.
#[sqlx::test]
async fn an_announcement_goes_out_once_its_write_commits(pool: PgPool) {
    setup(&pool).await;
    let watch = signals(&pool).await;
    let mut heard = watch.subscribe();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("SELECT weft_announce($1, 'rolled back')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    tx.rollback().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    for _ in 0..3 {
        sqlx::query("SELECT weft_announce($1, 'committed')").bind(TASK_READY_CHANNEL).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert!(on(&drain(&mut heard).await, TASK_READY_CHANNEL).is_empty(), "nothing goes out before the flusher is poked");
    weft_task_store::announce::committed(&pool);
    assert_eq!(on(&drain(&mut heard).await, TASK_READY_CHANNEL), vec!["committed".to_string()]);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM weft_announcement").fetch_one(&pool).await.unwrap();
    assert_eq!(left, 0, "the outbox is empty once sent");
}

/// What a process hears another way (the install's news) reaches its
/// waiters while its watch is quiet and the database asleep, without the
/// watch listening again.
#[sqlx::test]
async fn what_is_told_another_way_reaches_waiters_while_the_watch_is_quiet(pool: PgPool) {
    setup(&pool).await;
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(
        &pool.connect_options(),
        support::CHANNELS,
        weft_task_store::pg_signal::Listening::WhileBusy,
    )
    .await
    .unwrap();
    let mut heard = watch.subscribe();
    tokio::time::sleep(Duration::from_secs(6)).await;
    cut_the_listener(&pool).await;
    let next = tokio::time::timeout(Duration::from_secs(20), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Lost);
    let told = watch.also_told();
    assert!(told.signal(TASK_READY_CHANNEL, "x").unwrap());
    let next = tokio::time::timeout(Duration::from_secs(5), heard.next()).await.expect("heard in time");
    assert_eq!(next.unwrap(), Heard::Signal { channel: TASK_READY_CHANNEL, payload: "x".into() });
    assert!(!heard.listening(), "the watch stayed quiet");
}

/// A request holds its process's watch busy while it is answered, unless
/// its handler let go of it: a call that then waits for hours holds
/// nothing that would keep the database awake.
#[sqlx::test]
async fn a_request_that_let_go_of_its_listening_holds_the_watch_no_longer(pool: PgPool) {
    use std::sync::Arc;
    use axum::extract::Extension;
    use axum::routing::get;
    use tower::ServiceExt as _;
    use weft_task_store::pg_signal::{keep_listening, PgSignalWatch, RequestListening};
    setup(&pool).await;
    let watch = PgSignalWatch::start(&pool.connect_options(), support::CHANNELS, weft_task_store::pg_signal::Listening::WhileBusy).await.unwrap();
    let (waiting, release) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
    let handler = |lets_go: bool| {
        let (waiting, release) = (waiting.clone(), release.clone());
        move |Extension(listening): Extension<RequestListening>| {
            let (waiting, release) = (waiting.clone(), release.clone());
            async move {
                if lets_go {
                    listening.let_go();
                }
                waiting.notify_one();
                release.notified().await;
                "done"
            }
        }
    };
    let app = axum::Router::new()
        .route("/holds", get(handler(false)))
        .route("/lets-go", get(handler(true)))
        .layer(axum::middleware::from_fn_with_state(watch.clone(), keep_listening));
    for (path, holds) in [("/holds", true), ("/lets-go", false)] {
        let request = axum::http::Request::get(path).body(axum::body::Body::empty()).unwrap();
        let call = tokio::spawn(app.clone().oneshot(request));
        waiting.notified().await;
        assert_eq!(watch.holds(), holds, "{path}");
        release.notify_one();
        let answer = call.await.unwrap().unwrap();
        axum::body::to_bytes(answer.into_body(), usize::MAX).await.unwrap();
    }
}
