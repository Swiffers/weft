//! The local alarm against a real Postgres: a wake set before a restart
//! is delivered after it, a wake the receiver refuses comes back counting
//! the earlier deliveries it answered (and only those) apart from all
//! those tried, the same wake set twice is delivered once, and a
//! cancelled one never is.
#![cfg(feature = "db-tests")]

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use weft_platform_local::{Deliver, EarlierDeliveries, LocalAlarm, NotTaken};
use weft_platform_traits::{Alarm, CoreRole, Wake, WakeCall, WakeQueue};

#[derive(Default)]
struct Recorder {
    got: parking_lot::Mutex<Vec<(CoreRole, String, WakeCall<serde_json::Value>)>>,
    /// The earlier deliveries each delivery said its wake had.
    earlier: parking_lot::Mutex<Vec<EarlierDeliveries>>,
    /// Fail each delivery for now: as the receiver's own answer (`true`,
    /// "424"), or as one that never reached it (`false`).
    refuse: parking_lot::Mutex<Option<bool>>,
}

#[async_trait]
impl Deliver for Recorder {
    async fn deliver(&self, role: CoreRole, path: &str, call: WakeCall<serde_json::Value>, earlier: EarlierDeliveries) -> Result<(), NotTaken> {
        self.earlier.lock().push(earlier);
        match *self.refuse.lock() {
            Some(true) => return Err(NotTaken::answered_with(424, "failed".into())),
            Some(false) => return Err(NotTaken::unanswered("unreachable")),
            None => {}
        }
        self.got.lock().push((role, path.to_string(), call));
        Ok(())
    }
}

async fn schema(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&weft_task_store::alarm::GROUP]).await.unwrap();
}

fn wake(at: i64) -> Wake {
    Wake {
        key: "signal:t".into(),
        at_unix_ms: at,
        generation: 1,
        queue: WakeQueue::Triggers,
        role: CoreRole::Listener,
        path: "/wake".into(),
        body: serde_json::json!({ "token": "t" }),
    }
}

fn past() -> i64 {
    now_ms() - 1_000
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}

#[sqlx::test]
async fn a_wake_set_before_a_restart_is_delivered_after_it_once(pool: PgPool) {
    schema(&pool).await;
    let at = past();
    LocalAlarm::new(pool.clone()).set(wake(at)).await.unwrap();
    LocalAlarm::new(pool.clone()).set(wake(at)).await.unwrap();

    // A new process: nothing in memory, the row is all there is.
    let recorder = Arc::new(Recorder::default());
    let deliver: Arc<dyn Deliver> = recorder.clone();
    let after_restart = LocalAlarm::new(pool.clone());
    assert_eq!(after_restart.deliver_due(&deliver).await.unwrap(), 1, "the same wake set twice is one wake");
    let got = recorder.got.lock().clone();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, CoreRole::Listener);
    assert_eq!(got[0].1, "/wake");
    assert_eq!(got[0].2.at_unix_ms, at, "the receiver hears the moment it was set for");
    assert_eq!(after_restart.deliver_due(&deliver).await.unwrap(), 0, "delivered wakes are gone");
}

#[sqlx::test]
async fn a_refused_wake_comes_back_later(pool: PgPool) {
    schema(&pool).await;
    let alarm = LocalAlarm::new(pool.clone());
    alarm.set(wake(past())).await.unwrap();
    let recorder = Arc::new(Recorder::default());
    *recorder.refuse.lock() = Some(true);
    let deliver: Arc<dyn Deliver> = recorder.clone();
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 1);
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 0, "backing off, not due again at once");
    let (at, attempts, answered): (i64, i32, i32) =
        sqlx::query_as("SELECT at_unix_ms, attempts, answered FROM alarm").fetch_one(&pool).await.unwrap();
    assert!(at > now_ms(), "set to try again later");
    assert_eq!((attempts, answered), (1, 1));

    sqlx::query("UPDATE alarm SET at_unix_ms = 0").execute(&pool).await.unwrap();
    *recorder.refuse.lock() = None;
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 1);
    assert_eq!(recorder.got.lock().len(), 1, "taken on the retry");
    assert_eq!(*recorder.earlier.lock(), vec![earlier(0, 0), earlier(1, 1)], "each delivery says how many came before it");
}

/// A delivery that never reached the receiver backs off like any other,
/// but spends none of the receiver's tries: after an outage, the first
/// delivery it gets is still its first.
#[sqlx::test]
async fn a_delivery_that_never_reached_the_receiver_is_not_counted(pool: PgPool) {
    schema(&pool).await;
    let alarm = LocalAlarm::new(pool.clone());
    alarm.set(wake(past())).await.unwrap();
    let recorder = Arc::new(Recorder::default());
    *recorder.refuse.lock() = Some(false);
    let deliver: Arc<dyn Deliver> = recorder.clone();
    for _ in 0..3 {
        assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 1);
        sqlx::query("UPDATE alarm SET at_unix_ms = 0").execute(&pool).await.unwrap();
    }
    let (attempts, answered): (i32, i32) = sqlx::query_as("SELECT attempts, answered FROM alarm").fetch_one(&pool).await.unwrap();
    assert_eq!((attempts, answered), (3, 0), "backs off by every try, counts none");

    *recorder.refuse.lock() = None;
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 1);
    assert_eq!(*recorder.earlier.lock(), vec![earlier(0, 0), earlier(0, 1), earlier(0, 2), earlier(0, 3)], "tried, never answered");
}

/// A delivery whose process died holding the claim (no answer, no
/// retry written) comes due again when the lease runs out, and says it
/// was tried before though none of the receiver's tries is spent.
#[sqlx::test]
async fn a_delivery_after_one_whose_process_died_says_it_was_tried_before(pool: PgPool) {
    schema(&pool).await;
    let alarm = LocalAlarm::new(pool.clone());
    alarm.set(wake(past())).await.unwrap();
    let died = weft_task_store::alarm::claim_due(&pool, now_ms(), 60_000, 10).await.unwrap();
    assert_eq!(died.len(), 1, "claimed by a process that then died");
    sqlx::query("UPDATE alarm SET at_unix_ms = 0").execute(&pool).await.unwrap();

    let recorder = Arc::new(Recorder::default());
    let deliver: Arc<dyn Deliver> = recorder.clone();
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 1);
    assert_eq!(*recorder.earlier.lock(), vec![earlier(0, 1)]);
}

fn earlier(answered: u32, tried: u32) -> EarlierDeliveries {
    EarlierDeliveries { answered, tried }
}

/// A cancelled wake is never delivered, and cancelling one already gone
/// is no failure.
#[sqlx::test]
async fn a_cancelled_wake_is_not_delivered(pool: PgPool) {
    schema(&pool).await;
    let alarm = LocalAlarm::new(pool.clone());
    let w = wake(past());
    alarm.set(w.clone()).await.unwrap();
    alarm.cancel(&w).await.unwrap();
    alarm.cancel(&w).await.expect("cancelling a wake already gone");
    let recorder = Arc::new(Recorder::default());
    let deliver: Arc<dyn Deliver> = recorder.clone();
    assert_eq!(alarm.deliver_due(&deliver).await.unwrap(), 0);
    assert!(recorder.got.lock().is_empty());
}
