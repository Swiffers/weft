//! Saying that the database holds weft back (`weft_task_store::throttle`):
//! a request that waited for a connection to come free counts, whether
//! it got one late or gave up, never a pool merely in use at its size.

use std::time::Duration;

use weft_core::install::DatabaseHolder;
use weft_task_store::budget::Capacity;
use weft_task_store::db::{self, Client as _, NoConnection, Pool, Waits};
use weft_task_store::throttle;

/// The stretch written for `pool`'s role, once the write that runs beside
/// the request that waited has landed.
async fn written(pool: &Pool) -> Vec<throttle::Stretch> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let stretches = throttle::current(pool).await.unwrap();
        if !stretches.is_empty() {
            return stretches;
        }
        assert!(tokio::time::Instant::now() < deadline, "the wait was never written down");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A request that waited longer than `db::SLOW_WAIT` for the one
/// connection of a pool, already open and handed out, and then got it is
/// a wait the user felt: the pool's own measure of it counts it, and it is
/// written down. A lock pool's waits are its holders waiting for each
/// other, and say nothing.
#[weft_db_macros::test]
async fn a_request_that_waited_and_got_through_is_written_down(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&throttle::GROUP]).await.expect("throttle schema");
    let capacity = Capacity { max_connections: 100, reserved: 3, limit: None };
    throttle::record_into(pool.clone(), DatabaseHolder::Machine, &capacity, 1);
    let held_for = db::SLOW_WAIT + Duration::from_millis(500);

    let locks = Pool::open(pool.config().clone(), 1, Duration::from_secs(30), Waits::Expected);
    let held = locks.get().await.expect("the one lock connection");
    tokio::spawn(async move {
        tokio::time::sleep(held_for).await;
        drop(held);
    });
    locks.execute("SELECT 1", &[]).await.expect("the lock connection, once given back");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(throttle::current(&pool).await.unwrap(), vec![], "a lock pool's wait is no throttling");

    let one = Pool::open(pool.config().clone(), 1, Duration::from_secs(30), Waits::Said);
    let held = one.get().await.expect("the one connection");
    tokio::spawn(async move {
        tokio::time::sleep(held_for).await;
        drop(held);
    });
    one.execute("SELECT 1", &[]).await.expect("the connection, once given back");
    let written = written(&pool).await;
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!((written[0].role, written[0].waits), (DatabaseHolder::Machine, 1));
}

/// A pool of one connection (a cloud broker copy's record pool) is full
/// every time its one request runs, and nobody waits: request after
/// request through it writes nothing, so `weft status` says nothing is
/// busy. A request that gets no connection because the one is held for
/// longer than the pool's wait is busy, and is written down.
#[weft_db_macros::test]
async fn a_pool_used_at_its_size_is_not_busy_and_one_whose_wait_ran_out_is(pool: Pool) {
    weft_task_store::apply_groups(&pool, &[&throttle::GROUP]).await.expect("throttle schema");
    let one = Pool::open(pool.config().clone(), 1, Duration::from_millis(300), Waits::Said);
    let capacity = Capacity { max_connections: 100, reserved: 3, limit: None };
    throttle::record_into(pool.clone(), DatabaseHolder::Machine, &capacity, 1);

    for _ in 0..5 {
        one.execute("SELECT 1", &[]).await.expect("a request through the pool of one");
    }
    assert_eq!(throttle::current(&pool).await.unwrap(), vec![], "a pool in use at its size held nobody back");

    let held = one.get().await.expect("the one connection");
    let e = one.execute("SELECT 1", &[]).await.expect_err("the one connection is held");
    let why = db::no_connection(&e);
    assert_eq!(why, Some(NoConnection::Busy), "{e}");
    drop(held);
    throttle::no_connection(why.unwrap());

    let written = written(&pool).await;
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!((written[0].role, written[0].waits), (DatabaseHolder::Machine, 1));
}
