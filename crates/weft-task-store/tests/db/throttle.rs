//! Saying that the database holds weft back (`weft_task_store::throttle`):
//! a request that waited for a connection to come free counts, whether
//! it got one late or gave up, never a pool merely in use at its size.

use std::time::Duration;

use sqlx::PgPool;
use weft_core::install::DatabaseHolder;
use weft_task_store::budget::Capacity;
use weft_task_store::db::{self, NoConnection, Waits};
use weft_task_store::throttle;

/// The address of the database `pool` is on: `$DATABASE_URL`'s server,
/// with the database `#[sqlx::test]` made for this test.
fn address_of(pool: &PgPool) -> String {
    let server = std::env::var("DATABASE_URL").expect("the db tests run with $DATABASE_URL");
    let (server, _) = server.rsplit_once('/').expect("a Postgres address names its database");
    let database = pool.connect_options().get_database().expect("the test's database").to_string();
    format!("{server}/{database}")
}

/// The stretch written for `pool`'s role, once the write that runs beside
/// the request that waited has landed.
async fn written(pool: &PgPool) -> Vec<throttle::Stretch> {
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
/// a wait the user felt:
/// sqlx's own measure of it reaches the layer the runtime installs, and it
/// is written down. A lock pool's waits are its holders waiting for each
/// other, and say nothing.
#[sqlx::test]
async fn a_request_that_waited_and_got_through_is_written_down(pool: PgPool) {
    use tracing_subscriber::layer::SubscriberExt as _;
    weft_task_store::apply_groups(&pool, &[&throttle::GROUP]).await.expect("throttle schema");
    let _logging = tracing::subscriber::set_default(tracing_subscriber::registry().with(throttle::slow_acquires()));
    let capacity = Capacity { max_connections: 100, reserved: 3, limit: None };
    throttle::record_into(pool.clone(), DatabaseHolder::Machine, &capacity, 1);
    let held_for = db::SLOW_WAIT + Duration::from_millis(500);

    let locks = db::connect(&address_of(&pool), 1, Duration::from_secs(30), Waits::Expected).await.expect("a lock pool of one");
    let held = locks.acquire().await.expect("the one lock connection");
    tokio::spawn(async move {
        tokio::time::sleep(held_for).await;
        drop(held);
    });
    sqlx::query("SELECT 1").execute(&locks).await.expect("the lock connection, once given back");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(throttle::current(&pool).await.unwrap(), vec![], "a lock pool's wait is no throttling");

    let one = db::connect(&address_of(&pool), 1, Duration::from_secs(30), Waits::Said).await.expect("a pool of one");
    let held = one.acquire().await.expect("the one connection");
    tokio::spawn(async move {
        tokio::time::sleep(held_for).await;
        drop(held);
    });
    sqlx::query("SELECT 1").execute(&one).await.expect("the connection, once given back");
    let written = written(&pool).await;
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!((written[0].role, written[0].waits), (DatabaseHolder::Machine, 1));
}

/// A pool of one connection (a cloud broker copy's record pool) is full
/// every time its one request runs, and nobody waits: request after
/// request through it writes nothing, so `weft status` says nothing is
/// busy. A request that gets no connection because the one is held for
/// longer than the pool's wait is busy, and is written down.
#[sqlx::test]
async fn a_pool_used_at_its_size_is_not_busy_and_one_whose_wait_ran_out_is(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&throttle::GROUP]).await.expect("throttle schema");
    let one = db::connect(&address_of(&pool), 1, Duration::from_millis(300), Waits::Said).await.expect("a pool of one");
    let capacity = Capacity { max_connections: 100, reserved: 3, limit: None };
    throttle::record_into(pool.clone(), DatabaseHolder::Machine, &capacity, 1);

    for _ in 0..5 {
        sqlx::query("SELECT 1").execute(&one).await.expect("a request through the pool of one");
    }
    assert_eq!(throttle::current(&pool).await.unwrap(), vec![], "a pool in use at its size held nobody back");

    let held = one.acquire().await.expect("the one connection");
    let e = sqlx::query("SELECT 1").execute(&one).await.expect_err("the one connection is held");
    let why = db::no_connection(&e);
    assert_eq!(why, Some(NoConnection::Busy), "{e}");
    drop(held);
    throttle::no_connection(why.unwrap());

    let written = written(&pool).await;
    assert_eq!(written.len(), 1, "{written:?}");
    assert_eq!((written[0].role, written[0].waits), (DatabaseHolder::Machine, 1));
}
