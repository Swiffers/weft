//! A role's tick against a real Postgres: a ring the alarm carried is
//! answered by the tick that takes it, and no other tick touches what was
//! written down.
//!
//! Gated behind `db-tests`; `scripts/run-db-tests.sh weft-runtime` runs them.
#![cfg(feature = "db-tests")]

use std::sync::Arc;

use sqlx::PgPool;
use weft_platform_traits::CoreRole;
use weft_runtime::server::Tick;
use weft_task_store::drain::TickBody;
use weft_task_store::unanswered::{self, Callee};

fn tick(pool: &PgPool) -> Tick {
    Tick {
        role: CoreRole::Dispatcher,
        run: Arc::new(|_| Box::pin(async { Ok(()) })),
        alarm: Arc::new(weft_platform_traits::FakeAlarm::new()),
        pool: Some(pool.clone()),
    }
}

/// Where a ring not answered is written down, with the run columns every
/// write of a failure reads.
async fn setup(pool: &PgPool) {
    weft_task_store::apply_groups(pool, &[&unanswered::GROUP]).await.expect("schema");
    unanswered::judged_run_columns(pool).await.expect("the run columns");
}

async fn written(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT callee FROM unanswered_call ORDER BY callee").fetch_all(pool).await.unwrap()
}

#[sqlx::test]
async fn only_the_tick_of_a_ring_the_alarm_carried_answers_it(pool: PgPool) {
    setup(&pool).await;
    unanswered::failed(&pool, Callee::Role("dispatcher"), None, "connection refused").await.unwrap().log();
    unanswered::failed(&pool, Callee::Role("supervisor"), None, "connection refused").await.unwrap().log();
    let t = tick(&pool);
    t.pass(TickBody { loops: vec!["delivery".into()], ..TickBody::default() }).await.unwrap();
    assert_eq!(written(&pool).await, vec!["role:dispatcher".to_string(), "role:supervisor".to_string()], "a plain tick answers nothing");
    t.pass(TickBody { loops: vec!["delivery".into()], after_failed_ring: true, ..TickBody::default() }).await.unwrap();
    assert_eq!(written(&pool).await, vec!["role:supervisor".to_string()], "the carried ring is answered, for this role only");
}

/// A carried ring whose pass failed is not answered: the queue calls the
/// tick again, and the role still reads as not answering meanwhile.
#[sqlx::test]
async fn a_carried_ring_whose_pass_failed_stays_written_down(pool: PgPool) {
    setup(&pool).await;
    unanswered::failed(&pool, Callee::Role("dispatcher"), None, "connection refused").await.unwrap().log();
    let t = Tick {
        run: Arc::new(|_| {
            Box::pin(async { Err(weft_task_store::drain::PassFailed { loops: vec!["delivery".into()], error: "the database is down".into() }) })
        }),
        ..tick(&pool)
    };
    t.pass(TickBody { loops: vec!["delivery".into()], after_failed_ring: true, ..TickBody::default() }).await.unwrap_err();
    assert_eq!(written(&pool).await, vec!["role:dispatcher".to_string()]);
}
