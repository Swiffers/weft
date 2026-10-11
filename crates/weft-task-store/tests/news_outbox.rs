//! Layer-3 tests for the install's news going out through the outbox,
//! against a REAL Postgres: news a write left is told in one call per
//! batch, taken out only once told, kept in memory and told again (without
//! reading the database) while it cannot be, and never holds up the
//! database's own announcements.
//!
//! Gated behind `db-tests` like the rest of this crate's database tests;
//! `scripts/run-db-tests.sh weft-task-store` runs them.
#![cfg(feature = "db-tests")]

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;
use weft_platform_traits::{Listening, News, NewsHeard, NewsKind, RunNews};

use support::setup;

/// Refuses the first `fails` calls, then records each batch announced.
struct Flaky {
    fails: AtomicUsize,
    told: Mutex<Vec<Vec<RunNews>>>,
    heard: tokio::sync::broadcast::Sender<NewsHeard>,
}

impl Flaky {
    fn failing(fails: usize) -> Arc<Self> {
        Arc::new(Self { fails: AtomicUsize::new(fails), told: Mutex::default(), heard: tokio::sync::broadcast::channel(1).0 })
    }

    fn told(&self) -> Vec<Vec<RunNews>> {
        self.told.lock().unwrap().clone()
    }
}

#[async_trait]
impl News for Flaky {
    async fn announce(&self, news: &[RunNews]) -> anyhow::Result<()> {
        if self.fails.load(Ordering::SeqCst) > 0 {
            self.fails.fetch_sub(1, Ordering::SeqCst);
            anyhow::bail!("the bus is away");
        }
        self.told.lock().unwrap().push(news.to_vec());
        Ok(())
    }

    fn listen(&self) -> Listening {
        self.heard.subscribe().into()
    }

    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn tell(pool: &PgPool, execution: u128) -> RunNews {
    let news = RunNews { project: uuid::Uuid::from_u128(1), execution: uuid::Uuid::from_u128(execution), kind: NewsKind::Cancel };
    sqlx::query("SELECT weft_tell($1, $2, 'cancel')").bind(news.project).bind(news.execution).execute(pool).await.unwrap();
    news
}

async fn waiting(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM weft_news").fetch_one(pool).await.unwrap()
}

async fn until(what: &str, holds: impl AsyncFn() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !holds().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// News waiting goes out in one call per batch, once, and is then taken
/// out of the outbox.
#[sqlx::test]
async fn waiting_news_goes_out_in_one_call_and_once(pool: PgPool) {
    setup(&pool).await;
    let batch = vec![tell(&pool, 2).await, tell(&pool, 3).await, tell(&pool, 4).await];
    let bus = Flaky::failing(0);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("taken out once told", async || waiting(&pool).await == 0).await;
    weft_task_store::announce::committed(&pool);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(bus.told(), vec![batch], "one call, once");
}

/// A batch refused is told again as it was, from memory: news written
/// meanwhile is not read into it, and goes out after it.
#[sqlx::test]
async fn a_refused_batch_is_told_again_without_reading_the_database(pool: PgPool) {
    setup(&pool).await;
    let first = tell(&pool, 2).await;
    let bus = Flaky::failing(2);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("the first batch was tried", async || bus.fails.load(Ordering::SeqCst) < 2).await;
    let second = tell(&pool, 3).await;
    weft_task_store::announce::news_written(&pool);
    until("both told", async || bus.told().len() == 2).await;
    assert_eq!(bus.told(), vec![vec![first], vec![second]], "the refused batch went out as it was read");
    until("taken out once told", async || waiting(&pool).await == 0).await;
}

/// A commit that left no news costs no look at the news: only a write that
/// left some says so, and that wakes the teller.
#[sqlx::test]
async fn only_a_write_that_left_news_wakes_the_teller(pool: PgPool) {
    setup(&pool).await;
    let bus = Flaky::failing(0);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    // The teller's first look, for news a stopped process left, finds none.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let news = tell(&pool, 2).await;
    weft_task_store::announce::committed(&pool);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(bus.told().is_empty() && waiting(&pool).await == 1, "a commit alone looked at no news");
    weft_task_store::announce::news_written(&pool);
    until("told once its write said so", async || bus.told() == vec![vec![news]]).await;
}

/// A bus that refuses everything holds up none of the database's own
/// announcements: they go out at once, while the news waits.
#[sqlx::test]
async fn a_bus_that_refuses_holds_up_no_announcement(pool: PgPool) {
    setup(&pool).await;
    tell(&pool, 2).await;
    let bus = Flaky::failing(usize::MAX);
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    until("the bus refused", async || bus.fails.load(Ordering::SeqCst) < usize::MAX).await;
    let watch = support::signals(&pool).await;
    let mut heard = watch.subscribe();
    sqlx::query("SELECT weft_announce($1, 'x')").bind(weft_task_store::tasks::TASK_READY_CHANNEL).execute(&pool).await.unwrap();
    weft_task_store::announce::committed(&pool);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert!(
        heard.woken_before(deadline, |channel, payload| channel == weft_task_store::tasks::TASK_READY_CHANNEL && payload == "x").await.unwrap(),
        "the announcement went out while the news waits"
    );
    assert_eq!(waiting(&pool).await, 1, "the news still waits");
}
