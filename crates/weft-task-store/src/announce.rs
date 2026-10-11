//! Announcing the writes a run makes, without making every commit wait on
//! every other.
//!
//! A transaction that calls `pg_notify` takes one lock shared by the whole
//! database while it commits, and holds it until its commit is on disk,
//! because Postgres hands notifications out in commit order. Every write
//! that announces itself therefore commits one after another, whatever
//! rows they touch, and under load the database spends most of its time
//! with writes queued on that one lock.
//!
//! So the frequent writes do not notify. A worker's records are announced
//! by the broker once their batch commits (`weft_broker::notices`); the
//! rest (an answer written into a waiting run, a run queued again) go
//! through here. Their triggers put what they
//! would have announced in [`TABLE`] (`weft_announce(channel, payload)`),
//! a row like any other that commits with the write or not at all, and
//! takes no shared lock. After the write commits, its writer pokes this
//! process's flusher ([`committed`]), which takes every row waiting there
//! and sends them all in ONE transaction: the shared lock is taken once
//! per batch, not once per write. Under load the flusher runs back to back
//! and each batch carries every write that committed meanwhile; with
//! nothing to do it sends nothing.
//!
//! Listeners are unchanged: they hear the same channels with the same
//! payloads, and read their rows when woken. A writer that forgets to poke
//! only delays its announcement until the next poke of any writer on the
//! database, since a flush takes every row waiting; a process that dies
//! between its commit and its flush leaves its rows for the next one the
//! same way, and a process's flusher sends what it finds waiting as it
//! starts. While a release rolls out, a process still on the one before
//! writes through the same triggers and never pokes: what it announces
//! goes out with the next write of a process on the new one, or, on a
//! quiet database, at the waiter's own next look.
//!
//! The rare writes (a connection changed, an infra copy's status, a domain)
//! still notify from their own trigger: they commit seldom, so the lock
//! costs them nothing, and they keep needing no poke.
//!
//! The same outbox carries the install's news (`weft_platform_traits::News`):
//! what happened to a run a worker drives (an answer handed to it, a cancel
//! asked of it), which the broker copy holding that worker's line must hear
//! while the database may sleep. The write leaves it in [`NEWS_TABLE`]
//! (`weft_tell(project, execution, kind)`), committing with it, and its
//! writer then says so ([`news_written`]: only a cancel or an answer
//! reaching a run pays a look at the news, never the run path's writes). Beside the flushing, once the process gave it its news
//! ([`tell_news_through`]), the flusher's task reads a batch, announces it
//! in one call, and only then takes exactly those rows out. A batch that
//! cannot go out is kept in memory and announced again, loudly, with a
//! growing pause, until it does; the database is not touched meanwhile,
//! and the announcements never wait on it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sqlx::postgres::PgPool;
use weft_platform_traits::{News, NewsKind, RunNews};

/// Where the writes a run makes leave what they announce.
pub const TABLE: &str = "weft_announcement";

/// Where the writes leave the news they make, until it is told.
pub const NEWS_TABLE: &str = "weft_news";

/// How many announcements one flush sends at most; a fuller outbox is
/// flushed again at once.
const BATCH: i32 = 10_000;

/// How many pieces of news one batch tells at most: one announcement each
/// (Pub/Sub takes up to a thousand messages in one publish).
const NEWS_BATCH: i64 = 100;

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "announcement",
    tables: &["weft_announcement", "weft_news"],
    ddl: &[
        // UNLOGGED: an announcement only says when to look at rows that
        // are durable on their own, so a database crash losing the ones
        // not sent yet costs a waiter its next safety look, never data.
        r#"CREATE UNLOGGED TABLE IF NOT EXISTS weft_announcement (
            channel TEXT NOT NULL,
            payload TEXT NOT NULL
        )"#,
        // The news a write makes, kept until the install was told
        // ([`tell_when_wanted`]). Logged: a run waiting on it would otherwise
        // hear it only when its hold gives up.
        // SYNC: the kinds <-> kind_named
        r#"CREATE TABLE IF NOT EXISTS weft_news (
            seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            project_id UUID NOT NULL,
            execution_id UUID NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('answer', 'cancel'))
        )"#,
        // What a write calls to make news, in the write's transaction.
        r#"CREATE OR REPLACE FUNCTION weft_tell(p_project UUID, p_execution UUID, p_kind TEXT) RETURNS VOID AS $$
            INSERT INTO weft_news (project_id, execution_id, kind) VALUES (p_project, p_execution, p_kind)
            $$ LANGUAGE sql"#,
        // What a trigger on a run's path calls instead of `pg_notify`.
        r#"CREATE OR REPLACE FUNCTION weft_announce(p_channel TEXT, p_payload TEXT) RETURNS VOID AS $$
            INSERT INTO weft_announcement (channel, payload) VALUES (p_channel, p_payload)
            $$ LANGUAGE sql"#,
        // The flush ([`flush`]): take up to `p_limit` waiting rows (those
        // another flusher is sending right now are left to it), send each
        // distinct one, all in the caller's one transaction, and answer
        // how many rows were taken.
        r#"CREATE OR REPLACE FUNCTION weft_flush_announcements(p_limit INTEGER) RETURNS INTEGER AS $$
            DECLARE
                v_channels TEXT[];
                v_payloads TEXT[];
            BEGIN
                WITH taken AS (
                    DELETE FROM weft_announcement a
                    WHERE a.ctid IN (SELECT ctid FROM weft_announcement FOR UPDATE SKIP LOCKED LIMIT p_limit)
                    RETURNING channel, payload)
                SELECT array_agg(channel), array_agg(payload) INTO v_channels, v_payloads FROM taken;
                IF v_channels IS NULL THEN
                    RETURN 0;
                END IF;
                PERFORM pg_notify(d.channel, d.payload)
                    FROM (SELECT DISTINCT u.channel, u.payload FROM unnest(v_channels, v_payloads) AS u(channel, payload)) d;
                RETURN array_length(v_channels, 1);
            END;
            $$ LANGUAGE plpgsql"#,
    ],
    seed: &[],
};

/// Tell this process's flusher for `pool`'s database that a write which
/// may have left announcements just committed.
pub fn committed(pool: &PgPool) {
    flusher(pool).wanted.notify_one();
}

/// Tell this process's teller for `pool`'s database that a write which
/// left news (`weft_tell`: a cancel asked of a run a worker drives, an
/// answer handed to one) just committed. A cancel and an answer reaching
/// its run call it, an answer written into a paused run's record included
/// (that one leaves no news, and costs one empty look); the run path's
/// writes never do.
pub fn news_written(pool: &PgPool) {
    flusher(pool).news_wanted.notify_one();
}

/// From now on, this process tells the install's news that writes on
/// `pool`'s database leave, through `news`, starting with whatever news a
/// process that stopped left waiting: beside the announcements, never
/// holding them up.
pub fn tell_news_through(pool: &PgPool, news: Arc<dyn News>) {
    let flusher = flusher(pool);
    *flusher.news.lock().expect("announcement flusher news") = Some(news);
    flusher.news_wanted.notify_one();
}

/// One database's flusher in this process, and the pool it sends through:
/// the last one a write poked with that is still open, so a pool closing
/// (a test's, at its end) never leaves the database without one. Its task
/// sends the announcements and tells the news, side by side.
struct Flusher {
    wanted: tokio::sync::Notify,
    pool: Mutex<PgPool>,
    /// Told when news may be waiting ([`news_written`], [`tell_news_through`]).
    news_wanted: tokio::sync::Notify,
    /// What tells the install's news, once the process gave it
    /// ([`tell_news_through`]); until then news waits in its table.
    news: Mutex<Option<Arc<dyn News>>>,
}

/// The flushers of this process, by database ([`database_key`]). An entry
/// leaves when its flusher stops, so the next poke starts another.
type Flushers = Mutex<HashMap<String, Arc<Flusher>>>;

fn flushers() -> &'static Flushers {
    static FLUSHERS: OnceLock<Flushers> = OnceLock::new();
    FLUSHERS.get_or_init(Mutex::default)
}

/// Where `pool` connects: one flusher per database, whatever the number of
/// pools on it.
fn database_key(pool: &PgPool) -> String {
    let options = pool.connect_options();
    format!("{}:{}/{}", options.get_host(), options.get_port(), options.get_database().unwrap_or(""))
}

/// The flusher of `pool`'s database, started when there is none.
fn flusher(pool: &PgPool) -> Arc<Flusher> {
    let key = database_key(pool);
    let mut flushers = flushers().lock().expect("announcement flushers");
    if let Some(flusher) = flushers.get(&key) {
        let mut sends_through = flusher.pool.lock().expect("announcement flusher pool");
        if sends_through.is_closed() {
            *sends_through = pool.clone();
        }
        return flusher.clone();
    }
    let flusher = Arc::new(Flusher {
        wanted: tokio::sync::Notify::new(),
        pool: Mutex::new(pool.clone()),
        news_wanted: tokio::sync::Notify::new(),
        news: Mutex::new(None),
    });
    tokio::spawn(flush_when_wanted(key.clone(), flusher.clone()));
    flushers.insert(key, flusher.clone());
    flusher
}

/// Takes a flusher out of [`flushers`] if its task ends (the runtime it
/// ran on went away), so a later poke on the same database starts a new
/// one instead of waking nobody.
struct Stopped {
    key: String,
    flusher: Arc<Flusher>,
}

impl Drop for Stopped {
    fn drop(&mut self) {
        let mut flushers = flushers().lock().expect("announcement flushers");
        if flushers.get(&self.key).is_some_and(|f| Arc::ptr_eq(f, &self.flusher)) {
            flushers.remove(&self.key);
        }
    }
}

/// Flush at once (what a process that died between its commit and its
/// flush left waiting goes out with the first write after it), then each
/// time a write asks; and, side by side in the same task (so neither waits
/// on the other, and both live and go with the flusher), tell the news.
async fn flush_when_wanted(key: String, flusher: Arc<Flusher>) {
    let _stopped = Stopped { key, flusher: flusher.clone() };
    let announcements = async {
        loop {
            let pool = flusher.pool.lock().expect("announcement flusher pool").clone();
            flush_waiting(&pool).await;
            flusher.wanted.notified().await;
        }
    };
    tokio::join!(announcements, tell_when_wanted(&flusher));
}

/// How long a flush that failed waits before it is tried again, at first
/// and at most.
const RETRY_FIRST: Duration = Duration::from_millis(500);
const RETRY_LONGEST: Duration = Duration::from_secs(30);

/// Send everything waiting, in batches of [`BATCH`]. A flush that fails is
/// tried again until it goes through: the rows stay in the outbox
/// meanwhile, and the writes that left them wait on what they announce.
async fn flush_waiting(pool: &PgPool) {
    let mut retry = RETRY_FIRST;
    loop {
        match flush(pool).await {
            Ok(sent) if sent >= BATCH as usize => retry = RETRY_FIRST,
            Ok(_) => return,
            Err(_) if pool.is_closed() => return,
            Err(e) => {
                tracing::warn!(
                    target: "weft_task_store::announce",
                    error = %e, retry_in_ms = retry.as_millis() as u64,
                    "could not send the waiting announcements; trying again"
                );
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(RETRY_LONGEST);
            }
        }
    }
}

/// Send what waits in the outbox, in one transaction
/// (`weft_flush_announcements`): how many rows it took.
async fn flush(pool: &PgPool) -> Result<usize, sqlx::Error> {
    let taken: i32 = sqlx::query_scalar("SELECT weft_flush_announcements($1)").bind(BATCH).fetch_one(pool).await?;
    Ok(taken.max(0) as usize)
}

/// Tell the news waiting in `flusher`'s database, a batch at a time, each
/// time some may be waiting, once the process gave it its news. A failure
/// to read or take out waits a growing pause (or, the pool gone, the next
/// wake, which brings a pool that is open).
async fn tell_when_wanted(flusher: &Flusher) {
    let mut retry = RETRY_FIRST;
    loop {
        let news = flusher.news.lock().expect("announcement flusher news").clone();
        let Some(news) = news else {
            flusher.news_wanted.notified().await;
            continue;
        };
        let pool = flusher.pool.lock().expect("announcement flusher pool").clone();
        let waiting = match waiting_news(&pool).await {
            Ok(waiting) => waiting,
            Err(_) if pool.is_closed() => {
                flusher.news_wanted.notified().await;
                continue;
            }
            Err(e) => {
                tracing::warn!(target: "weft_task_store::announce", error = %format!("{e:#}"), "could not read the news waiting; trying again");
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(RETRY_LONGEST);
                continue;
            }
        };
        retry = RETRY_FIRST;
        if waiting.is_empty() {
            flusher.news_wanted.notified().await;
            continue;
        }
        let batch: Vec<RunNews> = waiting.iter().filter_map(|(_, news)| *news).collect();
        if !batch.is_empty() {
            tell_until_told(news.as_ref(), &batch).await;
        }
        let seqs: Vec<i64> = waiting.iter().map(|(seq, _)| *seq).collect();
        while let Err(e) = sqlx::query("DELETE FROM weft_news WHERE seq = ANY($1)").bind(&seqs).execute(&pool).await {
            if pool.is_closed() {
                break;
            }
            tracing::warn!(target: "weft_task_store::announce", error = %format!("{e:#}"), "could not take the news told out of its table; trying again (it would only be told twice)");
            tokio::time::sleep(retry).await;
            retry = (retry * 2).min(RETRY_LONGEST);
        }
        retry = RETRY_FIRST;
    }
}

/// The oldest [`NEWS_BATCH`] pieces of news waiting, each with the news it
/// is (`None` for one of a kind nobody tells, which goes out of the table
/// unsaid, loudly). Read with no lock: two processes telling the same
/// piece only ring the same doorbell twice.
async fn waiting_news(pool: &PgPool) -> Result<Vec<(i64, Option<RunNews>)>, sqlx::Error> {
    let rows: Vec<(i64, uuid::Uuid, uuid::Uuid, String)> =
        sqlx::query_as("SELECT seq, project_id, execution_id, kind FROM weft_news ORDER BY seq LIMIT $1").bind(NEWS_BATCH).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|(seq, project, execution, kind)| {
            let news = kind_named(&kind).map(|kind| RunNews { project, execution, kind });
            if news.is_none() {
                tracing::error!(target: "weft_task_store::announce", seq, %kind, "news of a kind nobody tells; dropped");
            }
            (seq, news)
        })
        .collect())
}

/// Announce `batch` in one call until it goes out: a refusal is logged
/// loudly and the same batch is announced again, after a growing pause.
/// Nothing here touches the database.
async fn tell_until_told(news: &dyn News, batch: &[RunNews]) {
    let mut retry = RETRY_FIRST;
    while let Err(e) = news.announce(batch).await {
        tracing::error!(
            target: "weft_task_store::announce",
            error = %format!("{e:#}"), pieces = batch.len(), retry_in_ms = retry.as_millis() as u64,
            "could not tell the install's news; the runs it is for hear of it late; trying again"
        );
        tokio::time::sleep(retry).await;
        retry = (retry * 2).min(RETRY_LONGEST);
    }
}

/// The kind a row of [`NEWS_TABLE`] names.
// SYNC: kind_named <-> the kinds in weft_news (GROUP), crates/weft-task-store/src/parked_fires.rs (parked_fire_notify), crates/weft-dispatcher/src/journal/postgres.rs (cancel_execution)
fn kind_named(kind: &str) -> Option<NewsKind> {
    match kind {
        "answer" => Some(NewsKind::Answer),
        "cancel" => Some(NewsKind::Cancel),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Refuses the first `fails` calls, then records each batch.
    struct Flaky {
        fails: AtomicUsize,
        told: Mutex<Vec<Vec<RunNews>>>,
    }

    #[async_trait::async_trait]
    impl News for Flaky {
        async fn announce(&self, news: &[RunNews]) -> anyhow::Result<()> {
            if self.fails.load(Ordering::SeqCst) > 0 {
                self.fails.fetch_sub(1, Ordering::SeqCst);
                anyhow::bail!("the bus is away");
            }
            self.told.lock().unwrap().push(news.to_vec());
            Ok(())
        }
        fn listen(&self) -> weft_platform_traits::Listening {
            unreachable!()
        }
        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// A batch refused is announced again whole, in one call, until it
    /// goes out, after a growing pause.
    #[tokio::test(start_paused = true)]
    async fn a_refused_batch_is_announced_again_whole_until_it_goes_out() {
        let bus = Flaky { fails: AtomicUsize::new(3), told: Mutex::default() };
        let batch: Vec<RunNews> = (1..=3)
            .map(|n| RunNews { project: uuid::Uuid::from_u128(1), execution: uuid::Uuid::from_u128(n), kind: NewsKind::Answer })
            .collect();
        let started = tokio::time::Instant::now();
        tell_until_told(&bus, &batch).await;
        assert_eq!(*bus.told.lock().unwrap(), vec![batch], "the whole batch, in one call");
        assert_eq!(started.elapsed(), RETRY_FIRST * 7, "paused 1, 2 and 4 times the first pause");
    }

    #[test]
    fn every_kind_a_write_makes_is_told() {
        assert_eq!(super::kind_named("answer"), Some(weft_platform_traits::NewsKind::Answer));
        assert_eq!(super::kind_named("cancel"), Some(weft_platform_traits::NewsKind::Cancel));
        assert_eq!(super::kind_named("other"), None);
    }
}
