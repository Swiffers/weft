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
//! A flush that fails is tried again a few times, shortly, then left: its
//! rows stay in the outbox and go out with the next write's flush, so a
//! database that cannot be reached is not tried forever by a process with
//! nothing else to do. The outbox keeps no log, so its rows do not survive
//! the database restarting (a serverless one, Neon, restarts when it
//! wakes from sleep). For one to be lost that way, every flush in this
//! process would have failed for the whole quiet spell before the
//! database slept; the waiter it was for then wakes at its next ring or
//! booked look instead.
//!
//! The rare writes (a connection changed, an infra copy's status, a domain)
//! still notify from their own trigger: they commit seldom, so the lock
//! costs them nothing, and they keep needing no poke.
//!
//! The same outbox carries the install's news (`weft_platform_traits::News`):
//! what happened to a run a worker drives (an answer handed to it, a cancel
//! asked of it), which the broker copy holding that worker's line must hear
//! while the database may sleep. The write leaves it in [`NEWS_TABLE`]
//! (`weft_tell(project, run, kind)`), committing with it, and its
//! writer then says so ([`news_written`]: only a cancel or an answer
//! reaching a run pays a look at the news, never the run path's writes).
//! Beside the flushing, once the process gave it its news
//! ([`tell_news_through`]), the flusher's task looks at the news waiting
//! ([`tell_waiting`]). It tells the news the bus never refused first, a
//! batch at a time, oldest first, each batch in one call, and only then
//! takes exactly those rows out. A batch the bus refuses is announced
//! again a few times, shortly; then its rows are marked refused on the
//! row itself, and the refusal is written down for `weft status`
//! ([`Callee::News`]) in the same transaction. Marked rows are never read
//! with new news again, so news refused in an earlier look never travels
//! with new news nor holds it up. A batch being refused in this look does:
//! news written meanwhile waits for that batch's few tries and its mark,
//! and is told by the next batch or the next look. The read of the new
//! news also says, in the same statement, whether any refused row or
//! written-down refusal exists; only then, once the new news is told, does
//! the same look take the lock refusals are written under and try the
//! news refused before it (not what it just refused itself), so an install
//! whose bus never refused anything pays one read per look. Those are
//! tried one piece per call, so a piece the bus will never take only ever
//! refuses itself; the first piece refused again ends that part of the
//! look and goes to the back of the line, so each look spends at most one
//! refused call on it. A look that finds no refused row left, an empty
//! look included, clears what was written down. A read, a mark or a
//! taking out of the news that fails is tried again a few times, shortly,
//! then left until news is next written, or until a process starts
//! telling (each tells what it finds waiting). The announcements never
//! wait on any of it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sqlx::postgres::PgPool;
use weft_platform_traits::{News, NewsKind, RunNews};

use crate::unanswered::{self, Callee};

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
        // not sent yet costs a waiter its wake, never data (see the
        // module docs).
        r#"CREATE UNLOGGED TABLE IF NOT EXISTS weft_announcement (
            channel TEXT NOT NULL,
            payload TEXT NOT NULL
        )"#,
        // The news a write makes, kept until the install was told
        // ([`tell_waiting`]). Logged: a run waiting on it would otherwise
        // hear it only when its hold gives up. `refused_at` is when the
        // bus last refused it (NULL: never), set by `weft_news_refuse`.
        // SYNC: the kinds <-> kind_named
        r#"CREATE TABLE IF NOT EXISTS weft_news (
            seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            project_id UUID NOT NULL,
            run_id UUID NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('answer', 'cancel')),
            refused_at TIMESTAMPTZ
        )"#,
        // The news the bus refused, in the order it is tried again.
        r#"CREATE INDEX IF NOT EXISTS weft_news_refused_idx ON weft_news (refused_at, seq) WHERE refused_at IS NOT NULL"#,
        // Mark the news `p_seqs` refused, now, and answer how many of
        // them were still there to mark (another process may have told
        // and taken them out meanwhile). Serialized with
        // `weft_news_refused`, so a look never clears a refusal being
        // written down as it commits.
        // SYNC: the lock 'weft:news-refused' <-> weft_news_refused
        r#"CREATE OR REPLACE FUNCTION weft_news_refuse(p_seqs BIGINT[]) RETURNS BIGINT AS $$
            DECLARE
                v_marked BIGINT;
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('weft:news-refused', 0));
                UPDATE weft_news SET refused_at = clock_timestamp() WHERE seq = ANY(p_seqs);
                GET DIAGNOSTICS v_marked = ROW_COUNT;
                RETURN v_marked;
            END;
            $$ LANGUAGE plpgsql"#,
        // The news the bus refused, but `p_skip`, longest refused first,
        // `p_limit` at most. With no refused row left at all, it clears
        // the refusal written down under `p_callee` instead, and answers
        // none.
        // SYNC: the lock 'weft:news-refused' <-> weft_news_refuse
        r#"CREATE OR REPLACE FUNCTION weft_news_refused(p_limit BIGINT, p_skip BIGINT[], p_callee TEXT)
            RETURNS TABLE (seq BIGINT, project_id UUID, run_id UUID, kind TEXT) AS $$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended('weft:news-refused', 0));
                IF NOT EXISTS (SELECT 1 FROM weft_news n WHERE n.refused_at IS NOT NULL) THEN
                    DELETE FROM unanswered_call u WHERE u.callee = p_callee;
                    RETURN;
                END IF;
                RETURN QUERY SELECT n.seq, n.project_id, n.run_id, n.kind FROM weft_news n
                    WHERE n.refused_at IS NOT NULL AND n.seq <> ALL(p_skip)
                    ORDER BY n.refused_at, n.seq LIMIT p_limit;
            END;
            $$ LANGUAGE plpgsql"#,
        // What a write calls to make news, in the write's transaction.
        r#"CREATE OR REPLACE FUNCTION weft_tell(p_project UUID, p_run UUID, p_kind TEXT) RETURNS VOID AS $$
            INSERT INTO weft_news (project_id, run_id, kind) VALUES (p_project, p_run, p_kind)
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

/// How long a flush, a read or write of the news, or an announcement of
/// it, that failed waits before its next try, for each try after the
/// first: then it is left until the next write asks (see the module docs).
const RETRIES: [Duration; 3] = [Duration::from_millis(500), Duration::from_secs(2), Duration::from_secs(5)];

/// Run `attempt` until it works, pausing [`RETRIES`] between tries (each
/// failure logged as `what`); its last error once they ran out, or at once
/// when `stop` says trying again is pointless (the pool closed).
async fn retried<T, E, F, Fut>(what: &str, stop: impl Fn() -> bool, mut attempt: F) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut pauses = RETRIES.iter();
    loop {
        match attempt().await {
            Ok(done) => return Ok(done),
            Err(e) => match pauses.next() {
                Some(pause) if !stop() => {
                    tracing::warn!(target: "weft_task_store::announce", error = %e, "{what}; trying again");
                    tokio::time::sleep(*pause).await;
                }
                _ => return Err(e),
            },
        }
    }
}

/// Send everything waiting, in batches of [`BATCH`]. A flush that fails is
/// tried again a few times ([`retried`]), then left: the rows stay in the
/// outbox and go out with the next flush a write asks for.
async fn flush_waiting(pool: &PgPool) {
    loop {
        match retried("could not send the waiting announcements", || pool.is_closed(), || flush(pool)).await {
            Ok(sent) if sent >= BATCH as usize => {}
            Ok(_) => return,
            Err(_) if pool.is_closed() => return,
            Err(e) => {
                tracing::error!(
                    target: "weft_task_store::announce", error = %e,
                    "could not send the waiting announcements; they stay in the outbox until the next write's flush"
                );
                return;
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

/// Each time some may be waiting, once the process gave it its news, tell
/// the news waiting in `flusher`'s database ([`tell_waiting`]), then wait
/// until news is next written (or, the pool gone, until then too, which
/// brings a pool that is open).
async fn tell_when_wanted(flusher: &Flusher) {
    loop {
        let news = flusher.news.lock().expect("announcement flusher news").clone();
        if let Some(news) = news {
            let pool = flusher.pool.lock().expect("announcement flusher pool").clone();
            tell_waiting(&pool, news.as_ref()).await;
        }
        flusher.news_wanted.notified().await;
    }
}

/// One look at the news waiting in `pool`'s database: first the news the
/// bus never refused ([`tell_new`]), then, only when its last read saw
/// refusals, the news refused before this look ([`tell_refused`]), which
/// also clears the refusal written down once no refused row is left.
async fn tell_waiting(pool: &PgPool, news: &dyn News) {
    let Some(told) = tell_new(pool, news).await else { return };
    if told.refusals {
        tell_refused(pool, news, &told.refused_now).await;
    }
}

/// What [`tell_new`] leaves for the rest of its look.
struct ToldNew {
    /// The rows this look marked refused.
    refused_now: Vec<i64>,
    /// What its last read said of refusals ([`NewNews::refusals`]).
    refusals: bool,
}

/// Tell the news the bus never refused, a batch at a time, oldest first,
/// taking each batch out once told. A batch the bus refuses past its few
/// tries ([`retried`]) is marked refused ([`refuse`]) and the look goes on
/// to the news after it. `None` when a read or a taking out failed past
/// its few tries, which ends the look: the news stays in its table until
/// the next.
async fn tell_new(pool: &PgPool, news: &dyn News) -> Option<ToldNew> {
    let closed = || pool.is_closed();
    // The news this look has read past: everything after it is read next,
    // so a batch whose mark failed is not read again by the same look.
    let mut read_past: i64 = 0;
    let mut refused_now = Vec::new();
    loop {
        let NewNews { pieces: waiting, refusals } = match retried("could not read the news waiting", closed, || new_news(pool, read_past)).await {
            Ok(read) => read,
            Err(e) => {
                if !pool.is_closed() {
                    tracing::error!(target: "weft_task_store::announce", error = %format!("{e:#}"), "could not read the news waiting; it is told once news is next written");
                }
                return None;
            }
        };
        let Some(&(last, _)) = waiting.last() else { return Some(ToldNew { refused_now, refusals }) };
        read_past = last;
        // A batch short of full read everything there was: news committed
        // since comes with its own `news_written`, so no read is spent
        // finding nothing.
        let full = waiting.len() as i64 == NEWS_BATCH;
        let seqs: Vec<i64> = waiting.iter().map(|(seq, _)| *seq).collect();
        let batch: Vec<RunNews> = waiting.iter().filter_map(|(_, news)| *news).collect();
        let announced = match batch.is_empty() {
            true => Ok(()),
            false => retried("could not tell the install's news", closed, || news.announce(&batch)).await,
        };
        match announced {
            Ok(()) => take_out_told(pool, &seqs).await?,
            Err(e) => {
                let error = format!("{e:#}");
                tracing::error!(
                    target: "weft_task_store::announce", %error, pieces = batch.len(),
                    "the install's news refused a batch; it is tried again apart from new news each time news is next written"
                );
                refuse(pool, &seqs, &error).await;
                refused_now.extend(seqs);
            }
        }
        if !full {
            return Some(ToldNew { refused_now, refusals });
        }
    }
}

/// Tell the news the bus refused before, but `refused_now` (what this
/// look just refused, after its own few tries), one piece per call,
/// longest refused first, taking out what is told. The first piece
/// refused again is marked refused anew, which puts it at the back of the
/// line, and ends the look. With no refused row left, the read clears the
/// refusal written down (`weft_news_refused`), so it never outlives the
/// news it was about.
async fn tell_refused(pool: &PgPool, news: &dyn News, refused_now: &[i64]) {
    let closed = || pool.is_closed();
    loop {
        let refused = match retried("could not read the news the bus refused", closed, || refused_news(pool, refused_now)).await {
            Ok(refused) => refused,
            Err(e) => {
                if !pool.is_closed() {
                    tracing::error!(target: "weft_task_store::announce", error = %format!("{e:#}"), "could not read the news the bus refused; it is tried once news is next written");
                }
                return;
            }
        };
        if refused.is_empty() {
            return;
        }
        let mut told = Vec::new();
        let mut again = None;
        for (seq, piece) in &refused {
            if let Some(piece) = piece {
                if let Err(e) = news.announce(std::slice::from_ref(piece)).await {
                    again = Some((*seq, format!("{e:#}")));
                    break;
                }
            }
            told.push(*seq);
        }
        if !told.is_empty() && take_out_told(pool, &told).await.is_none() {
            return;
        }
        if let Some((seq, error)) = again {
            tracing::error!(
                target: "weft_task_store::announce", %error, seq,
                "the install's news refused a piece again; it is tried again each time news is next written"
            );
            refuse(pool, &[seq], &error).await;
            return;
        }
    }
}

/// Mark the news `seqs` refused by the bus with `error`, and write the
/// refusal down for `weft status` ([`Callee::News`]) in the same
/// transaction, only when some of it was still there to mark: news
/// another process told and took out meanwhile leaves nothing written
/// down. Tried again a few times; past them the news stays as it was
/// (never refused, it is read with the next look's new news).
async fn refuse(pool: &PgPool, seqs: &[i64], error: &str) {
    let marked = retried("could not mark the news the bus refused", || pool.is_closed(), move || async move {
        let mut tx = pool.begin().await?;
        let marked: i64 = sqlx::query_scalar("SELECT weft_news_refuse($1)").bind(seqs).fetch_one(&mut *tx).await?;
        let alerts = match marked > 0 {
            true => unanswered::gave_up(&mut *tx, Callee::News, None, error).await?,
            false => unanswered::Alerts::default(),
        };
        tx.commit().await?;
        anyhow::Ok(alerts)
    })
    .await;
    match marked {
        Ok(alerts) => alerts.log(),
        Err(e) if !pool.is_closed() => {
            tracing::error!(target: "weft_task_store::announce", error = %format!("{e:#}"), "could not mark the news the bus refused, nor write the refusal down");
        }
        Err(_) => {}
    }
}

/// Take the news `seqs` out of its table, told, trying again a few times;
/// `None` when it could not, which ends the look.
async fn take_out_told(pool: &PgPool, seqs: &[i64]) -> Option<()> {
    let taken_out = retried("could not take the news told out of its table (it would only be told twice)", || pool.is_closed(), move || async move {
        sqlx::query("DELETE FROM weft_news WHERE seq = ANY($1)").bind(seqs).execute(pool).await
    })
    .await;
    match taken_out {
        Ok(_) => Some(()),
        Err(e) => {
            if !pool.is_closed() {
                // Told already: left in the table, it is only told again
                // with the next news.
                tracing::error!(target: "weft_task_store::announce", error = %format!("{e:#}"), "could not take the news told out of its table; it is told again with the next news");
            }
            None
        }
    }
}

/// A piece of news as its row holds it: its `seq`, and the news it is.
type NewsRow = (i64, uuid::Uuid, uuid::Uuid, String);

/// What one read of the news never refused found.
struct NewNews {
    /// The oldest [`NEWS_BATCH`] pieces waiting after the read's start.
    pieces: Vec<(i64, Option<RunNews>)>,
    /// Whether, as it read, the bus had refused news still waiting or a
    /// refusal was written down ([`Callee::News`]): only then does a look
    /// go on to the refused news ([`tell_refused`]).
    refusals: bool,
}

/// One row of [`new_news`]'s query: whether there are refusals, then a
/// piece of news (all four `None` when none is waiting).
type NewNewsRow = (bool, Option<i64>, Option<uuid::Uuid>, Option<uuid::Uuid>, Option<String>);

/// The oldest [`NEWS_BATCH`] pieces of news the bus never refused, waiting
/// after `read_past` ([`pieces`]), and, in the same statement, whether
/// there are refusals ([`NewNews::refusals`]), so a look on an install
/// whose bus never refused anything costs this one read. Read with no
/// lock: two processes telling the same piece only ring the same doorbell
/// twice.
async fn new_news(pool: &PgPool, read_past: i64) -> Result<NewNews, sqlx::Error> {
    let rows: Vec<NewNewsRow> = sqlx::query_as(
        "SELECT f.refusals, n.seq, n.project_id, n.run_id, n.kind \
         FROM (SELECT EXISTS (SELECT 1 FROM weft_news r WHERE r.refused_at IS NOT NULL) \
                   OR EXISTS (SELECT 1 FROM unanswered_call u WHERE u.callee = $3) AS refusals) f \
         LEFT JOIN LATERAL (SELECT seq, project_id, run_id, kind FROM weft_news \
             WHERE refused_at IS NULL AND seq > $2 ORDER BY seq LIMIT $1) n ON TRUE \
         ORDER BY n.seq",
    )
    .bind(NEWS_BATCH)
    .bind(read_past)
    .bind(Callee::News.key())
    .fetch_all(pool)
    .await?;
    let refusals = rows.first().is_some_and(|row| row.0);
    let rows = rows
        .into_iter()
        .filter_map(|(_, seq, project, run, kind)| Some((seq?, project?, run?, kind?)))
        .collect();
    Ok(NewNews { pieces: pieces(rows), refusals })
}

/// Up to [`NEWS_BATCH`] pieces of news the bus refused, but `skip`,
/// longest refused first ([`pieces`]); none, with the refusal written down
/// cleared, when no refused row is left (`weft_news_refused`).
async fn refused_news(pool: &PgPool, skip: &[i64]) -> Result<Vec<(i64, Option<RunNews>)>, sqlx::Error> {
    let rows: Vec<NewsRow> = sqlx::query_as("SELECT seq, project_id, run_id, kind FROM weft_news_refused($1, $2, $3)")
        .bind(NEWS_BATCH)
        .bind(skip)
        .bind(Callee::News.key())
        .fetch_all(pool)
        .await?;
    Ok(pieces(rows))
}

/// Each row with the news it is (`None` for one of a kind nobody tells,
/// which goes out of the table unsaid, loudly).
fn pieces(rows: Vec<NewsRow>) -> Vec<(i64, Option<RunNews>)> {
    rows.into_iter()
        .map(|(seq, project, run, kind)| {
            let news = kind_named(&kind).map(|kind| RunNews { project, run, kind });
            if news.is_none() {
                tracing::error!(target: "weft_task_store::announce", seq, %kind, "news of a kind nobody tells; dropped");
            }
            (seq, news)
        })
        .collect()
}

/// The kind a row of [`NEWS_TABLE`] names.
// SYNC: kind_named <-> the kinds in weft_news (GROUP), crates/weft-task-store/src/parked_fires.rs (parked_fire_notify), crates/weft-dispatcher/src/journal/postgres.rs (cancel_run)
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

    fn batch() -> Vec<RunNews> {
        (1..=3)
            .map(|n| RunNews { project: uuid::Uuid::from_u128(1), run: uuid::Uuid::from_u128(n), kind: NewsKind::Answer })
            .collect()
    }

    /// A batch refused is announced again whole, in one call, after each
    /// of the few growing pauses; one the bus refuses past them is given
    /// up, with no clock left running.
    #[tokio::test(start_paused = true)]
    async fn a_refused_batch_is_announced_again_whole_a_few_times_then_given_up() {
        let pieces = batch();
        let bus = Flaky { fails: AtomicUsize::new(RETRIES.len()), told: Mutex::default() };
        let started = tokio::time::Instant::now();
        retried("tell", || false, || bus.announce(&pieces)).await.unwrap();
        assert_eq!(*bus.told.lock().unwrap(), vec![batch()], "the whole batch, in one call");
        assert_eq!(started.elapsed(), RETRIES.iter().sum::<Duration>());

        let bus = Flaky { fails: AtomicUsize::new(RETRIES.len() + 1), told: Mutex::default() };
        let started = tokio::time::Instant::now();
        retried("tell", || false, || bus.announce(&pieces)).await.unwrap_err();
        assert!(bus.told.lock().unwrap().is_empty());
        assert_eq!(started.elapsed(), RETRIES.iter().sum::<Duration>(), "given up after the last pause");
    }

    #[test]
    fn every_kind_a_write_makes_is_told() {
        assert_eq!(super::kind_named("answer"), Some(weft_platform_traits::NewsKind::Answer));
        assert_eq!(super::kind_named("cancel"), Some(weft_platform_traits::NewsKind::Cancel));
        assert_eq!(super::kind_named("other"), None);
    }
}
