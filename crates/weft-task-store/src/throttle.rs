//! Saying when the install's database holds weft back.
//!
//! Each process sizes its pools to its share of the database
//! (`crate::budget`). When a request finds every connection of its pool
//! in use, it waits in that pool's line for one; that wait is the install
//! running as fast as its database lets it, and it must be seen without
//! flooding anything. So each process counts its waits ([`Tally`]), says
//! so in its log at once and then at most once a [`SAY_EVERY`] with the
//! count since, and, in a process that opens the database, writes the
//! count into `database_throttle` on the same beat ([`record_into`]), one
//! row per role, which `weft status` reads whichever copy wrote it. The
//! normal path writes nothing: a row is only written while waits happen.
//!
//! What counts as a wait: a request that got a connection of a work or
//! record pool only after waiting longer than `crate::db::SLOW_WAIT` while
//! no connection opened (sqlx measures every acquire and reports the slow
//! ones, heard by [`slow_acquires`] and told apart by
//! `crate::db::acquired_slowly`), one that got none within its pool's
//! whole wait with every connection of a pool handed out
//! (`crate::db::NoConnection::Busy`), and, in a process that reaches the
//! database through the broker, a call the broker answered busy
//! (`weft_broker_client`, log only). A pool in use at its size is not one:
//! nobody need be waiting (`crate::db::options` says why that is not
//! counted).
//!
//! A request that got none because no new connection would open
//! (`crate::db::NoConnection::Unreachable`) is not a wait for capacity:
//! it is said apart, on the same beat, as the database not answering
//! this copy ([`unreached`]), and written nowhere (the database it would
//! be written into is the one not answering).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use sqlx::postgres::PgPool;
use weft_core::install::DatabaseHolder;

use crate::drain::DB_NOW_MS;
use crate::sql;

/// How often a process says it is being held back, at most: once at the
/// first wait, then once this long after with the waits since.
pub const SAY_EVERY: Duration = Duration::from_secs(60);

/// How long a stretch of waits stays current once its last write: `weft
/// status` shows a role as held back while its last write is newer than
/// this, and a write after a longer quiet starts a new stretch.
pub const RECENT: Duration = Duration::from_secs(15 * 60);

pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "database_throttle",
    tables: &["database_throttle"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS database_throttle (
            -- Whose copies waited (`weft_core::install::DatabaseHolder`).
            -- SYNC: role's CHECK <-> crates/weft-core/src/install.rs (DatabaseHolder)
            role TEXT PRIMARY KEY CHECK (role IN ('machine', 'dispatcher', 'broker')),
            -- The first wait written of this stretch and the last, in
            -- unix milliseconds on the database's clock.
            since_ms BIGINT NOT NULL,
            last_ms BIGINT NOT NULL,
            -- The waits counted in this stretch, every copy together.
            waits BIGINT NOT NULL,
            -- What the database said it takes, and one copy's share of
            -- it, as the last writer read them at its start
            -- (`crate::budget`).
            max_connections INTEGER NOT NULL,
            share INTEGER NOT NULL
        )"#],
    seed: &[],
    data_steps: &[],
};

/// Waits counted in one process, said at most once a [`SAY_EVERY`].
pub struct Tally {
    waits: AtomicU64,
    /// When the waits were last said; `None` before the first.
    said: Mutex<Option<Instant>>,
}

impl Tally {
    pub const fn new() -> Self {
        Self { waits: AtomicU64::new(0), said: Mutex::new(None) }
    }

    /// Count one wait at `now`. Answers the waits to say, counting this
    /// one, when it is time to say them ([`due`]); `None` otherwise.
    pub fn note(&self, now: Instant) -> Option<u64> {
        self.waits.fetch_add(1, Ordering::Relaxed);
        let mut said = self.said.lock().expect("throttle tally");
        if !due(*said, now) {
            return None;
        }
        *said = Some(now);
        Some(self.waits.swap(0, Ordering::Relaxed))
    }
}

impl Default for Tally {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether waits are said at `now`, last said at `said`: the first at
/// once, then once a [`SAY_EVERY`].
pub fn due(said: Option<Instant>, now: Instant) -> bool {
    said.is_none_or(|said| now.saturating_duration_since(said) >= SAY_EVERY)
}

/// This process's waits for a database connection.
static WAITS: Tally = Tally::new();

/// Where this process writes its waits, once it opened the database.
static RECORD: OnceLock<Record> = OnceLock::new();

struct Record {
    pool: PgPool,
    role: DatabaseHolder,
    max_connections: u32,
    share: u32,
}

/// Write this process's waits into `pool`'s `database_throttle`, under
/// `role`, with what it read of the database at start. Once per process;
/// a second call is ignored.
pub fn record_into(pool: PgPool, role: DatabaseHolder, capacity: &crate::budget::Capacity, share: u32) {
    let _ = RECORD.set(Record { pool, role, max_connections: capacity.max_connections, share });
}

/// A request of this process waited for a database connection to come
/// free (`crate::db::acquired_slowly`, or [`no_connection`] for one that
/// gave up): counted, and said and written when due. Cheap when not
/// due (a counter and a lock no request holds for longer than a
/// comparison).
pub(crate) fn waited() {
    let Some(waits) = WAITS.note(Instant::now()) else { return };
    tracing::warn!(
        target: "weft_task_store::throttle",
        waits,
        "requests are waiting for a database connection: every connection this copy may hold is in use. \
         The install is running as fast as its database lets it; `weft status` says how to give it more"
    );
    if let Some(record) = RECORD.get() {
        let (pool, role, max_connections, share) = (record.pool.clone(), record.role, record.max_connections, record.share);
        // Not on the request that waited: the write waits in the same line.
        tokio::spawn(async move {
            if let Err(e) = write(&pool, role, waits, max_connections, share).await {
                tracing::warn!(target: "weft_task_store::throttle", error = %e, "could not write down that requests waited for the database");
            }
        });
    }
}

/// This process's calls that got no connection because the database
/// did not answer.
static UNREACHED: Tally = Tally::new();

/// A request of this process got no connection because no new one would
/// open ([`crate::db::NoConnection::Unreachable`]): counted, and said in
/// the log when due, naming this copy, with no capacity advice (more
/// capacity does not bring a database back).
pub fn unreached() {
    let Some(calls) = UNREACHED.note(Instant::now()) else { return };
    let copy = RECORD.get().map_or("this process", |record| record.role.as_str());
    tracing::warn!(
        target: "weft_task_store::throttle",
        calls,
        copy,
        "the database cannot be reached from this copy: no new connection to it opens (it is down, restarting, still \
         starting, or the network to it is broken). Nothing of the calls that waited ran; they wait for it to answer again"
    );
}

/// Say a request's missing connection as what kept it ([`waited`],
/// [`unreached`]); a closed pool is this process shutting down, and is
/// said by nothing.
pub fn no_connection(why: crate::db::NoConnection) {
    match why {
        crate::db::NoConnection::Busy => waited(),
        crate::db::NoConnection::Unreachable => unreached(),
        crate::db::NoConnection::Closed => {}
    }
}

/// The target sqlx reports how long a pool's acquire took under.
const SQLX_ACQUIRE: &str = "sqlx::pool::acquire";

/// The field only sqlx's report of a slow acquire carries (its report of
/// every acquire, off on weft's pools, has none).
const SLOW_FIELD: &str = "slow_acquire_threshold_secs";

/// The field of sqlx's acquire reports that says how long the acquire
/// took, in seconds.
const WAITED_FIELD: &str = "acquired_after_secs";

/// The layer that hears sqlx's reports of a slow acquire (on the pools
/// whose waits are said, `crate::db::options`) and hands how long each
/// took to `crate::db::acquired_slowly`, so a wait that got through is counted from
/// sqlx's own measure, never from reading log text. Install it beside the
/// process's log, which should leave those reports out: they come one per
/// slow request, and this module says them once a [`SAY_EVERY`] instead.
///
/// It sees only events of sqlx's acquire target (its per-layer filter),
/// and sqlx sends one only for an acquire past `crate::db::SLOW_WAIT`, so
/// a request that got its connection at once costs nothing here.
pub fn slow_acquires<S>() -> impl tracing_subscriber::Layer<S> + Send + Sync
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use tracing_subscriber::Layer as _;
    SlowAcquires { heard: crate::db::acquired_slowly }.with_filter(only_acquires())
}

/// sqlx's acquire reports, at the level weft's pools send the slow ones.
fn only_acquires() -> tracing_subscriber::filter::Targets {
    tracing_subscriber::filter::Targets::new().with_target(SQLX_ACQUIRE, tracing::Level::WARN)
}

/// Calls `heard` with how long the acquire took, for each report of a
/// slow acquire it is given.
struct SlowAcquires {
    heard: fn(Duration),
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SlowAcquires {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().fields().field(SLOW_FIELD).is_none() {
            return;
        }
        let mut waited = Waited(None);
        event.record(&mut waited);
        // sqlx-core's `PoolInner::acquire` sends it as an f64 of seconds.
        // A report without it is a sqlx that changed its report: said,
        // never guessed at.
        let Some(waited) = waited.0 else {
            tracing::error!(target: "weft_task_store::throttle", "sqlx reported a slow acquire without `{WAITED_FIELD}`; it is not counted");
            return;
        };
        (self.heard)(waited);
    }
}

/// Reads [`WAITED_FIELD`] off a report.
struct Waited(Option<Duration>);

impl tracing::field::Visit for Waited {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if field.name() == WAITED_FIELD {
            self.0 = Duration::try_from_secs_f64(value).ok();
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

/// Add `waits` to `role`'s stretch, starting a new one after a quiet
/// longer than [`RECENT`].
pub async fn write(pool: &PgPool, role: DatabaseHolder, waits: u64, max_connections: u32, share: u32) -> sqlx::Result<()> {
    let recent_ms = i64::try_from(RECENT.as_millis()).expect("minutes fit");
    sqlx::query(sql!(
        "INSERT INTO database_throttle AS t (role, since_ms, last_ms, waits, max_connections, share) \
         VALUES ($1, {DB_NOW_MS}, {DB_NOW_MS}, $2, $3, $4) \
         ON CONFLICT (role) DO UPDATE SET \
           since_ms = CASE WHEN t.last_ms < EXCLUDED.last_ms - $5 THEN EXCLUDED.since_ms ELSE t.since_ms END, \
           waits = CASE WHEN t.last_ms < EXCLUDED.last_ms - $5 THEN EXCLUDED.waits ELSE t.waits + EXCLUDED.waits END, \
           last_ms = EXCLUDED.last_ms, max_connections = EXCLUDED.max_connections, share = EXCLUDED.share"
    ))
    .bind(role.as_str())
    .bind(i64::try_from(waits).unwrap_or(i64::MAX))
    .bind(i32::try_from(max_connections).unwrap_or(i32::MAX))
    .bind(i32::try_from(share).unwrap_or(i32::MAX))
    .bind(recent_ms)
    .execute(pool)
    .await?;
    Ok(())
}

/// One role's current stretch of waits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stretch {
    pub role: DatabaseHolder,
    pub since_ms: i64,
    pub last_ms: i64,
    pub waits: u64,
    pub max_connections: u32,
    pub share: u32,
}

/// The roles whose copies waited for the database within [`RECENT`]. A
/// row naming no [`DatabaseHolder`] fails the read, naming it: the table's
/// CHECK holds it to that set, so one would be a database edited by hand.
pub async fn current(pool: &PgPool) -> anyhow::Result<Vec<Stretch>> {
    let recent_ms = i64::try_from(RECENT.as_millis()).expect("minutes fit");
    let rows: Vec<(String, i64, i64, i64, i32, i32)> = sqlx::query_as(sql!(
        "SELECT role, since_ms, last_ms, waits, max_connections, share FROM database_throttle \
         WHERE last_ms >= {DB_NOW_MS} - $1 ORDER BY role"
    ))
    .bind(recent_ms)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(role, since_ms, last_ms, waits, max_connections, share)| {
            let role = DatabaseHolder::parse(&role).ok_or_else(|| {
                anyhow::anyhow!("database_throttle names '{role}', which is none of {}", DatabaseHolder::accepted())
            })?;
            Ok(Stretch {
                role,
                since_ms,
                last_ms,
                waits: u64::try_from(waits).unwrap_or(0),
                max_connections: u32::try_from(max_connections).unwrap_or(0),
                share: u32::try_from(share).unwrap_or(0),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first wait is said at once; the ones after it within a minute
    /// are only counted, and said together with the one that comes a
    /// minute on.
    #[test]
    fn waits_are_said_at_once_then_once_a_minute_with_their_count() {
        let tally = Tally::new();
        let start = Instant::now();
        assert_eq!(tally.note(start), Some(1), "the first is said at once");
        for s in 1..60 {
            assert_eq!(tally.note(start + Duration::from_secs(s)), None, "within the minute: counted only");
        }
        assert_eq!(tally.note(start + SAY_EVERY), Some(60), "a minute on: every wait since, this one with them");
        assert_eq!(tally.note(start + SAY_EVERY + Duration::from_secs(1)), None);
    }

    static HEARD: Mutex<Vec<Duration>> = Mutex::new(Vec::new());

    fn heard(waited: Duration) {
        HEARD.lock().unwrap().push(waited);
    }

    /// Only sqlx's report of a slow acquire is heard, with how long it
    /// took: not its report of every acquire, not its other warnings, not
    /// another target's.
    #[test]
    fn only_a_slow_acquire_is_heard() {
        use tracing_subscriber::layer::SubscriberExt as _;
        use tracing_subscriber::Layer as _;
        let layer = SlowAcquires { heard }.with_filter(only_acquires());
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::warn!(target: "sqlx::pool::acquire", acquired_after_secs = 2.5, slow_acquire_threshold_secs = 1.0, "acquired connection, but time to acquire exceeded slow threshold");
            assert_eq!(*HEARD.lock().unwrap(), vec![Duration::from_millis(2500)], "a slow acquire");
            tracing::warn!(target: "sqlx::pool::acquire", acquired_after_secs = 0.001, "acquired connection");
            tracing::debug!(target: "sqlx::pool::acquire", acquired_after_secs = 2.5, slow_acquire_threshold_secs = 1.0, "below the level weft's pools send");
            tracing::warn!(target: "sqlx::query", slow_acquire_threshold_secs = 1.0, "slow statement");
            tracing::warn!(target: "weft_task_store::db", slow_acquire_threshold_secs = 1.0, "ours");
            assert_eq!(HEARD.lock().unwrap().len(), 1, "nothing else");
        });
    }

    #[test]
    fn a_wait_is_due_first_and_then_a_minute_after_the_last_said() {
        let now = Instant::now();
        assert!(due(None, now));
        assert!(!due(Some(now), now + SAY_EVERY - Duration::from_millis(1)));
        assert!(due(Some(now), now + SAY_EVERY));
    }
}
