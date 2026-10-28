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
//! that pool opened no connection (`crate::db::Pool::get` measures every
//! one), one that got none within its pool's whole wait with every
//! connection of the pool handed out (`crate::db::NoConnection::Busy`),
//! and, in a process that reaches the database through the broker, a call
//! the broker answered busy (`weft_broker_client`, log only). A pool in use
//! at its size is not one: nobody need be waiting. A pool of one (a cloud
//! broker copy's record pool) is full every time its one batch runs, and
//! loops looking in the same instant fill a machine's work pool for a few
//! milliseconds; neither is the database holding the install back.
//!
//! A request that got none because no new connection would open
//! (`crate::db::NoConnection::Unreachable`) is not a wait for capacity:
//! it is said apart, on the same beat, as the database not answering
//! this copy ([`unreached`]), and written nowhere (the database it would
//! be written into is the one not answering).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use weft_core::install::DatabaseHolder;

use crate::db::{Client as _, Pool};
use crate::drain::DB_NOW_MS;

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
    pool: Pool,
    role: DatabaseHolder,
    max_connections: u32,
    share: u32,
}

/// Write this process's waits into `pool`'s `database_throttle`, under
/// `role`, with what it read of the database at start. Once per process;
/// a second call is ignored.
pub fn record_into(pool: Pool, role: DatabaseHolder, capacity: &crate::budget::Capacity, share: u32) {
    let _ = RECORD.set(Record { pool, role, max_connections: capacity.max_connections, share });
}

/// A request of this process waited for a database connection to come
/// free (`crate::db::Pool::get`, or [`no_connection`] for one that
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

/// Add `waits` to `role`'s stretch, starting a new one after a quiet
/// longer than [`RECENT`].
pub async fn write(pool: &Pool, role: DatabaseHolder, waits: u64, max_connections: u32, share: u32) -> crate::db::Result<()> {
    let recent_ms = i64::try_from(RECENT.as_millis()).expect("minutes fit");
    pool.execute(
        &format!(
            "INSERT INTO database_throttle AS t (role, since_ms, last_ms, waits, max_connections, share) \
             VALUES ($1, {DB_NOW_MS}, {DB_NOW_MS}, $2, $3, $4) \
             ON CONFLICT (role) DO UPDATE SET \
               since_ms = CASE WHEN t.last_ms < EXCLUDED.last_ms - $5 THEN EXCLUDED.since_ms ELSE t.since_ms END, \
               waits = CASE WHEN t.last_ms < EXCLUDED.last_ms - $5 THEN EXCLUDED.waits ELSE t.waits + EXCLUDED.waits END, \
               last_ms = EXCLUDED.last_ms, max_connections = EXCLUDED.max_connections, share = EXCLUDED.share"
        ),
        &[
            &role.as_str(),
            &i64::try_from(waits).unwrap_or(i64::MAX),
            &i32::try_from(max_connections).unwrap_or(i32::MAX),
            &i32::try_from(share).unwrap_or(i32::MAX),
            &recent_ms,
        ],
    )
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
pub async fn current(pool: &Pool) -> anyhow::Result<Vec<Stretch>> {
    let recent_ms = i64::try_from(RECENT.as_millis()).expect("minutes fit");
    let rows: Vec<(String, i64, i64, i64, i32, i32)> = pool
        .all(
            &format!(
                "SELECT role, since_ms, last_ms, waits, max_connections, share FROM database_throttle \
                 WHERE last_ms >= {DB_NOW_MS} - $1 ORDER BY role"
            ),
            &[&recent_ms],
        )
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

    #[test]
    fn a_wait_is_due_first_and_then_a_minute_after_the_last_said() {
        let now = Instant::now();
        assert!(due(None, now));
        assert!(!due(Some(now), now + SAY_EVERY - Duration::from_millis(1)));
        assert!(due(Some(now), now + SAY_EVERY));
    }
}
