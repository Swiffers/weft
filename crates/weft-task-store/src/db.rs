//! Connecting to the install's database. Every pool weft opens comes from
//! here, and keeps its connections for as long as the process runs. A
//! database that sleeps (a serverless Postgres) sleeps once no query has
//! run for a while, whatever connections stay open, and closes them as it
//! goes; a pool finds that on its next call (see `PING_AFTER_IDLE`) and
//! connects again. Closing idle connections earlier would only make the
//! next call open new ones (an encrypted handshake and a sign-in). How
//! many connections each pool takes is `crate::budget`'s to say.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::Connection;

/// How long a connection may have sat idle before it is pinged on its way
/// out of the pool. A connection a request just handed back goes straight
/// out again: the ping is one more round trip to the database, and a
/// request that runs thirty queries would pay it thirty times. One that
/// sat longer may have been cut under the pool (the database restarted,
/// the network dropped it), so it is checked and, if dead, replaced.
pub const PING_AFTER_IDLE: Duration = Duration::from_secs(5);

/// How long a boot keeps trying to reach a database that is not
/// answering yet (a local one still starting, a serverless one waking).
const REACH_FOR: Duration = Duration::from_secs(60);

/// The name a broker's record pool connections carry
/// (`application_name`), ahead of the broker's replica: one name per
/// broker, so its batches are told apart from other brokers' in the
/// database's own view of its sessions.
pub const RECORD_POOL_APPLICATION: &str = "weft-records";

/// Whether a database error means the database could not be reached or
/// is restarting, rather than that the statement itself failed: no
/// connection to be had (whatever kept it, [`no_connection`] says which),
/// a connection that broke, or the server refusing because it is shutting
/// down or starting up (SQLSTATE class 08, and 57P01 to 57P03). Only that
/// is worth asking again on; anything else fails the same way next time.
/// The one reading, for every service that answers a caller who can wait.
pub fn unreachable(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_) | sqlx::Error::Protocol(_) => true,
        sqlx::Error::Database(db) => db
            .code()
            .is_some_and(|code| code.starts_with("08") || matches!(&*code, "57P01" | "57P02" | "57P03")),
        _ => false,
    }
}

/// Why a call got no connection, so not one of its statements ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoConnection {
    /// The database was too busy to give the call one: every connection
    /// this copy may hold was in use for the whole wait, or the database
    /// refused a new one because it was full (SQLSTATE 53300). Waiting
    /// works once the load passes, and more capacity makes it pass sooner.
    Busy,
    /// No new connection could be opened within the wait while this copy
    /// had room for one: the database is down, restarting, still starting
    /// (57P03), or out of reach of the network. Waiting works once it
    /// answers again; capacity has nothing to do with it.
    Unreachable,
    /// The pool was closed: this process is shutting down.
    Closed,
}

/// Why `e` means the call got no connection ([`NoConnection`]), or `None`
/// when it got one (or failed before asking). The pool answers both a
/// wait that ran out with every connection in use and one that ran out
/// opening a new connection the same way (`PoolTimedOut`: sqlx keeps
/// trying a refused or still-starting database until the wait ends), so
/// which one it was is read off this process's said pools at the time:
/// one with every connection it may hold handed out to a request
/// ([`InUse`]) is busy; with none like that, the connections it tried to
/// open did not come.
///
/// Not the pool's own size: sqlx counts a place while its connection is
/// still being opened, so during an outage a pool with one caller still
/// connecting reads full for as long as the outage lasts.
///
/// What still reads wrong, both for a moment and both settled by the
/// next wait that runs out: a connection handed out before an outage and
/// stuck on the dead database counts as in use until the system gives up
/// on it (`connect_options` says why that takes long), so a pool of them
/// reads busy that long; and a connection on its way back (sqlx pings it after
/// `after_release`, before it is free) does not count, so a wait that
/// runs out in that round trip, with every other connection in use,
/// reads unreachable.
pub fn no_connection(e: &sqlx::Error) -> Option<NoConnection> {
    match e {
        sqlx::Error::PoolTimedOut => {
            Some(if a_pool_is_full() { NoConnection::Busy } else { NoConnection::Unreachable })
        }
        sqlx::Error::PoolClosed => Some(NoConnection::Closed),
        sqlx::Error::Database(db) if db.code().is_some_and(|code| code == "53300") => Some(NoConnection::Busy),
        _ => None,
    }
}

/// Whether a database error means the database was too busy to give the
/// call a connection ([`NoConnection::Busy`]).
pub fn busy(e: &sqlx::Error) -> bool {
    no_connection(e) == Some(NoConnection::Busy)
}

/// [`busy`], anywhere in `e`'s chain.
pub fn database_busy(e: &anyhow::Error) -> bool {
    e.chain().filter_map(|cause| cause.downcast_ref::<sqlx::Error>()).any(busy)
}

/// [`no_connection`], of the first cause in `e`'s chain that has one.
pub fn database_no_connection(e: &anyhow::Error) -> Option<NoConnection> {
    e.chain().filter_map(|cause| cause.downcast_ref::<sqlx::Error>()).find_map(no_connection)
}

/// How many of one pool's open connections are handed out to a request
/// right now, kept by the pool's own hooks ([`options`]): one
/// more when a connection leaves the pool for a request (an idle one
/// through `before_acquire`, a new one through `after_connect`), one less
/// when the request gives it back (`after_release`). A place whose
/// connection is still being opened is never counted ([`no_connection`]
/// says why).
///
/// The count stays true because every handed-out connection of weft's
/// pools comes back through `after_release`: weft never detaches, leaks
/// or closes a pooled connection by hand, the pools never retire a
/// connection for its age (`max_lifetime` is off, which sqlx checks
/// before `after_release`), and a pool is opened lazily and first
/// reached through an ordinary acquire ([`reach`]), since sqlx's
/// `connect_with` puts its first connection back without the hook.
#[derive(Debug)]
struct InUse {
    connections: AtomicU32,
    max: u32,
}

impl InUse {
    fn new(max: u32) -> Self {
        Self { connections: AtomicU32::new(0), max }
    }

    fn handed_out(&self) {
        self.connections.fetch_add(1, Ordering::AcqRel);
    }

    fn given_back(&self) {
        let before = self.connections.try_update(Ordering::AcqRel, Ordering::Acquire, |n| Some(n.saturating_sub(1)));
        debug_assert_ne!(before, Ok(0), "a connection given back that was never counted as handed out");
    }

    /// Every connection this pool may hold is handed out to a request.
    fn full(&self) -> bool {
        self.connections.load(Ordering::Acquire) >= self.max
    }
}

/// A pool this process opened ([`open`]), with its count of connections
/// handed out.
struct Opened {
    pool: PgPool,
    in_use: Arc<InUse>,
    waits: Waits,
}

/// Every pool this process opened. Each is kept for as long as the
/// process runs (as weft's pools are anyway), which is also what makes a
/// pool's address a stable name for it ([`every_connection_handed_out`]).
static OPENED: Mutex<Vec<Opened>> = Mutex::new(Vec::new());

/// Whether one of this process's said pools ([`Waits::Said`]), the ones
/// every request and batch goes through, has every connection handed out:
/// what [`no_connection`] reads to tell a busy pool from a database that
/// does not answer. A lock pool is left out: it is full by design while
/// its holders wait.
fn a_pool_is_full() -> bool {
    OPENED.lock().expect("opened pools").iter().any(|opened| opened.waits == Waits::Said && opened.in_use.full())
}

/// Whether every connection `pool` may hold is handed out to a request
/// ([`InUse`]): a wait on it that ran out was a wait for one of them to
/// come free, never a database that does not answer. A place whose
/// connection is still being opened does not count. `false` for a pool
/// this process did not open through this module (a test's own pool):
/// nothing counts its connections, so its wait running out is never
/// taken for a full pool.
pub fn every_connection_handed_out(pool: &PgPool) -> bool {
    // sqlx gives a pool no identity to compare; its options live inside
    // the pool's one shared allocation, so their address names the pool.
    OPENED
        .lock()
        .expect("opened pools")
        .iter()
        .find(|opened| std::ptr::eq(opened.pool.options(), pool.options()))
        .is_some_and(|opened| opened.in_use.full())
}

/// When a said pool of this process last opened a connection (its
/// `after_connect`); `None` before the first. What [`acquired_slowly`]
/// reads to tell a wait for capacity from a wait for a new connection.
static LAST_OPENED: Mutex<Option<Instant>> = Mutex::new(None);

/// Whether a pool's waits are the install being held back by its
/// database (`crate::throttle`): the work and record pools, which every
/// request and every batch go through. A lock pool is not: its holders
/// wait for each other by design (`crate::locks`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waits {
    /// Read by [`no_connection`] and [`acquired_slowly`]: a request whose
    /// wait ran out while one of these pools had every connection handed
    /// out, or that waited longer than [`SLOW_WAIT`] while none of them
    /// opened a connection, waited for capacity, said by
    /// `crate::throttle`.
    Said,
    /// Part of how the pool is used.
    Expected,
}

/// How every connection to the install's database at `url` is made. It
/// does not yet notice a database that vanished without a word (its host
/// gone silent, a network path broken): sqlx offers no way to set the
/// socket's dead-peer checks (`weft_core::net::KEEPALIVE_IDLE`), so such a
/// connection is held as live, the `LISTEN` connection hears nothing for
/// good, and a pooled connection with a query in flight waits until the
/// system gives up on it by itself.
pub fn connect_options(url: &str) -> anyhow::Result<PgConnectOptions> {
    url.parse().context("not a Postgres address")
}

/// A pool on `url` of at most `max_connections`, each request for a
/// connection waiting at most `acquire_timeout`, its waits counted as
/// `waits` says. The database may still be starting: it is tried again
/// for a minute before the error is answered.
pub async fn connect(url: &str, max_connections: u32, acquire_timeout: Duration, waits: Waits) -> anyhow::Result<PgPool> {
    reach(connect_options(url)?, max_connections, acquire_timeout, waits).await
}

/// The record pool of the broker `replica`, of `max_connections` (named
/// `RECORD_POOL_APPLICATION/<replica>`), on `url`, as [`connect`].
pub async fn connect_record_pool(url: &str, replica: &str, max_connections: u32, acquire_timeout: Duration) -> anyhow::Result<PgPool> {
    let named = connect_options(url)?.application_name(&format!("{RECORD_POOL_APPLICATION}/{replica}"));
    reach(named, max_connections, acquire_timeout, Waits::Said).await
}

/// [`open`], then reached through an ordinary acquire, never sqlx's
/// `connect_with`: that one gives its first connection back without
/// `after_release`, which would leave it counted in use for good
/// ([`InUse`]).
async fn reach(connect: PgConnectOptions, max_connections: u32, acquire_timeout: Duration, waits: Waits) -> anyhow::Result<PgPool> {
    let pool = open(connect, max_connections, acquire_timeout, waits);
    reaching(|| async { pool.acquire().await.map(drop) }).await?;
    Ok(pool)
}

/// A pool on `connect` with no connection yet, its connections handed out
/// counted ([`InUse`]) and the pool kept in [`OPENED`].
pub(crate) fn open(connect: PgConnectOptions, max_connections: u32, acquire_timeout: Duration, waits: Waits) -> PgPool {
    let in_use = Arc::new(InUse::new(max_connections));
    let pool = options(max_connections, acquire_timeout, in_use.clone(), waits).connect_lazy_with(connect);
    OPENED.lock().expect("opened pools").push(Opened { pool: pool.clone(), in_use, waits });
    pool
}

/// Run `attempt` until the database answers it, for [`REACH_FOR`] at most:
/// a database still starting, or waking, is waited for.
pub(crate) async fn reaching<T, F, Fut>(mut attempt: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
    let deadline = std::time::Instant::now() + REACH_FOR;
    loop {
        match attempt().await {
            Ok(done) => return Ok(done),
            Err(e) if std::time::Instant::now() < deadline => {
                tracing::warn!(target: "weft_task_store::db", error = %e, "the database is not answering yet; trying again");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// How long a request may wait for a connection of a pool whose waits
/// are said ([`Waits::Said`]) before its wait counts as the database
/// holding the install back ([`acquired_slowly`]). A second: a request
/// through these pools runs a few short statements, so a connection comes
/// free within milliseconds while the pool keeps up; opening a new one
/// takes milliseconds on a machine and well under a second to a cloud
/// database; and a second is where a person waiting on an editor action or
/// a CLI command starts to feel it.
pub const SLOW_WAIT: Duration = Duration::from_secs(1);

/// A request of this process got a connection of one of its pools only
/// after waiting longer than [`SLOW_WAIT`] (sqlx measures it, and says it
/// to `crate::throttle::slow_acquires`, the only one that hears it).
///
/// sqlx's measure (`waited`) covers the whole acquire: waiting for a free
/// place, the ping of a connection that sat idle, and opening a new
/// connection. Only the first is the database holding the install back; a
/// serverless database waking from its sleep makes the first requests
/// after it open connections for a second or more, every time, and that is
/// the install working as it should. So a slow acquire is a wait for
/// capacity only when no said pool opened a connection while it waited
/// ([`LAST_OPENED`]); with one opened inside its wait, the time went to
/// connecting, and nothing is said.
///
/// Not by how full the pools are when sqlx reports it: the report comes
/// after the acquire returned, with the request holding its connection,
/// so the batch that woke the database leaves a pool of one "full" every
/// time. sqlx's report names no pool, so a connection opened by any said
/// pool during the wait counts.
pub fn acquired_slowly(waited: Duration) {
    let last_opened = *LAST_OPENED.lock().expect("last opened");
    if waited_for_capacity(waited, Instant::now(), last_opened) {
        crate::throttle::waited();
    }
}

/// Whether an acquire that took `waited` and ended at `now` waited for
/// capacity: no connection opened inside it (the last at `last_opened`).
fn waited_for_capacity(waited: Duration, now: Instant, last_opened: Option<Instant>) -> bool {
    match (last_opened, now.checked_sub(waited)) {
        (None, _) => true,
        (Some(opened), Some(began)) => opened < began,
        // The wait began before this process's clock can say: a
        // connection it opened since is inside it.
        (Some(_), None) => false,
    }
}

/// The options every pool is built with: `in_use` is its count of
/// connections handed out ([`InUse`]), kept by its hooks, and `waits`
/// says whether its slow acquires and the connections it opens are
/// reported ([`acquired_slowly`]).
///
/// `max_lifetime` is off: the pools keep their connections for as long as
/// the process runs (the module docs say why), and a connection retired
/// for its age would go back without `after_release`.
///
/// A request taking a connection without waiting is never counted, even
/// one that takes the last free connection of a full pool: that says the
/// pool is in use at its size, never that anybody waited. A pool of one
/// (a cloud broker copy's record pool) is full every time its one batch
/// runs, and loops looking in the same instant fill a machine's work pool
/// for a few milliseconds; both would read as the database holding the
/// install back when nothing was. What is counted is a wait itself: one
/// longer than [`SLOW_WAIT`] that got through ([`acquired_slowly`]), and
/// one that ran out with a pool's every connection handed out
/// ([`no_connection`]). A pool whose
/// waits are expected reports none: a lock pool's holders wait for each
/// other for as long as a lock is held.
fn options(max_connections: u32, acquire_timeout: Duration, in_use: Arc<InUse>, waits: Waits) -> PgPoolOptions {
    let said = waits == Waits::Said;
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(0)
        .idle_timeout(None)
        .max_lifetime(None)
        .acquire_timeout(acquire_timeout)
        .acquire_slow_threshold(SLOW_WAIT)
        .acquire_slow_level(if said { log::LevelFilter::Warn } else { log::LevelFilter::Off })
        .test_before_acquire(false);
    let reused = in_use.clone();
    let pool = pool.before_acquire(move |conn, meta| {
        let reused = reused.clone();
        Box::pin(async move {
            if meta.idle_for >= PING_AFTER_IDLE {
                conn.ping().await?;
            }
            // Last, after the only await: sqlx hands the connection over
            // with no await after this, so a request that gave up while
            // the ping ran was never counted.
            reused.handed_out();
            Ok(true)
        })
    });
    let opened = in_use.clone();
    pool.after_connect(move |_, _| {
        // sqlx hands a new connection over as soon as this returns.
        if said {
            *LAST_OPENED.lock().expect("last opened") = Some(Instant::now());
        }
        opened.handed_out();
        Box::pin(async { Ok(()) })
    })
    .after_release(move |_, _| {
        in_use.given_back();
        Box::pin(async { Ok(true) })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pool is full only with every connection it may hold handed out;
    /// one given back frees its place.
    #[test]
    fn a_pool_is_full_only_with_every_connection_handed_out() {
        let in_use = InUse::new(2);
        assert!(!in_use.full(), "nothing handed out");
        in_use.handed_out();
        assert!(!in_use.full(), "room for one more");
        in_use.handed_out();
        assert!(in_use.full());
        in_use.given_back();
        assert!(!in_use.full(), "one given back");
    }

    /// A place whose connection is still being opened is not in use: a
    /// pool whose one caller keeps trying a database that refuses it has
    /// its one place taken and none free (sqlx's own count calls it
    /// full), yet the wait that runs out reads unreachable, never busy.
    #[tokio::test]
    async fn a_place_still_connecting_is_not_in_use() {
        // Port 1 on this machine refuses at once, and sqlx tries again
        // until the wait ends, as it does for a database still starting.
        let refused = connect_options("postgres://weft:secret@127.0.0.1:1/weft").unwrap();
        let pool = open(refused, 1, Duration::from_millis(500), Waits::Said);
        let waiting = tokio::spawn({
            let pool = pool.clone();
            async move { pool.acquire().await.map(drop) }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!((pool.size(), pool.num_idle()), (1, 0), "the one place is taken, by a connection still opening");
        assert!(!every_connection_handed_out(&pool), "and nothing is in use");
        let e = waiting.await.unwrap().expect_err("nothing answers on port 1");
        assert!(matches!(e, sqlx::Error::PoolTimedOut), "{e}");
        assert_eq!(no_connection(&e), Some(NoConnection::Unreachable));
    }

    /// A pool this module did not open has nobody counting its
    /// connections, so its wait running out is never read as full.
    #[tokio::test]
    async fn a_pool_opened_elsewhere_is_never_read_as_full() {
        let elsewhere = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy_with(connect_options("postgres://weft:secret@127.0.0.1:1/weft").unwrap());
        assert!(!every_connection_handed_out(&elsewhere));
    }

    /// A slow acquire is a wait for capacity only when no connection
    /// opened while it waited: one that opened inside the wait means the
    /// time went to connecting (a database waking), and is not counted.
    #[test]
    fn a_slow_acquire_counts_only_when_no_connection_opened_inside_it() {
        let now = Instant::now() + Duration::from_secs(60);
        let waited = Duration::from_secs(3);
        assert!(waited_for_capacity(waited, now, None), "no connection ever opened");
        assert!(waited_for_capacity(waited, now, Some(now - Duration::from_secs(10))), "the last opened before the wait");
        assert!(!waited_for_capacity(waited, now, Some(now - Duration::from_secs(1))), "one opened inside the wait");
        assert!(!waited_for_capacity(waited, now, Some(now)), "the request's own new connection");
    }

    /// A wait that ran out in a process none of whose pools is full is a
    /// database that did not answer, never a busy one; a database that
    /// said it is full is busy, and a closed pool is neither.
    #[test]
    fn a_pool_timeout_with_no_pool_full_reads_unreachable() {
        assert_eq!(no_connection(&sqlx::Error::PoolTimedOut), Some(NoConnection::Unreachable));
        assert!(!busy(&sqlx::Error::PoolTimedOut));
        assert_eq!(no_connection(&sqlx::Error::PoolClosed), Some(NoConnection::Closed));
        assert_eq!(no_connection(&sqlx::Error::RowNotFound), None);
        let chained = anyhow::Error::from(sqlx::Error::PoolTimedOut).context("read the run");
        assert_eq!(database_no_connection(&chained), Some(NoConnection::Unreachable));
    }
}
