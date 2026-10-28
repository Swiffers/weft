//! Advisory locks in Postgres, shared by every role that coordinates
//! through the database: a key derived from a `(domain, scope)` pair, a
//! try-lock that skips when somebody holds it, and a lock that waits for
//! its holder without holding a connection while it waits. Each is held
//! by a transaction, so it is released however the holder ends.

use crate::db::{Client as _, Pool};

/// Derive a stable i64 advisory-lock key from a `(domain, scope)`
/// pair. The domain string namespaces unrelated coordination
/// regimes so a collision in one doesn't bleed into another.
///
/// Uses FNV-1a, which is spec-stable: the same `(domain, scope)`
/// pair always derives to the same i64, regardless of rustc
/// version, std implementation, or build target. `DefaultHasher`
/// is NOT spec-stable across toolchains and using it here means
/// a rolling deploy across a rustc upgrade would split the lock
/// space mid-migration. FNV-1a ([`fnv1a_64`]) is plenty for our
/// needs (a few thousand distinct keys at most).
///
/// Collisions across (domain, scope) pairs in a 64-bit space are
/// vanishingly unlikely (birthday at ~4 billion distinct keys)
/// and harmless: two unrelated operations would serialize against
/// each other, adding latency, not breaking correctness.
pub fn advisory_key(domain: &str, scope: &str) -> i64 {
    // A null byte between domain and scope, so `advisory_key("foo",
    // "barbaz") != advisory_key("foobar", "baz")`. Do NOT drop it without
    // re-pinning advisory_key's test vectors; it changes the hash output.
    fnv1a_64(domain.bytes().chain([0]).chain(scope.bytes())) as i64
}

/// The 64-bit FNV-1a hash of `bytes`: a pure function of them, the same
/// on every build, toolchain and target (spec at
/// http://www.isthe.com/chongo/tech/comp/fnv/). Everything in the crate
/// that needs a stable hash takes it from here.
pub(crate) fn fnv1a_64(bytes: impl IntoIterator<Item = u8>) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;
    bytes.into_iter().fold(FNV_OFFSET, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME))
}

/// Run `body` while holding the TRANSACTION-SCOPED advisory lock for
/// `key`, TRY-locking. Returns `Ok(None)` immediately if another holder
/// has the lock (caller decides what skipping means), else runs `body`
/// and returns `Ok(Some(result))`.
///
/// Panic-safety is the reason this is transaction-scoped, not session-
/// scoped. `pg_advisory_lock` (session-scoped) is NOT released when a
/// pooled connection is returned to the pool, so a panic mid-`body`
/// would orphan the lock on a recycled connection and wedge every future
/// acquisition until that physical connection ages out. A
/// `pg_try_advisory_xact_lock` is held by the transaction and released
/// the instant the transaction ends, including the ROLLBACK a dropped
/// transaction issues on a panic unwind. So we hold the lock via
/// a live `Transaction` (its connection is the lock holder) while `body`
/// runs its own work on SEPARATE pool connections, then drop the
/// transaction to release. No `catch_unwind`, no orphaned lock.
/// `lock_pool` is never the pool `body` works through, for the reason
/// [`with_lock_waiting`] gives.
///
/// The lock lives in Postgres, so it serializes across N dispatcher
/// replicas.
pub async fn with_advisory_lock<T, F, Fut>(
    lock_pool: &Pool,
    key: i64,
    body: F,
) -> anyhow::Result<Option<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    // The transaction's connection holds the xact lock for as long as the
    // transaction is alive. We never write through `tx`; `body` uses the
    // pool. Dropping `tx` (normal end OR panic unwind) rolls back and
    // releases the lock.
    let mut conn = lock_pool.get().await?;
    let tx = conn.transaction().await?;
    let got: bool = tx.scalar("SELECT pg_try_advisory_xact_lock($1)", &[&key]).await?;
    if !got {
        return Ok(None);
    }
    let result = body().await;
    // Explicit rollback releases the xact lock now (rather than waiting
    // for the implicit drop-rollback); errors here are non-fatal because
    // the drop would release it anyway.
    let _ = tx.rollback().await;
    result.map(Some)
}

/// The `lease` table: what one holder across the install does right now
/// and holds for the length of it ([`with_lease`], [`with_lease_waiting`]):
/// a sweep, a project's front, a tenant's signal registrations.
pub static GROUP: crate::SchemaGroup = crate::SchemaGroup {
    name: "lease",
    tables: &["lease"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS lease (
            -- What is held (`<what>/<scope>`: `sweep/<loop>`,
            -- `front/<project>`, `signal_mount/<tenant>`).
            name TEXT PRIMARY KEY,
            -- The one holding it: a token of its own per hold, so two
            -- holds in one process are told apart too.
            holder UUID NOT NULL,
            -- Until when, in unix milliseconds on the database's clock;
            -- renewed while the holder works. Past it, anyone takes it.
            until_ms BIGINT NOT NULL
        )"#],
    seed: &[],
    data_steps: &[],
};

/// Where a lease given back is announced, its name as payload: what wakes
/// a holder waiting for it ([`with_lease_waiting`]). Every process that
/// waits on a lease listens on it.
pub const LEASE_RELEASED_CHANNEL: &str = "weft_lease_released";

/// How long a lease lasts unless renewed: how long a holder whose process
/// died holds back everyone waiting for what it held.
const LEASE_FOR: std::time::Duration = std::time::Duration::from_secs(30);
/// How often a holder still working renews its lease.
const LEASE_RENEW_EVERY: std::time::Duration = std::time::Duration::from_secs(10);
/// How long before its lease could lapse a holder that has not managed to
/// renew it stops its work: the time the stop takes to reach the work.
const LEASE_STOP_BEFORE: std::time::Duration = std::time::Duration::from_secs(5);
/// How long a holder pauses between two tries at a renewal that failed.
const LEASE_RENEW_RETRY: std::time::Duration = std::time::Duration::from_secs(1);
// A renewal has `LEASE_FOR - LEASE_RENEW_EVERY - LEASE_STOP_BEFORE` (15 s)
// to land, waiting for a connection included, before its holder stops. A
// busy database can never make a holder work on past its lease: it stops
// first ([`keep_renewed`]).
const _: () = assert!(LEASE_RENEW_EVERY.as_millis() + LEASE_STOP_BEFORE.as_millis() < LEASE_FOR.as_millis());

/// What a try at a lease that does not wait ([`with_lease`]) did.
#[derive(Debug, PartialEq, Eq)]
pub enum Leased<T> {
    /// It held the lease and ran the work, which gave this.
    Ran(T),
    /// Someone else holds it: it ends no later than this from now unless
    /// its holder renews it, so a look then finds it free or renewed.
    HeldFor(std::time::Duration),
}

/// Why a lease, rather than a lock: a lock lives in a transaction, so a
/// lock held across a long piece of work (a sweep waiting on a trigger's
/// setup, a front deploying for minutes, a provider's subscribe) holds a
/// connection that does nothing the whole time, and with the lock pool as
/// small as the process's share of the database allows, every other lock
/// of the process waits behind it. A lease is a row: nothing is held while
/// the work runs, and holding one while taking another never pins a
/// connection either. Its price is a write to take it, one every
/// [`LEASE_RENEW_EVERY`] while the work lasts, and one to give it back.
///
/// Run `body` holding the lease `name`, or skip it while someone else
/// holds it ([`Leased::HeldFor`]). A holder that cannot renew its lease
/// in time stops `body` and fails ([`held`]).
pub async fn with_lease<T, F, Fut>(pool: &Pool, name: &str, body: F) -> anyhow::Result<Leased<T>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let holder = uuid::Uuid::now_v7();
    match take(pool, name, holder).await? {
        Taken::Yes { held_until } => Ok(Leased::Ran(held(pool, name, holder, held_until, body).await?)),
        Taken::HeldFor(left_ms) => Ok(Leased::HeldFor(lapse_wait(left_ms))),
    }
}

/// Run `body` holding the lease `name`, waiting for whoever holds it now.
/// `waiting_for` names what is waited on, for the breadcrumb a long wait
/// leaves. The wait holds no connection: it is woken when the lease is
/// given back ([`LEASE_RELEASED_CHANNEL`], heard through `signals`, which
/// must listen on it, or this is refused) and looks again when the
/// holder's lease would lapse, in case its process died. No deadline: a
/// holder ends, or its lease lapses. A holder that cannot renew its lease
/// in time stops `body` and fails ([`held`]).
pub async fn with_lease_waiting<T, F, Fut>(
    pool: &Pool,
    signals: &crate::pg_signal::PgSignalWatch,
    name: &str,
    waiting_for: &str,
    body: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    // A watch that does not listen would only wake this on each lapse, so
    // every wait would last a whole lease.
    signals.require(LEASE_RELEASED_CHANNEL).map_err(|e| e.context(format!("wait for {waiting_for}")))?;
    let holder = uuid::Uuid::now_v7();
    let waiting_since = std::time::Instant::now();
    let mut said_at = std::time::Duration::ZERO;
    loop {
        // Subscribed before the look, so a release between the two is heard.
        let mut heard = signals.subscribe();
        let left_ms = match take(pool, name, holder).await.map_err(|e| e.context(format!("wait for {waiting_for}")))? {
            Taken::Yes { held_until } => return held(pool, name, holder, held_until, body).await,
            Taken::HeldFor(left_ms) => left_ms,
        };
        let lapses_in = lapse_wait(left_ms);
        let deadline = tokio::time::Instant::now() + lapses_in.min(WAITING_BREADCRUMB);
        heard.woken_before(deadline, |channel, payload| channel == LEASE_RELEASED_CHANNEL && payload == name).await?;
        let waited = waiting_since.elapsed();
        if waited.saturating_sub(said_at) >= WAITING_BREADCRUMB {
            said_at = waited;
            tracing::info!(target: "weft_task_store::locks", waited_secs = waited.as_secs(), "still waiting for {waiting_for}");
        }
    }
}

/// How long a waiter waits before looking again at a lease that lapses in
/// `left_ms` (as the database counted it): until just after it would
/// lapse, so a holder whose process died is waited for no longer than its
/// lease. The extra moment covers the time the answer took to arrive.
fn lapse_wait(left_ms: i64) -> std::time::Duration {
    std::time::Duration::from_millis(u64::try_from(left_ms).unwrap_or(0)) + std::time::Duration::from_millis(100)
}

/// What a try at a lease found.
enum Taken {
    /// Taken, and surely held until `held_until` on this process's clock.
    Yes { held_until: tokio::time::Instant },
    /// Someone holds it, for this many more milliseconds unless renewed.
    HeldFor(i64),
}

async fn take(pool: &Pool, name: &str, holder: uuid::Uuid) -> anyhow::Result<Taken> {
    let lease_ms = i64::try_from(LEASE_FOR.as_millis()).expect("seconds fit");
    // The connection first, so the time spent waiting for one is not
    // counted into the lease: it starts when the statement is sent.
    let conn = pool.get().await?;
    let sent_at = tokio::time::Instant::now();
    // One statement: take it when free or lapsed, else read how long it
    // is held for still.
    let (mine, left_ms): (bool, i64) = conn
        .opt(
            &format!(
                "WITH took AS ( \
                   INSERT INTO lease (name, holder, until_ms) VALUES ($1, $2, {now} + $3) \
                   ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, until_ms = EXCLUDED.until_ms \
                   WHERE lease.until_ms < {now} RETURNING until_ms) \
                 SELECT TRUE, 0::bigint FROM took \
                 UNION ALL SELECT FALSE, until_ms - {now} FROM lease WHERE name = $1 AND NOT EXISTS (SELECT 1 FROM took)",
                now = crate::drain::DB_NOW_MS
            ),
            &[&name, &holder, &lease_ms],
        )
        .await?
    // Someone else's first take of this name committed after this
    // statement's snapshot: its row conflicts with the insert, yet the read
    // cannot see it. Held, for a term this cannot tell, so a waiter looks
    // again in a moment ([`lapse_wait`] of zero).
    .unwrap_or((false, 0));
    Ok(if mine { Taken::Yes { held_until: sent_at + LEASE_FOR } } else { Taken::HeldFor(left_ms) })
}

/// Run `body` under the lease `holder` just took (surely held until
/// `held_until`), renewing it while `body` runs, and give it back after,
/// announcing it ([`LEASE_RELEASED_CHANNEL`]).
///
/// The lease is what keeps a second holder out, so a holder that can no
/// longer show it still holds it stops: when a renewal finds the lease
/// gone, or cannot land before the lease could lapse (a database too busy
/// to lend a connection in time), `body` is dropped where it stands and
/// this fails with a [`LeaseLost`] naming the lease. Only the one piece of
/// work it guarded stops, and is tried again by whoever runs it. Given
/// back however this ends, dropped by its caller included ([`GiveBack`]).
async fn held<T, F, Fut>(
    pool: &Pool,
    name: &str,
    holder: uuid::Uuid,
    held_until: tokio::time::Instant,
    body: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let give_back = GiveBack { pool: pool.clone(), name: name.to_string(), holder, armed: true };
    let result = tokio::select! {
        result = body() => result,
        lost = keep_renewed(pool, name, holder, held_until) => Err(anyhow::Error::new(lost)),
    };
    give_back.now().await;
    result
}

/// Renew the lease for as long as its holder works; what this returns is
/// why the holder must stop. Each renewal is sent after its connection is
/// in hand, and the database starts the new term when it runs it, so the
/// lease is surely held for [`LEASE_FOR`] from the sending. A renewal that
/// fails is tried again, until [`LEASE_STOP_BEFORE`] ahead of that.
async fn keep_renewed(pool: &Pool, name: &str, holder: uuid::Uuid, mut held_until: tokio::time::Instant) -> LeaseLost {
    let lease_ms = i64::try_from(LEASE_FOR.as_millis()).expect("seconds fit");
    loop {
        tokio::time::sleep_until(held_until - LEASE_FOR + LEASE_RENEW_EVERY).await;
        let give_up_at = held_until - LEASE_STOP_BEFORE;
        let mut failed = None;
        held_until = loop {
            match tokio::time::timeout_at(give_up_at, renew(pool, name, holder, lease_ms)).await {
                Ok(Ok(Some(sent_at))) => break sent_at + LEASE_FOR,
                Ok(Ok(None)) => {
                    return LeaseLost(format!(
                        "lost the lease '{name}' while working under it: it was not renewed in time and someone else \
                         took it, so this work stopped rather than run beside theirs; try it again"
                    ))
                }
                Ok(Err(e)) => failed = Some(e),
                Err(_) => {}
            }
            let pause_until = tokio::time::Instant::now() + LEASE_RENEW_RETRY;
            if pause_until >= give_up_at {
                let why = failed.map_or_else(|| "the database did not answer in time".to_string(), |e| format!("{e:#}"));
                return LeaseLost(format!(
                    "stopped work under the lease '{name}': the lease could not be renewed before it would lapse \
                     ({why}), and working on past it could run beside whoever takes it next; try it again once the \
                     database answers"
                ));
            }
            tokio::time::sleep_until(pause_until).await;
        };
    }
}

/// Why work under a lease was stopped: its holder could no longer show it
/// held the lease (a renewal found it gone, or could not land before it
/// could lapse). The work itself did not fail, so whoever runs it tries it
/// again rather than failing it: the task executor gives such a task back
/// for its next claim ([`lease_lost`]), as it does a task whose own claim
/// could not be renewed.
#[derive(Debug)]
pub struct LeaseLost(pub(crate) String);

impl std::fmt::Display for LeaseLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LeaseLost {}

/// Whether `e` stopped because a lease its work ran under was lost
/// ([`LeaseLost`]), anywhere in its chain.
pub fn lease_lost(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| cause.is::<LeaseLost>())
}

/// One renewal: when it was sent, or `None` when the lease is no longer
/// this holder's.
async fn renew(pool: &Pool, name: &str, holder: uuid::Uuid, lease_ms: i64) -> anyhow::Result<Option<tokio::time::Instant>> {
    let conn = pool.get().await?;
    let sent_at = tokio::time::Instant::now();
    let renewed = conn
        .execute(
            &format!("UPDATE lease SET until_ms = {} + $3 WHERE name = $1 AND holder = $2", crate::drain::DB_NOW_MS),
            &[&name, &holder, &lease_ms],
        )
        .await?;
    Ok((renewed == 1).then_some(sent_at))
}

/// Gives a lease back however its holder ends. Awaited when the work ends
/// ([`GiveBack::now`]), so a waiter is woken before the holder's caller
/// goes on; when the holder is dropped instead (its caller went away, a
/// request whose client disconnected), the give-back is spawned, so
/// waiters are not held back until the lease lapses.
struct GiveBack {
    pool: Pool,
    name: String,
    holder: uuid::Uuid,
    armed: bool,
}

impl GiveBack {
    /// Disarmed only once the give-back finished: a holder dropped during
    /// it cancels the delete, and Drop then gives it back instead.
    async fn now(mut self) {
        give_back(&self.pool, &self.name, self.holder).await;
        self.armed = false;
    }
}

impl Drop for GiveBack {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (pool, name, holder) = (self.pool.clone(), std::mem::take(&mut self.name), self.holder);
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { give_back(&pool, &name, holder).await });
            }
            Err(_) => tracing::warn!(target: "weft_task_store::locks", lease = name, "a lease's holder ended outside a runtime; it lapses on its own"),
        }
    }
}

/// Delete the lease if `holder` still has it, and announce it given back.
async fn give_back(pool: &Pool, name: &str, holder: uuid::Uuid) {
    let given_back = pool
        .execute(
            &format!(
                "WITH gone AS (DELETE FROM lease WHERE name = $1 AND holder = $2 RETURNING name) \
                 SELECT pg_notify('{LEASE_RELEASED_CHANNEL}', name) FROM gone"
            ),
            &[&name, &holder],
        )
        .await;
    if let Err(e) = given_back {
        tracing::warn!(target: "weft_task_store::locks", lease = name, error = %e, "could not give a lease back; it lapses on its own");
    }
}

/// How often a caller waiting in [`with_lock_waiting`] says so.
///
/// There is no deadline on the wait: a holder whose process dies drops
/// the lock with its connection, so a wait ends on its own, and a
/// deadline could only fail work that was merely queued. What a long wait
/// gets instead is a breadcrumb at this interval, so it is visible.
const WAITING_BREADCRUMB: std::time::Duration = std::time::Duration::from_secs(20);

/// Hold the lock for `key`, waiting behind its current holder, while
/// `body` runs. `waiting_for` names what is waited on, for the breadcrumb
/// a long wait leaves.
///
/// `lock_pool` is a pool of its own, never the one `body` works through.
/// An advisory lock lives in a transaction, so holding one holds a
/// connection that then does nothing, while every query the locked body
/// makes takes another. From one shared pool that is a deadlock waiting
/// for a busy day: as many concurrent holders as the pool is wide, none
/// able to get a connection to do its work, and so none able to finish
/// and release.
///
/// The wait POLLS a try-lock instead of blocking inside Postgres. A
/// blocking `pg_advisory_xact_lock` waits in the server, so every waiter
/// would pin a connection of the lock pool for as long as it waits, and a
/// burst on one key would exhaust the lock pool for every other key. A
/// waiter holds nothing between attempts, and a lock pool all of whose
/// connections are held by holders is waited out the same way. Polling
/// has no queue, so a waiter is not served in its turn; the backoff stays
/// short so one that has waited long is never far behind a newcomer.
pub async fn with_lock_waiting<T, F, Fut>(
    lock_pool: &Pool,
    key: i64,
    waiting_for: &str,
    body: F,
) -> anyhow::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let waiting_since = std::time::Instant::now();
    let mut said_at = std::time::Duration::ZERO;
    let mut backoff = std::time::Duration::from_millis(5);
    loop {
        match lock_pool.get().await {
            Ok(mut conn) => {
                let tx = conn.transaction().await.map_err(|e| anyhow::Error::from(e).context(format!("wait for {waiting_for}")))?;
                let got: bool = tx
                    .scalar("SELECT pg_try_advisory_xact_lock($1)", &[&key])
                    .await
                    .map_err(|e| anyhow::Error::from(e).context(format!("wait for {waiting_for}")))?;
                if got {
                    let result = body().await;
                    // The xact lock dies with the transaction, panic or not.
                    let _ = tx.rollback().await;
                    return result;
                }
                // Rolled back BEFORE the sleep, so the connection goes
                // back to the pool while this caller waits.
                let _ = tx.rollback().await;
            }
            // A full pool: every lock connection is handed out to a
            // holder, each of which ends, so wait for one like for the
            // lock. Otherwise the wait went to opening a connection that
            // never came (the database does not answer), which is no
            // wait: the error goes back.
            Err(e) if crate::db::busy(&e) => {}
            Err(e) => return Err(anyhow::Error::from(e).context(format!("wait for {waiting_for}"))),
        }
        let waited = waiting_since.elapsed();
        if waited.saturating_sub(said_at) >= WAITING_BREADCRUMB {
            said_at = waited;
            tracing::info!(target: "weft_task_store::locks", waited_secs = waited.as_secs(), "still waiting for {waiting_for}");
        }
        // Jittered, so a burst of waiters on one key does not retry in
        // lockstep on the same tick.
        let jitter = std::time::Duration::from_micros(
            (uuid::Uuid::new_v4().as_u128() as u64) % (backoff.as_micros() as u64).max(1),
        );
        tokio::time::sleep(backoff + jitter).await;
        backoff = (backoff * 2).min(std::time::Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the FNV-1a output values. FNV-1a is spec-stable so
    /// these values are fixed across toolchains, OSes, and build
    /// targets. If any of these assertions fail, the
    /// implementation of `advisory_key` has been changed in a
    /// way that breaks lock-space compatibility with deployed
    /// processes. Roll forward only after confirming no two processes
    /// running different implementations coexist.
    #[test]
    fn advisory_key_pinned_values() {
        assert_eq!(advisory_key("weft_supervisor_coord", "tenant-a"), 5099131965359238650);
        assert_eq!(advisory_key("weft_project_transition", "00000000-0000-0000-0000-000000000042"), 8667617249734746809);
    }

    /// Domain separation: same scope, different domain, different key.
    #[test]
    fn advisory_key_domains_separate() {
        assert_ne!(advisory_key("weft_reaper", "tenant-a"), advisory_key("some_other_domain", "tenant-a"));
    }

    /// A lock waiter on a database that does not answer gets the timeout
    /// back instead of waiting for good: the lock pool's one place is
    /// taken by a connection still opening, which is not a holder.
    #[tokio::test]
    async fn a_lock_wait_on_a_database_that_does_not_answer_fails() {
        // Port 1 on this machine refuses at once; the pool keeps trying
        // until the wait ends.
        let refused = crate::db::config("postgres://weft:secret@127.0.0.1:1/weft").unwrap();
        let lock_pool = Pool::open(refused, 1, std::time::Duration::from_millis(300), crate::db::Waits::Expected);
        let waited = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            with_lock_waiting(&lock_pool, 42, "a lock", || async { anyhow::Ok(()) }),
        )
        .await
        .expect("the wait gave up rather than looping");
        let e = waited.expect_err("nothing answers on port 1");
        assert_eq!(crate::db::database_no_connection(&e), Some(crate::db::NoConnection::Unreachable), "{e:#}");
    }
}
