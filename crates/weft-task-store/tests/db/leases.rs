//! A lease (`weft_task_store::locks`): one holder at a time across every
//! process, nothing held while it works, a waiter woken the moment it is
//! given back.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use weft_task_store::locks::{with_lease, with_lease_waiting, Leased, LEASE_RELEASED_CHANNEL};
use weft_task_store::pg_signal::{Listening, PgSignalWatch};

async fn setup(pool: &PgPool) -> Arc<PgSignalWatch> {
    weft_task_store::apply_groups(pool, &[&weft_task_store::locks::GROUP]).await.expect("lease schema");
    PgSignalWatch::start(&pool.connect_options(), &[LEASE_RELEASED_CHANNEL], Listening::Always).await.expect("start the signal watch")
}

/// While one holder works, a second try skips and a waiter waits; the
/// waiter goes on as soon as the lease is given back, well before it would
/// have lapsed, and no lease row is left once both are done.
#[sqlx::test]
async fn a_lease_lets_one_holder_work_and_wakes_the_next_when_given_back(pool: PgPool) {
    let signals = setup(&pool).await;
    let (holding, release) = (tokio::sync::oneshot::channel::<()>(), tokio::sync::oneshot::channel::<()>());
    let (held_tx, held_rx) = holding;
    let (release_tx, release_rx) = release;
    let first = tokio::spawn({
        let pool = pool.clone();
        async move {
            with_lease(&pool, "front/p", || async move {
                held_tx.send(()).unwrap();
                release_rx.await.unwrap();
                anyhow::Ok(1)
            })
            .await
        }
    });
    held_rx.await.unwrap();

    let skipped = with_lease(&pool, "front/p", || async { anyhow::Ok(2) }).await.unwrap();
    let Leased::HeldFor(left) = skipped else { panic!("held by the first: a try skips, got {skipped:?}") };
    assert!(left > Duration::from_secs(20) && left <= Duration::from_millis(30_100), "the first's lease lapses within its term: {left:?}");
    assert_eq!(with_lease(&pool, "front/q", || async { anyhow::Ok(3) }).await.unwrap(), Leased::Ran(3), "another name is free");

    let waited_through = Arc::new(AtomicBool::new(false));
    let waiter = tokio::spawn({
        let (pool, signals, waited_through) = (pool.clone(), signals.clone(), waited_through.clone());
        async move {
            with_lease_waiting(&pool, &signals, "front/p", "the test's front", || async move {
                waited_through.store(true, Ordering::SeqCst);
                anyhow::Ok(4)
            })
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!waited_through.load(Ordering::SeqCst), "the waiter waits while the first holds it");

    release_tx.send(()).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), Leased::Ran(1));
    let started = std::time::Instant::now();
    assert_eq!(tokio::time::timeout(Duration::from_secs(10), waiter).await.expect("woken").unwrap().unwrap(), 4);
    assert!(started.elapsed() < Duration::from_secs(5), "woken by the release, not by the lease lapsing (30 s)");
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM lease").fetch_one(&pool).await.unwrap();
    assert_eq!(left, 0, "every lease given back");
}

/// A holder whose process died never gives its lease back: the next one
/// takes it once it lapses.
#[sqlx::test]
async fn a_lease_nobody_renews_lapses(pool: PgPool) {
    setup(&pool).await;
    sqlx::query("INSERT INTO lease (name, holder, until_ms) VALUES ('sweep/x', gen_random_uuid(), (extract(epoch from clock_timestamp()) * 1000)::bigint - 1)")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(with_lease(&pool, "sweep/x", || async { anyhow::Ok(5) }).await.unwrap(), Leased::Ran(5), "a lapsed lease is anyone's");
}

/// Sets its flag when dropped: whether a holder's work was stopped.
struct DroppedFlag(Arc<AtomicBool>);

impl Drop for DroppedFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A holder whose lease was taken from it (its renewals did not land in
/// time and it lapsed) stops its work at its next renewal and fails naming
/// the lease, so two holders never work at once.
#[sqlx::test]
async fn a_holder_that_lost_its_lease_stops_its_work(pool: PgPool) {
    setup(&pool).await;
    let stopped = Arc::new(AtomicBool::new(false));
    let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = tokio::spawn({
        let (pool, stopped) = (pool.clone(), stopped.clone());
        async move {
            with_lease(&pool, "front/p", || async move {
                let _flag = DroppedFlag(stopped);
                held_tx.send(()).unwrap();
                std::future::pending::<()>().await;
                anyhow::Ok(())
            })
            .await
        }
    });
    held_rx.await.unwrap();
    let thief = uuid::Uuid::now_v7();
    sqlx::query("UPDATE lease SET holder = $1 WHERE name = 'front/p'").bind(thief).execute(&pool).await.unwrap();

    // The first renewal comes 10 s after the take.
    let failed = tokio::time::timeout(Duration::from_secs(20), holder).await.expect("the holder stops").unwrap();
    let failed = failed.expect_err("a lost lease fails the work");
    assert!(weft_task_store::locks::lease_lost(&failed), "typed, so the task running it is tried again rather than failed: {failed:#}");
    let error = format!("{failed:#}");
    assert!(error.contains("lost the lease 'front/p'"), "{error}");
    assert!(stopped.load(Ordering::SeqCst), "the work under the lease was stopped");
    let still: uuid::Uuid = sqlx::query_scalar("SELECT holder FROM lease WHERE name = 'front/p'").fetch_one(&pool).await.unwrap();
    assert_eq!(still, thief, "giving back never deletes the new holder's lease");
}

/// A holder dropped while it works (its caller went away) still gives the
/// lease back, so a waiter goes on at once instead of after the lapse.
#[sqlx::test]
async fn a_dropped_holder_gives_its_lease_back(pool: PgPool) {
    let signals = setup(&pool).await;
    let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = tokio::spawn({
        let pool = pool.clone();
        async move {
            with_lease(&pool, "front/p", || async move {
                held_tx.send(()).unwrap();
                std::future::pending::<()>().await;
                anyhow::Ok(())
            })
            .await
        }
    });
    held_rx.await.unwrap();
    holder.abort();

    let started = std::time::Instant::now();
    let got = tokio::time::timeout(
        Duration::from_secs(10),
        with_lease_waiting(&pool, &signals, "front/p", "the test's front", || async { anyhow::Ok(6) }),
    )
    .await
    .expect("given back")
    .unwrap();
    assert_eq!(got, 6);
    assert!(started.elapsed() < Duration::from_secs(5), "given back on drop, not lapsed (30 s)");
}

/// A holder dropped while it gives its lease back (its caller went away
/// during the delete) still gives it back: the delete it was making is
/// made again, so a waiter goes on at once instead of after the lapse.
#[sqlx::test]
async fn a_holder_dropped_while_giving_back_still_gives_it_back(pool: PgPool) {
    let signals = setup(&pool).await;
    // One connection, which the work takes and hands out, so the
    // give-back after it waits for a connection until the test lets go.
    let narrow = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(60))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .expect("a pool of one");
    let (conn_tx, conn_rx) = tokio::sync::oneshot::channel();
    let mut holder = Box::pin({
        let (narrow, worker) = (narrow.clone(), narrow.clone());
        async move {
            with_lease(&narrow, "front/p", || async move {
                conn_tx.send(worker.acquire().await?).map_err(|_| anyhow::anyhow!("the test went away"))?;
                anyhow::Ok(())
            })
            .await
        }
    });
    let conn = tokio::select! {
        conn = conn_rx => conn.expect("the work hands its connection out"),
        ended = &mut holder => panic!("the holder ended before its work handed the connection out: {ended:?}"),
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut holder).await.is_err(),
        "the work is done and the give-back waits for the one connection"
    );
    drop(holder);
    drop(conn);

    let started = std::time::Instant::now();
    let got = tokio::time::timeout(
        Duration::from_secs(10),
        with_lease_waiting(&pool, &signals, "front/p", "the test's front", || async { anyhow::Ok(7) }),
    )
    .await
    .expect("given back")
    .unwrap();
    assert_eq!(got, 7);
    assert!(started.elapsed() < Duration::from_secs(5), "given back after the drop, not lapsed (30 s)");
}

/// A process whose signal watch does not listen for leases given back is
/// refused a wait at once: it would only look again on each lapse.
#[sqlx::test]
async fn waiting_without_hearing_releases_is_refused(pool: PgPool) {
    weft_task_store::apply_groups(&pool, &[&weft_task_store::locks::GROUP]).await.expect("lease schema");
    let deaf = PgSignalWatch::start(&pool.connect_options(), &["some_other_channel"], Listening::Always).await.expect("start the signal watch");
    let refused = with_lease_waiting(&pool, &deaf, "front/p", "the test's front", || async { anyhow::Ok(()) }).await;
    let error = format!("{:#}", refused.expect_err("refused"));
    assert!(error.contains(LEASE_RELEASED_CHANNEL), "{error}");
}
