//! Layer-3 tests for how the dispatcher holds a task it claimed, against a
//! REAL Postgres: a result kept on a task still claimed, and a claim given
//! back. It all lives IN the SQL, so a faked store would not catch what
//! these catch.

use crate::support;

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_task_store::tasks;

use support::{claimed_task, setup};

fn dispatcher_task(dedup: &str) -> tasks::NewTask {
    tasks::NewTask {
        kind: weft_task_store::TaskKind::RunNodeTest,
        project_id: Some(Uuid::from_u128(0x1)),
        dedup_key: Some(dedup.to_string()),
        run_id: None,
        tenant_id: "tenant-1".to_string(),
        payload: json!({}),
    }
}

/// The partial-result surface a non-re-runnable executor uses: a
/// still-claimed row records its harvested result without completing,
/// guarded on the claimant; a later read returns it; a write from a
/// claimant that no longer holds the claim fails loudly.
#[sqlx::test]
async fn partial_result_round_trips_on_the_task_row(pool: PgPool) {
    setup(&pool).await;
    let task_id = tasks::enqueue(&pool, dispatcher_task("t1")).await.expect("enqueue");
    assert_eq!(tasks::stored_result(&pool, task_id).await.expect("read"), None);
    let report = json!({"passed": true, "node": "X", "test": "t"});
    assert!(tasks::store_result_partial(&pool, task_id, "disp-1", &report).await.is_err(), "unclaimed");
    let claimed = claimed_task(&pool, "disp-1").await.expect("claim").expect("the task");
    assert_eq!(claimed.id, task_id);
    assert_eq!(claimed.attempts, 1, "first claim");
    assert!(tasks::store_result_partial(&pool, task_id, "disp-2", &report).await.is_err(), "not the claimant");
    tasks::store_result_partial(&pool, task_id, "disp-1", &report).await.expect("store partial");
    assert_eq!(tasks::stored_result(&pool, task_id).await.expect("read"), Some(report));
    let (status,): (String,) = sqlx::query_as("SELECT status FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!(status, "claimed", "recording is not completing");
    assert_eq!(tasks::stored_result(&pool, Uuid::new_v4()).await.expect("read"), None);
}

/// Surrender-requeue: the claimer that can no longer renew its lease puts
/// the row back to `pending`, and a requeue from a claimant that lost the
/// row is a no-op that never clobbers the new claim.
#[sqlx::test]
async fn surrender_requeues_only_while_claim_is_ours(pool: PgPool) {
    setup(&pool).await;
    let task_id = tasks::enqueue(&pool, dispatcher_task("t1")).await.expect("enqueue");
    claimed_task(&pool, "disp-1").await.expect("claim").expect("the task");
    assert!(!tasks::surrender(&pool, task_id, "disp-2").await.expect("requeue"));
    assert!(tasks::surrender(&pool, task_id, "disp-1").await.expect("requeue"));
    let (status, claimed_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, claimed_by FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!((status.as_str(), claimed_by), ("pending", None));
    let reclaimed = claimed_task(&pool, "disp-2").await.expect("claim").expect("requeued");
    assert_eq!(reclaimed.attempts, 2);
    assert!(!tasks::surrender(&pool, task_id, "disp-1").await.expect("requeue"));
    let (status, claimed_by): (String, Option<String>) =
        sqlx::query_as("SELECT status, claimed_by FROM task WHERE id = $1").bind(task_id).fetch_one(&pool).await.unwrap();
    assert_eq!((status.as_str(), claimed_by.as_deref()), ("claimed", Some("disp-2")));
}

/// The picker run dry looks again when the earliest claim it may have to
/// rescue lapses, and sleeps until a task is announced when none is out.
/// Running dry, it prunes finished tasks older than an hour, and only
/// those; an enqueue prunes nothing.
#[sqlx::test]
async fn a_dry_picker_prunes_and_looks_at_its_earliest_claim_lapse(pool: PgPool) {
    use std::sync::Arc;
    use weft_task_store::drain::DrainStep;
    setup(&pool).await;
    let store = Arc::new(weft_task_store::PostgresTaskStoreClient::new(pool.clone(), support::signals(&pool).await).unwrap());
    // This picker only ever finds nothing to claim: no executor runs.
    struct Unused;
    #[async_trait::async_trait]
    impl weft_task_store::TaskExecutor<()> for Unused {
        async fn execute(&self, _: &(), task: &tasks::Task) -> anyhow::Result<serde_json::Value> {
            anyhow::bail!("this test claims no task, yet {} ran", task.id)
        }
    }
    let registry = weft_task_store::TaskRegistry::<()>::every_kind(|_| Arc::new(Unused));
    let picker = weft_task_store::dispatcher_picker_loop(store, (), registry, "disp-1".into());
    assert_eq!((picker.drain)().await.unwrap(), DrainStep::Done, "no claim out");

    let old = tasks::enqueue(&pool, dispatcher_task("old")).await.unwrap();
    let recent = tasks::enqueue(&pool, dispatcher_task("recent")).await.unwrap();
    for (id, at) in [(old, tasks::TERMINAL_RETENTION_SECS * 2), (recent, 10)] {
        sqlx::query("UPDATE task SET status = 'complete', completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT - $2 WHERE id = $1")
            .bind(id)
            .bind(at)
            .execute(&pool)
            .await
            .unwrap();
    }
    let held = tasks::enqueue(&pool, dispatcher_task("held")).await.unwrap();
    sqlx::query("UPDATE task SET status = 'claimed', claimed_by = 'disp-2', claimed_until_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + 600 WHERE id = $1")
        .bind(held)
        .execute(&pool)
        .await
        .unwrap();
    let DrainStep::RetryIn(after) = (picker.drain)().await.unwrap() else { panic!("a claim is out") };
    assert!(after.as_secs() > 590 && after.as_secs() <= 602, "looks once the claim could lapse: {after:?}");
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM task ORDER BY id").fetch_all(&pool).await.unwrap();
    assert_eq!(left, vec![recent, held], "only the task finished over an hour ago went");

    sqlx::query("UPDATE task SET completed_at_unix = completed_at_unix - $2 WHERE id = $1")
        .bind(recent)
        .bind(tasks::TERMINAL_RETENTION_SECS)
        .execute(&pool)
        .await
        .unwrap();
    let tasks::DedupOutcome::Inserted(deduped) = tasks::enqueue_dedup(&pool, dispatcher_task("deduped")).await.unwrap() else {
        panic!("a new task")
    };
    let plain = tasks::enqueue(&pool, dispatcher_task("plain")).await.unwrap();
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM task ORDER BY id").fetch_all(&pool).await.unwrap();
    assert_eq!(left, vec![recent, held, deduped, plain], "an enqueue prunes nothing");
    sqlx::query("UPDATE task SET status = 'complete', completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT WHERE id = ANY($1)")
        .bind(vec![deduped, plain])
        .execute(&pool)
        .await
        .unwrap();
    let DrainStep::RetryIn(_) = (picker.drain)().await.unwrap() else { panic!("a claim is out") };
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM task ORDER BY id").fetch_all(&pool).await.unwrap();
    assert_eq!(left, vec![held, deduped, plain], "the next dry pass prunes what aged since");
}
