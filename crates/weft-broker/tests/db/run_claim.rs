//! Layer-3 contract test for a worker claiming a queued run
//! (`weft_journal::record::claim`, behind `POST /v1/run/claim`), against
//! a REAL Postgres: the claim and the end of what `weft status` shows of
//! the project's workers commit together.

use sqlx::PgPool;
use weft_dispatcher::store::RunStore;
use weft_journal::RunEvent;
use weft_task_store::unanswered::{failed, Callee};

const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const TENANT: &str = "t1";

/// The schema, the project, and one run of it queued.
async fn queued(pool: &PgPool) -> weft_core::RunId {
    queued_running(pool, Some("def-1"), Some("bin-1")).await
}

/// [`queued`], running the program `definition_hash` on the build
/// `binary_hash` (`None` both for a run of no program, a node test's).
async fn queued_running(pool: &PgPool, definition_hash: Option<&str>, binary_hash: Option<&str>) -> weft_core::RunId {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    sqlx::query(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at, project_json_version, resync_version, running_build_inputs_version) \
         VALUES ($1, 'p', $2, 'registered', '{}', 0, $3, 1, 1)",
    )
    .bind(PROJECT)
    .bind(TENANT)
    .bind(weft_core::ProjectDefinition::STORED.version())
    .execute(pool)
    .await
    .expect("project row");
    let run_id = weft_core::new_run_id();
    let birth = RunEvent::RunStarted {
        given: Default::default(),
        run_id,
        project_id: PROJECT,
        entry_node: "route".into(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: definition_hash.map(str::to_string),
        binary_hash: binary_hash.map(str::to_string),
        source_version: None,
        run_kind: weft_core::run::RunKind::Project,
        cut_from: None,
        seed: None,
        instance: None,
        stand_in: None,
        fired_trigger: None,
        instance_values: Default::default(),
        picks: Default::default(),
        settings: Default::default(),
        at_unix: 0,
    };
    let queued = weft_journal::record::Queued {
        events: std::slice::from_ref(&birth),
        tenant: TENANT,
        keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: false,
        stale: &[],
        spec: None,
        example: None,
        program: None,
    };
    let store = weft_dispatcher::store::postgres::PostgresStore::from_pool(pool.clone());
    assert!(store.queue_run(queued, false).await.expect("queue the run"));
    run_id
}

/// The run's state, epoch and mark that its workers failed it.
async fn standing(pool: &PgPool, run_id: weft_core::RunId) -> (String, i32, bool) {
    sqlx::query_as("SELECT state, epoch, waited_on_workers FROM run WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .expect("the run's row")
}

/// Whether `weft status` has the project's workers written down as failing.
async fn workers_written(pool: &PgPool) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM unanswered_call WHERE callee = $1)")
        .bind(format!("workers:{PROJECT}"))
        .fetch_one(pool)
        .await
        .unwrap()
}

/// A run its workers answered busy is claimed: the claim and the end of
/// what is written of its workers land together. When the clearing
/// fails, the claim is not kept either (the run is still queued, its mark
/// and the failure still there), so a claim never stands while
/// `weft status` keeps saying the project waits on its workers.
#[sqlx::test]
async fn a_claim_and_the_end_of_its_workers_failure_land_together(pool: PgPool) {
    let run_id = queued(&pool).await;
    sqlx::query("UPDATE run SET busy_since = 1, waited_on_workers = TRUE WHERE run_id = $1")
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
    failed(&pool, Callee::Workers(PROJECT), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    let before = standing(&pool, run_id).await;
    assert_eq!((before.0.as_str(), before.2), ("queued", true));

    // The clearing fails: nothing of the claim is kept.
    sqlx::query("CREATE FUNCTION refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused'; END $$")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER refuse BEFORE DELETE ON unanswered_call FOR EACH ROW EXECUTE FUNCTION refuse()")
        .execute(&pool)
        .await
        .unwrap();
    let mut conn = pool.acquire().await.unwrap();
    assert!(weft_journal::record::claim(&mut conn, run_id, PROJECT, "w1").await.is_err());
    assert_eq!(standing(&pool, run_id).await, before, "the claim went with its clearing");
    assert!(workers_written(&pool).await);

    // The clearing goes through: both are kept.
    sqlx::query("DROP TRIGGER refuse ON unanswered_call").execute(&pool).await.unwrap();
    let claimed = weft_journal::record::claim(&mut conn, run_id, PROJECT, "w1").await.unwrap().expect("claimed");
    assert_eq!(standing(&pool, run_id).await, ("running".to_string(), claimed.epoch, false));
    assert!(!workers_written(&pool).await, "the last run its workers failed reached one");
}

/// A run of no program is never claimed: the claim says so and writes
/// nothing, so the run is left with no owner to wait out.
#[sqlx::test]
async fn a_run_of_no_program_is_refused_with_nothing_claimed(pool: PgPool) {
    let run_id = queued_running(&pool, None, None).await;
    let before = standing(&pool, run_id).await;
    let mut conn = pool.acquire().await.unwrap();
    let refused = weft_journal::record::claim(&mut conn, run_id, PROJECT, "w1").await.unwrap_err();
    assert!(format!("{refused:#}").contains("runs no program"), "{refused:#}");
    assert_eq!(standing(&pool, run_id).await, before, "nothing of the claim was written");
    let owner: Option<String> = sqlx::query_scalar("SELECT owner FROM run WHERE run_id = $1").bind(run_id).fetch_one(&pool).await.unwrap();
    assert_eq!(owner, None);
}

/// A run's program and its build are one fact: the database refuses a
/// row naming one without the other, since delivery decides on the build
/// and a claim on the program.
#[sqlx::test]
async fn a_run_naming_its_program_without_its_build_is_refused(pool: PgPool) {
    let run_id = queued(&pool).await;
    for column in ["binary_hash", "definition_hash"] {
        let refused = sqlx::query(weft_task_store::sql!("UPDATE run SET {column} = NULL WHERE run_id = $1"))
            .bind(run_id)
            .execute(&pool)
            .await
            .unwrap_err();
        assert!(refused.to_string().contains("run_program_identity_whole"), "{column}: {refused}");
    }
}
