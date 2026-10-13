//! Layer-3 contract tests for the broker's fenced lifecycle writes
//! (`lifecycle_writes`) against a REAL Postgres. The fence, the roster
//! membership check, the rollup and the Displaced-versus-Gone answer
//! all live IN SQL (one UPDATE evaluated on one row snapshot), so the
//! supervisor's in-memory fake cannot prove them; these tests drive the
//! exact statements the handlers serve. Also proves the repair statement
//! `decode_units_json` prints actually heals a bricked row.
//!
//! Gated behind `db-tests` (off by default) so a plain `cargo test` needs no PG.
#![cfg(feature = "db-tests")]

use sqlx::PgPool;

use weft_broker::lifecycle_writes::{
    cancels_for, claim_next, complete_command, health_state, issue_command, record_event, set_health_state, set_status,
    set_waiting, sync_ownership, unowned_work_waiting, FencedWrite, IssuedCommand,
};
use weft_broker_client::protocol::{
    decode_units_json, InfraLifecycleVerb, units_json_repair_sql, FailureStage, HealthProtocolState, InfraNodeStatus as Status,
    SupervisorCommandCompleteRequest, SupervisorSetHealthStateRequest, SupervisorSetStatusRequest, SupervisorSetWaitingRequest,
    SupervisorSyncOwnershipRequest,
};

const TENANT: &str = "t1";
const PROJECT: uuid::Uuid = uuid::Uuid::from_u128(1);
const NODE: &str = "bridge";
const OWNER: &str = "supervisor-a";
const OTHER: &str = "supervisor-b";

async fn schema(pool: &PgPool) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
}

/// `process` holds the project's `infra_owner` lease for the next hour.
async fn lease(pool: &PgPool, replica: &str) {
    lease_of(pool, PROJECT, replica).await
}

/// `process` holds `project`'s `infra_owner` lease for the next hour.
async fn lease_of(pool: &PgPool, project: uuid::Uuid, replica: &str) {
    sqlx::query(
        "INSERT INTO infra_owner (project_id, supervisor_replica, tenant_id, leased_until_unix) \
         VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT + 3600) \
         ON CONFLICT (project_id) DO UPDATE SET supervisor_replica = EXCLUDED.supervisor_replica, \
         leased_until_unix = EXCLUDED.leased_until_unix",
    )
    .bind(project)
    .bind(replica)
    .bind(TENANT)
    .execute(pool)
    .await
    .expect("lease");
}

/// An uncompleted lifecycle command targeting the node; returns its id.
async fn command(pool: &PgPool) -> i64 {
    command_of(pool, PROJECT).await
}

/// An uncompleted lifecycle command of `project`; returns its id.
async fn command_of(pool: &PgPool, project: uuid::Uuid) -> i64 {
    command_on(pool, project, None).await
}

/// An uncompleted lifecycle command of `project` on one copy of the node:
/// the shared one (`None`) or an instance's.
async fn command_on(pool: &PgPool, project: uuid::Uuid, instance: Option<&str>) -> i64 {
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_replica, issued_at_unix, instance_id) \
         VALUES ($1, $2, $3, 'stop', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT, $4) \
         RETURNING id",
    )
    .bind(TENANT)
    .bind(project)
    .bind(NODE)
    .bind(instance)
    .fetch_one(pool)
    .await
    .expect("command");
    id
}

/// A `project` row; `has_infra` false = no supervisor may take it (it
/// declares no infra and runs none).
async fn project_row(pool: &PgPool, id: uuid::Uuid, has_infra: bool) {
    sqlx::query(
        "INSERT INTO project (id, name, tenant_id, status, project_json, updated_at, has_infra) \
         VALUES ($1, 'p', 'tenant', 'inactive', '{}', 0, $2)",
    )
    .bind(id)
    .bind(has_infra)
    .execute(pool)
    .await
    .expect("project row");
}

fn unit(status: &str) -> serde_json::Value {
    serde_json::json!({
        "status": status,
        "stop_behavior": { "kind": "stop" },
        "flaky_after_seconds": 30,
        "recovery_after_seconds": 30
    })
}

async fn node_row(pool: &PgPool, status: &str, units: serde_json::Value) {
    sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, units_json) \
         VALUES ($1, $2, 'inst1', $3, $4)",
    )
    .bind(PROJECT)
    .bind(NODE)
    .bind(status)
    .bind(units)
    .execute(pool)
    .await
    .expect("node row");
}

async fn row(pool: &PgPool) -> (String, serde_json::Value) {
    sqlx::query_as("SELECT status, units_json FROM infra_node WHERE project_id = $1 AND node_id = $2")
        .bind(PROJECT)
        .bind(NODE)
        .fetch_one(pool)
        .await
        .expect("row")
}

fn stamp(replica: &str, command_id: Option<i64>, unit: Option<&str>, status: Status) -> SupervisorSetStatusRequest {
    SupervisorSetStatusRequest {
        replica: replica.into(),
        command_id,
        project_id: PROJECT,
        node_id: NODE.into(),
        unit: unit.map(String::from),
        status,
        failure_stage: None,
        failure_message: None,
        instance: None,
        not_ready_since_unix: None,
        ready_since_unix: None,
    }
}

/// An ordinary ownership sync of `replica`: no copies on its host, nothing
/// settled, not ending.
fn syncing(replica: &str) -> SupervisorSyncOwnershipRequest {
    SupervisorSyncOwnershipRequest {
        replica: replica.into(),
        held_projects: Some(vec![]),
        looked: vec![],
        settled: vec![],
        failed: vec![],
        gave_up: vec![],
        all_failed: None,
        ending: false,
    }
}

/// A per-unit stamp patches that unit and re-rolls the node status
/// from the UPDATED roster (Flaky outranks Running; the stamped unit's
/// old status is not what gets rolled up).
#[sqlx::test]
async fn per_unit_stamp_patches_the_unit_and_rolls_up_the_new_roster(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running"), "b": unit("running") })).await;

    let out = set_status(&pool, &stamp(OWNER, Some(cmd), Some("a"), Status::Flaky)).await.unwrap();
    assert_eq!(out, FencedWrite::Applied);
    let (status, units) = row(&pool).await;
    assert_eq!(status, "flaky");
    assert_eq!(units["a"]["status"], "flaky");
    assert_eq!(units["b"]["status"], "running");
    // The patch kept every other field of the entry.
    assert_eq!(units["a"]["stop_behavior"]["kind"], "stop");
}

/// A per-unit stamp for a unit the roster does not carry is Gone and
/// writes NOTHING: no status-only stub entry (the corruption that
/// bricked every decode of the row), no status change.
#[sqlx::test]
async fn per_unit_stamp_on_a_missing_unit_is_gone_and_writes_nothing(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;

    let out = set_status(&pool, &stamp(OWNER, Some(cmd), Some("ghost"), Status::Stopped)).await.unwrap();
    assert_eq!(out, FencedWrite::Gone);
    let (status, units) = row(&pool).await;
    assert_eq!(status, "running");
    assert_eq!(units, serde_json::json!({ "a": unit("running") }));
    // The autonomous (no command) branch answers the same.
    let out = set_status(&pool, &stamp(OWNER, None, Some("ghost"), Status::Running)).await.unwrap();
    assert_eq!(out, FencedWrite::Gone);
}

/// A stamp by a process that does not hold the lease is Displaced (not
/// Gone: the target is there, the writer is not the owner), and the
/// row is untouched.
#[sqlx::test]
async fn stamp_by_a_displaced_replica_is_displaced(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;

    let out = set_status(&pool, &stamp(OTHER, Some(cmd), Some("a"), Status::Stopped)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
    // Node-wide from the displaced process: same answer.
    let out = set_status(&pool, &stamp(OTHER, Some(cmd), None, Status::Terminating)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
}

/// What an apply waits on is recorded by the owner under a command still
/// under way, refused (the row untouched) for a process that lost the
/// project, and cleared, with when it began, once the copy leaves
/// provisioning.
#[sqlx::test]
async fn what_an_apply_waits_on_is_its_own_and_ends_with_it(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let stop = command(&pool).await;
    let (cmd,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command (tenant_id, project_id, node_id, verb, issued_by_replica, issued_at_unix) \
         VALUES ($1, $2, $3, 'apply', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT) RETURNING id",
    )
    .bind(TENANT)
    .bind(PROJECT)
    .bind(NODE)
    .fetch_one(&pool)
    .await
    .unwrap();
    node_row(&pool, "provisioning", serde_json::json!({ "a": unit("provisioning") })).await;
    sqlx::query("UPDATE infra_node SET provisioning_since_unix = 100 WHERE project_id = $1").bind(PROJECT).execute(&pool).await.unwrap();
    let waiting = |replica: &str, text: &str| SupervisorSetWaitingRequest {
        replica: replica.into(),
        command_id: cmd,
        project_id: PROJECT,
        node_id: NODE.into(),
        instance: None,
        waiting: text.into(),
    };
    let read = || async {
        let (w,): (Option<String>,) = sqlx::query_as("SELECT waiting_on FROM infra_node WHERE project_id = $1")
            .bind(PROJECT)
            .fetch_one(&pool)
            .await
            .unwrap();
        w
    };
    assert_eq!(set_waiting(&pool, &waiting(OTHER, "theirs")).await.unwrap(), FencedWrite::Displaced);
    assert_eq!(read().await, None);
    sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = 1 WHERE id = $1")
        .bind(stop)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        set_waiting(&pool, &SupervisorSetWaitingRequest { command_id: stop, ..waiting(OWNER, "under a done stop") }).await.unwrap(),
        FencedWrite::Gone,
        "a finished command records nothing"
    );
    assert_eq!(read().await, None);
    assert_eq!(set_waiting(&pool, &waiting(OWNER, "a: its machine's agent does not answer yet")).await.unwrap(), FencedWrite::Applied);
    assert_eq!(read().await.as_deref(), Some("a: its machine's agent does not answer yet"));
    // The start fails: its progress goes with it, so a later start never
    // shows this one's.
    let failed = SupervisorSetStatusRequest { command_id: Some(cmd), ..stamp(OWNER, None, None, Status::Failed) };
    assert_eq!(set_status(&pool, &failed).await.unwrap(), FencedWrite::Applied);
    assert_eq!(read().await, None);
    let (since,): (Option<i64>,) = sqlx::query_as("SELECT provisioning_since_unix FROM infra_node WHERE project_id = $1")
        .bind(PROJECT)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(since, None);
}

/// What a stop waits on is recorded under the stop, kept while the copy
/// stays stopping, and cleared once a unit's stop lands and the copy
/// leaves stopping.
#[sqlx::test]
async fn what_a_stop_waits_on_ends_when_the_copy_leaves_stopping(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let stop = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running"), "b": unit("running") })).await;
    let read = || async {
        let (w,): (Option<String>,) = sqlx::query_as("SELECT waiting_on FROM infra_node WHERE project_id = $1")
            .bind(PROJECT)
            .fetch_one(&pool)
            .await
            .unwrap();
        w
    };
    let waiting = |text: &str| SupervisorSetWaitingRequest {
        replica: OWNER.into(),
        command_id: stop,
        project_id: PROJECT,
        node_id: NODE.into(),
        instance: None,
        waiting: text.into(),
    };
    assert_eq!(set_status(&pool, &stamp(OWNER, Some(stop), Some("a"), Status::Stopping)).await.unwrap(), FencedWrite::Applied);
    assert_eq!(set_waiting(&pool, &waiting("a: its host is taking it down")).await.unwrap(), FencedWrite::Applied);
    // A stamp that leaves the copy stopping keeps it.
    assert_eq!(set_status(&pool, &stamp(OWNER, Some(stop), Some("b"), Status::Stopping)).await.unwrap(), FencedWrite::Applied);
    assert_eq!(read().await.as_deref(), Some("a: its host is taking it down"));
    assert_eq!(set_status(&pool, &stamp(OWNER, Some(stop), Some("a"), Status::Stopped)).await.unwrap(), FencedWrite::Applied);
    assert_eq!(read().await.as_deref(), Some("a: its host is taking it down"), "b still stops, so the copy reads stopping");
    assert_eq!(set_status(&pool, &stamp(OWNER, Some(stop), Some("b"), Status::Stopped)).await.unwrap(), FencedWrite::Applied);
    assert_eq!(row(&pool).await.0, "stopped");
    assert_eq!(read().await, None);
}

/// The autonomous (health) stamp is fenced by ownership too: a process that
/// lost the project cannot stamp it while no command is in flight.
#[sqlx::test]
async fn autonomous_stamp_by_a_displaced_replica_is_displaced(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;

    let out = set_status(&pool, &stamp(OTHER, None, Some("a"), Status::Flaky)).await.unwrap();
    assert_eq!(out, FencedWrite::Displaced);
    assert_eq!(row(&pool).await.0, "running");
    assert_eq!(
        set_status(&pool, &stamp(OWNER, None, Some("a"), Status::Flaky)).await.unwrap(),
        FencedWrite::Applied
    );
}

/// A node-wide stamp writes every unit AND the node status directly,
/// so a unit-less roster still takes it (the rollup would have said
/// 'stopped'); a completed command no longer fences anything (Gone).
#[sqlx::test]
async fn node_wide_stamp_sets_every_unit_and_the_node_even_with_no_units(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running"), "b": unit("flaky") })).await;

    let mut req = stamp(OWNER, Some(cmd), None, Status::Failed);
    req.failure_stage = Some(FailureStage::Apply);
    req.failure_message = Some("boom".into());
    assert_eq!(set_status(&pool, &req).await.unwrap(), FencedWrite::Applied);
    let (status, units) = row(&pool).await;
    assert_eq!(status, "failed");
    assert_eq!(units["a"]["status"], "failed");
    assert_eq!(units["b"]["status"], "failed");

    // Unit-less roster.
    sqlx::query("UPDATE infra_node SET units_json = '{}'::jsonb").execute(&pool).await.unwrap();
    assert_eq!(
        set_status(&pool, &stamp(OWNER, Some(cmd), None, Status::Terminating)).await.unwrap(),
        FencedWrite::Applied
    );
    assert_eq!(row(&pool).await.0, "terminating");

    // Completed command: the fence no longer matches, owner still owns.
    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: cmd, error: None, cancelled: false },
    )
    .await
    .unwrap();
    assert_eq!(
        set_status(&pool, &stamp(OWNER, Some(cmd), None, Status::Stopped)).await.unwrap(),
        FencedWrite::Gone
    );
}

/// The autonomous (health) stamp is fenced by ANY uncompleted command
/// for the node and answers Gone while one is in flight; it applies
/// once the command completes.
#[sqlx::test]
async fn autonomous_stamp_is_fenced_by_an_in_flight_command(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;
    let cmd = command(&pool).await;

    assert_eq!(
        set_status(&pool, &stamp(OWNER, None, Some("a"), Status::Flaky)).await.unwrap(),
        FencedWrite::Gone
    );
    assert_eq!(row(&pool).await.0, "running");
    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: cmd, error: None, cancelled: false },
    )
    .await
    .unwrap();
    assert_eq!(
        set_status(&pool, &stamp(OWNER, None, Some("a"), Status::Flaky)).await.unwrap(),
        FencedWrite::Applied
    );
    assert_eq!(row(&pool).await.0, "flaky");
}

/// Completing a command is exactly-once by exactly the owner: the
/// first completion applies and records the outcome, a second is Gone,
/// a displaced process's is Displaced, and an unknown id is Gone.
#[sqlx::test]
async fn command_completion_is_once_and_owner_only(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    let req = |replica: &str, id: i64, error: Option<&str>| SupervisorCommandCompleteRequest {
        replica: replica.into(),
        command_id: id,
        error: error.map(String::from),
        cancelled: false,
    };

    assert_eq!(complete_command(&pool, &req(OTHER, cmd, None)).await.unwrap(), FencedWrite::Displaced);
    assert_eq!(complete_command(&pool, &req(OWNER, cmd, Some("boom"))).await.unwrap(), FencedWrite::Applied);
    let (outcome, message): (String, Option<String>) =
        sqlx::query_as("SELECT outcome, outcome_message FROM infra_lifecycle_command WHERE id = $1")
            .bind(cmd)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((outcome.as_str(), message.as_deref()), ("failed", Some("boom")));
    assert_eq!(complete_command(&pool, &req(OWNER, cmd, None)).await.unwrap(), FencedWrite::Gone);
    assert_eq!(complete_command(&pool, &req(OWNER, cmd + 1000, None)).await.unwrap(), FencedWrite::Gone);
}

/// The owner is handed its projects' commands oldest first. A command it
/// is already running is never handed out again, a command waits while an
/// older one on the same copy is uncompleted, commands on different copies
/// of one node are handed out side by side, and a project it does not own
/// is never handed out at all.
#[sqlx::test]
async fn next_command_runs_copies_side_by_side_and_one_copy_in_order(pool: PgPool) {
    schema(&pool).await;
    let second = uuid::Uuid::from_u128(2);
    let foreign = uuid::Uuid::from_u128(3);
    for project in [PROJECT, second, foreign] {
        project_row(&pool, project, true).await;
    }
    lease_of(&pool, PROJECT, OWNER).await;
    lease_of(&pool, second, OWNER).await;
    lease_of(&pool, foreign, OTHER).await;
    let foreign_cmd = command_of(&pool, foreign).await;
    let ada = command_on(&pool, PROJECT, Some("ada")).await;
    let bob = command_on(&pool, PROJECT, Some("bob")).await;
    let ada_again = command_on(&pool, PROJECT, Some("ada")).await;
    let other_project = command_of(&pool, second).await;

    let next = |busy: Vec<i64>| {
        let pool = pool.clone();
        async move { claim_next(&pool, OWNER, &busy).await.unwrap().map(|c| c.id) }
    };
    assert_eq!(next(vec![]).await, Some(ada));
    // Running ada's command: bob's copy is another copy, so it goes now.
    assert_eq!(next(vec![ada]).await, Some(bob));
    // ada's second command waits for her first; the other project goes.
    assert_eq!(next(vec![ada, bob]).await, Some(other_project));
    assert_eq!(next(vec![ada, bob, other_project]).await, None);
    // The other supervisor's project is never handed out.
    assert_ne!(next(vec![]).await, Some(foreign_cmd));

    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest {
            replica: OWNER.into(),
            command_id: ada,
            error: None,
            cancelled: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(next(vec![bob, other_project]).await, Some(ada_again));
}

/// A command on every copy (the project going) overlaps every copy's
/// command, so it waits for the older ones and the younger ones wait for it.
#[sqlx::test]
async fn a_command_on_every_copy_waits_for_and_holds_up_each_copy(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let ada = command_on(&pool, PROJECT, Some("ada")).await;
    let (every,): (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, issued_by_replica, issued_at_unix, every_copy) \
         VALUES ($1, $2, NULL, 'terminate', 'dispatcher', EXTRACT(EPOCH FROM NOW())::BIGINT, TRUE) \
         RETURNING id",
    )
    .bind(TENANT)
    .bind(PROJECT)
    .fetch_one(&pool)
    .await
    .expect("every-copy command");
    let bob = command_on(&pool, PROJECT, Some("bob")).await;

    let next = |busy: Vec<i64>| {
        let pool = pool.clone();
        async move { claim_next(&pool, OWNER, &busy).await.unwrap().map(|c| c.id) }
    };
    let done = |command_id: i64| {
        let pool = pool.clone();
        async move {
            let req = SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id, error: None, cancelled: false };
            complete_command(&pool, &req).await.unwrap();
        }
    };
    assert_eq!(next(vec![]).await, Some(ada));
    assert_eq!(next(vec![ada]).await, None, "everything waits on ada");
    done(ada).await;
    assert_eq!(next(vec![]).await, Some(every));
    assert_eq!(next(vec![every]).await, None, "bob waits on the project going");
    done(every).await;
    assert_eq!(next(vec![]).await, Some(bob));
}

/// A command nobody holds a lease over is unowned work; once a live
/// lease covers it, or the command completes, it is not.
#[sqlx::test]
async fn unowned_work_is_a_waiting_command_on_an_unleased_project(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    let cmd = command(&pool).await;
    assert!(unowned_work_waiting(&pool).await.unwrap());
    lease(&pool, OWNER).await;
    assert!(!unowned_work_waiting(&pool).await.unwrap(), "an owned project's command is its owner's");
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0").execute(&pool).await.unwrap();
    assert!(unowned_work_waiting(&pool).await.unwrap(), "an expired lease owns nothing");
    sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = 1 WHERE id = $1")
        .bind(cmd)
        .execute(&pool)
        .await
        .unwrap();
    assert!(!unowned_work_waiting(&pool).await.unwrap(), "a completed command waits on nobody");
}

/// A tick reports the projects it took on: a fresh claim, and the process's
/// own lease revived after it lapsed. A lease it merely renewed is not
/// news, and neither is a project another process holds.
#[sqlx::test]
async fn a_tick_reports_the_projects_it_took_on(pool: PgPool) {
    schema(&pool).await;
    let foreign = uuid::Uuid::from_u128(3);
    project_row(&pool, PROJECT, true).await;
    project_row(&pool, foreign, true).await;
    lease_of(&pool, foreign, OTHER).await;
    let claimed = || {
        let pool = pool.clone();
        async move { sync_ownership(&pool, &syncing(OWNER), None).await.unwrap().claimed }
    };

    assert_eq!(claimed().await, vec![PROJECT], "a free project is claimed");
    assert!(claimed().await.is_empty(), "a renewed lease is not news");
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0 WHERE project_id = $1")
        .bind(PROJECT)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(claimed().await, vec![PROJECT], "a lapsed lease taken back is news");
}

/// The database's clock, unix seconds.
async fn db_now(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM NOW())::BIGINT").fetch_one(pool).await.unwrap()
}

/// A booking of the supervisor's passes over a fake alarm, and when it is
/// booked (unix ms).
fn supervisor_booking(pool: &PgPool) -> (weft_task_store::drain::Booking, std::sync::Arc<weft_platform_traits::alarm::fake::FakeAlarm>) {
    let alarm = std::sync::Arc::new(weft_platform_traits::alarm::fake::FakeAlarm::default());
    (weft_task_store::drain::Booking::new(pool.clone(), weft_platform_traits::CoreRole::Supervisor, alarm.clone()), alarm)
}

async fn booked_pass(pool: &PgPool) -> Option<i64> {
    sqlx::query_scalar::<_, Option<i64>>("SELECT due_ms FROM role_loop_due WHERE role = 'supervisor' AND loop_name = 'pass'")
        .fetch_optional(pool)
        .await
        .unwrap()
        .flatten()
}

/// A sync says when its supervisor next has something to look at that
/// nothing announces, and books its next pass then: nothing over infra that
/// runs well (a machine saying how its unit stands changed wakes it
/// instead), a unit's health window when it ends, a sibling's lease over a
/// project with running infra when it lapses, and now for a command
/// waiting on a project no lease covers. A sync with nothing to look at
/// clears the booking and cancels its wake.
#[sqlx::test]
async fn sync_ownership_books_the_supervisors_pass_at_next_due_and_clears_it_when_none(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    let (booking, alarm) = supervisor_booking(&pool);
    let sync = |replica: &'static str| {
        let (pool, booking) = (pool.clone(), &booking);
        async move { sync_ownership(&pool, &syncing(replica), Some(booking)).await.unwrap() }
    };
    let declared = sync(OWNER).await;
    assert_eq!(declared.owned.len(), 1, "declared infra is ownable");
    assert_eq!(declared.next_due_unix, None, "but gives nothing to look at");

    let now = db_now(&pool).await;
    let mut flaky = unit("running");
    flaky["not_ready_since_unix"] = serde_json::json!(now - 10);
    node_row(&pool, "running", serde_json::json!({ "web": flaky })).await;
    let window = sync(OWNER).await;
    assert_eq!(window.next_due_unix, Some(now + 20), "the window ends 30 s after the unit stopped being ready");
    assert_eq!(booked_pass(&pool).await, Some((now + 20) * 1000));
    assert_eq!(alarm.wakes().len(), 1, "one wake for the booking");

    sqlx::query("UPDATE infra_node SET units_json = $1").bind(serde_json::json!({ "web": unit("running") })).execute(&pool).await.unwrap();
    assert_eq!(sync(OWNER).await.next_due_unix, None, "a unit that is ready again leaves nothing to look at");
    assert_eq!(booked_pass(&pool).await, None, "the booking is cleared");
    assert_eq!(alarm.cancelled().len(), 1, "and its wake cancelled");

    lease(&pool, OTHER).await;
    let sibling = sync(OWNER).await;
    assert!(sibling.owned.is_empty(), "a sibling holds the lease");
    let lapse = sibling.next_due_unix.expect("its lease over running infra may lapse");
    assert!((now + 3600..=now + 3610).contains(&lapse), "looked at once the sibling's lease lapses: {lapse}");
    sqlx::query("UPDATE infra_node SET status = 'stopped'").execute(&pool).await.unwrap();
    assert_eq!(sync(OWNER).await.next_due_unix, None, "a stopped copy says nothing to wake for");

    // A pass ending claims nothing, so a command on a project nobody
    // leases is still waiting: the next pass is due at once.
    sqlx::query("DELETE FROM infra_owner").execute(&pool).await.unwrap();
    command(&pool).await;
    let ending = SupervisorSyncOwnershipRequest { ending: true, ..syncing(OWNER) };
    let waiting = sync_ownership(&pool, &ending, Some(&booking)).await.unwrap().next_due_unix.expect("due");
    assert!((now..=now + 10).contains(&waiting), "due now: {waiting}");
}

/// A look whose time already came and failed (the supervisor wrote it
/// down as given up, at or after that time) is not booked again: a
/// person's look or the next real event retries it, never a clock.
#[sqlx::test]
async fn a_due_time_already_given_up_on_is_not_booked(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    let (booking, _alarm) = supervisor_booking(&pool);
    let now = db_now(&pool).await;
    let mut flaky = unit("running");
    flaky["not_ready_since_unix"] = serde_json::json!(now - 100);
    node_row(&pool, "running", serde_json::json!({ "web": flaky })).await;
    let due = sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap().next_due_unix;
    assert_eq!(due, Some(now - 70), "a window that already ended is due");
    weft_task_store::unanswered::failed(&pool, weft_task_store::unanswered::Callee::Infra(PROJECT), None, "the look failed")
        .await
        .unwrap()
        .log();
    assert_eq!(sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap().next_due_unix, None);
}

/// A pass that ends gives back every lease, and what it settled is no
/// longer written down as given up.
#[sqlx::test]
async fn an_ending_sync_gives_back_every_lease_and_clears_settled(pool: PgPool) {
    schema(&pool).await;
    let other = uuid::Uuid::from_u128(2);
    for project in [PROJECT, other] {
        project_row(&pool, project, true).await;
    }
    assert_eq!(sync_ownership(&pool, &syncing(OWNER), None).await.unwrap().owned.len(), 2);
    weft_task_store::unanswered::failed(&pool, weft_task_store::unanswered::Callee::Infra(PROJECT), None, "gave up").await.unwrap().log();
    let ending = SupervisorSyncOwnershipRequest { settled: vec![PROJECT], ending: true, ..syncing(OWNER) };
    let ended = sync_ownership(&pool, &ending, None).await.unwrap();
    assert!(ended.owned.is_empty(), "no lease stays");
    assert!(ended.claimed.is_empty(), "an ending sync claims nothing");
    let written: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM unanswered_call").fetch_one(&pool).await.unwrap();
    assert_eq!(written, 0, "the settled project's give-up is cleared");
}

/// The failure stretch written down for `PROJECT`, if any.
async fn infra_failure(pool: &PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT error FROM unanswered_call WHERE callee = $1")
        .bind(format!("infra:{PROJECT}"))
        .fetch_optional(pool)
        .await
        .unwrap()
}

/// A pass that failed over a project whose window already ended is
/// written down and tried again 5 s, then 10 s later, booked as the
/// supervisor's next pass even though the failure masks the window's own
/// time; holding the project leaves the retry set, a look that failed
/// again takes the retry and sets the next, the third failure in a row
/// stops the retries, and a look that fails in nothing ends the count, so
/// only failures in a row count. A project whose lease moved, or whose
/// lease of this replica lapsed, writes nothing.
#[sqlx::test]
async fn a_failed_pass_is_tried_again_twice_then_given_up(pool: PgPool) {
    use weft_broker_client::protocol::ProjectTrouble;
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    let (booking, _alarm) = supervisor_booking(&pool);
    let now = db_now(&pool).await;
    let mut flaky = unit("running");
    flaky["not_ready_since_unix"] = serde_json::json!(now - 100);
    node_row(&pool, "running", serde_json::json!({ "web": flaky })).await;
    let failing = || SupervisorSyncOwnershipRequest {
        failed: vec![ProjectTrouble { project_id: PROJECT, error: "the host did not answer".into() }],
        ending: true,
        ..syncing(OWNER)
    };
    let pass_fails = || async {
        sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap();
        sync_ownership(&pool, &failing(), Some(&booking)).await.unwrap().next_due_unix
    };

    let first = pass_fails().await.expect("the retry is due");
    assert!((now + 5..=now + 7).contains(&first), "tried again 5 s later: {first}");
    assert!(infra_failure(&pool).await.unwrap().contains("failure 1 in a row; tried again in 5 s"));

    let taken = sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap();
    assert_eq!(taken.owned.len(), 1);
    sqlx::query("UPDATE infra_health_look SET retry_at_unix = retry_at_unix - 10").execute(&pool).await.unwrap();
    let retry = || async {
        sqlx::query_scalar::<_, Option<i64>>("SELECT retry_at_unix FROM infra_health_look").fetch_one(&pool).await.unwrap()
    };
    sync_ownership(&pool, &syncing(OWNER), None).await.unwrap();
    assert!(retry().await.is_some(), "holding the project is not looking at it");
    let looked_and_failed = SupervisorSyncOwnershipRequest { looked: vec![PROJECT], ..failing() };
    let second = sync_ownership(&pool, &looked_and_failed, Some(&booking)).await.unwrap().next_due_unix.expect("due");
    assert!((now + 10..=now + 12).contains(&second), "then 10 s later: {second}");
    let third = pass_fails().await;
    assert_eq!(third, None, "the third failure in a row stops the retries, and masks the window");
    assert!(infra_failure(&pool).await.unwrap().contains("`weft infra look` tries again"));

    sync_ownership(&pool, &syncing(OWNER), None).await.unwrap();
    let clean = SupervisorSyncOwnershipRequest { looked: vec![PROJECT], settled: vec![PROJECT], ending: true, ..syncing(OWNER) };
    sync_ownership(&pool, &clean, None).await.unwrap();
    let failures: i32 = sqlx::query_scalar("SELECT failures FROM infra_health_look").fetch_one(&pool).await.unwrap();
    assert_eq!(failures, 0, "a look that fails in nothing ends the count");
    assert_eq!(infra_failure(&pool).await, None);
    pass_fails().await.expect("a failure after one that went through is retried");
    assert!(infra_failure(&pool).await.unwrap().contains("failure 1 in a row"), "only failures in a row count");

    sync_ownership(&pool, &syncing(OWNER), None).await.unwrap();
    let unsettled_clean = SupervisorSyncOwnershipRequest { looked: vec![PROJECT], ..syncing(OWNER) };
    sync_ownership(&pool, &unsettled_clean, None).await.unwrap();
    let failures: i32 = sqlx::query_scalar("SELECT failures FROM infra_health_look").fetch_one(&pool).await.unwrap();
    assert_eq!(failures, 0, "a clean look ends the count even when a protocol keeps it from settling");
    assert_eq!(retry().await, None, "and takes the retry");

    sqlx::query("UPDATE infra_owner SET leased_until_unix = EXTRACT(EPOCH FROM NOW())::BIGINT - 1").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM unanswered_call").execute(&pool).await.unwrap();
    sync_ownership(&pool, &failing(), Some(&booking)).await.unwrap();
    assert_eq!(infra_failure(&pool).await, None, "a lease of this replica that lapsed judges nothing");

    lease(&pool, OTHER).await;
    sync_ownership(&pool, &failing(), None).await.unwrap();
    assert_eq!(infra_failure(&pool).await, None, "a project whose lease moved is the new owner's to judge");
}

/// A pass whose host listing failed fails over a project only the
/// listing could make ownable whose retry was due: no lease covers it, so
/// the pass counts the failed listing against it, sets its next retry,
/// and keeps that retry booked though the project is not ownable without
/// a listing. A sync whose listing went through and does not make the
/// project ownable clears its retry, its failures and what was written
/// down of it, so a later pass that cannot list the host charges it
/// nothing.
#[sqlx::test]
async fn a_failed_listing_fails_the_pass_over_a_due_retry_it_could_not_claim(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, false).await;
    let (booking, _alarm) = supervisor_booking(&pool);
    let now = db_now(&pool).await;
    sqlx::query("INSERT INTO infra_health_look (project_id, failures, retry_at_unix) VALUES ($1, 1, $2)")
        .bind(PROJECT)
        .bind(now - 1)
        .execute(&pool)
        .await
        .unwrap();
    let unlisted = || SupervisorSyncOwnershipRequest { held_projects: None, ..syncing(OWNER) };
    let claim = sync_ownership(&pool, &unlisted(), Some(&booking)).await.unwrap();
    assert!(claim.owned.is_empty(), "nothing makes the project ownable without the listing");
    assert_eq!(claim.next_due_unix, Some(now - 1), "its due retry stays booked");

    let ending = || SupervisorSyncOwnershipRequest { all_failed: Some("the host did not list".into()), ending: true, ..unlisted() };
    let second = sync_ownership(&pool, &ending(), Some(&booking)).await.unwrap().next_due_unix.expect("the retry stays booked");
    assert!((now + 10..=now + 12).contains(&second), "tried again 10 s later: {second}");
    assert_eq!(infra_failure(&pool).await.as_deref(), Some("the host did not list (failure 2 in a row; tried again in 10 s)"));
    let listed = sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap();
    assert_eq!(listed.next_due_unix, None, "a listing that went through without the project books no retry for it");
    assert_eq!(infra_failure(&pool).await, None, "and clears what was written down of it");
    let look: (i32, Option<i64>) =
        sqlx::query_as("SELECT failures, retry_at_unix FROM infra_health_look").fetch_one(&pool).await.unwrap();
    assert_eq!(look, (0, None), "with its retry and its failures in a row");

    let later = sync_ownership(&pool, &ending(), Some(&booking)).await.unwrap().next_due_unix;
    assert_eq!(later, None, "so a later pass that cannot list the host finds nothing left to fail over");
    assert_eq!(infra_failure(&pool).await, None);
}

/// A pass that failed as a whole (what it owns could not be read) fails
/// over every project whose lease it holds as it ends, the ones it took
/// on mid-pass included, once each with its own error joined; a project
/// another replica holds is left to it.
#[sqlx::test]
async fn a_pass_that_failed_whole_fails_over_every_project_it_holds(pool: PgPool) {
    use weft_broker_client::protocol::ProjectTrouble;
    schema(&pool).await;
    let (mid_pass, sibling) = (uuid::Uuid::from_u128(2), uuid::Uuid::from_u128(3));
    for project in [PROJECT, mid_pass, sibling] {
        project_row(&pool, project, true).await;
    }
    let (booking, _alarm) = supervisor_booking(&pool);
    lease_of(&pool, sibling, OTHER).await;
    lease(&pool, OWNER).await;
    lease_of(&pool, mid_pass, OWNER).await;
    let ending = SupervisorSyncOwnershipRequest {
        failed: vec![ProjectTrouble { project_id: PROJECT, error: "the sweep failed".into() }],
        all_failed: Some("could not read what it owns".into()),
        ending: true,
        ..syncing(OWNER)
    };
    sync_ownership(&pool, &ending, Some(&booking)).await.unwrap();
    let written: Vec<(String, String)> = sqlx::query_as("SELECT callee, error FROM unanswered_call ORDER BY callee")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        written,
        vec![
            (format!("infra:{PROJECT}"), "the sweep failed; could not read what it owns (failure 1 in a row; tried again in 5 s)".to_string()),
            (format!("infra:{mid_pass}"), "could not read what it owns (failure 1 in a row; tried again in 5 s)".to_string()),
        ]
    );
}

/// A claim takes the projects with something due first: a retry due over
/// a project past the first batch by id is claimed by the first sync, so
/// the pass it books looks at it instead of booking itself again at once.
#[sqlx::test]
async fn a_claim_takes_the_projects_with_something_due_first(pool: PgPool) {
    schema(&pool).await;
    let batch = weft_broker_client::lifecycle_command::SUPERVISOR_CLAIM_BATCH as u128;
    let projects: Vec<uuid::Uuid> = (1..=batch + 1).map(|i| uuid::Uuid::from_u128(100 + i)).collect();
    for project in &projects {
        project_row(&pool, *project, true).await;
    }
    let last = *projects.last().unwrap();
    sqlx::query("INSERT INTO infra_health_look (project_id, failures, retry_at_unix) VALUES ($1, 1, EXTRACT(EPOCH FROM NOW())::BIGINT - 1)")
        .bind(last)
        .execute(&pool)
        .await
        .unwrap();
    let claimed = sync_ownership(&pool, &syncing(OWNER), None).await.unwrap().claimed;
    assert_eq!(claimed.len(), batch as usize, "one batch");
    assert!(claimed.contains(&last), "the project with a retry due is in it: {claimed:?}");
}

/// A failed pass's retry over a project that stopped being ownable before
/// it came due is not booked: nothing would claim the project and take
/// the retry, so it would be due again at every pass. The sync that
/// listed the host and found it not ownable cleared the retry, so the
/// project ownable again (here through a copy on the host) starts afresh.
#[sqlx::test]
async fn a_retry_over_a_project_nobody_can_own_is_cleared(pool: PgPool) {
    use weft_broker_client::protocol::ProjectTrouble;
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    let (booking, _alarm) = supervisor_booking(&pool);
    let now = db_now(&pool).await;
    sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap();
    let failing = SupervisorSyncOwnershipRequest {
        failed: vec![ProjectTrouble { project_id: PROJECT, error: "the host did not answer".into() }],
        ending: true,
        ..syncing(OWNER)
    };
    let due = sync_ownership(&pool, &failing, Some(&booking)).await.unwrap().next_due_unix.expect("the retry is due");
    assert!((now + 5..=now + 7).contains(&due), "tried again 5 s later: {due}");

    sqlx::query("UPDATE project SET has_infra = FALSE").execute(&pool).await.unwrap();
    let idle = sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap();
    assert!(idle.owned.is_empty(), "nothing is left to own");
    assert_eq!(idle.next_due_unix, None, "so its retry books nothing");
    assert_eq!(booked_pass(&pool).await, None);
    assert_eq!(infra_failure(&pool).await, None, "what was written down of it is cleared");

    let holding = SupervisorSyncOwnershipRequest { held_projects: Some(vec![PROJECT]), ending: true, ..syncing(OWNER) };
    let back = sync_ownership(&pool, &holding, Some(&booking)).await.unwrap().next_due_unix;
    assert_eq!(back, None, "ownable again, it has no retry left over");
}

/// What a pass could not delete of a removed project (no row, so no
/// lease) is counted like a project's failed pass and written down once
/// for the whole install: tried again 5 s, then 10 s later (booked though
/// nothing can own the project, a pass that cannot list the host counting
/// as its retry), then given up and shown. It stays while a listing still
/// holds the project, and goes with its count once one does not.
#[sqlx::test]
async fn a_removed_projects_leftovers_are_written_down_for_the_install_retried_then_cleared(pool: PgPool) {
    use weft_broker_client::protocol::ProjectTrouble;
    schema(&pool).await;
    let removed = uuid::Uuid::from_u128(9);
    let (booking, _alarm) = supervisor_booking(&pool);
    let now = db_now(&pool).await;
    let holding = || SupervisorSyncOwnershipRequest { held_projects: Some(vec![removed]), ..syncing(OWNER) };
    let failing = || SupervisorSyncOwnershipRequest {
        failed: vec![ProjectTrouble { project_id: removed, error: "the disk is busy".into() }],
        ending: true,
        ..holding()
    };
    let written = || async {
        sqlx::query_as::<_, (String, String)>("SELECT callee, error FROM unanswered_call").fetch_all(&pool).await.unwrap()
    };
    let shift = |secs: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query("UPDATE infra_health_look SET retry_at_unix = retry_at_unix - $1").bind(secs).execute(&pool).await.unwrap();
        }
    };

    let first = sync_ownership(&pool, &failing(), Some(&booking)).await.unwrap().next_due_unix.expect("the retry is due");
    assert!((now + 5..=now + 7).contains(&first), "tried again 5 s later: {first}");
    assert_eq!(
        written().await,
        vec![(
            "gone-copies:".to_string(),
            format!("removed project {removed}: the disk is busy (failure 1 in a row; tried again in 5 s)")
        )]
    );

    shift(10).await;
    let unlisted = SupervisorSyncOwnershipRequest { held_projects: None, ..syncing(OWNER) };
    let due = sync_ownership(&pool, &unlisted, Some(&booking)).await.unwrap().next_due_unix.expect("still booked");
    assert!(due <= now, "its retry is due though nothing can own the project: {due}");
    let not_listed = SupervisorSyncOwnershipRequest { all_failed: Some("the host did not list".into()), ending: true, ..unlisted };
    let second = sync_ownership(&pool, &not_listed, Some(&booking)).await.unwrap().next_due_unix.expect("due");
    assert!((now + 10..=now + 12).contains(&second), "a pass that cannot list is its retry, tried again 10 s later: {second}");
    assert!(written().await[0].1.contains(&format!("removed project {removed}: the host did not list (failure 2 in a row")));

    shift(20).await;
    let third = sync_ownership(&pool, &failing(), Some(&booking)).await.unwrap().next_due_unix;
    assert_eq!(third, None, "the third failure in a row gives up");
    assert!(written().await[0].1.contains("failed 3 times in a row, so the supervisor stopped trying; its next pass tries again"));

    sync_ownership(&pool, &holding(), Some(&booking)).await.unwrap();
    assert_eq!(written().await.len(), 1, "a listing that still holds the project keeps it shown");
    sync_ownership(&pool, &syncing(OWNER), Some(&booking)).await.unwrap();
    assert!(written().await.is_empty(), "a listing that holds none of it clears it");
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM infra_health_look").fetch_one(&pool).await.unwrap();
    assert_eq!(rows, 0, "with its count");
}

/// A health protocol that gave up comes back armed once a person asks for
/// a look after it gave up.
#[sqlx::test]
async fn a_persons_look_arms_a_protocol_that_gave_up(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let now = db_now(&pool).await;
    let gave_up = HealthProtocolState {
        protocol: "restart".into(),
        acted: None,
        consecutive_failures: 3,
        next_retry_at_unix: None,
        gave_up_at_unix: Some(now - 5),
        gave_up_seen: Some(serde_json::json!([])),
        last_error: Some("refused".into()),
    };
    let req = SupervisorSetHealthStateRequest { replica: OWNER.into(), project_id: PROJECT, protocols: vec![gave_up.clone()] };
    assert!(matches!(set_health_state(&pool, &req).await.unwrap(), FencedWrite::Applied));
    assert_eq!(health_state(&pool, PROJECT).await.unwrap(), vec![gave_up]);
    weft_dispatcher::infra_lifecycle_command::person_asks_for_look(&pool, PROJECT).await.unwrap();
    let armed = health_state(&pool, PROJECT).await.unwrap();
    assert_eq!(armed[0].gave_up_at_unix, None, "armed again: {armed:?}");
    assert_eq!(armed[0].consecutive_failures, 0);
}

/// A claim marks the command as under way: a cancel then flags it rather
/// than completing it, the held claim of the one running it answers the
/// cancel, and it completes as cancelled when the supervisor says so.
#[sqlx::test]
async fn cancelling_a_claimed_supervisor_command_leaves_it_pending_until_the_supervisor_completes_it(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    let claimed = claim_next(&pool, OWNER, &[]).await.unwrap().expect("claimed");
    assert!(!claimed.cancel_requested);
    let by: Option<String> = sqlx::query_scalar("SELECT claimed_by_replica FROM infra_lifecycle_command WHERE id = $1")
        .bind(cmd)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(by.as_deref(), Some(OWNER), "the claim marks the row");
    // The dispatcher's cancel: complete the unclaimed, flag the claimed.
    sqlx::query("UPDATE infra_lifecycle_command SET cancel_requested = TRUE WHERE id = $1 AND claimed_by_replica IS NOT NULL")
        .bind(cmd)
        .execute(&pool)
        .await
        .unwrap();
    let pending: bool = sqlx::query_scalar("SELECT completed_at_unix IS NULL FROM infra_lifecycle_command WHERE id = $1")
        .bind(cmd)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(pending, "flagged, not completed");
    assert_eq!(cancels_for(&pool, &[cmd], &[]).await.unwrap(), vec![cmd], "the runner's claim hears the cancel");
    assert!(cancels_for(&pool, &[cmd], &[cmd]).await.unwrap().is_empty(), "once");
    let req = SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: cmd, error: Some("halted".into()), cancelled: true };
    assert_eq!(complete_command(&pool, &req).await.unwrap(), FencedWrite::Applied);
    let outcome: Option<String> = sqlx::query_scalar("SELECT outcome FROM infra_lifecycle_command WHERE id = $1")
        .bind(cmd)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outcome.as_deref(), Some("cancelled"));
}

/// A command claimed three times never ended: the fourth claim gives it
/// up with an outcome saying so and writes it down for `weft status` as
/// given up, and hands out the younger command it held back. The ending of that one is
/// written even with a NUL byte in its error, and clears the record.
#[sqlx::test]
async fn a_command_that_never_ends_is_given_up_after_three_runs(pool: PgPool) {
    use weft_broker_client::protocol::SUPERVISOR_GIVE_UP_AFTER;
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    weft_task_store::unanswered::alert_after(&pool, Some(std::time::Duration::from_secs(3600))).await.unwrap();
    let stuck = command(&pool).await;
    let younger = command(&pool).await;
    for run in 1..=SUPERVISOR_GIVE_UP_AFTER {
        assert_eq!(claim_next(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(stuck), "run {run}");
    }
    assert_eq!(claim_next(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(younger), "no longer held back");
    let (outcome, message): (String, String) =
        sqlx::query_as("SELECT outcome, outcome_message FROM infra_lifecycle_command WHERE id = $1")
            .bind(stuck)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(outcome, "failed");
    assert!(message.contains("took this command up 3 times"), "{message}");
    let shown = || async {
        sqlx::query_scalar::<_, String>("SELECT error FROM unanswered_call WHERE callee = $1")
            .bind(format!("commands:{PROJECT}"))
            .fetch_optional(&pool)
            .await
            .unwrap()
    };
    assert!(shown().await.expect("written down").starts_with(&format!("command {stuck}: ")));
    let alerted: Option<i64> = sqlx::query_scalar("SELECT alerted_ms FROM unanswered_call WHERE callee = $1")
        .bind(format!("commands:{PROJECT}"))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(alerted.is_some(), "written down as given up, which an install that alerts alerts on at once");

    let req = SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: younger, error: Some("bad\0byte".into()), cancelled: false };
    assert_eq!(complete_command(&pool, &req).await.unwrap(), FencedWrite::Applied);
    let message: String = sqlx::query_scalar("SELECT outcome_message FROM infra_lifecycle_command WHERE id = $1")
        .bind(younger)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(message, "badbyte");
    assert_eq!(shown().await, None, "an ending written clears it");
}

/// A supervisor that does not scale to zero looks again on its own clock:
/// its failed pass is written down as tried again at its next look, with
/// nothing counted toward a give-up and no retry set.
#[sqlx::test]
async fn an_always_on_supervisors_failure_counts_nothing(pool: PgPool) {
    use weft_broker_client::protocol::ProjectTrouble;
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    sync_ownership(&pool, &syncing(OWNER), None).await.unwrap();
    let failing = SupervisorSyncOwnershipRequest {
        failed: vec![ProjectTrouble { project_id: PROJECT, error: "the host did not answer".into() }],
        ..syncing(OWNER)
    };
    sync_ownership(&pool, &failing, None).await.unwrap();
    sync_ownership(&pool, &failing, None).await.unwrap();
    sync_ownership(&pool, &failing, None).await.unwrap();
    assert_eq!(
        infra_failure(&pool).await.as_deref(),
        Some("the host did not answer (tried again at the supervisor's next look)")
    );
    let looks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM infra_health_look").fetch_one(&pool).await.unwrap();
    assert_eq!(looks, 0, "nothing counted, nothing booked");
}

/// A command claimed by a replica that lost the project is claimable again
/// by the new owner; one claimed and held by a live owner is skipped by
/// anybody else.
#[sqlx::test]
async fn a_claim_held_by_a_replica_that_lost_the_project_is_claimable_again(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OTHER).await;
    let cmd = command(&pool).await;
    assert_eq!(claim_next(&pool, OTHER, &[]).await.unwrap().map(|c| c.id), Some(cmd));
    lease(&pool, OWNER).await;
    assert_eq!(claim_next(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(cmd), "OTHER no longer holds the project");
}

#[sqlx::test]
async fn a_command_claimed_and_held_is_skipped_by_a_second_claimer(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let cmd = command(&pool).await;
    assert_eq!(claim_next(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(cmd));
    sqlx::query("UPDATE infra_lifecycle_command SET claimed_by_replica = $2 WHERE id = $1")
        .bind(cmd)
        .bind("supervisor-c")
        .execute(&pool)
        .await
        .unwrap();
    // supervisor-c holds no lease: its claim does not hold.
    assert_eq!(claim_next(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(cmd));
    // OWNER runs it now (busy): a pass of the same owner never takes it again.
    assert_eq!(claim_next(&pool, OWNER, &[cmd]).await.unwrap().map(|c| c.id), None);
}

/// A health protocol that gave up comes back armed once a person's infra
/// command touched the project since, keeping what it acted on.
#[sqlx::test]
async fn a_given_up_protocol_comes_back_armed_after_a_persons_command(pool: PgPool) {
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let now = db_now(&pool).await;
    let given_up = HealthProtocolState {
        protocol: "restart".into(),
        acted: Some(Default::default()),
        consecutive_failures: 3,
        next_retry_at_unix: None,
        gave_up_at_unix: Some(now + 5),
        gave_up_seen: Some(serde_json::json!({ "web": "flaky" })),
        last_error: Some("refused".into()),
    };
    let req = SupervisorSetHealthStateRequest { replica: OWNER.into(), project_id: PROJECT, protocols: vec![given_up.clone()] };
    assert_eq!(set_health_state(&pool, &req).await.unwrap(), FencedWrite::Applied);
    let displaced = SupervisorSetHealthStateRequest { replica: OTHER.into(), ..req.clone() };
    assert_eq!(set_health_state(&pool, &displaced).await.unwrap(), FencedWrite::Displaced, "only the lease holder writes");
    assert_eq!(health_state(&pool, PROJECT).await.unwrap(), vec![given_up.clone()], "given up, nothing touched it since");

    command(&pool).await;
    sqlx::query("UPDATE infra_lifecycle_command SET issued_at_unix = $1").bind(now + 10).execute(&pool).await.unwrap();
    let armed = health_state(&pool, PROJECT).await.unwrap();
    assert_eq!(
        armed,
        vec![HealthProtocolState {
            consecutive_failures: 0,
            gave_up_at_unix: None,
            gave_up_seen: None,
            last_error: None,
            ..given_up
        }],
        "a person's command re-arms it, keeping what it acted on"
    );
}

/// A project with no infra, no row and nothing on the host is owned only
/// while a supervisor command waits on it: claimed for it, renewed and
/// reported owned until it completes, then no longer renewed.
#[sqlx::test]
async fn a_pending_command_alone_makes_a_project_owned(pool: PgPool) {
    schema(&pool).await;
    let idle = uuid::Uuid::from_u128(9);
    project_row(&pool, idle, false).await;
    let sync = || {
        let pool = pool.clone();
        async move { sync_ownership(&pool, &syncing(OWNER), None).await.unwrap() }
    };
    assert!(sync().await.owned.is_empty(), "nothing to own, nothing claimed");

    let cmd = command_of(&pool, idle).await;
    assert!(unowned_work_waiting(&pool).await.unwrap(), "the command waits for an owner");
    let synced = sync().await;
    assert_eq!(synced.claimed, vec![idle]);
    assert_eq!(synced.owned.iter().map(|p| p.project_id).collect::<Vec<_>>(), vec![idle]);
    assert_eq!(claim_next(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(cmd));

    // Renewed while pending: expire it, and the tick takes it back.
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0").execute(&pool).await.unwrap();
    assert_eq!(sync().await.claimed, vec![idle], "a pending command's lease is renewed");

    complete_command(
        &pool,
        &SupervisorCommandCompleteRequest { replica: OWNER.into(), command_id: cmd, error: None, cancelled: false },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE infra_owner SET leased_until_unix = 0").execute(&pool).await.unwrap();
    assert!(sync().await.owned.is_empty(), "with the command done, the lease is not renewed");
}

/// A command or event for a deleted project writes nothing (the caller
/// was authorized from a cache that can outlive the removal); for a live
/// project it is written, and a retried apply hands back the same id.
#[sqlx::test]
async fn nothing_is_issued_or_recorded_for_a_deleted_project(pool: PgPool) {
    schema(&pool).await;
    let spec = serde_json::json!({ "units": [] });
    let apply = |project_id: uuid::Uuid| IssuedCommand {
        tenant_id: TENANT,
        project_id,
        node_id: Some(NODE),
        copies: &weft_core::instance::Copies::Shared,
        verb: InfraLifecycleVerb::Apply,
        spec_json: Some(&spec),
        issued_by_replica: "worker-1",
    };
    let gone = uuid::Uuid::from_u128(9);
    assert_eq!(issue_command(&pool, &apply(gone)).await.unwrap(), None);
    let event = record_event(&pool, TENANT, gone, Some(NODE), None, "recovered", &serde_json::json!({})).await;
    assert_eq!(event.unwrap(), None);

    project_row(&pool, PROJECT, true).await;
    let first = issue_command(&pool, &apply(PROJECT)).await.unwrap().expect("issued");
    assert_eq!(issue_command(&pool, &apply(PROJECT)).await.unwrap(), Some(first), "a retried apply is the same command");
    let reactivate = IssuedCommand {
        node_id: None,
        verb: InfraLifecycleVerb::Reactivate,
        spec_json: None,
        ..apply(PROJECT)
    };
    assert!(issue_command(&pool, &reactivate).await.unwrap().is_some_and(|id| id != first));
    let event = record_event(&pool, TENANT, PROJECT, Some(NODE), None, "recovered", &serde_json::json!({})).await;
    assert!(event.unwrap().is_some());
}

/// The repair statement the decode error prints heals a row carrying a
/// status-only stub: before it the canonical decode fails, after it
/// the stub is gone and the real entries are intact.
#[sqlx::test]
async fn repair_sql_drops_status_only_stubs(pool: PgPool) {
    schema(&pool).await;
    node_row(
        &pool,
        "running",
        serde_json::json!({ "a": unit("running"), "stub": { "status": "stopped" } }),
    )
    .await;
    let (_, units) = row(&pool).await;
    let err = decode_units_json(units, PROJECT, NODE).unwrap_err().to_string();
    assert!(err.contains(&units_json_repair_sql(PROJECT, NODE)), "{err}");

    sqlx::query(&units_json_repair_sql(PROJECT, NODE)).execute(&pool).await.expect("repair");
    let (_, units) = row(&pool).await;
    let decoded = decode_units_json(units, PROJECT, NODE).expect("healed row decodes");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded["a"].status, Status::Running);
}

/// An instance's copy is its own row: a stamp naming the instance moves
/// only that copy, and a command naming copies hands the same copies
/// back to the supervisor that claims it.
#[sqlx::test]
async fn an_instances_copy_is_stamped_and_commanded_on_its_own(pool: PgPool) {
    use weft_core::instance::{Copies, InstanceId};
    schema(&pool).await;
    project_row(&pool, PROJECT, true).await;
    lease(&pool, OWNER).await;
    let ada = InstanceId::new("ada").unwrap();
    node_row(&pool, "running", serde_json::json!({ "a": unit("running") })).await;
    sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, units_json, instance_id) \
         VALUES ($1, $2, 'inst-ada', 'running', $3, 'ada')",
    )
    .bind(PROJECT)
    .bind(NODE)
    .bind(serde_json::json!({ "a": unit("running") }))
    .execute(&pool)
    .await
    .expect("ada's copy");
    let duplicate = sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, status, instance_id) VALUES ($1, $2, 'running', 'ada')",
    )
    .bind(PROJECT)
    .bind(NODE)
    .execute(&pool)
    .await;
    assert!(duplicate.is_err(), "one copy per instance");

    let stamp_ada = SupervisorSetStatusRequest { instance: Some(ada.clone()), ..stamp(OWNER, None, Some("a"), Status::Flaky) };
    assert_eq!(set_status(&pool, &stamp_ada).await.unwrap(), FencedWrite::Applied);
    let statuses: Vec<(Option<String>, String)> =
        sqlx::query_as("SELECT instance_id, status FROM infra_node WHERE project_id = $1 ORDER BY instance_id NULLS FIRST")
            .bind(PROJECT)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(statuses, vec![(None, "running".to_string()), (Some("ada".to_string()), "flaky".to_string())]);
    let stamp_bob = SupervisorSetStatusRequest { instance: Some(InstanceId::new("bob").unwrap()), ..stamp_ada.clone() };
    assert_eq!(set_status(&pool, &stamp_bob).await.unwrap(), FencedWrite::Gone, "bob has no copy");

    let spec = serde_json::json!({ "units": [] });
    let copies = Copies::Instance(ada.clone());
    let apply = IssuedCommand {
        tenant_id: TENANT,
        project_id: PROJECT,
        node_id: Some(NODE),
        copies: &copies,
        verb: InfraLifecycleVerb::Apply,
        spec_json: Some(&spec),
        issued_by_replica: "worker-1",
    };
    let id = issue_command(&pool, &apply).await.unwrap().expect("issued");
    let shared = IssuedCommand { copies: &Copies::Shared, ..apply };
    assert_ne!(issue_command(&pool, &shared).await.unwrap(), Some(id), "the shared copy's apply is another command");
    let claimed = claim_next(&pool, OWNER, &[]).await.unwrap().expect("a command");
    assert_eq!((claimed.id, claimed.copies), (id, Copies::Instance(ada)));
}

/// A take-down that drains first is the dispatcher's until its running
/// work is done: the supervisor is handed it only once the drain hands it
/// over, and a command on the same copy behind it waits for it.
#[sqlx::test]
async fn a_command_still_draining_is_not_handed_to_the_supervisor(pool: PgPool) {
    schema(&pool).await;
    lease(&pool, OWNER).await;
    let draining = command(&pool).await;
    let behind = command(&pool).await;
    sqlx::query("UPDATE infra_lifecycle_command SET drain_by_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + 60 WHERE id = $1")
        .bind(draining)
        .execute(&pool)
        .await
        .unwrap();
    assert!(claim_next(&pool, OWNER, &[]).await.unwrap().is_none(), "draining, and the one behind it waits");
    sqlx::query("UPDATE infra_lifecycle_command SET drain_by_unix = NULL WHERE id = $1").bind(draining).execute(&pool).await.unwrap();
    assert_eq!(claim_next(&pool, OWNER, &[]).await.unwrap().map(|c| c.id), Some(draining));
    assert_eq!(claim_next(&pool, OWNER, &[draining]).await.unwrap().map(|c| c.id), None, "{behind} waits for it");
}
