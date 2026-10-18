//! Carrying a database of the previous generation into this one
//! (`weft_runtime::carry`), against a real Postgres: a database built
//! from the last schema of the `serverless` generation
//! (`fixtures/serverless_schema.sql`), holding rows written the way that
//! generation wrote them, comes out of the carry as the install's, read
//! back with today's types, its run files at their new keys. And every
//! state an interrupted carry can leave behind is finished by the next.
//!
//! Each test makes databases of its own on `$DATABASE_URL`'s server, under
//! a name no other test uses. Gated behind `db-tests`:
//! `scripts/run-db-tests.sh weft-runtime <name>` runs it.
#![cfg(feature = "db-tests")]

use std::str::FromStr as _;
use std::sync::Arc;

use bytes::Bytes;
use serde_json::{json, Value};
use sqlx::postgres::{PgConnectOptions, PgConnection};
use sqlx::Connection as _;
use weft_platform_traits::FakeObjectStore;
use weft_runtime::carry::{carry, Done, Install};
use weft_task_store::schema_guard::{generation, Generation};

const OLD_SCHEMA: &str = include_str!("fixtures/serverless_schema.sql");
const RUN: &str = "e4366595-1cfd-446c-b70a-5f1af995f80a";
const OTHER_RUN: &str = "11111111-2222-4333-8444-555555555555";
const PROJECT: &str = "9dfba28c-45c3-4ccc-aa77-985947728672";

fn server() -> PgConnectOptions {
    PgConnectOptions::from_str(&std::env::var("DATABASE_URL").expect("DATABASE_URL names the test server")).unwrap()
}

/// An install whose database is a fresh name on the test server.
fn install(objects: Arc<FakeObjectStore>) -> Install {
    let name = format!("carry_{}", uuid::Uuid::new_v4().simple());
    Install { live: server().database(&name), objects: objects as weft_platform_traits::SharedObjectStore }
}

async fn on(install: &Install, database: &str) -> PgConnection {
    PgConnection::connect_with(&install.live.clone().database(database)).await.unwrap()
}

async fn exists(install: &Install, database: &str) -> bool {
    let mut server = on(install, "postgres").await;
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)").bind(database).fetch_one(&mut server).await.unwrap()
}

fn name(install: &Install) -> String {
    install.live.get_database().unwrap().to_string()
}

fn old_name(install: &Install) -> String {
    format!("{}_gen_serverless", name(install))
}

/// A run record row as the `serverless` generation wrote it: today's
/// events, serialized, with the names this generation changed put back.
fn old_record() -> Vec<u8> {
    use weft_journal::RunEvent;
    let run_id = weft_core::RunId::parse_str(RUN).unwrap();
    let events = vec![
        RunEvent::RunStarted {
            run_id,
            project_id: uuid::Uuid::parse_str(PROJECT).unwrap(),
            entry_node: "fetch".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("d".into()),
            binary_hash: Some("b".into()),
            source_version: Some("v1".into()),
            run_kind: weft_core::run::RunKind::Project,
            selection: None,
            seed: None,
            instance: None,
            stand_in: None,
            fired_trigger: None,
            instance_values: Default::default(),
            picks: Default::default(),
            settings: Default::default(),
            at_unix: 1,
        },
        RunEvent::PortEmitted {
            run_id,
            emission_id: uuid::Uuid::nil(),
            node_id: "fetch".into(),
            frames: Vec::new(),
            port: "file".into(),
            value: Arc::new(json!({ "key": format!("t1/run/{RUN}/f0"), "mimeType": "text/plain" })),
            provided: false,
            at_unix: 2,
        },
        RunEvent::RunCancelled {
            run_id,
            reason: "stopped by a sibling".into(),
            cause: Some(weft_core::run::CancelCause::Run { by: weft_core::RunId::parse_str(OTHER_RUN).unwrap(), tag: "t".into() }),
            at_unix: 3,
        },
    ];
    let json = serde_json::to_string(&events)
        .unwrap()
        .replace("\"run_started\"", "\"execution_started\"")
        .replace("\"run_cancelled\"", "\"execution_cancelled\"")
        .replace("{\"kind\":\"run\",\"by\"", "{\"kind\":\"execution\",\"by\"")
        .replace("t1/run/", "t1/exec/");
    weft_journal::stored::row_of_json(json.as_bytes())
}

/// A database of the previous generation named `database`, with one run
/// of each shape the carry rewrites, and its run file in `objects`.
async fn old_install(install: &Install, database: &str) {
    let mut server = on(install, "postgres").await;
    sqlx::raw_sql(&format!("CREATE DATABASE \"{database}\"")).execute(&mut server).await.unwrap();
    let mut old = on(install, database).await;
    sqlx::raw_sql(OLD_SCHEMA).execute(&mut old).await.expect("the previous generation's schema");
    let mut old = on(install, database).await;
    sqlx::query(
        "INSERT INTO run (execution_id, project_id, tenant_id, phase, kind, keeping, recorded, state, last_seq, started_at, \
         ended_at, outcome, cancel_cause, spec) \
         VALUES ($1::uuid, $2::uuid, 't1', 'fire', 'execution', 'durable', true, 'ended', 0, 1, 3, 'cancelled', $3, $4)",
    )
    .bind(RUN)
    .bind(PROJECT)
    .bind(json!({ "kind": "execution", "by": OTHER_RUN, "tag": "t" }))
    .bind(json!({ "name": "again", "frozen_from": { "version": "v1", "execution_id": RUN, "definition_hash": "d" },
                  "answers": [{ "node": "ask", "gave_up": "quiet" }] }))
    .execute(&mut old)
    .await
    .unwrap();
    sqlx::query("INSERT INTO run_log (execution_id, seq, events, writer, written_at) VALUES ($1::uuid, 0, $2, 'w', 1)")
        .bind(RUN)
        .bind(old_record())
        .execute(&mut old)
        .await
        .unwrap();
    sqlx::query("INSERT INTO execution_tag (execution_id, tag, tagged_at_unix) VALUES ($1::uuid, 't', 1)").bind(RUN).execute(&mut old).await.unwrap();
    sqlx::query("INSERT INTO trigger_bake (project_id, program_hash, bake_json) VALUES ($1::uuid, 'h', $2)")
        .bind(PROJECT)
        .bind(
            json!({ "project_id": PROJECT, "source_version": "v1", "execution_id": RUN, "at_unix": 1,
                    "program": { "definition_hash": "d", "binary_hash": "b", "implementations": {} }, "captured": {} })
            .to_string(),
        )
        .execute(&mut old)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO task (id, kind, status, tenant_id, payload, result, created_at_unix) \
         VALUES ('11111111-0000-4000-8000-000000000001'::uuid, 'program_call', 'done', 't1', $1, $2, 1)",
    )
    .bind(json!({ "by": RUN, "stop_self": "keep", "call": { "call": "runs_clean", "filter": { "older_than_secs": 60 }, "running": "wait" } }))
    .bind(json!({ "value": { "deleted": 1, "cancelled": 0, "left_running": 2 }, "stops_asker": false }))
    .execute(&mut old)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO task (id, kind, status, tenant_id, payload, created_at_unix) \
         VALUES ('11111111-0000-4000-8000-000000000002'::uuid, 'program_call', 'pending', 't1', $1, 1)",
    )
    .bind(json!({ "by": RUN, "stop_self": "keep", "call": { "call": "values_change", "instance": "ada",
                  "set": [{ "step": "read", "field": "sheet", "value": "s-1" }], "clear": [{ "step": "tick", "field": "cron" }] } }))
    .execute(&mut old)
    .await
    .unwrap();
    sqlx::query("INSERT INTO instance_value (tenant_id, project_id, instance_id, step, field, value) VALUES ('t1', $1::uuid, 'ada', 'read', 'sheet', '\"s-1\"')")
        .bind(PROJECT)
        .execute(&mut old)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO runtime_file (key, tenant_id, mime_type, filename, size_bytes, created_at_unix) \
         VALUES ($1, 't1', 'text/plain', 'f.txt', 5, 1)",
    )
    .bind(format!("t1/exec/{RUN}/f0"))
    .execute(&mut old)
    .await
    .unwrap();
    sqlx::query("INSERT INTO project_definition (project_id, definition_hash, project_json, recorded_at_unix) VALUES ($1::uuid, 'd', $2, 1)")
        .bind(PROJECT)
        .bind(program_with_a_loop().to_string())
        .execute(&mut old)
        .await
        .unwrap();
    install.objects.put(&format!("runtime/t1/exec/{RUN}/f0"), Bytes::from_static(b"hello")).await.unwrap();
}

/// A stored program with one loop capped at 5 iterations, its settings
/// spelled the way the previous generation spelled them.
fn program_with_a_loop() -> Value {
    json!({
        "id": PROJECT,
        "nodes": [
            { "id": "l__in", "nodeType": "LoopIn", "label": null, "position": { "x": 0.0, "y": 0.0 },
              "config": { "parentId": "l", "parallel": false, "max_iters": 5, "trim_on_mismatch": false } },
            { "id": "l__out", "nodeType": "LoopOut", "label": null, "position": { "x": 0.0, "y": 0.0 }, "config": { "parentId": "l" } },
        ],
        "edges": [],
        "groups": [{ "id": "l", "kind": "loop", "label": null, "loopConfig": { "parallel": false, "max_iters": 5, "trim_on_mismatch": false } }],
    })
}

/// What the carried database must hold: every row, as this generation
/// writes it.
async fn assert_carried(install: &Install) {
    let mut db = on(install, &name(install)).await;
    assert_eq!(generation(&mut db).await.unwrap(), Generation::Current);
    let (kind, cause, spec): (String, Value, Value) =
        sqlx::query_as("SELECT kind, cancel_cause, spec FROM run WHERE run_id = $1::uuid").bind(RUN).fetch_one(&mut db).await.unwrap();
    assert_eq!(kind, "project");
    assert_eq!(cause, json!({ "kind": "run", "by": OTHER_RUN, "tag": "t" }));
    assert_eq!(spec["frozenFrom"], json!({ "version": "v1", "runId": RUN, "definitionHash": "d" }));
    assert_eq!(spec["answers"][0]["gaveUp"], "quiet");
    let record = weft_journal::record::read_record(&mut db, weft_core::RunId::parse_str(RUN).unwrap(), None).await.unwrap();
    let events = serde_json::to_value(record.events(weft_core::RunId::parse_str(RUN).unwrap()).expect("the record reads")).unwrap();
    assert_eq!(events[0]["kind"], "run_started");
    assert_eq!(events[1]["value"]["key"], format!("t1/run/{RUN}/f0"), "a file value in a record names the file's new key");
    assert_eq!(events[2]["cause"]["kind"], "run");
    let (tag,): (String,) = sqlx::query_as("SELECT tag FROM run_tag WHERE run_id = $1::uuid").bind(RUN).fetch_one(&mut db).await.unwrap();
    assert_eq!(tag, "t");
    let (bake,): (String,) = sqlx::query_as("SELECT bake_json FROM trigger_bake").fetch_one(&mut db).await.unwrap();
    let bake: weft_dispatcher::journal::TriggerBake = serde_json::from_str(&bake).expect("the bake reads");
    assert_eq!(bake.run_id.to_string(), RUN);
    let (payload, result): (Value, Value) = sqlx::query_as("SELECT payload, result FROM task WHERE id = '11111111-0000-4000-8000-000000000001'::uuid")
        .fetch_one(&mut db)
        .await
        .unwrap();
    assert_eq!(payload["call"]["filter"], json!({ "olderThanSecs": 60 }));
    assert_eq!(result["value"]["leftRunning"], 2);
    let (change,): (Value,) = sqlx::query_as("SELECT payload FROM task WHERE id = '11111111-0000-4000-8000-000000000002'::uuid")
        .fetch_one(&mut db)
        .await
        .unwrap();
    assert_eq!(change["call"]["set"], json!([{ "node": "read", "field": "sheet", "value": "s-1" }]));
    assert_eq!(change["call"]["clear"], json!([{ "node": "tick", "field": "cron" }]));
    let (node, value): (String, Value) =
        sqlx::query_as("SELECT node, value FROM instance_value WHERE instance_id = 'ada'").fetch_one(&mut db).await.unwrap();
    assert_eq!((node.as_str(), value), ("read", json!("s-1")), "the place a value is kept for moved to its new column");
    let (key,): (String,) = sqlx::query_as("SELECT key FROM runtime_file").fetch_one(&mut db).await.unwrap();
    assert_eq!(key, format!("t1/run/{RUN}/f0"));
    let (queued,): (i64,) = sqlx::query_as("SELECT count(*) FROM run_search_queue").fetch_one(&mut db).await.unwrap();
    assert_eq!(queued, 1, "the ended run is queued for the search index");
    assert_eq!(install.objects.get(&format!("runtime/t1/run/{RUN}/f0")).await.unwrap().as_deref(), Some(&b"hello"[..]));
    let (program,): (String,) = sqlx::query_as("SELECT project_json FROM project_definition").fetch_one(&mut db).await.unwrap();
    let program: weft_core::ProjectDefinition = serde_json::from_str(&program).expect("the stored program reads");
    let loop_in = program.nodes.iter().find(|n| n.node_type == "LoopIn").expect("the loop's boundary");
    assert_eq!(loop_in.config, json!({ "parentId": "l", "parallel": false, "maxIters": 5, "trimOnMismatch": false }));
    let config = weft_core::run::loop_runtime::LoopConfig::from_node_config(&loop_in.config).unwrap();
    assert_eq!(config.max_iters, Some(5), "the carried loop keeps its cap");
    let weft_core::project::GroupKind::Loop { loop_config } = &program.groups[0].kind else { panic!("the loop group") };
    assert_eq!(loop_config, &json!({ "parallel": false, "maxIters": 5, "trimOnMismatch": false }));
}

async fn assert_cleaned_up(install: &Install) {
    assert!(!exists(install, &old_name(install)).await, "the old database is gone");
    assert!(!exists(install, &format!("{}_carrying", name(install))).await, "no carrying database is left");
    assert_eq!(install.objects.get(&format!("runtime/t1/exec/{RUN}/f0")).await.unwrap(), None, "the old file copy is gone");
}

async fn drop_all(install: &Install) {
    let mut server = on(install, "postgres").await;
    for db in [name(install), old_name(install), format!("{}_carrying", name(install))] {
        sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)")).execute(&mut server).await.unwrap();
    }
}

#[tokio::test]
async fn a_database_of_the_previous_generation_is_carried_whole() {
    let install = install(Arc::new(FakeObjectStore::new()));
    old_install(&install, &name(&install)).await;
    let done = carry(&install).await.expect("the carry");
    assert_eq!(done, Done::Carried { runs: 1, files: 1 });
    assert_carried(&install).await;
    assert_cleaned_up(&install).await;
    assert_eq!(carry(&install).await.unwrap(), Done::Nothing, "a second start finds nothing to do");
    drop_all(&install).await;
}

/// Interrupted right after the old database was set aside: the install
/// has no database of its name, the next start carries from the old one.
#[tokio::test]
async fn a_carry_interrupted_after_setting_the_old_database_aside_is_finished() {
    let install = install(Arc::new(FakeObjectStore::new()));
    old_install(&install, &old_name(&install)).await;
    carry(&install).await.expect("the carry picks up from the set-aside database");
    assert_carried(&install).await;
    assert_cleaned_up(&install).await;
    drop_all(&install).await;
}

/// Interrupted in the middle of the copy: a half-filled carrying database
/// beside the set-aside one is thrown away and built again.
#[tokio::test]
async fn a_carry_interrupted_mid_copy_starts_the_copy_again() {
    let install = install(Arc::new(FakeObjectStore::new()));
    old_install(&install, &old_name(&install)).await;
    let carrying = format!("{}_carrying", name(&install));
    let mut server = on(&install, "postgres").await;
    sqlx::raw_sql(&format!("CREATE DATABASE \"{carrying}\"")).execute(&mut server).await.unwrap();
    let mut half = on(&install, &carrying).await;
    sqlx::raw_sql("CREATE TABLE run (run_id UUID PRIMARY KEY); INSERT INTO run VALUES (gen_random_uuid())").execute(&mut half).await.unwrap();
    half.close().await.unwrap();
    // A file copy that already landed is copied again, harmlessly.
    install.objects.put(&format!("runtime/t1/run/{RUN}/f0"), Bytes::from_static(b"hel")).await.unwrap();
    carry(&install).await.expect("the carry builds the carrying database again");
    assert_carried(&install).await;
    assert_cleaned_up(&install).await;
    drop_all(&install).await;
}

/// Interrupted after the carried database took the install's name: only
/// the cleanup is left, and the next start does it.
#[tokio::test]
async fn a_carry_interrupted_before_the_cleanup_finishes_it() {
    let install = install(Arc::new(FakeObjectStore::new()));
    old_install(&install, &name(&install)).await;
    carry(&install).await.unwrap();
    // Put the leftovers back, as if the cleanup never ran.
    old_install(&install, &old_name(&install)).await;
    assert!(exists(&install, &old_name(&install)).await);
    assert_eq!(carry(&install).await.unwrap(), Done::Nothing, "nothing is carried again");
    assert_cleaned_up(&install).await;
    let mut db = on(&install, &name(&install)).await;
    let (runs,): (i64,) = sqlx::query_as("SELECT count(*) FROM run").fetch_one(&mut db).await.unwrap();
    assert_eq!(runs, 1, "the carried database is the one kept");
    drop_all(&install).await;
}

/// A database already of this generation is left alone.
#[tokio::test]
async fn a_database_of_this_generation_is_left_alone() {
    let install = install(Arc::new(FakeObjectStore::new()));
    let mut server = on(&install, "postgres").await;
    sqlx::raw_sql(&format!("CREATE DATABASE \"{}\"", name(&install))).execute(&mut server).await.unwrap();
    let pool = sqlx::PgPool::connect_with(install.live.clone()).await.unwrap();
    weft_runtime::schema::apply(&pool).await.unwrap();
    pool.close().await;
    assert_eq!(carry(&install).await.unwrap(), Done::Nothing);
    drop_all(&install).await;
}
