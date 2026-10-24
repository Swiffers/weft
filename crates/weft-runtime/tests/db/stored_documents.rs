//! A database an older weft filled, booted by this one: every document it
//! wrote is carried to today's shape (`weft_runtime::schema::documents`)
//! and reads, what was derived from the old JSON follows it, and what a
//! step moved out of the JSON is where it lives now. The
//! database is built as that older weft left it (its history up to
//! [`OLDER_WEFT_HAD`]), and the rows are the committed version 1 samples,
//! as the weft before versions wrote them: a Slack
//! connection and its recorded events recipes, a signing-secret
//! connection, a program carrying a service recipe, and a project's resync.
//! (What the same boot moves out of a column it then drops is
//! `infra_units_move`.)

use serde_json::Value;
use sqlx::PgPool;

fn samples(document: &str, version: i32) -> Vec<Value> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("documents")
        .join(document)
        .join(format!("v{version}.json"));
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

/// The hash the weft before wrote for a recipe: sha256 over its canonical
/// JSON, as it stood.
fn old_recipe_hash(events: &Value) -> String {
    weft_core::project::hash::sha256_hex(weft_core::project::hash::canonical_json(events).as_bytes())
}

/// The last migration the weft these rows come from had: the release
/// before the units and the resync's drive moved out of where they were.
pub(crate) const OLDER_WEFT_HAD: &str = "20261020T134642_infra_lifecycle_command_health_commands";

#[sqlx::test]
async fn a_database_an_older_weft_filled_boots_and_every_row_reads(pool: PgPool) {
    weft_task_store::schema_guard::build_as_of(&pool, &weft_runtime::schema::groups(), |m| m.id <= OLDER_WEFT_HAD)
        .await
        .unwrap();
    let specs = samples("access_grant.spec_json", 1);
    let (slack, hmac) = (&specs[0], &specs[1]);
    let slack_events = &slack["events"];
    let old_hash = old_recipe_hash(slack_events);
    let slack_id = uuid::Uuid::new_v4();
    let hmac_id = uuid::Uuid::new_v4();
    for (id, service, spec, hash) in
        [(slack_id, "slack", slack, Some(old_hash.clone())), (hmac_id, "hmac_auth", hmac, None)]
    {
        sqlx::query(
            "INSERT INTO access_grant (id, tenant_id, service, client_id, spec_json, values_sealed, \
             provider_account, events_recipe_hash) VALUES ($1, 'local', $2, 'cid', $3, '', 'T123', $4)",
        )
        .bind(id)
        .bind(service)
        .bind(spec)
        .bind(hash)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO service_events_recipe (service, recipe_hash, events_json) VALUES ('slack', $1, $2)")
        .bind(&old_hash)
        .bind(slack_events)
        .execute(&pool)
        .await
        .unwrap();
    let project_id = uuid::Uuid::new_v4();
    let program = &samples("project_definition.project_json", 1)[0];
    sqlx::query(
        "INSERT INTO project_definition (project_id, definition_hash, project_json, recorded_at_unix) \
         VALUES ($1, 'the-hash-it-was-made-under', $2, 0)",
    )
    .bind(project_id)
    .bind(program.to_string())
    .execute(&pool)
    .await
    .unwrap();
    // A resync under way, its drive inside its JSON.
    let resync = &samples("project.resync", 1)[0];
    sqlx::query(
        "INSERT INTO project (id, name, status, project_json, updated_at, tenant_id, transition, resync) \
         VALUES ($1, 'p', 'registered', $2, 0, 'local', 'resyncing', $3)",
    )
    .bind(project_id)
    .bind(program.to_string())
    .bind(resync)
    .execute(&pool)
    .await
    .unwrap();
    weft_runtime::schema::apply(&pool).await.unwrap();

    let read_spec = |id: uuid::Uuid| {
        let pool = pool.clone();
        async move {
            let (spec, version): (Value, i32) =
                sqlx::query_as("SELECT spec_json, spec_json_version FROM access_grant WHERE id = $1")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(version, weft_core::AccessSpec::STORED.version(), "the row was carried");
            weft_core::AccessSpec::STORED.decode(spec, version).expect("the connection's recipe reads")
        }
    };
    let slack_spec = read_spec(slack_id).await;
    let hmac_spec = read_spec(hmac_id).await;
    assert!(matches!(hmac_spec.verify, Some(weft_core::access::events::VerifyKind::Hmac { .. })));

    // The recorded recipe is kept under the hash of its new JSON, and the
    // Slack connection names that hash, so a verified push finds it.
    let recipes = weft_access_store::events_recipes_of(&pool, "slack").await.expect("the recorded recipes read");
    assert_eq!(recipes.len(), 1);
    let new_hash = weft_access_store::events_recipe_hash(&slack_spec.events).unwrap();
    assert_ne!(new_hash, old_hash);
    assert_eq!(recipes[0].recipe_hash, new_hash);
    let routed = weft_access_store::connections_for_event(&pool, "slack", "T123", &["cid".to_string()], &new_hash)
        .await
        .unwrap();
    assert_eq!(routed.iter().map(|t| t.id).collect::<Vec<_>>(), vec![slack_id]);
    let hmac_hash: Option<String> =
        sqlx::query_scalar("SELECT events_recipe_hash FROM access_grant WHERE id = $1").bind(hmac_id).fetch_one(&pool).await.unwrap();
    assert_eq!(hmac_hash, None);

    // The program reads, under the hash it was made under.
    let (json, version): (String, i32) = sqlx::query_as(
        "SELECT project_json, project_json_version FROM project_definition \
         WHERE project_id = $1 AND definition_hash = 'the-hash-it-was-made-under'",
    )
    .bind(project_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    weft_core::project::ProjectDefinition::STORED.decode(json.as_str(), version).expect("the program reads");

    // The resync's drive moved to its own column.
    let (plan, plan_version, driver): (Value, i32, Option<uuid::Uuid>) =
        sqlx::query_as("SELECT resync, resync_version, resync_driver FROM project WHERE id = $1")
            .bind(project_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(driver.map(|d| d.to_string()).as_deref(), resync["driver"].as_str());
    assert!(plan.get("driver").is_none(), "{plan}");
    let plan = weft_dispatcher::task_kinds::resync::ResyncPlan::stored(project_id, plan, plan_version, driver).expect("the plan reads");
    assert_eq!(Some(plan.driver), driver);
}
