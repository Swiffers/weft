//! Copying the old database into the carrying one, table by table, and
//! the run-scoped files to their new keys.

use std::collections::{BTreeMap, HashSet};

use anyhow::Context as _;
use futures::TryStreamExt as _;
use serde_json::{Map, Value};
use sqlx::postgres::PgConnection;

use super::rewrite;

/// The one table renamed between the generations.
const TABLE_RENAMES: &[(&str, &str)] = &[("run_tag", "execution_tag")];

/// The columns renamed between the generations past the execution-to-run
/// one (`table`, today's name, the old name): the place a pick or an
/// instance's value is kept for was called the step.
const COLUMN_RENAMES: &[(&str, &str, &str)] = &[("install_pick", "node", "step"), ("instance_value", "node", "step")];

/// The tables the carry does not copy: the search index, which is built
/// again from the runs (every ended run is queued for it).
const REBUILT: &[&str] = &["run_search"];

/// Columns whose whole value is the user's own data: only the file keys
/// inside it move.
const USER_COLUMNS: &[(&str, &str)] = &[
    ("parked_fire", "payload"),
    ("signal", "port_snapshot"),
    ("signal", "kind_state"),
    ("signal", "consumer_payload"),
    ("instance_value", "value"),
];

/// How many rows one insert carries, and how many bytes of them at most
/// (one statement's JSON stays well under Postgres's limit on one value).
const BATCH: usize = 500;
const BATCH_BYTES: usize = 32 << 20;

/// What the copy moved.
pub struct Copied {
    pub runs: u64,
}

/// One column of a table, as `information_schema` describes it.
struct Column {
    name: String,
    data_type: String,
    generated: bool,
}

impl Column {
    fn json(&self) -> bool {
        matches!(self.data_type.as_str(), "jsonb" | "json") || (self.data_type == "text" && self.name.ends_with("_json"))
    }
}

async fn columns(conn: &mut PgConnection, table: &str) -> anyhow::Result<Vec<Column>> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT column_name::text, data_type::text, is_generated::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(conn)
    .await?;
    Ok(rows.into_iter().map(|(name, data_type, generated)| Column { name, data_type, generated: generated == "ALWAYS" }).collect())
}

/// Copy every table of the old database `from` into `to`, rewritten on
/// the way. Triggers and foreign keys are off for the copy: the rows are
/// copied as they stood, and nothing a trigger would do (a notice, a queue
/// row) is anyone's to hear while the runtime is stopped.
pub async fn copy(from: &mut PgConnection, to: &mut PgConnection) -> anyhow::Result<Copied> {
    sqlx::raw_sql("SET session_replication_role = replica").execute(&mut *to).await?;
    let skipped = unfinished_run_uploads(from).await?;
    let tables: Vec<&str> = crate::schema::groups().iter().flat_map(|g| g.tables.iter().copied()).collect();
    for table in tables {
        if REBUILT.contains(&table) {
            continue;
        }
        let old = TABLE_RENAMES.iter().find(|(new, _)| *new == table).map_or(table, |(_, old)| *old);
        let source = columns(from, old).await?;
        if source.is_empty() {
            continue;
        }
        let target = columns(to, table).await?;
        let copied = copy_table(from, to, old, table, &source, &target, &skipped).await.with_context(|| format!("carry the table {table}"))?;
        println!("  carried {copied} row(s) of {table}");
    }
    let queued = sqlx::query("INSERT INTO run_search_queue (run_id) SELECT run_id FROM run WHERE state = 'ended' AND recorded ON CONFLICT DO NOTHING")
        .execute(&mut *to)
        .await?
        .rows_affected();
    println!("  queued {queued} run(s) for the search index to build again");
    reset_sequences(to).await?;
    sqlx::raw_sql("SET session_replication_role = origin").execute(&mut *to).await?;
    let (runs,): (i64,) = sqlx::query_as("SELECT count(*) FROM run").fetch_one(&mut *to).await?;
    Ok(Copied { runs: runs as u64 })
}

/// The run-scoped files still being uploaded when the old runtime
/// stopped: their parts are tied to the old key in the bucket, so they
/// are not carried (the upload was cut short with the runtime anyway).
async fn unfinished_run_uploads(from: &mut PgConnection) -> anyhow::Result<HashSet<String>> {
    if columns(from, "runtime_file").await?.is_empty() {
        return Ok(HashSet::new());
    }
    let keys: Vec<String> = sqlx::query_scalar("SELECT key FROM runtime_file WHERE status <> 'active'").fetch_all(from).await?;
    Ok(keys.into_iter().filter(|k| rewrite::file_key(k).is_some()).collect())
}

async fn copy_table(
    from: &mut PgConnection,
    to: &mut PgConnection,
    old: &str,
    table: &str,
    source: &[Column],
    target: &[Column],
    skipped: &HashSet<String>,
) -> anyhow::Result<u64> {
    let have: HashSet<&str> = source.iter().map(|c| c.name.as_str()).collect();
    // Each column of today's table, and the old column it is read from:
    // its own name, the one it was renamed from, or the one that said
    // execution.
    let mapping: Vec<(&Column, String)> = target
        .iter()
        .filter(|c| !c.generated)
        .filter_map(|c| {
            let renamed = COLUMN_RENAMES
                .iter()
                .find(|(t, new, _)| *t == table && *new == c.name)
                .map_or_else(|| c.name.replace("run", "execution"), |(_, _, old)| old.to_string());
            if have.contains(c.name.as_str()) {
                Some((c, c.name.clone()))
            } else if have.contains(renamed.as_str()) {
                Some((c, renamed))
            } else {
                None
            }
        })
        .collect();
    // A JSON column holds either SQL NULL or a JSON value, `null`
    // included, and `row_to_json` would write both as `null`; so each is
    // read wrapped in a one-element array (SQL NULL stays NULL) and
    // written back unwrapped.
    let is_json = |c: &Column| matches!(c.data_type.as_str(), "jsonb" | "json");
    let names: Vec<String> = mapping.iter().map(|(c, _)| format!("\"{}\"", c.name)).collect();
    let values: Vec<String> = mapping
        .iter()
        .map(|(c, _)| match c.data_type.as_str() {
            "jsonb" => format!("(x->'{}')->0", c.name),
            "json" => format!("((x->'{}')->0)::json", c.name),
            _ => format!("r.\"{}\"", c.name),
        })
        .collect();
    let insert = format!(
        "INSERT INTO \"{table}\" ({}) OVERRIDING SYSTEM VALUE SELECT {} \
         FROM jsonb_array_elements($1::jsonb) AS x, jsonb_populate_record(NULL::\"{table}\", x) AS r",
        names.join(", "),
        values.join(", ")
    );
    let reads: Vec<String> = source
        .iter()
        .map(|c| match c.data_type.as_str() {
            "jsonb" | "json" => format!("CASE WHEN \"{0}\" IS NULL THEN NULL ELSE jsonb_build_array(\"{0}\") END AS \"{0}\"", c.name),
            _ => format!("\"{}\"", c.name),
        })
        .collect();
    let select = format!("SELECT row_to_json(t)::text FROM (SELECT {} FROM \"{old}\") t", reads.join(", "));
    let source_json: HashSet<&str> = source.iter().filter(|c| is_json(c)).map(|c| c.name.as_str()).collect();
    let mut rows = sqlx::query_scalar::<_, String>(&select).fetch(from);
    let mut batch: Vec<Value> = Vec::with_capacity(BATCH);
    let mut bytes = 0usize;
    let mut count = 0u64;
    while let Some(row) = rows.try_next().await? {
        bytes += row.len();
        let old_row: Map<String, Value> = serde_json::from_str(&row)?;
        let mut new_row = Map::new();
        for (column, from_name) in &mapping {
            match old_row.get(from_name) {
                // SQL NULL: left out, so it is written as NULL.
                Some(Value::Null) if source_json.contains(from_name.as_str()) => {}
                Some(Value::Array(wrapped)) if source_json.contains(from_name.as_str()) => {
                    new_row.insert(column.name.clone(), wrapped.first().cloned().unwrap_or(Value::Null));
                }
                Some(value) => {
                    new_row.insert(column.name.clone(), value.clone());
                }
                None => {}
            }
        }
        if skips(table, &new_row, skipped) {
            continue;
        }
        rewrite_row(table, &mut new_row, target)?;
        for (column, _) in &mapping {
            if is_json(column) {
                if let Some(value) = new_row.remove(&column.name) {
                    new_row.insert(column.name.clone(), Value::Array(vec![value]));
                }
            }
        }
        batch.push(Value::Object(new_row));
        if batch.len() == BATCH || bytes >= BATCH_BYTES {
            count += flush(to, &insert, &mut batch).await?;
            bytes = 0;
        }
    }
    drop(rows);
    count += flush(to, &insert, &mut batch).await?;
    Ok(count)
}

/// A row of an unfinished run-scoped upload, which is not carried.
fn skips(table: &str, row: &Map<String, Value>, skipped: &HashSet<String>) -> bool {
    matches!(table, "runtime_file" | "runtime_file_part" | "public_file_link")
        && row.get("key").and_then(Value::as_str).is_some_and(|k| skipped.contains(k))
}

async fn flush(to: &mut PgConnection, insert: &str, batch: &mut Vec<Value>) -> anyhow::Result<u64> {
    if batch.is_empty() {
        return Ok(0);
    }
    let n = sqlx::query(insert).bind(Value::Array(std::mem::take(batch))).execute(&mut *to).await?.rows_affected();
    Ok(n)
}

/// One row of `table`, keyed by today's column names, as this generation
/// stores it.
fn rewrite_row(table: &str, row: &mut Map<String, Value>, columns: &[Column]) -> anyhow::Result<()> {
    let mut done: HashSet<&str> = HashSet::new();
    match table {
        "run" => {
            if row.get("kind").and_then(Value::as_str) == Some("execution") {
                row.insert("kind".into(), Value::String("project".into()));
            }
            if let Some(spec) = row.get_mut("spec") {
                rewrite::run_spec(spec);
                done.insert("spec");
            }
        }
        "run_log" => {
            if let Some(Value::String(hex)) = row.get_mut("events") {
                *hex = rewritten_record(hex)?;
            }
        }
        "trigger_bake" => {
            json_column(row, columns, "bake_json", rewrite::trigger_bake)?;
            done.insert("bake_json");
        }
        "task" => {
            let kind = row.get("kind").and_then(Value::as_str).unwrap_or_default().to_string();
            let mut result = row.remove("result");
            if let Some(payload) = row.get_mut("payload") {
                rewrite::task(&kind, payload, result.as_mut().filter(|r| !r.is_null()));
            }
            if let Some(result) = result {
                row.insert("result".into(), result);
            }
            done.extend(["payload", "result"]);
        }
        "access_grant" => {
            json_column(row, columns, "spec_json", rewrite::metadata_keys)?;
            // The recipe hash is over the spec's events, as stored now.
            let hash = match row.get("spec_json") {
                Some(spec) if !spec.is_null() => {
                    let spec: weft_core::AccessSpec = serde_json::from_value(spec.clone()).context("a connection's spec does not read after the carry")?;
                    weft_access_store::events_recipe_hash(&spec.events)
                }
                _ => None,
            };
            if row.contains_key("events_recipe_hash") {
                row.insert("events_recipe_hash".into(), hash.map_or(Value::Null, Value::String));
            }
            if let Some(Value::String(sealed)) = row.get_mut("values_sealed") {
                let mut values = weft_access_store::open_json(sealed).context("open a connection's stored values")?;
                rewrite::connection_values(&mut values);
                *sealed = weft_access_store::seal_json(&values)?;
            }
            json_column(row, columns, "value_names", rewrite::connection_value_names)?;
            done.extend(["value_names"]);
        }
        "access_connect" => json_column(row, columns, "spec_json", rewrite::metadata_keys)?,
        "access_connect_result" => {
            json_column(row, columns, "result_json", rewrite::camel_keys)?;
        }
        "signal_subscription" => {
            json_column(row, columns, "captures_json", rewrite::connection_values)?;
            done.insert("captures_json");
        }
        "service_events_recipe" => {
            json_column(row, columns, "events_json", rewrite::metadata_keys)?;
            let events: std::collections::BTreeMap<String, weft_core::access::events::EventsSpec> =
                serde_json::from_value(row.get("events_json").cloned().unwrap_or(Value::Null)).context("an events recipe does not read after the carry")?;
            let hash = weft_access_store::events_recipe_hash(&events).context("a recorded events recipe declares no topic")?;
            row.insert("recipe_hash".into(), Value::String(hash));
        }
        "project" | "project_definition" => json_column(row, columns, "project_json", |program| {
            rewrite::metadata_keys(program);
            rewrite::loop_settings(program);
        })?,
        _ => {}
    }
    for column in columns {
        let name = column.name.as_str();
        if done.contains(name) {
            continue;
        }
        let whole_user_data = USER_COLUMNS.contains(&(table, name));
        if column.json() {
            json_column(row, columns, name, |v| if whole_user_data { rewrite::file_keys(v) } else { rewrite::everywhere(v) })?;
        } else if let Some(Value::String(s)) = row.get_mut(name) {
            if let Some(new) = rewrite::file_key(s) {
                *s = new;
            }
        } else if let Some(Value::Array(items)) = row.get_mut(name) {
            // A text[] column (a run's `stale` nodes, a signal's tags).
            for item in items {
                if let Value::String(s) = item {
                    if let Some(new) = rewrite::file_key(s) {
                        *s = new;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Rewrite the JSON column `name` of `row` with `f`, whether Postgres
/// holds it as JSON (a string there is a JSON string) or as JSON text.
fn json_column(row: &mut Map<String, Value>, columns: &[Column], name: &str, f: impl FnOnce(&mut Value)) -> anyhow::Result<()> {
    let as_text = columns.iter().any(|c| c.name == name && c.data_type == "text");
    match row.get_mut(name) {
        None | Some(Value::Null) => {}
        Some(Value::String(text)) if as_text => {
            let mut value: Value = serde_json::from_str(text).with_context(|| format!("the column {name} is not JSON"))?;
            f(&mut value);
            *text = value.to_string();
        }
        Some(value) => f(value),
    }
    Ok(())
}

/// A `run_log` row's events, as `row_to_json` writes a `bytea` (`\x` and
/// hex), rewritten for this generation and written back the same way.
fn rewritten_record(hex_text: &str) -> anyhow::Result<String> {
    let bytes = hex::decode(hex_text.strip_prefix("\\x").unwrap_or(hex_text)).context("a run record row is not hex")?;
    let json = weft_journal::stored::json_of_row(&bytes).context("a run record row does not decompress")?;
    let mut events: Value = serde_json::from_slice(&json).context("a run record row is not JSON")?;
    rewrite::events(&mut events)?;
    let row = weft_journal::stored::row_of_json(&serde_json::to_vec(&events)?);
    Ok(format!("\\x{}", hex::encode(row)))
}

/// Every sequence a column draws from, set past the largest value the
/// copy wrote, so the next row the runtime writes takes a fresh one.
async fn reset_sequences(to: &mut PgConnection) -> anyhow::Result<()> {
    let owned: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT s.relname::text, t.relname::text, a.attname::text FROM pg_depend d \
         JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S' \
         JOIN pg_class t ON t.oid = d.refobjid \
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid \
         JOIN pg_namespace n ON n.oid = s.relnamespace AND n.nspname = 'public' \
         WHERE d.deptype IN ('a', 'i')",
    )
    .fetch_all(&mut *to)
    .await?;
    for (sequence, table, column) in owned {
        sqlx::raw_sql(&format!(
            "SELECT setval('\"{sequence}\"', COALESCE((SELECT max(\"{column}\") FROM \"{table}\"), 0) + 1, false)"
        ))
        .execute(&mut *to)
        .await
        .with_context(|| format!("move the sequence {sequence} past what was carried"))?;
    }
    Ok(())
}

/// The bucket object of a runtime file (`weft_broker::runtime_store`).
// SYNC: object_key <-> crates/weft-broker/src/runtime_store.rs object_key
fn object_key(key: &str) -> String {
    format!("runtime/{key}")
}

/// The finished run-scoped files of the old database, and the key each
/// moves to.
async fn moving_files(from: &mut PgConnection) -> anyhow::Result<BTreeMap<String, String>> {
    if columns(from, "runtime_file").await?.is_empty() {
        return Ok(BTreeMap::new());
    }
    let keys: Vec<String> = sqlx::query_scalar("SELECT key FROM runtime_file WHERE status = 'active'").fetch_all(from).await?;
    Ok(keys.into_iter().filter_map(|k| rewrite::file_key(&k).map(|new| (k, new))).collect())
}

/// Copy every finished run-scoped file to its new key. Copying again
/// is harmless, so an interrupted copy is simply done again.
pub async fn copy_files(from: &mut PgConnection, objects: &weft_platform_traits::SharedObjectStore) -> anyhow::Result<u64> {
    let files = moving_files(from).await?;
    for (old, new) in &files {
        objects.copy(&object_key(old), &object_key(new)).await.with_context(|| format!("copy the file {old} to {new}"))?;
    }
    if !files.is_empty() {
        println!("  copied {} run file(s) to their new keys", files.len());
    }
    Ok(files.len() as u64)
}

/// Delete the old copies of the files [`copy_files`] copied, once the
/// carried database is the install's.
pub async fn delete_old_files(from: &mut PgConnection, objects: &weft_platform_traits::SharedObjectStore) -> anyhow::Result<()> {
    for old in moving_files(from).await?.keys() {
        objects.delete(&object_key(old)).await.with_context(|| format!("delete the old copy of {old}"))?;
    }
    Ok(())
}
