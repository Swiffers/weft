//! Reading the carried database back with today's types, before it
//! becomes the install's: every run record, every stored spec, bake,
//! program, signal and queued program call. A row that does not read is
//! a rule the carry is missing, and the carry stops there, with the old
//! database untouched, naming the row.

use anyhow::Context as _;
use futures::TryStreamExt as _;
use sqlx::postgres::PgConnection;
use weft_core::RunId;

/// Read back what the runtime reads from the carried database `conn`.
pub async fn verify(conn: &mut PgConnection) -> anyhow::Result<()> {
    let runs: Vec<RunId> = sqlx::query_scalar("SELECT run_id FROM run ORDER BY run_id").fetch_all(&mut *conn).await?;
    for run in &runs {
        let record = weft_journal::record::read_record(conn, *run, None).await.with_context(|| format!("read the carried record of run {run}"))?;
        record.events(*run).map_err(|e| anyhow::anyhow!("the carried record of run {run} does not read: {e}"))?;
    }
    println!("  read back {} run record(s)", runs.len());

    each::<weft_core::run_spec::RunSpec>(conn, "SELECT run_id::text, spec FROM run WHERE spec IS NOT NULL", "the spec of run").await?;
    each_text::<weft_dispatcher::journal::TriggerBake>(conn, "SELECT project_id::text, bake_json FROM trigger_bake", "the bake of project").await?;
    each_text::<weft_core::primitive::SignalSpec>(conn, "SELECT token, spec_json FROM signal", "the spec of signal").await?;
    each_text::<weft_core::ProjectDefinition>(conn, "SELECT id::text, project_json FROM project", "the program of project").await?;
    each_text::<weft_core::ProjectDefinition>(conn, "SELECT project_id::text, project_json FROM project_definition", "a stored program of project").await?;
    each::<weft_core::AccessSpec>(conn, "SELECT id::text, spec_json FROM access_grant", "the spec of connection").await?;
    each::<weft_core::AccessSpec>(conn, "SELECT state, spec_json FROM access_connect", "the spec of connect").await?;
    each::<std::collections::BTreeMap<String, weft_core::access::events::EventsSpec>>(
        conn,
        "SELECT service || ':' || recipe_hash, events_json FROM service_events_recipe",
        "the events recipe",
    )
    .await?;
    each::<weft_core::program::ProgramCallPayload>(conn, "SELECT id::text, payload FROM task WHERE kind = 'program_call'", "the program call of task").await?;
    each::<weft_core::program::ProgramCallOutcome>(
        conn,
        "SELECT id::text, result FROM task WHERE kind = 'program_call' AND result IS NOT NULL AND result <> 'null'::jsonb",
        "the answer of task",
    )
    .await?;
    Ok(())
}

/// Every `(id, json)` row `query` answers reads as a `T`.
async fn each<T: serde::de::DeserializeOwned>(conn: &mut PgConnection, query: &str, what: &str) -> anyhow::Result<()> {
    let mut rows = sqlx::query_as::<_, (String, serde_json::Value)>(query).fetch(conn);
    while let Some((id, value)) = rows.try_next().await? {
        serde_json::from_value::<T>(value).map_err(|e| anyhow::anyhow!("{what} {id} does not read after the carry: {e}"))?;
    }
    Ok(())
}

/// [`each`], for JSON held as text.
async fn each_text<T: serde::de::DeserializeOwned>(conn: &mut PgConnection, query: &str, what: &str) -> anyhow::Result<()> {
    let mut rows = sqlx::query_as::<_, (String, String)>(query).fetch(conn);
    while let Some((id, text)) = rows.try_next().await? {
        serde_json::from_str::<T>(&text).map_err(|e| anyhow::anyhow!("{what} {id} does not read after the carry: {e}"))?;
    }
    Ok(())
}
