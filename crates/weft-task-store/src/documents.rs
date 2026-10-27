//! The JSON documents a database keeps in its columns, carried at boot
//! from the shape an older weft wrote to the shape the code reads today.
//!
//! The table migrations (`crate::schema_guard`) move a table's SHAPE: its
//! columns, indexes, triggers. A column holding JSON has a shape of its
//! own, the Rust type the code reads it into, and renaming a field of that
//! type leaves every row an older weft wrote unreadable. So each column
//! whose rows outlive an update is a [`Document`]: the steps that carry
//! its JSON from version 1 to today, which its type's
//! `weft_core::stored::Kind` declares, listed once for the install
//! (`weft_runtime::schema::documents`).
//!
//! Every row says which version its document is at, in the integer column
//! beside it ([`Document::version_column`], `<column>_version`), which
//! every write sets to the version the writing code produces (the column
//! has no default) and every read hands to
//! `weft_core::stored::Kind::decode`.
//!
//! The boot carries the older rows between the schema's two parts
//! (`crate::schema_guard::apply_schema`): after the expand part, so a
//! column or table a step moves a value out to is there, and before the
//! contract part, which drops what the values moved out of and adds the
//! constraints that keep them out. So the readers almost never carry a
//! row, and what SQL reads of a row matches its JSON (a hash of it that a
//! join compares, a value moved out to a column of its own). For each
//! column, one step at a time, the rows at the step's version are read in
//! key order, carried, and written back with the next version, in batches
//! of [`BATCH`], each batch its own transaction under the schema's
//! advisory lock. The version column is the cursor: a boot that stops
//! midway, or a second copy booting beside it, finds exactly the rows
//! still to carry, and no row takes a step twice. A step whose ops change
//! nothing (it only moves values out, through its [`Fill`]) is one
//! statement, which needs no key to walk.
//!
//! The database records, per column, the version it was last carried to
//! (`weft_document_version`, one of the schema guard's own tables): a boot
//! that finds every column at the version this build reads does that one
//! read and nothing else, with no lock. A column recorded below today is
//! carried from the version it is recorded at (the steps before it ran,
//! and what their fills read may since be gone), and a column recorded at
//! a version this build does not know was carried by a newer weft, so the
//! boot is refused naming both versions.

use std::collections::HashMap;

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};

use crate::sql;
use weft_core::stored::{apply, current_version, round_trip, Kind, Op, Version};

/// How many rows one batch carries forward.
const BATCH: i64 = 500;

/// One column of JSON documents and what carries its rows to today. Made
/// from the type it holds ([`Document::of`]), so its steps and the check
/// of its samples are that one type's.
pub struct Document {
    pub table: &'static str,
    pub column: &'static str,
    pub encoding: Encoding,
    /// The steps its JSON takes: `steps[0]` takes version 1 to 2.
    steps: &'static [&'static [Op]],
    /// SQL that takes values out of the JSON before a step drops them
    /// (into a column of their own, or a table): see [`Fill`].
    pub fills: &'static [Fill],
    /// What other columns derive from this one (a hash of its JSON that
    /// keys or names it): run, in the same transaction, when the column is
    /// recorded at today's version, so what was derived from the old JSON
    /// is derived again from the new. `None` when nothing derives from it.
    pub rederive: Option<Rederive>,
    /// One row's JSON read as today's type and written back
    /// (`weft_core::stored::round_trip`).
    read: fn(Value) -> Result<Value, String>,
}

/// A statement that reads the rows still at version `from` (it says
/// `WHERE <version column> = <from>` itself) and writes what the step
/// from `from` takes out of their JSON to where it lives now. It runs in
/// the transaction of the step's first batch, so with the rows it reads;
/// a boot that stops after it runs it again over the rows still at
/// `from`, so it must give the same result run twice (`ON CONFLICT DO
/// NOTHING`, a `SET` from the JSON). A column that goes as a whole has no
/// steps left to run; what it held moves with a
/// [`crate::DataStep`].
pub struct Fill {
    pub from: Version,
    pub sql: &'static str,
}

/// What a [`Document`] re-derives once its rows are at today's version,
/// given the transaction that brought them there.
pub type Rederive = for<'c> fn(&'c mut PgConnection) -> Rederived<'c>;

/// The work a [`Rederive`] hands back.
pub type Rederived<'c> = BoxFuture<'c, anyhow::Result<()>>;

/// How a column holds its JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// As JSON or JSONB, or as JSON text in a TEXT column.
    Json,
    /// As zstd-compressed JSON bytes in a BYTEA column, compressed back
    /// at `level`.
    Zstd { level: i32 },
}

impl Document {
    /// The column `table.column`, holding the documents `kind` reads. A
    /// column is registered the day its rows first need to outlive an
    /// update, with a version column (`INTEGER NOT NULL`, no default) and
    /// its type's `Kind` declared with no steps: the migration adding the
    /// column fills the rows already there with version 1
    /// ([`Self::backfill`]).
    pub const fn of<T: serde::Serialize + serde::de::DeserializeOwned>(
        kind: &'static Kind<T>,
        table: &'static str,
        column: &'static str,
        encoding: Encoding,
        fills: &'static [Fill],
        rederive: Option<Rederive>,
    ) -> Self {
        Document { table, column, encoding, steps: kind.steps, fills, rederive, read: round_trip::<T> }
    }

    /// The version its rows are at once every step has run.
    pub const fn version(&self) -> Version {
        current_version(self.steps)
    }

    /// The steps its JSON takes: `steps[0]` takes version 1 to 2.
    pub fn steps(&self) -> &'static [&'static [Op]] {
        self.steps
    }

    /// One document of today's version read as today's type and written
    /// back, which a committed sample is checked with (so a type whose
    /// shape moved without a step is caught before it ships, by
    /// `weft_runtime`'s `documents` test).
    pub fn read(&self) -> fn(Value) -> Result<Value, String> {
        self.read
    }

    /// `table.column`: where it sits, as boot messages and the samples'
    /// directory name it (the type's own noun, `Kind::name`, is what a
    /// reader's message calls one document).
    pub fn name(&self) -> String {
        format!("{}.{}", self.table, self.column)
    }

    /// The column beside it holding each row's version.
    pub fn version_column(&self) -> String {
        format!("{}_version", self.column)
    }

    /// What its version column holds on the rows already there the day the
    /// column is added: version 1, the shape before documents carried one
    /// (`weft_core::stored::unversioned`). The canonical DDL gives the
    /// column no default, so a write that forgets its version fails on the
    /// NOT NULL instead of being labelled 1; the migration adding it fills
    /// the rows there with this ([`crate::schema_guard::plan_migration`]).
    pub fn backfill(&self) -> crate::schema_guard::Backfill {
        crate::schema_guard::Backfill {
            table: self.table.to_string(),
            column: self.version_column(),
            value: weft_core::stored::unversioned().to_string(),
        }
    }
}

/// Where a carry's transactions come from: the boot gives each batch its
/// own (under the schema's advisory lock, given up and tried again when
/// it meets writes holding the table), a release on a working database
/// runs every batch in the one transaction it already holds the lock in.
enum Work<'a> {
    Boot(&'a PgPool),
    Within(&'a mut PgConnection),
}

/// Bring every column `documents` names to the version this build reads,
/// each batch its own transaction. Run between the schema's expand and
/// contract parts (`crate::schema_guard::apply_schema`), before any role
/// serves.
pub async fn apply_documents(pool: &PgPool, documents: &[&Document]) -> anyhow::Result<()> {
    bring(Work::Boot(pool), documents).await
}

/// [`apply_documents`] inside the caller's transaction, which holds the
/// schema's advisory lock: every batch runs in it, and nothing commits
/// until the caller does (a release on a working database,
/// `crate::schema_guard::settle_release`).
pub(crate) async fn apply_documents_within(conn: &mut PgConnection, documents: &[&Document]) -> anyhow::Result<()> {
    bring(Work::Within(conn), documents).await
}

async fn bring(mut work: Work<'_>, documents: &[&Document]) -> anyhow::Result<()> {
    if documents.is_empty() {
        return Ok(());
    }
    validate(documents)?;
    let recorded = match &mut work {
        Work::Boot(pool) => recorded(&mut *pool.acquire().await?).await?,
        Work::Within(conn) => recorded(conn).await?,
    };
    let at = |d: &Document| recorded.get(&(d.table.to_string(), d.column.to_string())).copied();
    for document in documents {
        if let Some(at) = at(document) {
            refuse_newer(document, at)?;
        }
    }
    for document in documents.iter().filter(|d| at(d) != Some(d.version())) {
        let (table, column) = (document.table, document.column);
        let present = match &mut work {
            Work::Boot(pool) => crate::schema_guard::column_present(&mut *pool.acquire().await?, table, column).await?,
            Work::Within(conn) => crate::schema_guard::column_present(conn, table, column).await?,
        };
        anyhow::ensure!(
            present,
            "document '{}': the table has no such column, and the code still reads it: add it to the \
             table's CREATE TABLE, or, if the column is gone, take it out of the documents and move what it \
             held with a data step on the group of its new home (`weft_task_store::DataStep`, `Until::Dropped(column)`)",
            document.name()
        );
        for from in at(document).unwrap_or(1)..document.version() {
            carry(&mut work, document, from).await?;
        }
        match &mut work {
            Work::Boot(pool) => {
                let pool: &PgPool = pool;
                crate::schema_guard::while_contended(|| async move {
                    let mut tx = crate::schema_guard::locked_tx(pool).await?;
                    record(&mut tx, document).await?;
                    tx.commit().await?;
                    Ok(())
                })
                .await?
            }
            Work::Within(conn) => record(conn, document).await?,
        }
    }
    Ok(())
}

/// Two documents naming one column would carry its rows twice, and the
/// names are spliced into SQL, so they must be plain identifiers. A fill
/// runs before a step, so it names one.
fn validate(documents: &[&Document]) -> anyhow::Result<()> {
    let mut seen = std::collections::HashSet::new();
    for document in documents {
        for name in [document.table, document.column] {
            anyhow::ensure!(
                crate::schema_guard::is_identifier(name),
                "document '{}' names '{name}', which is not a plain SQL identifier ([a-z_][a-z0-9_]*)",
                document.name()
            );
        }
        anyhow::ensure!(seen.insert(document.name()), "document '{}' is listed twice", document.name());
        for fill in document.fills {
            anyhow::ensure!(
                (1..document.version()).contains(&fill.from),
                "document '{}' has a fill from version {}, and its steps start from versions 1 to {}",
                document.name(),
                fill.from,
                document.version() - 1
            );
        }
    }
    Ok(())
}

/// The version each column was last carried to, read without a lock: the
/// one thing a boot with nothing to do pays. The ledger is the schema
/// guard's own table, so it is there once the schema is.
async fn recorded(conn: &mut PgConnection) -> anyhow::Result<HashMap<(String, String), Version>> {
    Ok(sqlx::query_as::<_, (String, String, Version)>(
        "SELECT table_name, column_name, version FROM weft_document_version",
    )
    .fetch_all(conn)
    .await?
    .into_iter()
    .map(|(table, column, version)| ((table, column), version))
    .collect())
}

/// A column carried to a version this build does not know was carried by
/// a newer weft, which may since have written rows only it reads.
fn refuse_newer(document: &Document, at: Version) -> anyhow::Result<()> {
    anyhow::ensure!(
        at <= document.version(),
        "the documents in {} were carried to version {at} by a newer weft, and this weft reads up to \
         version {}: it cannot read what that one writes. Run the newer weft (on a local install, \
         ./setup.sh from the checkout that has it).",
        document.name(),
        document.version()
    );
    Ok(())
}

/// Carry every row of `document` at version `from` to `from + 1`, batch by
/// batch (see the module doc).
async fn carry(work: &mut Work<'_>, document: &Document, from: Version) -> anyhow::Result<()> {
    let mut after: Option<Vec<String>> = None;
    let mut first = true;
    let mut moved = 0usize;
    let mut of: Option<i64> = None;
    loop {
        let batch = match work {
            Work::Boot(pool) => {
                let pool: &PgPool = pool;
                crate::schema_guard::while_contended(|| {
                    let after = after.clone();
                    async move {
                        let mut tx = crate::schema_guard::locked_tx(pool).await?;
                        let batch = next_batch(&mut tx, document, from, first, after).await?;
                        tx.commit().await?;
                        Ok(batch)
                    }
                })
                .await?
            }
            Work::Within(conn) => next_batch(conn, document, from, first, after.clone()).await?,
        };
        first = false;
        let Batch { rows, last } = batch;
        if rows == 0 && moved == 0 {
            return Ok(());
        }
        moved += rows;
        // Counted once per step, so a large column says how far along it
        // is without a count per batch.
        let of = match of {
            Some(of) => of,
            None => {
                let left = match work {
                    Work::Boot(pool) => count(&mut *pool.acquire().await?, document, from).await?,
                    Work::Within(conn) => count(conn, document, from).await?,
                };
                *of.insert(left + moved as i64)
            }
        };
        tracing::info!(
            target: "weft_task_store::documents",
            "{}: carrying rows from version {from} to {}, {moved} of {of} done",
            document.name(),
            from + 1
        );
        match last {
            Some(last) => after = Some(last),
            None => return Ok(()),
        }
    }
}

/// What one batch did: how many rows it carried, and the key of the last
/// one when there may be more (`None` when the step is done).
struct Batch {
    rows: usize,
    last: Option<Vec<String>>,
}

/// One batch of one step, on a connection whose transaction holds the
/// schema's advisory lock. The step's fills run in its first one.
async fn next_batch(
    tx: &mut PgConnection,
    document: &Document,
    from: Version,
    first: bool,
    after: Option<Vec<String>>,
) -> anyhow::Result<Batch> {
    let (table, column) = (document.table, document.column);
    let version_column = document.version_column();
    if first {
        for fill in document.fills.iter().filter(|f| f.from == from) {
            sqlx::raw_sql(fill.sql).execute(&mut *tx).await.map_err(|e| {
                anyhow::anyhow!("{}: the fill before the step from version {from} failed: {e}", document.name())
            })?;
        }
    }
    let step = document.steps[from as usize - 1];
    if step.is_empty() {
        // Nothing in the JSON changes: the rows only say they took it.
        let moved = sqlx::query(sql!(
            "UPDATE {table} SET {version_column} = $2 WHERE {version_column} = $1 AND {column} IS NOT NULL"
        ))
        .bind(from)
        .bind(from + 1)
        .execute(&mut *tx)
        .await?;
        return Ok(Batch { rows: moved.rows_affected() as usize, last: None });
    }

    let keys = primary_key(&mut *tx, table).await?;
    let column_type: String = sqlx::query_scalar(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute \
         WHERE attrelid = $1::regclass AND attname = $2 AND NOT attisdropped",
    )
    .bind(table)
    .bind(column)
    .fetch_one(&mut *tx)
    .await?;
    let key_list = keys.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>().join(", ");
    let key_text = keys.iter().map(|(name, _)| format!("{name}::text")).collect::<Vec<_>>().join(", ");
    // The walk goes on from the last row's key, as text: each part cast
    // back to its column's type, so it uses the key's own order (and
    // index). Rows before it took the step already, or were written at
    // another version since; the version column decides, the key only
    // keeps the walk from reading the table from its start each time.
    let after_clause = if after.is_some() {
        let after_key = keys
            .iter()
            .enumerate()
            .map(|(i, (_, ty))| format!("(($2::text[])[{}])::{ty}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        format!("AND ({key_list}) > ({after_key})")
    } else {
        String::new()
    };
    let read = match document.encoding {
        Encoding::Json => format!("{column}::text"),
        Encoding::Zstd { .. } => column.to_string(),
    };
    let select = sql!(
        "SELECT ARRAY[{key_text}] AS key, {read} AS doc FROM {table} \
         WHERE {version_column} = $1 AND {column} IS NOT NULL {after_clause} ORDER BY {key_list} LIMIT {BATCH} \
         FOR UPDATE"
    );
    let rows = sqlx::query(select).bind(from);
    let rows = match &after {
        Some(after) => rows.bind(after),
        None => rows,
    }
    .fetch_all(&mut *tx)
    .await?;

    // The rows are locked until the batch commits, so a write the code
    // makes meanwhile (at today's version) waits, and is never written
    // over. Every row of the batch is carried in memory, then written in
    // one statement: one round trip per batch, however large the column.
    let mut keys_out: Vec<Vec<String>> = vec![Vec::with_capacity(rows.len()); keys.len()];
    let mut texts: Vec<String> = Vec::new();
    let mut packed: Vec<Vec<u8>> = Vec::new();
    let mut last: Option<Vec<String>> = None;
    for row in &rows {
        let key: Vec<String> = row.try_get("key")?;
        let what = || format!("{table}.{column} row ({})", key.join(", "));
        let mut doc: Value = match document.encoding {
            Encoding::Json => serde_json::from_str(&row.try_get::<String, _>("doc")?),
            Encoding::Zstd { .. } => {
                let bytes: Vec<u8> = row.try_get("doc")?;
                let json = zstd::stream::decode_all(&bytes[..]).map_err(|e| anyhow::anyhow!("{}: does not decompress: {e}", what()))?;
                serde_json::from_slice(&json)
            }
        }
        .map_err(|e| anyhow::anyhow!("{}: is not JSON: {e}", what()))?;
        apply(step, &mut doc).map_err(|e| {
            anyhow::anyhow!("{}: its JSON does not carry from version {from} to {}: {e}", what(), from + 1)
        })?;
        match document.encoding {
            Encoding::Json => texts.push(serde_json::to_string(&doc)?),
            Encoding::Zstd { level } => packed.push(zstd::bulk::compress(&serde_json::to_vec(&doc)?, level)?),
        }
        for (at, part) in key.iter().enumerate() {
            keys_out[at].push(part.clone());
        }
        last = Some(key);
    }
    if !rows.is_empty() {
        let doc_param = keys.len() + 1;
        let doc_array = match document.encoding {
            Encoding::Json => "text[]",
            Encoding::Zstd { .. } => "bytea[]",
        };
        let arrays = (1..=keys.len()).map(|i| format!("${i}::text[]")).collect::<Vec<_>>().join(", ");
        let names = (0..keys.len()).map(|i| format!("k{i}")).collect::<Vec<_>>().join(", ");
        let matched = keys
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| format!("t.{name} = u.k{i}::{ty}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let write = sql!(
            "UPDATE {table} AS t SET {column} = (u.doc)::{column_type}, {version_column} = ${} \
             FROM unnest({arrays}, ${doc_param}::{doc_array}) AS u({names}, doc) WHERE {matched}",
            doc_param + 1
        );
        let mut query = sqlx::query(write);
        for part in &keys_out {
            query = query.bind(part);
        }
        let query = match document.encoding {
            Encoding::Json => query.bind(&texts),
            Encoding::Zstd { .. } => query.bind(&packed),
        };
        query.bind(from + 1).execute(&mut *tx).await?;
    }
    let more = rows.len() as i64 == BATCH;
    Ok(Batch { rows: rows.len(), last: if more { last } else { None } })
}

/// Record `document` at today's version, once every step has run, re-deriving
/// what derives from it in the same transaction.
async fn record(tx: &mut PgConnection, document: &Document) -> anyhow::Result<()> {
    let at: Option<Version> = sqlx::query_scalar(
        "SELECT version FROM weft_document_version WHERE table_name = $1 AND column_name = $2",
    )
    .bind(document.table)
    .bind(document.column)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(at) = at {
        // A newer weft may have booted since this one read the record.
        refuse_newer(document, at)?;
        if at == document.version() {
            return Ok(());
        }
    }
    if let Some(rederive) = document.rederive {
        rederive(&mut *tx)
            .await
            .map_err(|e| e.context(format!("{}: re-derive what other columns hold of it", document.name())))?;
    }
    sqlx::query(
        "INSERT INTO weft_document_version (table_name, column_name, version) VALUES ($1, $2, $3) \
         ON CONFLICT (table_name, column_name) DO UPDATE SET version = EXCLUDED.version",
    )
    .bind(document.table)
    .bind(document.column)
    .bind(document.version())
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// The primary key's columns of `table`, in key order, with their types:
/// what a batch walks the rows in, and finds each one again by.
async fn primary_key(tx: &mut PgConnection, table: &str) -> anyhow::Result<Vec<(String, String)>> {
    let keys: Vec<(String, String)> = sqlx::query_as(
        "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod) \
         FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         WHERE i.indrelid = $1::regclass AND i.indisprimary \
         ORDER BY array_position(i.indkey::int2[], a.attnum)",
    )
    .bind(table)
    .fetch_all(&mut *tx)
    .await?;
    anyhow::ensure!(!keys.is_empty(), "table '{table}' holds documents and has no primary key to walk its rows by");
    Ok(keys)
}

/// How many rows are still at `from`.
async fn count(conn: &mut PgConnection, document: &Document, from: Version) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT count(*) FROM {} WHERE {} = $1 AND {} IS NOT NULL",
        document.table,
        document.version_column(),
        document.column
    ))
    .bind(from)
    .fetch_one(conn)
    .await?)
}
