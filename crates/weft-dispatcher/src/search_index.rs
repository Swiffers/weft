//! Finding a run by what went through it.
//!
//! When a person traces an incident ("this customer says their order
//! failed at noon"), what they hold is a word that went through the run:
//! an email, an order id, a phrase from an error. Each finished recorded
//! run gets one search entry: the words of every value its record holds
//! (the trigger's input, what every node sent on, every error and log
//! line, `weft_journal::search::SearchText`) and the nodes it started. The
//! runs listing matches it (`--search`, `--through`), beside its other
//! filters.
//!
//! Built when somebody searches, never on a run's path and never on a
//! timer: a run's ending queues it (`run_search_queue`, in the write that
//! ends it), and a listing that reads the entries first builds the ones
//! queued ([`index_queued`]). So an install where nobody searches spends
//! nothing on it, and the first search after many runs ended takes longer.
//!
//! Words, not substrings: `ada@example.com` finds the runs that carried that
//! address, and several words find the runs carrying all of them (a
//! quoted phrase, the runs carrying it as written).

use sqlx::PgPool;
use weft_core::RunId;
use weft_journal::RunEvent;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "run_search",
    tables: &["run_search_queue", "run_search"],
    ddl: &[
        // The ended recorded runs whose entry is not built yet.
        r#"CREATE TABLE IF NOT EXISTS run_search_queue (
            run_id UUID PRIMARY KEY
        )"#,
        // One row per indexed run: its words, in the `simple` configuration
        // (every word as written, lowercased, no language's stemming or
        // stop words: an id or a name is found as it is), and the nodes it
        // started, spelled the way the record names them.
        r#"CREATE TABLE IF NOT EXISTS run_search (
            run_id UUID PRIMARY KEY,
            words TSVECTOR NOT NULL,
            nodes TEXT[] NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS run_search_words ON run_search USING GIN (words)"#,
        r#"CREATE INDEX IF NOT EXISTS run_search_nodes ON run_search USING GIN (nodes)"#,
    ],
    seed: &[],
};

/// How many queued runs one round takes.
const ROUND: i64 = 1_000;

/// What one run's entry holds.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Entry {
    pub words: String,
    pub nodes: Vec<String>,
}

impl Entry {
    /// The entry of a run whose record is `events`.
    pub fn of(events: &[RunEvent]) -> Self {
        let mut words = weft_journal::search::SearchText::default();
        let mut nodes = std::collections::BTreeSet::new();
        for event in events {
            words.push(event);
            if let RunEvent::NodeStarted { node_id, .. } = event {
                nodes.insert(node_id.clone());
            }
        }
        Self { words: words.into_text(), nodes: nodes.into_iter().collect() }
    }
}

/// Build the entry of every run queued, a round of [`ROUND`] at a time,
/// and answer how many. Each round takes its runs with `FOR UPDATE SKIP
/// LOCKED`, so two searches at once never index one run twice.
pub async fn index_queued(pool: &PgPool) -> anyhow::Result<usize> {
    let mut indexed = 0;
    loop {
        let round = round(pool).await?;
        indexed += round;
        if (round as i64) < ROUND {
            return Ok(indexed);
        }
    }
}

/// One round: up to [`ROUND`] queued runs indexed; answers how many.
async fn round(pool: &PgPool) -> anyhow::Result<usize> {
    let mut tx = pool.begin().await?;
    let ids: Vec<RunId> = sqlx::query_scalar(
        "SELECT run_id FROM run_search_queue ORDER BY run_id LIMIT $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(ROUND)
    .fetch_all(&mut *tx)
    .await?;
    for run_id in &ids {
        let record = weft_journal::record::read_record(&mut tx, *run_id, None).await?;
        let entry = match record.events(*run_id) {
            Ok(events) => Entry::of(&events),
            Err(e) => {
                tracing::warn!(target: "weft_dispatcher::search", %run_id, error = %e, "a run's record does not read; it is not searchable");
                Entry::default()
            }
        };
        sqlx::query(
            "INSERT INTO run_search (run_id, words, nodes) VALUES ($1, to_tsvector('simple', $2), $3) \
             ON CONFLICT (run_id) DO NOTHING",
        )
        .bind(run_id)
        .bind(&entry.words)
        .bind(&entry.nodes)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM run_search_queue WHERE run_id = ANY($1)").bind(&ids).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(ids.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An entry holds the run's values and the nodes it started, each once.
    #[test]
    fn an_entry_is_the_runs_words_and_started_nodes() {
        let run_id = weft_core::RunId::from_u128(7);
        let events = vec![
            RunEvent::NodeStarted { run_id, node_id: "lookup".into(), frames: vec![], at_unix: 0 },
            RunEvent::LogLine {
                run_id,
                node_id: "lookup".into(),
                frames: vec![],
                level: "info".into(),
                message: "ada@example.com".into(),
                at_unix_ms: None,
                seq: None,
                at_unix: 0,
            },
            RunEvent::NodeStarted { run_id, node_id: "lookup".into(), frames: vec![], at_unix: 0 },
            RunEvent::NodeStarted { run_id, node_id: "cards.post".into(), frames: vec![], at_unix: 0 },
        ];
        let entry = Entry::of(&events);
        assert!(entry.words.contains("ada@example.com"), "{}", entry.words);
        assert_eq!(entry.nodes, ["cards.post", "lookup"]);
    }
}
