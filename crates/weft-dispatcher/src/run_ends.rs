//! What a run's ending leaves for the dispatcher to do.
//!
//! Most endings need nothing: the batch that writes a run's ending also
//! queues its search entry and its storage sweep. Two kinds of run leave
//! work behind, and keep a flag on their row until it is done, which the
//! `run_end_unhandled` index finds:
//!
//! - `watch_end`: somebody waits on the run's ending. A trigger setup's
//!   ending is baked here (`crate::journal::TriggerBake`), so an activation
//!   whose request died still gets its bake;
//! - `holds_signals`: the run ended holding resume signals. Their rows go,
//!   and the listener is told to let go of them.
//!
//! The write that ends such a run announces it on
//! `weft_journal::RUN_ENDED_CHANNEL`, which wakes this loop at once. A run
//! whose work fails stays flagged, and the loop fails with it: it is tried
//! again shortly a few times, then written down for `weft status` until a
//! later ending wakes the loop (`weft_task_store::drain`).
//!
//! A watched run's ending may also leave a hold with nobody to take it
//! back: an activation whose driver died while its setup run went, a task
//! claim that rested on that run. The `lapses` loop judges those, so it is
//! booked to look.

use weft_core::ExecutionId;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn, SAFETY_POLL_INTERVAL};

use crate::state::DispatcherState;

pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(weft_journal::RUN_ENDED_CHANNEL)];

/// How many flagged runs one read of them brings.
const PAGE: i64 = 100;

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("run_ends", "finish what runs' endings leave to do", WAKE_ON, SAFETY_POLL_INTERVAL, move || {
        let state = state.clone();
        async move { handle_endings(&state).await }
    })
}

/// Handle every ended run still flagged, one at a time, read in pages by
/// id: each in a transaction of its own that holds that run's row alone
/// (so two dispatchers never take the same run, and a crash midway leaves
/// it flagged for the next look), its flags cleared once its work is done.
/// A run whose work fails stays flagged and is said in the log, the runs
/// after it are handled all the same, and the drain fails naming how many.
async fn handle_endings(state: &DispatcherState) -> anyhow::Result<DrainStep> {
    let mut after: Option<ExecutionId> = None;
    let mut failed = 0usize;
    let mut watched = false;
    // The earliest erase time of the runs unflagged.
    let mut erased_at: Option<i64> = None;
    loop {
        let page: Vec<ExecutionId> = sqlx::query_scalar(
            "SELECT execution_id FROM run WHERE state = 'ended' AND (watch_end OR holds_signals) \
               AND ($1::uuid IS NULL OR execution_id > $1) ORDER BY execution_id LIMIT $2",
        )
        .bind(after)
        .bind(PAGE)
        .fetch_all(&state.pg_pool)
        .await?;
        for execution_id in &page {
            let mut tx = state.pg_pool.begin().await?;
            let Some(ending) = take_flagged(&mut tx, *execution_id).await? else { continue };
            match handle_ending(state, &ending).await {
                Ok(()) => {
                    let kept = clear_flags(&mut tx, &ending).await?;
                    tx.commit().await?;
                    erased_at = kept.into_iter().chain(erased_at).min();
                    watched |= ending.watch_end;
                }
                Err(e) => {
                    failed += 1;
                    tracing::error!(
                        target: "weft_dispatcher::run_ends",
                        execution_id = %ending.execution_id,
                        error = %format!("{e:#}"),
                        "what a run's ending left could not be done"
                    );
                }
            }
        }
        if (page.len() as i64) < PAGE {
            break;
        }
        after = page.last().copied();
    }
    if let Some(at) = erased_at {
        crate::retention::book_at(state, at).await?;
    }
    if watched {
        crate::lapses::book_after(state, 0).await?;
    }
    anyhow::ensure!(failed == 0, "what {failed} runs' endings left could not be done");
    Ok(DrainStep::Done)
}

/// What `ending` left: its trigger setup's bake, its resume signals taken
/// down and let go of by the listener.
async fn handle_ending(state: &DispatcherState, ending: &Flagged) -> anyhow::Result<()> {
    if ending.watch_end {
        finish_trigger_setup(state, ending.execution_id, ending.project_id).await?;
    }
    if ending.holds_signals {
        let removed = state.journal.signal_remove_for_execution_id(ending.execution_id).await?;
        state.listener.unregister_many(&removed).await;
    }
    Ok(())
}

/// An ended run whose ending still has work left (see the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::FromRow)]
pub struct Flagged {
    pub execution_id: ExecutionId,
    pub project_id: uuid::Uuid,
    pub watch_end: bool,
    pub holds_signals: bool,
}

/// The ended run `execution_id`, still flagged, locked for the caller's
/// transaction (`FOR UPDATE SKIP LOCKED`: one another dispatcher holds is
/// left to it). Read off the row, so an announcement lost on the way loses
/// nothing.
pub async fn take_flagged(conn: &mut sqlx::PgConnection, execution_id: ExecutionId) -> anyhow::Result<Option<Flagged>> {
    Ok(sqlx::query_as(
        "SELECT execution_id, project_id, watch_end, holds_signals FROM run \
         WHERE execution_id = $1 AND state = 'ended' AND (watch_end OR holds_signals) FOR UPDATE SKIP LOCKED",
    )
    .bind(execution_id)
    .fetch_optional(conn)
    .await?)
}

/// The work `ending` left is done: its flags go. Answers until when the
/// run is kept (`None`: for ever): retention left it alone while it was
/// flagged, so no look it booked counts it.
pub async fn clear_flags(conn: &mut sqlx::PgConnection, ending: &Flagged) -> anyhow::Result<Option<i64>> {
    Ok(sqlx::query_scalar("UPDATE run SET watch_end = FALSE, holds_signals = FALSE WHERE execution_id = $1 RETURNING keep_until")
        .bind(ending.execution_id)
        .fetch_one(conn)
        .await?)
}

/// Bake the trigger setup `execution_id` from its record, when it is one
/// still waiting for its bake. A capture that cannot be read leaves the
/// last good bake in place and says so on the project's events.
async fn finish_trigger_setup(state: &DispatcherState, execution_id: ExecutionId, project_id: uuid::Uuid) -> anyhow::Result<()> {
    if !state.journal.is_trigger_setup_pending(execution_id).await? {
        return Ok(());
    }
    let (rows, bad) = state.journal.events_log_lossy(execution_id).await?;
    let capture = match bad.into_iter().next() {
        Some(reason) => Err(anyhow::Error::msg(reason)),
        None => {
            let events = rows.into_iter().map(|row| row.event).collect::<Vec<_>>();
            match state.run_program_identity(&events).await {
                Ok(program) => crate::journal::TriggerBake::from_events(&events, &program),
                Err(e) => Err(e),
            }
        }
    };
    match capture {
        Ok(bake) => state.journal.finish_trigger_setup(execution_id, bake.as_ref()).await,
        Err(error) => {
            state.journal.finish_trigger_setup(execution_id, None).await?;
            crate::live_view::publish_unreadable(
                state,
                execution_id,
                project_id,
                weft_core::primitive::CorruptionSite::UndecodableRow,
                format!("cannot save trigger bake: {error:#}; the last good bake is preserved. Run `weft bake` again to refresh it."),
            )
            .await;
            Ok(())
        }
    }
}
