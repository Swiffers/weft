//! What the dispatcher keeps of the workers' doors: which worker processes
//! are alive (the tables are `weft_task_store::worker_door`'s).
//!
//! A running run's owner is a worker process, and the worker's lease is
//! what says it is still being driven. A run whose owner's lease ran out is
//! lost, and [`sweep_lost_runs`] lets go of it.

use anyhow::{Context, Result};

/// How far past its lease a worker's runs are left alone before they are
/// read as lost: a worker that missed a few ticks under load is not gone.
pub(crate) fn lost_margin_secs() -> i64 {
    weft_core::time_scale::scaled_secs(weft_task_store::worker_door::WORKER_LEASE_SECS)
}

/// How many lost runs one read of a sweep finds; a full read is followed
/// by the next, past the last run it found.
const LOST_BATCH: i64 = 500;

/// THE running runs whose owner went away, as a SQL condition over a
/// `run` row aliased `r`: its owner stopped holding it
/// (`weft_task_store::run_held_until_sql!`) before `$lapsed_before`, which
/// is [`lost_margin_secs`] ago. What [`sweep_lost_runs`] lets go of, and
/// what the look it books waits for (`crate::lapses::earliest_lapse`).
macro_rules! lost_run {
    ($lapsed_before:literal) => {
        concat!("r.state = 'running' AND ", weft_task_store::run_held_until_sql!("r"), " < ", $lapsed_before)
    };
}
pub(crate) use lost_run;

/// Let go of the runs whose owner went away ([`lost_run!`]): a run asked
/// to stop ends, else a durable run is queued again, a fast run ends
/// (`crate::store::RunStore::let_go_of_lost`). Each run is taken under
/// its own row lock, so a batch its old owner sends meanwhile and this
/// sweep never write the run at once, and the epoch this raises refuses
/// whichever of that owner's batches comes after. A worker whose runs only
/// wait vouched for itself until their waits give up
/// (`weft_task_store::worker_door::lease_until`), so its lease, and its
/// runs, hold until then. Every lost run is looked at, in reads of
/// [`LOST_BATCH`]; answers the ones that could not be let go of, each
/// with why.
pub async fn sweep_lost_runs(state: &crate::state::DispatcherState, now: i64) -> Result<Vec<crate::lapses::RepairFailed>> {
    let lapsed_before = now - lost_margin_secs();
    let mut failed = Vec::new();
    let mut after: Option<weft_core::RunId> = None;
    loop {
        let lost: Vec<(weft_core::RunId, uuid::Uuid, String, bool)> = sqlx::query_as(concat!(
            "SELECT r.run_id, r.project_id, r.keeping, r.cancel_requested IS NOT NULL FROM run r WHERE ",
            lost_run!("$1"),
            " AND ($3::UUID IS NULL OR r.run_id > $3) ORDER BY r.run_id LIMIT $2"
        ))
        .bind(lapsed_before)
        .bind(LOST_BATCH)
        .bind(after)
        .fetch_all(&state.pg_pool)
        .await
        .context("find runs whose worker went away")?;
        let full = lost.len() as i64 == LOST_BATCH;
        after = lost.last().map(|(run_id, _, _, _)| *run_id);
        let_go_of(state, lost, lapsed_before, &mut failed).await;
        if !full {
            return Ok(failed);
        }
    }
}

/// Let go of each of `lost`, adding the ones that could not be to
/// `failed`.
async fn let_go_of(
    state: &crate::state::DispatcherState,
    lost: Vec<(weft_core::RunId, uuid::Uuid, String, bool)>,
    lapsed_before: i64,
    failed: &mut Vec<crate::lapses::RepairFailed>,
) {
    for (run_id, project_id, keeping, cancelled) in lost {
        // Only a run that ends here (a fast one, or one asked to stop)
        // folds its per-node cancels.
        let program = match keeping == weft_core::run_settings::Keeping::Durable.as_str() && !cancelled {
            true => Ok(None),
            false => crate::api::run::program_for_cancel(state, run_id).await,
        };
        let let_go = match program {
            Ok(program) => state.runs.let_go_of_lost(run_id, lapsed_before, program.as_deref()).await,
            Err(e) => Err(e),
        };
        match let_go {
            Ok(crate::store::Lost::NotLost) => {}
            Ok(lost) => tracing::warn!(target: "weft_dispatcher::worker_door", %run_id, ?lost, "a run whose worker went away was let go of"),
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::worker_door",
                    %run_id, error = %format!("{e:#}"),
                    "could not let go of a run whose worker went away"
                );
                failed.push(crate::lapses::RepairFailed {
                    project_id,
                    error: format!("run {run_id}, whose worker went away, could not be let go of: {e:#}"),
                });
            }
        }
    }
}
