//! Erasing the runs kept long enough.
//!
//! A run is kept for its `keep_for` once it ends (the project's
//! `[runs] keep_for`, or what started it said: a trigger's `keepRunsFor`,
//! `weft run --keep-for`), and its row says until when (`run.keep_until`,
//! set as it ends). This loop erases the runs past it, with everything of
//! theirs, a round of [`ROUND`] at a time oldest first through the
//! `run_expiry` index, pausing between rounds so a backlog never holds the
//! database. A parked or queued run has not ended, so it is never erased
//! however old it is, and one whose ending the dispatcher still has to
//! handle (`crate::run_ends`) waits for that.
//!
//! Once nothing is due, it looks again exactly when the earliest erasable
//! run comes due, and with none it sleeps. A run ending is what can bring
//! an earlier one: the broker rings this loop for the runs its batches end
//! when one may come due before the look booked (`weft_broker::notices`),
//! the endings written one at a time ring it each
//! (`weft_journal::record::append_locked_in`), and a run whose ending was
//! just handled books its own erase time ([`book_at`]).

use std::time::Duration;

use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// How many runs one round erases.
const ROUND: i64 = 5_000;

/// The pause between two full rounds.
const BETWEEN_ROUNDS: Duration = Duration::from_secs(1);

/// A run ending that may come due before the look booked.
pub(crate) static WAKE_ON: &[WakeOn] = &[WakeOn::any(weft_journal::RETENTION_CHANNEL)];

/// How often a machine's process looks anyway: once a minute, at this
/// install's pace (`weft_core::time_scale`).
fn every() -> Duration {
    weft_core::time_scale::scaled(Duration::from_secs(60))
}

pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("retention", "erase runs past their keep", WAKE_ON, every(), move || {
        let state = state.clone();
        async move {
            let now = crate::lease::now_unix();
            let (erased, removed) = state.journal.erase_expired(now, ROUND).await?;
            state.listener.unregister_many(&removed).await;
            if erased > 0 {
                tracing::info!(target: "weft_dispatcher::retention", erased, "erased runs kept long enough");
            }
            if erased as i64 == ROUND {
                return Ok(DrainStep::RetryIn(BETWEEN_ROUNDS));
            }
            // A run is erased once its keep is past, so a second after it.
            Ok(match state.journal.next_expiry().await? {
                Some(at) => DrainStep::RetryIn(Duration::from_secs((at + 1 - now).max(1).unsigned_abs())),
                None => DrainStep::Done,
            })
        }
    })
    .booked_by(&["run_ends"])
}

/// Book retention's look at `keep_until` (unix seconds) unless one is
/// booked by then: for a run that became erasable outside the writes that
/// ring it. Only when the dispatcher scales to zero; a machine's loop looks
/// on its own. A booking that cannot be written fails, for the caller to
/// fail with.
pub async fn book_at(state: &DispatcherState, keep_until: i64) -> anyhow::Result<()> {
    let Some(booking) = &state.booking else { return Ok(()) };
    booking
        .book_sooner("retention", keep_until.saturating_add(1).saturating_mul(1000))
        .await
        .map_err(|e| e.context("book retention's look"))
}
