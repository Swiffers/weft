//! What the broker tells the dispatcher after a batch of records commits:
//! which runs got rows, by project (`weft_journal::RUN_LOG_CHANNEL`, for
//! the live view), which runs ended with somebody to tell
//! (`weft_journal::RUN_ENDED_CHANNEL`), when a run ended that may be
//! due for erasing before retention's next look,
//! `weft_journal::RETENTION_CHANNEL`, and, when a worker bore a run before
//! its lease was ever written, the look at lapsed holds
//! (`weft_task_store::worker_door::LEASE_ALIVE_CHANNEL`). Never inside the write: a `NOTIFY` in
//! a transaction takes a lock that makes commits go one at a time
//! server-wide, so the broker gathers what it has to say over a few
//! milliseconds and says it in a statement of its own.
//!
//! The runs are flagged on their rows, so a wake-up for several costs the
//! dispatcher one look; but nothing else would wake it for them. So a
//! notice that cannot be sent is tried a few times shortly, and then handed
//! to the platform's alarm, which calls the dispatcher's tick for the
//! loops it was for (`weft_task_store::drain::ring_later`) when the
//! dispatcher scales to zero. On a machine the dispatcher's own safety look
//! finds them.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use weft_task_store::db::{Client as _, Pool};
use weft_platform_traits::{Alarm, CoreRole};

/// How long the broker gathers before it speaks.
const GATHER: Duration = Duration::from_millis(10);

/// The pauses between the tries of one notice: three tries in all.
const RETRY_PAUSES: [Duration; 2] = [Duration::from_millis(500), Duration::from_secs(2)];

/// The broker's pending notifications, and the task that sends them.
pub struct Notices {
    pending: Mutex<Pending>,
    wake: tokio::sync::Notify,
}

#[derive(Default)]
struct Pending {
    run_log: BTreeMap<uuid::Uuid, BTreeSet<weft_core::RunId>>,
    run_ended: BTreeSet<weft_core::RunId>,
    /// The earliest erase time of the runs ended since the last notice.
    keep_until: Option<i64>,
    /// Writers that bore a run running with no live lease of their own
    /// (`weft_journal::record::Recorded::unwatched`).
    unwatched: BTreeSet<String>,
}

impl Notices {
    /// The notices, sent on `pool` by a task of their own. `dispatcher`
    /// is the alarm that calls the dispatcher's tick, when it scales to
    /// zero.
    pub fn start(pool: Pool, dispatcher: Option<Arc<dyn Alarm>>) -> Arc<Self> {
        let notices = Arc::new(Self { pending: Mutex::new(Pending::default()), wake: tokio::sync::Notify::new() });
        tokio::spawn(send_loop(notices.clone(), pool, dispatcher));
        notices
    }

    /// After a batch committed: `logged` runs (each with its project) got
    /// rows, `ended` ended with somebody to tell, `kept` are the erase
    /// times of the runs it ended, and `unwatched` is the writer when it
    /// bore a run running with no live lease of its own: the dispatcher's
    /// look at lapsed holds is woken for it, as a lease coming alive wakes
    /// it, so the run is noticed if the worker dies before its first tick.
    pub fn tell(
        &self,
        logged: impl IntoIterator<Item = (uuid::Uuid, weft_core::RunId)>,
        ended: impl IntoIterator<Item = weft_core::RunId>,
        kept: impl IntoIterator<Item = i64>,
        unwatched: Option<&str>,
    ) {
        let mut pending = self.pending.lock().expect("broker notices");
        for (project, run) in logged {
            pending.run_log.entry(project).or_default().insert(run);
        }
        pending.run_ended.extend(ended);
        pending.keep_until = kept.into_iter().chain(pending.keep_until).min();
        pending.unwatched.extend(unwatched.map(str::to_string));
        let any = !pending.run_log.is_empty()
            || !pending.run_ended.is_empty()
            || pending.keep_until.is_some()
            || !pending.unwatched.is_empty();
        drop(pending);
        if any {
            self.wake.notify_one();
        }
    }
}

/// The earliest deadline this broker rang a loop for (retention for an
/// erase time, the file expiry loop for an expiry), while it is still
/// ahead. A ring makes the loop look at once and book its next look at
/// the earliest deadline of all, and that look stays booked until that
/// deadline is dealt with; so while a ring's time is still ahead, a
/// deadline no sooner is covered by the look that deals with the earlier
/// one.
#[derive(Default)]
pub(crate) struct EarliestRung(Option<i64>);

impl EarliestRung {
    /// Whether a deadline `at` needs a ring at `now`: nothing rung for is
    /// ahead any more, or it is sooner.
    pub(crate) fn needs(&self, at: i64, now: i64) -> bool {
        self.0.is_none_or(|rung| rung <= now || at < rung)
    }

    /// A ring for `at` went out.
    pub(crate) fn rang(&mut self, at: i64) {
        self.0 = Some(at);
    }
}

async fn send_loop(notices: Arc<Notices>, pool: Pool, dispatcher: Option<Arc<dyn Alarm>>) {
    let mut rung = EarliestRung::default();
    loop {
        notices.wake.notified().await;
        tokio::time::sleep(GATHER).await;
        let Pending { run_log, run_ended, keep_until, unwatched } = std::mem::take(&mut *notices.pending.lock().expect("broker notices"));
        let mut channels = Vec::new();
        let mut payloads = Vec::new();
        for (project, runs) in &run_log {
            for payload in weft_journal::run_log_payloads(project, runs) {
                channels.push(weft_journal::RUN_LOG_CHANNEL);
                payloads.push(payload);
            }
        }
        for payload in weft_journal::run_ended_payloads(&run_ended) {
            channels.push(weft_journal::RUN_ENDED_CHANNEL);
            payloads.push(payload);
        }
        for writer in &unwatched {
            channels.push(weft_task_store::worker_door::LEASE_ALIVE_CHANNEL);
            payloads.push(writer.clone());
        }
        let now = chrono::Utc::now().timestamp();
        let retention = keep_until.filter(|keep_until| rung.needs(*keep_until, now));
        if retention.is_some() {
            channels.push(weft_journal::RETENTION_CHANNEL);
            payloads.push(String::new());
        }
        if channels.is_empty() {
            continue;
        }
        match send(&pool, &channels, &payloads).await {
            Ok(()) => {
                if let Some(at) = retention {
                    rung.rang(at);
                }
            }
            Err(e) => {
                // The live view's rows are read again by whoever watches;
                // what the dispatcher's loops must act on is rung later.
                let mut loops = Vec::new();
                if !run_ended.is_empty() {
                    loops.push("run_ends".to_string());
                }
                if retention.is_some() {
                    loops.push("retention".to_string());
                }
                if !unwatched.is_empty() {
                    loops.push("lapses".to_string());
                }
                tracing::error!(
                    target: "weft_broker::notices", error = %format!("{e:#}"), ?loops,
                    "could not tell the dispatcher about records just written"
                );
                if let (Some(alarm), false) = (&dispatcher, loops.is_empty()) {
                    if let Err(e) = weft_task_store::drain::ring_later(alarm.as_ref(), CoreRole::Dispatcher, loops).await {
                        tracing::error!(
                            target: "weft_broker::notices", error = %format!("{e:#}"),
                            "could not hand the dispatcher's wake to the alarm either; it looks when a later write wakes it"
                        );
                    }
                }
            }
        }
    }
}

/// Send the notifications, trying a few times shortly.
async fn send(pool: &Pool, channels: &[&str], payloads: &[String]) -> anyhow::Result<()> {
    let mut pauses = RETRY_PAUSES.iter();
    loop {
        let sent = pool.execute("SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)", &[&channels, &payloads]).await;
        match (sent, pauses.next()) {
            (Ok(_), _) => return Ok(()),
            (Err(e), None) => return Err(e.into()),
            (Err(e), Some(pause)) => {
                tracing::warn!(target: "weft_broker::notices", error = %e, "could not tell the dispatcher about records just written; trying again");
                tokio::time::sleep(*pause).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ring is needed while nothing rung for is ahead, or for a sooner
    /// erase time; a later one is covered.
    #[test]
    fn retention_is_rung_only_for_what_no_ring_covers() {
        let mut rung = EarliestRung::default();
        assert!(rung.needs(500, 100), "nothing rung yet");
        rung.rang(500);
        assert!(!rung.needs(800, 100), "erased after the rung time: covered");
        assert!(rung.needs(300, 100), "erased sooner");
        assert!(rung.needs(800, 600), "the rung time has passed");
    }

    /// Three tries on a database that does not answer, then one wake handed
    /// to the alarm naming `run_ends`, and `lapses` for a run born
    /// unwatched.
    #[tokio::test]
    async fn a_notice_that_cannot_be_sent_is_tried_three_times_then_rung_later() {
        let nobody = weft_task_store::db::config("postgres://nobody@127.0.0.1:1/none").expect("a Postgres address");
        let pool = Pool::open(nobody, 1, Duration::from_millis(100), weft_task_store::db::Waits::Expected);
        let alarm = Arc::new(weft_platform_traits::alarm::fake::FakeAlarm::default());
        let notices = Notices::start(pool, Some(alarm.clone()));
        notices.tell([], [weft_core::RunId::from_u128(1)], [], Some("worker-1"));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while alarm.wakes().is_empty() {
            assert!(tokio::time::Instant::now() < deadline, "no wake was handed to the alarm");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let wakes = alarm.wakes();
        assert_eq!(wakes.len(), 1);
        let body: weft_task_store::drain::TickBody = serde_json::from_value(wakes[0].body.clone()).unwrap();
        assert_eq!(body.loops, ["run_ends", "lapses"]);
        assert!(body.after_failed_ring);
        assert_eq!(wakes[0].role, CoreRole::Dispatcher);
    }
}
