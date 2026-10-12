//! What a process that drives runs hears on its line to the broker: the
//! cancels of the runs it drives, and the answers handed to them. A worker
//! keeps every run it drives in [`Cancels`] and runs one [`spawn_line_watch`];
//! the node-test server does the same for the one test run it bears, so a
//! live test hears its answers and its cancel the way a worker's run does.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use weft_core::cancellation::CancellationFlag;
use weft_core::ExecutionId;

/// How long an ask that failed waits before it is made again, at first and
/// at most.
pub(crate) const REASK_FIRST: std::time::Duration = std::time::Duration::from_secs(1);
pub(crate) const REASK_LONGEST: std::time::Duration = std::time::Duration::from_secs(30);

/// How many locks the cancel registry is cut into, by the random tail of
/// a run's id: runs registering and leaving at once rarely wait on each
/// other.
const CANCEL_SHARDS: usize = 16;

/// The cancellation flag of every run this process drives: a cancel looks
/// up the run and fires its flag. Registered and removed inline, by the
/// run's own drive ([`Registered`]).
pub(crate) struct Cancels {
    shards: [std::sync::Mutex<HashMap<ExecutionId, Arc<CancellationFlag>>>; CANCEL_SHARDS],
    /// Poked whenever a run comes or goes, so the line watch holds the
    /// line open exactly while there are some.
    changed: tokio::sync::Notify,
    /// Told whenever a run leaves, for a claim waiting on its run's last
    /// drive here to end ([`Self::claim_once_left`]).
    pub(crate) left: tokio::sync::Notify,
    /// Told whenever a run comes, once it is registered: a worker's door
    /// tick (`crate::door::Door::wake`), which must keep the worker's lease
    /// for it however the door slept, and the watch on the line's cut.
    came: Box<dyn Fn() + Send + Sync>,
}

impl Cancels {
    pub(crate) fn new(came: Box<dyn Fn() + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            shards: std::array::from_fn(|_| std::sync::Mutex::new(HashMap::new())),
            changed: tokio::sync::Notify::new(),
            left: tokio::sync::Notify::new(),
            came,
        })
    }

    fn shard(&self, execution_id: ExecutionId) -> &std::sync::Mutex<HashMap<ExecutionId, Arc<CancellationFlag>>> {
        let tail = u64::from_be_bytes(execution_id.as_bytes()[8..16].try_into().expect("eight bytes"));
        &self.shards[(tail % CANCEL_SHARDS as u64) as usize]
    }

    /// `execution_id` is driven here from now on, under a fresh cancel,
    /// until the answer is dropped; `None` when it already is. The one
    /// door a run takes onto this process, however it came (a claim, a
    /// caller, a fire, a node test): checked and taken in one step, so a
    /// run delivered twice at once is driven once, and a cancel announced
    /// from here on finds it.
    pub(crate) fn claim(self: &Arc<Self>, execution_id: ExecutionId) -> Option<Registered> {
        let flag = CancellationFlag::new_arc();
        {
            let mut shard = self.shard(execution_id).lock().expect("cancel registry");
            if shard.contains_key(&execution_id) {
                return None;
            }
            shard.insert(execution_id, flag.clone());
        }
        self.changed.notify_one();
        (self.came)();
        Some(Registered { cancels: self.clone(), execution_id, flag })
    }

    /// [`Self::claim`] a queued run, once a drive of it on this worker has
    /// ended. A run let go of may be queued again in the same breath (an
    /// answer that came while it ran), and its delivery can reach this
    /// worker before the drive that let go of it has left. A second
    /// delivery of a run this worker drives waits out that drive the same
    /// way, and then finds nothing queued.
    pub(crate) async fn claim_once_left(self: &Arc<Self>, execution_id: ExecutionId) -> Registered {
        loop {
            let left = self.left.notified();
            tokio::pin!(left);
            left.as_mut().enable();
            if let Some(registered) = self.claim(execution_id) {
                return registered;
            }
            left.await;
        }
    }

    pub(crate) fn contains(&self, execution_id: ExecutionId) -> bool {
        self.shard(execution_id).lock().expect("cancel registry").contains_key(&execution_id)
    }

    /// Fire `execution_id`'s flag for `cause`. A run not driven here ended
    /// on its own before the cancel landed (or another process drives it):
    /// nothing to do.
    pub(crate) fn cancel(&self, execution_id: ExecutionId, cause: weft_core::exec::CancelCause) {
        let flag = self.shard(execution_id).lock().expect("cancel registry").get(&execution_id).cloned();
        if let Some(flag) = flag {
            tracing::info!(target: "weft_engine::line_watch", %execution_id, %cause, "firing the run's cancel");
            flag.cancel_because(cause);
        }
    }

    /// Every run driven here with its flag, held still: no run comes or
    /// goes until the guards go.
    pub(crate) fn held_still(&self) -> Vec<std::sync::MutexGuard<'_, HashMap<ExecutionId, Arc<CancellationFlag>>>> {
        self.shards.iter().map(|shard| shard.lock().expect("cancel registry")).collect()
    }

    /// How many runs this process drives.
    pub(crate) fn count(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().expect("cancel registry").len()).sum()
    }
}

/// A run's place in [`Cancels`], removed when its drive ends however it
/// ends (an unwind included). Removes only its own flag: a resume of the
/// same run may register its own the moment this one is released.
pub(crate) struct Registered {
    cancels: Arc<Cancels>,
    pub(crate) execution_id: ExecutionId,
    pub(crate) flag: Arc<CancellationFlag>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        // Never panics, even on a poisoned lock: this runs during unwinds.
        if let Ok(mut shard) = self.cancels.shard(self.execution_id).lock() {
            if shard.get(&self.execution_id).is_some_and(|flag| Arc::ptr_eq(flag, &self.flag)) {
                shard.remove(&self.execution_id);
            }
        }
        self.cancels.changed.notify_one();
        self.cancels.left.notify_waiters();
    }
}

/// What the line watch needs: the line itself, where the runs' drives hear
/// of their answers, the broker's side of the runs, and the runs driven
/// here, all of project `project_id`.
#[derive(Clone)]
pub(crate) struct LineWatch {
    project_id: uuid::Uuid,
    line: Arc<dyn crate::context::WorkerLine>,
    told: tokio::sync::broadcast::Sender<crate::context::RunTold>,
    runs: Arc<dyn crate::context::RunClient>,
    cancels: Arc<Cancels>,
}

impl LineWatch {
    /// The watch over `clients`' line for the runs in `cancels`.
    pub(crate) fn new(project_id: uuid::Uuid, clients: &crate::context::EngineClients, cancels: Arc<Cancels>) -> Self {
        Self { project_id, line: clients.line.clone(), told: clients.told.clone(), runs: clients.runs.clone(), cancels }
    }
}

/// The line watch: the broker pushes every cancel of this process's project
/// down its line (`weft_task_store::runs::CANCEL_CHANNEL`), and every answer
/// handed to one of its runs (`weft_task_store::parked_fires::PARKED_FIRE_CHANNEL`).
/// An answer is passed on to the run's drive, which reads its own
/// ([`crate::context::RunTold::Answered`]). For a cancel of a run driven
/// here, and when the line may have missed some (it opened again after a
/// break, or fell behind), the process reads once what waits for all its
/// runs ([`look_again`]): the cancels fire their flags, the answers go to
/// the drives ([`crate::context::RunTold::Rechecked`]). The line is held open
/// while anything is driven, so a cancel is heard however long a run goes
/// without calling the broker. Runs until the line stops, or until the
/// answer is aborted.
pub(crate) fn spawn_line_watch(watch: LineWatch) -> tokio::task::JoinHandle<()> {
    // Subscribed before anything is driven, so no cancel is announced
    // before the watch hears.
    let mut heard = watch.line.subscribe();
    tokio::spawn(async move {
        let mut held_open: Option<Box<dyn Send + Sync>> = None;
        loop {
            // Armed before the count is read, so a change between the read
            // and the wait still wakes it.
            let changed = watch.cancels.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let driving = watch.cancels.count() > 0;
            match (driving, held_open.is_some()) {
                (true, false) => held_open = Some(watch.line.stay_open()),
                (false, true) => held_open = None,
                _ => {}
            }
            let next = tokio::select! {
                _ = &mut changed => continue,
                next = heard.next() => next,
            };
            use weft_task_store::pg_signal::Heard;
            match next {
                Ok(Heard::Signal { channel, payload })
                    if channel == weft_task_store::runs::CANCEL_CHANNEL || channel == weft_task_store::parked_fires::PARKED_FIRE_CHANNEL =>
                {
                    // An event waiting for its trigger names its project
                    // alone: it is for no run.
                    let Some((project, execution_id)) = weft_task_store::runs::parse_run_payload(&payload) else { continue };
                    if project != watch.project_id || !watch.cancels.contains(execution_id) {
                        continue;
                    }
                    if channel == weft_task_store::runs::CANCEL_CHANNEL {
                        look_again(&watch).await;
                    } else {
                        let _ = watch.told.send(crate::context::RunTold::Answered(execution_id));
                    }
                }
                // The line may have missed some (it was lost, or its copy
                // missed news). The broker hearing again after a quiet spell
                // (`Resumed`) says nothing missed, and reads nothing.
                Ok(Heard::Recheck) if driving => look_again(&watch).await,
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(target: "weft_engine::line_watch", error = %format!("{e:#}"), "the line watch stopped hearing the line");
                    return;
                }
            }
        }
    })
}

/// Read once the cancels and the answers waiting for every run this process
/// drives: fire the flags of the cancelled ones, and hand the answers to
/// the drives. A read that fails (the broker could not answer, or its
/// answer was lost) is made again for as long as this process drives
/// anything: reading changes nothing, so what waits is still there to
/// find, and nothing would announce it again.
async fn look_again(watch: &LineWatch) {
    if let Err(e) = look_again_once(watch).await {
        tracing::warn!(target: "weft_engine::line_watch", error = %format!("{e:#}"), "could not read what waits for this worker's runs; asking again");
        let watch = watch.clone();
        tokio::spawn(async move {
            let mut wait = REASK_FIRST;
            loop {
                tokio::time::sleep(wait).await;
                if watch.cancels.count() == 0 {
                    return;
                }
                match look_again_once(&watch).await {
                    Ok(()) => return,
                    Err(e) => tracing::warn!(target: "weft_engine::line_watch", error = %format!("{e:#}"), "could not read what waits for this worker's runs; asking again"),
                }
                wait = (wait * 2).min(REASK_LONGEST);
            }
        });
    }
}

async fn look_again_once(watch: &LineWatch) -> Result<()> {
    let waiting = watch.runs.rechecked().await?;
    for cancel in waiting.cancels {
        watch.cancels.cancel(cancel.execution_id, cancel.cause);
    }
    if !waiting.answers.is_empty() {
        let _ = watch.told.send(crate::context::RunTold::Rechecked(Arc::new(waiting.answers)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A delivery of a run whose drive here is still leaving claims it
    /// once that drive left.
    #[tokio::test]
    async fn a_claim_waits_for_the_runs_last_drive_here_to_leave() {
        let cancels = Cancels::new(Box::new(|| {}));
        let execution_id = weft_core::new_execution_id();
        let leaving = cancels.claim(execution_id).expect("a fresh run");
        let mut claiming = tokio::spawn({
            let cancels = cancels.clone();
            async move { cancels.claim_once_left(execution_id).await }
        });
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), &mut claiming).await.is_err(), "the drive has not left");
        drop(leaving);
        let claimed = claiming.await.unwrap();
        assert!(cancels.contains(execution_id));
        drop(claimed);
        assert!(!cancels.contains(execution_id));
    }
}
