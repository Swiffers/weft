//! Running claimed tasks under their lease.
//!
//! The dispatcher claims its tasks in a picker loop
//! ([`dispatcher_picker_loop`]) and runs each through a registry keyed by
//! kind, under a lease guard: the claim is renewed while the work runs,
//! and the task ends `complete`, `failed`, or back to `pending` when the
//! lease could not be renewed.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::FutureExt;
use serde_json::Value;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::tasks::{claim_duration_secs, claim_heartbeat_interval, Task};

#[async_trait]
pub trait TaskExecutor<Ctx: Send + Sync>: Send + Sync {
    async fn execute(&self, ctx: &Ctx, task: &Task) -> Result<Value>;
}

/// The executor of every task kind.
pub struct TaskRegistry<Ctx: Send + Sync> {
    inner: HashMap<crate::kinds::TaskKind, Arc<dyn TaskExecutor<Ctx>>>,
    _phantom: PhantomData<Ctx>,
}

impl<Ctx: Send + Sync> Clone for TaskRegistry<Ctx> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<Ctx: Send + Sync> TaskRegistry<Ctx> {
    /// The registry holding `executor_for(kind)` for every kind. Give it a
    /// function with an exhaustive `match`: a kind added with no executor
    /// is then a compile error, never a task failed at run time.
    pub fn every_kind(executor_for: impl Fn(crate::kinds::TaskKind) -> Arc<dyn TaskExecutor<Ctx>>) -> Self {
        Self {
            inner: crate::kinds::TaskKind::VARIANTS.iter().map(|&kind| (kind, executor_for(kind))).collect(),
            _phantom: PhantomData,
        }
    }

    /// The executor of the kind stored as `kind` on a row, `None` for a tag
    /// this build does not know (a row another weft version wrote).
    pub fn get(&self, kind: &str) -> Option<Arc<dyn TaskExecutor<Ctx>>> {
        self.inner.get(&crate::kinds::TaskKind::parse(kind)?).cloned()
    }
}

/// Most dispatcher tasks one process runs at once. Tasks like
/// `register_signal` (HTTP to the listener) or a build can take a while;
/// running them one after another would head-of-line-block the claims. 8
/// is enough that one slow op doesn't park everything else, low enough
/// that we don't open arbitrarily many DB connections at once.
pub const DISPATCHER_PICKER_CONCURRENCY: usize = 8;

/// A task that became claimable.
pub static DISPATCHER_READY: &[crate::drain::WakeOn] = &[crate::drain::WakeOn::any(crate::tasks::TASK_READY_CHANNEL)];

/// The dispatcher's picker as a drain loop: each pass claims one task and
/// runs it on a task of its own (under the concurrency cap), until none
/// is claimable. Woken by a task becoming claimable. Run dry, it looks
/// again when the earliest claim it may have to rescue lapses (its
/// claimant died, which nothing announces), which the claim that found
/// nothing read too, or sleeps until a task is announced when no claim is
/// out.
pub fn dispatcher_picker_loop<Ctx>(
    store: Arc<crate::traits::PostgresTaskStoreClient>,
    ctx: Ctx,
    registry: TaskRegistry<Ctx>,
    replica: String,
) -> crate::drain::DrainLoop
where
    Ctx: Send + Sync + Clone + 'static,
{
    let slots = Arc::new(tokio::sync::Semaphore::new(DISPATCHER_PICKER_CONCURRENCY));
    crate::drain::DrainLoop::new("dispatcher_picker", "pick up the tasks waiting for it", DISPATCHER_READY, crate::drain::SAFETY_POLL_INTERVAL, move || {
        let (store, ctx, registry, replica, slots) = (store.clone(), ctx.clone(), registry.clone(), replica.clone(), slots.clone());
        async move {
            // At capacity: wait for a running task to finish before claiming
            // another.
            let slot = slots.acquire_owned().await.expect("the picker's semaphore is never closed");
            match store.claim_dispatcher_task(&replica).await? {
                crate::tasks::Claim::Task(task) => {
                    tokio::spawn(async move {
                        let _slot = slot;
                        run_dispatcher_task(store.pool().clone(), ctx, registry, replica, task).await;
                    });
                    Ok(crate::drain::DrainStep::More)
                }
                crate::tasks::Claim::Dry { next_lapse } => Ok(match next_lapse {
                    // Rescued once it is past, so a second after it.
                    Some(lapse) => crate::drain::DrainStep::RetryIn(std::time::Duration::from_secs(
                        (lapse + 1 - crate::tasks::unix_now()).max(1).unsigned_abs(),
                    )),
                    None => crate::drain::DrainStep::Done,
                }),
            }
        }
    })
}

/// Run one claimed dispatcher task through its executor, under its lease.
async fn run_dispatcher_task<Ctx>(
    pool: PgPool,
    ctx: Ctx,
    registry: TaskRegistry<Ctx>,
    replica: String,
    task: Task,
) where
    Ctx: Send + Sync + Clone + 'static,
{
    let Some(executor) = registry.get(&task.kind) else {
        let err = format!("no executor for task kind '{}'", task.kind);
        tracing::error!(
            target: "weft_task_store::executor",
            id = %task.id, kind = %task.kind, error = %err,
            "rejecting unknown task kind"
        );
        if let Err(e) = crate::tasks::fail(&pool, task.id, &replica, err).await {
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task.id, error = %e,
                "fail write failed for unknown-kind reject; row sits claimed until lease expiry"
            );
        }
        return;
    };
    let task_id = task.id;
    let kind = task.kind.clone();
    let lease = LeaseSignal::new();
    let claim = TaskClaim {
        pool: pool.clone(),
        task_id,
        replica: replica.clone(),
        resting: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        lease: lease.clone(),
    };
    let heartbeat = spawn_claim_heartbeat(claim.clone());
    let outcome = run_with_lease_guard(
        CLAIM.scope(claim, executor.execute(&ctx, &task)),
        lease,
        task_id,
        &kind,
    )
    .await;
    drop(heartbeat);
    finalize_task(&pool, task_id, &replica, &kind, outcome).await;
}

tokio::task_local! {
    /// The claim of the task the executor running on this task works for.
    static CLAIM: TaskClaim;
}

/// The claim of a task being run, as its executor reaches it
/// ([`current_claim`]): the work may wait on a run that waits for days (an
/// activation's setup run waiting on a person), and renewing the claim
/// every few seconds meanwhile would keep the database awake for nothing.
/// So the claim can rest on that run ([`TaskClaim::rest_on`]) and be
/// taken back once it ends ([`TaskClaim::resume`]).
#[derive(Clone)]
pub struct TaskClaim {
    pool: PgPool,
    task_id: uuid::Uuid,
    replica: String,
    /// The heartbeat leaves the claim alone while it rests.
    resting: Arc<std::sync::atomic::AtomicBool>,
    lease: LeaseSignal,
}

/// The claim of the task whose executor calls this, when one does.
pub fn current_claim() -> Option<TaskClaim> {
    CLAIM.try_with(Clone::clone).ok()
}

impl TaskClaim {
    /// Rest the claim on `run` (`crate::tasks::rest_claim`): no more
    /// renewals until [`Self::resume`].
    pub async fn rest_on(&self, run: weft_core::RunId) -> Result<()> {
        self.resting.store(true, std::sync::atomic::Ordering::Release);
        if !crate::tasks::rest_claim(&self.pool, self.task_id, &self.replica, run).await? {
            self.lease.lost(LeaseLoss::Stolen);
        }
        Ok(())
    }

    /// Renew the claim again (`crate::tasks::resume_claim`). A claim
    /// handed back while it rested is another claimant's now: the work
    /// stops.
    pub async fn resume(&self) -> Result<()> {
        if !crate::tasks::resume_claim(&self.pool, self.task_id, &self.replica).await? {
            self.lease.lost(LeaseLoss::Stolen);
        }
        self.resting.store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

/// Why the heartbeat task told the executor to stop. Typed so the
/// finalizer acts on the CAUSE, not on a synthesized error string:
/// the two cases need opposite reactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseLoss {
    /// The heartbeat came back "row no longer claimed by us": a
    /// sibling process already re-claimed it (our lease lapsed and was
    /// taken). The thief owns the task now; we touch nothing.
    Stolen,
    /// The heartbeat could not REACH the store past the lease window.
    /// The work did not fail, WE lost the ability to prove liveness,
    /// so the task is surrendered (`tasks::surrender`): back to
    /// `pending` for any process (including us) to claim again.
    Unrenewable,
}

/// Cancellation token + the reason it fired. The heartbeat task sets
/// the reason before cancelling, so the guard always reads a cause.
#[derive(Clone)]
struct LeaseSignal {
    reason: Arc<std::sync::OnceLock<LeaseLoss>>,
    token: CancellationToken,
}

impl LeaseSignal {
    fn new() -> Self {
        Self {
            reason: Arc::new(std::sync::OnceLock::new()),
            token: CancellationToken::new(),
        }
    }

    fn lost(&self, why: LeaseLoss) {
        let _ = self.reason.set(why);
        self.token.cancel();
    }

    async fn cancelled(&self) {
        self.token.cancelled().await
    }

    fn reason(&self) -> LeaseLoss {
        *self
            .reason
            .get()
            .expect("lease token cancelled without a recorded reason")
    }
}

/// The guard's verdict on one executor run.
enum ExecOutcome {
    /// The executor future ran to completion: a value, an error, or a
    /// panic caught by `catch_unwind`.
    Finished(std::thread::Result<Result<Value>>),
    /// The heartbeat signalled lease loss first; the executor future
    /// was dropped mid-flight.
    LeaseLost(LeaseLoss),
}

/// Run an executor future under the lease guard. If the heartbeat
/// task signals lease loss before the executor finishes, the future
/// is dropped and the typed cause is handed to `finalize_task`, which
/// requeues (surrender) or stands down (stolen). A later claim of the
/// row redoes the work; every task kind is re-runnable by contract
/// (see the idempotency note in `tasks`).
async fn run_with_lease_guard<F>(
    fut: F,
    lease: LeaseSignal,
    task_id: uuid::Uuid,
    kind: &str,
) -> ExecOutcome
where
    F: std::future::Future<Output = Result<Value>>,
{
    tokio::select! {
        out = AssertUnwindSafe(fut).catch_unwind() => ExecOutcome::Finished(out),
        _ = lease.cancelled() => {
            let why = lease.reason();
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind, cause = ?why,
                "lease lost mid-run; abandoning the executor future"
            );
            ExecOutcome::LeaseLost(why)
        }
    }
}

/// Persist the verdict for one task.
///
///   - `Finished(Ok(Ok(value)))`: → `tasks::complete`.
///   - `Finished(Ok(Err(e)))`: → `tasks::fail` with the error message.
///   - `Finished(Err(panic))`: → `tasks::fail` with a "panic: ..."
///     prefix so clients can flag it visibly. Without this
///     layering, a panicking executor would ride the spawned task's
///     JoinError up and get discarded by `try_join_next`, and the row
///     would sit `claimed` until the lease expired.
///   - `LeaseLost(Unrenewable)`: → `tasks::surrender`
///     (guarded on our claim, tried until it lands), putting the row back
///     to `pending` for the next claim. A transient store outage must
///     never terminalize work that did not fail.
///   - `LeaseLost(Stolen)`: → nothing. The re-claimer owns the row;
///     any write from us would race its run.
async fn finalize_task(
    pool: &PgPool,
    task_id: uuid::Uuid,
    replica: &str,
    kind: &str,
    outcome: ExecOutcome,
) {
    let outcome = match outcome {
        ExecOutcome::Finished(finished) => finished,
        ExecOutcome::LeaseLost(LeaseLoss::Stolen) => {
            tracing::warn!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind,
                "lease stolen by another claimant; it owns the task, standing down"
            );
            return;
        }
        ExecOutcome::LeaseLost(LeaseLoss::Unrenewable) => {
            // Until the surrender lands, this process still holds the task,
            // so nothing else may pick it up: it is tried again for as long
            // as the store does not answer.
            loop {
                match crate::tasks::surrender(pool, task_id, replica).await {
                    Ok(true) => {
                        tracing::warn!(
                            target: "weft_task_store::executor",
                            id = %task_id, kind = %kind,
                            "claim surrendered; the task goes back for the next claim"
                        );
                        break;
                    }
                    Ok(false) => {
                        tracing::warn!(
                            target: "weft_task_store::executor",
                            id = %task_id, kind = %kind,
                            "surrender found the row no longer ours (already re-claimed); the claimer owns it"
                        );
                        break;
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "weft_task_store::executor",
                            id = %task_id, kind = %kind, error = %e,
                            "surrender failed; trying again"
                        );
                        tokio::time::sleep(crate::tasks::claim_heartbeat_interval()).await;
                    }
                }
            }
            return;
        }
    };
    match outcome {
        Ok(Ok(result)) => {
            if let Err(e) = crate::tasks::complete(pool, task_id, replica, result).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e,
                    "complete write failed; row may have been re-claimed"
                );
            }
        }
        Ok(Err(e)) => {
            let msg = format!("{e:#}");
            if let Err(e2) = crate::tasks::fail(pool, task_id, replica, msg).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e2,
                    "fail write failed"
                );
            }
        }
        Err(panic) => {
            let panic_msg = panic_message(&panic);
            tracing::error!(
                target: "weft_task_store::executor",
                id = %task_id, kind = %kind, panic = %panic_msg,
                "task panicked; writing tasks::fail"
            );
            let msg = format!("panic: {panic_msg}");
            if let Err(e) = crate::tasks::fail(pool, task_id, replica, msg).await {
                tracing::warn!(
                    target: "weft_task_store::executor",
                    id = %task_id, kind = %kind, error = %e,
                    "fail write after panic also failed"
                );
            }
        }
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Renew the claim every [`claim_heartbeat_interval`] until the
/// owning future finishes (which aborts this handle).
///
/// Two exit conditions fire the lease signal, each with its cause:
///   - heartbeat returns `Ok(false)`: the row is no longer claimed by
///     us (sibling process took the lease): `LeaseLoss::Stolen`, the
///     finalizer stands down.
///   - heartbeat errors past `claim_duration_secs() / interval` ticks:
///     the lease has lapsed at this point regardless of what the DB
///     says; a sibling process can re-claim, so we must stop or risk
///     parallel run of the same row. `LeaseLoss::Unrenewable`,
///     the finalizer requeues the row.
/// A stalled executor is NOT an exit: the heartbeat keeps renewing,
/// the executor keeps running, no leak. A claim resting on a run
/// ([`TaskClaim::rest_on`]) is not renewed, and touches nothing.
fn spawn_claim_heartbeat(claim: TaskClaim) -> Heartbeat {
    let TaskClaim { pool, task_id, replica, resting, lease } = claim;
    let interval = claim_heartbeat_interval();
    let max_consecutive_errors =
        (claim_duration_secs() as f64 / interval.as_secs_f64()) as u32 + 1;
    Heartbeat(tokio::spawn(async move {
        let mut consecutive_errors: u32 = 0;
        loop {
            tokio::time::sleep(interval).await;
            if resting.load(std::sync::atomic::Ordering::Acquire) {
                continue;
            }
            match crate::tasks::heartbeat(&pool, task_id, &replica).await {
                Ok(true) => {
                    consecutive_errors = 0;
                }
                Ok(false) => {
                    tracing::warn!(
                        target: "weft_task_store::executor",
                        id = %task_id,
                        "heartbeat: lease no longer ours; signalling executor"
                    );
                    lease.lost(LeaseLoss::Stolen);
                    break;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors >= max_consecutive_errors {
                        tracing::error!(
                            target: "weft_task_store::executor",
                            id = %task_id, error = %e, consecutive_errors,
                            "heartbeat unreachable past lease window; signalling executor"
                        );
                        lease.lost(LeaseLoss::Unrenewable);
                        break;
                    }
                    tracing::warn!(
                        target: "weft_task_store::executor",
                        id = %task_id, error = %e, consecutive_errors,
                        "heartbeat error; will retry"
                    );
                }
            }
        }
    }))
}

/// A claim's heartbeat, stopped when it is dropped: the work it keeps
/// claimed may be dropped mid-way (its caller went away, its task was
/// aborted), and a heartbeat left running would hold the claim forever
/// with nothing doing the work.
struct Heartbeat(tokio::task::JoinHandle<()>);

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}
