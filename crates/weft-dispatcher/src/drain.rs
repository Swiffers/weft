//! Every "let the running work finish, then go on" goes through here.
//!
//! A take-down that waits (`--running-policy wait`), a resync, a new image
//! going live, an infra stop: each one first stops new work coming in (a
//! trigger that is not Live refuses its callers and parks its events),
//! then waits for the work already going in what it touches, up to a
//! deadline, and cancels what is left there.
//!
//! "Work going" is two things added up. What the project's live workers
//! say they drive right now, per trigger, at their door tick
//! (`worker_lease.in_flight`, from the door's own count of the runs it
//! admitted): a run a worker bore at its door is counted from the moment
//! the door takes it, before anything of it is on record. And the runs on
//! record that are queued for a worker or being driven by one
//! (`weft_task_store::in_flight_sql!`). A run is often counted twice, once
//! by its worker and once by its row, which is harmless: the count is only
//! ever compared to zero. A run parked on a wait is not going: it survives
//! a take-down and resumes when its trigger is back.

use std::time::Duration;

use tokio::time::Instant;

use weft_core::activation::ActivationKey;
use weft_core::exec::CancelCause;
use weft_core::instance::Copies;
use weft_core::running_policy::RunningPolicy;
use weft_core::ExecutionId;
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// What a drain waits on: the work of one project that reaches something.
#[derive(Debug, Clone, Copy)]
pub struct DrainScope<'a> {
    pub project_id: uuid::Uuid,
    pub reaching: Reaching<'a>,
    /// The run that asked for the drain (a program taking its own infra
    /// down): it is waited for like any other, but never cancelled by the
    /// drain; its own `StopSelf` decides its fate.
    pub except: Option<ExecutionId>,
}

/// What the work a drain waits on reaches.
#[derive(Debug, Clone, Copy)]
pub enum Reaching<'a> {
    /// The runs these activations' triggers fired (each for its owner).
    Triggers(&'a [ActivationKey]),
    /// The runs that may use these infra copies: every run of the project
    /// for the shared copies, an instance's runs for its own (which run
    /// uses which copy is not recorded).
    Copies(&'a Copies),
    /// The runs on images of the program other than this one.
    OtherImages(&'a str),
}

/// How a drain ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drained {
    /// Nothing was left going.
    Empty,
    /// The policy was `cancel`, or the deadline came with this much still
    /// going: what was on record was cancelled.
    Cancelled { left: i64 },
}

/// How often a drain still waiting says so in the log.
const BREADCRUMB_EVERY: Duration = Duration::from_secs(30);

/// What makes a drain count again: a run ended (every run it waits on,
/// and every run in its scope waiting to resume, is marked watched,
/// [`mark_watched`]), or an activation changing status (one landing
/// elsewhere, or rolled back).
pub(crate) static WAKE_ON: &[WakeOn] =
    &[WakeOn::any(weft_journal::RUN_ENDED_CHANNEL), WakeOn::any(crate::holders::HELD_SIGNALS_CHANNEL)];

/// Wait for the work `scope` reaches, as `policy` says: `cancel` cancels
/// what is going now; `wait` waits until nothing is going, or until
/// `deadline` (none: for as long as it takes, logged every half minute
/// with no query), where it cancels what is left. It counts again when a
/// run it waits on ends ([`WAKE_ON`]), when the lease of a worker driving
/// what it waits on could lapse ([`workers_look`]), and at the deadline;
/// never on a timer of its own. A drain run under a command's `claim`
/// renews it at each count to last past the next one, and counts again
/// within a claim lease, so the claim never runs further ahead than one
/// lease; it fails once the claim is no longer this process's.
pub async fn drain(
    state: &DispatcherState,
    scope: &DrainScope<'_>,
    deadline: Option<Instant>,
    policy: RunningPolicy,
    claim: Option<&crate::lifecycle_claimer::CommandClaim<'_>>,
) -> anyhow::Result<Drained> {
    if policy == RunningPolicy::Cancel {
        let left = going(&state.pg_pool, scope).await?;
        cancel_left(state, scope, &CancelCause::User).await?;
        return Ok(if left == 0 { Drained::Empty } else { Drained::Cancelled { left } });
    }
    let started = Instant::now();
    let mut breadcrumb = started + BREADCRUMB_EVERY;
    // Subscribed before the first count, so an ending between the count
    // and the wait is heard.
    let mut heard = state.signals.subscribe();
    loop {
        let left = going(&state.pg_pool, scope).await?;
        if left == 0 {
            return Ok(Drained::Empty);
        }
        let now = Instant::now();
        if deadline.is_some_and(|deadline| now >= deadline) {
            tracing::warn!(
                target: "weft_dispatcher::drain",
                project_id = %scope.project_id, ?scope.reaching, left,
                "the wait for running work reached its cap; cancelling what is left"
            );
            cancel_left(state, scope, &scope.reaching.cap_cause()).await?;
            return Ok(Drained::Cancelled { left });
        }
        mark_watched(&state.pg_pool, scope).await?;
        let look = workers_look(&state.pg_pool, scope)
            .await?
            .map(|at| now + Duration::from_secs((at + 1 - crate::lease::now_unix()).max(1).unsigned_abs()));
        let mut count_at = [deadline, look].into_iter().flatten().min();
        if let Some(claim) = claim {
            // The claim is kept one claim lease at most: a dispatcher that
            // dies mid-drain leaves a claim another takes over within a
            // lease, whatever the drain was waiting for.
            use weft_broker_client::lifecycle_command::{CLAIM_LEASE_TTL, CLAIM_RENEW_INTERVAL};
            let renew_by = now + CLAIM_LEASE_TTL - CLAIM_RENEW_INTERVAL;
            let next = count_at.map_or(renew_by, |count_at| count_at.min(renew_by));
            count_at = Some(next);
            let until = crate::lease::now_unix()
                + i64::try_from(next.saturating_duration_since(now).as_secs() + CLAIM_RENEW_INTERVAL.as_secs()).unwrap_or(i64::MAX / 2);
            anyhow::ensure!(
                claim.keep_until(&state.pg_pool, until).await?,
                "the claim on command {} was taken over by another replica, which answers for it now",
                claim.id
            );
        }
        loop {
            let wake = count_at.map_or(breadcrumb, |count_at| count_at.min(breadcrumb));
            if heard.woken_before(wake, |channel, payload| WAKE_ON.iter().any(|w| w.hears(channel, payload))).await? {
                break;
            }
            if count_at.is_some_and(|count_at| Instant::now() >= count_at) {
                break;
            }
            tracing::info!(
                target: "weft_dispatcher::drain",
                project_id = %scope.project_id, ?scope.reaching, left,
                waited_secs = Instant::now().duration_since(started).as_secs(),
                "still waiting for running work to finish"
            );
            breadcrumb += BREADCRUMB_EVERY;
        }
    }
}

impl Reaching<'_> {
    /// What a run still going at a drain's deadline is cancelled for.
    fn cap_cause(&self) -> CancelCause {
        let detail = match self {
            Self::Triggers(_) => "its trigger was taken down, and the wait for running work to finish reached its cap",
            Self::Copies(_) => "the infra it may use is going down, and the wait for running work to finish reached its cap",
            Self::OtherImages(_) => {
                "this execution ran on an older image of the program, and the wait for it reached its cap after a new one went live"
            }
        };
        CancelCause::Runtime { detail: detail.into() }
    }
}

/// How much work `scope` reaches is going right now (see the module doc).
pub async fn going(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<i64> {
    Ok(driven_by_workers(pool, scope).await? + on_record(pool, scope).await?.len() as i64)
}

/// Cancel every run on record `scope` reaches that is queued or being
/// driven, but the one that asked, for `cause`.
pub(crate) async fn cancel_left(state: &DispatcherState, scope: &DrainScope<'_>, cause: &CancelCause) -> anyhow::Result<()> {
    let runs = on_record(&state.pg_pool, scope).await?;
    let targets: Vec<(ExecutionId, &CancelCause)> =
        runs.iter().filter(|id| Some(**id) != scope.except).map(|id| (*id, cause)).collect();
    crate::api::execution::cancel_execution_ids(state, &targets).await?;
    Ok(())
}

/// What `scope` reaches, as the queries below select it: the arguments
/// their filters read from `$2` on.
enum Binds<'a> {
    None,
    /// Triggers, and owners (`NULL` for the shared one).
    Keys(Vec<String>, Vec<Option<String>>),
    One(&'a str),
}

/// How the queries pick what `scope` reaches: among a live worker `l`'s
/// counts `f(token, n)`, among runs `r`, and the arguments both read.
struct Filter<'a> {
    counted: &'static str,
    runs: &'static str,
    binds: Binds<'a>,
}

impl<'a> Reaching<'a> {
    fn filter(&self) -> Filter<'a> {
        match *self {
            Reaching::Triggers(keys) => {
                let (triggers, instances) = keys.iter().map(|key| (key.trigger.clone(), key.instance().map(|m| m.as_str().to_string()))).unzip();
                Filter {
                    counted: "f.token IN (SELECT s.token FROM signal s \
                              JOIN unnest($2::text[], $3::text[]) AS k(trigger, instance) \
                                ON s.activation_trigger = k.trigger AND s.instance_id IS NOT DISTINCT FROM k.instance \
                              WHERE s.project_id = $1 AND NOT s.is_resume)",
                    runs: "EXISTS (SELECT 1 FROM unnest($2::text[], $3::text[]) AS k(trigger, instance) \
                           WHERE r.fired_by = k.trigger AND r.instance_id IS NOT DISTINCT FROM k.instance)",
                    binds: Binds::Keys(triggers, instances),
                }
            }
            Reaching::Copies(Copies::Shared | Copies::Every) => Filter { counted: "TRUE", runs: "TRUE", binds: Binds::None },
            Reaching::Copies(Copies::Instance(instance)) => Filter {
                counted: "f.token IN (SELECT s.token FROM signal s WHERE s.project_id = $1 AND NOT s.is_resume AND s.instance_id = $2)",
                runs: "r.instance_id = $2",
                binds: Binds::One(instance.as_str()),
            },
            Reaching::OtherImages(keep) => Filter { counted: "l.binary_hash <> $2", runs: "r.binary_hash <> $2", binds: Binds::One(keep) },
        }
    }
}

/// `query` with `scope`'s project as `$1` and `filter`'s arguments after.
fn bound<'q, O>(
    query: sqlx::query::QueryScalar<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    scope: &DrainScope<'_>,
    filter: &'q Filter<'_>,
) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments> {
    let query = query.bind(scope.project_id);
    match &filter.binds {
        Binds::None => query,
        Binds::Keys(triggers, instances) => query.bind(triggers).bind(instances),
        Binds::One(value) => query.bind(*value),
    }
}

/// A run on record that is queued for a worker or being driven by one.
const GOING: &str = concat!("(r.state = 'queued' OR ", weft_task_store::in_flight_sql!("r"), ")");

/// What the project's live workers say they drive of what `scope` reaches.
async fn driven_by_workers(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<i64> {
    const ALIVE: &str = weft_task_store::worker_alive!("l");
    let filter = scope.reaching.filter();
    let sql = format!(
        "SELECT COALESCE(SUM(f.n::bigint), 0)::bigint FROM worker_lease l \
         CROSS JOIN LATERAL jsonb_each_text(l.in_flight) AS f(token, n) \
         WHERE l.project_id = $1 AND {ALIVE} AND {}",
        filter.counted
    );
    Ok(bound(sqlx::query_scalar(&sql), scope, &filter).fetch_one(pool).await?)
}

/// The runs on record `scope` reaches that are queued for a worker or
/// being driven by one.
async fn on_record(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<Vec<ExecutionId>> {
    let filter = scope.reaching.filter();
    let sql = format!("SELECT r.execution_id FROM run r WHERE r.project_id = $1 AND {GOING} AND {}", filter.runs);
    Ok(bound(sqlx::query_scalar(&sql), scope, &filter).fetch_all(pool).await?)
}

/// Mark watched every run on record `scope` reaches that is going, or
/// parked and so able to go again (`run.watch_end`): its ending then
/// announces itself (`weft_journal::RUN_ENDED_CHANNEL`), so the drain
/// counts again when it ends rather than on a timer, a parked run that
/// resumed meanwhile included. Its going again needs no announcing: while
/// it goes, the drain counts it and looks at its owner's hold
/// ([`workers_look`]). Marked when a drain looks, never on a run's own
/// path.
pub async fn mark_watched(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<()> {
    let filter = scope.reaching.filter();
    let sql = format!(
        "WITH marked AS (UPDATE run r SET watch_end = TRUE \
             WHERE r.project_id = $1 AND NOT r.watch_end AND (r.state = 'parked' OR {GOING}) AND {} RETURNING 1) \
         SELECT count(*) FROM marked",
        filter.runs
    );
    let _marked: i64 = bound(sqlx::query_scalar(&sql), scope, &filter).fetch_one(pool).await?;
    Ok(())
}

/// When what `scope` reaches could next stop counting as going without
/// anything announcing it, in unix seconds: the earliest lease of a live
/// worker counting some of it at its door (a busy worker renews it every
/// second, one whose runs only wait vouched for itself until then, and a
/// lapsed one counts for nothing), and the earliest moment a run of it on
/// record stops being held by its owner (`weft_task_store::run_held_until_sql!`:
/// its owner's lease, or the grace of a run just handed to a worker whose
/// first tick has not landed). The drain counts again then.
pub async fn workers_look(pool: &sqlx::PgPool, scope: &DrainScope<'_>) -> anyhow::Result<Option<i64>> {
    const ALIVE: &str = weft_task_store::worker_alive!("l");
    const HELD: &str = weft_task_store::run_held_until_sql!("r");
    let filter = scope.reaching.filter();
    let sql = format!(
        "SELECT MIN(at) FROM ( \
             SELECT MIN(l.leased_until_unix) AS at FROM worker_lease l \
             WHERE l.project_id = $1 AND {ALIVE} \
               AND EXISTS (SELECT 1 FROM jsonb_each_text(l.in_flight) AS f(token, n) WHERE f.n::bigint > 0 AND {counted}) \
             UNION ALL \
             SELECT MIN({HELD}) FROM run r \
             WHERE r.project_id = $1 AND r.state = 'running' AND {HELD} >= EXTRACT(EPOCH FROM NOW())::BIGINT AND {runs} \
         ) looks",
        counted = filter.counted,
        runs = filter.runs,
    );
    Ok(bound(sqlx::query_scalar(&sql), scope, &filter).fetch_one(pool).await?)
}

/// The take-downs that wait: every activation left `Deactivating` lands
/// `Inactive` once nothing it waits on is going, and at its deadline what
/// is left is cancelled first. Counts again when a run it waits on ends
/// ([`WAKE_ON`]), and at the earliest of every drain's deadline and the
/// lapse of a worker lease it waits on.
pub fn drain_loop(state: DispatcherState) -> DrainLoop {
    DrainLoop::new("drains", "take down what waits for its runs to end", WAKE_ON, weft_task_store::drain::SAFETY_POLL_INTERVAL, move || {
        let state = state.clone();
        async move {
            let deactivating = state.activations.list_deactivating().await?;
            let now = crate::lease::now_unix();
            let mut next: Option<i64> = None;
            for (project_id, key) in deactivating {
                let deadline = state
                    .activations
                    .list(project_id)
                    .await?
                    .into_iter()
                    .find(|a| a.key == key)
                    .and_then(|a| a.lifecycle.drain_deadline_unix);
                let keys = std::slice::from_ref(&key);
                let scope = DrainScope { project_id, reaching: Reaching::Triggers(keys), except: None };
                if deadline.is_some_and(|deadline| now >= deadline) {
                    tracing::warn!(
                        target: "weft_dispatcher::drain",
                        %project_id, trigger = %key,
                        "a deactivation's wait reached its cap; cancelling what is left"
                    );
                    cancel_left(&state, &scope, &scope.reaching.cap_cause()).await?;
                }
                if land_if_drained(&state, project_id, &key).await? {
                    continue;
                }
                mark_watched(&state.pg_pool, &scope).await?;
                let look = workers_look(&state.pg_pool, &scope).await?.map(|at| at + 1);
                next = [next, deadline.filter(|deadline| *deadline > now), look].into_iter().flatten().min();
            }
            Ok(match next {
                Some(at) => DrainStep::RetryIn(Duration::from_secs((at - now).max(1).unsigned_abs())),
                None => DrainStep::Done,
            })
        }
    })
}

/// Land the activation `key` `Inactive` when it is `Deactivating` and
/// nothing it waits on is going. Idempotent: a stale view loses the
/// compare-and-set and the next look checks again, and an activate that
/// flipped it back to `Active` meanwhile wins it, so the deactivation
/// rolls back cleanly. Answers whether it waits no more: it landed, or
/// was not draining.
pub(crate) async fn land_if_drained(state: &DispatcherState, project_id: uuid::Uuid, key: &ActivationKey) -> anyhow::Result<bool> {
    use crate::activation_store::ProjectStatus;
    let draining = state
        .activations
        .list(project_id)
        .await?
        .iter()
        .any(|a| &a.key == key && a.lifecycle.status == ProjectStatus::Deactivating);
    if !draining {
        return Ok(true);
    }
    let scope = DrainScope { project_id, reaching: Reaching::Triggers(std::slice::from_ref(key)), except: None };
    if going(&state.pg_pool, &scope).await? > 0 {
        return Ok(false);
    }
    if state.activations.cas_status(project_id, key, ProjectStatus::Deactivating, ProjectStatus::Inactive).await? {
        tracing::info!(target: "weft_dispatcher::drain", %project_id, trigger = %key, "drain finished: deactivating -> inactive");
        // Both frontends see the transitional state end without asking.
        crate::transition::publish_transition_changed(state, project_id).await;
        // A trigger that landed taking no work may leave nothing of the
        // project taking any: its front goes.
        crate::front::let_go_if_idle(state, project_id).await?;
    }
    Ok(true)
}
