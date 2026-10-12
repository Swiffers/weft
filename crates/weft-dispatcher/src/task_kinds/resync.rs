//! `resync` task: one resync's take-down and bring-back, carried out
//! apart from the request that asked for it.
//!
//! `weft resync` takes a project's triggers that are on down with the
//! person's choice and brings them back on the registered build. Between
//! the two halves those triggers are off, so the work must not stop
//! halfway whatever happens to the request (its caller goes away, a
//! process restarts) or to one step (a database blip, a worker that could
//! not be reached). The request checks everything it can refuse for
//! first, then writes the resync down on the project row
//! (`project.resync`, a [`ResyncPlan`]: for each owner, the triggers that
//! were on), enters the project's `resyncing` transition and enqueues this
//! task in one transaction, and follows the task to its end
//! (`crate::api::project::resync`).
//!
//! The task drives the resync owner by owner
//! (`crate::api::project::resync_owner`). It writes down which triggers
//! it takes down before it takes them down, brings back exactly those, and
//! writes each owner it brought back into the plan, so a task run again
//! (its process died, the claim lapsed and another claimed it) carries on
//! where the last stopped, and a trigger that was off, or that went down
//! some other way meanwhile, is never turned on by it. While a
//! reactivation's setup run waits (on a person, for days), the task's
//! claim rests on that run (`crate::transition::ActivationHeartbeat`) and
//! is handed back once it ends (`crate::lapses`).
//!
//! Every drive of a plan has its own id ([`ResyncPlan::driver`]), carried
//! by its task, and every write of the plan is fenced on it: a task of an
//! earlier drive, still finishing a step after the plan was given up or
//! taken up again, writes nothing and turns nothing on.
//!
//! While the project holds the transition, no build registers
//! (`crate::build::waiting`) and no other activation of its triggers
//! starts (`crate::activation_store`).
//!
//! A step that fails is tried again a couple of times, a little later each
//! time; then the resync stops, with why written into its plan for `weft
//! status`, and the plan keeps holding the transition. A stop that waits
//! on something that comes back by itself (the infra its triggers read, an
//! activation still landing) is taken up again when it does
//! ([`retry_stopped`]); any stop is taken up again by `weft resync
//! --resume`; `weft cancel-activate` gives it up, leaving the triggers it
//! had not brought back down, and says which.

use anyhow::Result;
use async_trait::async_trait;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::activation::{ActivationKey, ActivationScope, OwnerTriggers};
use weft_core::instance::Owner;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

/// The task kind, dispatched by string (`crate::app::core_task_registry_builder`).
pub const RESYNC_KIND: &str = "resync";

/// How long a resync waits before it tries a step that failed again, after
/// each failure but the last, which stops it.
const RETRY_WAITS: [u64; 2] = [5, 20];

/// A resync under way, as the project row keeps it (`project.resync`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResyncPlan {
    /// The drive under way: minted when the resync begins and each time a
    /// stopped one is taken up again, carried by the task driving it, and
    /// every write of the plan names it.
    pub driver: uuid::Uuid,
    /// Whose triggers, one owner each, in the order they are resynced.
    pub owners: Vec<ResyncOwner>,
    /// The person's answer for taking them down, whose running-work part
    /// also governs the worker replacement as they come back.
    pub spec: weft_broker_client::protocol::DeactivateSpec,
    /// The build it brings live, as the person named it.
    pub build: weft_core::builds::BuildHashes,
    /// The owners brought back so far, the first of `owners` in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resynced: Vec<Owner>,
    /// The addresses their reactivations answer on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<weft_core::activation::ActivationUrl>,
    /// Why it stopped, once its tries ran out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<Stopped>,
    /// A person gave it up (`crate::project_store::ProjectStoreOps::give_up_resync`):
    /// no drive of it goes on, and this says when and what it left down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub given_up: Option<GivenUp>,
}

/// A resync a person gave up: when, and the triggers it had taken down
/// and not brought back at that moment ([`left_down`]). A trigger leaves
/// `left_down` once someone activates or takes it down
/// (`crate::activation_store`'s writes, in their transaction), and `weft
/// status` shows the rest that the program still has until the next
/// resync begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GivenUp {
    pub at_unix: i64,
    pub left_down: Vec<OwnerTriggers>,
}

impl GivenUp {
    /// Drop `keys` from what it left down, owners with none left out:
    /// someone decided about them since. Answers whether any was there.
    pub fn forget(&mut self, keys: &[ActivationKey]) -> bool {
        let before: usize = self.left_down.iter().map(|owner| owner.triggers.len()).sum();
        for owner in &mut self.left_down {
            owner.triggers.retain(|trigger| !keys.iter().any(|key| key.trigger == *trigger && key.owner == owner.owner));
        }
        self.left_down.retain(|owner| !owner.triggers.is_empty());
        self.left_down.iter().map(|owner| owner.triggers.len()).sum::<usize>() < before
    }

    /// Of the triggers it left down, the ones `project` still has for
    /// their owner: the ones `weft activate` can turn on.
    pub fn still_down(&self, project: &weft_core::project::ProjectDefinition) -> Vec<OwnerTriggers> {
        self.left_down
            .iter()
            .filter_map(|owner| {
                let kept: Vec<String> = owner
                    .triggers
                    .iter()
                    .filter(|trigger| {
                        ActivationScope { triggers: vec![(*trigger).clone()], instance: owner.owner.instance().cloned() }
                            .resolve(project)
                            .is_ok()
                    })
                    .cloned()
                    .collect();
                (!kept.is_empty()).then(|| OwnerTriggers { owner: owner.owner.clone(), triggers: kept })
            })
            .collect()
    }
}

/// One owner's triggers a resync takes down and brings back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResyncOwner {
    pub owner: Owner,
    /// Its triggers that were on when the resync began: the only ones it
    /// takes down.
    pub on: Vec<String>,
    /// The ones of `on` it took down, written down before it takes them
    /// down: the ones it brings back.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub down: Vec<String>,
}

impl ResyncOwner {
    /// The activations of `triggers` for this owner.
    pub fn keys(&self, triggers: &[String]) -> Vec<ActivationKey> {
        triggers.iter().map(|trigger| ActivationKey::new(trigger.clone(), self.owner.clone())).collect()
    }

    /// The scope naming `triggers` of this owner.
    pub fn scope(&self, triggers: &[String]) -> ActivationScope {
        ActivationScope { triggers: triggers.to_vec(), instance: self.owner.instance().cloned() }
    }
}

impl ResyncPlan {
    /// The owners not brought back yet.
    pub fn remaining(&self) -> &[ResyncOwner] {
        self.owners.get(self.resynced.len()..).unwrap_or_default()
    }

    /// What its task answers once every owner is back.
    fn answer(self) -> weft_core::activation::ResyncResponse {
        weft_core::activation::ResyncResponse { urls: self.urls, resynced: self.resynced }
    }
}

/// Why a resync stopped with triggers still down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stopped {
    /// The HTTP status its last failure answered with.
    pub status: u16,
    pub error: String,
    /// When it stopped (unix seconds).
    pub at_unix: i64,
    /// What it waits on that comes back by itself, when its failure was
    /// one: it is taken up again then ([`retry_stopped`]). `None`: only a
    /// person takes it up again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waits_for: Option<ResyncWait>,
}

/// What a stopped resync waits on that comes back by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncWait {
    /// The infra the triggers it brings back read is not running.
    Infra,
    /// An activation of its triggers is still landing.
    Activation,
}

/// The resync a project holds, and whether a task drives it now.
#[derive(Debug, Clone, PartialEq)]
pub struct ResyncState {
    pub plan: ResyncPlan,
    pub driven: bool,
}

impl ResyncState {
    /// Why it stands still with nothing driving it: its stop, or, when it
    /// recorded none (its task ended without writing it), that.
    pub fn standing_still(&self) -> Option<String> {
        if self.driven {
            return None;
        }
        Some(match &self.plan.stopped {
            Some(stopped) => stopped.error.clone(),
            None => "its task ended without recording why; the dispatcher's log has it".to_string(),
        })
    }
}

/// The task that drives `project_id`'s resync under `driver`: one at a
/// time per drive.
pub fn task(tenant: &str, project_id: uuid::Uuid, driver: uuid::Uuid) -> weft_task_store::tasks::NewTask {
    weft_task_store::tasks::NewTask {
        kind: RESYNC_KIND.to_string(),
        project_id: Some(project_id),
        dedup_key: Some(driver.to_string()),
        execution_id: None,
        tenant_id: tenant.to_string(),
        payload: serde_json::json!({ "driver": driver }),
    }
}

/// The drive a resync task carries ([`task`]).
fn driver_of(task: &Task) -> Result<uuid::Uuid> {
    let driver = task.payload.get("driver").and_then(Value::as_str).ok_or_else(|| anyhow::anyhow!("a resync task names no driver"))?;
    Ok(driver.parse()?)
}

pub struct ResyncExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for ResyncExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let id = task.project_id.ok_or_else(|| anyhow::anyhow!("a resync task names no project"))?;
        let driver = driver_of(task)?;
        let mut tries = RETRY_WAITS.iter();
        loop {
            match advance(state, id, driver).await {
                Ok(done) => return Ok(serde_json::to_value(done)?),
                Err(Halt::Given) => {
                    anyhow::bail!(
                        "the resync was stopped with `weft cancel-activate`; the triggers it had not brought back stay down"
                    )
                }
                Err(Halt::Removing) => {
                    anyhow::bail!("the resync stood down: {}", weft_core::projects::PROJECT_BEING_REMOVED)
                }
                Err(Halt::Failed(failure)) => {
                    if let Some(wait) = tries.next() {
                        tracing::warn!(target: "weft_dispatcher::resync", project_id = %id, error = %failure.error, "a resync step failed; trying it again");
                        tokio::time::sleep(weft_core::time_scale::scaled(std::time::Duration::from_secs(*wait))).await;
                        continue;
                    }
                    stop(state, id, driver, &failure).await?;
                    anyhow::bail!("{}", failure.error);
                }
            }
        }
    }
}

/// Why a resync did not finish ([`advance`], one owner's
/// `crate::api::project::resync_owner`).
pub(crate) enum Halt {
    /// A person gave it up, or took it up again under another drive: this
    /// drive's plan is gone.
    Given,
    /// `weft rm` marked the project: what the resync had not brought back
    /// is the removal's to take down.
    Removing,
    /// A step failed.
    Failed(Failure),
}

/// A resync step that failed: the status it answered, why, and what it
/// waits on when that comes back by itself.
pub(crate) struct Failure {
    pub status: StatusCode,
    pub error: String,
    pub waits_for: Option<ResyncWait>,
}

impl Halt {
    /// A step that failed waiting on `wait`.
    pub(crate) fn waiting(wait: ResyncWait, (status, error): (StatusCode, String)) -> Self {
        Halt::Failed(Failure { status, error, waits_for: Some(wait) })
    }
}

impl From<(StatusCode, String)> for Halt {
    fn from((status, error): (StatusCode, String)) -> Self {
        Halt::Failed(Failure { status, error, waits_for: None })
    }
}

fn internal(what: &'static str) -> impl Fn(anyhow::Error) -> Halt {
    move |e| Halt::from((StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}")))
}

/// Resync every owner not brought back yet under `driver`, writing each
/// into the plan as it comes back, then end the resync, which lands the
/// transition at rest. A drive that ended already (its task is run again
/// after its end) answers as it ended.
async fn advance(state: &DispatcherState, id: uuid::Uuid, driver: uuid::Uuid) -> Result<weft_core::activation::ResyncResponse, Halt> {
    let (mut plan, under_way) = state.projects.drive(id, driver).await.map_err(internal("read the resync"))?.ok_or(Halt::Given)?;
    if !under_way {
        return Ok(plan.answer());
    }
    let several = plan.owners.len() > 1;
    while plan.resynced.len() < plan.owners.len() {
        let owner = plan.owners[plan.resynced.len()].owner.clone();
        let back = crate::api::project::resync_owner(state, id, &mut plan).await.map_err(|halt| match halt {
            Halt::Failed(failure) if several => {
                let done = if plan.resynced.is_empty() {
                    "none resynced before it".to_string()
                } else {
                    let whose: Vec<String> = plan.resynced.iter().map(weft_core::deactivation::whose_triggers).collect();
                    format!("already resynced: {}", whose.join(", "))
                };
                let error = format!("resyncing {}: {} ({done})", weft_core::deactivation::whose_triggers(&owner), failure.error);
                Halt::Failed(Failure { error, ..failure })
            }
            halt => halt,
        })?;
        plan.urls.extend(back);
        plan.resynced.push(owner);
        if !state.projects.write_resync(id, &plan).await.map_err(internal("write down the owners resynced"))? {
            return Err(Halt::Given);
        }
    }
    if !state.projects.end_resync(id, driver).await.map_err(internal("end the resync"))? {
        return Err(Halt::Given);
    }
    crate::transition::publish_transition_changed(state, id).await;
    // Triggers that went down with their infra while the resync held the
    // project could not come back then (it refused every other
    // activation), and their infra may be up already.
    if let Err(e) = crate::api::infra::bring_back_what_went_down_with_infra(state, id).await {
        tracing::error!(
            target: "weft_dispatcher::resync",
            project_id = %id,
            error = %format!("{e:#}"),
            "the resync ended, but bringing back the triggers that went down with their infra meanwhile failed; \
             the next change of a copy tries again, or `weft activate` switches them on"
        );
    }
    Ok(plan.answer())
}

/// Write down why the resync stopped, for `weft status` and the request
/// following it. A drive given up or taken up again meanwhile has nothing
/// to write to.
async fn stop(state: &DispatcherState, id: uuid::Uuid, driver: uuid::Uuid, failure: &Failure) -> Result<()> {
    tracing::error!(target: "weft_dispatcher::resync", project_id = %id, error = %failure.error, "a resync stopped with triggers down");
    let Some((plan, true)) = state.projects.drive(id, driver).await? else { return Ok(()) };
    let stopped = Stopped {
        status: failure.status.as_u16(),
        error: failure.error.clone(),
        at_unix: crate::lease::now_unix(),
        waits_for: failure.waits_for,
    };
    state.projects.write_resync(id, &ResyncPlan { stopped: Some(stopped), ..plan }).await?;
    crate::transition::publish_transition_changed(state, id).await;
    Ok(())
}

/// Take up again every stopped resync whose stop waits on something that
/// is back now: the infra its triggers read runs, or no activation of
/// them is still landing. Called where those changes are heard (a copy's
/// status changing, an activation changing status:
/// `crate::api::infra::bring_back_triggers_whose_infra_returned`), so a
/// resync waiting on them goes on without a person. A stop that waits on
/// nothing that comes back by itself is left for `weft resync --resume`.
pub(crate) async fn retry_stopped(state: &DispatcherState) -> Result<()> {
    let mut failed = Vec::new();
    for (id, plan) in state.projects.stopped_resyncs().await? {
        if let Err(e) = retry_if_back(state, id, &plan).await {
            failed.push(format!("project {id}: {e:#}"));
        }
    }
    anyhow::ensure!(failed.is_empty(), "taking stopped resyncs up again failed: {}", failed.join("; "));
    Ok(())
}

async fn retry_if_back(state: &DispatcherState, id: uuid::Uuid, plan: &ResyncPlan) -> Result<()> {
    let back = match plan.stopped.as_ref().and_then(|stopped| stopped.waits_for) {
        None => false,
        Some(ResyncWait::Activation) => {
            let keys: Vec<ActivationKey> = plan.remaining().iter().flat_map(|owner| owner.keys(&owner.on)).collect();
            !state
                .activations
                .list(id)
                .await?
                .iter()
                .any(|a| keys.contains(&a.key) && a.lifecycle.status == crate::activation_store::ProjectStatus::Activating)
        }
        Some(ResyncWait::Infra) => {
            let Some(project) = state.projects.project(id).await? else { return Ok(()) };
            let mut up = true;
            for owner in plan.remaining() {
                up &= crate::api::project::require_trigger_infra(state, id, &project, &owner.keys(&owner.on)).await.is_ok();
            }
            up
        }
    };
    if !back {
        return Ok(());
    }
    let tenant = state.projects.tenant_for(id).await?.ok_or_else(|| anyhow::anyhow!("no project {id}"))?;
    if state.projects.resume_resync(id, &tenant).await?.is_some() {
        tracing::info!(target: "weft_dispatcher::resync", project_id = %id, "what a stopped resync waited on is back; taking it up again");
        crate::transition::publish_transition_changed(state, id).await;
    }
    Ok(())
}

/// The triggers `plan` took down that are still not on, by owner, as
/// `rows` stand: what a person who gives it up is told stays down. Read
/// in the give-up's own transaction, the rows are the truth: its take-down
/// and its turn-on both go through only while its drive holds the
/// project, so a trigger of `down` it never took down is still on, and one
/// it took down is on only once brought back.
pub fn left_down(plan: &ResyncPlan, rows: &[crate::activation_store::Activation]) -> Vec<OwnerTriggers> {
    plan.owners
        .iter()
        .filter_map(|owner| {
            let down: Vec<String> = owner
                .down
                .iter()
                .filter(|trigger| {
                    let key = ActivationKey::new((*trigger).clone(), owner.owner.clone());
                    !rows.iter().any(|a| a.key == key && a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
                })
                .cloned()
                .collect();
            (!down.is_empty()).then(|| OwnerTriggers { owner: owner.owner.clone(), triggers: down })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> ResyncPlan {
        let ada = Owner::Instance(weft_core::instance::InstanceId::new("ada").unwrap());
        ResyncPlan {
            driver: uuid::Uuid::nil(),
            owners: vec![
                ResyncOwner { owner: Owner::Shared, on: vec!["feed".into(), "door".into()], down: vec!["feed".into(), "door".into()] },
                ResyncOwner { owner: ada, on: vec!["feed".into()], down: Vec::new() },
            ],
            spec: serde_json::from_value(serde_json::json!({ "mode": "park", "runningPolicy": "cancel" })).unwrap(),
            build: Default::default(),
            resynced: Vec::new(),
            urls: Vec::new(),
            stopped: None,
            given_up: None,
        }
    }

    /// The owners left are the ones after those brought back.
    #[test]
    fn the_owners_left_follow_those_brought_back() {
        let mut plan = plan();
        assert_eq!(plan.remaining().len(), 2);
        plan.resynced.push(Owner::Shared);
        assert_eq!(plan.remaining().iter().map(|o| o.on.clone()).collect::<Vec<_>>(), [vec!["feed".to_string()]]);
        plan.resynced.push(plan.owners[1].owner.clone());
        assert!(plan.remaining().is_empty());
    }

    /// A stop that waits on nothing reads as it did; one that waits on
    /// infra says so on the wire.
    #[test]
    fn a_stop_says_what_it_waits_on() {
        let stopped = Stopped { status: 428, error: "infra down".into(), at_unix: 1, waits_for: Some(ResyncWait::Infra) };
        let wire = serde_json::to_value(&stopped).unwrap();
        assert_eq!(wire["waits_for"], "infra");
        let none = serde_json::json!({ "status": 409, "error": "no", "at_unix": 1 });
        assert_eq!(serde_json::from_value::<Stopped>(none).unwrap().waits_for, None);
    }

    /// What a given-up resync leaves down is what it took down and is not
    /// on now: never a trigger it did not take down, never one back on.
    #[test]
    fn what_stays_down_is_what_it_took_down_and_is_not_back() {
        use crate::activation_store::{Activation, ActivationLifecycle};
        let plan = plan();
        let row = |trigger: &str, lifecycle: ActivationLifecycle| Activation {
            key: ActivationKey::new(trigger, Owner::Shared),
            lifecycle,
            program: None,
            source_version: None,
        };
        let rows = [row("feed", ActivationLifecycle::active()), row("door", ActivationLifecycle::parked())];
        assert_eq!(left_down(&plan, &rows), [OwnerTriggers { owner: Owner::Shared, triggers: vec!["door".into()] }]);
        let rows = [row("feed", ActivationLifecycle::active()), row("door", ActivationLifecycle::active())];
        assert!(left_down(&plan, &rows).is_empty(), "ada's feed was never taken down");
    }

    /// What `weft status` shows of a given-up resync is what it left down
    /// that nobody decided about since and that the program still has for
    /// its owner: a trigger someone activated or took down drops out, and
    /// so does one the program lost.
    #[test]
    fn a_given_up_resync_shows_only_what_is_still_left_down() {
        let ada = Owner::Instance(weft_core::instance::InstanceId::new("ada").unwrap());
        let mut given_up = GivenUp {
            at_unix: 1,
            left_down: vec![
                OwnerTriggers { owner: Owner::Shared, triggers: vec!["feed".into(), "door".into(), "gone".into()] },
                OwnerTriggers { owner: ada.clone(), triggers: vec!["feed".into()] },
            ],
        };
        let nodes = ["feed", "door"].map(|id| {
            serde_json::json!({
                "id": id, "nodeType": "T", "label": null, "config": {}, "position": { "x": 0, "y": 0 },
                "features": { "isTrigger": true },
            })
        });
        let project: weft_core::project::ProjectDefinition =
            serde_json::from_value(serde_json::json!({ "id": uuid::Uuid::nil(), "nodes": nodes, "edges": [] })).unwrap();
        let shared = |triggers: &[&str]| OwnerTriggers { owner: Owner::Shared, triggers: triggers.iter().map(|t| t.to_string()).collect() };
        assert_eq!(given_up.still_down(&project), [shared(&["feed", "door"])], "a trigger the program lost, or no longer has per instance, is not shown");

        assert!(!given_up.forget(&[ActivationKey::new("feed", Owner::Instance(weft_core::instance::InstanceId::new("bob").unwrap()))]));
        assert!(given_up.forget(&[ActivationKey::new("door", Owner::Shared)]), "someone decided about the door");
        assert_eq!(given_up.still_down(&project), [shared(&["feed"])]);
        assert!(given_up.forget(&[ActivationKey::new("feed", Owner::Shared), ActivationKey::new("feed", ada)]));
        assert_eq!(given_up.left_down, [shared(&["gone"])], "an owner with nothing left down goes");
        assert!(given_up.still_down(&project).is_empty());
    }

    /// The task names the drive it carries, and two drives are two tasks.
    #[test]
    fn a_task_carries_its_drive() {
        let (a, b) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let task_a = task("t", uuid::Uuid::nil(), a);
        assert_ne!(task_a.dedup_key, task("t", uuid::Uuid::nil(), b).dedup_key);
        assert_eq!(task_a.payload["driver"], a.to_string());
    }
}
