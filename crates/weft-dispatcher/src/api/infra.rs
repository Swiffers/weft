//! Infra lifecycle endpoints.
//!
//! `/sync` (`weft infra start`) starts the copies that are not running and
//! nothing else, `/upgrade` cycles the ones running onto the build (and
//! starts the ones it newly needs), `/stop` and `/terminate` take copies
//! down. Each acts on the copies of the owners it names (`instances`: the
//! shared ones by default) and the nodes it names (every one of their kind
//! by default).
//!
//! `/sync` runs an `InfraSetup` subworkflow run: the worker walks the
//! nodes it starts and their upstream closure; each infra node calls
//! `Node::provision_infra`. The engine enqueues an `Apply` lifecycle
//! command carrying the spec. The supervisor that owns the project picks
//! it up, makes the skip / fresh / replace decision (comparing the spec
//! hash against the stored `infra_node.applied_spec_hash`), applies, and
//! writes the updated `infra_node` row.
//!
//! A take-down (a stop, a terminate, an upgrade's stop leg) first takes
//! down the triggers whose cut contains the copies (`crate::infra_reach`),
//! as the request's answers say (`weft_core::take_down`, an infra
//! command's defaults: the door holds what arrives, what is held is kept,
//! and the copies wait for the runs reaching them to end). Those triggers
//! come back once the copies run again. Then it enqueues an
//! `infra_lifecycle_command` row for the supervisor that owns the project
//! to claim and execute, after the dispatcher's wait for the runs reaching
//! the copies when it waits.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;

use crate::api::project::StatusError;
use crate::authenticator::{authorize_project, CallerTenant};
use crate::infra_lifecycle_command::{self, Teardown};
use weft_core::infra::wire::{
    CommandOutcome, CommandStatus, CopyRef, Door, DoorsResponse, InfraLogs, InfraStatus, InfraStatusEntry,
    LifecycleCommandIssued, LogBlock, LogCursor, LogsFrom, StopRequest, SyncRequest, UpgradeRequest,
};
use weft_core::instance::{Copies, InstanceTarget};
use weft_core::take_down::{Held, Running, TakeDown};
use crate::infra_node::{self, InfraNodeRow, InfraNodeStatus};
use crate::state::DispatcherState;

/// Whether the infra place `spelled` (`one.db`) is a node that exists
/// once per instance.
pub(crate) fn is_per_instance_place(project: &weft_core::ProjectDefinition, spelled: &str) -> bool {
    let (id, _) = weft_core::project::resolve_address(project, spelled);
    project.nodes.iter().any(|n| n.id == id && n.per_instance.is_some())
}

/// The infra places a verb aimed at `nodes` (every infra node when
/// empty) for `instance` acts on: the nodes marked per instance for an
/// instance, the shared ones otherwise. A mismatch is refused naming the
/// fix, like the trigger verbs do.
pub(crate) fn resolve_infra_nodes(
    project: &weft_core::ProjectDefinition,
    nodes: &[String],
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<std::collections::BTreeSet<String>, (StatusCode, String)> {
    let per_instance_of = |spelled: &str| is_per_instance_place(project, spelled);
    let declared = weft_core::project::infra_place_spellings(project);
    if nodes.is_empty() {
        return Ok(declared.into_iter().filter(|n| per_instance_of(n) == instance.is_some()).collect());
    }
    let mut out = std::collections::BTreeSet::new();
    for node in nodes {
        if !declared.contains(node) {
            // A node inside a group or an included file written by its
            // short name: name the full spelling it most likely meant.
            let suffix = format!(".{node}");
            let meant: Vec<String> = declared.iter().filter(|d| d.ends_with(&suffix)).map(|d| format!("'{d}'")).collect();
            let hint = if meant.is_empty() { String::new() } else { format!(" (did you mean {}?)", meant.join(" or ")) };
            return Err((
                StatusCode::NOT_FOUND,
                format!("'{node}' is no infra node of this program{hint}; name it the way the program writes it (`work.{node}` inside a group, `one.{node}` inside an included file)"),
            ));
        }
        match (per_instance_of(node), instance) {
            (true, None) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "infra node '{node}' exists once per instance; name the instances whose copies to act on \
                         (`--instance <id>` or `--all-instances` on a command, the `instance` input on a node)"
                    ),
                ));
            }
            (false, Some(m)) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "infra node '{node}' is shared by every instance, so there is no copy of it for '{m}'; name no \
                         instance (leave out `--instance` and `--all-instances` on a command, leave the `instance` input \
                         unwired on a node)"
                    ),
                ));
            }
            _ => {
                out.insert(node.clone());
            }
        }
    }
    Ok(out)
}

/// The nodes of `nodes` (every one when empty) owner `instance` acts on
/// under `target`: all of them, except under `--all`, where each node
/// named reaches the owners of its kind, the program for a shared node and
/// each instance for a per-instance one. `None` when nodes were named and
/// none is of this owner's kind: nothing to do for it. A name that is no
/// infra node stays, for [`resolve_infra_nodes`] to refuse.
pub(crate) fn owner_nodes(
    project: &weft_core::ProjectDefinition,
    target: &InstanceTarget,
    nodes: &[String],
    instance: Option<&weft_core::instance::InstanceId>,
) -> Option<Vec<String>> {
    if !matches!(target, InstanceTarget::Everything) || nodes.is_empty() {
        return Some(nodes.to_vec());
    }
    let declared = weft_core::project::infra_place_spellings(project);
    let mine: Vec<String> = nodes
        .iter()
        .filter(|node| !declared.contains(*node) || is_per_instance_place(project, node) == instance.is_some())
        .cloned()
        .collect();
    (!mine.is_empty()).then_some(mine)
}

/// The owners whose copies an infra verb aimed at `instances` acts on,
/// each once (`None`: the program's own). For every instance (`--all-instances`,
/// and `--all` beside the program's own): the instances with a
/// copy of some infra for a verb on copies that exist (an instance with
/// none has nothing to take down or cycle), and every instance weft holds
/// anything for for a start, which makes the copies it lacks.
async fn copy_owners(
    state: &DispatcherState,
    id: uuid::Uuid,
    instances: &InstanceTarget,
    starts: bool,
) -> Result<Vec<Option<weft_core::instance::InstanceId>>, (StatusCode, String)> {
    let every: Vec<weft_core::instance::InstanceId> = match (instances.every_instance(), starts) {
        (true, true) => crate::api::project::known_instances(state, id).await?,
        (true, false) => {
            let rows = infra_node::list_for_project(&state.pg_pool, id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e:#}")))?;
            rows.into_iter().filter_map(|row| row.instance).collect()
        }
        _ => Vec::new(),
    };
    Ok(instances.owners(&every).into_iter().map(|owner| owner.instance().cloned()).collect())
}

// =================================================================
// Handlers
// =================================================================

/// `POST /projects/{id}/infra/sync` (`weft infra start`): start the
/// copies of the owners named that are not running, and nothing else.
pub async fn sync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<SyncRequest>>,
) -> Result<Json<InfraStatus>, StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    sync_inner(state, id, body).await
}

pub(super) async fn sync_inner(
    state: DispatcherState,
    id: uuid::Uuid,
    body: SyncRequest,
) -> Result<Json<InfraStatus>, StatusError> {
    // Owner by owner, each on its own: one that cannot start is said with
    // its owner, and the others still start.
    let owners = copy_owners(&state, id, &body.instances, true).await?;
    let asked = owners.len();
    let mut failed: Vec<(Option<weft_core::instance::InstanceId>, StatusError)> = Vec::new();
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    for instance in owners {
        let Some(nodes) = owner_nodes(&registered, &body.instances, &body.nodes, instance.as_ref()) else { continue };
        let started: Result<(), StatusError> = async {
            if let Some(begun) = begin_sync(&state, id, &body.build, instance.as_ref(), &nodes, body.instances.is_shared()).await? {
                finish_sync(&state, begun).await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = started {
            failed.push((instance, e));
        }
    }
    if asked == 1 {
        if let Some((_, e)) = failed.pop() {
            return Err(e);
        }
    }
    if !failed.is_empty() {
        let mut status = None;
        let lines: Vec<String> = failed
            .into_iter()
            .map(|(instance, e)| {
                let (code, message) = e.into_parts();
                status.get_or_insert(code);
                match instance {
                    Some(instance) => format!("instance '{instance}': {message}"),
                    None => format!("the program's own infra: {message}"),
                }
            })
            .collect();
        let status = status.expect("one failed");
        return Err(StatusError::Other(status, format!("some copies did not start:\n  {}", lines.join("\n  "))));
    }
    Ok(Json(InfraStatus {
        nodes: read_infra_entries(&state, id).await?,
        unanswered: crate::api::project::unanswered_for(&state, id).await?,
    }))
}

/// `POST /projects/{id}/infra/upgrade`: replaces with the registered build
/// only the copies running on another one: checks the upgrade can go
/// ahead, then issues an `upgrade` command per owner that has some and
/// answers 202 with their ids at once, after taking down the triggers
/// whose cut contains the copies it cycles. Nothing running behind the
/// build is nothing to do, answered with no command; 409 while another upgrade of the same copies is
/// in flight. A dispatcher runs each ([`run_upgrade`]): the stop leg,
/// then the start. The caller follows the commands
/// (`/infra/commands/{id}`) to their outcome; nothing about them depends
/// on this request staying open, and a process dying mid-way hands one to
/// another.
pub async fn upgrade(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<UpgradeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    let UpgradeRequest { sync, take_down } = body.map(|Json(b)| b).unwrap_or_default();
    let take_down = take_down.resolve().map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    // What it replaces: each owner's copies running on another build than
    // the registered one, never one that is stopped or missing, nor one
    // already on it. Nothing of that kind anywhere is nothing to do, with
    // nothing checked or taken down: what a deploy that runs it on every
    // push meets on its first one.
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let binary_hash = state.projects.running_binary_hash(id).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running hashes: {e:#}")))?;
    let definition_hash =
        state.projects.running_definition_hash(id).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running hashes: {e:#}")))?;
    let desired = crate::api::project::Desired::registered(&state, id, &registered, binary_hash.as_deref(), definition_hash.as_deref()).await?;
    let observed = infra_node::observe(&state.pg_pool, id, &registered)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra copies: {e:#}")))?;
    let mut behind_by_owner = Vec::new();
    for instance in copy_owners(&state, id, &sync.instances, false).await? {
        let Some(nodes) = owner_nodes(&registered, &sync.instances, &sync.nodes, instance.as_ref()) else { continue };
        let targeted = resolve_infra_nodes(&registered, &nodes, instance.as_ref())?;
        let running: std::collections::BTreeSet<String> = targeted.into_iter().filter(|place| upgrades(&observed, &desired, place, instance.as_ref())).collect();
        if !running.is_empty() {
            behind_by_owner.push((instance, nodes, running));
        }
    }
    // Every owner is checked before the first one's triggers come down, so
    // a refusal leaves everything as it was.
    let mut planned = Vec::new();
    for (instance, nodes, running) in behind_by_owner {
        let registered = gate_sync(&state, id, &sync.build, instance.as_ref(), &nodes, SyncKind::Upgrade, sync.instances.is_shared()).await?;
        let in_flight_now = infra_lifecycle_command::upgrade_in_flight(&state.pg_pool, id, instance.as_ref().map(|m| m.as_str()))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("upgrades in flight: {e:#}")))?;
        if let Some(command_id) = in_flight_now {
            let (status, message) = infra_lifecycle_command::upgrade_in_flight_refusal(command_id);
            return Err(StatusError::Other(status, message));
        }
        let reach = crate::infra_reach::InfraReach { places: running.clone(), copies: Copies::of(instance.clone()) };
        let live = triggers_reached(&state, id, &registered, &reach).await?;
        planned.push((instance, nodes, running, reach, live));
    }
    let tenant = tenant_of(&state, id).await?;
    let mut taken = crate::take_down::Taken::default();
    for (instance, nodes, running, reach, live) in planned {
        // Down with the command, in one write, as the person answered: the
        // command is claimable only once the doors are set, so nothing
        // slips in between. Their start brings them back.
        let commands = infra_lifecycle_command::TakeDownCommands {
            tenant_id: tenant.clone(),
            issued_by_replica: state.replica.as_str().to_string(),
            work: infra_lifecycle_command::TakeDownWork::Upgrade {
                instance,
                work: infra_lifecycle_command::UpgradeWork {
                    nodes,
                    running: running.into_iter().collect(),
                    take_down,
                    binary_hash: sync.build.binary_hash.clone(),
                    definition_hash: sync.build.definition_hash.clone(),
                    infra_hash: sync.build.infra_hash.clone(),
                    waits_on_tokens: Vec::new(),
                    stopped: false,
                },
            },
        };
        taken.add(take_down_reached(&state, id, &reach, live, &take_down, crate::take_down::TakeDownAsker::Outside, &commands).await?);
    }
    Ok((StatusCode::ACCEPTED, Json(LifecycleCommandIssued { command_ids: taken.commands.clone(), report: taken.report(take_down) })))
}

/// Whether an upgrade replaces `instance`'s copy at `place`: it is
/// running, on another build of its place than the registered one
/// (`desired`). A copy stopped, missing, or already on the build is left
/// as it is. What a person's `weft infra upgrade` and a program's
/// `ctx.infra(..).upgrade(..)` both go by.
pub(crate) fn upgrades(
    observed: &infra_node::ObservedCopies,
    desired: &crate::api::project::Desired,
    place: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> bool {
    observed.status_of(place, instance) == Some(InfraNodeStatus::Running)
        && observed
            .rows
            .iter()
            .find(|row| row.node_id == place && row.instance.as_ref() == instance)
            .is_some_and(|row| desired.infra_behind(place, row.source_hash.as_deref()))
}

/// How an upgrade command ended, short of failing.
pub(crate) enum UpgradeEnd {
    Done,
    /// Cancelled (`/infra/cancel`, or the project went); the reason.
    Cancelled(String),
}

/// Carry out upgrade command `command_id` of `project_id`: `instance`'s
/// copies (the shared ones for `None`). The stop leg of the copies that
/// were running (unless a claimer before this one already landed it,
/// `work.stopped`), waiting for the runs reaching them first when the
/// person said so, then the start of those, nothing else. Safe to run again from the top on a takeover: the stop is
/// skipped once recorded, and a setup a dead claimer left in flight is
/// waited out before the start goes again.
pub(crate) async fn run_upgrade(
    state: &DispatcherState,
    id: uuid::Uuid,
    command_id: i64,
    instance: Option<&weft_core::instance::InstanceId>,
    work: &infra_lifecycle_command::UpgradeWork,
) -> Result<UpgradeEnd, (StatusCode, String)> {
    match upgrade_legs(state, id, command_id, instance, work).await {
        Ok(end) => Ok(end),
        Err(crate::api::project::SyncNotLanded::Cancelled(reason)) => Ok(UpgradeEnd::Cancelled(reason)),
        // An infra cancel ends the setup or the stop it lands in, and
        // that surfaces here as whatever error the leg met: the cancel
        // is what happened.
        Err(crate::api::project::SyncNotLanded::Failed(code, message)) => {
            if infra_lifecycle_command::cancel_requested(&state.pg_pool, command_id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the cancel flag: {e:#}")))?
            {
                return Ok(UpgradeEnd::Cancelled(format!("cancelled ({message})")));
            }
            Err((code, message))
        }
    }
}

/// [`run_upgrade`]'s legs, in order.
async fn upgrade_legs(
    state: &DispatcherState,
    id: uuid::Uuid,
    command_id: i64,
    instance: Option<&weft_core::instance::InstanceId>,
    work: &infra_lifecycle_command::UpgradeWork,
) -> Result<UpgradeEnd, crate::api::project::SyncNotLanded> {
    let Some(project) = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
    else {
        return Ok(UpgradeEnd::Cancelled(format!("project {id} no longer exists")));
    };
    let copies = Copies::of(instance.cloned());
    if !work.stopped && !work.running.is_empty() {
        let running: std::collections::BTreeSet<String> = work.running.iter().cloned().collect();
        // The apply path leaves up units frozen, so to cycle a running
        // unit onto a new spec the stop comes first (respecting each
        // unit's on_stop). The runs reaching the copies are the person's
        // answer: `wait` lets them end first, `cancel` ended them as the
        // upgrade was asked for.
        let mut pending =
            issue_per_nodes(state, id, &running, &copies, Teardown::Stop { force: false }, work.take_down.running == Running::Wait, &work.waits_on_tokens)
                .await?;
        // No deadline: a wait the person asked for can run as long as
        // their runs do. A breadcrumb each minute keeps the wait
        // legible, and `weft infra cancel` is the way out (it completes
        // the stop as cancelled, which ends this wait).
        let mut waited_minutes = 0u64;
        while !pending.is_empty() {
            let outcomes = crate::infra_lifecycle_command::wait_for_commands(
                &state.pg_pool,
                &state.signals,
                &pending,
                std::time::Duration::from_secs(60),
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("wait for stop leg: {e}")))?;
            pending.clear();
            for (stop_id, outcome) in outcomes {
                match outcome {
                    crate::infra_lifecycle_command::WaitOutcome::Succeeded => {}
                    crate::infra_lifecycle_command::WaitOutcome::Failed { error } => {
                        return Err(crate::api::project::SyncNotLanded::Failed(
                            StatusCode::BAD_GATEWAY,
                            format!("upgrade stop leg failed: {error}; infra left as-is, retry or act \
                                     per node (`weft infra status`)"),
                        ));
                    }
                    crate::infra_lifecycle_command::WaitOutcome::Cancelled { reason } => {
                        return Ok(UpgradeEnd::Cancelled(format!("upgrade stop leg cancelled ({reason}); infra left as-is")));
                    }
                    crate::infra_lifecycle_command::WaitOutcome::Timeout => pending.push(stop_id),
                }
            }
            if !pending.is_empty() {
                waited_minutes += 1;
                tracing::info!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    command_id,
                    waited_minutes,
                    still_stopping = pending.len(),
                    "upgrade stop leg still in flight; `weft infra cancel` calls it off"
                );
            }
        }
        let still_ours = infra_lifecycle_command::mark_upgrade_stopped(&state.pg_pool, command_id, state.replica.as_str())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("record the stop leg: {e:#}")))?;
        if !still_ours {
            return Ok(UpgradeEnd::Cancelled("another dispatcher took this upgrade over".into()));
        }
    }
    // A cancel that came while the stop was landing ends the upgrade
    // here, with the infra stopped (a cancel halts, never rolls back).
    if infra_lifecycle_command::cancel_requested(&state.pg_pool, command_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the cancel flag: {e:#}")))?
    {
        return Ok(UpgradeEnd::Cancelled("cancelled between the stop and the start; the infra is stopped".into()));
    }
    // A setup a claimer before this one started is waited out first: a
    // second start while it runs would be refused as in flight.
    // One nothing will advance any more is ended instead of followed.
    // Its outcome is not this upgrade's: whatever it ended in, the start
    // below goes again (a cancel meant for this upgrade is read off the
    // command's own flag, right after).
    let left = crate::api::project::infra_setup_run_ids(state, id, Some(instance))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup runs: {e:#}")))?;
    for run_id in left {
        if let Err(ended) =
            crate::api::project::await_infra_setup(state, crate::api::project::InfraSetupRun::follow(state, id, run_id).await)
                .await
        {
            tracing::info!(%id, %run_id, ?ended, "an earlier infra setup ended before this upgrade's start");
        }
    }
    if infra_lifecycle_command::cancel_requested(&state.pg_pool, command_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the cancel flag: {e:#}")))?
    {
        return Ok(UpgradeEnd::Cancelled("cancelled before the start; the infra is stopped".into()));
    }
    // What starts: the copies it cycled, nothing else. A copy that was
    // stopped before the upgrade stays stopped, and one the build newly
    // needs is `weft infra start`'s to make.
    let targeted = resolve_infra_nodes(&project, &work.nodes, instance)?;
    let start: Vec<String> = targeted.into_iter().filter(|place| work.running.contains(place)).collect();
    if start.is_empty() {
        return Ok(UpgradeEnd::Done);
    }
    // The build it was asked to apply, still the registered one: another
    // that registered since is not what the person upgraded to.
    let build = weft_core::builds::BuildHashes {
        binary_hash: work.binary_hash.clone(),
        definition_hash: work.definition_hash.clone(),
        infra_hash: work.infra_hash.clone(),
    };
    crate::api::project::require_registered_build(state, id, &build).await?;
    if let Some(begun) = apply_sync(state, id, instance, &start).await? {
        finish_sync(state, begun).await?;
    }
    Ok(UpgradeEnd::Done)
}

/// The triggers on whose cut contains the copies `reach` names: what
/// taking those copies down takes down with them. Refused while one of
/// them is being switched on.
pub(crate) async fn triggers_reached(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
    reach: &crate::infra_reach::InfraReach,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let activations = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let reached = crate::infra_reach::activations_reached(reach, &activations, project);
    if let Some(busy) = reached.iter().find(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Activating) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            format!("trigger {} has this infra in its cut and is being switched on; wait for it, or cancel it first", busy.key),
        ));
    }
    Ok(crate::infra_reach::live_triggers_reached(reach, &activations, project))
}

/// Take down `live` (the triggers [`triggers_reached`] found for the
/// copies `reach` names) as `take_down` says, as `asker` asked, issuing
/// `commands` in the same write and marked by them (the copies' return
/// brings them back), then what `take_down` says of the runs reaching the
/// copies: the paused ones cancelled when what is held is dropped, the
/// ones going cancelled when they are. The run that asked is never among
/// them (its own `StopSelf` decides its fate). What it did, the commands
/// issued among it.
pub(crate) async fn take_down_reached(
    state: &DispatcherState,
    id: uuid::Uuid,
    reach: &crate::infra_reach::InfraReach,
    live: Vec<weft_core::activation::ActivationKey>,
    take_down: &TakeDown,
    asker: crate::take_down::TakeDownAsker,
    commands: &infra_lifecycle_command::TakeDownCommands,
) -> Result<crate::take_down::Taken, (StatusCode, String)> {
    let mut taken =
        crate::api::project::take_down_triggers(state, id, live, take_down, crate::take_down::DownCause::Infra(commands), asker).await?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("the runs reaching the infra: {e:#}"));
    let asked_by = asker.run();
    let cause = weft_core::run::CancelCause::User;
    taken.runs = match take_down.running {
        Running::Cancel => crate::drain::cancel_reached(state, id, reach, asked_by, take_down.held == Held::Drop, &cause).await.map_err(internal)?,
        Running::Wait => {
            if take_down.held == Held::Drop {
                // The paused ones only.
                let paused: Vec<(weft_core::RunId, &weft_core::run::CancelCause)> =
                    crate::infra_reach::runs_reached(state, id, reach, asked_by)
                        .await
                        .map_err(internal)?
                        .into_iter()
                        .filter(|run| run.paused)
                        .map(|run| (run.run_id, &cause))
                        .collect();
                crate::api::run::cancel_run_ids(state, &paused).await.map_err(internal)?;
            }
            crate::drain::going_runs(state, id, reach, asked_by).await.map_err(internal)?
        }
    };
    Ok(taken)
}

/// What a sync's InfraSetup run is waited on as, once it is journaled.
pub(crate) struct BegunSync {
    run: crate::api::project::InfraSetupRun,
}

/// Which face of sync a request is: a plain start, or an upgrade's.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SyncKind {
    Start,
    Upgrade,
}

/// Everything of a plain sync of `instance`'s copies (`None`: the shared
/// ones) up to its InfraSetup being journaled and queued: the gates, then
/// [`apply_sync`] over the targeted copies that are not running. `None`
/// when every one runs already: nothing to start. What a program's
/// `ctx.infra(..).start()` waits for before it returns (the copy then
/// comes up on its own).
pub(crate) async fn begin_sync(
    state: &DispatcherState,
    id: uuid::Uuid,
    build: &weft_core::builds::BuildHashes,
    instance: Option<&weft_core::instance::InstanceId>,
    nodes: &[String],
    table: bool,
) -> Result<Option<BegunSync>, StatusError> {
    let registered = gate_sync(state, id, build, instance, nodes, SyncKind::Start, table).await?;
    let observed = infra_node::observe(&state.pg_pool, id, &registered)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra copies: {e:#}")))?;
    let off: Vec<String> = resolve_infra_nodes(&registered, nodes, instance)?
        .into_iter()
        .filter(|place| !matches!(observed.status_of(place, instance), Some(InfraNodeStatus::Running | InfraNodeStatus::Provisioning)))
        .collect();
    if off.is_empty() {
        return Ok(None);
    }
    Ok(apply_sync(state, id, instance, &off).await?)
}

/// The checks a sync or an upgrade passes before anything changes;
/// answers the registered program they act on. With `table`, the status's
/// action table is read too: what a verb aimed at the shared copies alone
/// is checked against (`--all` takes each owner as it finds it).
#[allow(clippy::too_many_arguments)]
async fn gate_sync(
    state: &DispatcherState,
    id: uuid::Uuid,
    build: &weft_core::builds::BuildHashes,
    instance: Option<&weft_core::instance::InstanceId>,
    nodes: &[String],
    kind: SyncKind,
    table: bool,
) -> Result<weft_core::ProjectDefinition, StatusError> {
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    resolve_infra_nodes(&registered, nodes, instance)?;
    let transition = state
        .projects
        .transition(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("transition: {e}")))?
        .unwrap_or(crate::project_store::ProjectTransition::None);
    if let Some(why) = transition.refusal() {
        return Err(StatusError::Other(StatusCode::CONFLICT, format!("infra cannot change right now: {why}")));
    }
    // Fast reject before any side effect; re-checked under the lock
    // in `apply_sync` (the locked re-check is the race-safe one).
    if crate::api::project::infra_setup_in_flight(state, id, Some(instance))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}")))?
    {
        return Err(StatusError::Other(
            StatusCode::CONFLICT,
            "an infra sync is already in flight for these copies; wait for it to \
             finish or cancel it (`weft infra cancel`)"
                .into(),
        ));
    }

    // The build this sync applies is the one registered last; the hashes
    // a client names are the build it just made, and a sync against
    // another one (a teammate's build landed between the two) is refused
    // rather than applied under the wrong name.
    crate::api::project::require_registered_build(state, id, build).await?;

    // Enforce against the same reconciliation the status renders: the
    // two faces of sync are distinct table verbs. The table is the
    // project's SHARED infra; an instance's copies are started by the
    // program or `--instance`, and gated by their own checks above.
    if table && instance.is_none() {
        let action = match kind {
            SyncKind::Start => "infra_start",
            SyncKind::Upgrade => "infra_upgrade",
        };
        crate::api::project::require_action(state, id, &[action]).await?;
    }
    Ok(registered)
}

/// A gated sync (or an upgrade past its stop leg) of `places` of
/// `instance`'s copies, from the orphans' reap to its InfraSetup being
/// journaled and queued. A start never takes a run down: what runs on
/// another image finishes there.
async fn apply_sync(
    state: &DispatcherState,
    id: uuid::Uuid,
    instance: Option<&weft_core::instance::InstanceId>,
    places: &[String],
) -> Result<Option<BegunSync>, (StatusCode, String)> {
    // Orphan reap. An `infra_node` row whose `node_id` isn't in the
    // current project source (or no longer carries `requiresInfra`)
    // is stale: the user removed it from .weft but the supervisor
    // still runs its units and keeps its disks. Reap before
    // running the subworkflow so the new shape is the only thing
    // alive afterwards. Hard error: leaking stale infra is silently
    // worse than asking the user to retry.
    reap_orphans(state, id).await?;

    // UNDER the per-project transition lock (short; two concurrent syncs
    // serialize here and the second is rejected by the in-flight
    // re-check): re-check no InfraSetup run is in flight, then journal
    // the InfraSetup run (the durable "sync in flight" state) and enqueue
    // it. Its wait happens OUTSIDE the lock ([`finish_sync`]): user code
    // upstream of infra nodes may legitimately be slow, and no lock is
    // held across it.
    let started: Result<Option<crate::api::project::InfraSetupRun>, (StatusCode, String)> =
        crate::lease::with_project_transition_lock(&state.lock_pool, id, || async {
            if crate::api::project::infra_setup_in_flight(state, id, Some(instance)).await? {
                return Ok(Err((
                    StatusCode::CONFLICT,
                    "an infra sync is already in flight for these copies; wait for it \
                     to finish or cancel it (`weft infra cancel`)"
                        .into(),
                )));
            }
            Ok(crate::api::project::start_infra_setup(state, id, instance, places).await)
        })
        .await
        .map_err(|e| crate::lease::lock_answer("project transition lock", e))?;
    Ok(started?.map(|run| BegunSync { run }))
}

/// The rest of a sync: wait for its InfraSetup to land.
pub(crate) async fn finish_sync(state: &DispatcherState, begun: BegunSync) -> Result<(), crate::api::project::SyncNotLanded> {
    crate::api::project::await_infra_setup(state, begun.run).await
}

/// Switch back on, in every project, the triggers an infra verb took down
/// with the copies in their cut (`DownWith::Infra`), once each of those
/// copies runs again, or the take-down ended without taking them down (a
/// cancel, a failure) (`infra_lifecycle_command::SinceTakeDown`). Woken by any change of a copy's
/// status and by every infra command's ending (`crate::reaper`'s
/// `infra_returns`), so whichever way a copy came back
/// up (a `weft infra start`, an upgrade's start leg, a program's own
/// `ctx.infra(..).start()`) and whichever dispatcher heard it, the marks
/// in the rows are what it acts on: one that died midway leaves them for
/// the next pass. One a person took down since carries no mark and stays
/// down, and one whose other copies are still down waits for their start.
/// Also woken when an activation ends or a held signal goes, so one
/// refused because another claim held its trigger comes back once that
/// claim ends. Then the queued events that waited on infra are handed over
/// again, and a stopped resync waiting on either change is taken up again
/// (`crate::task_kinds::resync::retry_stopped`).
pub(crate) async fn bring_back_triggers_whose_infra_returned(state: DispatcherState) -> anyhow::Result<weft_task_store::drain::DrainStep> {
    // SYNC: 'infra' <-> weft_broker_client::activation::DownWith::Infra
    let marked: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT project_id FROM trigger_activation WHERE went_down_with = 'infra' AND status = 'inactive'",
    )
    .fetch_all(&state.pg_pool)
    .await?;
    for id in marked {
        bring_back_what_went_down_with_infra(&state, id).await?;
    }
    // The events that waited on infra the run reads: each is held to the
    // copies as they are now, and waits again if one is still down.
    crate::parked_drain::drain_waiting_on(&state, weft_core::door_fire::UnclockedWait::Infra, None).await?;
    crate::task_kinds::resync::retry_stopped(&state).await?;
    Ok(weft_task_store::drain::DrainStep::Done)
}

/// [`bring_back_triggers_whose_infra_returned`] for project `id`: the
/// triggers it took down with their infra whose cut's copies all run, no
/// take-down reaching those copies under way any more, and either one of
/// them came back up since the command that took them down (a start, an
/// upgrade's start leg) or the last take-down reaching them since was
/// called off (cancelled, or failed, with the copies still up)
/// (`infra_lifecycle_command::SinceTakeDown::brings_back`). Owner by owner, since one
/// setup run is one owner's: one that cannot come back is logged with its
/// owner and the others still come back.
pub(crate) async fn bring_back_what_went_down_with_infra(state: &DispatcherState, id: uuid::Uuid) -> anyhow::Result<()> {
    let Some(project) = state.projects.project(id).await? else { return Ok(()) };
    let mut by_owner: std::collections::BTreeMap<weft_core::instance::Owner, Vec<String>> = Default::default();
    for activation in state.activations.list(id).await? {
        let lifecycle = &activation.lifecycle;
        let Some(first_command) = lifecycle.went_down.and_then(|w| w.first_command()) else { continue };
        if lifecycle.status != crate::activation_store::ProjectStatus::Inactive {
            continue;
        }
        let depends_on: std::collections::HashSet<String> = crate::infra_reach::depends_on(&activation, &project).into_iter().collect();
        match crate::api::project::missing_infra_nodes(state, id, &project, Some(&depends_on), activation.key.instance()).await {
            Ok(missing) if missing.is_empty() => {
                let read: Vec<(String, Option<weft_core::instance::InstanceId>)> =
                    weft_core::infra::run_gate::copies_read(&project, Some(&depends_on), activation.key.instance())
                        .into_iter()
                        .filter_map(|(place, copy)| Some((place, copy?.cloned())))
                        .collect();
                let since = infra_lifecycle_command::since_take_down(&state.pg_pool, id, &read, first_command).await?;
                if since.brings_back() {
                    by_owner.entry(activation.key.owner.clone()).or_default().push(activation.key.trigger)
                }
            }
            Ok(_) => {}
            Err((status, message)) => tracing::error!(
                target: "weft_dispatcher::infra",
                project_id = %id, trigger = %activation.key.trigger, instance = ?activation.key.instance(),
                "could not tell whether a trigger's infra is back, so it stays down for now: {status}: {message}"
            ),
        }
    }
    bring_back(state, id, by_owner).await;
    Ok(())
}

/// Switch `by_owner`'s triggers back on, owner by owner, each through an
/// activate: what an infra's return, or the cancel of the command that
/// took them down, does. A refusal is logged, never raised: the next
/// change of a copy tries again, and `weft activate` switches them on.
async fn bring_back(state: &DispatcherState, id: uuid::Uuid, by_owner: std::collections::BTreeMap<weft_core::instance::Owner, Vec<String>>) {
    for (owner, triggers) in by_owner {
        tracing::info!(target: "weft_dispatcher::infra", project_id = %id, ?owner, ?triggers, "bringing back the triggers that went down with their infra");
        let request = weft_core::activation::ActivateRequest {
            scope: weft_core::activation::ActivationScope::of_owner(triggers.clone(), &owner),
            ..Default::default()
        };
        match crate::api::project::activate_with(state, id, request, crate::api::project::ActivateAsker::InfraBack).await {
            Ok(_) => {}
            // Another dispatcher heard the same copy come back and is
            // switching them on.
            Err(crate::api::project::ActivateError::Refused(crate::activation_store::ClaimRefused::Claimed)) => {
                tracing::info!(target: "weft_dispatcher::infra", project_id = %id, ?owner, ?triggers, "already being brought back");
            }
            // A resync holds the project: they come back once it ends
            // (`crate::task_kinds::resync`).
            Err(crate::api::project::ActivateError::Refused(crate::activation_store::ClaimRefused::Resyncing)) => {
                tracing::info!(target: "weft_dispatcher::infra", project_id = %id, ?owner, ?triggers, "a resync holds the project; brought back once it ends");
            }
            // `weft rm` marked the project: its triggers go with it.
            Err(crate::api::project::ActivateError::Refused(crate::activation_store::ClaimRefused::Removing)) => {
                tracing::info!(
                    target: "weft_dispatcher::infra",
                    project_id = %id, ?owner, ?triggers,
                    "the infra the triggers need is up, but they stay down: {}",
                    weft_core::projects::PROJECT_BEING_REMOVED
                );
            }
            Err(error) => {
                let (status, message) = <(axum::http::StatusCode, String)>::from(error);
                tracing::error!(
                    target: "weft_dispatcher::infra",
                    project_id = %id, ?owner, ?triggers,
                    "the infra the triggers need is up, but they did not come back; tried again at the next change of a copy, \
                     or `weft activate` switches them on: {status}: {message}"
                );
            }
        }
    }
}

pub async fn stop(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let teardown = Teardown::Stop { force: body.force };
    issue_destroy(&state, id, teardown, &body).await
}

pub async fn terminate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    if body.force {
        return Err(StatusError::Other(
            StatusCode::BAD_REQUEST,
            "`force` brings down the units that stay up on a stop; a terminate removes every unit anyway".into(),
        ));
    }
    issue_destroy(&state, id, Teardown::TERMINATE, &body).await
}

/// `POST /projects/{id}/infra/cancel[?instance=<id>]`. Cancel one
/// owner's in-flight infra work: the shared copies' (no `instance`) or
/// one instance's. Flags that owner's claimed supervisor commands (the
/// executing supervisor halts between platform calls), cancels its
/// still-unclaimed ones outright, and cancels its non-terminal
/// InfraSetup provisioning run. A take-down called off while it waits for
/// the runs reaching its copies leaves them running, so the triggers it
/// took down with them come back. Dispatcher-owned verbs (deactivate /
/// reactivate) are never touched: they are health's own work, not
/// something a person started. Cancel = HALT, never rollback: no platform
/// applies a project transactionally, so per-node partial state stays
/// visible and the user terminates/retries per-node from where it stopped.
///
/// 412 when nothing infra-transitional is in flight for that owner
/// (stale tab; the client refetches `/status` and reconciles).
pub async fn cancel(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Query(copy): Query<CopyQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;

    let touched = infra_lifecycle_command::request_cancel_owner(&state.pg_pool, id, copy.instance.as_ref())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("request cancel: {e}")))?;

    // Cancel the provisioning sub-run too (the InfraSetup worker
    // run that computes specs and enqueues applies).
    let run_ids = crate::api::project::infra_setup_run_ids(&state, id, Some(copy.instance.as_ref()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup runs: {e}")))?;
    let had_setup = !run_ids.is_empty();
    for run_id in run_ids {
        crate::api::run::cancel_run_id(&state, run_id, &weft_core::run::CancelCause::User)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_run_id: {e}")))?;
    }

    if touched == 0 && !had_setup {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "no infra operation in flight to cancel".into(),
        ));
    }
    // The triggers the cancelled commands took down come back once those
    // commands end with their copies still up
    // (`infra_lifecycle_command::SinceTakeDown`): the
    // `infra_returns` loop hears every command end.
    Ok(StatusCode::ACCEPTED)
}

/// `POST /projects/{id}/infra/look`: a person asks the supervisor to look
/// at the project's infra again, so what it gave up on (`weft status`) is
/// tried once more, its health rules that gave up included. The
/// notice wakes a supervisor that scales to zero
/// (`weft_broker_client::lifecycle_command::LOOK_WAKE`).
pub async fn look(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    infra_lifecycle_command::person_asks_for_look(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("ask the supervisor to look: {e:#}")))?;
    Ok(StatusCode::ACCEPTED)
}

/// Stop / Terminate: for each owner the body names, the triggers whose cut
/// contains the copies taken down as the body's answers say, the runs
/// reaching them as those answers say, then the command (after the wait
/// for those runs, when it waits). Returns `202 Accepted` with the
/// commands and what the take-down did. The supervisor hasn't run yet at
/// this point; clients follow the commands for the outcome.
async fn issue_destroy(
    state: &DispatcherState,
    id: uuid::Uuid,
    teardown: Teardown,
    body: &StopRequest,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    let take_down = body.take_down.resolve().map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    // Reject-don't-crash against the same reconciliation the status
    // renders (a stale screen firing stop into a transitional /
    // already-stopped project). The table is the SHARED infra's; an
    // instance's copies are the program's and `--instance`'s to manage.
    if body.instances.is_shared() {
        let action = match teardown {
            Teardown::Stop { .. } => "infra_stop",
            Teardown::Terminate { .. } => "infra_terminate",
        };
        crate::api::project::require_action(state, id, &[action]).await?;
    }
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    // Every owner is checked before the first one's triggers come down, so
    // a refusal leaves everything as it was.
    let mut planned = Vec::new();
    for instance in copy_owners(state, id, &body.instances, false).await? {
        let Some(nodes) = owner_nodes(&project, &body.instances, &body.nodes, instance.as_ref()) else { continue };
        planned.push(plan_copies_down(state, id, &project, instance, &nodes).await?);
    }
    let mut taken = crate::take_down::Taken::default();
    for plan in planned {
        taken.add(take_planned_copies_down(state, id, plan, teardown, &take_down, crate::take_down::TakeDownAsker::Outside).await?);
    }
    Ok((StatusCode::ACCEPTED, Json(LifecycleCommandIssued { command_ids: taken.commands.clone(), report: taken.report(take_down) })))
}

/// Take `instance`'s copies (`None`: the shared ones) of `nodes` (every
/// one of their kind when empty) down with `teardown`, what reaches them
/// first as `take_down` says ([`take_down_reached`]): one command per node
/// named, one for the owner's copies otherwise. A node the program no
/// longer declares that still has a copy (an orphan) may be named: no
/// trigger's cut and no run reaches it. What a person's stop or terminate
/// and a program's `ctx.infra(..).stop(..)` both do.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn take_copies_down(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
    nodes: &[String],
    teardown: Teardown,
    take_down: &TakeDown,
    asker: crate::take_down::TakeDownAsker,
) -> Result<crate::take_down::Taken, (StatusCode, String)> {
    let plan = plan_copies_down(state, id, project, instance.cloned(), nodes).await?;
    take_planned_copies_down(state, id, plan, teardown, take_down, asker).await
}

/// One owner's copies a stop or terminate takes down, checked: the places,
/// the nodes it was asked for, and the triggers on that go down with them.
pub(crate) struct CopiesDown {
    reach: crate::infra_reach::InfraReach,
    /// The nodes named, one command each; empty for one command over the
    /// owner's copies.
    nodes: Vec<String>,
    live: Vec<weft_core::activation::ActivationKey>,
}

/// Check what taking `instance`'s copies of `nodes` down does, refusing a
/// node that is no copy (see [`take_copies_down`]) and a trigger it would
/// take down that is being switched on, before anything moves.
async fn plan_copies_down(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
    instance: Option<weft_core::instance::InstanceId>,
    nodes: &[String],
) -> Result<CopiesDown, (StatusCode, String)> {
    let instance = instance.as_ref();
    let copies = Copies::of(instance.cloned());
    // Each node must be a copy the program declares (a declared node on
    // the side `instance` names), or one a live row still holds: an
    // orphan taken down by hand, left behind when the node was removed or
    // changed side. A spelling that is neither would enqueue a command
    // for a row that cannot exist.
    let mut declared = Vec::new();
    for node in nodes {
        match resolve_infra_nodes(project, std::slice::from_ref(node), instance) {
            Ok(_) => declared.push(node.clone()),
            Err(refusal) => {
                let held = infra_node::get(&state.pg_pool, id, node, instance)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node lookup: {e}")))?
                    .is_some();
                if !held {
                    return Err(refusal);
                }
            }
        }
    }
    let places = match nodes.is_empty() {
        true => resolve_infra_nodes(project, &[], instance)?,
        false => declared.into_iter().collect(),
    };
    let reach = crate::infra_reach::InfraReach { places, copies };
    let live = triggers_reached(state, id, project, &reach).await?;
    Ok(CopiesDown { reach, nodes: nodes.to_vec(), live })
}

/// Carry out a checked [`CopiesDown`]: the triggers on that go down with
/// the copies, as `take_down` says, the commands issued in the same
/// write.
async fn take_planned_copies_down(
    state: &DispatcherState,
    id: uuid::Uuid,
    plan: CopiesDown,
    teardown: Teardown,
    take_down: &TakeDown,
    asker: crate::take_down::TakeDownAsker,
) -> Result<crate::take_down::Taken, (StatusCode, String)> {
    let CopiesDown { reach, nodes, live } = plan;
    let commands = infra_lifecycle_command::TakeDownCommands {
        tenant_id: tenant_of(state, id).await?,
        issued_by_replica: state.replica.as_str().to_string(),
        work: infra_lifecycle_command::TakeDownWork::Teardown {
            nodes,
            copies: reach.copies.clone(),
            teardown,
            waits: take_down.running == Running::Wait,
        },
    };
    take_down_reached(state, id, &reach, live, take_down, asker, &commands).await
}

/// The tenant `id` belongs to.
async fn tenant_of(state: &DispatcherState, id: uuid::Uuid) -> Result<String, (StatusCode, String)> {
    Ok(state
        .tenant_router
        .tenant_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .as_str()
        .to_string())
}

pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<InfraStatus>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(InfraStatus {
        nodes: read_infra_entries(&state, id).await?,
        unanswered: crate::api::project::unanswered_for(&state, id).await?,
    }))
}

/// The doors this project has serving: every `SameNetwork` endpoint of
/// every copy, at the address its host gave it when it applied the unit
/// (`infra_node.doors`), plus the copies still being applied, whose doors
/// have no address yet.
pub async fn doors(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<DoorsResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let rows = infra_node::list_for_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e}")))?;
    Ok(Json(doors_of(&rows)))
}

fn copy_ref(row: &InfraNodeRow) -> CopyRef {
    CopyRef { node: row.node_id.clone(), instance: row.instance.clone() }
}

/// One entry per door of each row, at its address as the host wrote it,
/// and every copy mid-apply.
fn doors_of(rows: &[InfraNodeRow]) -> DoorsResponse {
    DoorsResponse {
        doors: rows
            .iter()
            .flat_map(|row| {
                row.doors.iter().map(move |(endpoint, address)| Door {
                    copy: copy_ref(row),
                    endpoint: endpoint.clone(),
                    address: address.clone(),
                })
            })
            .collect(),
        applying: rows.iter().filter(|r| r.status == InfraNodeStatus::Provisioning).map(copy_ref).collect(),
    }
}

/// `GET /projects/{id}/infra/logs?node=&tail=&after=`.
#[derive(Deserialize)]
pub struct LogsQuery {
    /// One infra placement (`db`, `one.db`); every one when absent.
    pub node: Option<String>,
    #[serde(default = "default_tail")]
    pub tail: usize,
    /// A follower's `LogCursor` (JSON, as the previous answer gave it):
    /// only the lines written after it, `tail` ignored.
    pub after: Option<String>,
}

fn default_tail() -> usize {
    100
}

/// The infra units' own output, every container of every unit of every
/// copy of the node (or of every node), each block headed by what wrote
/// it, and the cursor that asks for exactly what comes next.
pub async fn logs(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    axum::extract::Query(q): axum::extract::Query<LogsQuery>,
) -> Result<Json<InfraLogs>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let after: Option<LogCursor> = q
        .after
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("after: not a log cursor: {e}")))?;
    let rows = infra_node::list_for_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e}")))?;
    let rows: Vec<&InfraNodeRow> = rows.iter().filter(|r| q.node.as_deref().is_none_or(|n| r.node_id == n)).collect();
    if rows.is_empty() {
        let why = match &q.node {
            Some(node) => format!("no infra node at `{node}`: it is not provisioned (`weft infra status` says where each one stands), or it is not an infra node"),
            None => "nothing is provisioned for this project (`weft infra start`)".to_string(),
        };
        return Err((StatusCode::NOT_FOUND, why));
    }
    let mut out = InfraLogs { blocks: Vec::new(), cursor: after.clone().unwrap_or_default() };
    for row in rows {
        let node = weft_core::infra::NodeRef {
            tenant: caller.0.as_str().to_string(),
            project: id,
            node: row.node_id.clone(),
            copy_id: row.copy_id.clone(),
        };
        for unit in row.units.keys() {
            let from = match &after {
                Some(cursor) => LogsFrom::After(cursor.for_unit(&row.copy_id, unit)),
                None => LogsFrom::Tail(q.tail),
            };
            let streams = state
                .host
                .logs(&node, unit, &from)
                .await
                .map_err(|e| (StatusCode::BAD_GATEWAY, format!("the logs of {} {unit}: {e:#}", row.node_id)))?;
            for stream in streams {
                // The host made the mark: it reads the clock that stamped
                // the lines, which this process's clock may not agree with.
                out.cursor.0.insert(LogCursor::key(&row.copy_id, unit, &stream.source), stream.mark);
                if !stream.lines.is_empty() {
                    out.blocks.push(LogBlock { copy: copy_ref(row), unit: unit.clone(), stream });
                }
            }
        }
    }
    Ok(Json(out))
}

/// Poll target for a stop / terminate command's completion. The
/// command outcome is the honest "is it done" signal: a stop where a
/// NoOp unit stays up leaves the project rollup at `running`, so the
/// CLI can't infer completion from the rollup.
pub async fn command_status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, cmd_id)): Path<(uuid::Uuid, i64)>,
    Query(hold): Query<super::HoldQuery>,
) -> Result<Json<CommandStatus>, (StatusCode, String)> {
    // Scope the command read to this project: the command is looked up
    // by `(id, project_id)`, so a caller can't read another project's
    // command outcome by enumerating the sequential id.
    authorize_project(&state, &caller.0, id).await?;
    use infra_lifecycle_command::WaitOutcome;
    let outcome = infra_lifecycle_command::held_command_outcome(
        &state.pg_pool,
        &state.signals,
        id,
        cmd_id,
        hold.hold(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read command: {e}")))?;
    Ok(Json(match outcome {
        None => CommandStatus { done: false, outcome: None, message: None },
        Some(WaitOutcome::Succeeded) => {
            CommandStatus { done: true, outcome: Some(CommandOutcome::Succeeded), message: None }
        }
        Some(WaitOutcome::Failed { error }) => CommandStatus {
            done: true,
            outcome: Some(CommandOutcome::Failed),
            message: Some(error),
        },
        Some(WaitOutcome::Cancelled { reason }) => CommandStatus {
            done: true,
            outcome: Some(CommandOutcome::Cancelled),
            message: Some(reason),
        },
        // read_command_outcome never returns Timeout (non-blocking).
        Some(WaitOutcome::Timeout) => CommandStatus { done: false, outcome: None, message: None },
    }))
}

/// Whose copy a per-node read or press is about, or whose in-flight
/// work a cancel halts: absent for the shared one.
#[derive(Debug, Default, Deserialize)]
pub struct CopyQuery {
    #[serde(default)]
    pub instance: Option<weft_core::instance::InstanceId>,
}

/// `GET /projects/{id}/infra/nodes/{node}/live`, `{node}` being the
/// node's placement as a person spells it (`one.db`).
pub async fn live(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
    Query(copy): Query<CopyQuery>,
) -> Result<Json<weft_core::live::LiveFeed>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(read_live(&state, id, &node, copy.instance.as_ref()).await?))
}

/// What an infra node's container is showing right now.
///
/// The container's `/live` answer, read into the one display shape on
/// the way through: an answer that is not `{ "items": [...] }` is a
/// 502 naming what it sent, and an item that does not fit becomes a
/// line saying so. The caller has already been authorized for the
/// project (the editor by its project token, an outside consumer by
/// its signal token), so this is the one implementation both doors
/// share.
pub(crate) async fn read_live(
    state: &DispatcherState,
    id: uuid::Uuid,
    node: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<weft_core::live::LiveFeed, (StatusCode, String)> {
    fetch_live(state, &live_endpoint_url(state, id, node, instance).await?).await
}

/// What the container at `endpoint_url` (a node's `live_endpoint`,
/// [`live_endpoint_url`]) shows on its `/live`. Asks only the container.
pub(crate) async fn fetch_live(state: &DispatcherState, endpoint_url: &str) -> Result<weft_core::live::LiveFeed, (StatusCode, String)> {
    let live_url = format!("{}/live", endpoint_url.trim_end_matches('/'));
    // Reuse the dispatcher's shared HTTP client (one connection pool for the
    // process, not a fresh pool per request). Bound the WHOLE exchange, connect +
    // headers + body, with a single 3s deadline so a downstream node that accepts
    // the connection then trickles the body can't pin the request open.
    let answer = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let resp = state
            .http
            .get(&live_url)
            .send()
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live fetch: {e}")))?;
        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live parse: {e}")))
    })
    .await
    .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "live endpoint timed out".to_string()))??;
    // Read it into the one display shape here, at the door, so a
    // container serving something else is a loud 502 naming what it
    // sent rather than an empty panel every reader has to interpret.
    weft_core::live::LiveFeed::from_answer(&answer)
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("the container's /live: {e}")))
}

/// POST /projects/{id}/infra/nodes/{node}/action, `{node}` being the
/// node's placement as a person spells it (`one.db`): press a button a
/// `/live` item offered. The container serving `/live` also serves
/// `/action` with the bridge envelope (`{ "action", "payload" }` in,
/// `{ "result" }` out; a `result.error` is the container refusing),
/// and this route carries the press there, so a container's own
/// action (log a phone out, rotate a key) needs nothing in weft beyond
/// the button on its `/live` item. The container's refusal comes back
/// as 400 with its text, whether it refused in the envelope or with a
/// 4xx of its own; an unreachable container or a 5xx is a 502.
///
/// SYNC: the /action envelope <-> catalog/bailey/bridge/images/bridge/src/actions.js, catalog/postgres/database/images/credential/bootstrap.py The press waits as
/// long as the container takes: the work is the container's and the
/// wait the user's, so no deadline is put on it here (the editor shows
/// the button pressed until the answer lands).
pub async fn action(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
    Query(copy): Query<CopyQuery>,
    Json(body): Json<weft_core::live::LivePress>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(press_live(&state, id, &node, copy.instance.as_ref(), &body.kind, &body.payload).await?))
}

/// POST /projects/{id}/infra/nodes/{node}/rebake, `{node}` being the
/// node's placement as a person spells it (`one.db`): make the node's
/// baked outputs again (`crate::infra_bake::rebake`), by hand, for a copy
/// whose values changed in a way weft did not see (a password changed by
/// hand inside a database). Answers once its infra setup ended.
pub async fn rebake(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
    Query(copy): Query<CopyQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    crate::infra_bake::rebake(&state, id, &node, copy.instance.as_ref()).await?;
    Ok(Json(serde_json::json!({ "rebaked": node })))
}

/// Press a button one of an infra node's `/live` items carries. The
/// caller has already been authorized for the project; both doors onto
/// a node's display land here.
pub(crate) async fn press_live(
    state: &DispatcherState,
    id: uuid::Uuid,
    node: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    kind: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let endpoint_url = live_endpoint_url(state, id, node, instance).await?;
    use weft_core::infra::action::{action_request, ACTION_PATH};
    let action_url = format!("{}{ACTION_PATH}", endpoint_url.trim_end_matches('/'));
    let resp = state
        .http
        .post(&action_url)
        .json(&action_request(kind, payload.clone()))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("action send: {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("action read: {e}")))?;
    if !status.is_success() {
        // A 4xx is the container saying the press was wrong (an action
        // it does not have, a missing field), which is the caller's
        // problem and comes back as one. Only a 5xx or an unreachable
        // container is a gateway fault.
        let code = if status.is_client_error() {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::BAD_GATEWAY
        };
        return Err((code, format!("the container answered {status}: {text}")));
    }
    // The press changed what the node shows (a fresh QR code, a new
    // key): an editor watching it through any process sees it now rather
    // than at the next look.
    crate::display_feeds::DisplayFeeds::announce_look_now(
        &state.pg_pool,
        &crate::display_feeds::DisplayKey {
            project: id,
            source: crate::display_feeds::DisplaySource::Infra,
            node: node.to_string(),
            instance: instance.cloned(),
        },
    )
    .await;
    let answer = serde_json::from_str::<serde_json::Value>(&text)
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("action parse: {e}")))?;
    press_answer(kind, answer)
}

/// The `result` of a container's action answer, read by the one reader
/// every action goes through (`weft_core::infra::action`), with the
/// failure as the editor's answer: the refusal the container carried
/// (`result.error`) is the caller's, a 400 naming the action; an answer
/// that is not the envelope at all is the container's fault, a 502.
fn press_answer(kind: &str, answer: serde_json::Value) -> Result<serde_json::Value, (StatusCode, String)> {
    use weft_core::infra::action::{action_result, ActionFailure};
    action_result(kind, answer).map_err(|failure| {
        let code = match failure {
            ActionFailure::Refused { .. } => StatusCode::BAD_REQUEST,
            ActionFailure::NotEnvelope { .. } => StatusCode::BAD_GATEWAY,
        };
        (code, failure.to_string())
    })
}

/// The URL of the endpoint an infra node names as serving `/live` (and
/// `/action`): the node must opt in through `features.live_endpoint`,
/// and the endpoint must be provisioned. Every miss is a 404 naming
/// which of those it is, so a TCP-only node (Postgres) answers "no
/// live endpoint" instead of a 502 from a refused connection.
pub(crate) async fn live_endpoint_url(
    state: &DispatcherState,
    id: uuid::Uuid,
    node: &str,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<String, (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    // `node` is a place spelling; the node behind it says whether it
    // serves a display, and the row under that spelling says where.
    let (node_id, _) = weft_core::project::resolve_address(&project, node);
    let node_def = project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .ok_or((StatusCode::NOT_FOUND, "no such node in project".into()))?;
    let live_endpoint = node_def.features.live_endpoint.as_deref().ok_or((
        StatusCode::NOT_FOUND,
        "node does not expose a /live endpoint".to_string(),
    ))?;
    let row = infra_node::get(&state.pg_pool, id, node, instance)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node lookup: {e}")))?
        .ok_or_else(|| match instance {
            Some(instance) => (StatusCode::NOT_FOUND, format!("instance '{instance}' has no copy of infra node '{node}'")),
            None => (StatusCode::NOT_FOUND, "no such infra node".to_string()),
        })?;
    // A copy that is not up has nothing behind its address (a stopped
    // one runs nothing, a starting one is not ready yet), so
    // asking it would be a connection error. Nothing to show is a 404,
    // naming the state so a reader can say "starting" rather than
    // "not started".
    if !matches!(row.status, InfraNodeStatus::Running | InfraNodeStatus::Flaky) {
        return Err((
            StatusCode::NOT_FOUND,
            format!("infra node '{node}' is {}, so it has nothing to show yet", row.status.as_str()),
        ));
    }
    row.install_endpoints.get(live_endpoint).cloned().ok_or((
        StatusCode::NOT_FOUND,
        format!("infra node has no endpoint named '{live_endpoint}' (live_endpoint)"),
    ))
}

// =================================================================
// Helpers
// =================================================================

/// Every copy as `weft status` and a program's `ctx.infra(..).status()`
/// show it (`infra_node::observe`): a start or a stop under way reads as
/// such before the supervisor reaches the copy, and a start counts its
/// progress from when it was asked for.
async fn read_infra_entries(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<Vec<InfraStatusEntry>, (StatusCode, String)> {
    let project = state
        .projects
        .project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let copies = infra_node::observe(&state.pg_pool, project_id, &project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra copies: {e:#}")))?;
    let starting: Vec<InfraStatusEntry> = copies
        .starting
        .iter()
        .map(|(node, instance)| InfraStatusEntry {
            node: node.clone(),
            instance: instance.clone(),
            status: infra_node::InfraNodeStatus::Provisioning.as_str().to_string(),
            endpoint_url: None,
            public_urls: Default::default(),
            failure_stage: None,
            failure_message: None,
            notes: Vec::new(),
            progress: copies.progress_of(node, instance.as_ref()),
        })
        .collect();
    let progress: Vec<_> = copies.rows.iter().map(|row| copies.progress_of(&row.node_id, row.instance.as_ref())).collect();
    Ok(copies
        .rows
        .into_iter()
        .zip(progress)
        .map(|(row, progress)| row_to_entry(row, progress, state.external_base_url()))
        .chain(starting)
        .collect())
}

/// One row as an entry, with the progress [`infra_node::ObservedCopies::progress_of`]
/// gives its copy.
fn row_to_entry(
    row: InfraNodeRow,
    progress: Option<weft_core::infra::wire::ChangeProgress>,
    front_door: &str,
) -> InfraStatusEntry {
    InfraStatusEntry {
        public_urls: row
            .public_paths
            .iter()
            .map(|(name, path)| (name.clone(), weft_core::infra::public_url(front_door, path)))
            .collect(),
        node: row.node_id,
        instance: row.instance,
        status: row.status.as_str().to_string(),
        // Coarse UI hint: the first endpoint by name (BTreeMap, so
        // deterministic). Node code resolves a specific endpoint by
        // name via ctx.endpoint(...); this is just a status summary.
        endpoint_url: row.install_endpoints.values().next().cloned(),
        failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
        failure_message: row.failure_message,
        notes: row.notes,
        progress,
    }
}

/// Enqueue a lifecycle command for `project_id`, under its tenant. The
/// command's row announces itself (`INFRA_COMMAND_CHANNEL`), which wakes
/// the supervisor wherever it runs (`weft_runtime::role_waker` for one at
/// zero).
pub(crate) async fn issue_lifecycle_for(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    copies: &weft_core::instance::Copies,
    teardown: Teardown,
    waits: bool,
    waits_on_tokens: &[String],
) -> Result<i64, (StatusCode, String)> {
    let tenant = state
        .tenant_router
        .tenant_for_project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let issued = infra_lifecycle_command::issue_lifecycle(
        &state.pg_pool,
        tenant.as_str(),
        project_id,
        node_id,
        copies,
        teardown,
        waits,
        waits_on_tokens,
        state.replica.as_str(),
    )
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("issue {}: {e}", teardown.verb().as_str()),
        )
    })?;
    Ok(issued)
}

/// One command per node for `nodes` of `copies` (an upgrade's stop leg
/// over exactly the nodes it cycles), its wait counting `waits_on_tokens`
/// at the door.
async fn issue_per_nodes(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    nodes: &std::collections::BTreeSet<String>,
    copies: &weft_core::instance::Copies,
    teardown: Teardown,
    waits: bool,
    waits_on_tokens: &[String],
) -> Result<Vec<i64>, (StatusCode, String)> {
    let mut ids = Vec::with_capacity(nodes.len());
    for node in nodes {
        ids.push(issue_lifecycle_for(state, project_id, Some(node), copies, teardown, waits, waits_on_tokens).await?);
    }
    Ok(ids)
}

/// Reap infra_node rows whose place is no longer one the project
/// source puts a `requiresInfra` node at: the node was deleted, or the
/// include that reached it was (a file included twice and then once
/// leaves the second call's instance behind). Step 1 of the sync
/// pipeline so the rest of the subworkflow operates on the new
/// shape only.
///
/// Issues a per-node Terminate lifecycle command for each orphan
/// and waits up to 60s for the supervisor to complete. Top-level
/// "look up the project / list its infra_nodes" failures propagate
/// (the rest of sync can't reason about state without them).
/// Per-orphan supervisor outcomes (Failed / Timeout / Cancelled)
/// are logged; one wedged orphan does not block the rest.
///
/// The terminate keeps the disks the node listed in `keepOnTerminate`,
/// like any terminate: the supervisor's sweep of copies the program no
/// longer declares deletes them once the row is gone (see
/// `weft_infra_supervisor::ownership::sweep_gone_copies`), which is also
/// what reaches a kept copy that had no row left to reap.
async fn reap_orphans(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let declared = weft_core::project::DeclaredInfra::of(&project);
    let rows = crate::infra_node::list_for_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e}")))?;
    // A copy the source no longer declares: the node is gone, or it
    // changed sides (a node no longer per instance leaves its instances'
    // copies behind, a node now per instance leaves its shared one). One
    // a part that does not compile may be hiding is left alone
    // (`DeclaredInfra::gone`): a typo in an included file must never cost
    // the database inside it.
    let orphans: Vec<(String, Option<weft_core::instance::InstanceId>)> = rows
        .into_iter()
        .filter(|r| declared.gone(&r.node_id, r.instance.is_some()))
        .map(|r| (r.node_id, r.instance))
        .collect();
    if orphans.is_empty() {
        return Ok(());
    }
    let tenant = state
        .tenant_router
        .tenant_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Step 1: issue every terminate in parallel. issue_lifecycle is
    // a single INSERT; bundling them keeps DB roundtrip cost flat
    // regardless of orphan count.
    let issue_futures = orphans.iter().map(|(node_id, instance)| {
        let tenant_str = tenant.as_str().to_string();
        let node_id = node_id.clone();
        let copies = weft_core::instance::Copies::of(instance.clone());
        let pool = state.pg_pool.clone();
        let replica = state.replica.as_str().to_string();
        async move {
            let res = infra_lifecycle_command::issue_lifecycle(
                &pool,
                &tenant_str,
                id,
                Some(&node_id),
                &copies,
                Teardown::TERMINATE,
                // An orphan is no copy of the program's: no run reaches it.
                false,
                // Nothing taken down with it: no trigger's door to count.
                &[],
                &replica,
            )
            .await;
            (node_id, res)
        }
    });
    let issued: Vec<(String, anyhow::Result<i64>)> = futures::future::join_all(issue_futures).await;

    // Step 2: wait on every issued command in ONE batched poll.
    // The previous shape spawned N concurrent `wait_for_command`
    // tasks (each polling the DB at 2 qps), saturating the pool on
    // a wedged supervisor with many orphans. The batched wait
    // collapses to one `SELECT ... WHERE id = ANY($1)` per cycle.
    let deadline = std::time::Duration::from_secs(60);
    let mut cmd_ids: Vec<i64> = Vec::new();
    let mut node_by_id: std::collections::HashMap<i64, String> = std::collections::HashMap::new();
    for (node_id, res) in issued {
        match res {
            Ok(cmd_id) => {
                cmd_ids.push(cmd_id);
                node_by_id.insert(cmd_id, node_id);
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    node_id = %node_id,
                    error = %e,
                    "orphan reap: failed to issue terminate; skipping"
                );
            }
        }
    }
    let outcomes =
        infra_lifecycle_command::wait_for_commands(&state.pg_pool, &state.signals, &cmd_ids, deadline)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("orphan reap: batched wait: {e}"),
                )
            })?;
    for (cmd_id, outcome) in outcomes {
        // `wait_for_commands` returns in input order; every cmd_id
        // it returns came from `node_by_id`. A missing entry would
        // be a programming error.
        let node_id = node_by_id
            .remove(&cmd_id)
            .expect("wait_for_commands returned cmd_id that wasn't in our issue set");
        match outcome {
            infra_lifecycle_command::WaitOutcome::Succeeded => tracing::info!(
                target: "weft_dispatcher::api::infra",
                project_id = %id,
                node_id = %node_id,
                "orphan terminated"
            ),
            infra_lifecycle_command::WaitOutcome::Failed { error } => tracing::warn!(
                target: "weft_dispatcher::api::infra",
                project_id = %id,
                node_id = %node_id,
                error = %error,
                "orphan reap: supervisor reported error"
            ),
            infra_lifecycle_command::WaitOutcome::Cancelled { reason } => tracing::info!(
                target: "weft_dispatcher::api::infra",
                project_id = %id,
                node_id = %node_id,
                reason = %reason,
                "orphan reap: command cancelled (likely raced a node removal)"
            ),
            infra_lifecycle_command::WaitOutcome::Timeout => tracing::warn!(
                target: "weft_dispatcher::api::infra",
                project_id = %id,
                node_id = %node_id,
                "orphan reap: supervisor did not complete within 60s"
            ),
        }
    }
    Ok(())
}

/// Project deletion entry point. Called by `weft rm`.
///
/// For a project with infra, issues a `Terminate` of every copy (the
/// shared ones and each instance's) and waits up to 120s for the
/// supervisor to complete it. Then removes the connections the
/// project's nodes published and releases the project's supervisor
/// lease. The `infra_*` rows go with the project row itself
/// (`ProjectStore::remove`), not here.
///
/// `force = true` (i.e. `weft rm --force`) skips the wait: the
/// dispatcher proceeds immediately. What the host still holds for the
/// project once its rows are gone (what a forced removal did not wait
/// for, and every disk its nodes listed in `keepOnTerminate`) is deleted
/// by the supervisor's sweep of copies whose project no longer exists.
pub async fn delete_project(
    state: &DispatcherState,
    id: uuid::Uuid,
    tenant: &str,
    force: bool,
) -> Result<(), (StatusCode, String)> {
    // Step 0: does this project have infra at all? A no-infra project has NOTHING
    // for the supervisor to terminate, so enqueuing a Terminate + waiting on it is
    // pure waste: no supervisor owns the project, the command is never marked
    // complete, and the wait below burns the full 120s timeout on EVERY no-infra
    // `weft rm` (the common case, and most e2e teardowns). Skip the supervisor
    // round-trip entirely; go straight to the broker-row cleanup. `None` (project
    // already unregistered) is also "nothing to terminate".
    let has_infra = state
        .projects
        .project_has_infra(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project_has_infra: {e}")))?
        .unwrap_or(false);
    if has_infra {
        // Step 1: enqueue a project-wide terminate so the supervisor
        // tears down the workloads. The command's row announces itself
        // (`issue_lifecycle_for`), which wakes the supervisor even at
        // zero. A failure to enqueue it refuses the rm, so the user
        // retries rather than leaving the project's infrastructure
        // behind.
        let cmd_id = issue_lifecycle_for(
            state,
            id,
            None,
            &weft_core::instance::Copies::Every,
            // The disks the nodes keep go with the project: the
            // supervisor's sweep deletes a removed project's copies. What
            // ran was cancelled by the removal's take-down.
            Teardown::TERMINATE,
            false,
            // Nothing is left to wait for: no trigger's door to count.
            &[],
        )
        .await?;
        // Step 2: wait for the supervisor unless --force. A wedged
        // supervisor blocks rm indefinitely otherwise; force lets the
        // user proceed knowing orphans may persist. Wait failures are
        // logged but don't block rm: the terminate row is in the queue
        // and the next sweep tick will catch it.
        if !force {
        match infra_lifecycle_command::wait_for_command(
            &state.pg_pool,
            &state.signals,
            cmd_id,
            std::time::Duration::from_secs(120),
        )
        .await
        {
            Ok(infra_lifecycle_command::WaitOutcome::Failed { error }) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    error = %error,
                    "supervisor reported terminate failure; continuing with rm cleanup"
                );
            }
            Ok(infra_lifecycle_command::WaitOutcome::Cancelled { reason }) => {
                tracing::info!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    reason = %reason,
                    "terminate cancelled (race with node removal); continuing with rm cleanup"
                );
            }
            Ok(infra_lifecycle_command::WaitOutcome::Succeeded) => {}
            Ok(infra_lifecycle_command::WaitOutcome::Timeout) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    "supervisor did not complete terminate within 120s; \
                     continuing with rm cleanup (orphans will be swept by the \
                     next supervisor sweep cycle)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    error = %e,
                    "wait_for_command errored; continuing with rm cleanup"
                );
            }
        }
        }
    }
    // Step 3: the project's infra rows (`infra_node`, `infra_event`,
    // `infra_lifecycle_command`) are NOT dropped here: they go with the
    // project row itself, in `ProjectStore::remove`'s one transaction,
    // because the broker inserts commands and events for as long as the
    // project row is visible. Dropped earlier, a health tick or a
    // worker's apply landing in between left rows nobody would ever run
    // or read.
    // The connections this project's nodes published opened services
    // this project ran; with the project gone they name nothing. The
    // per-node cleanup on terminate does not cover this path (a forced
    // delete does not WAIT for the terminate to land, and a project
    // with no infra rows never issues one at all), and a credential
    // nobody can place is exactly the junk the cleanup rule forbids. Connections a PERSON
    // connected are untouched: those are theirs.
    weft_access_store::delete_published_grants(&state.pg_pool, tenant, id, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("delete the connections this project's nodes published: {e}"),
            )
        })?;
    // The project's exclusive supervisor lease (`infra_owner`) goes with
    // its row (`crate::project_store::ProjectStoreOps::remove`): released
    // here, a supervisor could claim it again before the row went.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn door_row(node_id: &str, instance: Option<&str>, status: InfraNodeStatus, doors: &[(&str, &str)]) -> InfraNodeRow {
        InfraNodeRow {
            project_id: uuid::Uuid::nil(),
            node_id: node_id.into(),
            instance: instance.map(|m| weft_core::instance::InstanceId::new(m).unwrap()),
            copy_id: String::new(),
            status,
            failure_stage: None,
            failure_message: None,
            applied_spec_hash: None,
            applied_at_unix: None,
            endpoints: Default::default(),
            public_paths: Default::default(),
            doors: doors.iter().map(|(e, a)| (e.to_string(), a.to_string())).collect(),
            install_endpoints: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
            source_hash: None,
        }
    }

    /// Under `--all`, a node named reaches the owners of its kind: the
    /// program for a shared one, each instance for a per-instance one; an
    /// owner none of the named nodes is for has nothing to do. Any other
    /// target hands every owner the nodes as named.
    #[test]
    fn under_all_each_named_node_reaches_the_owners_of_its_kind() {
        let mut project = crate::api::project::infra_kick_and_dep_tests::project(&[("db", false, true), ("svc", false, true)], &[]);
        project.nodes.iter_mut().find(|n| n.id == "svc").unwrap().per_instance = Some(weft_core::instance::PerInstance::Marked);
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        let named = |nodes: &[&str]| nodes.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let all = InstanceTarget::Everything;
        assert_eq!(owner_nodes(&project, &all, &named(&["db", "svc"]), None), Some(named(&["db"])));
        assert_eq!(owner_nodes(&project, &all, &named(&["db", "svc"]), Some(&ada)), Some(named(&["svc"])));
        assert_eq!(owner_nodes(&project, &all, &named(&["db"]), Some(&ada)), None, "nothing named is ada's");
        assert_eq!(owner_nodes(&project, &all, &[], Some(&ada)), Some(Vec::new()), "none named is every one of its kind");
        assert_eq!(owner_nodes(&project, &all, &named(&["nope"]), None), Some(named(&["nope"])), "left for the refusal");
        assert_eq!(owner_nodes(&project, &InstanceTarget::Shared, &named(&["svc"]), None), Some(named(&["svc"])), "refused later, as named");
    }

    /// An upgrade replaces a copy only when it is running on another build
    /// of its place: never one on the build, stopped, or missing; and an
    /// instance's copy is its own.
    #[test]
    fn an_upgrade_replaces_only_a_running_copy_behind_its_build() {
        let desired = crate::api::project::Desired {
            binary: None,
            full_binary: None,
            definition: None,
            trigger_cuts: Default::default(),
            infra_places: [("db".to_string(), "db-2".to_string()), ("cache".to_string(), "cache-2".to_string())].into(),
        };
        let row = |node: &str, instance: Option<&str>, status, applied: &str| InfraNodeRow {
            source_hash: Some(applied.to_string()),
            ..door_row(node, instance, status, &[])
        };
        let observed = infra_node::ObservedCopies {
            rows: vec![
                row("db", None, InfraNodeStatus::Running, "db-1"),
                row("cache", None, InfraNodeStatus::Running, "cache-2"),
                row("db", Some("ada"), InfraNodeStatus::Stopped, "db-1"),
            ],
            starting: Vec::new(),
            change_asked_unix: Default::default(),
            as_of_unix: 0,
        };
        let ada = weft_core::instance::InstanceId::new("ada").unwrap();
        assert!(upgrades(&observed, &desired, "db", None), "running on the build before");
        assert!(!upgrades(&observed, &desired, "cache", None), "running on the build already");
        assert!(!upgrades(&observed, &desired, "db", Some(&ada)), "stopped: an upgrade never starts it");
        assert!(!upgrades(&observed, &desired, "queue", None), "missing: `weft infra start` makes it");
    }

    /// Every door keeps its address as the host wrote it (a name as the
    /// host part too), names whose copy it is, and a copy mid-apply is
    /// reported rather than left out.
    #[test]
    fn doors_carry_every_address_and_the_copies_still_applying() {
        let rows = [
            door_row("db", None, InfraNodeStatus::Running, &[("pg", "weft-db-main:5432")]),
            door_row("db", Some("ann"), InfraNodeStatus::Running, &[("pg", "10.0.0.3:5432")]),
            door_row("cache", None, InfraNodeStatus::Provisioning, &[]),
        ];
        let answer = doors_of(&rows);
        assert_eq!(answer.doors.len(), 2);
        assert_eq!(answer.doors[0].address, "weft-db-main:5432");
        assert_eq!(answer.doors[1].copy.instance.as_ref().map(|m| m.as_str()), Some("ann"));
        assert_eq!(answer.applying, vec![CopyRef { node: "cache".into(), instance: None }]);
    }

    fn build() -> serde_json::Value {
        json!({ "binaryHash": "abc", "definitionHash": "def0", "infraHash": "def" })
    }

    #[test]
    fn a_sync_names_the_build_it_applies_or_none() {
        let r: SyncRequest = serde_json::from_value(build()).unwrap();
        assert_eq!(r.build.binary_hash.as_deref(), Some("abc"));
        assert_eq!(r.build.definition_hash.as_deref(), Some("def0"));
        assert_eq!(r.build.infra_hash.as_deref(), Some("def"));
        let bare: SyncRequest = serde_json::from_value(json!({})).unwrap();
        assert!(bare.build.binary_hash.is_none(), "a program's sync applies the registered build");
        assert!(bare.instances.is_shared());
    }

    #[test]
    fn an_upgrade_parses_camelcase_only() {
        let mut body = build();
        body["door"] = json!("hide");
        body["running"] = json!("cancel");
        let camel: UpgradeRequest = serde_json::from_value(body).unwrap();
        assert_eq!(camel.sync.build.binary_hash.as_deref(), Some("abc"));
        let resolved = camel.take_down.resolve().unwrap();
        assert_eq!(resolved.door, weft_core::take_down::Door::Hide);
        assert_eq!(resolved.running, Running::Cancel);

        // ONE wire spelling: the build's hashes spelled snake_case are
        // not read, so the sync names no build.
        let snake: SyncRequest = serde_json::from_value(json!({
            "binary_hash": "abc", "definition_hash": "d", "infra_hash": "i",
        }))
        .unwrap();
        assert!(snake.build.binary_hash.is_none());
    }

    #[test]
    fn a_pressed_actions_failure_is_the_callers_or_the_containers() {
        use super::press_answer;
        let ok = press_answer("logout", serde_json::json!({ "result": { "success": true } })).unwrap();
        assert_eq!(ok, serde_json::json!({ "success": true }));
        let (status, msg) = press_answer(
            "logout",
            serde_json::json!({ "result": { "error": "WhatsApp not connected" } }),
        )
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(msg, "logout: WhatsApp not connected");
        // No `result` at all is not the envelope: reported, never read as success.
        let (status, msg) = press_answer("x", serde_json::json!({ "ok": true })).unwrap_err();
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(msg.contains("without a `result` envelope"), "{msg}");
    }
}
