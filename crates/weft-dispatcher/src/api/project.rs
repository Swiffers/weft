//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh run.
//! `weft run` on the CLI calls these.

use std::collections::HashSet;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use weft_core::activation::{
    ActivateRequest, ActivateResponse, ActivationTarget, ActivationUrl, BakeRequest, ReactivateChoice, ResyncResponse,
};
use weft_core::deactivation::{whose_triggers, DeactivateRequest, DeactivateResponse, ResyncRequest};
use weft_core::projects::{
    ActivationEntry, DeclareRequest, InstanceInfraEntry, LimitedEntry, PreservationCounts, ProjectDrift,
    ProjectRunsSummary, ProjectInfraEntry, ProjectStatusResponse, ProjectSummary, GoingRun, StatusQuery,
};
use weft_core::infra::wire::{INFRA_NOT_STARTED, INFRA_PER_INSTANCE};
use weft_core::frames::Located;
use weft_core::infra::run_gate::{copies_read, infra_not_running, missing_and_fix, MissingCopy};
use weft_core::ProjectDefinition;

use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

pub use weft_core::run_spec::KickPlan as Kick;

/// The summary of one project: its shared activations read as one
/// status (`activation_store::aggregate`), or, for a project `weft rm`
/// stopped partway through removing, that it is being removed.
async fn summary_of(
    state: &DispatcherState,
    summary: crate::project_store::StoredProjectSummary,
) -> Result<ProjectSummary, (StatusCode, String)> {
    let status = if summary.removing {
        crate::project_store::ProjectRemoving.to_string()
    } else {
        let activations = state
            .activations
            .list(summary.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
        crate::activation_store::aggregate(
            activations.iter().filter(|a| a.key.owner == weft_core::instance::Owner::Shared).map(|a| &a.lifecycle),
        )
        .status
        .as_str()
        .to_string()
    };
    Ok(ProjectSummary { id: summary.id.to_string(), name: summary.name, description: summary.description, status })
}

/// Every image the system still needs, across ALL tenants: the keep-set
/// image reclamation deletes against (`crate::build::prune::keep_set`),
/// so under-reporting here deletes an image something still runs.
/// `worker_hashes` are binary hashes (the tag of `<registry>/weft-worker:<hash>`):
/// each project's `running_binary_hash` (what a fresh spawn would run),
/// the `binary_hash` stamped on every pending/claimed task, and every live
/// run's. `infra_refs` are full infra image refs, registry-qualified as the
/// build registered them: every project's complete infra image-tag map
/// (`infra_image_tags_json`, written atomically with the running-hash
/// trio on every infra sync) AND the image refs recorded on every
/// `infra_node` unit (an UP unit is deliberately left frozen at its old
/// image across syncs until it is force-stopped, so its recorded ref can
/// be older than the project's current map and must stay referenced while
/// the unit runs).
#[derive(Debug)]
pub struct ReferencedImages {
    pub worker_hashes: Vec<String>,
    pub infra_refs: Vec<String>,
}

/// `POST /images/prune`: delete from the install's registry every image it
/// built that nothing references (`weft clean --images`); one project's
/// when the body names it. Control-plane only, like the referenced set it
/// deletes against.
pub async fn prune_images(
    State(state): State<DispatcherState>,
    _ops: crate::authenticator::ControlPlaneCaller,
    Json(req): Json<weft_core::images::PruneRequest>,
) -> Result<Json<weft_core::images::PruneReport>, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // Images a build in progress claims, or registers after this read, are
    // spared inside `prune`, one by one under each image's lock, so this
    // never waits for builds to finish.
    let referenced = {
        let mut conn = state.pg_pool.acquire().await.map_err(|e| internal("referenced images", e.into()))?;
        crate::build::prune::keep_set(&mut conn, crate::build::prune::ImageScope::All)
            .await
            .map_err(|e| internal("referenced images", e))?
    };
    let mut built = crate::build::ledger::built_images(&state.pg_pool, req.project)
        .await
        .map_err(|e| internal("built images", e))?;
    if let Some(project) = req.project {
        // One project's clean takes what it does not run, never what
        // another project is on or could go back to.
        let uses = crate::build::ledger::image_uses(&state.pg_pool).await.map_err(|e| internal("image uses", e))?;
        built = crate::build::prune::not_recent_elsewhere(&uses, built, project);
    }
    let report = crate::build::prune::prune(
        &state.pg_pool,
        state.builder.images.as_ref(),
        state.runner.as_ref(),
        &built,
        &referenced,
    )
    .await
    .map_err(|e| internal("prune images", e))?;
    Ok(Json(report))
}

/// The queries behind [`ReferencedImages`], restricted to `scope`: the
/// whole set for a prune's keep-set, one image's part when a prune
/// re-checks that image under its lock (`crate::build::ledger::begin_reclaim`).
/// One definition for both, so the bulk read and the per-image re-check
/// can never disagree on what "referenced" means.
pub async fn referenced_images_query(
    conn: &mut sqlx::PgConnection,
    scope: crate::build::prune::ImageScope<'_>,
) -> anyhow::Result<ReferencedImages> {
    // Every image something may still run: each project's current one,
    // each run's that has not ended (a run waiting on a form or a timer
    // resumes on the image it started on, however many times the project
    // was rebuilt since), and each live worker's (it may be driving a run
    // its door just bore, before anything of it is on record). `$1`
    // narrows every arm to one binary hash; NULL keeps them all.
    let query = format!(
        "SELECT DISTINCT running_binary_hash FROM project \
         WHERE running_binary_hash IS NOT NULL AND running_binary_hash <> '' \
           AND ($1::TEXT IS NULL OR running_binary_hash = $1) \
         UNION \
         SELECT DISTINCT binary_hash FROM run \
         WHERE state <> 'ended' AND binary_hash IS NOT NULL \
           AND ($1::TEXT IS NULL OR binary_hash = $1) \
         UNION \
         SELECT DISTINCT binary_hash FROM worker_lease l \
         WHERE {alive} AND binary_hash IS NOT NULL \
           AND ($1::TEXT IS NULL OR binary_hash = $1)",
        alive = weft_task_store::worker_alive!("l"),
    );
    let rows: Vec<(String,)> = if scope.covers_workers() {
        sqlx::query_as(&query).bind(scope.worker_hash()).fetch_all(&mut *conn).await?
    } else {
        Vec::new()
    };
    let mut infra_refs = std::collections::BTreeSet::new();
    // A worker image's re-check needs no infra row.
    if !scope.covers_infra() {
        return Ok(ReferencedImages { worker_hashes: rows.into_iter().map(|(h,)| h).collect(), infra_refs: Vec::new() });
    }
    // One row per project, each the COMPLETE per-node tag map written by
    // its last committed infra sync. Rows are small and few (one per
    // project), so decoding in Rust beats a JSON-path query nobody can
    // read. The canonical decode names the project in its error, so one
    // corrupt row among many is diagnosable, and a row that fails it is a
    // loud error, never a skipped keep-set entry (skipping it would
    // condemn images the supervisor may still apply). Blank refs are
    // skipped, mirroring the `<> ''` convention of the worker-hash arms
    // (a blank matches no image; keeping it would only launder a
    // buggy-writer blank into the set).
    let tag_rows: Vec<(uuid::Uuid, serde_json::Value)> =
        sqlx::query_as("SELECT id, infra_image_tags_json FROM project")
            .fetch_all(&mut *conn)
            .await?;
    for (project_id, tags) in tag_rows {
        let by_node = weft_broker_client::protocol::decode_infra_image_tags(
            tags,
            &format!("project={project_id}"),
        )?;
        for node_tags in by_node.values() {
            for tag in node_tags.values() {
                if !tag.is_empty() && scope.covers_infra_ref(tag) {
                    infra_refs.insert(tag.clone());
                }
            }
        }
    }
    // The refs live units actually run, recorded per unit on the
    // infra_node row at every apply (see `UnitRuntime::image_refs`).
    // Covers what the project-row map cannot: an UP unit frozen at an
    // older image across one or more syncs keeps running that ref until
    // it is force-stopped, so its old ref must stay referenced.
    // All rows, all units, regardless of status: over-keeping a ref is
    // bounded (rows die with the node on terminate) and the safe
    // direction for a set reclamation deletes against.
    let unit_rows: Vec<(uuid::Uuid, String, serde_json::Value)> =
        sqlx::query_as("SELECT project_id, node_id, units_json FROM infra_node")
            .fetch_all(&mut *conn)
            .await?;
    for (project_id, node_id, units) in unit_rows {
        let by_unit =
            weft_broker_client::protocol::decode_units_json(units, project_id, &node_id)?;
        for runtime in by_unit.into_values() {
            for image_ref in runtime.image_refs {
                if !image_ref.is_empty() && scope.covers_infra_ref(&image_ref) {
                    infra_refs.insert(image_ref);
                }
            }
        }
    }
    Ok(ReferencedImages {
        worker_hashes: rows.into_iter().map(|(h,)| h).collect(),
        infra_refs: infra_refs.into_iter().collect(),
    })
}

/// `GET /install`: what this install tells about itself (its addresses
/// and, on a cloud, what a project's CI deploys a frontend with). Behind
/// the admin surface's authentication, so `weft login` proves a key with
/// it.
pub async fn install(
    State(state): State<DispatcherState>,
    _caller: CallerTenant,
) -> Json<weft_core::install::InstallInfo> {
    Json(state.install_info.clone())
}

pub async fn list(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<Vec<ProjectSummary>>, (StatusCode, String)> {
    let items = state
        .projects
        .list(caller.0.as_str())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list projects: {e}")))?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(summary_of(&state, item).await?);
    }
    Ok(Json(out))
}

/// `POST /projects`: the project exists on this install, under the
/// caller's tenant, with nothing built yet. What a version needs before
/// its first build (`weft checkpoint` records one without building), and
/// the first thing a build does. Idempotent: an existing project of the
/// caller's is left as it is; one of another tenant answers as if it did
/// not exist.
pub async fn declare(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<DeclareRequest>,
) -> Result<Json<ProjectSummary>, (StatusCode, String)> {
    let summary = declare_project(&state, req.id, &req.name, &caller.0).await?;
    Ok(Json(summary_of(&state, summary).await?))
}

/// Make sure the project row exists under `tenant`: registered with an
/// empty program and no running hashes when it does not.
pub(crate) async fn declare_project(
    state: &DispatcherState,
    id: uuid::Uuid,
    name: &str,
    tenant: &crate::tenant::TenantId,
) -> Result<crate::project_store::StoredProjectSummary, (StatusCode, String)> {
    match state.projects.tenant_for(id).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant_for: {e}")))? {
        Some(owner) if owner == tenant.as_str() => {
            return state
                .projects
                .get(id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
                .ok_or((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()))?
                .unless_removing();
        }
        Some(_) => return Err((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string())),
        None => {}
    }
    let empty: ProjectDefinition = serde_json::from_value(serde_json::json!({ "id": id, "nodes": [], "edges": [] }))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("empty program: {e}")))?;
    let summary = state
        .projects
        .register_with_hashes(empty, name, "", tenant.as_str(), None, None, None, None, None, None, None)
        .await
        .map_err(|e| {
            if crate::project_store::ProjectRemoving::is(&e) {
                crate::project_store::ProjectRemoving::refusal()
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("declare the project: {e}"))
            }
        })?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "declaring the project registered nothing".to_string()))?;
    state
        .events
        .publish(DispatcherEvent::ProjectRegistered {
            project_id: summary.id,
            name: weft_core::truncate_user_string(&summary.name, 4096),
        })
        .await;
    Ok(summary)
}

/// `POST /projects/{id}/builds`: build this version of the project inside
/// the install (`crate::build`).
///
/// When every image the version needs is in the registry, register what
/// it produced and answer it (200, [`weft_core::builds::BuiltProgram`]):
/// the compiled program, its three hashes, the implementations its worker
/// carries, the infra images every place runs, and the version's files as
/// the program's source. The project's running pointers move to it in the
/// same transaction, so the next verb acts on exactly this build.
///
/// When images are missing, start their builds (or join the ones already
/// running for the same content), write the version down as waiting on
/// them, and answer at once (202, [`weft_core::builds::BuildsUnderway`]),
/// never after the minutes the builds take: a request held open that long
/// with nothing on the wire is dropped by an idle network path. The
/// install registers the version itself once they all ended
/// (`crate::build::waiting`); the caller follows it (`version_build`).
///
/// This is the ONE way a project's program and images come to exist on an
/// install: nothing the client computed is believed.
pub async fn build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(req): Json<weft_core::builds::VersionBuildRequest>,
) -> Result<axum::response::Response, axum::response::Response> {
    use axum::response::IntoResponse;
    declare_project(&state, id, &req.name, &caller.0).await.map_err(IntoResponse::into_response)?;
    crate::api::versions::validate_manifest(&req.manifest).map_err(IntoResponse::into_response)?;
    // Taken as the ask arrives, before the minutes of compiling: a
    // version registers only over an older ask, whichever finishes first
    // (`crate::build::waiting`).
    let ask = state
        .projects
        .next_build_ask(id)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("take the build's place in the project's order", e).into_response())?;
    // Claims every image the build plans from its first look at the
    // registry until the version registers, so no prune deletes an image
    // this build found or built before anything references it
    // (`crate::build::prune::ImageHold`).
    let hold = crate::build::prune::ImageHold::new(&state.pg_pool, id);
    let (version, hold) = match crate::transition::build_version_gated(&state, id, &caller.0, &req, ask, hold).await? {
        crate::build::VersionBuild::Waiting(underway) => {
            return Ok((StatusCode::ACCEPTED, Json(underway)).into_response());
        }
        crate::build::VersionBuild::Ready { version, hold } => (version, hold),
    };
    if let Err(e) = hold.confirm().await {
        let released = hold.release().await;
        let settled = crate::transition::settle(&state, Some(id)).await;
        let also: Vec<String> = [released.err(), settled.err()].into_iter().flatten().map(|e| format!("{e:#}")).collect();
        let mut message = format!("{e:#}");
        if !also.is_empty() {
            message.push_str(&format!("\n(and letting go of the build failed too: {})", also.join("; ")));
        }
        return Err((StatusCode::CONFLICT, message).into_response());
    }
    let registrar = crate::build::waiting::Registrar::Request { hold };
    match crate::build::waiting::register(&state, id, caller.0.as_str(), &version, registrar).await {
        Ok(Some(built)) => Ok(Json(built).into_response()),
        Ok(None) => Err((
            StatusCode::CONFLICT,
            "a newer build of this project registered while this one was compiling, so this one was not registered; \
             build again to run this version"
                .to_string(),
        )
            .into_response()),
        Err(e) if crate::build::waiting::ResyncHolds::is(&e) => Err((StatusCode::CONFLICT, format!("{e:#}")).into_response()),
        Err(e) => {
            let (status, mut message) = if crate::project_store::ProjectRemoving::is(&e) {
                crate::project_store::ProjectRemoving::refusal()
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("register the build: {e:#}"))
            };
            if let Err(settled) = crate::transition::settle(&state, Some(id)).await {
                message.push_str(&format!("\n(and settling the project failed too: {settled:#})"));
            }
            Err((status, message).into_response())
        }
    }
}

/// Answers through `StatusError` so "no project you may see under this
/// id" carries the `x-weft-not-found: project` marker. A client asking
/// whether a project is known yet (the CLI's checkpoint, before it
/// registers the source) must be able to tell that apart from a
/// version-skewed dispatcher missing the route.
pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<ProjectSummary>, StatusError> {
    authorize_project_marked(&state, &caller.0, id).await?;
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or(StatusError::NotMyProject)?;
    Ok(Json(summary_of(&state, summary).await?))
}

#[derive(Debug, Default, Deserialize)]
pub struct RemoveQuery {
    /// `weft rm --force` sets this. When true, the dispatcher skips
    /// the supervisor terminate-wait window and drops the project's
    /// infra rows straight away, and goes on past cloud leftovers it
    /// names. Use when the supervisor is wedged and the user wants the
    /// project gone NOW. It never goes past workers that cannot be
    /// killed: nothing is erased while one runs.
    #[serde(default)]
    pub force: bool,
}

pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    axum::extract::Query(query): axum::extract::Query<RemoveQuery>,
) -> Result<Json<weft_core::projects::ProjectRemoved>, StatusError> {
    // Marked gate, not `authorize_project`: rm is a delete, so the CLI
    // retries it idempotently and needs the `x-weft-not-found` marker
    // to treat "already gone" as the desired end state instead of an
    // error (a headerless 404 must still bubble as version skew).
    authorize_project_marked(&state, &caller.0, id).await?;
    // Marked first: no run of the project is born from here on, so the
    // take-down below reaches every run it will ever have. A failed `weft
    // rm` leaves it marked, refusing runs, until `weft rm` is run again.
    let marked = state
        .projects
        .mark_removing(id)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, format!("could not start removing the project: {e:#}; retry `weft rm`")))?;
    if !marked {
        // A concurrent rm dropped the row after our gate.
        return Err(StatusError::NotMyProject);
    }
    // Deactivate first: cancels any in-flight runs, unregisters
    // every wake signal (entry + resume) from the listener processes holding them,
    // drops entry tokens. If deactivate fails on a DB / listener
    // error we must abort: removing the project row while signals
    // remain in the listener leaves dangling registrations that
    // outlive their owning project.
    deactivate_project(&state, id).await?;
    send_last_calls(&state, id).await?;
    // The project's OWNING tenant is its `project.tenant_id`, the same source that
    // keyed everything of the project's that lives outside the project row: its
    // stored data, and the connections its nodes published. `tenant_router` is a
    // request-routing lookup (the default returns `local` for every project), NOT
    // the resource owner, so using it here would key those by the wrong tenant and
    // find nothing.
    //
    // Resolved ONCE, here, before anything starts deleting: every step below needs
    // it, and a lookup repeated after a step has already run could come back empty
    // and leave that step silently skipped.
    let tenant = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant_for: {e:#}")))?
        // The row vanished mid-remove (a concurrent rm won): that IS
        // rm's desired end state, so it gets the marker too.
        .ok_or(StatusError::NotMyProject)?;
    // Its workers, killed outright before anything of the project is
    // erased: one left alive would keep serving from what it remembers of
    // a project that is gone (the programs it holds as on record among
    // them). Not before the take-down above, which ends every run of the
    // project: a run still waking up could start a fresh worker after the
    // kill. `--force` does not get past this one: what is still running is
    // named, with how to stop it by hand.
    state.runner.kill_workers(tenant.as_str(), id).await.map_err(|e| StatusError::Other(
        StatusCode::BAD_GATEWAY,
        format!("the project's workers could not be stopped, so none of its data was erased: {e:#}; once they are stopped, run `weft rm` again"),
    ))?;
    // Its frontends: one whose service cannot be removed stops the
    // removal with everything else still whole (`--force` forgets it, and
    // the answer names what stays on the cloud).
    let mut left = crate::frontends::remove_project(&state, &tenant, id, query.force).await.map_err(|e| {
        (StatusCode::BAD_GATEWAY, format!("remove the project's frontends: {e:#}; retry `weft rm`, or `weft rm --force`"))
    })?;
    // What the platform keeps for the project once its workers are gone
    // (on a cloud, the account they ran as), the same way: what cannot be
    // removed stops the removal, unless `--force`, whose answer names
    // what stays.
    match state.runner.retire(tenant.as_str(), id).await {
        Ok(()) => {}
        Err(e) if query.force => left.push(format!("what the platform keeps for the project's workers: {e:#}")),
        Err(e) => {
            return Err(StatusError::Other(
                StatusCode::BAD_GATEWAY,
                format!("remove what the platform keeps for the project's workers: {e:#}; retry `weft rm`, or `weft rm --force`"),
            ))
        }
    }
    // Tear down infra: issues a supervisor terminate command, waits
    // up to 120s for completion (unless --force), then drops all
    // infra_* rows. MUST succeed: if any of the DB cascade writes fail,
    // the project row stays (so a retry replays cleanly).
    crate::api::infra::delete_project(&state, id, &tenant, query.force).await?;
    // Its images stop counting as anybody's builds: the ones no other
    // project names become unused, and the pass asked for here reclaims
    // them (`crate::build::prune::AfterBuildPrunes`).
    crate::build::ledger::forget_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    // Its version builds go too, a waiting one's claim with it.
    crate::build::waiting::forget_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    state.builder.prunes.request(&state, None);
    // Reclaim the project's instance-specific stored data through the ONE hook.
    // The default frees the project's `project/`-scoped runtime files; a reclaimer
    // that also stores project content elsewhere (a versioned project-editing
    // history) extends it. `shared/`-scoped runtime files are the owner's and are
    // deliberately NOT touched (they outlive the project). MUST run before the
    // project row is dropped: a row-cascade would otherwise erase the bookkeeping
    // this reclaim reads, stranding bytes. A failure aborts the rm (a retry
    // replays cleanly).
    state
        .project_reclaimer
        .reclaim(&state, tenant.as_str(), id)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                // `{e:#}` prints the full anyhow cause chain, so the real reason
                // (a SQL error, a ledger inconsistency, a missing tree) reaches the
                // caller instead of just the top wrapper.
                format!("could not reclaim the project's stored data: {e:#}; retry `weft rm`"),
            )
        })?;
    // The row and everything kept under it, in one transaction: its runs,
    // its programs and version tree, its signals and queued work, its
    // infra rows and lease. A failure removes nothing, so the person runs
    // `weft rm` again. Its domains go with the row, and the platform's
    // door follows the rows on its own (`crate::domains::drain_loop`).
    let removed = state
        .projects
        .remove(id)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, format!("could not remove the project: {e:#}; retry `weft rm`")))?;
    match removed {
        Some(signals) => {
            // The listener lets go of what it held for the project.
            state.listener.unregister_many(&signals).await;
            Ok(Json(weft_core::projects::ProjectRemoved { left }))
        }
        // A concurrent rm dropped the row after our gate: still rm's
        // desired end state, so it gets the marker.
        None => Err(StatusError::NotMyProject),
    }
}

/// The project's CURRENT definition as one coherent (hash, shape)
/// pair: read `running_definition_hash` once, then fetch the
/// definition recorded under THAT hash. Every run-starting path
/// MUST use this instead of pairing a live `project_json` read with a
/// separate hash read: a concurrent re-register between two separate
/// reads would journal hash B while the kick set was computed from
/// shape A, and the worker (which fetches the definition BY the
/// journaled hash) would fold kicks that may not exist in the shape
/// it actually runs.
pub(crate) async fn coherent_definition(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(weft_core::project::hash::ProgramIdentity, std::sync::Arc<ProjectDefinition>), (StatusCode, String)> {
    let running = state
        .projects
        .running_program(id)
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("running program identity: {e}"))
        })?
        .ok_or_else(|| {
            (
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "project {id} has no production code identity; build it (`weft build`, or any \
                     verb that runs it) before starting a run"
                ),
            )
        })?;
    let hash = &running.identity.definition_hash;
    let project = state
        .program(id, hash)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("recorded definition {hash}: {e:#}")))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "project {id} has no recorded definition for hash {hash}; the \
                     definition history must cover the running hash"
                ),
            )
        })?;
    let project = with_left_out(project, &running.left_out, running.worker_log.as_deref());
    Ok((running.identity, project))
}

/// `project` as its worker can run it: the node packages the worker's
/// build left out because they did not compile (`left_out`, recorded with
/// the registered worker, `project_store::ProgramCode`) added to its
/// broken parts, so a run, fire or setup whose cut uses one of their node
/// types is refused up front, naming the package, and every other cut
/// runs. Kept beside the definition rather than in it: the definition is
/// stored and cached under its hash, and a fixed node changes the worker,
/// never that hash.
fn with_left_out(
    project: std::sync::Arc<ProjectDefinition>,
    left_out: &[weft_core::builds::LeftOutPackage],
    log: Option<&str>,
) -> std::sync::Arc<ProjectDefinition> {
    if left_out.is_empty() {
        return project;
    }
    let mut runnable = (*project).clone();
    runnable.broken.extend(weft_core::builds::LeftOut::broken_parts(left_out, &project, log));
    std::sync::Arc::new(runnable)
}


/// For each `requiresInfra` node in the project, list the
/// `is_trigger` nodes that have it in their upstream closure.
///
/// Re-export from weft-core. The function lives there because both
/// the dispatcher (per-node safety check) and the broker
/// (supervisor_trigger_deps) need it; one definition, no clones.
pub use weft_core::project::compute_trigger_deps;

/// What a setup phase of `seeds` runs: its cut of `project` (everything
/// the seeds need, a trigger's inputs included) and the roots that cut
/// kicks, payload-less (`weft_core::run_spec::setup_cut`). `None` with
/// nothing to set up.
fn setup_cut(project: &ProjectDefinition, seeds: &[Located]) -> Result<Option<(ProjectDefinition, Vec<Kick>)>, String> {
    if seeds.is_empty() {
        return Ok(None);
    }
    weft_core::run_spec::setup_cut(project, seeds).map(Some)
}

/// What a setup run is recorded as started by: its first kick, or, when
/// every root it runs starts with the group around it (kicked as that group
/// opens, not at the start), the first place it runs.
fn setup_entry(project: &ProjectDefinition, cut: &ProjectDefinition, kicks: &[Kick]) -> String {
    match kicks.first() {
        Some(kick) => kick_place(project, kick),
        None => weft_core::project::places::running_places(cut)
            .into_iter()
            .next()
            .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
            .expect("a setup's cut runs the places it sets up"),
    }
}

/// The program a run the dispatcher starts runs: `cut`, a cut of the
/// registered program `registered` (`weft_core::project::cut`), as its
/// birth names it and its start keeps it.
pub(crate) fn runs_program(
    registered: &weft_core::project::hash::ProgramIdentity,
    cut: &ProjectDefinition,
) -> Result<weft_journal::record::RunsProgram, (StatusCode, String)> {
    weft_journal::record::RunsProgram::of(&registered.definition_hash, cut.id, cut)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the run's program: {e:#}")))
}

/// The place a kick fires at, spelled the way the program reads it
/// (`keep.db` for the db of a file included as `keep`): the node under
/// the call sites on the kick's frames. What a run is recorded as
/// started by, setup runs here and fire runs in `versions::run`, so
/// `weft runs` lists every run by a place and never by a
/// compiled id.
pub(crate) fn kick_place(project: &ProjectDefinition, kick: &Kick) -> String {
    let place = Located::at(&kick.node, &kick.frames);
    weft_core::project::address_of(project, &place.id, &place.path)
}

/// The InfraSetup runs of the project that have not ended: the durable
/// "infra sync in flight" state, cancellable through the per-run cancel and
/// visible to every dispatcher. A run that has not ended is always on its
/// way somewhere (queued for a worker, driven by one, or parked on a
/// wait; a worker that went away has its runs requeued or ended by the
/// lost-run sweep), so nothing here can be a setup nobody will advance.
/// Sync rejects while any exists (two concurrent syncs would race the
/// provisioning subworkflow); the infra-cancel verb interrupts them.
pub(crate) async fn infra_setup_run_ids(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies: `None` for any owner's setup, `Some(None)` for the
    // shared copies' setup, `Some(Some(m))` for instance m's.
    owner: Option<Option<&weft_core::instance::InstanceId>>,
) -> anyhow::Result<Vec<weft_core::RunId>> {
    Ok(sqlx::query_scalar(
        "SELECT run_id FROM run \
         WHERE project_id = $1 AND phase = 'infra_setup' AND state <> 'ended' \
           AND ($2 OR instance_id IS NOT DISTINCT FROM $3) \
         ORDER BY started_at, run_id",
    )
    .bind(project_id)
    .bind(owner.is_none())
    .bind(owner.flatten().map(|m| m.as_str()))
    .fetch_all(&state.pg_pool)
    .await?)
}

/// Whether an infra setup of `owner` (see [`infra_setup_run_ids`])
/// is in flight: one instance starting its copy does not hold another
/// instance's back.
pub(crate) async fn infra_setup_in_flight(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    owner: Option<Option<&weft_core::instance::InstanceId>>,
) -> anyhow::Result<bool> {
    Ok(!infra_setup_run_ids(state, project_id, owner).await?.is_empty())
}

/// A started InfraSetup sub-run: the run to await plus the
/// subscriptions opened BEFORE the enqueue (so the worker can't beat the
/// waiter to the terminal event): the project's events, and the signal
/// watch, which hears the run's ending (`weft_journal::RUN_ENDED_CHANNEL`:
/// the run is queued watched).
pub struct InfraSetupRun {
    pub(crate) run_id: weft_core::RunId,
    events: tokio::sync::broadcast::Receiver<crate::events::LiveEvent>,
    heard: weft_task_store::pg_signal::Subscription,
    project_id: uuid::Uuid,
}

impl InfraSetupRun {
    /// Follow a setup run already in flight (one a dispatcher that died
    /// mid-upgrade left behind), for [`await_infra_setup`] to wait out.
    pub(crate) async fn follow(state: &DispatcherState, project_id: uuid::Uuid, run_id: weft_core::RunId) -> Self {
        let heard = state.signals.subscribe();
        let events = state.events.subscribe_project(project_id).await;
        Self { run_id, events, heard, project_id }
    }
}

/// Queue a run of the program `program` started by the dispatcher (a setup
/// run), recording the program's source as the version it ran.
async fn start_queued_run(
    state: &DispatcherState,
    program: &weft_core::project::hash::ProgramIdentity,
    defaults: &weft_core::project::ProjectDefaults,
    mut birth: weft_journal::birth::Birth<'_>,
    queue_as: QueueAs<'_>,
) -> Result<(), (StatusCode, String)> {
    let source_version = super::versions::record_program_source(state, birth.project_id, program).await?;
    birth.source_version = Some(&source_version);
    start_queued_run_with(state, defaults, birth, &[], queue_as).await
}

/// What a run the dispatcher queues is born with beside its birth.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct QueueAs<'a> {
    /// A trigger setup an activation asked for (the activation is the
    /// setup's own run): queued only while that activation still
    /// owns its rows.
    pub for_activation: bool,
    /// Somebody waits on its ending, which then announces itself
    /// (`weft_journal::RUN_ENDED_CHANNEL`): an infra setup's waiter, or a
    /// trigger setup whose bake is the dispatcher's (`crate::run_ends`).
    pub watch_end: bool,
    /// What the version tree shows of a run started by hand: the places it
    /// ran again rather than inherit, what it was asked to run, and the
    /// saved example it came from.
    pub stale: &'a [String],
    pub spec: Option<&'a serde_json::Value>,
    pub example: Option<&'a str>,
}

/// THE one way the dispatcher starts a run (entrance 3): its birth (its
/// `RunStarted`, one `NodeKicked` per root) and `extra_rows` (birth
/// facts beyond the kicks: the log lines of the baked infra it reads),
/// written as its first record row with its `run` row, `queued`, and the
/// cut its birth names (`Birth::program`), in one transaction; delivery
/// hands it to a worker. Every
/// start path (`run`, trigger setup, infra setup) goes through here.
pub(crate) async fn start_queued_run_with(
    state: &DispatcherState,
    // The program's defaults (`weft.toml`): how long the run is kept when
    // what started it says nothing.
    defaults: &weft_core::project::ProjectDefaults,
    birth: weft_journal::birth::Birth<'_>,
    extra_rows: &[weft_journal::RunEvent],
    queue_as: QueueAs<'_>,
) -> Result<(), (StatusCode, String)> {
    let tenant = state
        .tenant_router
        .tenant_for_project(birth.project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let run_id = birth.run_id;
    let keep_for = birth.settings.kept_for(defaults.keep_for());
    let program = birth.program;
    let (start, kicks) = weft_journal::birth::birth_events(birth);
    let mut events = Vec::with_capacity(1 + kicks.len() + extra_rows.len());
    events.push(start);
    events.extend(kicks);
    events.extend_from_slice(extra_rows);
    let queued = weft_journal::record::Queued {
        events: &events,
        tenant: tenant.as_str(),
        keep_for,
        watch_end: queue_as.watch_end,
        stale: queue_as.stale,
        spec: queue_as.spec,
        example: queue_as.example,
        program: Some(program),
    };
    let inserted = state
        .journal
        .queue_run(queued, queue_as.for_activation)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("start run: {e:#}")))?;
    if !inserted {
        return Err((StatusCode::CONFLICT, format!("run {run_id} was started already")));
    }
    Ok(())
}

/// Is every infra node the project's TRIGGERS depend on Running?
///
/// The one rule joining the two lifetimes: a trigger reads the address
/// off the infra node feeding it, so arming it before that node is up
/// would register a signal against an address that does not answer.
/// Infra no trigger depends on is the RUN's to wait on and does not
/// hold an activation back.
///
/// Vacuously true when the triggers depend on none, which includes
/// every project without a trigger. A node with no row at all is not
/// Running: nothing was ever provisioned for it.
pub(crate) fn trigger_infra_ready(
    depends_on: &std::collections::BTreeSet<String>,
    rows: &[crate::infra_node::InfraNodeRow],
) -> bool {
    // The shared copies: the project's own triggers read those (a
    // instance's triggers are checked against the instance's copies when
    // they are activated).
    depends_on.iter().all(|id| {
        rows.iter().any(|r| {
            &r.node_id == id && r.instance.is_none() && r.status == crate::infra_node::InfraNodeStatus::Running
        })
    })
}

/// Is every infra node the source declares Running, which is what a
/// WHOLE-GRAPH run touches? The one definition of the fact the table
/// reports about run (`ActionInputs::run_infra_ready`).
pub(crate) fn run_infra_ready(has_infra: bool, infra_rollup: &str) -> bool {
    !has_infra || infra_rollup == "running"
}

/// The infra copies that are NOT currently Running (a Stopped, Failed,
/// Flaky or missing one is not running). Empty means every one is up. The
/// ONE definition of "is the infra up": a run's gate ([`require_run_infra`]),
/// arming triggers ([`require_trigger_infra`]) and activate's note about
/// idle infra all read it, each wording its own message.
///
/// `within` narrows the check to the named places: a targeted run only
/// touches its own upstream subgraph, and arming the triggers only touches
/// the infra they depend on, so infra outside either has no bearing on it
/// and must not gate it. `None` checks the whole project.
pub(crate) async fn missing_infra_nodes(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    within: Option<&HashSet<String>>,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<Vec<MissingCopy>, (StatusCode, String)> {
    // The project's copies as this dispatcher holds them (`crate::held`).
    let wanted = copies_read(project, within, instance);
    let missing_of = |copies: &[crate::infra_node::CopyStatus]| -> Vec<MissingCopy> {
        let up: Vec<weft_core::infra::run_gate::InfraCopyUp> = copies.iter().map(crate::infra_node::CopyStatus::up).collect();
        weft_core::infra::run_gate::missing_copies(&wanted, &up)
    };
    if wanted.iter().all(|(_, copy)| copy.is_none()) {
        return Ok(missing_of(&[]));
    }
    // A copy that came up a moment ago may not have been heard yet: a
    // refusal is made on the rows themselves.
    if let Some(held) = state.held.infra_status.held(&project_id) {
        let missing = missing_of(&held);
        if missing.iter().all(|m| m.needs_instance) {
            return Ok(missing);
        }
    }
    let fresh = state
        .held
        .infra_status
        .load_fresh(project_id, || crate::infra_node::statuses(&state.pg_pool, project_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e:#}")))?;
    Ok(missing_of(&fresh))
}


/// What the infra places a run for `instance` may read have saved
/// (`weft_core::infra::bake::saved_for_run`), from the project's copies
/// as this dispatcher holds them: a value saved a moment ago and not
/// heard yet leaves its node to run this time.
pub(crate) async fn saved_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<weft_core::infra::bake::Saved, (StatusCode, String)> {
    if project.nodes.iter().all(|node| node.baked_outputs.is_empty()) {
        return Ok(Default::default());
    }
    let copies = state
        .held
        .infra_status
        .get_or_load(project_id, || crate::infra_node::statuses(&state.pg_pool, project_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e:#}")))?;
    let up: Vec<weft_core::infra::run_gate::InfraCopyUp> = copies.iter().map(crate::infra_node::CopyStatus::up).collect();
    Ok(weft_core::infra::bake::saved_for_run(project, &up, instance))
}

/// What a run of `program` (its cut) for `instance` starts with: the
/// instance's stored values (with `overlay` made: the change a store call
/// is about to commit, which the setup it re-arms has to run with),
/// checked against the program by `weft_core::run_spec::instance_run_values`.
/// Answers a 422 carrying the refusal, the same shape every run refusal
/// has, naming every field the instance left unfilled or filled wrong.
pub(crate) async fn instance_values_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    program: &ProjectDefinition,
    instance: &weft_core::instance::InstanceId,
    overlay: Option<&weft_core::instance::ValueChanges>,
) -> Result<weft_core::instance::InstanceValues, (StatusCode, String)> {
    let stored = stored_instance_values(state, project_id, instance, overlay, Read::Held).await?;
    match weft_core::run_spec::instance_run_values(program, instance, &stored) {
        Ok(values) => Ok(values),
        // Refused on what this process kept: confirmed against the rows,
        // since a value stored a moment ago may not have been heard yet.
        Err(_) => {
            let stored = stored_instance_values(state, project_id, instance, overlay, Read::Fresh).await?;
            weft_core::run_spec::instance_run_values(program, instance, &stored).map_err(|refusal| refusal_error(&refusal))
        }
    }
}

/// Whether a read may answer from what this process keeps
/// (`crate::held::Held`), or reads the rows themselves: a refusal made on
/// what was kept is confirmed with a fresh read.
#[derive(Clone, Copy)]
enum Read {
    Held,
    Fresh,
}

/// What `instance` has stored, with `overlay` (a change about to be stored)
/// applied on top.
async fn stored_instance_values(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    instance: &weft_core::instance::InstanceId,
    overlay: Option<&weft_core::instance::ValueChanges>,
    read: Read,
) -> Result<weft_core::instance::InstanceValues, (StatusCode, String)> {
    let load = || async {
        let tenant = crate::instance_values::owning_tenant(state, project_id).await?;
        let rows = weft_access_store::instance_values(&state.pg_pool, &tenant, project_id, instance)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read instance values: {e:#}")))?;
        Ok::<_, (StatusCode, String)>(crate::held::OfTenant { tenant, rows })
    };
    let key = (project_id, instance.clone());
    let held = match read {
        Read::Held => state.held.instance_values.get_or_load(key, load).await?,
        Read::Fresh => state.held.instance_values.load_fresh(key, load).await?,
    };
    let mut stored = held.rows.clone();
    if let Some(change) = overlay {
        change.apply(&mut stored);
    }
    Ok(stored)
}

/// A run refusal as the API answers it: 422, the refusal as JSON.
pub(crate) fn refusal_error(refusal: &weft_core::run_spec::Refusal) -> (StatusCode, String) {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        serde_json::to_string(refusal).expect("a Refusal serializes"),
    )
}

/// THE infra gate on a run of `program` (its cut of the project), scoped
/// to the places it executes (`weft_core::infra::run_gate::copies_read`):
/// a run aimed at part of the graph waits on that part's infra alone, a
/// whole-graph run on all of it, and an instance's run on that instance's
/// copies of the per-instance ones. A run that reaches something per
/// instance and names no instance is told so first: which instance's
/// copies to check is what it has not said.
pub(crate) async fn require_run_infra(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    program: &ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<(), (StatusCode, String)> {
    weft_core::run_spec::refuse_instanceless(program, instance).map_err(|refusal| refusal_error(&refusal))?;
    let missing = missing_infra_nodes(state, project_id, program, None, instance).await?;
    if !missing.is_empty() {
        return Err((StatusCode::PRECONDITION_REQUIRED, infra_not_running(&missing)));
    }
    Ok(())
}


/// START the InfraSetup sub-run for every `requiresInfra` node
/// in the project: journal `RunStarted` + the upstream-closure
/// root kicks (so programmatic-infra patterns, text → compute →
/// infra, flow values into the infra body), then enqueue the execute
/// task. Returns `None` when the project has no infra nodes (nothing
/// to provision). Split from `await_infra_setup` so the caller (sync)
/// can perform the start under the short per-project transition lock
/// (serializing the flip) and do the unbounded wait OUTSIDE it.
pub async fn start_infra_setup(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies: an instance's copies of the per-instance nodes, or the
    // shared nodes.
    instance: Option<&weft_core::instance::InstanceId>,
    // Only these nodes (places); every one of the owner's kind when empty.
    nodes: &[String],
) -> Result<Option<InfraSetupRun>, (StatusCode, String)> {
    // One coherent (hash, shape) pair: the kicks below and the
    // journaled/enqueued hash must come from the SAME definition (see
    // `coherent_definition`).
    let (program, project) = coherent_definition(state, project_id).await?;
    let targeted = crate::api::infra::resolve_infra_nodes(&project, nodes, instance)?;
    let places: Vec<Located> = targeted
        .iter()
        .map(|spelled| {
            let (id, path) = weft_core::project::resolve_address(&project, spelled);
            Located::new(id, path)
        })
        .collect();
    let Some((cut, kicks)) = setup_cut(&project, &places).map_err(|error| (StatusCode::BAD_REQUEST, error))? else {
        return Ok(None);
    };
    let run_program = runs_program(&program, &cut)?;
    // An instance's copy is set up from what reaches it, which may be what
    // that instance provides.
    let instance_values = match instance {
        Some(instance) => instance_values_for_run(state, project_id, &cut, instance, None).await?,
        None => Default::default(),
    };
    let picks = picks_for_run(state, project_id, &cut).await?;
    let run_id = weft_core::new_run_id();

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to the
    // completion event.
    let heard = state.signals.subscribe();
    let events = state.events.subscribe_project(project_id).await;

    // Infra reconciliation already holds the project transition lock.
    let source_version = super::versions::record_program_source_locked(state, project_id, &program).await?;
    let entry_node = setup_entry(&project, &cut, &kicks);
    start_queued_run_with(
        state,
        &project.defaults,
        weft_journal::birth::Birth {
            run_id,
            project_id,
            phase: weft_core::context::Phase::InfraSetup,
            entry_node: &entry_node,
            kicks: &kicks,
            program: &run_program,
            // A setup is handed nothing and stands nothing in.
            given: &Default::default(),
            binary_hash: &program.binary_hash,
            seed: None,
            source_version: Some(&source_version),
            instance: instance.map(|instance| weft_journal::birth::RunFor { instance, values: &instance_values }),
            picks: &picks,
            fired_trigger: None,
            stand_in: None,
            settings: weft_core::run_settings::RunSettings::bookkeeping(),
            at_unix: crate::lease::now_unix() as u64,
        },
        &[],
        // Its waiter hears it end ([`await_infra_setup`]).
        QueueAs { watch_end: true, ..QueueAs::default() },
    )
    .await?;
    Ok(Some(InfraSetupRun { run_id, events, heard, project_id }))
}

/// Why a sync (its InfraSetup, then the landing) did not land.
#[derive(Debug)]
pub(crate) enum SyncNotLanded {
    /// The setup run was cancelled (an infra cancel, a `weft stop`):
    /// the state is what someone asked for.
    Cancelled(String),
    Failed(StatusCode, String),
}

impl From<(StatusCode, String)> for SyncNotLanded {
    fn from((code, message): (StatusCode, String)) -> Self {
        SyncNotLanded::Failed(code, message)
    }
}

impl From<SyncNotLanded> for (StatusCode, String) {
    fn from(end: SyncNotLanded) -> Self {
        match end {
            // 409, not 500: the state is exactly what was asked for;
            // per-node partial state stays visible for per-node
            // terminate/retry.
            SyncNotLanded::Cancelled(message) => (StatusCode::CONFLICT, message),
            SyncNotLanded::Failed(code, message) => (code, message),
        }
    }
}

impl From<SyncNotLanded> for StatusError {
    fn from(end: SyncNotLanded) -> Self {
        <(StatusCode, String)>::from(end).into()
    }
}

/// Wait for a started InfraSetup sub-run to settle.
/// How the infra setup `run_id` ended, as its journal says: `None`
/// while it is still in flight.
async fn setup_ended(
    state: &DispatcherState,
    run_id: weft_core::RunId,
) -> anyhow::Result<Option<Result<(), SyncNotLanded>>> {
    use crate::api::run::TerminalOutcome;
    Ok(crate::api::run::terminal_outcome(&state.pg_pool, run_id).await?.map(|outcome| match outcome {
        TerminalOutcome::Completed => Ok(()),
        TerminalOutcome::Cancelled => Err(SyncNotLanded::Cancelled("infra setup cancelled".into())),
        _ => Err(SyncNotLanded::Failed(StatusCode::INTERNAL_SERVER_ERROR, "infra setup failed".into())),
    }))
}

pub(crate) async fn await_infra_setup(
    state: &DispatcherState,
    run: InfraSetupRun,
) -> Result<(), SyncNotLanded> {
    let InfraSetupRun { run_id, mut events, mut heard, project_id } = run;
    // Wait for completion. NO deadline: the InfraSetup run runs
    // user-authored upstream nodes (text -> compute -> infra), and
    // user code may legitimately be slow; a hard cap would refuse
    // legitimate provisioning. Instead, a periodic breadcrumb keeps
    // the stuck-state legible in the dispatcher logs, and the user
    // can always cancel it (`weft infra cancel`) to unblock.
    let started = std::time::Instant::now();
    // The journal is authoritative, and the bus is not: a terminal event
    // raised while nothing was subscribed (a setup that ended before this
    // waiter followed it, a dispatcher restart mid-wait) never reaches the
    // bus arm below. So the journal is read now, when the run's ending is
    // announced (it is queued watched), and whenever the watch or the bus
    // may have missed something; never on a clock.
    let ended_now = || async {
        setup_ended(state, run_id)
            .await
            .map_err(|e| SyncNotLanded::Failed(StatusCode::INTERNAL_SERVER_ERROR, format!("infra setup terminal lookup: {e:#}")))
    };
    if let Some(landed) = ended_now().await? {
        return landed;
    }
    let mut breadcrumb = tokio::time::interval(std::time::Duration::from_secs(30));
    breadcrumb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    breadcrumb.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! {
            heard = heard.next() => {
                let look = match heard {
                    Ok(weft_task_store::pg_signal::Heard::Signal { channel, payload }) => {
                        channel == weft_journal::RUN_ENDED_CHANNEL && weft_journal::run_ended_of_payload(&payload).contains(&run_id)
                    }
                    Ok(weft_task_store::pg_signal::Heard::Recheck | weft_task_store::pg_signal::Heard::Resumed) => true,
                    Ok(weft_task_store::pg_signal::Heard::Lost) => false,
                    Err(e) => {
                        return Err(SyncNotLanded::Failed(StatusCode::INTERNAL_SERVER_ERROR, format!("the signal watch stopped during infra setup: {e:#}")));
                    }
                };
                if look {
                    if let Some(landed) = ended_now().await? {
                        return landed;
                    }
                }
            }
            _ = breadcrumb.tick() => {
                tracing::info!(
                    target: "weft_dispatcher::infra_setup",
                    project_id = %project_id,
                    run_id = %run_id,
                    elapsed_secs = started.elapsed().as_secs(),
                    "infra setup still running; waiting on the InfraSetup run \
                     (cancel it with `weft infra cancel` to unblock)"
                );
            }
            res = events.recv() => {
                match res.map(|record| record.event) {
                    Ok(crate::events::DispatcherEvent::RunCompleted { run_id: c, .. })
                        if c == run_id => return Ok(()),
                    Ok(crate::events::DispatcherEvent::RunFailed { run_id: c, error, .. })
                        if c == run_id => {
                        return Err(SyncNotLanded::Failed(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("infra setup failed: {error}"),
                        ));
                    }
                    Ok(crate::events::DispatcherEvent::RunCancelled { run_id: c, reason, .. })
                        if c == run_id => {
                        // The user's infra-cancel (or a per-run stop)
                        // interrupted the provisioning run.
                        return Err(SyncNotLanded::Cancelled(format!("infra setup cancelled: {reason}")));
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // The bus dropped a batch that may have held this
                        // run's terminal event. The journal is
                        // authoritative: re-query it rather than wait
                        // blind (which would spuriously time out even
                        // though infra setup already finished).
                        if let Some(landed) = ended_now().await? {
                            return landed;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(SyncNotLanded::Failed(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "event bus closed during infra setup".into(),
                        ));
                    }
                }
            }
        }
    }
}

/// The project's entries that refused calls in this minute and the one
/// before.
async fn limited_entries(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<LimitedEntry>> {
    let entries: Vec<(String, String)> = sqlx::query_as(
        "SELECT token, node_id FROM signal \
         WHERE project_id = $1 AND surface_kind = 'public_entry' AND NOT is_resume",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await?;
    let now = crate::lease::now_unix();
    let mut out = Vec::new();
    for (token, node) in entries {
        for (reason, refused) in crate::entry_limits::recent_refusals(&state.pg_pool, project_id, &token, now).await? {
            out.push(LimitedEntry { node: node.clone(), limit: reason.describe().to_string(), refused });
        }
    }
    Ok(out)
}

/// weft's own work the project's work waits on that keeps failing: every
/// role weft cannot wake, every loop of weft's that gave up, stored files
/// the bucket would not remove, the project's workers, infra, infra
/// commands, repairs and triggers kept past their hibernation, a resync of
/// it that stopped with triggers down, and the triggers a resync a person
/// gave up left down, each said in a person's words.
pub(crate) async fn unanswered_for(state: &DispatcherState, project_id: uuid::Uuid) -> Result<Vec<weft_core::projects::Unanswered>, (StatusCode, String)> {
    let failing = weft_task_store::unanswered::failing_for(&state.pg_pool, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("work that keeps failing: {e:#}")))?;
    // A resync that stopped with triggers down, read off the project's own
    // plan of it.
    let resync = state
        .projects
        .resync(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the project's resync: {e:#}")))?;
    let stopped = resync.and_then(|held| {
        let error = held.standing_still()?;
        let now = crate::lease::now_unix();
        let at = held.plan.stopped.as_ref().map_or(now, |stopped| stopped.at_unix);
        let by_itself = match held.plan.stopped.as_ref().and_then(|stopped| stopped.waits_for) {
            Some(crate::task_kinds::resync::ResyncWait::Infra) => "it goes on by itself once the infra those triggers read runs; ",
            Some(crate::task_kinds::resync::ResyncWait::Activation) => "it goes on by itself once that activation has landed; ",
            None => "",
        };
        Some(weft_core::projects::Unanswered {
            what: "a resync of this project stopped with triggers it took down still down; builds wait for it".to_string(),
            then: format!(
                "{by_itself}`weft resync --resume` tries it again where it stopped; `weft cancel-activate` gives it up and leaves those triggers down"
            ),
            error,
            since_unix: at,
            last_unix: at,
            as_of_unix: now,
        })
    });
    let given_up = given_up_resync_left_down(state, project_id).await?;
    Ok(failing
        .into_iter()
        .map(|f| weft_core::projects::Unanswered {
            what: f.what.what(f.label.as_deref()),
            then: f.what.then().to_string(),
            error: f.error,
            since_unix: f.since_ms / 1000,
            last_unix: f.last_ms / 1000,
            as_of_unix: f.as_of_ms / 1000,
        })
        .chain(stopped)
        .chain(given_up)
        .collect())
}

/// The triggers a resync of the project a person gave up left down, that
/// nobody activated or took down since and that the program still has,
/// said in a person's words; `None` when its last resync was not given
/// up, or no such trigger is left.
async fn given_up_resync_left_down(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<Option<weft_core::projects::Unanswered>, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let Some(given_up) = state.projects.given_up_resync(project_id).await.map_err(|e| internal("the project's given-up resync", e))? else {
        return Ok(None);
    };
    if given_up.left_down.is_empty() {
        return Ok(None);
    }
    let project = state
        .projects
        .project(project_id)
        .await
        .map_err(|e| internal("the project", e))?
        .ok_or_else(|| internal("the project", anyhow::anyhow!("project {project_id} has no registered program")))?;
    let down = given_up.still_down(&project);
    if down.is_empty() {
        return Ok(None);
    }
    let down: Vec<String> = down
        .iter()
        .map(|owner| format!("{} ({})", weft_core::deactivation::whose_triggers(&owner.owner), owner.triggers.join(", ")))
        .collect();
    Ok(Some(weft_core::projects::Unanswered {
        what: format!("a given-up resync left these triggers down: {}", down.join("; ")),
        then: "`weft activate` turns them on; a trigger leaves this once it is activated or deactivated, and it all clears when the next resync begins".to_string(),
        error: "the resync was given up with `weft cancel-activate` before it brought them back".to_string(),
        since_unix: given_up.at_unix,
        last_unix: given_up.at_unix,
        as_of_unix: crate::lease::now_unix(),
    }))
}

/// The runs each of the project's triggers started in this minute and the
/// one before, named by the trigger's node.
async fn trigger_runs(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<weft_core::projects::TriggerRuns>> {
    let counts = crate::entry_limits::recent_runs(&state.pg_pool, project_id, crate::lease::now_unix()).await?;
    if counts.is_empty() {
        return Ok(Vec::new());
    }
    let triggers: Vec<(String, String)> = sqlx::query_as("SELECT token, node_id FROM signal WHERE project_id = $1 AND NOT is_resume")
        .bind(project_id)
        .fetch_all(&state.pg_pool)
        .await?;
    let mut runs: Vec<weft_core::projects::TriggerRuns> = triggers
        .into_iter()
        .filter_map(|(token, node)| counts.get(&token).map(|&(started, failed)| weft_core::projects::TriggerRuns { node, started, failed }))
        .collect();
    runs.sort_by(|a, b| a.node.cmp(&b.node));
    Ok(runs)
}

/// Error envelope for project-scoped handlers whose 404 must be
/// machine-readable (`status`, `remove`). Most arms are a plain
/// `(StatusCode, String)` (via `From`, so existing `.map_err`
/// closures are unchanged); the one special case is `NotMyProject`,
/// which renders a 404 carrying the `x-weft-not-found: project`
/// marker header, so a client can tell "no project I may see under
/// this id" apart from a bare routing 404 (e.g. a version-skewed
/// dispatcher missing the route), which must bubble as an error.
/// `remove` needs it so the CLI's `delete_idempotent` can treat
/// "already gone" as rm's desired end state on a retry.
///
/// `NotMyProject` covers BOTH "no such project" AND "exists but owned by
/// another tenant", with the SAME response. Collapsing them is the
/// no-existence-leak property: an authenticated caller probing another
/// tenant's project id cannot distinguish "exists but not yours" from
/// "does not exist" (both get 404 + the marker).
pub enum StatusError {
    /// No project the caller may run under this id: either no row, or a row
    /// owned by another tenant. Marker header set; the two are
    /// indistinguishable on the wire (no existence leak).
    NotMyProject,
    /// Triggers are on and the request did not say how they come down:
    /// 428 with [`weft_core::TRIGGER_CHOICE_REQUIRED_HEADER`], so a client
    /// tells it apart from the 428 for infra that is not running and asks
    /// the person (or names the flags) instead of failing on it.
    NeedsTriggerChoice(String),
    /// Any other failure: status + body, no marker.
    Other(StatusCode, String),
}

impl From<(StatusCode, String)> for StatusError {
    fn from((code, msg): (StatusCode, String)) -> Self {
        StatusError::Other(code, msg)
    }
}

impl StatusError {
    /// The status and message, for an in-process caller that logs a
    /// refusal rather than answering it over HTTP.
    pub fn into_parts(self) -> (StatusCode, String) {
        match self {
            StatusError::NotMyProject => (StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()),
            StatusError::NeedsTriggerChoice(msg) => (StatusCode::PRECONDITION_REQUIRED, msg),
            StatusError::Other(code, msg) => (code, msg),
        }
    }
}

impl IntoResponse for StatusError {
    fn into_response(self) -> Response {
        match self {
            StatusError::NotMyProject => (
                StatusCode::NOT_FOUND,
                [("x-weft-not-found", "project")],
                crate::authenticator::NO_SUCH_PROJECT,
            )
                .into_response(),
            StatusError::NeedsTriggerChoice(msg) => (
                StatusCode::PRECONDITION_REQUIRED,
                [(weft_core::TRIGGER_CHOICE_REQUIRED_HEADER, "triggerDeactivation")],
                msg,
            )
                .into_response(),
            StatusError::Other(code, msg) => (code, msg).into_response(),
        }
    }
}

/// Ownership gate for marked handlers: `authorize_project` (the ONE
/// ownership lookup, existence-leak collapse included) with its 404
/// upgraded to `NotMyProject` so the response carries the
/// `x-weft-not-found: project` marker instead of a headerless 404 the
/// client cannot tell apart from a version-skew routing 404.
async fn authorize_project_marked(
    state: &DispatcherState,
    caller: &crate::tenant::TenantId,
    id: uuid::Uuid,
) -> Result<(), StatusError> {
    authorize_project(state, caller, id).await.map_err(|(status, msg)| {
        if status == StatusCode::NOT_FOUND {
            StatusError::NotMyProject
        } else {
            (status, msg).into()
        }
    })
}

/// Aggregate view for `weft status`: registration, listener state,
/// per-node infra state, a rollup of recent runs, drift (when the
/// desired hashes ride the query), and the verbs valid right now. One
/// answer, no stitching required by a client.
pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Result<Json<ProjectStatusResponse>, StatusError> {
    // The `x-weft-not-found: project` marker lets a client tell "no
    // project I may see under this id" apart from a version-skew
    // routing 404 it must bubble. We must NOT call
    // `authorize_project` first: it returns a headerless 404, which would
    // make the marker unreachable and a brand-new `weft run` could never
    // pass the gate. So resolve ownership through the marked gate.
    authorize_project_marked(&state, &caller.0, id).await?;
    // A project `weft rm` stopped partway through removing is half taken
    // down: its status says so (`removing`), describes what is left, and
    // offers no verb.
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or(StatusError::NotMyProject)?;
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        // A registered project missing its definition is a real
        // inconsistency, NOT "no such project": Other (no marker), so
        // the CLI bubbles it loudly instead of skipping the gate.
        .ok_or((StatusCode::NOT_FOUND, "project definition missing".to_string()))?;
    let listener_running = crate::listener::project_is_listening(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("listener status: {e}")))?;

    // The verbs and what they would decide about, for the triggers the
    // query names (the ones a verb is about to act on), else the
    // program's own.
    let scope = query.scope();
    let snapshot = gather_action_snapshot(&state, id, &project, scope.as_ref()).await?;
    let (infra, instance_infra) = infra_entries(&project, &snapshot.infra);
    let waiting = crate::api::signal::waiting_fires(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("waiting fires: {e:#}")))?;
    let has_infra = snapshot.has_infra;
    let has_triggers = snapshot.has_triggers;
    let infra_rollup = snapshot.infra_rollup.clone();

    // Project-filtered, newest first: `total` is the true count of the
    // program's own runs (from SQL, not capped at a fetched window), the
    // set `weft tree` lists, and the first row is the latest for the
    // `last_*` fields. The runs that set it up are counted beside them.
    let runs_of = |phase: Option<weft_core::context::Phase>| crate::journal::RunListQuery {
        limit: 1,
        offset: 0,
        project_id: Some(id),
        started_after: None,
        started_before: None,
        phase,
        entry_node: None,
        status: None,
        instance: None,
        tag: None,
        node: None,
        search: None,
        below: None,
    };
    let (program_runs, every_run) = (runs_of(Some(weft_core::context::Phase::Fire)), runs_of(None));
    let (runs, every) = tokio::try_join!(
        state.journal.list_runs(caller.0.as_str(), &program_runs),
        state.journal.list_runs(caller.0.as_str(), &every_run),
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let last = runs.runs.first();
    let running = state
        .journal
        .going_run_ids_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("runs going: {e}")))?;
    // Oldest first, so the LAST is the most recently started. The
    // editor's action bar follows "the latest run" and nothing else on
    // the wire says which that is; sorting these by id, as this used
    // to, made "latest" mean whichever uuid happened to sort highest.
    let running: Vec<GoingRun> = running
        .into_iter()
        .map(|(run_id, phase)| GoingRun { run_id: run_id.to_string(), phase })
        .collect();
    let runs = ProjectRunsSummary {
        total: runs.total as usize,
        setup: every.total.saturating_sub(runs.total) as usize,
        last_completed_at: last.and_then(|l| l.completed_at),
        last_run_id: last.map(|l| l.run_id.to_string()),
        last_status: last.map(|l| l.status),
        running,
    };

    let binary_hash = state
        .projects
        .running_binary_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_binary_hash: {e}")))?;
    let definition_hash = state
        .projects
        .running_definition_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_definition_hash: {e}")))?;
    let infra_hash = state
        .projects
        .running_infra_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_infra_hash: {e}")))?;
    let mut drift = compute_drift(
        &query,
        binary_hash.as_deref(),
        definition_hash.as_deref(),
        infra_hash.as_deref(),
    );
    // The listeners fire an older program when any live activation, the
    // program's own or an instance's, was set up on code other than what is
    // registered now: a plain `weft resync` brings every one of them up.
    drift.activation_drift = snapshot
        .activations
        .iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .any(|a| activation_has_drifted(&query, binary_hash.as_deref(), definition_hash.as_deref(), a.program.as_ref(), drift.definition_drift));
    let available_actions = compute_available_actions(&ActionInputs {
        removing: summary.removing,
        lifecycle: &snapshot.lifecycle,
        partly_down: snapshot.partly_down,
        transition: snapshot.transition,
        resync_stopped: snapshot.resync_stopped,
        has_triggers,
        has_infra,
        trigger_infra_ready: snapshot.trigger_infra_ready,
        run_infra_ready: run_infra_ready(has_infra, &infra_rollup),
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup: &infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift: &drift,
        preservation: &snapshot.preservation,
        running_count: snapshot.running_count,
    });

    Ok(Json(ProjectStatusResponse {
        id,
        name: summary.name,
        removing: summary.removing,
        status: snapshot.lifecycle.status,
        transition: snapshot.transition,
        mode: snapshot.lifecycle.mode(),
        fires_deadline_unix: snapshot.lifecycle.fires_deadline_unix,
        running_count: snapshot.running_count,
        listener_running,
        has_triggers,
        infra,
        runs,
        has_infra,
        built: binary_hash.is_some(),
        builds: crate::build::ledger::running(&state.pg_pool, Some(id))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running builds: {e:#}")))?
            .into_iter()
            // A build not made on the builder yet has no id there to name.
            .filter_map(|b| {
                Some(weft_core::projects::BuildInFlight {
                    image: b.image_ref,
                    build: b.builder_id?,
                    started_at_unix: b.started_at,
                    log_url: b.log_url,
                })
            })
            .collect(),
        address: project_address(&state, id).await,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift,
        available_actions,
        preservation: snapshot.preservation,
        instance_infra,
        activations: snapshot
            .activations
            .iter()
            .map(|a| ActivationEntry {
                trigger: a.key.trigger.clone(),
                instance: a.key.instance().cloned(),
                status: a.lifecycle.status,
                mode: a.lifecycle.mode(),
                waiting: waiting.get(&a.key).cloned(),
                version: a.source_version.clone(),
            })
            .collect(),
        limited: limited_entries(&state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit refusals: {e}")))?,
        trigger_runs: trigger_runs(&state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("run counts: {e}")))?,
        unanswered: unanswered_for(&state, id).await?,
    }))
}

/// The status response's two infra lists, from every copy as the readers
/// show it: one entry per infra place the program declares (a shared one
/// with its copy's state, `not_started` when it has none; a `@per_instance`
/// one as `per_instance` with its instances' copies counted), and every
/// instance's copy on its own. A copy whose place the source no longer
/// declares (an orphan) is left out of both: it is counted project-wide
/// (`orphaned_infra`, the rollup) so the infra controls never vanish while
/// it lives.
fn infra_entries(
    project: &ProjectDefinition,
    copies: &crate::infra_node::ObservedCopies,
) -> (Vec<ProjectInfraEntry>, Vec<InstanceInfraEntry>) {
    let mut instance_infra: Vec<InstanceInfraEntry> = copies
        .rows
        .iter()
        .filter_map(|row| {
            Some(InstanceInfraEntry {
                node: row.node_id.clone(),
                instance: row.instance.clone()?,
                status: row.status.as_str().to_string(),
                progress: copies.progress_of(&row.node_id, row.instance.as_ref()),
            })
        })
        .chain(copies.starting.iter().filter_map(|(node, instance)| {
            Some(InstanceInfraEntry {
                node: node.clone(),
                instance: instance.clone()?,
                status: crate::infra_node::InfraNodeStatus::Provisioning.as_str().to_string(),
                progress: copies.progress_of(node, instance.as_ref()),
            })
        }))
        .collect();
    instance_infra.sort_by(|a, b| (&a.node, &a.instance).cmp(&(&b.node, &b.instance)));
    let mut infra = Vec::new();
    for spelled in weft_core::project::infra_place_spellings(project) {
        let (node_id, _) = weft_core::project::resolve_address(project, &spelled);
        let Some(node) = project.nodes.iter().find(|n| n.id == node_id && n.requires_infra) else {
            continue;
        };
        let entry = |status: String| ProjectInfraEntry {
            node: spelled.clone(),
            node_type: node.node_type.clone(),
            status,
            endpoint_url: None,
            failure_stage: None,
            failure_message: None,
            instance_copy_count: None,
            progress: None,
        };
        if node.per_instance.is_some() {
            infra.push(ProjectInfraEntry {
                instance_copy_count: Some(instance_infra.iter().filter(|c| c.node == spelled).count()),
                ..entry(INFRA_PER_INSTANCE.to_string())
            });
            continue;
        }
        match copies.rows.iter().find(|r| r.node_id == spelled && r.instance.is_none()) {
            Some(row) => infra.push(ProjectInfraEntry {
                // Coarse UI hint: first endpoint by name (BTreeMap = stable).
                endpoint_url: row.install_endpoints.values().next().cloned(),
                failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
                failure_message: row.failure_message.clone(),
                progress: copies.progress_of(&spelled, None),
                ..entry(row.status.as_str().to_string())
            }),
            None => infra.push(match copies.status_of(&spelled, None) {
                Some(status) => ProjectInfraEntry {
                    progress: copies.progress_of(&spelled, None),
                    ..entry(status.as_str().to_string())
                },
                None => entry(INFRA_NOT_STARTED.to_string()),
            }),
        }
    }
    (infra, instance_infra)
}

/// Every reconciliation input gathered from live state, shared by the
/// status handler (renders the list) and `require_action` (enforces
/// against the same list). One producer so the two can never disagree.
pub(crate) struct ActionSnapshot {
    /// The project's shared activations as one lifecycle
    /// (`activation_store::aggregate`), or the named scope's when the
    /// snapshot was taken for a verb aimed at some triggers.
    pub lifecycle: crate::activation_store::ActivationLifecycle,
    /// Whether some of those activations are on and others off (an infra
    /// stop took down only the triggers reading it): the aggregate reads
    /// active, and activate still has triggers to turn on.
    pub partly_down: bool,
    /// Every activation row, shared and per instance.
    pub activations: Vec<crate::activation_store::Activation>,
    pub transition: crate::project_store::ProjectTransition,
    /// The project's resync stopped with triggers down, nothing driving it
    /// (`crate::task_kinds::resync::ResyncState::standing_still`).
    pub resync_stopped: bool,
    pub has_triggers: bool,
    pub has_infra: bool,
    /// Every shared infra node the project's shared TRIGGERS depend on is
    /// Running, walked from the REGISTERED definition (see
    /// `ActionInputs::trigger_infra_ready` for what reads it).
    pub trigger_infra_ready: bool,
    pub orphaned_infra: bool,
    pub infra_rollup: String,
    pub infra_busy: bool,
    /// Every copy, as every status reader shows it.
    pub infra: crate::infra_node::ObservedCopies,
    pub preservation: PreservationCounts,
    pub running_count: usize,
}

pub(crate) async fn gather_action_snapshot(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    scope: Option<&weft_core::activation::ActivationScope>,
) -> Result<ActionSnapshot, (StatusCode, String)> {
    let activations = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    // What the verb is aimed at: the scope's activations, or every
    // shared one (what the action bar acts on). A scope that no longer
    // resolves against the registered definition (the source moved on)
    // is judged by what is recorded under its owner, and the verb's own
    // resolve against the built definition refuses it properly.
    let in_scope = |key: &weft_core::activation::ActivationKey| match scope {
        None => key.owner == weft_core::instance::Owner::Shared,
        Some(scope) => match scope.resolve(project) {
            Ok(keys) => keys.contains(key),
            Err(_) => key.owner == scope.owner(),
        },
    };
    let lifecycle = crate::activation_store::aggregate(
        activations.iter().filter(|a| in_scope(&a.key)).map(|a| &a.lifecycle),
    );
    let partly_down = lifecycle.status == crate::project_store::ProjectStatus::Active
        && activations.iter().filter(|a| in_scope(&a.key)).any(|a| is_down(&a.lifecycle));
    let transition = state
        .projects
        .transition(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("transition: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let resync_stopped = match transition {
        crate::project_store::ProjectTransition::Resyncing => state
            .projects
            .resync(id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the project's resync: {e:#}")))?
            .is_some_and(|held| held.standing_still().is_some()),
        _ => false,
    };
    // Every copy as every reader shows it: a start, stop or terminate
    // under way reads as such before the supervisor reaches the row.
    let observed = crate::infra_node::observe(&state.pg_pool, id, project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra copies: {e:#}")))?;
    let infra_rows = &observed.rows;

    // Every SHARED infra place the source declares, spelled the way its
    // row is keyed (a file included twice declares two placements): the
    // project's infra, which the action bar starts and stops. An instance's
    // copies are their program's to run; they count here only when they
    // are orphans, so live infra never vanishes from view.
    // SYNC: shared roles <-> packages/weft-graph/src/webview/lib/utils/node-roles.ts projectHasInfra, projectHasTriggers
    // SYNC: shared roles <-> crates/weft-compiler/src/weft_compiler.rs collect_included_contents
    let declared = weft_core::project::DeclaredInfra::of(project);
    let source_infra: std::collections::BTreeSet<String> = weft_core::project::infra_place_spellings(project)
        .into_iter()
        .filter(|n| declared.declares(n, false))
        .collect();
    let has_infra = !source_infra.is_empty();
    // The program's own triggers, which the action bar arms, and the
    // shared infra they read: an instance's triggers read that instance's
    // copies, which have no shared row, and are armed per instance.
    let shared_triggers: Vec<Located> = weft_core::project::trigger_places(project)
        .into_iter()
        .filter(|place| !weft_core::project::is_per_instance(project, &place.id))
        .collect();
    let has_triggers = !shared_triggers.is_empty();
    let shared_trigger_infra: std::collections::BTreeSet<String> =
        weft_core::project::cut::walk_bounds(&weft_core::project::graph::ProjectGraph::new(project), &weft_core::project::cut::Bounds::setup(&shared_triggers))
            .runs
            .iter()
            .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
            .filter(|spelled| source_infra.contains(spelled))
            .collect();
    let trigger_infra_ready = trigger_infra_ready(&shared_trigger_infra, infra_rows);
    // Live rows the source does not declare: the user deleted the node
    // from source, or it sits behind a part that does not compile
    // (`DeclaredInfra::gone` tells the two apart, for what takes copies
    // down on its own). Either way they count into the rollup (below) and
    // raise the project-level signal, so live infra the source does not
    // show never collapses the rollup to `none` and never makes the infra
    // controls vanish (Model 1's never-lose-track guarantee): what to do
    // with them is the person's call.
    let is_undeclared = |r: &crate::infra_node::InfraNodeRow| !declared.declares(&r.node_id, r.instance.is_some());
    let undeclared_count = infra_rows.iter().filter(|r| is_undeclared(r)).count();
    let orphaned_infra = undeclared_count > 0;

    // Aggregate infra state across SOURCE nodes + orphan rows:
    //   - none:    no source infra node and no live row.
    //   - running: every counted slot is Running.
    //   - stopped: every counted slot is Stopped or absent.
    //   - partial: mixed (some running, some not, no failures).
    //   - failed:  at least one Failed.
    //   - flaky:   at least one Flaky and the rest Running.
    // Transient states take precedence (terminating > stopping >
    // provisioning) so the action bar shows the in-flight state and
    // offers only its cancel.
    let total = source_infra.len() + undeclared_count;
    let infra_rollup = if total == 0 {
        "none".to_string()
    } else {
        use crate::infra_node::InfraNodeStatus;
        let mut running = 0usize;
        let mut stopped = 0usize;
        let mut absent = 0usize; // source node never provisioned OR terminated
        let mut failed = 0usize;
        let mut flaky = 0usize;
        let mut stopping = 0usize;
        let mut terminating = 0usize;
        let mut provisioning = 0usize;
        // One slot per source node (absent counted) + one per undeclared
        // row (it always HAS a row, by definition).
        let mut count_status = |status: InfraNodeStatus| match status {
            InfraNodeStatus::Running => running += 1,
            InfraNodeStatus::Failed => failed += 1,
            InfraNodeStatus::Flaky => flaky += 1,
            InfraNodeStatus::Stopping => stopping += 1,
            InfraNodeStatus::Terminating => terminating += 1,
            InfraNodeStatus::Provisioning => provisioning += 1,
            InfraNodeStatus::Stopped => stopped += 1,
        };
        for spelled in &source_infra {
            match observed.status_of(spelled, None) {
                Some(status) => count_status(status),
                // No row and nothing starting one: never provisioned OR
                // terminated (terminate removes the row). Nothing lives
                // in the namespace for this node.
                None => absent += 1,
            }
        }
        for r in infra_rows.iter().filter(|r| is_undeclared(r)) {
            count_status(r.status);
        }
        if terminating > 0 {
            "terminating".to_string()
        } else if stopping > 0 {
            "stopping".to_string()
        } else if provisioning > 0 {
            "provisioning".to_string()
        } else if failed > 0 {
            "failed".to_string()
        } else if flaky > 0 && running + flaky == total {
            "flaky".to_string()
        } else if running == total {
            "running".to_string()
        } else if absent == total {
            "none".to_string()
        } else if stopped + absent == total {
            // Some stopped + some never-provisioned. Pragmatic bucket
            // as `stopped`: the user can re-start, and Terminate still
            // makes sense for the actually-stopped subset.
            "stopped".to_string()
        } else {
            "partial".to_string()
        }
    };

    // Infra-op-in-flight fact the rollup can't always see (a claimed
    // stop mid-drain; a provisioning run before any row flips).
    let infra_busy = crate::infra_lifecycle_command::any_in_flight(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("commands in flight: {e}")))?
        || infra_setup_in_flight(state, id, Some(None))
            .await
            .map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}"))
            })?;

    // What the verb's triggers that are off kept: what a reactivate's
    // choice decides about, and nothing else (a run started by hand, a
    // trigger still on).
    let off: Vec<weft_core::activation::ActivationKey> =
        activations.iter().filter(|a| in_scope(&a.key) && is_down(&a.lifecycle)).map(|a| a.key.clone()).collect();
    let preservation = match off.is_empty() {
        true => PreservationCounts::default(),
        false => crate::journal::postgres::activation_preservation(&state.pg_pool, id, &off)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?,
    };
    let running_now = state
        .journal
        .going_run_ids_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("runs going: {e}")))?
        .len();
    Ok(ActionSnapshot {
        lifecycle,
        partly_down,
        activations,
        transition,
        resync_stopped,
        has_triggers,
        has_infra,
        trigger_infra_ready,
        orphaned_infra,
        infra_rollup,
        infra_busy,
        infra: observed,
        preservation,
        running_count: running_now,
    })
}

/// Whether an activation's triggers are off: never turned on, or taken
/// down (whatever it keeps of their fires).
fn is_down(lifecycle: &crate::activation_store::ActivationLifecycle) -> bool {
    matches!(lifecycle.status, crate::project_store::ProjectStatus::Registered | crate::project_store::ProjectStatus::Inactive)
}

/// Enforce a verb against the SAME reconciliation the status handler
/// renders: the greyed-out button prevents the common case, this
/// rejection is the race safety net (a stale tab whose bar hasn't
/// refreshed). `verbs` is the acceptable set for the handler (some
/// verbs are aliases of one endpoint, e.g. activate serves activate /
/// reactivate / resume_active). Drift bits are client-side facts, so
/// enforcement treats them as set (drift-gated verbs are never
/// spuriously rejected here; their own handlers validate further).
pub(crate) async fn require_action(
    state: &DispatcherState,
    id: uuid::Uuid,
    scope: Option<&weft_core::activation::ActivationScope>,
    verbs: &[&str],
) -> Result<ActionSnapshot, (StatusCode, String)> {
    // A project `weft rm` marked takes no verb, said in its own words
    // rather than as a verb the table does not offer.
    state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?
        .unless_removing()?;
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let snapshot = gather_action_snapshot(state, id, &project, scope).await?;
    let allowed = compute_available_actions(&ActionInputs {
        // Refused just above.
        removing: false,
        lifecycle: &snapshot.lifecycle,
        partly_down: snapshot.partly_down,
        transition: snapshot.transition,
        resync_stopped: snapshot.resync_stopped,
        // Client-side fact, treated as satisfied (same posture as the
        // drift bits below): when the build happens at verb time the user
        // clicks activate on the SAVED source's triggers before a build
        // has re-registered them, so the REGISTERED definition's
        // has_triggers may lag. `activate_inner` re-checks honestly
        // against the built definition and 412s a genuinely
        // trigger-less project after its build.
        has_triggers: true,
        has_infra: snapshot.has_infra,
        // Same staleness posture, and for the same reason: this is
        // walked from the REGISTERED definition, which lags the source
        // the user just saved. Enforcing it here would refuse an
        // activate over an infra node the user has already deleted,
        // BEFORE the build that would have said so. `require_trigger_infra`
        // runs after the build and refuses against the fresh definition,
        // naming the nodes to start.
        trigger_infra_ready: true,
        // And the same for a run: the door is `versions::run`'s own
        // pre-flight, after the build, scoped to the places the run
        // executes. Enforcing the whole-graph fact here would refuse a
        // run aimed at nodes whose infra is up because some other node's
        // is down, which that run never touches.
        run_infra_ready: true,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup: &snapshot.infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift: &ProjectDrift { infra_drift: true, binary_drift: true, definition_drift: true, activation_drift: true },
        preservation: &snapshot.preservation,
        running_count: snapshot.running_count,
    });
    if verbs.iter().any(|v| allowed.iter().any(|a| a == v)) {
        return Ok(snapshot);
    }
    let status = snapshot.lifecycle.status;
    Err((StatusCode::CONFLICT, unavailable_action(verbs[0], status, snapshot.transition, &snapshot.infra_rollup, &allowed)))
}

/// What an activate over triggers that are all on already is told. It is
/// nearly always somebody who changed the source and wants it live, which
/// is resync's job.
const ALREADY_ON: &str = "these triggers are already on. If you changed the source, run `weft resync --mode <park|hibernate|wipe>` \
     to put the change live (park and hibernate keep the work waiting on them, wipe drops it); \
     `weft deactivate` turns them off instead";

/// Why `verb` is refused, as a person reads it. An activate over triggers
/// that are already on is nearly always somebody who changed the source
/// and wants it live, which is resync's job, so that case says so plainly
/// instead of listing the table's state.
fn unavailable_action(
    verb: &str,
    status: crate::project_store::ProjectStatus,
    transition: crate::project_store::ProjectTransition,
    infra_rollup: &str,
    allowed: &[String],
) -> String {
    if verb == "activate" && status == crate::project_store::ProjectStatus::Active {
        return ALREADY_ON.into();
    }
    // A build or a resync in flight blocks every verb but its cancels, so
    // it is named as the blocker rather than buried in the triggers' state.
    if let Some(why) = transition.refusal() {
        return format!("'{verb}' is not available right now: {why}");
    }
    // Infra being started, stopped or terminated blocks every verb but
    // its cancel (the table's master rule), so the infra is named as the
    // blocker rather than the triggers' state, which is not why.
    if allowed == ["infra_cancel"] {
        return format!(
            "'{verb}' is not available right now: this program's infra is changing (infra {infra_rollup}). \
             `weft infra status` shows where each piece is and `weft infra cancel` stops the change; \
             run this again once it settles"
        );
    }
    // Starting infra that already runs is somebody who changed it and wants
    // the change live (an upgrade) or who did not know it was up.
    if verb == "infra_start" && infra_rollup == "running" && allowed.iter().any(|a| a == "infra_upgrade") {
        return "every piece of this program's infrastructure is already running. If you changed it, \
                `weft infra upgrade` puts the change live"
            .into();
    }
    format!(
        "'{verb}' is not available right now: the triggers it names are {} (infra {infra_rollup}); \
         allowed actions: [{}]",
        status.as_str(),
        allowed.join(", "),
    )
}

/// Pure drift comparison: desired (CLI's view) vs running (DB view).
/// Both sides compute the SAME hash function for each signal, so the
/// comparison is a string equality check.
fn compute_drift(
    query: &StatusQuery,
    running_binary_hash: Option<&str>,
    running_definition_hash: Option<&str>,
    running_infra_hash: Option<&str>,
) -> ProjectDrift {
    // Each drift bit is only meaningful when both sides have a
    // hash. No running hash means the project was never built /
    // activated; the action bar shouldn't surface drift then.
    let binary_drift = match (query.desired_binary_hash.as_deref(), running_binary_hash) {
        (Some(want), Some(have)) => {
            want != have && query.desired_full_binary_hash.as_deref() != Some(have)
        }
        _ => false,
    };
    let definition_drift = match (
        query.desired_definition_hash.as_deref(),
        running_definition_hash,
    ) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    let infra_drift = match (query.desired_infra_hash.as_deref(), running_infra_hash) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    ProjectDrift {
        activation_drift: false,
        infra_drift,
        binary_drift,
        definition_drift,
    }
}

/// Whether the listeners' registrations pin a program other than the
/// one the user is looking at. A registration pins the exact code
/// identity it was made from; a project activated before that identity
/// was recorded (`activation_program` is null) has only the graph hash
/// to go by, so it falls back to `definition_drift`.
fn activation_has_drifted(query: &StatusQuery, registered_binary: Option<&str>, registered_definition: Option<&str>, activated: Option<&weft_core::project::hash::ProgramIdentity>, definition_drift: bool) -> bool {
    let Some(activated) = activated else { return definition_drift };
    let desired_binary = query.desired_binary_hash.as_deref().or(registered_binary);
    let desired_definition = query.desired_definition_hash.as_deref().or(registered_definition);
    desired_definition != Some(activated.definition_hash.as_str())
        || (desired_binary != Some(activated.binary_hash.as_str())
            && query.desired_full_binary_hash.as_deref() != Some(activated.binary_hash.as_str()))
}

/// Every input the reconciliation reads, gathered in one place so the
/// status handler (UI list) and verb enforcement (`require_action`)
/// feed the SAME pure function from the SAME facts. `drift` is the one
/// input only a client can supply (the desired hashes live on the
/// client); enforcement passes all-true so drift-gated verbs are never
/// spuriously rejected by the dispatcher.
pub(crate) struct ActionInputs<'a> {
    /// `weft rm` marked the project and has not finished removing it
    /// (`StoredProjectSummary::removing`).
    pub removing: bool,
    pub lifecycle: &'a crate::activation_store::ActivationLifecycle,
    /// Some of the triggers are on and others off (`ActionSnapshot::partly_down`).
    pub partly_down: bool,
    pub transition: crate::project_store::ProjectTransition,
    /// The project's resync stopped with triggers down (`ActionSnapshot::resync_stopped`).
    pub resync_stopped: bool,
    /// Source declares a shared trigger (frontend-parse fact, derived
    /// here from the stored definition); an instance's triggers are its own.
    pub has_triggers: bool,
    /// Source declares any infra node. This is the SOURCE fact only;
    /// orphaned live infra does not count (Model 1).
    pub has_infra: bool,
    /// Every infra node the project's TRIGGERS DEPEND ON is running
    /// (vacuously true when they depend on none). Infra that only a
    /// RUN touches is gated at run time and does not hold an
    /// activation back. See `weft_core::project::infra_triggers_depend_on`.
    ///
    /// This is what the table REPORTS about activate, from the
    /// registered definition. It is not what the door enforces:
    /// `require_action` passes it satisfied, because the registered
    /// definition lags the source, and `require_trigger_infra` does
    /// the honest check after the build, on the definition the
    /// activation will register.
    pub trigger_infra_ready: bool,
    /// Every infra node the source declares is running, which is what
    /// a WHOLE-GRAPH run touches (`run_infra_ready`). The same posture
    /// as `trigger_infra_ready`: what the table reports about run, from
    /// the registered definition, for the bar's unaimed Run button.
    /// `require_action` passes it satisfied, and `versions::run` does
    /// the honest check after the build, on the definition the run will
    /// execute and scoped to the places it executes: a run aimed at
    /// part of the graph waits on that part's infra alone.
    pub run_infra_ready: bool,
    /// Live `infra_node` rows exist whose node is NOT in the current
    /// source (the user deleted the node while it was deployed, or it sits
    /// behind a part that does not compile). Does
    /// NOT gate run/activate; DOES keep the infra controls offered so
    /// the user never loses track of live infra.
    pub orphaned_infra: bool,
    pub infra_rollup: &'a str,
    /// An infra operation is in flight: an uncompleted lifecycle
    /// command (apply / stop / terminate) or a running InfraSetup
    /// provisioning run. Transitional per the master rule even
    /// BEFORE the supervisor flips any node status (the window where
    /// the rollup alone would still read as stable), so a drain in
    /// progress is never starved by new runs.
    pub infra_busy: bool,
    pub drift: &'a ProjectDrift,
    pub preservation: &'a PreservationCounts,
    pub running_count: usize,
}

/// THE reconciliation table (`docs/project-lifecycle-state-model.md`
/// §8) as a pure function: verbs the dispatcher will currently accept,
/// computed from the concern-lifecycles + source facts, never stored.
/// Clients render the action bar from this list directly AND the verb
/// handlers enforce against the same list, so the button state and the
/// enforcement cannot disagree.
///
/// A project `weft rm` marked offers nothing: only `weft rm` again goes on
/// with it.
///
/// The master rule collapses the table: in ANY transitional state
/// (building / cancelling_build / resyncing / activating / deactivating /
/// infra provisioning / stopping / terminating) the only offered action is
/// the matching CANCEL. Stable states then enumerate:
///
///   - run           : allowed iff the source's infra (if any) is
///                     running. Never gated on a build (run builds as
///                     step 0) and never gated on an orphan (a
///                     no-infra graph runs in the shared pool,
///                     unlinked). Allowed while Active (manual run
///                     alongside live triggers; the bar may not show
///                     a button, the backend permits it).
///   - activate /
///     reactivate    : Registered/Inactive, source has triggers, and
///                     every infra node the TRIGGERS DEPEND ON running
///                     (infra only a run touches is gated at run time
///                     and does not hold activation back; the door
///                     re-checks this against the definition it
///                     builds). Reactivate variant when preserved
///                     state exists.
///   - deactivate    : Active. NOT gated on has_triggers: deleting
///                     the last trigger from source while active must
///                     keep Deactivate offered (trigger divergence).
///   - resync        : activation drift, which counts only triggers
///                     that are on (the program's or an instance's), so
///                     it does not wait on the shared status.
///                     Deactivate-then-reactivate under the hood;
///                     re-picks placement from the CURRENT source.
///                     Never touches orphaned infra.
///   - cancel_*      : the transitional rows.
///   - infra_*       : offered whenever live-or-declared infra exists
///                     (`has_infra || orphaned_infra`), per rollup.
///                     start/upgrade additionally need the SOURCE to
///                     declare infra (there is no spec to provision an
///                     orphan from); stop/terminate work on the live
///                     rows themselves, so they stay offered for a
///                     pure orphan (the never-lose-track guarantee).
// SYNC: compute_available_actions <-> packages/weft-graph/src/webview/lib/verb-gates.ts (the starter verbs the bar offers on top of this table; resyncStopped reads the resync_stopped branch), packages/weft-graph/src/protocol.ts ActionVerb
fn compute_available_actions(inputs: &ActionInputs<'_>) -> Vec<String> {
    use crate::project_store::ProjectStatus;

    if inputs.removing {
        return Vec::new();
    }
    // Master rule, build axis: a build in flight offers only its
    // cancel (idempotent while already cancelling).
    if inputs.transition.is_building() {
        return vec!["cancel_build".to_string()];
    }
    // A resync drives its triggers down and back on itself, so only the
    // cancel of the step it is at is offered: ending its wait on running
    // work, or stopping it as the triggers come back. One that stopped
    // with triggers down is taken up again, or given up.
    if inputs.transition == crate::project_store::ProjectTransition::Resyncing {
        if inputs.resync_stopped {
            return vec!["resync".to_string(), "cancel_activate".to_string()];
        }
        return match inputs.lifecycle.status {
            ProjectStatus::Activating => vec!["cancel_activate".to_string()],
            ProjectStatus::Deactivating if inputs.running_count > 0 => vec!["cancel_running".to_string()],
            _ => Vec::new(),
        };
    }

    // Master rule, trigger axis.
    match inputs.lifecycle.status {
        ProjectStatus::Activating => {
            return vec!["cancel_activate".to_string()];
        }
        ProjectStatus::Deactivating => {
            // Mid-deactivate: give up on the wait (cancel_running,
            // the drain finishes immediately) or change your mind
            // (resume_active rolls forward into Active).
            let mut out = Vec::new();
            if inputs.running_count > 0 {
                out.push("cancel_running".to_string());
            }
            out.push("resume_active".to_string());
            return out;
        }
        ProjectStatus::Registered | ProjectStatus::Inactive | ProjectStatus::Active => {}
    }

    // Master rule, infra axis: either the derived rollup reports a
    // transitional state, or an infra operation is in flight that the
    // rollup can't see yet (a claimed stop still draining, a
    // provisioning run before any node row flips). Only
    // meaningful when infra state exists at all.
    let infra_live = inputs.has_infra || inputs.orphaned_infra;
    if infra_live
        && (inputs.infra_busy
            || matches!(inputs.infra_rollup, "provisioning" | "stopping" | "terminating"))
    {
        return vec!["infra_cancel".to_string()];
    }

    // Stable states.
    let mut out = Vec::new();

    // `run` is offered whenever it is a legal NEXT STEP, NOT gated on
    // whether a worker image is already built (a click auto-builds on
    // demand). The one gate is genuine live-state: the infra a run
    // touches must be running (a run would fail fetching infra outputs;
    // the user starts infra first, its own verb). What the table can
    // answer is the WHOLE-GRAPH run; a run aimed at part of the graph
    // waits on that part alone, which only the door can tell.
    if inputs.run_infra_ready {
        out.push("run".to_string());
    }

    // A trigger whose infra is down cannot be armed: its registration
    // reads the address off the infra node feeding it. Infra the trigger
    // does not depend on is gated at run time instead, so it does not
    // hold the activation back. The door's own copy of this is
    // `require_trigger_infra`, which runs after the build; this one is
    // what the bar and `weft status` are told.
    let turn_on = |out: &mut Vec<String>, some_inactive: bool| {
        if inputs.has_triggers && inputs.trigger_infra_ready {
            let has_preserved = inputs.preservation.parked + inputs.preservation.suspended > 0;
            out.push(if has_preserved && some_inactive { "reactivate" } else { "activate" }.to_string());
        }
    };
    match inputs.lifecycle.status {
        ProjectStatus::Active => {
            out.push("deactivate".to_string());
            // Some triggers on and others off (an infra stop took down the
            // ones reading it): turning the rest on is still a step.
            if inputs.partly_down {
                turn_on(&mut out, true);
            }
        }
        ProjectStatus::Registered | ProjectStatus::Inactive => {
            turn_on(&mut out, inputs.lifecycle.status == ProjectStatus::Inactive);
        }
        ProjectStatus::Activating | ProjectStatus::Deactivating => {
            unreachable!("transitional statuses returned above")
        }
    }

    // Registrations pin both the graph and the worker code. Either change
    // needs resync before listeners can fire the new program. The drift
    // bit counts only activations that are on, the program's or a
    // instance's, so it is offered whatever the shared status says: a
    // instance's triggers can be on while the program's are off. Resync
    // re-arms exactly what activate arms, so it waits on the same infra:
    // the triggers' own, checked again by the door after its build
    // (`require_trigger_infra`).
    if inputs.drift.activation_drift && inputs.trigger_infra_ready {
        out.push("resync".to_string());
    }

    // Infra controls, per rollup. `partial` (some units up, some down)
    // is a valid steady state, not a transient: Start brings the down
    // units up, Stop takes the up units down, Terminate kills
    // everything; all three are meaningful at once.
    if infra_live {
        let can_provision = inputs.has_infra; // needs a source spec
        match inputs.infra_rollup {
            "running" => {
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if can_provision && inputs.drift.infra_drift {
                    out.push("infra_upgrade".to_string());
                }
            }
            "stopped" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
                out.push("infra_terminate".to_string());
            }
            "none" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
            }
            "partial" | "failed" | "flaky" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if can_provision && inputs.drift.infra_drift {
                    out.push("infra_upgrade".to_string());
                }
            }
            // provisioning/stopping/terminating returned above.
            _ => {}
        }
    }

    out
}

/// The activations `scope` names in `project`, refusing a mismatch (a
/// per-instance trigger without an instance, a shared one with one, an
/// address that is not a trigger) and a scope that names nothing.
/// Refuse to turn on a trigger whose fire could never run: its setup may
/// need nothing broken (a route needs nothing to register), while every
/// fire runs the trigger's cut (`run_spec::compute_trigger_fire`), and one
/// that needs a part that does not compile is refused at each call. The
/// person is told now, naming the part, rather than every caller later.
/// Asked of the triggers an activate switches on, whoever asks, and of the
/// ones a resync puts back (before it takes any down); never of a
/// re-arm, whose triggers are on and whose change has nothing to do with
/// it.
async fn refuse_unfireable(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Result<(), (StatusCode, String)> {
    let mut refused = Vec::new();
    for key in keys {
        let saved = saved_for_run(state, id, project, key.instance()).await?;
        if let Err(why) = weft_core::run_spec::compute_trigger_fire(project, &key.trigger, &serde_json::Value::Null, None, &saved) {
            refused.push(why);
        }
    }
    if refused.is_empty() {
        return Ok(());
    }
    Err((StatusCode::UNPROCESSABLE_ENTITY, refused.join("\n")))
}

pub(crate) fn resolve_scope(
    project: &ProjectDefinition,
    scope: &weft_core::activation::ActivationScope,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let keys = scope.resolve(project).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    if keys.is_empty() && !has_triggers(project) {
        return Err((StatusCode::PRECONDITION_FAILED, "this program has no triggers".to_string()));
    }
    if keys.is_empty() {
        let what = match &scope.instance {
            None => "no shared trigger (every trigger it has exists once per instance; name one with --instance)".to_string(),
            Some(instance) => format!("no trigger that exists once per instance, so there is nothing to activate for '{instance}'"),
        };
        return Err((StatusCode::PRECONDITION_FAILED, format!("this program has {what}")));
    }
    Ok(keys)
}

/// The activations a verb that only takes triggers down acts on:
/// `scope`'s owner's rows among `rows`, every one of them, or the named
/// triggers'. Picked from the rows rather than the source, because what
/// is listening is what the rows say: a trigger the source has since
/// dropped, or an instance's copy of one the program no longer runs per
/// instance, still has a row, and taking it down is the only way to stop
/// it. A named trigger with no row for that owner is refused: nothing of
/// it is on, so the name is a mistake. No row at all is no keys (`weft rm
/// --journal` quiesces a trigger-less program through deactivate).
fn keys_to_take_down(
    rows: &[crate::activation_store::Activation],
    scope: &weft_core::activation::ActivationScope,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let owner = scope.owner();
    let owned = rows.iter().map(|a| &a.key).filter(|key| key.owner == owner);
    if scope.triggers.is_empty() {
        return Ok(owned.cloned().collect());
    }
    let keys: Vec<weft_core::activation::ActivationKey> =
        owned.filter(|key| scope.triggers.contains(&key.trigger)).cloned().collect();
    if let Some(unknown) = scope.triggers.iter().find(|name| !keys.iter().any(|key| &&key.trigger == name)) {
        let named = weft_core::activation::ActivationKey::new(unknown.clone(), owner);
        return Err((
            StatusCode::NOT_FOUND,
            format!("trigger {named} was never activated, so there is nothing of it to take down"),
        ));
    }
    Ok(keys)
}

/// Whether the program has any trigger, shared or per instance.
fn has_triggers(project: &ProjectDefinition) -> bool {
    !weft_core::project::trigger_places(project).is_empty()
}

/// The places the activations' triggers sit at: the seeds of their
/// setup run.
fn key_places(
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Vec<Located> {
    keys.iter()
        .map(|key| {
            let (id, path) = weft_core::project::resolve_address(project, &key.trigger);
            Located::new(id, path)
        })
        .collect()
}

/// The owner the keys share: every verb acts on one owner's triggers at
/// a time.
fn keys_instance(keys: &[weft_core::activation::ActivationKey]) -> Option<&weft_core::instance::InstanceId> {
    keys.first().and_then(|key| key.instance())
}

/// Prepare trigger settings without changing what is listening. The
/// body names which triggers (every shared one by default) and the
/// [`RunningChoice`] for a worker built from an older image, the same
/// question an activate answers (the setup runs on a worker, so a
/// stale one is replaced first).
pub async fn bake(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<BakeRequest>>,
) -> Result<Json<BakeAnswer>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let (program, project) = coherent_definition(&state, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let (running_policy, drain_timeout_secs) = body.running.resolve(None);
    let keys = resolve_scope(&project, &body.scope)?;
    prepare_trigger_setup(&state, id, &project, &keys, StaleWorkerChoice::Replace(running_policy, drain_timeout_secs)).await?;
    let bake = run_trigger_setup(&state, id, &project, &keys, &program, None, None).await?;
    Ok(Json(BakeAnswer { bake, broken: project.broken.clone() }))
}

/// What a bake answers: the settings it saved, and the parts of the baked
/// version that do not compile (`ProjectDefinition::broken`), which no
/// setup that needs one ran, so whoever baked is told about every one
/// however they asked (`weft bake`, by id or not).
#[derive(Debug, serde::Serialize)]
pub struct BakeAnswer {
    #[serde(flatten)]
    bake: crate::journal::TriggerBake,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    broken: Vec<weft_core::project::Broken>,
}

/// Refuse unless every infra copy the named activations' triggers read
/// is Running (the instance's own copy for a per-instance one).
///
/// The one rule joining the two lifetimes: a trigger reads its address
/// off the infra node feeding it, so arming it before that node is up
/// registers a signal against an address that does not answer. Nothing
/// here starts infra to satisfy itself; infra is the user's own verb (or
/// their program's), and provisioning on a click that said nothing about
/// it would spend their money for them.
///
/// `project` must be the definition the caller is about to REGISTER,
/// not the one on file: the registered one lags the source the moment
/// the user writes a node, and refusing over an infra node they have
/// already deleted is a dead end (see `require_action`, which exempts
/// the same fact for the same reason).
pub(crate) async fn require_trigger_infra(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Result<(), (StatusCode, String)> {
    // Validation first: a trigger setup that cannot be selected at all
    // (a trigger inside a half-selected loop) is a 400 about the graph,
    // not a story about infra.
    let places = key_places(project, keys);
    let walked = weft_core::project::cut::walk_setup(&weft_core::project::graph::ProjectGraph::new(project), &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let within: HashSet<String> = walked
        .runs
        .iter()
        .filter(|place| project.nodes.iter().any(|n| n.id == place.id && n.requires_infra))
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    let missing = missing_infra_nodes(state, project_id, project, Some(&within), keys_instance(keys)).await?;
    if missing.is_empty() {
        return Ok(());
    }
    let (listed, fix) = missing_and_fix(&missing);
    Err((
        StatusCode::PRECONDITION_REQUIRED,
        format!("these triggers' infra is not running: {listed}. Start it with {fix}, then run this again."),
    ))
}

/// What a trigger setup does first to runs still running on an
/// older image.
enum StaleWorkerChoice {
    /// Settle them under the caller's running policy and drain cap (see
    /// [`reconcile_worker`]); the caller's choice, never a fixed one here,
    /// because a `wait` sits inside the request for as long as those
    /// runs run.
    Replace(crate::infra_lifecycle_command::RunningPolicy, u64),
    /// Leave them be and only ready the new image: the setup runs on it
    /// either way. A re-arm from an instance's change of values takes this,
    /// because the change may come from one of those very runs, and
    /// waiting on it would wait on the run waiting for the change.
    Retire,
}

/// Who asks for an activate, which decides what happens to a worker
/// built from an older image.
pub(crate) enum ActivateAsker<'a> {
    /// A person or a lifecycle command: the stale worker is replaced
    /// under the request's running policy.
    Outside,
    /// A resync bringing back the triggers it took down, under its drive
    /// (`crate::task_kinds::resync::ResyncPlan::driver`): as `Outside`,
    /// and the one activation that goes through while the resync holds
    /// the project.
    Resync(uuid::Uuid),
    /// A program's run (`ctx.trigger(..).activate()`): that run may sit
    /// on the stale worker, so waiting on or cancelling its work would
    /// wait on or cancel the asker. The worker is retired instead.
    Run,
    /// A change of an instance's values re-arming its live triggers; it
    /// may come from a run too, so it retires the same way.
    Rearm(&'a Rearm<'a>),
}

/// Make the workers match what the triggers will need, and refuse first
/// if their infra is not up.
async fn prepare_trigger_setup(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
    stale: StaleWorkerChoice,
) -> Result<(), (StatusCode, String)> {
    require_trigger_infra(state, id, project, keys).await?;
    match stale {
        StaleWorkerChoice::Replace(running_policy, drain_timeout_secs) => {
            reconcile_worker(state, id, running_policy, drain_timeout_secs).await
        }
        StaleWorkerChoice::Retire => {
            let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running image: {e:#}"));
            match state.projects.running_binary_hash(id).await.map_err(internal)? {
                Some(hash) => prepare_running_image(state, id, &hash).await,
                None => Ok(()),
            }
        }
    }
}

/// `GET /projects/{id}/builds/{build}`: where the version build a 202 from
/// the build request named stands (`build`), with each image build it
/// waits on and, once registered, what registered, or why it ended
/// otherwise. What the caller following a build reads until it ends.
pub async fn version_build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, build)): Path<(uuid::Uuid, uuid::Uuid)>,
) -> Result<Json<weft_core::builds::VersionBuildState>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    // The database out of reach is the install unable to serve this right
    // now, never the build's state: a 503, which the caller following the
    // build reads as "look again". Anything else (a row that no longer
    // reads) is a 500 asking again would not fix.
    crate::build::waiting::state(&state.pg_pool, id, build)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("read where the build is", e))?
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, format!("no build {build} of this project")))
}

pub async fn activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<ActivateRequest>>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    // Enforce against the reconciliation, over the activations this call
    // names: this one endpoint serves three table verbs (activate from
    // Registered/Inactive, reactivate when preserved state exists,
    // resume_active while Deactivating), so any of the three being
    // offered admits the call. Everything else (already Active,
    // transitional, infra not ready, a project `weft rm` marked) rejects
    // here; the claim below refuses that last one too, under the row's
    // lock, for a `weft rm` that marks it meanwhile.
    let snapshot = require_action(&state, id, Some(&body.scope), &["activate", "reactivate", "resume_active"]).await?;
    // Work kept waiting on the triggers being turned on is the asker's to
    // decide about; a choice is never made for a person who was not asked.
    let kept = &snapshot.preservation;
    if body.target.reactivate_choice.is_none() && kept.parked + kept.suspended > 0 {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "these triggers kept work while they were off ({} fires parked, {} runs waiting on a person): \
                 say what to do with it with --reactivate-choice execute_parked_keep_suspended (run the parked \
                 fires, keep the waits), keep_suspended_only (drop the parked fires) or wipe_all (drop both)",
                kept.parked, kept.suspended
            ),
        ));
    }
    // A port the person names is checked before anything is activated, so
    // a taken one refuses the activation and changes nothing.
    if let Some(port) = body.port {
        let Some(ports) = &state.project_ports else {
            return Err((StatusCode::BAD_REQUEST, "this install gives projects no port: a project's own address is the platform's".into()));
        };
        let taken = ports
            .check_asked(&state.pg_pool, id, port)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("check port {port}: {e:#}")))?;
        if let Some(why) = taken {
            return Err((
                StatusCode::CONFLICT,
                format!("{why}, so nothing was activated: free it, or leave out `--port` and the project keeps a port of its own"),
            ));
        }
    }
    activate_inner(&state, id, body).await
}

/// How `activate_inner` must roll back a failed Activating-window. Every
/// signal it armed is taken back first, so the rollback puts each
/// trigger back as the claim found it, and wipes only when an armed
/// signal could not be put back and nothing kept would be lost.
#[derive(Clone, Copy)]
enum ActivateRollback {
    /// Put every trigger back as the claim found it (a parked one
    /// parked, a live one listening as before, one never activated
    /// without a row) and cancel the setup run. Keeps existing signals and
    /// what waits on them: parked fires and the waits of runs those
    /// triggers fired, which a transient error must never cost.
    Restore,
    /// End the activation at Inactive, cancel its setup run and drop its
    /// triggers' own entry rows: for a re-arm, or triggers that kept
    /// nothing, whose armed signals could not all be put back.
    WipeSignals,
}

struct ActivateWindowError {
    status: StatusCode,
    msg: String,
    rollback: ActivateRollback,
}

/// The Activating-window of `activate_inner`: every step that runs
/// while the named activations are `Activating`, ending with the CAS to
/// `Active`. Factored out so `activate_inner` has ONE rollback site
/// for the whole window (a failure anywhere here must un-stick them;
/// the caller does that once on Err).
///
/// The error carries the rollback MODE (`ActivateRollback`): failures
/// before the first arm restore (keep signals); from arming on, what was
/// armed is taken back and the triggers restored (`take_back_arms`).
///
/// Single-flight against a concurrent activate of the same triggers is
/// the exclusive Activating claim (`try_begin_activating`), not a lock.
async fn activate_trigger_setup_window(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
    choice: ReactivateChoice,
    project: &ProjectDefinition,
    program: &weft_core::project::hash::ProgramIdentity,
    rearm: Option<&Rearm<'_>>,
    stale: StaleWorkerChoice,
) -> Result<Vec<ActivationUrl>, ActivateWindowError> {
    // Failures before the first arm haven't touched signal state beyond
    // the reactivate choice (the person's), so they only need the
    // triggers put back.
    let unstick = |(status, msg): (StatusCode, String)| ActivateWindowError {
        status,
        msg,
        rollback: ActivateRollback::Restore,
    };

    // A binary-hash change must kill the stale-image worker before the
    // activate's TriggerSetup run goes, otherwise it's dispatched
    // against a worker that doesn't know about the new trigger nodes.
    // Idempotent (kill-by-binary-hash + respawn). It runs inside the
    // claim, so from the moment the verb is taken the triggers read
    // `activating` (a worker move can take a while), and a failure puts
    // them back as they were. The request's running policy says what
    // happens to the stale worker's runs (cancel unless the
    // caller asked to wait); either way the replacement is loud, never a
    // silent kill.
    //
    // This also carries the infra precondition, and it runs FIRST: an
    // activation whose triggers need infra that is down is refused here,
    // against the definition this activation just built, before any
    // worker work happens. An activate a run asked for only retires the
    // stale worker (see `ActivateAsker`).
    prepare_trigger_setup(state, id, project, keys, stale).await.map_err(unstick)?;

    // Apply the reactivate choice's destructive effect now that we
    // hold the exclusive Activating claim.
    let kept = apply_reactivate_choice(state, id, keys, activation, choice).await.map_err(unstick)?;
    // An arm that cannot be put back leaves its signal row as the failed
    // put-back left it. Triggers coming up from down with work kept on
    // them stay down, so that work is never lost; live triggers a re-arm
    // set up again no longer match what they ran, so they are turned off,
    // as are triggers that kept nothing.
    let if_not_put_back = if rearm.is_none() && kept { ActivateRollback::Restore } else { ActivateRollback::WipeSignals };

    // Capture first, then arm that completed capture. The capture
    // registers nothing, so until the first arm a failure puts the
    // triggers back as they were. Arming refreshes existing entry rows
    // while retaining their tokens and parked fires; a re-arm sets its
    // live triggers up again with the values it stores as it lands. A
    // trigger that fails to arm takes back the ones armed before it (a
    // row it created goes, a row it replaced is written back and the
    // listener runs it again), so the triggers end exactly as they were:
    // a parked one parked with its signals and what waits on them.
    let bake = run_trigger_setup(state, id, project, keys, program, Some(activation), rearm.and_then(|r| r.overlay))
        .await
        .map_err(unstick)?;
    let recorded = state
        .activations
        .record_activation_source(id, activation, &bake.program, &bake.source_version)
        .await
        .map_err(|e| unstick((StatusCode::INTERNAL_SERVER_ERROR, format!("record activation source: {e}"))))?;
    if !recorded {
        return Err(unstick((StatusCode::CONFLICT, format!("activation {activation} no longer owns its triggers"))));
    }
    let mut armed = Vec::new();
    for key in keys {
        // A trigger its setup skipped (behind a closed gate) arms nothing.
        let Some(capture) = bake.captured.get(&key.trigger) else { continue };
        let arming = crate::task_kinds::register_signal::RegisterSignalExecutor::arm(state,
            crate::task_kinds::register_signal::RegisterSignalPayload {
                run_id: bake.run_id, node_id: key.trigger.clone(), frames: Vec::new(),
                spec: capture.spec.clone(), is_resume: false, call_index: 0,
                port_snapshot: Some(capture.ports.clone()),
                asked_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            }).await;
        let error = match arming {
            Ok(done) => {
                armed.push(done);
                continue;
            }
            Err(error) => error,
        };
        return Err(take_back_arms(state, if_not_put_back, armed, format!("arm trigger {key}: {error:#}")).await);
    }

    // Every step that can fail runs before the one delete (the orphan
    // rows below) except the flip to Active after it, so up to the delete
    // a re-arm can still be put back exactly as it was, and after it only
    // the orphan rows stay gone. The orphans are read first: the listener
    // is told to leave them down, since they are about to go and one that
    // cannot come up must not fail the activation deleting it.
    let orphans = match orphan_entry_tokens(state, id, project, keys_instance(keys)).await {
        Ok(orphans) => orphans,
        Err(e) => return Err(take_back_arms(state, if_not_put_back, armed, format!("read orphan entry rows: {e:#}")).await),
    };

    // Reconcile what the listener holds of this project with the durable
    // signal table (resume signals belong to suspended runs whose
    // workers are gone). /rehydrate is idempotent, and fails only on this
    // project's own rows.
    if let Err(e) = state.listener.rehydrate(id, &orphans).await {
        return Err(take_back_arms(state, if_not_put_back, armed, format!("listener rehydrate: {e:#}")).await);
    }

    // The addresses the armed triggers listen at, read before the flip:
    // past it nothing may fail the call. The orphans are other triggers'
    // rows, so they are not among these keys'.
    let urls = match collect_listener_urls(state, id, keys).await {
        Ok(urls) => urls,
        Err(e) => return Err(take_back_arms(state, if_not_put_back, armed, format!("collect_listener_urls: {e:#}")).await),
    };

    // Last, the one step that cannot be taken back once it lands: drop
    // this owner's entry rows for triggers the source no longer has (the
    // user edited it while they were down). The delete is one
    // transaction, and the listener is told to forget the rows only
    // after it commits, so a failure deletes nothing: every orphan row is
    // still there (held, left down by the rehydrate above) and a re-arm
    // is put back like any earlier failure.
    if let Err(e) = drop_orphan_entry_rows(state, id, &orphans, activation).await {
        let msg = format!("drop the entry rows of triggers the source no longer has ({} row(s), none deleted): {e:#}", orphans.len());
        return Err(take_back_arms(state, if_not_put_back, armed, msg).await);
    }

    // Flip Activating → Active. The claim guards against a concurrent
    // cancel_activate that ended the activation: in that case the cancel
    // already wiped its signals, so we surrender with no further
    // rollback. The setup run is terminal by now (`run_trigger_setup`
    // returned on its terminal), so the run the flip clears is only
    // bookkeeping. A re-arm's values are stored in the same transaction:
    // until the flip, the triggers' fires park (an activating trigger's
    // are not visible yet), and the drain after it routes them with the
    // values the setup ran with.
    // A flip that fails still leaves a re-arm's own triggers able to go
    // back; only the orphan rows stay gone, and those belong to triggers
    // the source no longer has.
    let ended = match state
        .activations
        .end_activating(
            id,
            activation,
            &crate::activation_store::ActivationLifecycle::active(),
            false,
            rearm.and_then(|r| r.store.as_ref()),
        )
        .await
    {
        Ok(ended) => ended.is_some(),
        Err(e) => return Err(take_back_arms(state, if_not_put_back, armed, format!("end_activating: {e:#}")).await),
    };
    if !ended {
        return Err(ActivateWindowError {
            status: StatusCode::CONFLICT,
            msg: "activate raced with cancel-activate; the triggers are now inactive".into(),
            rollback: ActivateRollback::Restore,
        });
    }
    Ok(urls)
}

/// In-process callable for `activate`: the axum handler, `resync`'s
/// reactivate step, the supervisor's auto-recover path
/// (`lifecycle_claimer`) and a program's `ctx.trigger(..).activate()`
/// all land here. Same body as the handler minus the extractor plumbing.
pub async fn activate_inner(
    state: &DispatcherState,
    id: uuid::Uuid,
    request: ActivateRequest,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    activate_with(state, id, request, ActivateAsker::Outside).await.map_err(Into::into)
}

/// Why [`activate_with`] did not activate.
#[derive(Debug)]
pub(crate) enum ActivateError {
    /// The single-flight claim was refused, and why, as the claim's own
    /// transaction decided it.
    Refused(crate::activation_store::ClaimRefused),
    Failed(StatusCode, String),
}

impl From<(StatusCode, String)> for ActivateError {
    fn from((code, message): (StatusCode, String)) -> Self {
        ActivateError::Failed(code, message)
    }
}

impl From<ActivateError> for (StatusCode, String) {
    fn from(error: ActivateError) -> Self {
        use crate::activation_store::ClaimRefused;
        match error {
            ActivateError::Refused(refused) => (
                StatusCode::CONFLICT,
                match refused {
                    ClaimRefused::Building => "the project is building; wait for it to finish or cancel it with `weft cancel-build`",
                    ClaimRefused::Claimed => "these triggers are already activating; wait for it to finish or cancel it",
                    ClaimRefused::NotExpected => "a trigger this re-arm read as on was taken down meanwhile",
                    ClaimRefused::Resyncing => crate::project_store::ProjectTransition::Resyncing.refusal().unwrap_or_default(),
                    ClaimRefused::ResyncGone => "the resync bringing these triggers back was given up, or taken up again, meanwhile",
                    ClaimRefused::Removing => weft_core::projects::PROJECT_BEING_REMOVED,
                }
                .into(),
            ),
            ActivateError::Failed(code, message) => (code, message),
        }
    }
}

/// Refuse a verb that names a build other than the registered one: a
/// teammate's build landed between the caller's build and this verb, and
/// acting would run it under the caller's name. A verb that names none
/// (by id, or a program's own) acts on the registered build.
pub(crate) async fn require_registered_build(
    state: &DispatcherState,
    id: uuid::Uuid,
    named: &weft_core::builds::BuildHashes,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running hashes: {e}"));
    let running = (
        state.projects.running_binary_hash(id).await.map_err(internal)?,
        state.projects.running_definition_hash(id).await.map_err(internal)?,
        state.projects.running_infra_hash(id).await.map_err(internal)?,
    );
    let differs = |named: &Option<String>, running: &Option<String>| named.is_some() && named != running;
    if differs(&named.binary_hash, &running.0)
        || differs(&named.definition_hash, &running.1)
        || differs(&named.infra_hash, &running.2)
    {
        return Err((
            StatusCode::CONFLICT,
            "the project's build moved since this was prepared (another build registered after \
             it); run the command again"
                .into(),
        ));
    }
    Ok(())
}

/// A change of an instance's values, and the instance's live triggers it
/// re-arms (see `crate::instance_values::change`): the activation window
/// sets those triggers up again with `values`, arms them on what that
/// setup captured, and stores `store` in the transaction that lands them
/// Active. A failure before arming puts the triggers back as they were;
/// one after takes them down, and either way the values stay as they were.
pub(crate) struct Rearm<'a> {
    /// The instance's change the setup runs with, in place of what is
    /// stored; none when what is stored is already the answer (a pick
    /// change, stored before its re-arm: `crate::install_picks`).
    pub overlay: Option<&'a weft_core::instance::ValueChanges>,
    /// The same change as the store writes it; `None` for a trigger
    /// re-armed on a change another re-arm already stored.
    pub store: Option<crate::activation_store::ValuesStore<'a>>,
}

/// The install's picks a run of `program` (its cut) carries, checked by
/// `weft_core::picks::run_picks`: a 422 naming every required connection
/// nobody picked on this install.
pub(crate) async fn picks_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    program: &ProjectDefinition,
) -> Result<weft_core::picks::Picks, (StatusCode, String)> {
    if weft_core::picks::picked_places(program).is_empty() {
        return Ok(Default::default());
    }
    let stored = held_picks(state, project_id, Read::Held).await?;
    match weft_core::picks::run_picks(program, &stored.rows) {
        Ok(picks) => Ok(picks),
        // Confirmed against the rows before refusing (see `Read`).
        Err(_) => {
            let stored = held_picks(state, project_id, Read::Fresh).await?;
            weft_core::picks::run_picks(program, &stored.rows).map_err(|refusal| refusal_error(&refusal))
        }
    }
}

/// Every pick this install keeps for the project.
pub(crate) async fn stored_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<weft_core::picks::Picks, (StatusCode, String)> {
    Ok(held_picks(state, project_id, Read::Fresh).await?.rows.clone())
}

async fn held_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    read: Read,
) -> Result<std::sync::Arc<crate::held::OfTenant<weft_core::picks::Picks>>, (StatusCode, String)> {
    let load = || async {
        let tenant = crate::instance_values::owning_tenant(state, project_id).await?;
        let rows = weft_access_store::install_picks(&state.pg_pool, &tenant, project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the install's picks: {e:#}")))?;
        Ok::<_, (StatusCode, String)>(crate::held::OfTenant { tenant, rows })
    };
    match read {
        Read::Held => state.held.picks.get_or_load(project_id, load).await,
        Read::Fresh => state.held.picks.load_fresh(project_id, load).await,
    }
}

pub(crate) async fn activate_with(
    state: &DispatcherState,
    id: uuid::Uuid,
    request: ActivateRequest,
    asker: ActivateAsker<'_>,
) -> Result<Json<ActivateResponse>, ActivateError> {
    let ActivateRequest {
        target: ActivationTarget { build, reactivate_choice },
        running,
        scope,
        port,
    } = request;
    // No picker on an activate: the body's answer, or the default.
    let (running_policy, drain_timeout_secs) = running.resolve(None);
    // Every trigger an activate switches on is checked for a fire that
    // could never run, whoever asks (a person, a program's run, an infra
    // brought back); a re-arm's triggers are on already, and a resync
    // checked its own before taking any down.
    let checks_fires = !matches!(asker, ActivateAsker::Rearm(_) | ActivateAsker::Resync(_));
    let (stale, rearm, resync) = match asker {
        ActivateAsker::Outside => (StaleWorkerChoice::Replace(running_policy, drain_timeout_secs), None, None),
        ActivateAsker::Resync(driver) => (StaleWorkerChoice::Replace(running_policy, drain_timeout_secs), None, Some(driver)),
        ActivateAsker::Run => (StaleWorkerChoice::Retire, None, None),
        ActivateAsker::Rearm(rearm) => (StaleWorkerChoice::Retire, Some(rearm), None),
    };
    // What runs is what the last build registered (its program, hashes
    // and every place's images together, `POST /projects/{id}/builds`);
    // hashes named here only check the caller means that build.
    require_registered_build(state, id, &build).await?;

    // One coherent (hash, shape) pair for everything below: the
    // trigger checks here AND the kicks the activate window computes
    // must come from the definition recorded under the hash that
    // trigger-setup will journal (see `coherent_definition`).
    let (program, project) = coherent_definition(state, id).await?;

    // Activate is the trigger-setup verb. A program without a trigger
    // for this owner has nothing to register; refuse loudly so the CLI /
    // extension / program knows the verb doesn't apply.
    if !project.nodes.iter().any(|n| n.features.is_trigger) {
        return Err(ActivateError::Failed(
            StatusCode::PRECONDITION_FAILED,
            "project has no trigger nodes; nothing to activate. \
             Use `weft run` to fire a run directly."
                .into(),
        ));
    }
    let keys = resolve_scope(&project, &scope)?;
    // An activate turns on the triggers that are off and leaves the ones
    // already on (or being switched on) as they are: after an infra stop
    // took down only the triggers reading it, this is what brings them
    // back. One being switched off is not on: an activate on it is the
    // change of mind that rolls its drain back (`resume_active`). A re-arm
    // is the other way round: its triggers are on, and it sets them up
    // again.
    let keys = match rearm {
        Some(_) => keys,
        None => {
            let on: std::collections::HashSet<weft_core::activation::ActivationKey> = state
                .activations
                .list(id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?
                .into_iter()
                .filter(|a| {
                    matches!(
                        a.lifecycle.status,
                        crate::project_store::ProjectStatus::Active | crate::project_store::ProjectStatus::Activating
                    )
                })
                .map(|a| a.key)
                .collect();
            let off: Vec<weft_core::activation::ActivationKey> = keys.into_iter().filter(|k| !on.contains(k)).collect();
            if off.is_empty() {
                return Err(ActivateError::Failed(StatusCode::CONFLICT, ALREADY_ON.into()));
            }
            off
        }
    };
    if checks_fires {
        refuse_unfireable(state, id, &project, &keys).await?;
    }
    // Every connection the program needs picked on this install, before
    // any trigger moves: a run born without one is refused at its first
    // call, long after the activation said yes. A re-arm skips it: its
    // triggers are live already, and the change it carries is the
    // instance's or a pick's own, checked where it was asked.
    if rearm.is_none() {
        crate::install_picks::require_activation_picks(state, id, &project).await?;
    }
    // The shared infra that is not running, read now for the note the
    // answer carries: past the Active flip nothing may fail the call.
    let idle: Vec<MissingCopy> = missing_infra_nodes(state, id, &project, None, None)
        .await?
        .into_iter()
        .filter(|copy| !copy.needs_instance)
        .collect();

    // The choice's DESTRUCTIVE effect (clearing parked / wiping
    // signals) is applied AFTER the single-flight claim below, so a
    // losing concurrent activate that 409s never wipes the winner's
    // state.
    let choice = reactivate_choice.unwrap_or_default();

    // Single-flight gate: atomically claim Activating for every named
    // activation. While a trigger is activating (registering its
    // signals), no second activation of it may start. The claim wins or
    // loses whole; a losing caller (a concurrent activate from another
    // dispatcher, a double-click, CLI + extension racing, a program
    // activating the same instance twice) bails here with 409 BEFORE any
    // signal cleanup. Every later write is guarded by this activation's
    // reserved run.
    let activation = weft_core::new_run_id();
    let previous = state
        .activations
        .try_begin_activating(id, &keys, activation, rearm.is_some().then_some(crate::activation_store::ProjectStatus::Active), resync)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("try_begin_activating: {e}")))?
        .map_err(ActivateError::Refused)?;
    // Won the claim: broadcast it (other tabs' action bars flip to
    // "Activating… (cancel)" without a verb round-trip) and keep the
    // heartbeat fresh for the whole in-process window (resting while its
    // setup run goes) so the `lapses` loop only ever repairs a DEAD
    // driver's claim.
    // The guard drops when this function returns, on every path.
    crate::transition::publish_transition_changed(state, id).await;
    let _activation_heartbeat =
        crate::transition::ActivationHeartbeat::spawn(state, id, activation).await;

    // Everything from here to the Active flip happens while the
    // activations are Activating. ANY failure in this window must
    // un-stick them (a stranded Activating locks out every future
    // activate of those triggers). Rather than hand-roll a rollback at
    // each `?`, the whole window is ONE fallible block with ONE rollback
    // site below. A future step added here can't forget the un-stick.
    let setup = activate_trigger_setup_window(
        state,
        id,
        &keys,
        activation,
        choice,
        &project,
        &program,
        rearm,
        stale,
    )
    .await;
    let urls = match setup {
        Ok(urls) => urls,
        Err(ActivateWindowError { status, msg, rollback }) => {
            // Single rollback site for the whole window. The mode says
            // how much to undo: put the triggers back as the claim found
            // them (what was armed is already taken back), or wipe them
            // when an arm could not be taken back and nothing kept would
            // be lost. Both are idempotent and safe if a concurrent
            // cancel/success already ended the activation.
            let cause = weft_core::run::CancelCause::Runtime {
                detail: "the activation failed and rolled back its setup run".into(),
            };
            let rb = match rollback {
                ActivateRollback::Restore => match state.activations.restore(id, activation, &previous).await {
                    // The reserved run may not have started yet. Cancellation
                    // is harmless for an ownerless run and fences any born run.
                    Ok(true) => crate::api::run::cancel_run_id(state, activation, &cause)
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_run_id: {e}"))),
                    Ok(false) => Ok(()),
                    Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("restore the triggers: {e}"))),
                },
                // The activation failed on its own; the setup run it
                // spawned is cancelled by that failure, not by anyone's
                // decision.
                ActivateRollback::WipeSignals => wipe_activating_state(state, id, activation, &cause).await,
            };
            if let Err((rb_status, rb_msg)) = rb {
                tracing::error!(
                    target: "weft_dispatcher::activate",
                    project_id = %id, %activation, rb_status = %rb_status, rb_error = %rb_msg,
                    "activate rollback failed; the lapses loop will repair \
                     the activation once its heartbeat goes stale"
                );
            }
            // Broadcast whatever state the rollback landed (inactive on
            // success, or the still-stuck state the reaper will repair).
            crate::transition::publish_transition_changed(state, id).await;
            return Err(ActivateError::Failed(status, msg));
        }
    };
    // Past here the activations are Active (the window's final step
    // flipped them), so nothing fails the call.

    // Hand over every event those activations' signals kept through their
    // inactive window, now that the triggers take them, whatever they
    // waited on. One this pass could not hand over stays queued with its
    // next turn, which the sweep (`crate::parked_drain::drain_due`) is
    // booked for.
    if let Err(e) = crate::parked_drain::drain_activations(state, id, &keys).await {
        tracing::error!(
            target: "weft_dispatcher::activate",
            project_id = %id, %activation, error = %format!("{e:#}"),
            "the triggers are active, but handing over the events they kept failed; \
             what is left waits in their queues for the sweep's next look or the next activation"
        );
    }

    for url in &urls {
        state
            .events
            .publish(DispatcherEvent::TriggerUrlChanged {
                project_id: id,
                // User strings on a NOTIFY-path event: node ids are
                // user-authored and the url embeds the user's mount
                // path. Bound at construction.
                node_id: weft_core::truncate_user_string(&url.node_id, 4096),
                url: weft_core::truncate_user_string(&url.url, 4096),
            })
            .await;
    }
    state
        .events
        .publish(DispatcherEvent::ProjectActivated { project_id: id })
        .await;
    let infra_not_running = (!idle.is_empty()).then(|| {
        let (listed, fix) = missing_and_fix(&idle);
        format!("infra not running: {listed}. Activating does not start it; if anything uses it, run {fix}.")
    });
    // The program the project now runs answers its callers at the
    // project's own address, straight (`crate::front`).
    let address = crate::front::serve_or_log(state, id, port).await;
    Ok(Json(ActivateResponse {
        urls,
        infra_not_running,
        per_instance_left_out: scope.per_instance_left_out(&project),
        address,
        broken: project.broken.clone(),
    }))
}

/// The project's own address, as its front last answered it.
async fn project_address(state: &DispatcherState, id: uuid::Uuid) -> Option<weft_core::projects::ProjectAddress> {
    crate::front::address(state, id).await.unwrap_or_else(|e| {
        tracing::warn!(target: "weft_dispatcher::api::project", project_id = %id, error = %format!("{e:#}"), "could not read the project's address");
        None
    })
}

/// Sweep this owner's entry-trigger rows whose trigger no longer exists
/// in the source. Called after TriggerSetup so any trigger the user
/// removed while it was down has its leftover signal row dropped.
/// Resume rows (per-suspension) skip this: the corresponding suspended
/// run is the source of truth for them. None while a part that does not
/// compile may hide triggers (`ProjectDefinition::hides_nodes`): a row
/// missing from such a program may be a trigger inside it, and dropping
/// it would kill an address the person still has.
async fn orphan_entry_tokens(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
) -> anyhow::Result<Vec<String>> {
    if project.hides_nodes() {
        return Ok(Vec::new());
    }
    let triggers: HashSet<String> = weft_core::project::trigger_places(project)
        .iter()
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    Ok(state
        .journal
        .signal_list_for_project(project_id)
        .await?
        .into_iter()
        .filter(|s| !s.is_resume && s.instance.as_ref() == instance && !triggers.contains(&s.node_id))
        .map(|s| s.token)
        .collect())
}

/// Delete the entry rows [`orphan_entry_tokens`] named, while the
/// activation still owns its triggers, and have the listener forget them.
async fn drop_orphan_entry_rows(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    orphans: &[String],
    activation: uuid::Uuid,
) -> anyhow::Result<()> {
    let mut tx = activation_write(state, project_id, activation).await?;
    let removed = crate::journal::postgres::remove_signals(&mut *tx, orphans).await?;
    tx.commit().await?;
    state.listener.unregister_many(&removed).await;
    Ok(())
}

/// The failure of an activate window step from arming on: any step
/// before the one delete, and the flip to Active after it (the orphan
/// rows it deleted belong to triggers the source no longer has, so they
/// stay gone either way). Every trigger armed is taken back (a row it
/// created goes, a row it replaced is written back and the listener runs
/// it again) and the triggers are put back as they were. When one cannot
/// be put back, `if_not_put_back` says what becomes of them (see the
/// window).
async fn take_back_arms(
    state: &DispatcherState,
    if_not_put_back: ActivateRollback,
    armed: Vec<crate::task_kinds::register_signal::Armed>,
    msg: String,
) -> ActivateWindowError {
    let status = StatusCode::INTERNAL_SERVER_ERROR;
    match crate::task_kinds::register_signal::disarm(state.journal.as_ref(), &state.listener, armed).await {
        Ok(()) => ActivateWindowError { status, msg, rollback: ActivateRollback::Restore },
        Err(undo) => {
            let fate = match if_not_put_back {
                ActivateRollback::Restore => "so the triggers stay down as they were, with what they kept",
                ActivateRollback::WipeSignals => "so the triggers are turned off",
            };
            ActivateWindowError {
                status,
                msg: format!("{msg}; putting back the triggers armed before it failed, {fate}: {undo:#}"),
                rollback: if_not_put_back,
            }
        }
    }
}

/// Hold the activation identity while changing its persistent signal
/// state: the rows it claimed stay Activating under it for the whole
/// transaction, so a concurrent cancel waits behind this write.
async fn activation_write(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    activation: uuid::Uuid,
) -> anyhow::Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut tx = state.pg_pool.begin().await?;
    let owned: Vec<(String,)> = sqlx::query_as(
        "SELECT trigger FROM trigger_activation \
         WHERE project_id = $1 AND status = 'activating' AND activating_run_id = $2 FOR UPDATE",
    ).bind(project_id).bind(activation).fetch_all(&mut *tx).await?;
    anyhow::ensure!(!owned.is_empty(), "activation {activation} no longer owns any trigger of project {project_id}");
    Ok(tx)
}

/// Apply an activate `reactivate_choice`'s destructive effect on the
/// project's parked/suspended signal state. Run AFTER the
/// single-flight CAS so a losing concurrent activate can't wipe the
/// winner's state.
///   - `execute_parked_keep_suspended`: no-op (the drain at the end
///     of activate replays every parked fire).
///   - `keep_suspended_only`: clear parked fires (a person's answers to
///     waiting runs among them), keep the waits.
///   - `wipe_all`: drop every signal row.
///
/// Answers whether these activations still govern signals they kept (an
/// entry row of a trigger that was down, a wait of a run its trigger
/// fired), which a failed activation must never cost.
async fn apply_reactivate_choice(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
    choice: ReactivateChoice,
) -> Result<bool, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // What these activations kept: their signals, and the runs their
    // triggers fired. Another trigger's, another instance's, and a run
    // started by hand are not this activation's to touch.
    let governed: Vec<String> = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, keys)
        .await
        .map_err(|e| internal("activation signals", e))?
        .into_iter()
        .map(|s| s.token)
        .collect();
    let mut tx = activation_write(state, project_id, activation).await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
    let mut cancelled = Vec::new();
    let mut removed = Vec::new();
    match choice {
        ReactivateChoice::ExecuteParkedKeepSuspended => {}
        ReactivateChoice::KeepSuspendedOnly => {
            sqlx::query("DELETE FROM parked_fire WHERE token = ANY($1) AND run_id IS NULL")
                .bind(&governed)
                .execute(&mut *tx)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("clear parked: {e}")))?;
        }
        ReactivateChoice::WipeAll => {
            let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
            let runs = crate::take_down::live_runs(state, project_id).await.map_err(|e| internal("live runs", e))?;
            cancelled = crate::take_down::affected_runs(&target, &runs, None).into_iter().map(|r| r.run_id).collect();
            removed = crate::journal::postgres::remove_signals(&mut *tx, &governed).await
                .map_err(|e| internal("clear activation signals", e))?;
        }
    }
    tx.commit().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("apply activation choice: {e}")))?;
    state.listener.unregister_many(&removed).await;
    let cause = weft_core::run::CancelCause::User;
    let targets: Vec<_> = cancelled.iter().map(|run_id| (*run_id, &cause)).collect();
    crate::api::run::cancel_run_ids(state, &targets).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation's prior runs: {e}")))?;
    Ok(choice != ReactivateChoice::WipeAll && !governed.is_empty())
}

/// End the named activation and delete its triggers' entry rows atomically
/// (the waits of runs those triggers fired stay with their runs). Listener
/// cleanup receives only those deleted rows, so a newer activation's tokens
/// survive. The caller supplies the cause of the setup run's cancellation.
pub(crate) async fn wipe_activating_state(
    state: &DispatcherState,
    id: uuid::Uuid,
    activation: uuid::Uuid,
    cause: &weft_core::run::CancelCause,
) -> Result<(), (StatusCode, String)> {
    let Some(removed) = state.activations.end_activating(
        id, activation, &crate::activation_store::ActivationLifecycle::wiped(), true, None,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation: {e}")))? else {
        return Ok(());
    };
    state.listener.unregister_many(&removed).await;
    crate::api::run::cancel_run_id(state, activation, cause)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation {id}: {e}")))?;
    Ok(())
}

/// End an activation nobody will finish (a person pressed Cancel, or its
/// driver died: `crate::lapses`), its setup run cancelled for `cause`: its
/// triggers land parked with what they kept, or wiped when they kept no
/// work (`ActivationStoreOps::end_unfinished`). An activation already
/// ended (its driver finished, or another cancel) is left as it is.
pub(crate) async fn end_unfinished_activation(
    state: &DispatcherState,
    id: uuid::Uuid,
    activation: uuid::Uuid,
    cause: &weft_core::run::CancelCause,
) -> Result<(), (StatusCode, String)> {
    let Some(removed) = state
        .activations
        .end_unfinished(id, activation)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("end the activation: {e:#}")))?
    else {
        return Ok(());
    };
    state.listener.unregister_many(&removed).await;
    crate::api::run::cancel_run_id(state, activation, cause)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation {id}: {e:#}")))
}

/// Every owner of project `id` with at least one trigger on, each once:
/// the program itself first, then instances in id order. The activation
/// rows are the whole answer, so nothing is guessed. What a plain resync
/// brings up to date, and (instances only) what `deactivate --all-instances`
/// takes down and what a plain deactivate reports as still on.
pub async fn owners_with_triggers_on(
    activations: &dyn crate::activation_store::ActivationStoreOps,
    id: uuid::Uuid,
) -> Result<Vec<weft_core::instance::Owner>, (StatusCode, String)> {
    let rows = activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let live = rows.iter().filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active).map(|a| &a.key);
    Ok(weft_core::activation::owners_of(live))
}

/// The instances among [`owners_with_triggers_on`].
async fn instances_with_triggers_on(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<Vec<weft_core::instance::InstanceId>, (StatusCode, String)> {
    let owners = owners_with_triggers_on(state.activations.as_ref(), id).await?;
    Ok(owners.into_iter().filter_map(|owner| owner.instance().cloned()).collect())
}

// `DeactivationMode` is the wire contract for the `mode` field on a
// `DeactivateSpec`. It lives in `weft-broker-client::protocol` so
// the supervisor + dispatcher share one source of truth. Re-export
// here so this module stays the canonical home for deactivation glue.
pub use weft_broker_client::protocol::DeactivationMode;

/// Take down `keys` with the person's choice of how (Stop, Terminate and
/// Upgrade send it for the triggers their copies feed), marked
/// `went_down_with` when what takes them down is not the person.
/// Validation lives on `DeactivateSpec::validate`
/// (broker, dispatcher, supervisor share one validator); this site adds
/// the `triggerDeactivation:` prefix so clients see which field tripped
/// the check.
pub async fn execute_trigger_deactivation(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: Vec<weft_core::activation::ActivationKey>,
    spec: &weft_broker_client::protocol::DeactivateSpec,
    went_down_with: Option<crate::take_down::DownWith>,
) -> Result<(), (StatusCode, String)> {
    spec.validate()
        .map_err(|m| (StatusCode::BAD_REQUEST, format!("triggerDeactivation: {m}")))?;
    let target = crate::take_down::TakeDownTarget::Activations(keys);
    let taken = crate::take_down::take_down(state, id, &target, spec, went_down_with, crate::take_down::TakeDownAsker::Outside).await?;
    if taken == crate::take_down::TakenDown::ProjectGone {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "triggerDeactivation: project disappeared mid-deactivate".into(),
        ));
    }
    Ok(())
}

pub async fn deactivate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<DeactivateRequest>,
) -> Result<Json<DeactivateResponse>, StatusError> {
    // Marked gate: deactivate is part of teardown flows (`weft rm
    // --journal` quiesces through it), so its "no such project" must
    // be tellable apart from a routing 404, same as `remove`/`status`.
    authorize_project_marked(&state, &caller.0, id).await?;
    body.spec.validate().map_err(|m| (StatusCode::BAD_REQUEST, m.to_string()))?;
    let rows = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let owners: Vec<Option<weft_core::instance::InstanceId>> = if body.all_instances {
        if let Some(instance) = &body.scope.instance {
            return Err(StatusError::Other(
                StatusCode::BAD_REQUEST,
                format!(
                    "--all-instances takes down every instance's triggers and --instance names one \
                     ('{instance}'); pass one of the two"
                ),
            ));
        }
        instances_with_triggers_on(&state, id).await?.into_iter().map(Some).collect()
    } else {
        vec![body.scope.instance.clone()]
    };
    let mut deactivated = Vec::with_capacity(owners.len());
    for instance in owners {
        let scope = weft_core::activation::ActivationScope { triggers: body.scope.triggers.clone(), instance };
        let keys = keys_to_take_down(&rows, &scope)?;
        let target = crate::take_down::TakeDownTarget::Activations(keys);
        // user-initiated (the standalone Deactivate verb)
        let taken = crate::take_down::take_down(&state, id, &target, &body.spec, None, crate::take_down::TakeDownAsker::Outside).await?;
        if taken == crate::take_down::TakenDown::ProjectGone {
            // Vanished between the gate and the deactivate (a concurrent
            // rm): same "no project I may see" answer, marker included.
            return Err(StatusError::NotMyProject);
        }
        deactivated.push(scope.owner());
    }
    let instances_still_on = instances_with_triggers_on(&state, id).await?;
    Ok(Json(DeactivateResponse { deactivated, instances_still_on }))
}

/// `POST /projects/{id}/resync`. Deactivate-then-activate the named
/// activations against (optionally) fresh source / infra hashes,
/// bringing the deployed trigger/worker shape in line with the current
/// source.
///
/// A scope naming an instance, or triggers, is that one owner's. The plain
/// scope (nothing named) is every owner with a trigger on: the program's
/// shared triggers when any is on, then each such instance in id order,
/// each taken as a whole, exactly as `weft resync` / `weft resync
/// --instance <id>` would for it alone. The activation rows say who has
/// triggers on, so nothing is guessed.
///
/// The deactivation uses the USER'S spec (never a hardcoded wipe): with
/// `runningPolicy = wait` each owner drains through THE shared drain
/// loop up to the spec's cap, cancels the stragglers, lands, then
/// reactivates, carrying over what the take-down kept (parked fires run,
/// runs waiting on a person keep waiting): the person is never asked
/// about it here. Every refusal the request can meet up front (an owner
/// whose triggers are not all settled on, a build or infra change in
/// flight, another build registered since the caller's, infra a trigger
/// reads that is not running against the definition this resync builds)
/// comes BEFORE the first deactivation, so triggers it cannot bring back
/// are left exactly as they were; and the project holds its `resyncing`
/// transition from then to the end, so no build starts in between.
///
/// Only the triggers that are on are taken down and brought back: one
/// that was off (parked, hibernated, never activated) stays as it was.
///
/// The take-down and the bring-back are done by a task of their own
/// (`crate::task_kinds::resync`), which carries on whatever becomes of
/// this request; the request follows it and answers as it ended. A
/// project holding a resync already (under way, or stopped with triggers
/// down) refuses this: [`resume_resync`] takes that one up again.
pub async fn resync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<ResyncRequest>>,
) -> Result<Json<ResyncResponse>, StatusError> {
    use crate::activation_store::ProjectStatus;
    use weft_core::activation::ActivationScope;
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let tenant = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;

    // 1. Whose triggers: each owner's that are on, and only those.
    //    Resync means "the listeners fire an older program; register
    //    them against this one": on a hibernated or parked trigger there
    //    is nothing registered to bring across, and silently turning it
    //    into an activate would revive something the user parked on
    //    purpose. A trigger the person names must be on.
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    let activations = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let scopes: Vec<ActivationScope> = if body.scope == ActivationScope::shared() {
        let owners = owners_with_triggers_on(state.activations.as_ref(), id).await?;
        if owners.is_empty() {
            return Err(StatusError::Other(
                StatusCode::CONFLICT,
                "no trigger of this program is on, and resync only re-registers triggers that \
                 are on; `weft activate` brings them up on the current source"
                    .into(),
            ));
        }
        owners
            .into_iter()
            .map(|owner| ActivationScope { triggers: Vec::new(), instance: owner.instance().cloned() })
            .collect()
    } else {
        vec![body.scope.clone()]
    };
    let is_on = |key: &weft_core::activation::ActivationKey| {
        activations.iter().any(|a| &a.key == key && a.lifecycle.status == ProjectStatus::Active)
    };
    let mut owners = Vec::with_capacity(scopes.len());
    for scope in &scopes {
        let keys = resolve_scope(&registered, scope)?;
        let off: Vec<String> = keys.iter().filter(|key| !is_on(key)).map(|key| key.trigger.clone()).collect();
        let on: Vec<String> = keys.iter().filter(|key| is_on(key)).map(|key| key.trigger.clone()).collect();
        if on.is_empty() || (!scope.triggers.is_empty() && !off.is_empty()) {
            let what = if on.is_empty() { whose_triggers(&scope.owner()) } else { format!("{} ({})", whose_triggers(&scope.owner()), off.join(", ")) };
            return Err(StatusError::Other(
                StatusCode::CONFLICT,
                format!(
                    "{what} are not on, and resync only re-registers triggers that are on; \
                     `weft activate` brings them up on the current source"
                ),
            ));
        }
        owners.push(crate::task_kinds::resync::ResyncOwner { owner: scope.owner(), on, down: Vec::new() });
    }
    let Some(spec) = body.trigger_deactivation.as_ref() else {
        return Err(StatusError::NeedsTriggerChoice(weft_core::trigger_choice_required(
            "resync takes the triggers that are on down before registering them again",
        )));
    };
    // 2. Refuse BEFORE tearing anything down. Resync takes the triggers
    //    off and puts them back; a refusal after the first half leaves
    //    them down over a condition it could have checked while they
    //    were still up: a build or an infra change in flight, triggers
    //    mid-flip, a build other than the caller's registered meanwhile,
    //    infra a trigger reads not running, a connection not picked. The
    //    question is about the definition the last build registered,
    //    which is what this resync re-arms. Every owner is checked before
    //    the first comes down. Each activate below re-reads and re-checks;
    //    this is what keeps the project whole.
    for scope in &scopes {
        require_action(&state, id, Some(scope), &["resync"]).await?;
    }
    require_registered_build(&state, id, &body.build).await?;
    let built = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".to_string()))?;
    // The fires are judged on the program as its worker runs it, what it
    // left out included.
    let (_, runnable) = coherent_definition(&state, id).await?;
    for owner in &owners {
        require_trigger_infra(&state, id, &built, &owner.keys(&owner.on)).await?;
        refuse_unfireable(&state, id, &runnable, &owner.keys(&owner.on)).await?;
    }
    crate::install_picks::require_activation_picks(&state, id, &built).await?;

    // 3. Write the resync down and enter the project's `resyncing`
    //    transition, which it holds until every owner's triggers are back:
    //    no build lands a version, and no other activation starts, between
    //    an owner's take-down and its turn back on. Its task does the work,
    //    owner by owner, the program's first, whatever becomes of this
    //    request.
    let plan = crate::task_kinds::resync::ResyncPlan {
        driver: uuid::Uuid::now_v7(),
        owners,
        spec: spec.clone(),
        build: body.build.clone(),
        resynced: Vec::new(),
        urls: Vec::new(),
        stopped: None,
        given_up: None,
    };
    let task = crate::task_kinds::resync::task(&tenant, id, plan.driver);
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("resync transition: {e:#}"));
    let Some(task) = state.projects.try_begin_resyncing(id, &plan, task).await.map_err(internal)? else {
        let held = state.projects.transition(id).await.map_err(internal)?.unwrap_or(crate::project_store::ProjectTransition::None);
        if let Some(why) = held.refusal() {
            return Err(StatusError::Other(StatusCode::CONFLICT, format!("cannot resync now: {why}")));
        }
        require_registered_build(&state, id, &body.build).await?;
        return Err(StatusError::Other(
            StatusCode::CONFLICT,
            "cannot resync now: a trigger of the project is being switched on or off; try again once it has landed".into(),
        ));
    };
    crate::transition::publish_transition_changed(&state, id).await;
    Ok(follow_resync(&state, id, task).await?)
}

/// `POST /projects/{id}/resync/resume`: take the project's resync up
/// again, where it stopped, with the take-down choice and the build it
/// began with, and follow it as [`resync`] does. One still going is
/// followed as it is. Refused when the project holds no resync: it never
/// starts one.
pub async fn resume_resync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<ResyncResponse>, StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("take the resync up again: {e:#}"));
    let tenant = state.projects.tenant_for(id).await.map_err(internal)?.ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    let Some(task) = state.projects.resume_resync(id, &tenant).await.map_err(internal)? else {
        return Err(StatusError::Other(
            StatusCode::CONFLICT,
            "this project holds no resync to take up again: it finished, or was given up with `weft cancel-activate`; \
             `weft resync` starts a new one"
                .into(),
        ));
    };
    crate::transition::publish_transition_changed(&state, id).await;
    Ok(follow_resync(&state, id, task).await?)
}

/// Follow the task `driver` that drives `id`'s resync to its end, and
/// answer as it ended: what it brought back, or why it stopped (the
/// triggers it had not brought back down, the project held for it until
/// `weft resync --resume` takes it up again or `weft cancel-activate`
/// gives it up).
async fn follow_resync(state: &DispatcherState, id: uuid::Uuid, driver: uuid::Uuid) -> Result<Json<ResyncResponse>, (StatusCode, String)> {
    let internal = |what: &str, e: String| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e}"));
    let outcome = weft_task_store::terminal::wait_for_terminal_in_project(&state.pg_pool, &state.signals, driver, id, std::time::Duration::MAX)
        .await
        .map_err(|e| internal("follow the resync", format!("{e:#}")))?
        .ok_or_else(|| internal("follow the resync", format!("its task {driver} is gone")))?;
    if outcome.status == weft_core::task::TaskStatus::Complete {
        let done = outcome.result.ok_or_else(|| internal("the resync's answer", "it ended with none".into()))?;
        return serde_json::from_value(done).map(Json).map_err(|e| internal("the resync's answer", e.to_string()));
    }
    let held = state.projects.resync(id).await.map_err(|e| internal("read the resync", format!("{e:#}")))?;
    let error = outcome.error.unwrap_or_else(|| "the resync ended without saying why".into());
    Err(match held.and_then(|held| held.plan.stopped) {
        Some(stopped) => {
            let by_itself = match stopped.waits_for {
                Some(crate::task_kinds::resync::ResyncWait::Infra) => " It goes on by itself once that infra runs;",
                Some(crate::task_kinds::resync::ResyncWait::Activation) => " It goes on by itself once that activation has landed;",
                None => "",
            };
            (
                StatusCode::from_u16(stopped.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                format!(
                    "{}. The resync stopped with these triggers down, and holds the project until it finishes.{by_itself} \
                     `weft resync --resume` tries it again, `weft cancel-activate` leaves them down",
                    stopped.error
                ),
            )
        }
        None => (StatusCode::CONFLICT, error),
    })
}

/// The next owner's resync, for its task (`crate::task_kinds::resync`):
/// write down which of the owner's triggers that were on when the resync
/// began are still on, take those down with the person's spec, wait out
/// the drain when the spec asked to, then turn back on exactly the ones it
/// took down, on the plan's build, carrying over what the take-down kept.
/// Answers the addresses the reactivation answers on. Run again over an
/// owner it got part way through (its task was run again), it takes down
/// only what is still on, and brings back everything it wrote down.
pub(crate) async fn resync_owner(
    state: &DispatcherState,
    id: uuid::Uuid,
    plan: &mut crate::task_kinds::resync::ResyncPlan,
) -> Result<Vec<weft_core::activation::ActivationUrl>, crate::task_kinds::resync::Halt> {
    use crate::activation_store::{ClaimRefused, ProjectStatus};
    use crate::task_kinds::resync::{Halt, ResyncWait};
    let internal = |what: &'static str| move |e: anyhow::Error| Halt::from((StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}")));
    let at = plan.resynced.len();
    let owner = plan.owners[at].clone();
    let rows = state.activations.list(id).await.map_err(internal("activations"))?;
    let status_of = |trigger: &String| {
        let key = weft_core::activation::ActivationKey::new(trigger.clone(), owner.owner.clone());
        rows.iter().find(|a| a.key == key).map(|a| a.lifecycle.status)
    };
    if owner.on.iter().any(|trigger| status_of(trigger) == Some(ProjectStatus::Activating)) {
        return Err(Halt::waiting(
            ResyncWait::Activation,
            (StatusCode::CONFLICT, "an activation of these triggers is still going; the resync brings them back once it has landed".into()),
        ));
    }
    // Written down before they come down, so a run of the task again
    // brings back exactly these, and never one that was off.
    let still_on: Vec<String> = owner.on.iter().filter(|trigger| status_of(trigger) == Some(ProjectStatus::Active)).cloned().collect();
    let newly: Vec<String> = still_on.iter().filter(|trigger| !owner.down.contains(trigger)).cloned().collect();
    if !newly.is_empty() {
        plan.owners[at].down.extend(newly);
        if !state.projects.write_resync(id, plan).await.map_err(internal("write down the triggers it takes down"))? {
            return Err(Halt::Given);
        }
    }
    let owner = plan.owners[at].clone();
    let spec = plan.spec.clone();
    // The person's own take-down, marked with no cause: triggers a
    // stopped resync leaves down are never brought back by the infra they
    // read coming up. It goes through only while this drive holds the
    // project, decided in the same transaction as the lifecycle write: a
    // person who gave the resync up meanwhile finds every trigger it had
    // not taken down still on, and is told exactly what stays down.
    if !still_on.is_empty() {
        let target = crate::take_down::TakeDownTarget::Activations(owner.keys(&still_on));
        let asker = crate::take_down::TakeDownAsker::Resync(plan.driver);
        match crate::take_down::take_down(state, id, &target, &spec, None, asker).await? {
            crate::take_down::TakenDown::Done => {}
            crate::take_down::TakenDown::ResyncGone => return Err(Halt::Given),
            crate::take_down::TakenDown::ProjectGone => {
                return Err(Halt::from((StatusCode::NOT_FOUND, "the project was removed during its resync".to_string())));
            }
        }
    }
    if owner.down.is_empty() {
        return Ok(Vec::new());
    }
    let down = owner.keys(&owner.down);
    if spec.drains() {
        let cap = spec
            .drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
        let reach = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Triggers(&down), except: None };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(cap);
        crate::drain::drain(state, &reach, Some(deadline), spec.running_policy, None).await.map_err(internal("drain"))?;
        for key in &down {
            crate::drain::land_if_drained(state, id, key).await.map_err(internal("land the drain"))?;
        }
    }
    // The infra they read, checked apart so a stop over it waits for that
    // infra to run again ([`ResyncWait::Infra`]).
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(internal("project"))?
        .ok_or_else(|| Halt::from((StatusCode::NOT_FOUND, "project not found".to_string())))?;
    require_trigger_infra(state, id, &registered, &down).await.map_err(|refused| Halt::waiting(ResyncWait::Infra, refused))?;

    // Reactivate, through the same path as an activate (hash
    // persistence, cleanup on failure), past the activate handler's gates:
    // the resync checked them before its take-down, and its claim is the
    // one that goes through while it holds the project, only while this
    // drive holds it (a person who gave it up meanwhile leaves the
    // triggers down). It asks nobody what to do with what the take-down
    // kept: like every combined off-and-on, it carries it over (the
    // default `ReactivateChoice`). The person answered "what about the
    // running runs" ONCE, in the picker, and the answer, kept in the
    // plan, governs the reactivate's worker replacement as much as the
    // trigger-side drain above. The port stays the one the project has (a
    // resync names none). Triggers already back (an earlier run of the
    // task brought them back, and stopped before writing so) answer that
    // they are on.
    let target = ActivationTarget { build: plan.build.clone(), reactivate_choice: None };
    let request = ActivateRequest { target, running: spec.running_choice(), scope: owner.scope(&owner.down), port: None };
    match activate_with(state, id, request, ActivateAsker::Resync(plan.driver)).await {
        Ok(Json(activated)) => Ok(activated.urls),
        Err(ActivateError::Failed(StatusCode::CONFLICT, msg)) if msg == ALREADY_ON => Ok(Vec::new()),
        Err(ActivateError::Refused(ClaimRefused::ResyncGone)) => Err(Halt::Given),
        Err(ActivateError::Refused(ClaimRefused::Removing)) => Err(Halt::Removing),
        Err(error) => Err(<(StatusCode, String)>::from(error).into()),
    }
}

/// Settle what the project's workers still run on an older image, once a
/// new image went live.
///
/// Every run runs on the image it was born with (its run row
/// carries the binary hash), so a new image never disturbs anything by
/// itself: new runs go to the new image's workers, and one parked on
/// the old image resumes there. What the person chooses is the fate of the
/// work going on an older image right now (`crate::drain`, the runs the
/// older images' workers drive and the ones queued for them):
/// `RunningPolicy::Wait` waits for it to end, up to `drain_timeout_secs`,
/// then cancels what is left; `RunningPolicy::Cancel` cancels it at once.
/// Then the platform is told to have the new image's workers ready
/// (`Runner::prepare`).
///
/// Because Wait can sit for minutes, callers MUST NOT invoke this while
/// holding the per-project advisory lock. Idempotent.
pub async fn reconcile_worker(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    running_policy: crate::infra_lifecycle_command::RunningPolicy,
    drain_timeout_secs: u64,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("reconcile_worker: {e:#}"));
    let Some(want_hash) = state.projects.running_binary_hash(project_id).await.map_err(internal)? else {
        return Ok(());
    };
    let scope = crate::drain::DrainScope { project_id, reaching: crate::drain::Reaching::OtherImages(&want_hash), except: None };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(drain_timeout_secs);
    let drained = crate::drain::drain(state, &scope, Some(deadline), running_policy, None).await.map_err(internal)?;
    if drained != crate::drain::Drained::Empty {
        tracing::info!(
            target: "weft_dispatcher::api::project",
            %project_id,
            ?drained,
            policy = running_policy.as_str(),
            "a new image went live; what still ran on older ones was cancelled"
        );
    }
    prepare_running_image(state, project_id, &want_hash).await
}

/// Tell the platform to have the workers of the image `binary_hash`
/// ready (`Runner::prepare`).
async fn prepare_running_image(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    binary_hash: &str,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("prepare the workers: {e:#}"));
    let tenant = state.tenant_router.tenant_for_project(project_id).await.map_err(internal)?;
    let target = crate::delivery::worker_target(state, tenant.as_str(), project_id, binary_hash).await.map_err(internal)?;
    state.runner.prepare(&target).await.map_err(internal)
}

/// Take the whole project down for good: every activation of every
/// owner and every live run, wiped (project removal). Returns true if
/// the project existed.
pub async fn deactivate_project(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<bool, (StatusCode, String)> {
    let spec = weft_broker_client::protocol::DeactivateSpec {
        mode: DeactivationMode::Wipe,
        grace_minutes: 0,
        running_policy: crate::infra_lifecycle_command::RunningPolicy::Cancel,
        drain_timeout_secs: None,
    };
    let taken = crate::take_down::take_down(
        state,
        id,
        &crate::take_down::TakeDownTarget::WholeProject,
        &spec,
        None,
        crate::take_down::TakeDownAsker::Outside,
    )
    .await?;
    Ok(taken != crate::take_down::TakenDown::ProjectGone)
}

/// Wait until every last call the project's cancelled runs queued
/// (`weft_task_store::TaskKind::CancelCall`) has gone out, or failed (its
/// task logs why): what `weft rm` deletes next includes what a call needs
/// to go out (an instance's connections, the project's infra addresses).
/// No deadline: each call is bounded where it is sent
/// (`weft_listener::kinds::cancel_call`), and how long the queue takes to
/// reach it is not this call's to cut short.
async fn send_last_calls(state: &DispatcherState, id: uuid::Uuid) -> Result<(), StatusError> {
    let calls: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT id FROM task WHERE project_id = $1 AND kind = $2 AND status IN ('pending', 'claimed')",
    )
    .bind(id)
    .bind(weft_task_store::TaskKind::CancelCall.as_str())
    .fetch_all(&state.pg_pool)
    .await
    .map_err(|e| StatusError::Other(StatusCode::SERVICE_UNAVAILABLE, format!("could not read the project's last calls: {e}; retry `weft rm`")))?;
    for call in calls {
        weft_task_store::terminal::wait_for_terminal_in_project(&state.pg_pool, &state.signals, call, id, std::time::Duration::MAX)
            .await
            .map_err(|e| {
                StatusError::Other(StatusCode::SERVICE_UNAVAILABLE, format!("could not wait for a last call of a cancelled run: {e:#}; retry `weft rm`"))
            })?;
    }
    Ok(())
}

/// `POST /projects/{id}/quiesce`: take the whole project down (every
/// owner's triggers wiped, every live run cancelled, those started by
/// hand and unrecorded ones included) and answer once none of its runs
/// is live. What `weft rm --journal` asks before deleting the project's
/// history, so nothing is still writing a run it erases.
pub async fn quiesce(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, StatusError> {
    // Marked gate: rm tells "already gone" apart from a routing 404.
    authorize_project_marked(&state, &caller.0, id).await?;
    if !deactivate_project(&state, id).await? {
        return Err(StatusError::NotMyProject);
    }
    // Every run of the project, whatever copy it may use, waited for
    // with no deadline: how long a worker takes to let go of what it
    // was cancelled out of is not this call's to cut short.
    let scope = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Copies(&weft_core::instance::Copies::Every), except: None };
    crate::drain::drain(&state, &scope, None, crate::infra_lifecycle_command::RunningPolicy::Wait, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("waiting for cancelled runs to end: {e:#}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /projects/{id}/cancel-running`. Force a waiting deactivation
/// to finish now: cancel every running, non-suspended run the
/// named deactivating activations are waiting for (every shared one by
/// default). The drain-watcher then lands them Inactive. Nothing to do
/// when none of them is deactivating.
pub async fn cancel_running(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<weft_core::activation::ActivationScope>>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let scope = body.map(|Json(s)| s).unwrap_or_default();
    let rows = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let keys = keys_to_take_down(&rows, &scope)?;
    let draining: Vec<weft_core::activation::ActivationKey> = rows
        .into_iter()
        .filter(|a| keys.contains(&a.key) && a.lifecycle.status == crate::activation_store::ProjectStatus::Deactivating)
        .map(|a| a.key)
        .collect();
    let scope = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Triggers(&draining), except: None };
    crate::drain::cancel_left(&state, &scope, &weft_core::run::CancelCause::User)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e:#}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /projects/{id}/cancel-build`. Cancel the in-flight build:
/// `transition` → cancelling_build, then every image build the project
/// started is stopped and its waiting version ends cancelled
/// (`VersionBuilder::cancel_project`), and the transition lands at rest
/// once nothing holds it (`crate::transition::settle`). A build request
/// still starting its builds is refused its next claim, and stops what it
/// started itself. Cancel reconciles, never asserts: the
/// response is 202 and the displayed state is whatever the backend
/// reports next (a build may still complete if it beat the cancel).
///
/// 412 when no build is in flight (stale tab; the client refetches
/// `/status` and reconciles).
pub async fn cancel_build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let flipped = state
        .projects
        .request_cancel_build(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("request_cancel_build: {e}")))?;
    if !flipped {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "no build in flight to cancel".into(),
        ));
    }
    crate::transition::publish_transition_changed(&state, id).await;
    state
        .builder
        .cancel_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("stop the project's builds: {e:#}")))?;
    crate::transition::settle(&state, Some(id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("settle the build transition: {e:#}")))?;
    Ok(StatusCode::ACCEPTED)
}

/// Cancel in-flight activations of the named triggers (every shared
/// one by default): each activation among them ends, its setup run
/// cancelled, the way every unfinished activation ends
/// ([`end_unfinished_activation`]): triggers that kept work (parked events,
/// runs waiting on a person) land parked with every signal kept, others
/// land wiped. With no trigger and no instance named, a resync the project
/// holds is given up too, under way or stopped, with its own activation
/// still going: the triggers it had not brought back stay down, the
/// answer names them, `weft status` shows them until they are on again or
/// the next resync begins, and builds go through again.
///
/// 409 for a cancel naming triggers or an instance while a resync holds
/// the project: the resync covers every owner's triggers, so only the
/// unnamed cancel gives it up. 404 for a named trigger with no activation.
/// 412 if none of them is activating and the project holds no resync: the
/// user (or a stale UI) clicked cancel against triggers that are already
/// up or already down.
pub async fn cancel_activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<weft_core::activation::ActivationScope>>,
) -> Result<Json<weft_core::activation::CancelActivateResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let scope = body.map(|Json(s)| s).unwrap_or_default();
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // Every shared trigger, nothing named: the only scope a resync can be
    // given up in.
    let whole = scope == weft_core::activation::ActivationScope::shared();
    let given_up = if whole {
        // Given up first, in one transaction with the transition landing
        // and what it leaves down written into its plan: from here its
        // task writes, takes down and turns on nothing (its lifecycle
        // writes and its claim are refused), so the activations read below
        // are every one it started.
        let plan = state.projects.give_up_resync(id).await.map_err(|e| internal("give the resync up", e))?;
        if plan.is_some() {
            crate::transition::publish_transition_changed(&state, id).await;
        }
        plan
    } else {
        None
    };
    let rows = state.activations.list(id).await.map_err(|e| internal("activations", e))?;
    let mut keys = keys_to_take_down(&rows, &scope)?;
    // A resync takes down and brings back every owner's triggers, so one
    // trigger or one instance cannot leave it. Read after the rows: a
    // resync only begins while nothing activates, so one found absent here
    // started none of the activations those rows hold.
    if !whole
        && state.projects.resync(id).await.map_err(|e| internal("read the resync", e))?.is_some()
    {
        return Err((
            StatusCode::CONFLICT,
            "cancel-activate: a resync holds this project, and it covers every trigger of every owner, so it cannot be \
             cancelled for some of them. `weft cancel-activate` with no --trigger or --instance gives the whole resync up \
             (and names the triggers that stay down); `weft resync --resume` takes it up again if it stopped"
                .into(),
        ));
    }
    if let Some(plan) = &given_up {
        keys.extend(plan.owners.iter().flat_map(|owner| owner.keys(&owner.down)));
    }
    let activations: std::collections::BTreeSet<uuid::Uuid> = rows
        .into_iter()
        .filter(|a| keys.contains(&a.key))
        .filter_map(|a| a.lifecycle.activating_run_id)
        .collect();
    if activations.is_empty() && given_up.is_none() {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "cancel-activate: none of these triggers is activating".into(),
        ));
    }
    // The cause is the person's: they pressed Cancel.
    for activation in activations {
        end_unfinished_activation(&state, id, activation, &weft_core::run::CancelCause::User).await?;
    }
    state
        .events
        .publish(DispatcherEvent::ProjectDeactivated { project_id: id })
        .await;
    let resync_given_up = given_up.and_then(|plan| plan.given_up).map(|given_up| given_up.left_down);
    Ok(Json(weft_core::activation::CancelActivateResponse { resync_given_up }))
}

/// Spawn a worker for the TriggerSetup sub-run and block
/// until it settles. The run is recorded on the project row
/// before it starts, so a rollback (or a cancel-activate, or the
/// reaper) cancels just THIS run and never touches
/// suspended/running work from prior cycles.
async fn run_trigger_setup(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    // The activations whose triggers this setup captures; they share one
    // owner, whose run it is (an instance's triggers read that instance's
    // values).
    keys: &[weft_core::activation::ActivationKey],
    // The hash of the SAME definition the caller resolved `keys`
    // against (one `coherent_definition` pair). Reading the project
    // row's hash here instead would race a concurrent re-register:
    // kicks from shape A journaled under hash B.
    program: &weft_core::project::hash::ProgramIdentity,
    activation: Option<uuid::Uuid>,
    // An instance's change about to be stored, which this setup runs with in
    // place of what is stored (a store re-arming the triggers that read
    // it); nothing for every other setup.
    overlay: Option<&weft_core::instance::ValueChanges>,
) -> Result<crate::journal::TriggerBake, (StatusCode, String)> {
    let run_id = activation.unwrap_or_else(weft_core::new_run_id);
    let places = key_places(project, keys);
    let (cut, kicks) = setup_cut(project, &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "a trigger setup was asked for with no trigger to set up".to_string()))?;
    let run_program = runs_program(program, &cut)?;
    let instance = keys_instance(keys);
    // An instance's triggers are set up with what that instance provides.
    let instance_values = match instance {
        Some(instance) => instance_values_for_run(state, project_id, &cut, instance, overlay).await?,
        None => Default::default(),
    };
    let picks = picks_for_run(state, project_id, &cut).await?;

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to
    // the completion event.
    let mut events = state.events.subscribe_project(project_id).await;

    // The activation reserved this run when it began. A cancelled
    // driver cannot replace a newer activation's identity.
    let owned = |activations: Vec<crate::activation_store::Activation>| {
        activations.iter().any(|a| {
            a.lifecycle.status == crate::activation_store::ProjectStatus::Activating
                && a.lifecycle.activating_run_id == Some(run_id)
        })
    };
    let recorded = activation.is_none() || owned(
        state
            .activations
            .list(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read activation: {e}")))?,
    );
    if !recorded {
        return Err((
            StatusCode::CONFLICT,
            "the activation was cancelled before trigger setup started".into(),
        ));
    }

    // The birth is one transaction, so a start failure leaves no journal
    // rows and no task; the run the row recorded above then names a
    // run that never existed, which the rollback's cancel finds ownerless
    // and skips.
    let entry_node = setup_entry(project, &cut, &kicks);
    start_queued_run(
        state,
        program,
        &project.defaults,
        weft_journal::birth::Birth {
            run_id,
            project_id,
            phase: weft_core::context::Phase::TriggerSetup,
            entry_node: &entry_node,
            kicks: &kicks,
            program: &run_program,
            // A setup is handed nothing and stands nothing in.
            given: &Default::default(),
            binary_hash: &program.binary_hash,
            seed: None,
            source_version: None,
            instance: instance.map(|instance| weft_journal::birth::RunFor { instance, values: &instance_values }),
            picks: &picks,
            fired_trigger: None,
            stand_in: None,
            settings: weft_core::run_settings::RunSettings::bookkeeping(),
            at_unix: crate::lease::now_unix() as u64,
        },
        QueueAs { for_activation: activation.is_some(), watch_end: true, ..QueueAs::default() },
    )
    .await?;

    // Cancellation may happen immediately after the atomic birth commits.
    // Recheck ownership so this driver also cancels a run born during that race.
    let still_ours = activation.is_none() || owned(
        state
            .activations
            .list(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?,
    );
    if !still_ours {
        crate::api::run::cancel_run_id(
            state,
            run_id,
            &weft_core::run::CancelCause::Runtime {
                detail: "the activation ended before this trigger setup run started".into(),
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_run_id: {e}")))?;
        return Err((
            StatusCode::CONFLICT,
            "the activation was cancelled as trigger setup started".into(),
        ));
    }

    // No backend-imposed deadline. Trigger setup spans worker
    // spawn + image pull + fold + run + bridge wakeup; on a cold
    // install the legitimate path is just slow, and a hard timeout
    // would surface that as a 504 even though the work is still in
    // flight. The CLI / extension is the right layer to choose a
    // client-side patience budget.
    loop {
        match events.recv().await.map(|record| record.event) {
            Ok(crate::events::DispatcherEvent::RunCompleted { run_id: c, .. })
                if c == run_id =>
            {
                break;
            }
            Ok(crate::events::DispatcherEvent::RunFailed { run_id: c, error, .. })
                if c == run_id =>
            {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup failed: {error}"),
                ));
            }
            Ok(crate::events::DispatcherEvent::RunCancelled { run_id: c, reason, .. })
                if c == run_id =>
            {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup cancelled: {reason}"),
                ));
            }
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // Dropped batch may have held this run's terminal.
                // The journal is authoritative: re-query rather than
                // fail the trigger-setup on a transient lag.
                match crate::api::run::terminal_outcome(&state.pg_pool, run_id).await {
                    Ok(Some(crate::api::run::TerminalOutcome::Completed)) => break,
                    Ok(Some(crate::api::run::TerminalOutcome::Failed)) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "trigger setup failed".into(),
                        ))
                    }
                    Ok(Some(crate::api::run::TerminalOutcome::Cancelled)) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "trigger setup cancelled".into(),
                        ))
                    }
                    Ok(None) => continue, // still in flight; keep waiting
                    Err(e) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("trigger setup terminal lookup: {e}"),
                        ))
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "trigger setup event stream closed".into(),
                ));
            }
        }
    }
    let events = state.journal.events_log(run_id).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("read trigger setup: {error:#}")))?;
    let program = state.run_program_identity(&events).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")))?;
    let mut bake = crate::journal::TriggerBake::from_events(&events, &program)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| (StatusCode::CONFLICT, "trigger setup did not complete successfully".into()))?;
    bake.targets = keys.iter().map(|key| key.trigger.clone()).collect();
    state.journal.finish_trigger_setup(run_id, Some(&bake)).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("publish trigger bake: {error:#}")))?;
    Ok(bake)
}

/// After a trigger-setup sub-run, collect every persisted signal
/// for the project that has a user-facing URL. These become the
/// `urls` in ActivateResponse.
async fn collect_listener_urls(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<ActivationUrl>> {
    let signals = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, keys).await?;
    let mut out = Vec::new();
    for meta in signals {
        if meta.is_resume {
            continue;
        }
        if let Some(url) = meta.public_url(state.external_base_url()) {
            out.push(ActivationUrl {
                node_id: meta.node_id.clone(),
                url,
            });
        }
    }
    Ok(out)
}


// `crate::lease::now_unix` is the canonical wall-clock reader.

#[cfg(test)]
mod trigger_kick_tests {
    use serde_json::Value;
    use weft_core::run_spec::{compute_trigger_fire, TriggerFire};
    use super::*;
    use weft_core::project::{infra_places, trigger_places};

    /// Build a minimal ProjectDefinition from a JSON spec. Tests
    /// only care about node id, trigger status, the scope a node sits
    /// in, and edges; everything else is defaulted. The third element
    /// is the node's scope chain (empty at the top level). Every edge
    /// is `v -> v`: these tests are about which nodes a kick reaches,
    /// not which port feeds which.
    fn project(nodes: &[(&str, bool, &[&str])], edges: &[(&str, &str)]) -> ProjectDefinition {
        let wires: Vec<Value> = edges.iter().map(|(s, t)| wire(s, t, &[])).collect();
        project_with(nodes, wires, Vec::new())
    }

    /// A wire `source.v -> target.v` through the group ports `via`, each
    /// a group, its port and the side it is crossed on.
    fn wire(source: &str, target: &str, via: &[(&str, &str, &str)]) -> Value {
        serde_json::json!({
            "id": format!("e_{source}_{target}"),
            "source": source,
            "sourceHandle": "v",
            "target": target,
            "targetHandle": if via.last().is_some_and(|(group, _, _)| *group == target) { via.last().unwrap().1 } else { "v" },
            "via": via.iter().map(|(group, port, side)| serde_json::json!({ "group": group, "port": port, "side": side })).collect::<Vec<_>>(),
        })
    }

    fn project_with(nodes: &[(&str, bool, &[&str])], edges: Vec<Value>, groups: Vec<Value>) -> ProjectDefinition {
        let n_json: Vec<Value> = nodes.iter().map(|(id, is_trigger, scope)| serde_json::json!({
            "id": id,
            "nodeType": "T",
            "label": null,
            "config": {},
            "position": { "x": 0, "y": 0 },
            "scope": scope,
            "features": { "isTrigger": is_trigger },
        })).collect();
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": n_json,
            "edges": edges,
            "groups": groups
        })).expect("valid test project")
    }

    /// The group `g` holding `members`, with an input `x` and an output `y`.
    fn group_g(members: &[&str]) -> Value {
        let port = |name: &str| serde_json::json!({ "name": name, "portType": "Number", "required": false });
        serde_json::json!({ "id": "g", "kind": "group", "nodeIds": members, "inPorts": [port("x")], "outPorts": [port("y")] })
    }

    fn ids(kicks: &[Kick]) -> Vec<String> {
        let mut v: Vec<String> = kicks.iter().map(|k| k.node.clone()).collect();
        v.sort();
        v
    }

    /// Every place a program runs, spelled.
    fn sorted(program: &ProjectDefinition) -> Vec<String> {
        weft_core::project::places::running_places(program).iter().map(|place| place.to_string()).collect()
    }

    fn fire(p: &ProjectDefinition, node: &str, payload: Value) -> TriggerFire {
        compute_trigger_fire(p, node, &payload, None, &Default::default()).expect("the fire reaches an output")
    }

    /// The program and kicks of the setup of `targets`.
    fn setup(p: &ProjectDefinition, targets: Vec<weft_core::frames::Located>) -> Result<(ProjectDefinition, Vec<Kick>), String> {
        setup_cut(p, &targets).map(|cut| cut.expect("a setup with targets"))
    }

    #[test]
    fn trigger_only_upstream_node_is_skipped() {
        // A ──► TriggerX ──► B ──► Out
        // A feeds only the trigger; nothing past the trigger reaches Out
        // through A, so A must not run at fire time.
        let p = project(
            &[
                ("a", false, &[]),
                ("trigger_x", true, &[]),
                ("b", false, &[]),
                ("out", false, &[]),
            ],
            &[("a", "trigger_x"), ("trigger_x", "b"), ("b", "out")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["trigger_x".to_string()],
            "only the firing trigger should be a kick"
        );
        assert_eq!(kicks[0].payload, Some(Value::String("payload".into())));
        assert_eq!(
            sorted(&program),
            vec!["b".to_string(), "out".to_string(), "trigger_x".to_string()],
            "A feeds only the trigger, so it is outside the fire's subgraph"
        );
    }

    #[test]
    fn node_shared_with_non_trigger_path_runs() {
        //        ┌──► TriggerX ──► C ──► Out
        //  A ────┤
        //        └──► B ───────────────► Out
        // A feeds both the trigger AND B. B → Out is a non-trigger
        // path. A must run at fire time (via the B path).
        let p = project(
            &[
                ("a", false, &[]),
                ("trigger_x", true, &[]),
                ("b", false, &[]),
                ("c", false, &[]),
                ("out", false, &[]),
            ],
            &[
                ("a", "trigger_x"),
                ("a", "b"),
                ("trigger_x", "c"),
                ("c", "out"),
                ("b", "out"),
            ],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["a".to_string(), "trigger_x".to_string()],
            "A must run via its non-trigger path; trigger carries payload"
        );
        assert_eq!(sorted(&program), vec!["a", "b", "c", "out", "trigger_x"]);
        for k in &kicks {
            if k.node == "trigger_x" {
                assert_eq!(k.payload, Some(Value::String("payload".into())));
            } else {
                assert_eq!(k.payload, None);
            }
        }
    }

    #[test]
    fn non_firing_triggers_in_subgraph_get_no_payload() {
        // TriggerX ──► Out ◄── TriggerY
        // Firing TriggerX: TriggerY still gets kicked (without payload)
        // because it's reachable upstream from Out.
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("trigger_y", true, &[]),
                ("out", false, &[]),
            ],
            &[("trigger_x", "out"), ("trigger_y", "out")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("fire".into()));
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string(), "trigger_y".to_string()]);
        assert_eq!(sorted(&program), vec!["out", "trigger_x", "trigger_y"]);
        for k in &kicks {
            if k.node == "trigger_x" {
                assert_eq!(k.payload, Some(Value::String("fire".into())));
            } else {
                assert_eq!(k.payload, None);
            }
        }
    }

    #[test]
    fn a_fire_carves_out_the_other_trigger_s_own_path() {
        // TriggerX ──► NodeX      TriggerY ──► NodeY
        // Nothing joins the two, so firing one records a subgraph holding
        // only its own path: the graph view dims the rest of the program
        // for that run, the same way it does for an authored cut.
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("node_x", false, &[]),
                ("trigger_y", true, &[]),
                ("node_y", false, &[]),
            ],
            &[("trigger_x", "node_x"), ("trigger_y", "node_y")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()]);
        assert_eq!(sorted(&program), vec!["node_x", "trigger_x"]);
    }

    #[test]
    fn everything_the_trigger_reaches_runs() {
        // TriggerX ──► DeadEnd: nothing declares itself a deliverable
        // any more; whatever the fire reaches runs.
        let p = project(
            &[("trigger_x", true, &[]), ("dead_end", false, &[])],
            &[("trigger_x", "dead_end")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()]);
        assert_eq!(sorted(&program), vec!["dead_end", "trigger_x"]);
    }

    /// A trigger sitting in the middle of a group fires there: it is
    /// the run's one firing kick, the group comes whole around it (the
    /// sibling its output feeds, and the sibling's other input's source),
    /// and nothing outside the group is kicked to reach it but that source.
    #[test]
    fn a_trigger_in_the_middle_of_a_group_starts_the_run_from_itself() {
        // cfg ──► g.x ──► g.a ──► g.join ──► g.y ──► out
        //                 g.trig ─────────┘
        let p = project_with(
            &[
                ("cfg", false, &[]),
                ("g.a", false, &["g"]),
                ("g.trig", true, &["g"]),
                ("g.join", false, &["g"]),
                ("out", false, &[]),
            ],
            vec![
                wire("cfg", "g.a", &[("g", "x", "in")]),
                wire("g.a", "g.join", &[]),
                wire("g.trig", "g.join", &[]),
                wire("g.join", "out", &[("g", "y", "out")]),
            ],
            vec![group_g(&["g.a", "g.trig", "g.join"])],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "g.trig", serde_json::json!({ "hi": 1 }));
        assert_eq!(
            sorted(&program),
            vec!["cfg", "g.a", "g.join", "g.trig", "out"],
            "the run includes the join's dependencies and the group path"
        );
        let firing: Vec<&str> = kicks.iter().filter(|k| k.firing).map(|k| k.node.as_str()).collect();
        assert_eq!(firing, vec!["g.trig"], "the trigger is the one firing kick");
        assert_eq!(
            ids(&kicks),
            vec!["cfg".to_string(), "g.trig".to_string()],
            "the group's input source is kicked so g.join's other side arrives"
        );
    }

    #[test]
    fn a_scope_the_fire_touches_excludes_unrelated_instances() {
        // TriggerX ──► g.x ──► g.a ──► g.y ──► Out
        //                      g.seed (no wire feeds it)
        // Reaching the group's input follows only connected work. Its
        // unwired member is outside this trigger's program.
        let p = project_with(
            &[
                ("trigger_x", true, &[]),
                ("g.a", false, &["g"]),
                ("g.seed", false, &["g"]),
                ("out", false, &[]),
            ],
            vec![wire("trigger_x", "g.a", &[("g", "x", "in")]), wire("g.a", "out", &[("g", "y", "out")])],
            vec![group_g(&["g.a", "g.seed"])],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()], "the group's unwired member is not the run's root");
        assert_eq!(sorted(&program), vec!["g.a", "out", "trigger_x"]);
    }

    /// Each setup keeps its own target inside the group, which opens with
    /// nothing reaching it and starts that target itself. Neither the
    /// unused group input nor its source joins the setup.
    #[test]
    fn setup_phases_reach_a_trigger_or_infra_node_inside_a_group() {
        // cfg ──► g.x (read by nobody inside)
        //         g.db (infra), g.trig (trigger), both unwired
        let mut p = project_with(
            &[
                ("cfg", false, &[]),
                ("g.db", false, &["g"]),
                ("g.trig", true, &["g"]),
            ],
            vec![wire("cfg", "g", &[("g", "x", "in")])],
            vec![group_g(&["g.db", "g.trig"])],
        );
        p.nodes.iter_mut().find(|n| n.id == "g.db").unwrap().requires_infra = true;
        let (program, kicks) = setup(&p, infra_places(&p)).unwrap();
        assert_eq!(sorted(&program), vec!["g.db"]);
        assert!(kicks.is_empty(), "the group starts g.db as it opens: {kicks:?}");
        assert_eq!(setup_entry(&p, &program, &kicks), "g.db");
        let (program, _) = setup(&p, trigger_places(&p)).unwrap();
        assert_eq!(sorted(&program), vec!["g.trig"]);
        p.groups[0].kind = weft_core::project::GroupKind::Loop { loop_config: serde_json::json!({}) };
        assert!(setup(&p, infra_places(&p)).unwrap_err().contains("inside loop"));
        assert!(setup(&p, trigger_places(&p)).unwrap_err().contains("inside loop"));
        assert!(compute_trigger_fire(&p, "g.trig", &Value::Null, None, &Default::default()).unwrap_err().contains("inside loop"));
    }

    #[test]
    fn firing_non_trigger_returns_an_error() {
        let p = project(
            &[("a", false, &[]), ("out", false, &[])],
            &[("a", "out")],
        );
        assert!(compute_trigger_fire(&p, "a", &Value::Null, None, &Default::default()).unwrap_err().contains("not a trigger"));
    }

    /// Two programs in one file sharing an upstream node (a database, a
    /// provider). Firing one trigger must leave the other program's
    /// consumers OUTSIDE the subgraph: the shared node still runs (it
    /// feeds the fired program) and still emits down every wire, so the
    /// engine needs the boundary to skip the other program's nodes
    /// instead of parking their partial input sets forever. This is the
    /// shape that ended every fire of a two-trigger project as Stuck.
    #[test]
    fn a_shared_upstream_node_does_not_pull_the_other_program_in() {
        //  Shared ──┬──► TriggerX ──► Bx ──► OutX
        //           └──► TriggerY ──► By ──► OutY
        // (Shared also wires straight into Bx and By.)
        let p = project(
            &[
                ("shared", false, &[]),
                ("trigger_x", true, &[]),
                ("trigger_y", true, &[]),
                ("bx", false, &[]),
                ("by", false, &[]),
                ("out_x", false, &[]),
                ("out_y", false, &[]),
            ],
            &[
                ("shared", "bx"),
                ("shared", "by"),
                ("trigger_x", "bx"),
                ("trigger_y", "by"),
                ("bx", "out_x"),
                ("by", "out_y"),
            ],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("msg".into()));
        assert_eq!(ids(&kicks), vec!["shared".to_string(), "trigger_x".to_string()]);
        assert_eq!(
            sorted(&program),
            vec!["bx", "out_x", "shared", "trigger_x"],
            "the other program's trigger and consumers stay outside the fire"
        );
    }
}

#[cfg(test)]
mod infra_kick_and_dep_tests {
    use super::*;
    use weft_core::project::infra_places;

    /// (id, is_trigger, requires_infra). Shared with `run_subgraph_tests`.
    pub(super) fn project(nodes: &[(&str, bool, bool)], edges: &[(&str, &str)]) -> ProjectDefinition {
        let n_json: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(id, is_trigger, requires_infra)| {
                serde_json::json!({
                    "id": id,
                    "nodeType": "T",
                    "label": null,
                    "config": {},
                    "position": { "x": 0, "y": 0 },
                    "features": { "isTrigger": is_trigger },
                    "requiresInfra": requires_infra,
                })
            })
            .collect();
        let e_json: Vec<serde_json::Value> = edges
            .iter()
            .map(|(s, t)| {
                // `sourceHandle` / `targetHandle` are the names `Edge`
                // reads. Writing `sourcePort` here dropped them silently
                // (no `deny_unknown_fields`), so every wire was
                // default -> default and no port-aware walk could be
                // told apart from a port-blind one.
                serde_json::json!({
                    "id": format!("e_{}_{}", s, t),
                    "source": s,
                    "sourceHandle": "out",
                    "target": t,
                    "targetHandle": "in",
                })
            })
            .collect();
        let body = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": n_json,
            "edges": e_json,
            "groups": []
        });
        serde_json::from_value(body).expect("valid test project")
    }

    fn kick_ids(kicks: &[Kick]) -> Vec<String> {
        let mut v: Vec<String> = kicks.iter().map(|k| k.node.clone()).collect();
        v.sort();
        v
    }

    /// The kicks of the setup of `targets`: none when there is nothing to
    /// set up.
    fn setup_kicks(p: &ProjectDefinition, targets: Vec<weft_core::frames::Located>) -> Result<Vec<Kick>, String> {
        setup_cut(p, &targets).map(|cut| cut.map(|(_, kicks)| kicks).unwrap_or_default())
    }

    #[test]
    fn infra_kicks_are_upstream_roots_not_infra_nodes() {
        // text → compute → infra
        let p = project(
            &[("text", false, false), ("compute", false, false), ("infra", false, true)],
            &[("text", "compute"), ("compute", "infra")],
        );
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        // The kick is the upstream root (text), NOT the infra node.
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn infra_kicks_skip_unreachable_branches() {
        // unrelated standalone node + a real text → infra chain.
        let p = project(
            &[
                ("standalone", false, false),
                ("text", false, false),
                ("infra", false, true),
            ],
            &[("text", "infra")],
        );
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn infra_node_with_no_upstream_kicks_itself() {
        // A parameterless infra node (no upstream edges) IS its own
        // root: has to kick something to fire.
        let p = project(&[("infra", false, true)], &[]);
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        assert_eq!(kick_ids(&kicks), vec!["infra".to_string()]);
    }

    #[test]
    fn infra_kicks_empty_when_no_infra_nodes() {
        let p = project(&[("a", false, false), ("b", false, false)], &[("a", "b")]);
        assert!(setup_kicks(&p, infra_places(&p)).unwrap().is_empty());
    }

    #[test]
    fn infra_kicks_handle_multiple_infra_nodes_with_shared_root() {
        // text → infraA ; text → infraB
        let p = project(
            &[("text", false, false), ("infraA", false, true), ("infraB", false, true)],
            &[("text", "infraA"), ("text", "infraB")],
        );
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        // text is the only root reaching both.
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn trigger_deps_direct_chain() {
        // infra → trigger
        let p = project(
            &[("infra", false, true), ("trigger", true, false)],
            &[("infra", "trigger")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_indirect_chain() {
        // infra → middle → trigger
        let p = project(
            &[
                ("infra", false, true),
                ("middle", false, false),
                ("trigger", true, false),
            ],
            &[("infra", "middle"), ("middle", "trigger")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_skip_unrelated_triggers() {
        // infraA → triggerA ; infraB and triggerB are not connected.
        let p = project(
            &[
                ("infraA", false, true),
                ("infraB", false, true),
                ("triggerA", true, false),
                ("triggerB", true, false),
            ],
            &[("infraA", "triggerA")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infraA".to_string(), "triggerA".to_string())]);
    }

    #[test]
    fn trigger_deps_one_infra_two_triggers() {
        // infra → t1 ; infra → t2
        let p = project(
            &[("infra", false, true), ("t1", true, false), ("t2", true, false)],
            &[("infra", "t1"), ("infra", "t2")],
        );
        let deps = compute_trigger_deps(&p);
        // Sorted by (infra, trigger).
        assert_eq!(
            deps,
            vec![
                ("infra".to_string(), "t1".to_string()),
                ("infra".to_string(), "t2".to_string()),
            ]
        );
    }

    #[test]
    fn trigger_deps_empty_when_no_triggers() {
        let p = project(
            &[("infra", false, true), ("downstream", false, false)],
            &[("infra", "downstream")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }

    #[test]
    fn trigger_deps_empty_when_no_infra() {
        let p = project(
            &[("trigger", true, false), ("upstream", false, false)],
            &[("upstream", "trigger")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }

    #[test]
    fn trigger_deps_reach_through_an_ordinary_node() {
        // infra -> text -> trigger: the dependency is just as real one
        // hop further out.
        let p = project(
            &[
                ("text", false, false),
                ("trigger", true, false),
                ("infra", false, true),
            ],
            &[("text", "trigger"), ("infra", "text")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_skip_when_trigger_does_not_reach_infra() {
        // trigger -> text, and infra sits downstream of the trigger.
        // Stopping it cannot break an armed registration, so the guard
        // must not refuse that stop. The name of this test claimed the
        // negative case for a long time while its body asserted the
        // positive one; this is the negative case.
        let p = project(
            &[
                ("trigger", true, false),
                ("text", false, false),
                ("infra", false, true),
            ],
            &[("trigger", "text"), ("text", "infra")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }
}

#[cfg(test)]
mod resolve_scope_tests {
    use super::infra_kick_and_dep_tests::project;
    use super::*;
    use weft_core::activation::ActivationScope;

    fn shared() -> ActivationScope {
        ActivationScope::default()
    }

    fn instance() -> ActivationScope {
        ActivationScope { triggers: Vec::new(), instance: Some("alice".parse().expect("instance id")) }
    }

    /// A program whose only trigger exists once per instance.
    fn per_instance_only() -> ProjectDefinition {
        let mut p = project(&[("t", true, false), ("a", false, false)], &[("t", "a")]);
        p.nodes.iter_mut().find(|n| n.id == "t").expect("t").per_instance = Some(weft_core::instance::PerInstance::Marked);
        p
    }

    /// No triggers at all: the verbs that set triggers up refuse and say
    /// so, for the program and for an instance alike.
    #[test]
    fn no_triggers_refuses_with_its_own_message() {
        let p = project(&[("a", false, false)], &[]);
        for scope in [shared(), instance()] {
            let (status, msg) = resolve_scope(&p, &scope).expect_err("nothing to resolve");
            assert_eq!(status, StatusCode::PRECONDITION_FAILED);
            assert_eq!(msg, "this program has no triggers");
        }
    }

    fn row(trigger: &str, instance: Option<&str>) -> crate::activation_store::Activation {
        crate::activation_store::Activation {
            key: weft_core::activation::ActivationKey::new(
                trigger,
                weft_core::instance::Owner::from_instance(instance.map(|m| m.parse().expect("instance id"))),
            ),
            lifecycle: crate::activation_store::ActivationLifecycle::active(),
            program: None,
            source_version: None,
        }
    }

    /// Nothing activated: a take-down (deactivate, cancel-running,
    /// `weft rm --journal`'s quiesce) has nothing to do.
    #[test]
    fn nothing_activated_takes_down_nothing() {
        for scope in [shared(), instance()] {
            assert!(keys_to_take_down(&[], &scope).expect("no-op").is_empty());
        }
    }

    /// A take-down picks the owner's rows, whatever the source says now:
    /// a trigger the source dropped is still taken down, and the other
    /// owners' rows are left alone.
    #[test]
    fn a_take_down_picks_the_owners_rows() {
        let rows = [row("gone", None), row("door", None), row("door", Some("alice")), row("door", Some("bob"))];
        let triggers = |keys: Vec<weft_core::activation::ActivationKey>| {
            keys.into_iter().map(|k| (k.trigger, k.owner.instance().map(|m| m.to_string()))).collect::<Vec<_>>()
        };
        assert_eq!(
            triggers(keys_to_take_down(&rows, &shared()).unwrap()),
            vec![("gone".to_string(), None), ("door".to_string(), None)]
        );
        assert_eq!(triggers(keys_to_take_down(&rows, &instance()).unwrap()), vec![("door".to_string(), Some("alice".to_string()))]);
        let named = ActivationScope { triggers: vec!["gone".into()], instance: None };
        assert_eq!(triggers(keys_to_take_down(&rows, &named).unwrap()), vec![("gone".to_string(), None)]);
    }

    /// A named trigger with no row for the owner is a mistake.
    #[test]
    fn a_named_trigger_never_activated_is_refused() {
        let rows = [row("door", None)];
        let scope = ActivationScope { triggers: vec!["door".into()], instance: Some("alice".parse().expect("instance id")) };
        let (status, msg) = keys_to_take_down(&rows, &scope).expect_err("alice's door never activated");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(msg.contains("'door' of instance 'alice'"), "{msg}");
    }

    /// Only per-instance triggers: the program's own scope names nothing,
    /// and the refusal points at --instance.
    #[test]
    fn per_instance_only_refuses_shared_scope() {
        let p = per_instance_only();
        let (status, msg) = resolve_scope(&p, &shared()).expect_err("no shared trigger");
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert!(msg.contains("no shared trigger"), "{msg}");
        assert_eq!(resolve_scope(&p, &instance()).expect("instance's trigger").len(), 1);
    }
}

#[cfg(test)]
mod run_subgraph_tests {
    use super::infra_kick_and_dep_tests::project;
    use super::*;

    /// What a run aimed at `targets` keeps.
    fn aimed_at(p: &ProjectDefinition, targets: &[&str]) -> std::collections::BTreeSet<Located> {
        weft_core::project::cut::walk(&weft_core::project::graph::ProjectGraph::new(p), &weft_core::project::cut::SelectionBounds {
            target: targets.iter().map(|s| s.to_string()).collect(), ..Default::default()
        }).unwrap().kept()
    }

    /// Diamond: src feeds two branches, only one is aimed at. The
    /// other branch's exclusive nodes stay out of the subgraph, so
    /// they are neither infra-gated nor dispatched.
    #[test]
    fn aiming_at_one_output_excludes_the_other_branch() {
        let p = project(
            &[
                ("src", false, false),
                ("a", false, false),
                ("out1", false, false),
                ("b", false, false),
                ("out2", false, false),
            ],
            &[("src", "a"), ("a", "out1"), ("src", "b"), ("b", "out2")],
        );
        let sub = aimed_at(&p, &["out1"]);
        let mut nodes: Vec<&str> = sub.iter().map(|s| s.id.as_str()).collect();
        nodes.sort();
        assert_eq!(nodes, vec!["a", "out1", "src"]);
    }

    /// A trigger in the walk is a terminator: included (it gets kicked
    /// payload-less so its branch prunes), but its own upstream is not.
    #[test]
    fn a_trigger_terminates_the_upstream_walk_and_kicks_bare() {
        let p = project(
            &[
                ("setup", false, false),
                ("trig", true, false),
                ("mid", false, false),
                ("out", false, false),
            ],
            &[("setup", "trig"), ("trig", "mid"), ("mid", "out")],
        );
        let sub = aimed_at(&p, &["out"]);
        assert!(sub.contains(&Located::top("trig")));
        assert!(!sub.contains(&Located::top("setup")), "the trigger's own upstream stays out");

        let resolved = weft_core::run_spec::resolve_spec(&weft_core::run_spec::RunSpec {
            target: vec!["out".into()], ..weft_core::run_spec::RunSpec::whole("x")
        }, &p, &Default::default()).unwrap();
        let trig = resolved.kicks.iter().find(|k| k.node == "trig").expect("trigger is a root");
        assert!(trig.payload.is_none(), "a manual run kicks triggers payload-less");
        assert!(!trig.firing);
    }

    /// Fire time: only the firing trigger carries the wake payload
    /// and the snapshot; every other root is a bare kick.
    #[test]
    fn only_the_firing_trigger_carries_the_payload() {
        let p = project(
            &[
                ("trig", true, false),
                ("other", true, false),
                ("out", false, false),
            ],
            &[("trig", "out"), ("other", "out")],
        );
        let payload = serde_json::json!({ "msg": "hi" });
        let snapshot = serde_json::json!({ "port": "v" });
        let kicks = weft_core::run_spec::compute_trigger_fire(&p, "trig", &payload, Some(&snapshot), &Default::default()).unwrap().kicks;
        let trig = kicks.iter().find(|k| k.node == "trig").unwrap();
        assert!(trig.firing);
        assert_eq!(trig.payload.as_ref(), Some(&payload));
        assert_eq!(trig.port_snapshot.as_ref(), Some(&snapshot));
        let other = kicks.iter().find(|k| k.node == "other").unwrap();
        assert!(!other.firing);
        assert!(other.payload.is_none() && other.port_snapshot.is_none());
    }
}

#[cfg(test)]
mod trigger_infra_ready_tests {
    use super::trigger_infra_ready;
    use crate::infra_node::{InfraNodeRow, InfraNodeStatus};

    fn row(node_id: &str, status: InfraNodeStatus) -> InfraNodeRow {
        InfraNodeRow {
            project_id: uuid::Uuid::nil(),
            node_id: node_id.into(),
            instance: None,
            copy_id: String::new(),
            status,
            failure_stage: None,
            failure_message: None,
            applied_spec_hash: None,
            applied_at_unix: None,
            endpoints: Default::default(),
            public_paths: Default::default(),
            doors: Default::default(),
            install_endpoints: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
        }
    }

    fn deps(ids: &[&str]) -> std::collections::BTreeSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    #[test]
    fn a_project_whose_triggers_need_no_infra_is_ready() {
        assert!(trigger_infra_ready(&deps(&[]), &[]));
        // Including one with infra running for something else entirely.
        assert!(trigger_infra_ready(&deps(&[]), &[row("svc", InfraNodeStatus::Running)]));
    }

    #[test]
    fn every_node_the_triggers_need_has_to_be_running() {
        let both = deps(&["bridge", "db"]);
        assert!(trigger_infra_ready(
            &both,
            &[row("bridge", InfraNodeStatus::Running), row("db", InfraNodeStatus::Running)]
        ));
        assert!(!trigger_infra_ready(
            &both,
            &[row("bridge", InfraNodeStatus::Running), row("db", InfraNodeStatus::Stopped)]
        ));
    }

    #[test]
    fn a_node_that_is_not_running_in_any_way_holds_the_activation_back() {
        for status in [
            InfraNodeStatus::Stopped,
            InfraNodeStatus::Failed,
            InfraNodeStatus::Flaky,
            InfraNodeStatus::Provisioning,
        ] {
            assert!(!trigger_infra_ready(&deps(&["bridge"]), &[row("bridge", status)]), "{status:?}");
        }
    }

    #[test]
    fn a_node_with_no_row_at_all_is_not_running() {
        // Nothing was ever provisioned for it, which is the ordinary
        // state of a project whose infra has never been started.
        assert!(!trigger_infra_ready(&deps(&["bridge"]), &[]));
        // And a row for a DIFFERENT node does not stand in for it.
        assert!(!trigger_infra_ready(&deps(&["bridge"]), &[row("db", InfraNodeStatus::Running)]));
    }
}

#[cfg(test)]
mod available_actions_tests {
    //! Layer-1 tests for the reconciliation table
    //! (`docs/project-lifecycle-state-model.md` §8): one case per
    //! table row, pure inputs -> expected verb set.

    use super::*;
    use crate::activation_store::{ActivationLifecycle as ProjectLifecycle, ProjectStatus};
    use crate::project_store::ProjectTransition;

    struct Case {
        removing: bool,
        lifecycle: ProjectLifecycle,
        partly_down: bool,
        transition: ProjectTransition,
        resync_stopped: bool,
        has_triggers: bool,
        has_infra: bool,
        trigger_infra_ready: bool,
        orphaned_infra: bool,
        infra_rollup: &'static str,
        infra_busy: bool,
        drift: ProjectDrift,
        preservation: PreservationCounts,
        running_count: usize,
    }

    impl Default for Case {
        fn default() -> Self {
            Self {
                removing: false,
                lifecycle: ProjectLifecycle::wiped(),
                partly_down: false,
                transition: ProjectTransition::None,
                resync_stopped: false,
                has_triggers: true,
                has_infra: false,
                trigger_infra_ready: true,
                orphaned_infra: false,
                infra_rollup: "none",
                infra_busy: false,
                drift: ProjectDrift::default(),
                preservation: PreservationCounts::default(),
                running_count: 0,
            }
        }
    }

    fn actions(case: &Case) -> Vec<String> {
        compute_available_actions(&ActionInputs {
            removing: case.removing,
            lifecycle: &case.lifecycle,
            partly_down: case.partly_down,
            transition: case.transition,
            resync_stopped: case.resync_stopped,
            has_triggers: case.has_triggers,
            has_infra: case.has_infra,
            trigger_infra_ready: case.trigger_infra_ready,
            run_infra_ready: super::run_infra_ready(case.has_infra, case.infra_rollup),
            orphaned_infra: case.orphaned_infra,
            infra_rollup: case.infra_rollup,
            infra_busy: case.infra_busy,
            drift: &case.drift,
            preservation: &case.preservation,
            running_count: case.running_count,
        })
    }

    fn assert_actions(case: &Case, expected: &[&str]) {
        assert_eq!(actions(case), expected.to_vec());
    }

    /// A project `weft rm` marked offers nothing, whatever else holds:
    /// `weft rm` again is the one way on.
    #[test]
    fn a_project_being_removed_offers_nothing() {
        assert_actions(&Case { removing: true, ..Case::default() }, &[]);
        assert_actions(&Case { removing: true, transition: ProjectTransition::Building, ..Case::default() }, &[]);
        assert_actions(
            &Case { removing: true, has_infra: true, infra_rollup: "running", ..Case::default() },
            &[],
        );
    }

    // ---- master rule: any transitional state offers only its cancel ----

    #[test]
    fn building_offers_only_cancel_build() {
        assert_actions(
            &Case { transition: ProjectTransition::Building, ..Case::default() },
            &["cancel_build"],
        );
        // Idempotent while already cancelling.
        assert_actions(
            &Case { transition: ProjectTransition::CancellingBuild, ..Case::default() },
            &["cancel_build"],
        );
    }

    #[test]
    fn activating_offers_only_cancel_activate() {
        assert_actions(
            &Case { lifecycle: ProjectLifecycle::activating(uuid::Uuid::nil()), ..Case::default() },
            &["cancel_activate"],
        );
    }

    #[test]
    fn deactivating_offers_cancel_running_and_resume() {
        let deactivating = ProjectLifecycle::deactivating_to(ProjectLifecycle::parked(), 0);
        assert_actions(
            &Case { lifecycle: deactivating.clone(), running_count: 2, ..Case::default() },
            &["cancel_running", "resume_active"],
        );
        // Nothing left to cancel once the running set empties.
        assert_actions(
            &Case { lifecycle: deactivating, running_count: 0, ..Case::default() },
            &["resume_active"],
        );
    }

    #[test]
    fn a_resync_offers_only_the_cancel_of_the_step_it_is_at() {
        let resyncing = |lifecycle, running_count| Case {
            lifecycle,
            running_count,
            transition: ProjectTransition::Resyncing,
            has_infra: true,
            infra_rollup: "running",
            drift: ProjectDrift { activation_drift: true, ..ProjectDrift::default() },
            ..Case::default()
        };
        let deactivating = ProjectLifecycle::deactivating_to(ProjectLifecycle::parked(), 0);
        assert_actions(&resyncing(deactivating.clone(), 2), &["cancel_running"]);
        assert_actions(&resyncing(deactivating, 0), &[]);
        assert_actions(&resyncing(ProjectLifecycle::parked(), 0), &[]);
        assert_actions(&resyncing(ProjectLifecycle::activating(uuid::Uuid::nil()), 0), &["cancel_activate"]);
        assert_actions(&resyncing(ProjectLifecycle::active(), 0), &[]);
        // Stopped with triggers down: taken up again, or given up.
        assert_actions(&Case { resync_stopped: true, ..resyncing(ProjectLifecycle::parked(), 0) }, &["resync", "cancel_activate"]);
    }

    #[test]
    fn infra_transitional_offers_only_infra_cancel() {
        for rollup in ["provisioning", "stopping", "terminating"] {
            assert_actions(
                &Case { has_infra: true, infra_rollup: rollup, ..Case::default() },
                &["infra_cancel"],
            );
        }
    }

    #[test]
    fn infra_op_in_flight_is_transitional_even_with_a_stable_rollup() {
        // A claimed stop still draining (or an InfraSetup run
        // before any node row flips) leaves the rollup reading
        // "running"; the in-flight fact must still collapse the row
        // to cancel-only, or new runs would starve the drain.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "running",
                infra_busy: true,
                ..Case::default()
            },
            &["infra_cancel"],
        );
    }

    // ---- stable rows ----

    #[test]
    fn inactive_no_infra_offers_run_and_activate() {
        assert_actions(&Case::default(), &["run", "activate"]);
    }

    #[test]
    fn no_triggers_hides_activate_but_run_stands() {
        assert_actions(&Case { has_triggers: false, ..Case::default() }, &["run"]);
    }

    #[test]
    fn inactive_infra_resting_still_offers_activate_when_no_trigger_needs_it() {
        // The infra sits on a branch no trigger touches, so arming the
        // triggers is sound with it down. Run still waits: a run touches
        // the whole graph.
        assert_actions(
            &Case { has_infra: true, infra_rollup: "none", ..Case::default() },
            &["activate", "infra_start"],
        );
    }

    #[test]
    fn a_trigger_whose_infra_is_down_cannot_be_armed() {
        // The bridge the trigger DEPENDS ON is not up, so registering
        // the signal would point it at an address that does not answer.
        // Start the infra, and activate comes back.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "none",
                trigger_infra_ready: false,
                ..Case::default()
            },
            &["infra_start"],
        );
    }

    #[test]
    fn inactive_infra_running_offers_everything_stable() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "running", ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate"],
        );
        // Infra drift additionally lights upgrade.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "running",
                drift: ProjectDrift { infra_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "activate", "infra_stop", "infra_terminate", "infra_upgrade"],
        );
    }

    #[test]
    fn inactive_infra_degraded_holds_activate_back_only_when_a_trigger_needs_it() {
        // `partial` is a steady state, not a transient. Whether it holds
        // activation back depends on WHICH half is down.
        for rollup in ["failed", "flaky", "partial"] {
            assert_actions(
                &Case { has_infra: true, infra_rollup: rollup, ..Case::default() },
                &["activate", "infra_start", "infra_stop", "infra_terminate"],
            );
            assert_actions(
                &Case {
                    has_infra: true,
                    infra_rollup: rollup,
                    trigger_infra_ready: false,
                    ..Case::default()
                },
                &["infra_start", "infra_stop", "infra_terminate"],
            );
        }
    }

    #[test]
    fn each_verb_waits_on_the_infra_it_would_touch() {
        // `run` touches the whole graph, so the whole-graph rollup gates
        // it. `activate` arms the triggers, so only the infra those
        // triggers DEPEND ON gates it. The two answers differ exactly
        // when an infra node is not upstream of any trigger, which is
        // the pair below: same rollup, and the only thing that moves is
        // whether a trigger needs the node that is down.
        for rollup in ["none", "stopped", "partial", "failed", "flaky"] {
            let unrelated =
                actions(&Case { has_infra: true, infra_rollup: rollup, ..Case::default() });
            assert!(!unrelated.contains(&"run".to_string()), "{rollup}: run");
            assert!(unrelated.contains(&"activate".to_string()), "{rollup}: activate");
            let needed = actions(&Case {
                has_infra: true,
                infra_rollup: rollup,
                trigger_infra_ready: false,
                ..Case::default()
            });
            assert!(!needed.contains(&"run".to_string()), "{rollup}: run");
            assert!(!needed.contains(&"activate".to_string()), "{rollup}: activate");
        }
        let ready = actions(&Case { has_infra: true, infra_rollup: "running", ..Case::default() });
        assert!(ready.contains(&"run".to_string()));
        assert!(ready.contains(&"activate".to_string()));
    }

    #[test]
    fn inactive_infra_stopped_offers_start_and_terminate() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "stopped", ..Case::default() },
            &["activate", "infra_start", "infra_terminate"],
        );
    }

    /// The program's own triggers are off, an instance's are on and lag the
    /// code: the drift bit counts that instance's, and resync is offered
    /// beside the activate that would turn the program's own back on.
    #[test]
    fn a_instances_drift_lights_resync_while_the_program_is_off() {
        assert_actions(
            &Case { drift: ProjectDrift { activation_drift: true, ..Default::default() }, ..Case::default() },
            &["run", "activate", "resync"],
        );
    }

    #[test]
    fn active_offers_run_and_deactivate_and_drift_lights_resync() {
        let active = ProjectLifecycle::active();
        assert_actions(
            &Case { lifecycle: active.clone(), ..Case::default() },
            &["run", "deactivate"],
        );
        assert_actions(
            &Case {
                lifecycle: active.clone(),
                drift: ProjectDrift { activation_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "deactivate", "resync"],
        );
        // Resync re-arms the triggers, so the infra they depend on
        // gates it the way it gates activate: the door would 412.
        assert_actions(
            &Case {
                lifecycle: active,
                has_infra: true,
                infra_rollup: "running",
                trigger_infra_ready: false,
                drift: ProjectDrift { activation_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    /// Some triggers on and some off (an infra stop took down the ones
    /// reading it): activate turns the rest on, and reactivate when what
    /// they kept waits on them.
    #[test]
    fn partly_on_still_offers_turning_the_rest_on() {
        let active = ProjectLifecycle::active();
        assert_actions(&Case { lifecycle: active.clone(), partly_down: true, ..Case::default() }, &["run", "deactivate", "activate"]);
        assert_actions(
            &Case { lifecycle: active.clone(), partly_down: true, preservation: PreservationCounts { parked: 1, suspended: 0 }, ..Case::default() },
            &["run", "deactivate", "reactivate"],
        );
        assert_actions(
            &Case { lifecycle: active, partly_down: true, trigger_infra_ready: false, ..Case::default() },
            &["run", "deactivate"],
        );
    }

    #[test]
    fn a_build_does_not_erase_activation_drift() {
        let program = weft_core::project::hash::ProgramIdentity {
            binary_hash: "old-code".into(), definition_hash: "same-graph".into(), implementations: Default::default(),
        };
        let query = StatusQuery { desired_binary_hash: Some("new-code".into()), desired_definition_hash: Some("same-graph".into()), ..Default::default() };
        assert!(activation_has_drifted(&query, Some("new-code"), Some("same-graph"), Some(&program), false));
        assert!(activation_has_drifted(&StatusQuery::default(), Some("new-code"), Some("same-graph"), Some(&program), false));
        assert!(!activation_has_drifted(&StatusQuery::default(), Some("old-code"), Some("same-graph"), Some(&program), false));
        // Activated before the code identity was recorded: only the graph
        // hash can speak, so an upgrade does not light resync on its own.
        assert!(!activation_has_drifted(&query, Some("new-code"), Some("same-graph"), None, false));
        assert!(activation_has_drifted(&query, Some("new-code"), Some("same-graph"), None, true));
    }

    #[test]
    fn active_with_last_trigger_deleted_keeps_deactivate() {
        // Trigger divergence: the source no longer has triggers but the
        // backend is still active; deactivate must stay offered so the
        // user can bring the live trigger down.
        assert_actions(
            &Case { lifecycle: ProjectLifecycle::active(), has_triggers: false, ..Case::default() },
            &["run", "deactivate"],
        );
    }

    #[test]
    fn active_infra_running_keeps_stop_and_terminate() {
        // Resolved cell Q2: stop/terminate while active auto-deactivate
        // first (one click), so both stay offered.
        assert_actions(
            &Case {
                lifecycle: ProjectLifecycle::active(),
                has_infra: true,
                infra_rollup: "running",
                ..Case::default()
            },
            &["run", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn preserved_state_flips_activate_to_reactivate_only_when_inactive() {
        let preserved = PreservationCounts { parked: 2, suspended: 1 };
        assert_actions(
            &Case {
                preservation: PreservationCounts { ..preserved },
                ..Case::default()
            },
            &["run", "reactivate"],
        );
        // Registered (never activated) always offers plain activate.
        let mut registered = ProjectLifecycle::wiped();
        registered.status = ProjectStatus::Registered;
        assert_actions(
            &Case {
                lifecycle: registered,
                preservation: PreservationCounts { parked: 2, suspended: 1 },
                ..Case::default()
            },
            &["run", "activate"],
        );
    }

    // ---- Model 1: orphaned live infra ----

    #[test]
    fn orphan_never_gates_run_and_keeps_stop_terminate_visible() {
        // Source has NO infra (the user deleted the node) but live
        // rows still run: run/activate are free (the plain graph runs
        // in the shared pool, unlinked) AND the infra controls stay so
        // the user never loses track of the orphan. No start/upgrade:
        // there is no source spec to provision from.
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "running", ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn stopped_orphan_still_offers_terminate() {
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "stopped", ..Case::default() },
            &["run", "activate", "infra_terminate"],
        );
    }

    #[test]
    fn orphan_mid_terminate_offers_infra_cancel() {
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "terminating", ..Case::default() },
            &["infra_cancel"],
        );
    }
}

#[cfg(test)]
mod status_query_wire_shape_tests {
    use super::*;

    /// `StatusQuery`'s wire shape is the contract with the CLI's URL
    /// builder (and any future direct caller from the extension).
    /// A typo in one of the three `rename = "desiredXyzHash"`
    /// attributes silently sets the field to `None`, the drift
    /// comparison bypasses, and no compile error fires. Pin the
    /// camelCase wire shape here so a rename is loud.
    ///
    /// Tested via the JSON serde shape because Query at runtime
    /// flows through `serde_urlencoded` (transitive dep of axum,
    /// not directly in the dispatcher's dev-dep set). The `rename`
    /// attribute applies to BOTH formats, so a JSON round-trip is
    /// enough to catch a typo in the rename string.
    #[test]
    fn status_query_round_trips_camelcase_keys() {
        let json = serde_json::json!({
            "desiredBinaryHash": "abc",
            "desiredDefinitionHash": "def",
            "desiredInfraHash": "ghi",
        });
        let q: StatusQuery = serde_json::from_value(json)
            .expect("camelCase keys must deserialize");
        assert_eq!(q.desired_binary_hash.as_deref(), Some("abc"));
        assert_eq!(q.desired_definition_hash.as_deref(), Some("def"));
        assert_eq!(q.desired_infra_hash.as_deref(), Some("ghi"));
    }

    /// Snake_case used to be accepted via `alias`. The Round 4
    /// alias-drop removed that compatibility; pin it as not-aliased
    /// so a future regression that re-adds the alias is loud.
    #[test]
    fn status_query_ignores_snake_case_keys() {
        let json = serde_json::json!({
            "desired_binary_hash": "abc",
        });
        let q: StatusQuery = serde_json::from_value(json)
            .expect("deserialize never fails on optional-only fields");
        // The snake_case key is unknown; the camelCase field stays
        // None. If anyone re-adds `alias = "desired_binary_hash"`,
        // this flips to Some("abc") and the test fails.
        assert_eq!(q.desired_binary_hash, None);
    }
}

#[cfg(test)]
mod infra_entries_tests {
    use super::infra_entries;
    use crate::infra_node::{InfraNodeRow, InfraNodeStatus, ObservedCopies};
    use weft_core::instance::InstanceId;

    fn row(node_id: &str, instance: Option<&str>, status: InfraNodeStatus) -> InfraNodeRow {
        InfraNodeRow {
            project_id: uuid::Uuid::nil(),
            node_id: node_id.into(),
            instance: instance.map(|m| InstanceId::new(m).unwrap()),
            copy_id: String::new(),
            status,
            failure_stage: None,
            failure_message: None,
            applied_spec_hash: None,
            applied_at_unix: None,
            endpoints: Default::default(),
            public_paths: Default::default(),
            doors: Default::default(),
            install_endpoints: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
        }
    }

    /// Every infra place the program declares is listed, started or not:
    /// a shared one with its copy's state (or `not_started`, or
    /// `provisioning` while a start brings it up), a `@per_instance` one as
    /// `per_instance` with its instances' copies counted. Instance copies are
    /// listed on their own, a starting one included; a node that needs no
    /// infra is not listed.
    #[test]
    fn every_declared_infra_place_is_listed() {
        let mut project = super::infra_kick_and_dep_tests::project(
            &[("db", false, true), ("cache", false, true), ("queue", false, true), ("bridge", false, true), ("plain", false, false)],
            &[],
        );
        project.nodes.iter_mut().find(|n| n.id == "bridge").unwrap().per_instance = Some(weft_core::instance::PerInstance::Marked);
        let copies = ObservedCopies {
            rows: vec![row("db", None, InfraNodeStatus::Running), row("bridge", Some("ada"), InfraNodeStatus::Stopped)],
            starting: vec![("queue".into(), None), ("bridge".into(), Some(InstanceId::new("bob").unwrap()))],
            change_asked_unix: [(("queue".to_string(), None), 100), (("bridge".to_string(), Some(InstanceId::new("bob").unwrap())), 110)]
                .into(),
            as_of_unix: 130,
        };
        let (infra, instance_entries) = infra_entries(&project, &copies);
        let listed: Vec<(&str, &str, Option<usize>)> =
            infra.iter().map(|e| (e.node.as_str(), e.status.as_str(), e.instance_copy_count)).collect();
        assert_eq!(
            listed,
            [
                ("bridge", "per_instance", Some(2)),
                ("cache", "not_started", None),
                ("db", "running", None),
                ("queue", "provisioning", None),
            ]
        );
        let instances: Vec<(&str, &str, &str)> =
            instance_entries.iter().map(|c| (c.node.as_str(), c.instance.as_str(), c.status.as_str())).collect();
        assert_eq!(instances, [("bridge", "ada", "stopped"), ("bridge", "bob", "provisioning")]);
        // A copy being started counts from its request, row or no row, as
        // of the clock the copies were read on.
        let since = |progress: Option<&weft_core::infra::wire::ChangeProgress>| progress.map(|p| (p.since_unix, p.as_of_unix));
        let shared = |node: &str| infra.iter().find(|e| e.node == node).unwrap().progress.as_ref();
        assert_eq!(since(shared("queue")), Some((100, 130)), "a shared copy with no row");
        assert_eq!(since(shared("db")), None, "a running copy shows none");
        let bob = instance_entries.iter().find(|e| e.instance.as_str() == "bob").unwrap();
        assert_eq!(since(bob.progress.as_ref()), Some((110, 130)), "an instance's copy with no row");
    }

    /// A program with no infra node lists none, which is the one case the
    /// CLI says no node declares `requiresInfra`.
    #[test]
    fn a_program_without_infra_lists_none() {
        let project = super::infra_kick_and_dep_tests::project(&[("plain", false, false)], &[]);
        let (infra, instances) = infra_entries(&project, &ObservedCopies::default());
        assert!(infra.is_empty() && instances.is_empty());
    }
}

#[cfg(test)]
mod unavailable_action_tests {
    use super::unavailable_action;
    use crate::project_store::{ProjectStatus, ProjectTransition};

    #[test]
    fn activating_what_is_already_on_points_at_resync() {
        let allowed = vec!["run".to_string(), "deactivate".to_string(), "resync".to_string()];
        let message = unavailable_action("activate", ProjectStatus::Active, ProjectTransition::None, "none", &allowed);
        assert!(message.contains("already on") && message.contains("weft resync"), "{message}");
        let other = unavailable_action("run", ProjectStatus::Active, ProjectTransition::None, "stopped", &allowed);
        assert!(other.contains("allowed actions: [run, deactivate, resync]"), "{other}");
    }

    #[test]
    fn starting_infra_that_already_runs_points_at_upgrade() {
        let allowed = vec!["infra_stop".to_string(), "infra_upgrade".to_string()];
        let message = unavailable_action("infra_start", ProjectStatus::Active, ProjectTransition::None, "running", &allowed);
        assert!(message.contains("already running") && message.contains("weft infra upgrade"), "{message}");
        // Without an upgrade to offer (the infra left running belongs to
        // nodes no longer in the program), the plain answer stands.
        let leftover = unavailable_action("infra_start", ProjectStatus::Active, ProjectTransition::None, "running", &["infra_terminate".to_string()]);
        assert!(leftover.contains("allowed actions: [infra_terminate]"), "{leftover}");
    }

    #[test]
    fn infra_that_is_changing_is_named_as_the_blocker() {
        let message = unavailable_action("activate", ProjectStatus::Registered, ProjectTransition::None, "provisioning", &["infra_cancel".to_string()]);
        assert!(message.contains("infra is changing (infra provisioning)") && message.contains("weft infra status"), "{message}");
        assert!(!message.contains("registered"), "the triggers' state is not why: {message}");
    }

    #[test]
    fn a_build_in_flight_is_named_as_the_blocker() {
        let allowed = vec!["cancel_build".to_string()];
        let message = unavailable_action("activate", ProjectStatus::Registered, ProjectTransition::Building, "none", &allowed);
        assert!(message.contains("the project is building") && message.contains("weft cancel-build"), "{message}");
        let cancelling = unavailable_action("activate", ProjectStatus::Registered, ProjectTransition::CancellingBuild, "none", &allowed);
        assert!(cancelling.contains("being cancelled") && !cancelling.contains("weft cancel-build"), "{cancelling}");
    }
}
