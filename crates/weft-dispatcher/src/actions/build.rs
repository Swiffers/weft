//! Building a project: the one way a project's program and images come to
//! exist on an install.
//!
//! What is built is a version the install holds: a working copy's files
//! stamped into one first (`super::versions::stamp`), or a version named as
//! it is. The install fetches it back, compiles it, resolves its `@asset`s
//! itself (`crate::build::assets`), holds the run the build is for to the
//! compiled program, and registers it once every image it needs is there:
//! at once when they all are (a 200, [`weft_core::builds::BuiltProgram`]),
//! else once the builds it started end (a 202,
//! [`weft_core::builds::BuildsUnderway`], which the caller follows). The
//! project's running pointers move to it in the registration's
//! transaction, so the next action acts on exactly this build. Nothing a
//! client computed is believed.

use axum::http::StatusCode;
use weft_core::builds::{BuildRequest, VersionSource};

use super::Actor;
use crate::state::DispatcherState;
use crate::transition::BuildFailed;

/// How a build of a version went, short of failing.
pub enum Built {
    /// Every image was there: the version is the project's running one.
    Registered(Box<weft_core::builds::BuiltProgram>),
    /// Images are building: the install registers the version once they
    /// end (`crate::build::waiting`).
    Underway(weft_core::builds::BuildsUnderway),
}

/// Build what `request` names (see the module doc).
pub async fn build(state: &DispatcherState, actor: &Actor, project: uuid::Uuid, request: &BuildRequest) -> Result<Built, BuildFailed> {
    let refused = |(status, message): super::Refused| BuildFailed::of(status, message);
    actor.may_act_on(state, project).await.map_err(refused)?;
    if request.run.is_some() {
        super::runs::refuse_own_run(actor, project).map_err(refused)?;
    }
    let summary = state
        .projects
        .get(project)
        .await
        .map_err(|e| BuildFailed::of(StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e:#}")))?
        .ok_or_else(|| BuildFailed::of(StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()))?;
    summary.unless_removing().map_err(refused)?;
    let copy = super::versions::copy_of(state, project, request.working_copy).await.map_err(refused)?;
    let manifest = match &request.version {
        Some(version) => {
            if request.root {
                return Err(BuildFailed::of(StatusCode::BAD_REQUEST, "`root` stamps a working copy; a version named as it is is no new root".into()));
            }
            state
                .versions
                .version(project, version)
                .await
                .map_err(|e| BuildFailed::of(StatusCode::INTERNAL_SERVER_ERROR, format!("version: {e:#}")))?
                .ok_or_else(|| BuildFailed::of(StatusCode::NOT_FOUND, format!("no version {version} in this project")))?
                .manifest
        }
        None => {
            let (_, stamped) = super::versions::stamp(state, project, copy.id, None, request.root).await.map_err(refused)?;
            stamped.working
        }
    };
    let source = VersionSource { manifest, inputs: weft_core::builds::BuildInputs { node_set: request.node_set, assets: Default::default() } };
    let reach = crate::build::assets::Reach::of(state, project, &source.manifest, Some(&copy)).await.map_err(refused)?;
    let prepare = crate::build::assets::Resolving { reach, run: request.run.clone() };
    build_and_register(state, project, actor.tenant(), &source, &prepare).await
}

/// Build `source` for project `id` of `tenant` and, when every image is
/// there already, register it: what a build does past its checks, and
/// what the maintenance lift does to build a project again on a newer weft
/// (`crate::maintenance`, with the resolutions it was first built with,
/// [`crate::build::Kept`]).
pub(crate) async fn build_and_register(
    state: &DispatcherState,
    id: uuid::Uuid,
    tenant: &crate::tenant::TenantId,
    source: &VersionSource,
    prepare: &dyn crate::build::Prepare,
) -> Result<Built, BuildFailed> {
    // Taken as the ask arrives, before the minutes of compiling: a
    // version registers only over an older ask, whichever finishes first
    // (`crate::build::waiting`).
    let ask = state.projects.next_build_ask(id).await.map_err(|e| {
        let (status, message) = crate::api::unavailable_or_internal("take the build's place in the project's order", e);
        BuildFailed::of(status, message)
    })?;
    // Claims every image the build plans from its first look at the
    // registry until the version registers, so no prune deletes an image
    // this build found or built before anything references it
    // (`crate::build::prune::ImageHold`).
    let hold = crate::build::prune::ImageHold::new(&state.pg_pool, id);
    let (version, hold) = match crate::transition::build_version_gated(state, id, tenant, source, prepare, ask, hold).await? {
        crate::build::VersionBuild::Waiting(underway) => return Ok(Built::Underway(underway)),
        crate::build::VersionBuild::Ready { version, hold } => (version, hold),
    };
    if let Err(e) = hold.confirm().await {
        let released = hold.release().await;
        let settled = crate::transition::settle(state, Some(id)).await;
        let also: Vec<String> = [released.err(), settled.err()].into_iter().flatten().map(|e| format!("{e:#}")).collect();
        let mut message = format!("{e:#}");
        if !also.is_empty() {
            message.push_str(&format!("\n(and letting go of the build failed too: {})", also.join("; ")));
        }
        return Err(BuildFailed::of(StatusCode::CONFLICT, message));
    }
    let registrar = crate::build::waiting::Registrar::Request { hold };
    match crate::build::waiting::register(state, id, tenant.as_str(), &version, registrar).await {
        Ok(Some(built)) => Ok(Built::Registered(Box::new(built))),
        Ok(None) => Err(BuildFailed::of(
            StatusCode::CONFLICT,
            "a newer build of this project registered while this one was compiling, so this one was not registered; \
             build again to run this version"
                .to_string(),
        )),
        Err(e) if crate::build::waiting::ResyncHolds::is(&e) => Err(BuildFailed::of(StatusCode::CONFLICT, format!("{e:#}"))),
        Err(e) => {
            let (status, mut message) = if crate::project_store::ProjectRemoving::is(&e) {
                crate::project_store::ProjectRemoving::refusal()
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("register the build: {e:#}"))
            };
            if let Err(settled) = crate::transition::settle(state, Some(id)).await {
                message.push_str(&format!("\n(and settling the project failed too: {settled:#})"));
            }
            Err(BuildFailed::of(status, message))
        }
    }
}
