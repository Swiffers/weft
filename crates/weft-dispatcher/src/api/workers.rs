//! `GET/PUT /projects/{id}/workers`: the project's own worker levers.
//! Each handler names the caller as an operator and calls its action
//! (`crate::actions::settings::workers_get`, `workers_put`), where every
//! rule lives.

use axum::extract::{Path, State};
use axum::Json;
use weft_core::workers::{WorkerOverrides, WorkersResponse};

use crate::actions::{settings, Actor, Refused};
use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;

pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<WorkersResponse>, Refused> {
    Ok(Json(settings::workers_get(&state, &Actor::operator(caller.0), id).await?))
}

/// Replace the project's levers with `overrides` (an unset lever follows
/// the install again).
pub async fn put(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(overrides): Json<WorkerOverrides>,
) -> Result<Json<WorkersResponse>, Refused> {
    Ok(Json(settings::workers_put(&state, &Actor::operator(caller.0), id, &overrides).await?))
}
