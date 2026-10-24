//! A program minting an instance token (`ctx.tokens().mint_for_instance`).
//!
//! The token acts inside one instance of one project of the run's owner
//! (the run's own, or the one the request names) and nothing else, and it
//! always expires: it is meant for a browser or an extension working with
//! that instance. It is minted here rather than through a program call
//! because its value is in the answer, and a program call's answer is
//! stored in its task row and journaled: this way the value leaves once,
//! to the run, and the row keeps only its hash. The rules and the write
//! are `weft_access_store::instance_tokens::mint`, the same a person's
//! `weft token mint --instance` goes through.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode};

use weft_access_store::instance_tokens::{self, InstanceTokenMint};
use weft_broker_client::protocol::ProgramMintInstanceTokenRequest;
use weft_core::program::MintedInstanceToken;

use crate::auth::{AuthedCaller, Role};
use crate::state::BrokerState;
use weft_core::wire_shape::Json;

pub async fn mint_instance_token(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ProgramMintInstanceTokenRequest>,
) -> Result<Json<MintedInstanceToken>, (StatusCode, String)> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    let run = crate::scope::require_run_id_scope(&state.scope_cache, &state.pool, &caller, req.run_id).await?;
    // The run's owner is the anchor; the project named is held to it by
    // the mint itself.
    let mint = InstanceTokenMint {
        id: req.id,
        tenant: &run.tenant,
        project: req.project.unwrap_or(run.project),
        instance: &req.instance,
        name: req.name.as_deref(),
        allowed_tags: &[],
        allowed_displays: &[],
        all_displays: req.displays,
        expires_in_secs: req.expires_in_secs,
        now_unix: crate::handlers::unix_now_secs(),
    };
    let minted = instance_tokens::mint(&state.pool, &mint).await.map_err(|refused| {
        let status = StatusCode::from_u16(refused.status()).expect("mint statuses are valid");
        (status, refused.to_string())
    })?;
    Ok(Json(MintedInstanceToken { id: req.id, token: minted.token, expires_at_unix: minted.expires_at }))
}
