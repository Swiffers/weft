//! A project's instances, as a program reaches them: what they provide,
//! their connections, their tokens, and the list of them
//! (`weft_core::program::instances::InstanceCall`).

use axum::http::StatusCode;
use weft_core::instance::InstanceId;
use weft_core::program::instances::InstanceCall;
use weft_core::program::{ConnectionsForgotten, InstanceHoldings, InstanceTrigger, ProgramCallOutcome, TokensRevoked};

use super::{answered, failed, Actor, Refused};
use crate::state::DispatcherState;

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.connections.list", super::Reach::Everyone),
    ("weft.connections.forget", super::Reach::Everyone),
    ("weft.values.get", super::Reach::Everyone),
    ("weft.values.change", super::Reach::Everyone),
    ("weft.values.forget", super::Reach::Everyone),
    ("weft.instances.list", super::Reach::Everyone),
    ("weft.tokens.revoke", super::Reach::Everyone),
];

/// Carry out one instance call of `actor` on project `project_id`.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    call: &InstanceCall,
) -> Result<ProgramCallOutcome, Refused> {
    // What instances' values, connections and tokens are keyed by: the
    // project's owning tenant, which the actor was held to.
    let tenant = actor.tenant();
    match call {
        InstanceCall::List => answered(&instances(state, tenant, project_id).await?),
        InstanceCall::ConnectionsList { instance } => {
            let grants = weft_access_store::list_grants(
                &state.pg_pool,
                tenant.as_str(),
                None,
                weft_access_store::GrantOwnerScope::Instance { project_id, instance },
            )
            .await
            .map_err(access_error)?;
            answered(&grants)
        }
        InstanceCall::ConnectionsForget { instance } => {
            let forgotten = weft_access_store::forget_instance_grants(&state.pg_pool, tenant.as_str(), project_id, instance)
                .await
                .map_err(access_error)?;
            answered(&ConnectionsForgotten { forgotten })
        }
        InstanceCall::ValuesGet { instance } => {
            let values = weft_access_store::instance_values(&state.pg_pool, tenant.as_str(), project_id, instance)
                .await
                .map_err(access_error)?;
            answered(&values)
        }
        InstanceCall::ValuesChange { instance, set, clear } => {
            let clear: Vec<(String, String)> = clear.iter().map(|f| (f.node.clone(), f.field.clone())).collect();
            answered(&crate::instance_values::change(state, tenant.as_str(), project_id, instance, set, &clear).await?)
        }
        InstanceCall::ValuesForget { instance } => answered(&crate::instance_values::forget(state, tenant.as_str(), project_id, instance).await?),
        InstanceCall::TokensRevoke { instance, id } => {
            let revoked = weft_access_store::instance_tokens::revoke(&state.pg_pool, tenant.as_str(), project_id, instance, *id)
                .await
                .map_err(failed("revoke tokens"))?;
            answered(&TokensRevoked { revoked })
        }
    }
}

/// Every instance weft holds anything for in `project_id`, as `actor`
/// asks for it (`weft instances`, `ctx.instances().list()`).
pub async fn list(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid) -> Result<Vec<InstanceHoldings>, Refused> {
    actor.may_act_on(state, project_id).await?;
    instances(state, actor.tenant(), project_id).await
}

/// Every instance weft holds anything for in the project, with what it
/// holds (`InstanceHoldings::merge` over each store's counts): what
/// [`list`] and the project's status answer.
pub(crate) async fn instances(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project_id: uuid::Uuid,
) -> Result<Vec<InstanceHoldings>, Refused> {
    let values = weft_access_store::instance_value_counts(&state.pg_pool, tenant.as_str(), project_id)
        .await
        .map_err(failed("instance values"))?;
    let connections = weft_access_store::instance_connection_counts(&state.pg_pool, tenant.as_str(), project_id)
        .await
        .map_err(failed("instance connections"))?;
    let tokens = weft_access_store::instance_tokens::counts(&state.pg_pool, tenant.as_str(), project_id, crate::lease::now_unix())
        .await
        .map_err(failed("instance tokens"))?;
    let copies = super::infra::observed_copies(state, project_id).await?;
    let waiting = crate::api::signal::waiting_fires(&state.pg_pool, project_id).await.map_err(failed("waiting fires"))?;
    let triggers: Vec<(InstanceId, InstanceTrigger)> = state
        .activations
        .list(project_id)
        .await
        .map_err(failed("activations"))?
        .into_iter()
        .filter_map(|a| {
            let instance = a.key.instance()?.clone();
            Some((
                instance,
                InstanceTrigger {
                    waiting: waiting.get(&a.key).cloned(),
                    trigger: a.key.trigger,
                    mode: a.lifecycle.mode(),
                },
            ))
        })
        .collect();
    let last_runs: Vec<(String, i64)> = sqlx::query_as(
        "SELECT instance_id, MAX(started_at) FROM run WHERE project_id = $1 AND instance_id IS NOT NULL GROUP BY instance_id",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await
    .map_err(failed("instance runs"))?;
    let last_runs = last_runs
        .into_iter()
        .map(|(instance, at)| InstanceId::new(instance).map(|instance| (instance, at)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("a run holds a broken instance: {e}")))?;
    Ok(InstanceHoldings::merge(values, connections, tokens, copies, triggers, last_runs))
}

/// A store error as the store answers it on every surface
/// (`weft_access_store::client_error`).
fn access_error(e: anyhow::Error) -> Refused {
    let (status, message) = weft_access_store::client_error(e);
    (StatusCode::from_u16(status).expect("store status codes are valid"), message)
}
