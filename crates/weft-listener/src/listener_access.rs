//! Resolving a signal's connection (`SignalSpec.access`) into an HTTP
//! client whose requests are signed in: the shared plumbing that makes
//! authed polls, authed streams, and minted sockets exist for EVERY
//! kind. What the broker answers is kept by the process (`crate::reach`)
//! and fetched again when the outside refuses it ([`refused`]), before it
//! stops being good, or when a connection changed.

use anyhow::{Context, Result};
use weft_broker_client::protocol::{ListenerResolveRequest, ListenerResolvedSource};
use weft_core::access::client::{authed_client, plain_client, resolve_steps};
use weft_core::primitive::AccessRef;
use weft_core::reqwest_middleware;

use crate::kinds::SpawnCtx;

/// A connection's credentials, and whether they were fetched just now
/// (a refusal of fresh ones cannot heal by fetching again).
pub struct Resolved {
    pub source: ListenerResolvedSource,
    pub fresh: bool,
}

/// What the broker is asked for: the signal's tenant and instance (both
/// traveled with the registration, the same trust the listener's fires
/// already ride on). An instance's signal resolves through that
/// instance's connection alone.
fn request(access: &AccessRef, ctx: &SpawnCtx) -> ListenerResolveRequest {
    ListenerResolveRequest {
        tenant: ctx.fire.tenant_id().to_string(),
        for_instance: ctx.fire.for_instance().cloned(),
        access_id: access.id.clone(),
        service: access.service.clone(),
        required_values: access.required_values.clone(),
    }
}

/// The signal's connection, as kept for its registration or fetched from
/// the broker.
pub async fn resolve(access: &AccessRef, ctx: &SpawnCtx) -> Result<Resolved> {
    let req = request(access, ctx);
    let (token, registration) = (ctx.fire.token(), ctx.fire.generation());
    if let Some(source) = ctx.reach.credentials(token, registration, &req) {
        return Ok(Resolved { source, fresh: false });
    }
    let source = ctx.events_broker.listener_resolve(&req).await.with_context(|| format!("resolving the '{}' connection", access.service))?;
    ctx.reach.keep_credentials(token, registration, &req, source.clone());
    Ok(Resolved { source, fresh: true })
}

/// The outside refused the signal's credentials: they are fetched again
/// next time.
pub fn refused(ctx: &SpawnCtx) {
    ctx.reach.refused(ctx.fire.token());
}

/// The client one outbound use of this signal makes its calls with:
/// signed in through the signal's connection when one is set, plain
/// otherwise, and whether its credentials were fetched just now.
pub async fn client_for(
    access: &Option<AccessRef>,
    ctx: &SpawnCtx,
) -> Result<(reqwest_middleware::ClientWithMiddleware, bool)> {
    let Some(access) = access else { return Ok((plain_client(), false)) };
    let resolved = resolve(access, ctx).await?;
    let steps = resolve_steps(&resolved.source.auth, &resolved.source.values)
        .map_err(|e| anyhow::anyhow!("applying the connection's sign-in: {e}"))?;
    Ok((authed_client(steps), resolved.fresh))
}

/// Whether the broker's answer about a signal's connection is one that
/// asking again repeats: the request or a provider refused what was asked
/// (400), the connection is gone or not the signal's to use (404), its
/// credential was refused and only a reconnect brings it back (409), or
/// what it was asked for is gone (410). A provider that could not answer
/// for a moment reaches the listener as 502, never as one of these. A
/// broker that was not reached, or that answered otherwise, may answer
/// otherwise later: that is tried again, within the deliveries the alarm
/// gives a wake (`crate::kinds::wake_delivered`), or on a held signal's
/// ladder (`crate::kinds::repeats`).
pub fn answered_for_good(e: &anyhow::Error) -> bool {
    use reqwest::StatusCode;
    e.chain().filter_map(|cause| cause.downcast_ref::<weft_broker_client::BrokerRefused>()).any(|refused| {
        matches!(refused.status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND | StatusCode::CONFLICT | StatusCode::GONE)
    })
}

/// A failure of the broker's answer about a signal's connection, marked
/// [`crate::kinds::ForGood`] when asking again repeats it
/// ([`answered_for_good`]).
pub fn for_good_when_answered(e: anyhow::Error) -> anyhow::Error {
    if answered_for_good(&e) {
        crate::kinds::for_good(e)
    } else {
        e
    }
}

/// The most of a refusing answer's body [`is_refusal`] reads, and how
/// long it waits for it: an error body is small, and an endpoint that
/// holds one open must not hold the signal's loop with it.
const REFUSAL_BODY_CAP: usize = 64 * 1024;
const REFUSAL_BODY_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Whether a non-success answer says the credentials were refused, read
/// off its status and its body
/// ([`weft_core::access::client::RefusalKind::of_answer`]: a 403 naming a
/// rate limit is a busy endpoint). A body that does not arrive within
/// [`REFUSAL_BODY_WAIT`], or is not JSON, names no code, and the status
/// decides alone.
pub async fn is_refusal(mut answer: reqwest::Response) -> bool {
    let status = answer.status();
    let mut body = Vec::new();
    let _ = tokio::time::timeout(REFUSAL_BODY_WAIT, async {
        while body.len() < REFUSAL_BODY_CAP {
            match answer.chunk().await {
                Ok(Some(chunk)) => body.extend_from_slice(&chunk),
                Ok(None) | Err(_) => break,
            }
        }
    })
    .await;
    let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    weft_core::access::client::RefusalKind::of_answer(status, &body) == weft_core::access::client::RefusalKind::Auth
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answered(status: reqwest::StatusCode) -> anyhow::Error {
        anyhow::Error::new(weft_broker_client::BrokerRefused { path: "/v1/access/listener-resolve".into(), status, body: String::new() })
            .context("resolve the signal's connection")
    }

    #[test]
    fn only_an_answer_asking_again_repeats_is_answered_for_good() {
        use reqwest::StatusCode;
        for status in [StatusCode::BAD_REQUEST, StatusCode::NOT_FOUND, StatusCode::CONFLICT, StatusCode::GONE] {
            assert!(answered_for_good(&answered(status)), "{status}");
        }
        // A provider that could not answer reaches the listener as the
        // access store's 502, and a broker in trouble answers 5xx: either
        // may pass, so it is tried again.
        for status in [StatusCode::BAD_GATEWAY, StatusCode::SERVICE_UNAVAILABLE, StatusCode::INTERNAL_SERVER_ERROR] {
            assert!(!answered_for_good(&answered(status)), "{status}");
        }
        assert!(!answered_for_good(&anyhow::anyhow!("connection refused")));
    }
}
