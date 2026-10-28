//! The shared outbound-socket engine: dial, handshake, heartbeat,
//! declarative replies, fire per inbound frame, reconnect on the
//! shared backoff ladder. ONE engine, driven by two callers with
//! different configs: the `socket_listen` kind (per-signal socket,
//! static or minted address) and the `provider_events` kind (one
//! socket per connection, address minted from the service recipe,
//! events fanned to every subscription).
//!
//! The caller supplies two closures: `prepare` runs at the start of
//! EVERY connect cycle and answers the cycle's plan (minted addresses
//! are single-use, so the mint must re-run per reconnect), and
//! `on_event` receives each inbound payload after the reply rules ran.

use std::collections::BTreeMap;
use std::time::Instant;

use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use weft_core::access::client::run_connect_call;
use weft_core::access::spec::{lookup_path, MintedSocket, ReplyRule};

use crate::kinds::event_source::{Backoff, Said};
use crate::kinds::ServingReport;

/// One connect cycle's plan, answered by the caller's `prepare`.
pub struct CyclePlan {
    /// The ws(s) address to dial this cycle.
    pub url: String,
    /// Sent once right after the socket opens.
    pub handshake: Option<Message>,
    /// Resent every `heartbeat_secs`; `None` = protocol ping/pong only.
    pub heartbeat: Option<Message>,
    pub heartbeat_secs: u64,
    /// Declarative acks, applied to every inbound frame before
    /// `on_event` sees it.
    pub replies: Vec<ReplyRule>,
}

/// How `prepare` failed, which decides whether the engine retries.
pub enum PrepareError {
    /// Transient (the mint endpoint busy, down or refusing the
    /// credential, the broker unreachable): wait on the ladder and try
    /// again.
    Transient(anyhow::Error),
    /// Asking again answers the same: the configuration itself is broken
    /// (a malformed frame, a template naming a value that does not
    /// exist), or the signal's connection is gone, refused or needs a
    /// reconnect ([`PrepareError::of_resolve`]), which only a person
    /// fixes. The signal is given up with why, and the engine logs loud
    /// and exits.
    Fatal(anyhow::Error),
}

impl PrepareError {
    /// A failure resolving the signal's connection:
    /// [`PrepareError::Fatal`] when asking again repeats it
    /// ([`crate::kinds::repeats`]), as a wake gives its signal up on it;
    /// [`PrepareError::Transient`] otherwise (the broker not reached, a
    /// provider busy for now).
    pub fn of_resolve(e: anyhow::Error) -> Self {
        if crate::kinds::repeats(&e) {
            PrepareError::Fatal(e)
        } else {
            PrepareError::Transient(e)
        }
    }
}

/// Mint one cycle's socket address from a [`MintedSocket`]: run the
/// connect call with the resolved values and read the capture named
/// by `urlFrom`. Shared by every kind that dials a minted address;
/// these addresses are single-use, so callers run this per cycle.
pub async fn mint_socket_url(
    minted: &MintedSocket,
    values: &BTreeMap<String, String>,
) -> Result<String, PrepareError> {
    let Some(call) = &minted.connect else {
        // Validation refuses a recipe without the call; one landing
        // here anyway is misconfigured, and retrying would spin.
        return Err(PrepareError::Fatal(anyhow::anyhow!(
            "the socket recipe declares no connect call to mint the address"
        )));
    };
    let resp = run_connect_call(call, values).await.map_err(mint_failed)?;
    let capture = call
        .captures
        .iter()
        .find(|c| c.name == minted.url_from)
        .ok_or_else(|| {
            PrepareError::Fatal(anyhow::anyhow!(
                "the connect call captures nothing named '{}'",
                minted.url_from
            ))
        })?;
    lookup_path(&resp, &capture.path)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            PrepareError::Transient(anyhow::anyhow!(
                "the connect call answered nothing at '{}'",
                capture.path
            ))
        })
}

/// How a mint's connect call failing fails the cycle. A call that cannot
/// be built, and a definite no to the request itself (a 400, a 404),
/// answer the same to the same call: the signal is given up. A service
/// not reached or busy is tried again, and so is a refused credential,
/// which [`mint_signed_in`] sorts out.
fn mint_failed(e: weft_core::access::client::ConnectCallError) -> PrepareError {
    use weft_core::access::client::{ConnectCallError, RefusalKind};
    match e {
        ConnectCallError::Unbuildable(_) | ConnectCallError::Refused { kind: RefusalKind::Other, .. } => PrepareError::Fatal(e.into()),
        ConnectCallError::Unreached(_) | ConnectCallError::Refused { kind: RefusalKind::Temporary | RefusalKind::Auth, .. } => {
            PrepareError::Transient(e.into())
        }
    }
}

/// Whether a mint failed because the service refused the credential it
/// was called with: a refusal of who is calling, never a provider busy
/// or down, nor one refusing the request itself.
fn refused(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref(),
        Some(weft_core::access::client::ConnectCallError::Refused { kind: weft_core::access::client::RefusalKind::Auth, .. })
    )
}

/// [`mint_socket_url`] with `values` resolved from the signal's
/// connection (`fresh` when fetched just now). A refusal of credentials
/// kept from before drops them, so the next cycle fetches them again; a
/// refusal of ones fetched just now cannot heal by asking again, so it is
/// fatal.
pub async fn mint_signed_in(
    minted: &MintedSocket,
    values: &BTreeMap<String, String>,
    fresh: bool,
    ctx: &crate::kinds::SpawnCtx,
) -> Result<String, PrepareError> {
    mint_socket_url(minted, values).await.map_err(|e| match e {
        PrepareError::Transient(e) if refused(&e) && fresh => {
            PrepareError::Fatal(e.context("the connection's credential, fetched just now, was refused"))
        }
        PrepareError::Transient(e) if refused(&e) => {
            crate::listener_access::refused(ctx);
            PrepareError::Transient(e)
        }
        e => e,
    })
}

type Prepare = Box<dyn FnMut() -> BoxFuture<'static, Result<CyclePlan, PrepareError>> + Send>;
type OnEvent = Box<dyn FnMut(Value) -> BoxFuture<'static, ()> + Send>;

/// Run the engine until aborted (or a fatal prepare error). `target`
/// names the driving kind in every log line. `report` is where the
/// node's display reads what the engine is doing, said when it changes
/// (`crate::kinds::event_source::Said`): a person looking at a stuck
/// trigger sees since when it reconnects and why, not a panel that says
/// nothing.
pub fn spawn(
    mut prepare: Prepare,
    mut on_event: OnEvent,
    target: &'static str,
    report: ServingReport,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Backoff::new();
        let mut said = Said::new(report);
        loop {
            let plan = match prepare().await {
                Ok(p) => p,
                Err(PrepareError::Transient(e)) => {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %format!("{e:#}"), "connect preparation failed; retrying");
                    said.down(format!("connect preparation failed: {e:#}"));
                    backoff.wait_then_climb().await;
                    continue;
                }
                Err(PrepareError::Fatal(e)) => {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %format!("{e:#}"), "connect preparation fails the same every time; giving up on this socket");
                    said.stopped(format!("{e:#}"));
                    return;
                }
            };

            let stream = match connect_async(&plan.url).await {
                Ok((s, _resp)) => {
                    info!(target: "weft_listener::socket_engine", kind = target, "socket connected");
                    said.up("connected");
                    s
                }
                Err(e) => {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "connect failed; retrying");
                    said.down(format!("connect failed: {e}"));
                    backoff.wait_then_climb().await;
                    continue;
                }
            };
            let connected_at = Instant::now();
            let (mut write, mut read) = stream.split();

            if let Some(frame) = &plan.handshake {
                if let Err(e) = write.send(frame.clone()).await {
                    warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "handshake send failed; reconnecting");
                    said.down(format!("handshake send failed: {e}"));
                    backoff.wait_then_climb().await;
                    continue;
                }
            }

            let mut ticker = interval(Duration::from_secs(plan.heartbeat_secs.max(1)));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // Consume the immediate first tick so the heartbeat does
            // not fire the instant the loop starts.
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick(), if plan.heartbeat.is_some() => {
                        if let Some(m) = &plan.heartbeat {
                            if let Err(e) = write.send(m.clone()).await {
                                warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "heartbeat send failed; reconnecting");
                                said.down(format!("heartbeat send failed: {e}"));
                                break;
                            }
                        }
                    }
                    msg = read.next() => {
                        match msg {
                            Some(Ok(m)) => {
                                let Some(payload) = inbound_payload(m) else { continue };
                                // Acks first: a gateway that demands
                                // them drops the line on a slow one,
                                // and the fire path may be slower.
                                for rule in &plan.replies {
                                    if let Some(frame) = rule.reply_to(&payload) {
                                        if let Err(e) = write.send(Message::Text(frame.into())).await {
                                            warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "reply send failed");
                                        }
                                    }
                                }
                                on_event(payload).await;
                            }
                            Some(Err(e)) => {
                                warn!(target: "weft_listener::socket_engine", kind = target, error = %e, "socket error; reconnecting");
                                said.down(format!("socket error: {e}"));
                                break;
                            }
                            None => {
                                info!(target: "weft_listener::socket_engine", kind = target, "socket closed; reconnecting");
                                said.down("socket closed");
                                break;
                            }
                        }
                    }
                }
            }

            backoff.reset_if_healthy(connected_at.elapsed());
            backoff.wait_then_climb().await;
        }
    })
}

/// Convert an inbound message into the JSON fire payload. Text that
/// parses as JSON fires as JSON; otherwise as a JSON string. Binary
/// fires as a base64 JSON string (the fire pipeline is JSON-typed end
/// to end). Ping/pong/close are transport-level, not events.
pub fn inbound_payload(msg: Message) -> Option<Value> {
    use base64::Engine as _;
    match msg {
        Message::Text(t) => Some(crate::kinds::event_source::coerce_text_payload(t.as_str())),
        Message::Binary(b) => Some(Value::String(
            base64::engine::general_purpose::STANDARD.encode(b),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{mint_failed, PrepareError};
    use weft_core::access::client::{ConnectCallError, RefusalKind};

    /// A mint the service said a definite no to gives the signal up; one
    /// it was busy or down for, or whose credential it refused, is tried
    /// again.
    #[test]
    fn only_a_definite_no_to_the_mint_gives_the_signal_up() {
        let refused = |kind| ConnectCallError::Refused { kind, why: "no".into() };
        assert!(matches!(mint_failed(refused(RefusalKind::Other)), PrepareError::Fatal(_)), "a 400 answers the same again");
        assert!(matches!(mint_failed(ConnectCallError::Unbuildable("no url".into())), PrepareError::Fatal(_)));
        assert!(matches!(mint_failed(refused(RefusalKind::Temporary)), PrepareError::Transient(_)), "a 429 or 5xx");
        assert!(matches!(mint_failed(refused(RefusalKind::Auth)), PrepareError::Transient(_)), "mint_signed_in decides");
        assert!(matches!(mint_failed(ConnectCallError::Unreached("timed out".into())), PrepareError::Transient(_)));
    }
}
