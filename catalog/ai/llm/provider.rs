//! The shared body of every provider node.
//!
//! A provider node (a service's Model node, or CustomProvider) is the
//! one place a model is named. Its output is a single `LlmProvider`
//! object the inference nodes interpret: the service kind, the model,
//! the connection's Access marker, and any service-specific knobs. The
//! connection comes from where the node keeps it: a Model node takes
//! it on its `access` wire from the service's Access node (the one
//! place that connection is picked, so many models share one pick),
//! CustomProvider picks its own (an endpoint whose key belongs to it
//! alone, and which may need none). The body is the same for every
//! service, so it lives here once.

use serde_json::{json, Map, Value};

use weft::node::NodeOutput;
use weft::{Access, RunContext, WeftError, WeftResult};

/// OpenRouter's API base, for the single-service nodes (embeddings,
/// rerank) that call it directly.
pub const OPENROUTER_API: &str = "https://openrouter.ai/api/v1";

/// The input a Model node takes its service's connection on, wired
/// from that service's Access node.
pub const ACCESS_INPUT: &str = "access";

/// The connection picker of a provider node that keeps its own pick
/// (CustomProvider).
pub const CONNECTION_INPUT: &str = "connection";

/// A Model node's body: its `LlmProvider` object, with the connection
/// wired in on [`ACCESS_INPUT`]. The typed read fails HERE on a
/// malformed marker, at the node that names the model, not later
/// inside the inference node that consumes it.
pub async fn emit_model(ctx: &RunContext, kind: &str) -> WeftResult<()> {
    let account: Access = ctx.inputs.get(ACCESS_INPUT)?;
    emit(ctx, kind, Some(account)).await
}

/// Emit the `LlmProvider` object: `kind` + `model` + the connection
/// (`account`, omitted when there is none) + every OTHER declared
/// input that holds a value, forwarded verbatim under its own name.
/// Reading the declared inputs off the bag keeps the metadata the
/// single source of truth: a service-specific knob added there rides
/// the object with no code change, and none can silently desync.
pub async fn emit(ctx: &RunContext, kind: &str, account: Option<Access>) -> WeftResult<()> {
    let mut provider = Map::new();
    // Service-specific knobs first; the fields this function OWNS
    // (`kind`, `model`, `account`) are inserted after, so no declared
    // input can ever overwrite the object's identity.
    for (name, value) in ctx.inputs.declared() {
        if name == ACCESS_INPUT || name == CONNECTION_INPUT || name == "model" || value.is_null() {
            continue;
        }
        provider.insert(name.clone(), value.clone());
    }
    provider.insert("kind".into(), json!(kind));
    provider.insert("model".into(), json!(ctx.inputs.get::<String>("model")?));
    if let Some(account) = account {
        provider.insert("account".into(), account.to_value());
    }
    ctx.pulse_downstream(NodeOutput::new().set("provider", Value::Object(provider))).await
}

/// The wired `LlmProvider` object decoded for a single-service
/// endpoint node that needs the model too (rerank, embeddings):
/// refuses a provider of any other kind, and hands back the model and
/// the connection. The one reader beside `call::assemble`, so the object
/// has exactly two decoding paths (the multi-provider inference one
/// and this single-service one), never a per-node copy. `what` names
/// the capability ("embeddings", "reranking").
pub fn read_for(
    ctx: &RunContext,
    expected_kind: &str,
    what: &str,
) -> WeftResult<(String, Access)> {
    let provider = ctx.inputs.nested("provider")?;
    let kind: String = provider.get("kind")?;
    if kind != expected_kind {
        // Name the NODE the user has to wire, not the wire-level kind.
        let (api, node) = match expected_kind {
            "openrouter" => ("OpenRouter", "OpenRouterModel"),
            other => (other, other),
        };
        return Err(WeftError::Input(format!(
            "{what} speaks the {api} API; wire an {node} (got '{kind}')"
        )));
    }
    let model: String = provider.get("model")?;
    let account: Access = match provider.opt("account")? {
        Some(a) => a,
        None => {
            return Err(WeftError::Input(
                "the wired LlmProvider object carries no connection; wire the service's Access node into its Model node"
                    .to_string(),
            ))
        }
    };
    Ok((model, account))
}
