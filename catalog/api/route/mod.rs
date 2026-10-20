//! Route: trigger node that turns a weft program into an HTTP route. An
//! outside caller hits the route at the project's live URL; the
//! dispatcher matches the path and method, checks the caller against
//! the route's auth, holds the connection open and routes it to a
//! worker; this trigger fires a fresh run with the request on its
//! ports.
//!
//!   - `setup_trigger`: build a `Route` signal from the node's fields
//!     and register it. The dispatcher mounts the public route.
//!   - `run`: the caller is attached for this run. The opening request
//!     (`ctx.wake`) fans onto the fixed ports, and the body, read off
//!     the caller handle, lands whole on `body`, held to the type the
//!     program declared there (`-> (body: Signup)`, `JsonDict` when it
//!     declared none): a picture it carries in a file-typed place is
//!     stored on the way in, an open record drops the keys it does not
//!     declare, and a body that does not fit is answered `400` naming
//!     what is wrong, before anything downstream runs. A port the
//!     program adds reads the path capture of its name (`-> (id:
//!     String)` on `cards/{id}`). Nothing else here answers the caller;
//!     Reply, Stream and Close do, or a custom node through
//!     `ctx.http_caller()`.

use async_trait::async_trait;
use serde_json::Value;
use weft::caller::{InboundMessage, OutboundChunk, ResponseHead};
use weft::node::NodeOutput;
use weft::signal::{DataType, LiveConnectionConfig, Route};
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::wire;

#[derive(NodeManifest)]
pub struct RouteNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for RouteNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let fields = ctx.inputs.object()?;
        let common = LiveConnectionConfig::from_node_fields(fields).map_err(weft::node_error)?;
        ctx.register_signal(Route { common }).await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let request = wire::opening_request(&ctx)?;
        let http = ctx.http_caller().await?;
        let parts = http.request_parts()?;
        // Where the body comes from, and why this node decides it.
        //
        // A live call leaves its body on the open connection, and that
        // is `parts.body`. A `weft run --fire` has no connection: the
        // author typed the whole request, body included, and it arrives
        // as this trigger's wake payload like every other declared
        // field (`body?` in the metadata).
        //
        // So: the connection first, the wake when the connection has
        // nothing. Nothing upstream cuts the body out of the payload to
        // feed it back through the connection call, because that would
        // put a caller trigger's field name in the language, where no
        // node's vocabulary belongs.
        let body = match &parts.body {
            InboundMessage::Json(v) if v.is_null() => fired_body(&ctx)?,
            other => other.clone(),
        };
        let data_type = ctx.caller_data_type().unwrap_or_default();
        let declared = ctx.output_type("body")?;
        let body = match (data_type, body) {
            // A request with no body at all, which is every GET, leaves
            // `body` closed: the request ports below still fire, and a
            // GET is an ordinary way to call a route.
            (DataType::Json, InboundMessage::Json(Value::Null)) => None,
            // A json body is ONE object, the shape a declared record (or
            // `JsonDict`) holds: anything else is the caller's mistake.
            (DataType::Json, InboundMessage::Json(other)) if !other.is_object() => {
                let why = format!(
                    "the body has to be a json object, and the request sent {}",
                    wire::kind_of(&other)
                );
                return refuse(&http, data_type, why).await;
            }
            (DataType::Json, message @ InboundMessage::Json(_))
            | (DataType::Text, message @ InboundMessage::Text(_))
            | (DataType::Bytes, message @ InboundMessage::Bytes(_)) => {
                let content_type = request.header("content-type");
                match wire::take_message(&ctx, message, content_type, "body", &declared).await? {
                    Ok(body) => Some(body),
                    Err(why) => return refuse(&http, data_type, why).await,
                }
            }
            // The other shapes: a fired run whose body is text on a
            // json route or an object on a text one (a live connection
            // reads the body as the route's dataType already).
            (data_type, other) => {
                let why = format!(
                    "this route takes a {} body, and the request sent {}",
                    data_type.as_wire_str(),
                    shape_of(&other)
                );
                return refuse(&http, data_type, why).await;
            }
        };
        let output = match body {
            Some(body) => NodeOutput::new().set("body", body),
            None => NodeOutput::new(),
        };
        // A port the program added reads the path capture of its name
        // (the compiler holds every added port to a capture of the
        // path, `claimsRoute.capturesAreOutputs`). The fixed request
        // ports are set after it.
        let declared_ports = ctx.declared_outputs();
        let output = request
            .params
            .iter()
            .filter(|(name, _)| declared_ports.contains_key(*name))
            .fold(output, |out, (name, value)| out.set(name.clone(), Value::String(value.clone())));
        let output = wire::request_ports(self.manifest(), &request)
            .into_iter()
            .fold(output, |out, (port, value)| out.set(port, value));
        ctx.pulse_downstream(output).await
    }
}

/// Answer the caller `400` with `why`, the request it sent not being what
/// the program declared, and end the firing without starting anything
/// downstream: the request never became a run's input, so nothing past
/// the route has anything to act on, and the caller already has its
/// answer. A json route answers `{"error": why}`, the others the text.
async fn refuse(http: &weft::caller::HttpCaller, data_type: DataType, why: String) -> WeftResult<()> {
    let body = match data_type {
        DataType::Json => OutboundChunk::Json(serde_json::json!({ "error": why })),
        DataType::Text | DataType::Bytes => OutboundChunk::Text(why),
    };
    http.respond_with(ResponseHead::new(400), body).await
}

/// The body a FIRED run carries, off this trigger's own wake payload.
///
/// `body` is a field this node declares in its `firesWith`, optional
/// because a live call never has one there (it is on the connection)
/// and a GET never has one at all. Reading it here rather than having
/// something upstream serve it back as if a caller had sent it keeps
/// the question where it belongs: only this node knows that its `body`
/// field means the request's body.
///
/// A string is a text body, an object is a json one. Anything else is
/// refused by the fire payload contract before this runs, against the
/// type the metadata declares, so a bytes route is not fireable: its
/// body has no declared type a fire payload can carry.
fn fired_body(ctx: &RunContext) -> WeftResult<InboundMessage> {
    let Some(body) = ctx.wake.opt::<Value>("body")? else {
        return Ok(InboundMessage::Json(Value::Null));
    };
    Ok(match body {
        Value::String(text) => InboundMessage::Text(text),
        other => InboundMessage::Json(other),
    })
}

fn shape_of(message: &InboundMessage) -> &'static str {
    match message {
        InboundMessage::Json(_) => "json",
        InboundMessage::Text(_) => "text",
        InboundMessage::Bytes(_) => "bytes",
    }
}
