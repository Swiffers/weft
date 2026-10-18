//! The shared translation between port values and the caller's wire,
//! per the trigger's declared data type (`json`, `text`, `bytes`):
//!
//!   - OUT (`Reply`, `Stream`): a port value becomes an
//!     [`OutboundChunk`]. `json` carries any value; `text` wants a
//!     string; `bytes` wants a stored file (weft's one representation
//!     of bytes), whose content the node reads back.
//!   - IN (`Route`, `Socket`): an [`InboundMessage`] becomes the value
//!     the program declared it to be ([`take_message`]), or the
//!     caller's mistake in words the caller can act on. A byte body is
//!     stored as a run-scoped file and the stored-file value is what
//!     flows.
//!
//! Bytes never travel a wire: a picture a caller sends inside a JSON
//! body (a `data:` URL or bare base64 in a place declared as a file
//! kind) is stored on the way in and the stored-file value is what
//! flows ([`take_inbound`]); a stored file inside a JSON answer goes
//! out as a link the caller can fetch ([`link_files`]).
//!
//! One place, so the four nodes agree and a fifth node an author writes
//! can reuse it.

use serde_json::Value;

use weft::caller::{InboundMessage, LiveRequest, OutboundChunk, ResponseHead};
use weft::signal::DataType;
use weft::storage::{FileHandle, StorageScope, StoredFile};
use weft::{RunContext, WeftError, WeftResult, WeftType};

/// The opening request the trigger woke on, out of the wake payload.
/// Both triggers read the same payload, so they read it here and a
/// payload that is not a request is refused in the same words.
pub fn opening_request(ctx: &RunContext) -> WeftResult<LiveRequest> {
    ctx.wake.record().and_then(|v| {
        serde_json::from_value(v)
            .map_err(|e| weft::node_error(format!("the wake payload is not a caller request: {e}")))
    })
}

/// The fixed request ports' values, as `(port, value)` pairs, from the
/// caller's opening request, keeping only the ports the trigger's own
/// metadata declares (`trigger`): `Route` has no `params` (a program
/// reads a path capture on an output it declares under the capture's
/// name, which a capture called `params` is too), and `Socket` has no
/// `method` (an upgrade is always a GET). `fired` is always true: a port
/// a program can gate on "the trigger was called" without reading any
/// request part. Header names are lowercased, as HTTP reads them.
pub fn request_ports(trigger: &weft::NodeMetadata, request: &LiveRequest) -> Vec<(&'static str, Value)> {
    let headers: serde_json::Map<String, Value> = request
        .headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), Value::String(v.clone())))
        .collect();
    vec![
        ("fired", Value::Bool(true)),
        ("method", Value::String(request.method.clone())),
        ("path", Value::String(request.path.clone())),
        ("params", serde_json::to_value(&request.params).expect("a string map serializes")),
        ("query", serde_json::to_value(&request.query).expect("a string map serializes")),
        ("headers", Value::Object(headers)),
        ("caller", request.caller.clone().unwrap_or(Value::Null)),
    ]
    .into_iter()
    .filter(|(port, _)| trigger.outputs.iter().any(|o| o.name == *port))
    .collect()
}

/// A port value as the wire wants it for `data_type`. A JSON value's
/// stored files go out as links (see [`link_files`]).
pub async fn chunk_for(
    ctx: &RunContext,
    data_type: DataType,
    value: Value,
) -> WeftResult<OutboundChunk> {
    match data_type {
        DataType::Json => Ok(OutboundChunk::Json(link_files(ctx, value).await?)),
        DataType::Text => match value {
            Value::String(s) => Ok(OutboundChunk::Text(s)),
            other => weft::node_bail!(
                "this caller speaks text (the trigger's dataType), so the body must be a \
                 String; got {}",
                kind_of(&other)
            ),
        },
        DataType::Bytes => {
            // A stored-file VALUE (the marker object), never a bare key
            // string: a key is an implementation detail no port carries.
            let file = StoredFile::from_value(&value).map_err(|e| {
                weft::node_error(format!(
                    "this caller speaks bytes (the trigger's dataType), so the body must be a \
                     stored file; {e}"
                ))
            })?;
            let (_, bytes) = ctx
                .storage(StorageScope::Run)
                .get_bytes(&FileHandle::Key(file.key))
                .await?;
            Ok(OutboundChunk::Bytes(bytes.to_vec()))
        }
    }
}

/// An inbound message (a route's body, one socket message) as the value
/// the program declared it to be, `declared`. JSON is held to the type
/// as [`take_inbound`] holds it (inline pictures stored, open records
/// trimmed); text is held to it as a String; bytes are stored (run
/// scope, swept with the run) under `name`, and the stored-file value
/// is held to it.
///
/// A byte message is stored as the type the caller's `Content-Type`
/// states ([`weft::storage::received_file`]); its first bytes fill one in
/// when none was stated (a socket frame never states one) and hold a
/// place declared `Image` to bytes that are an image, whatever was stated.
///
/// `Ok(Err(why))` is the caller's mistake (a message that does not fit
/// what the program declared), `Err` the node's own failure.
pub async fn take_message(
    ctx: &RunContext,
    message: InboundMessage,
    content_type: Option<&str>,
    name: &str,
    declared: &WeftType,
) -> WeftResult<Result<Value, String>> {
    match message {
        InboundMessage::Json(v) => take_inbound(ctx, v, declared, name).await,
        InboundMessage::Text(s) => Ok(declared
            .conform_value(Value::String(s))
            .map_err(|why| format!("{name} does not fit {declared}: {why}"))),
        InboundMessage::Bytes(b) => {
            // Bytes only ever become a stored file, so a place that holds
            // no file kind is refused before anything is written.
            if declared.admitted_file_kinds().is_empty() {
                return Ok(Err(format!("{name} is bytes, and it takes {declared}")));
            }
            let mime = match weft::storage::received_file(&b, content_type, declared) {
                Ok((mime, _)) => mime,
                Err(why) => return Ok(Err(format!("{name}: the bytes sent are not what it takes: {why}"))),
            };
            let file = ctx.storage(StorageScope::Run).put(b, &mime, name, None).await?;
            Ok(declared.conform_value(file).map_err(|why| format!("{name} does not fit {declared}: {why}")))
        }
    }
}

/// A value the caller sent, held to the type the program declared for
/// it (a route's `body`, one item of a socket's `inbound`): every
/// picture it carries in a file-typed place as a `data:` URL or bare
/// base64 is stored on the way in (run scope, swept with the run) and
/// the stored-file value takes its place, then the whole is held to
/// the type, which drops what an open record does not declare. The
/// bytes are held to the kinds their place declares (base64 of a video
/// in an `Image` or `Image | Null` field is refused), and stored under
/// the type a `data:` link states, or the one the bytes announce when
/// none was stated.
///
/// A caller never hands over a stored-file value itself: a marker
/// names a key or a URL, and taking one on the caller's word would let
/// them point the program at a file of this project or make the worker
/// fetch an address of their choosing. Any marker anywhere in the
/// value is refused.
///
/// `Ok(Err(why))` is the CALLER's mistake, said in words the caller can
/// act on; `Err` is the node's own failure (storage refusing a write).
pub async fn take_inbound(
    ctx: &RunContext,
    value: Value,
    declared: &WeftType,
    name: &str,
) -> WeftResult<Result<Value, String>> {
    if weft::storage::media::holds_file_marker(&value) {
        return Ok(Err(format!(
            "{name} carries a stored-file object; send a file's bytes as a data: URL or base64 instead"
        )));
    }
    let mut stored = std::collections::HashMap::new();
    for (slot, slot_ty) in weft::storage::media::typed_media_slots(&value, declared) {
        let text = match &slot {
            Value::String(text) => text,
            // Anything else in a file place (a null, a number) is held
            // to the type with the rest of the value just below.
            _ => continue,
        };
        let (mime, bytes) = match decode_inline(text) {
            Ok(decoded) => decoded,
            Err(why) => {
                return Ok(Err(format!(
                    "{name}: a place declared {slot_ty} takes a picture as a data: URL or base64; {why}"
                )))
            }
        };
        let mime = match weft::storage::received_file(&bytes, mime.as_deref(), &slot_ty) {
            Ok((mime, _)) => mime,
            Err(why) => {
                return Ok(Err(format!(
                    "{name}: the bytes sent for a place declared {slot_ty} are not what it takes: {why}"
                )))
            }
        };
        let file = ctx.storage(StorageScope::Run).put(bytes, &mime, name, None).await?;
        stored.insert(slot.to_string(), file);
    }
    let value = weft::storage::media::substitute_media(&value, declared, &stored);
    Ok(declared.conform_value(value).map_err(|why| format!("{name} does not fit {declared}: {why}")))
}

/// The bytes an inline picture carries, and the media type when it said
/// one: a `data:` URL names it, bare base64 does not.
fn decode_inline(text: &str) -> Result<(Option<String>, Vec<u8>), String> {
    if let Some(rest) = text.strip_prefix("data:") {
        let (mime, payload) = rest
            .split_once(";base64,")
            .ok_or_else(|| "the data: URL carries no base64 payload".to_string())?;
        let bytes = weft::storage::media::base64_decode(payload)
            .map_err(|e| format!("the data: URL payload is not valid base64: {e}"))?;
        return Ok((Some(mime.to_string()), bytes));
    }
    let bytes = weft::storage::media::base64_decode(text.trim())
        .map_err(|e| format!("the string is not valid base64: {e}"))?;
    Ok((None, bytes))
}

/// Every stored file inside a JSON answer, replaced by what a caller can
/// use: `{ "url", "mimeType", "filename", "sizeBytes" }`, the url being a
/// link this install serves the caller (a browser follows it straight
/// into an `<img>`). Walks lists and objects, so a file nested in a row
/// is linked too. A file the install cannot mint a link for fails the
/// node: an answer with no way to the bytes is not an answer.
pub async fn link_files(ctx: &RunContext, value: Value) -> WeftResult<Value> {
    match value {
        Value::Object(map) => {
            if let Ok(file) = StoredFile::from_value(&Value::Object(map.clone())) {
                let url = ctx
                    .storage(StorageScope::Run)
                    .caller_link(&FileHandle::Key(file.key.clone()), None)
                    .await?
                    .ok_or_else(|| {
                        weft::node_error(format!(
                            "the answer carries the stored file {} but this install has no address a \
                             caller could fetch it from",
                            file.filename
                        ))
                    })?;
                return Ok(serde_json::json!({
                    "url": url,
                    "mimeType": file.mime_type,
                    "filename": file.filename,
                    "sizeBytes": file.size_bytes,
                }));
            }
            let mut linked = serde_json::Map::with_capacity(map.len());
            for (key, inner) in map {
                linked.insert(key, Box::pin(link_files(ctx, inner)).await?);
            }
            Ok(Value::Object(linked))
        }
        Value::Array(items) => {
            let mut linked = Vec::with_capacity(items.len());
            for item in items {
                linked.push(Box::pin(link_files(ctx, item)).await?);
            }
            Ok(Value::Array(linked))
        }
        other => Ok(other),
    }
}

/// The shape an answer node (`Reply`, `Stream`) sends in: its own
/// `answerAs` when set, else the shape the trigger's `dataType` declares.
/// Apart because what comes in and what goes out differ often: a GET has
/// no body at all and may still answer a picture as bytes.
pub fn answer_type(ctx: &RunContext) -> WeftResult<DataType> {
    match ctx.inputs.opt::<String>("answerAs")?.filter(|raw| !raw.is_empty()) {
        Some(raw) => DataType::parse_field(&raw)
            .map_err(|_| WeftError::Input(format!("answerAs must be json, text or bytes, got '{raw}'"))),
        None => Ok(ctx.caller_data_type().unwrap_or_default()),
    }
}

/// A stored file answered as bytes: the file's own type, and its name as
/// an inline `content-disposition` (a browser shows the picture, and
/// saving it keeps the name). Whatever the program set itself wins.
pub fn with_file_head(mut head: ResponseHead, file: &StoredFile) -> ResponseHead {
    if !head.has_content_type() {
        head = head.with_header("content-type", file.mime_type.clone());
    }
    if head.header("content-disposition").is_none() && !file.filename.is_empty() {
        head = head.with_header("content-disposition", inline_disposition(&file.filename));
    }
    head
}

/// `inline; filename="..."; filename*=UTF-8''...`: the quoted form for
/// old clients (ASCII only, quotes and backslashes dropped), the encoded
/// form for the exact name.
fn inline_disposition(filename: &str) -> String {
    let plain: String = filename
        .chars()
        .map(|c| if c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\' { c } else { '_' })
        .collect();
    let encoded: String = filename
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-' | b'_' | b'~' => (b as char).to_string(),
            other => format!("%{other:02X}"),
        })
        .collect();
    format!("inline; filename=\"{plain}\"; filename*=UTF-8''{encoded}")
}

/// The response head a node's `status` and `headers` inputs describe.
/// `headers` is a JSON object of string values (the port's declared
/// type); anything else is refused by name.
pub fn head_for(status: u16, headers: Option<&Value>) -> WeftResult<ResponseHead> {
    let mut head = ResponseHead::new(status);
    if let Some(headers) = headers {
        let Some(map) = headers.as_object() else {
            weft::node_bail!("headers must be an object of header name to value, got {}", kind_of(headers));
        };
        for (name, value) in map {
            let Some(value) = value.as_str() else {
                weft::node_bail!("header '{name}' must be a string, got {}", kind_of(value));
            };
            head = head.with_header(name.clone(), value.to_string());
        }
    }
    Ok(head)
}

/// Refuse a status or headers behind a Socket: a socket message has no
/// status line, so a program that set one meant a Route. `status` is
/// the node's default (200) and `headers` absent or empty when nothing
/// was set; anything else is the program's bug, said loud.
pub fn refuse_head_on_socket(node: &str, status: u16, headers: Option<&Value>) -> WeftResult<()> {
    if status != 200 || has_headers(headers) {
        weft::node_bail!(
            "{node} behind a Socket sends messages; a status ({status}) or headers have no \
             meaning on a socket. Drop them, or put the {node} behind a Route"
        );
    }
    Ok(())
}

/// Whether a program set headers: present and not an empty object.
pub fn has_headers(headers: Option<&Value>) -> bool {
    headers.is_some_and(|h| !h.as_object().is_some_and(|m| m.is_empty()))
}

/// A one-word description of a value's shape, for error messages.
pub fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "a list",
        Value::Object(_) => "an object",
    }
}
