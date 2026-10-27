//! The files door: `/files/...` on the install's outside doors, where a
//! link to a stored file that goes through the install lands
//! (`weft_platform_traits::object_store::forwarded_link`).
//!
//! It is a plain forwarder. `GET`, `HEAD` and `PUT` on
//! `/files/runtime/<key>?<query>`, `<key>` a storage key, go to the store
//! as `<store>/<bucket>/runtime/<key>?<query>`, with the request's headers
//! a read or an upload needs (`Range`, `If-None-Match`, the body's length,
//! which every `PUT` must say) and the host the caller reached the install
//! at as `X-Forwarded-Host`; the store's status comes back as it is, `206`
//! and `304` included, with the answer headers a read or an upload needs. The
//! link's signature is the credential: the store checks it against that
//! host, so nothing here reads a database or a token. A cross-origin
//! preflight never gets here (the outside doors answer it).
//!
//! On a store the open internet reaches itself (Cloud Storage), links go
//! straight to it and the door answers 404.
//!
//! Beside it, `/files/upload/<verb>` passes a page's upload through a slot
//! on to the broker (`weft_core::storage::slot`), which holds each verb to
//! the slot in its body.

use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::state::DispatcherState;

/// Where the files door passes requests on to: the store's address as this
/// process reaches it, and its bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesTarget {
    pub endpoint: String,
    pub bucket: String,
}

/// The request headers a read or an upload of a stored file needs, passed
/// on as they came. `Host` is never one of them: the store hears it as
/// `X-Forwarded-Host`. The website's pass-through sends the same set,
/// minus `content-length` (it writes the held part's own length).
// SYNC: the read headers <-> packages/weft-connect/src/server/passthrough.ts PASSED_REQUEST_HEADERS
const PASSED_REQUEST_HEADERS: [header::HeaderName; 7] = [
    header::RANGE,
    header::IF_RANGE,
    header::IF_NONE_MATCH,
    header::IF_MODIFIED_SINCE,
    header::IF_MATCH,
    header::CONTENT_LENGTH,
    header::CONTENT_TYPE,
];

/// The store's answer headers that travel back to the caller: what a read
/// (a range, a conditional one, a download's name and type) and a part
/// upload (its `ETag`, which the page hands back to complete the upload)
/// need. Everything else stays behind: the connection's own headers, the
/// store's cross-origin ones (the outside doors write their own, and two
/// of a kind make a browser refuse the answer), and whatever names the
/// store (`Server`, `x-amz-request-id`).
const PASSED_ANSWER_HEADERS: [header::HeaderName; 12] = [
    header::CONTENT_TYPE,
    header::CONTENT_LENGTH,
    header::CONTENT_RANGE,
    header::CONTENT_DISPOSITION,
    header::CONTENT_ENCODING,
    header::CONTENT_LANGUAGE,
    header::ACCEPT_RANGES,
    header::ETAG,
    header::LAST_MODIFIED,
    header::CACHE_CONTROL,
    header::EXPIRES,
    header::RETRY_AFTER,
];

/// The most bytes one `PUT` through the door may carry: the most any S3
/// store takes in one part or one object (5 GiB). Every part weft signs
/// carries its exact length in its signature; this bounds what a caller
/// can make the door stream before the store says no.
const MAX_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Why a path under the door reaches no stored file.
const NO_FILE: &str = "no stored file at this path";

/// Where a request under the door goes in the store, or why it goes
/// nowhere: only a runtime file's object passes, named by a storage key
/// (`weft_core::storage::key::parse_key`). Each segment is decoded before
/// it is checked, so an escaped `..` or `/` is refused like a plain one,
/// and the path sent on is the parsed key's own (its grammar allows only
/// characters a URL path carries as they are), never the caller's text.
/// The query, which holds the signature, rides as it came.
fn target_url(target: &FilesTarget, path: &str, query: Option<&str>) -> Result<String, &'static str> {
    use weft_core::storage::key::{parse_key, valid_segment};
    use weft_platform_traits::object_store::{FILES_DOOR, FORWARDED_PREFIX};
    let rest = path.strip_prefix(&format!("/{FILES_DOOR}/")).ok_or("not a stored file's path")?;
    let key = rest.strip_prefix(FORWARDED_PREFIX).ok_or(NO_FILE)?;
    let segments = key
        .split('/')
        .map(|raw| percent_encoding::percent_decode_str(raw).decode_utf8().ok().filter(|segment| valid_segment(segment)))
        .collect::<Option<Vec<_>>>()
        .ok_or(NO_FILE)?;
    let key = parse_key(&segments.join("/")).map_err(|_| NO_FILE)?;
    let query = query.map(|q| format!("?{q}")).unwrap_or_default();
    Ok(format!("{}/{}/{FORWARDED_PREFIX}{}{query}", target.endpoint.trim_end_matches('/'), target.bucket, key.to_key()))
}

/// The host the caller reached the install at, which a link through the
/// door is signed for: read the way every link base is
/// (`weft_core::net::request_base_url`: a site's pass-through host, its
/// first hop when a chain lists several, else the request's own `Host`).
/// Whoever holds a link may send any host; the signature only matches the
/// one it was made for.
fn caller_host(headers: &HeaderMap) -> Option<HeaderValue> {
    let base = reqwest::Url::parse(&weft_core::net::request_base_url(headers)?).ok()?;
    let host = base.host_str()?;
    let authority = match base.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    HeaderValue::from_str(&authority).ok()
}

/// Why a `PUT` is not passed on: it says no length (so nothing bounds
/// what it streams), or a length no store takes in one piece.
fn put_refusal(headers: &HeaderMap) -> Option<(StatusCode, String)> {
    let Some(length) = headers.get(header::CONTENT_LENGTH) else {
        return Some((StatusCode::LENGTH_REQUIRED, "an upload through the install says its length (Content-Length)".into()));
    };
    match length.to_str().ok().and_then(|text| text.parse::<u64>().ok()) {
        Some(bytes) if bytes <= MAX_PUT_BYTES => None,
        Some(_) => Some((StatusCode::PAYLOAD_TOO_LARGE, format!("one upload through the install carries at most {MAX_PUT_BYTES} bytes"))),
        None => Some((StatusCode::BAD_REQUEST, "the upload's Content-Length is not a number of bytes".into())),
    }
}

/// `GET|HEAD|PUT /files/runtime/...`
pub async fn forward(State(state): State<DispatcherState>, request: Request) -> Response {
    let Some(target) = state.files.as_ref() else {
        return (StatusCode::NOT_FOUND, "this install serves stored files straight from its store").into_response();
    };
    pass_on(&state.http, target, request).await
}

/// Pass `request` on to the store at `target`, and its answer back. No
/// limit on the call's length: the file streams through it either way, as
/// fast as the caller sends or reads it.
async fn pass_on(http: &reqwest::Client, target: &FilesTarget, request: Request) -> Response {
    if !matches!(*request.method(), Method::GET | Method::HEAD | Method::PUT) {
        return (StatusCode::METHOD_NOT_ALLOWED, "a stored file is read (GET, HEAD) or uploaded (PUT) here").into_response();
    }
    let url = match target_url(target, request.uri().path(), request.uri().query()) {
        Ok(url) => url,
        Err(why) => return (StatusCode::NOT_FOUND, why).into_response(),
    };
    let (parts, body) = request.into_parts();
    let Some(host) = caller_host(&parts.headers) else {
        return (StatusCode::BAD_REQUEST, "the request names no host it reached the install at").into_response();
    };
    if parts.method == Method::PUT {
        if let Some(refused) = put_refusal(&parts.headers) {
            return refused.into_response();
        }
    }
    let mut outgoing = http.request(parts.method.clone(), &url).header("x-forwarded-host", host);
    for name in PASSED_REQUEST_HEADERS {
        if let Some(value) = parts.headers.get(&name) {
            outgoing = outgoing.header(name, value);
        }
    }
    if parts.method == Method::PUT {
        outgoing = outgoing.body(reqwest::Body::wrap_stream(body.into_data_stream()));
    }
    let answer = match outgoing.send().await {
        Ok(answer) => answer,
        Err(e) => {
            tracing::error!(target: "weft_dispatcher::files_door", error = %e, "the store did not answer");
            return (StatusCode::BAD_GATEWAY, "the install's file store does not answer").into_response();
        }
    };
    let mut response = Response::builder().status(answer.status());
    for name in PASSED_ANSWER_HEADERS {
        for value in answer.headers().get_all(&name) {
            response = response.header(&name, value);
        }
    }
    // A file is what it says it is: a browser never guesses another type
    // for it, whatever its bytes look like.
    response = response.header("x-content-type-options", "nosniff");
    match response.body(Body::from_stream(answer.bytes_stream())) {
        Ok(response) => response,
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("answer the stored file: {e}")).into_response(),
    }
}

/// The verbs of a page's upload through a slot.
// SYNC: UPLOAD_VERBS <-> packages/weft-connect/src/core/upload.ts UPLOAD_PATH, crates/weft-broker/src/runtime_storage.rs (router, /v1/storage/slot/*)
const UPLOAD_VERBS: [&str; 7] = ["check", "begin", "parts", "part-done", "complete", "resume", "abort"];

/// `POST /files/upload/{verb}`: one verb of a page's upload through a
/// slot, passed on to the broker, its answer passed back.
pub async fn upload(State(state): State<DispatcherState>, Path(verb): Path<String>, body: axum::body::Bytes) -> Response {
    if !UPLOAD_VERBS.contains(&verb.as_str()) {
        return (StatusCode::NOT_FOUND, format!("an upload's verbs are {}", UPLOAD_VERBS.join(", "))).into_response();
    }
    let sent = async {
        let mut request = state
            .broker
            .request(reqwest::Method::POST, &format!("/v1/storage/slot/{verb}"))
            .await?
            .header(header::CONTENT_TYPE, "application/json")
            .body(body);
        // Completing assembles the file from its parts, which grows with
        // its size; every other verb is a fixed amount of work.
        if verb != "complete" {
            request = request.timeout(crate::storage::BROKER_STORAGE_CALL_TIMEOUT);
        }
        request.send().await.map_err(anyhow::Error::from)
    }
    .await;
    let answer = match sent {
        Ok(answer) => answer,
        Err(e) => {
            tracing::error!(target: "weft_dispatcher::files_door", error = format!("{e:#}"), "the broker did not answer an upload");
            return (StatusCode::BAD_GATEWAY, "the install could not take the upload right now; try again shortly").into_response();
        }
    };
    let status = answer.status();
    let content_type = answer.headers().get(header::CONTENT_TYPE).cloned();
    let completing = answer.headers().get(weft_core::storage::COMPLETING_HEADER).cloned();
    let bytes = match answer.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => return (StatusCode::BAD_GATEWAY, format!("read the broker's answer: {e}")).into_response(),
    };
    let mut response = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response = response.header(header::CONTENT_TYPE, content_type);
    }
    if let Some(completing) = completing {
        response = response.header(weft_core::storage::COMPLETING_HEADER, completing);
    }
    response.body(Body::from(bytes)).unwrap_or_else(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> FilesTarget {
        FilesTarget { endpoint: "http://127.0.0.1:8333/".into(), bucket: "weft".into() }
    }

    /// A runtime file's path goes to its object in the bucket with its
    /// query (the signature) as it came; anything else goes nowhere.
    #[test]
    fn only_a_runtime_file_passes_and_its_query_rides_unchanged() {
        assert_eq!(
            target_url(&target(), "/files/runtime/t/project/p/f1", Some("X-Amz-Signature=ab%2Fc&x-id=GetObject")).unwrap(),
            "http://127.0.0.1:8333/weft/runtime/t/project/p/f1?X-Amz-Signature=ab%2Fc&x-id=GetObject"
        );
        for refused in ["/files/chunks/abc", "/files/runtime/../chunks/x", "/files/runtime//x", "/files", "/other/runtime/x"] {
            assert!(target_url(&target(), refused, None).is_err(), "{refused}");
        }
    }

    /// Every character a key segment may hold passes, in every scope's
    /// shape, and the path sent on is the key itself.
    #[test]
    fn a_key_with_every_allowed_character_passes_as_it_is() {
        let hash = "a".repeat(64);
        for key in [
            "Tenant-0_9.x/project/Proj-1_a.B/file-Z_z.9".to_string(),
            "t/run/r-1/f.bin".to_string(),
            "t/shared/team_a/f".to_string(),
            "t/instance/p/in.st-1/f".to_string(),
            format!("t/asset/{hash}"),
        ] {
            assert_eq!(
                target_url(&target(), &format!("/files/runtime/{key}"), None).unwrap(),
                format!("http://127.0.0.1:8333/weft/runtime/{key}")
            );
        }
        assert_eq!(
            target_url(&target(), "/files/runtime/t/project/p/f%2D1", None).unwrap(),
            "http://127.0.0.1:8333/weft/runtime/t/project/p/f-1",
            "an escaped character is sent on as the key spells it"
        );
    }

    /// A climb or a separator hidden behind an escape is refused like a
    /// plain one, and so is anything that is not a storage key: the url
    /// crate would collapse an escaped `..` once the address is parsed,
    /// reaching any key, or another plane of the bucket.
    #[test]
    fn an_escaped_climb_or_separator_goes_nowhere() {
        for refused in [
            "/files/runtime/t/project/p/%2e%2e/%2e%2e/%2e%2e/chunks/x",
            "/files/runtime/t/project/p/%2E%2E",
            "/files/runtime/t/project/%2E%2E/f",
            "/files/runtime/t/project/.%2e/f",
            "/files/runtime/t/project/%2e./f",
            "/files/runtime/t/project/%2e/f",
            "/files/runtime/t/project/p%2Ff",
            "/files/runtime/t/project/p%2ff",
            "/files/runtime/t%2Fproject/p/f",
            "/files/runtime/t/project/p/f/",
            "/files/runtime/t/project//f",
            "/files/runtime/t//p/f",
            "/files/runtime/",
            "/files/runtime/t/nowhere/p/f",
            "/files/runtime/t/project/p",
            "/files/runtime/t/project/p/f%20x",
            "/files/runtime/t/project/p/f%ff",
            "/files/runtimex/t/project/p/f",
            "/files/chunks/t/project/p/f",
            "/files/trees/t/project/p/f",
        ] {
            assert!(target_url(&target(), refused, None).is_err(), "{refused}");
        }
    }

    /// The host a site's pass-through names wins over the request's own,
    /// since the link was signed for the site.
    #[test]
    fn the_signed_host_is_the_one_the_caller_reached() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:14111"));
        assert_eq!(caller_host(&headers).unwrap(), "127.0.0.1:14111");
        headers.insert("x-forwarded-host", HeaderValue::from_static("shop.example"));
        assert_eq!(caller_host(&headers).unwrap(), "shop.example");
    }

    /// A chain of proxies lists several hosts; the first is where the
    /// caller was, the same hop every link base is read from.
    #[test]
    fn a_chained_forwarded_host_takes_the_first_hop() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:14111"));
        headers.insert("x-forwarded-host", HeaderValue::from_static("shop.example, 10.0.0.2:8080"));
        assert_eq!(caller_host(&headers).unwrap(), "shop.example");
        headers.insert("x-forwarded-host", HeaderValue::from_static("evil@x.example"));
        assert!(caller_host(&headers).is_none(), "a host that is not a bare authority names nothing");
    }

    /// What the fake store saw of one request.
    #[derive(Debug, Clone)]
    struct Seen {
        method: Method,
        uri: String,
        headers: HeaderMap,
        body: Vec<u8>,
    }

    /// A store that records what it is asked and answers a partial read
    /// with its own cross-origin header, the way SeaweedFS does.
    async fn fake_store() -> (FilesTarget, std::sync::Arc<std::sync::Mutex<Vec<Seen>>>) {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = seen.clone();
        let app = axum::Router::new().fallback(move |request: Request| {
            let record = record.clone();
            async move {
                let (parts, body) = request.into_parts();
                let body = axum::body::to_bytes(body, usize::MAX).await.unwrap().to_vec();
                record.lock().unwrap().push(Seen { method: parts.method, uri: parts.uri.to_string(), headers: parts.headers, body });
                Response::builder()
                    .status(StatusCode::PARTIAL_CONTENT)
                    .header("content-range", "bytes 0-1/10")
                    .header("accept-ranges", "bytes")
                    .header("etag", "\"e1\"")
                    .header("access-control-allow-origin", "*")
                    .header("server", "SeaweedFS S3")
                    .header("x-amz-request-id", "r1")
                    .header("x-content-type-options", "nosniff")
                    .body(Body::from("ab"))
                    .unwrap()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (FilesTarget { endpoint: format!("http://{addr}"), bucket: "weft".into() }, seen)
    }

    fn request(method: Method, uri: &str, headers: &[(&str, &str)], body: &'static [u8]) -> Request {
        let mut builder = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(Body::from(body)).unwrap()
    }

    /// A read reaches the store with its range and the caller's host as
    /// `X-Forwarded-Host`, and the store's partial answer comes back as it
    /// is, its own cross-origin header left behind and `nosniff` added.
    #[tokio::test]
    async fn a_read_passes_on_with_its_range_and_the_callers_host() {
        let (target, seen) = fake_store().await;
        let http = reqwest::Client::new();
        let answer = pass_on(
            &http,
            &target,
            request(Method::GET, "/files/runtime/t/project/p/f?X-Amz-Signature=s", &[("host", "t.example"), ("range", "bytes=0-1"), ("cookie", "c=1")], b""),
        )
        .await;
        assert_eq!(answer.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(answer.headers()["content-range"], "bytes 0-1/10");
        assert_eq!(answer.headers()["etag"], "\"e1\"");
        assert_eq!(answer.headers()["x-content-type-options"], "nosniff");
        assert_eq!(answer.headers()["accept-ranges"], "bytes");
        for kept in ["access-control-allow-origin", "server", "x-amz-request-id"] {
            assert!(answer.headers().get(kept).is_none(), "{kept}");
        }
        assert_eq!(answer.headers().get_all("x-content-type-options").iter().count(), 1, "the door's own, once");
        let seen = seen.lock().unwrap()[0].clone();
        assert_eq!((seen.method, seen.uri.as_str()), (Method::GET, "/weft/runtime/t/project/p/f?X-Amz-Signature=s"));
        assert_eq!(seen.headers["range"], "bytes=0-1");
        assert_eq!(seen.headers["x-forwarded-host"], "t.example");
        assert!(seen.headers.get("cookie").is_none(), "only what a read needs travels");
    }

    /// A part upload reaches the store with its exact length and body; a
    /// method no file takes never reaches it.
    #[tokio::test]
    async fn an_upload_passes_on_whole_and_other_methods_stop_here() {
        let (target, seen) = fake_store().await;
        let http = reqwest::Client::new();
        pass_on(
            &http,
            &target,
            request(Method::PUT, "/files/runtime/t/project/p/f?partNumber=1", &[("host", "127.0.0.1:14111"), ("x-forwarded-host", "shop.example"), ("content-length", "4")], b"abcd"),
        )
        .await;
        for method in [Method::POST, Method::DELETE, Method::PATCH] {
            let answer = pass_on(&http, &target, request(method.clone(), "/files/runtime/t/project/p/f", &[], b"")).await;
            assert_eq!(answer.status(), StatusCode::METHOD_NOT_ALLOWED, "{method}");
        }
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "only the upload reached the store");
        assert_eq!(seen[0].method, Method::PUT);
        assert_eq!(seen[0].body, b"abcd");
        assert_eq!(seen[0].headers["content-length"], "4");
        assert_eq!(seen[0].headers["x-forwarded-host"], "shop.example", "the site's host, which the link was signed for");
    }

    /// A part upload's `ETag` reaches the page, which completes the
    /// upload with it.
    #[tokio::test]
    async fn an_uploads_etag_comes_back() {
        let (target, _) = fake_store().await;
        let answer = pass_on(
            &reqwest::Client::new(),
            &target,
            request(Method::PUT, "/files/runtime/t/project/p/f?partNumber=1", &[("host", "t.example"), ("content-length", "2")], b"ab"),
        )
        .await;
        assert_eq!(answer.headers()["etag"], "\"e1\"");
    }

    /// A `PUT` that says no length, or more than any store takes in one
    /// piece, never reaches the store.
    #[tokio::test]
    async fn an_upload_without_a_bounded_length_stops_here() {
        let (target, seen) = fake_store().await;
        let http = reqwest::Client::new();
        let over = (MAX_PUT_BYTES + 1).to_string();
        for (headers, status) in [
            (vec![("host", "t.example")], StatusCode::LENGTH_REQUIRED),
            (vec![("host", "t.example"), ("content-length", over.as_str())], StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            let answer = pass_on(&http, &target, request(Method::PUT, "/files/runtime/t/project/p/f", &headers, b"")).await;
            assert_eq!(answer.status(), status);
        }
        assert!(seen.lock().unwrap().is_empty());
    }
}
