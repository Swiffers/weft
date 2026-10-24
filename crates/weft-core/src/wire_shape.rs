//! A call to the broker whose JSON parses but is not in the shape the
//! broker reads, and how the broker answers it.
//!
//! An update runs with every part of weft switched off, so the install's
//! roles always run one weft. A worker is the exception: its program image
//! is the one the weft of its project's last build built, until the
//! project is resynced. When an update changed a message a worker sends
//! the broker, its body still parses as JSON but no longer as the type the
//! broker takes. The broker reads every call one way, [`Json`] (and
//! [`Query`] for a query string), and answers one of another shape one
//! way: [`UNREADABLE_SHAPE`], with a body naming the fix
//! ([`unreadable_shape`]), which the worker's client turns into
//! `weft_broker_client::WorkerOutdated`. The broker also says it in its own
//! log, at most once every [`SAY_EVERY`] for each sender ([`OnceAWhile`]):
//! a busy route of an outdated worker is refused on every call, and one
//! line now and then says it all.

use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use lru::LruCache;
use serde::de::DeserializeOwned;

/// The status a call whose body is JSON but not in the shape its reader
/// reads is answered (a field of another type, one missing, a variant it
/// does not know). `426 Upgrade Required` says what is true: one side has
/// to be brought up to date before this call can work, and no other
/// answer of weft's own parts uses it, so the status alone tells the
/// sender which answer this is.
// SYNC: UNREADABLE_SHAPE <-> crates/weft-broker/src/wire.rs (the broker's answer), crates/weft-broker-client/src/client.rs (WorkerOutdated), crates/weft-engine/src/storage.rs (status_err)
pub const UNREADABLE_SHAPE: http::StatusCode = http::StatusCode::UPGRADE_REQUIRED;

/// The body of the broker's [`UNREADABLE_SHAPE`] answer: what happened,
/// the fix, and what could not be read (`why`, serde's own words).
pub fn unreadable_shape(why: &str) -> String {
    format!(
        "this request is in a shape this weft does not read, so it most likely comes from a worker an older weft built: \
         run `weft resync` on its project to rebuild it ({why})"
    )
}

/// Why a body did not read.
#[derive(Debug)]
pub enum Unreadable {
    /// It is not JSON at all (cut short, not text): no weft sends that,
    /// so it is the caller's own mistake, answered `400`.
    NotJson(serde_json::Error),
    /// JSON of another shape: [`UNREADABLE_SHAPE`].
    OtherShape(serde_json::Error),
}

/// `bytes` read as a `T`, or why not.
pub fn read_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Unreadable> {
    serde_json::from_slice(bytes).map_err(|e| match e.classify() {
        serde_json::error::Category::Data => Unreadable::OtherShape(e),
        _ => Unreadable::NotJson(e),
    })
}

/// How often a log says that one sender's calls do not read.
pub const SAY_EVERY: Duration = Duration::from_secs(600);

/// How many senders a [`OnceAWhile`] remembers having said. Bounded, so
/// a flood of senders cannot grow its memory; one forgotten is said again.
const KEPT: usize = 1024;

/// When a log last said a thing for each key (a sender, a project),
/// so it says it at most once every while. Only a thing to say touches
/// it: a call that went right never does.
pub struct OnceAWhile<K: Hash + Eq> {
    every: Duration,
    said: Mutex<LruCache<K, Instant>>,
}

impl<K: Hash + Eq> OnceAWhile<K> {
    /// Says each key's thing at most once every `every`.
    pub fn new(every: Duration) -> Self {
        Self { every, said: Mutex::new(LruCache::new(NonZeroUsize::new(KEPT).expect("non-zero"))) }
    }

    /// Whether to say it for `key` now, marking it said.
    pub fn now(&self, key: K) -> bool {
        self.at(key, Instant::now())
    }

    /// Whether to say it for `key` at `now`, marking it said.
    pub fn at(&self, key: K, now: Instant) -> bool {
        let mut said = self.said.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if said.get(&key).is_some_and(|at| now.duration_since(*at) < self.every) {
            return false;
        }
        said.put(key, now);
        true
    }
}

impl<K: Hash + Eq> Default for OnceAWhile<K> {
    /// Once every [`SAY_EVERY`].
    fn default() -> Self {
        Self::new(SAY_EVERY)
    }
}

/// What a server answers a call that did not read ([`Json`], [`Query`]):
/// it knows who may call it, and what its log says. Every refusal the
/// reader makes asks [`Self::caller`] first, so a caller the server does
/// not know only ever gets its identity refusal, and learns nothing of the
/// endpoint or its shapes (a handler that checks its caller itself, after
/// the body is read, relies on it).
#[cfg(feature = "server")]
pub trait AnswersUnreadable: Send + Sync {
    /// Who sent a call, as the server's log names them.
    type Caller: Send;

    /// Who sent a call carrying `headers`, or the identity refusal a
    /// caller the server does not know gets.
    fn caller(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> impl std::future::Future<Output = Result<Self::Caller, axum::response::Response>> + Send;

    /// The answer to `caller`'s call to `path` whose JSON is of another
    /// shape (`why`): [`UNREADABLE_SHAPE`].
    fn other_shape(&self, caller: Self::Caller, path: &str, why: &str) -> axum::response::Response;
}

/// A server shared behind an `Arc` (the router's state) answers as the
/// server does.
#[cfg(feature = "server")]
impl<T: AnswersUnreadable> AnswersUnreadable for std::sync::Arc<T> {
    type Caller = T::Caller;

    async fn caller(&self, headers: &axum::http::HeaderMap) -> Result<Self::Caller, axum::response::Response> {
        T::caller(self.as_ref(), headers).await
    }

    fn other_shape(&self, caller: Self::Caller, path: &str, why: &str) -> axum::response::Response {
        T::other_shape(self.as_ref(), caller, path, why)
    }
}

/// Why the shared reader refuses a call.
#[cfg(feature = "server")]
enum Refusal {
    /// The body could not be read at all (past the route's limit, cut
    /// off): axum's own answer.
    Body(axum::response::Response),
    /// The body is not JSON (`400`).
    NotJson(serde_json::Error),
    /// JSON of another shape: the server's [`AnswersUnreadable::other_shape`].
    OtherShape(String),
}

/// The answer to a call carrying `headers` to `path` the reader refuses
/// for `refusal`: the server's identity refusal for a caller it does not
/// know, whatever the refusal, and the refusal for one it does.
#[cfg(feature = "server")]
async fn refuse<S: AnswersUnreadable>(state: &S, headers: &axum::http::HeaderMap, path: &str, refusal: Refusal) -> axum::response::Response {
    use axum::response::IntoResponse;
    let caller = match state.caller(headers).await {
        Ok(caller) => caller,
        Err(stranger) => return stranger,
    };
    match refusal {
        Refusal::Body(answer) => answer,
        Refusal::NotJson(e) => (axum::http::StatusCode::BAD_REQUEST, format!("the body of {path} is not JSON: {e}")).into_response(),
        Refusal::OtherShape(why) => state.other_shape(caller, path, &why),
    }
}

/// A JSON body, both ways: the call a handler reads (a body of another
/// shape is answered [`AnswersUnreadable::other_shape`], one that is not
/// JSON `400`, each only to a caller the server knows), or the answer it
/// sends.
#[cfg(feature = "server")]
#[derive(Debug, Clone)]
pub struct Json<T>(pub T);

#[cfg(feature = "server")]
impl<S, T> axum::extract::FromRequest<S> for Json<T>
where
    S: AnswersUnreadable,
    T: DeserializeOwned,
{
    type Rejection = axum::response::Response;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        use axum::response::IntoResponse;
        // The headers are kept aside for a refusal, which checks who sent
        // the call; the body is read as `Bytes` reads it, under the
        // route's body limit.
        let (mut parts, body) = req.into_parts();
        let headers = std::mem::take(&mut parts.headers);
        let path = parts.uri.path().to_string();
        let refusal = match axum::body::Bytes::from_request(axum::extract::Request::from_parts(parts, body), state).await {
            Err(rejection) => Refusal::Body(rejection.into_response()),
            Ok(bytes) => match read_json(&bytes) {
                Ok(value) => return Ok(Json(value)),
                Err(Unreadable::NotJson(e)) => Refusal::NotJson(e),
                Err(Unreadable::OtherShape(e)) => Refusal::OtherShape(e.to_string()),
            },
        };
        Err(refuse(state, &headers, &path, refusal).await)
    }
}

#[cfg(feature = "server")]
impl<T: serde::Serialize> axum::response::IntoResponse for Json<T> {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self.0).into_response()
    }
}

/// A query string read as a `T`; one that does not read is answered
/// [`AnswersUnreadable::other_shape`], as a body of another shape is.
#[cfg(feature = "server")]
#[derive(Debug, Clone)]
pub struct Query<T>(pub T);

#[cfg(feature = "server")]
impl<S, T> axum::extract::FromRequestParts<S> for Query<T>
where
    S: AnswersUnreadable,
    T: DeserializeOwned,
{
    type Rejection = axum::response::Response;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, state: &S) -> Result<Self, Self::Rejection> {
        let why = match axum::extract::Query::<T>::try_from_uri(&parts.uri) {
            Ok(axum::extract::Query(value)) => return Ok(Query(value)),
            Err(rejection) => rejection.body_text(),
        };
        Err(refuse(state, &parts.headers, parts.uri.path(), Refusal::OtherShape(why)).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_body_of_another_shape_is_told_from_one_that_is_not_json() {
        assert!(matches!(read_json::<Vec<u32>>(br#"{"a":1}"#), Err(Unreadable::OtherShape(_))));
        assert!(matches!(read_json::<Vec<u32>>(br#"[1,"#), Err(Unreadable::NotJson(_))));
        assert!(matches!(read_json::<Vec<u32>>(b"not json"), Err(Unreadable::NotJson(_))));
        assert_eq!(read_json::<Vec<u32>>(b"[1,2]").unwrap(), vec![1, 2]);
    }

    #[test]
    fn the_answer_names_the_fix() {
        assert!(unreadable_shape("x").contains("`weft resync`"));
    }

    #[test]
    fn each_key_is_said_once_a_while() {
        let said = OnceAWhile::new(SAY_EVERY);
        let now = Instant::now();
        assert!(said.at(1, now));
        assert!(!said.at(1, now + Duration::from_secs(1)));
        assert!(said.at(2, now + Duration::from_secs(1)), "another key is said on its own");
        assert!(said.at(1, now + SAY_EVERY), "said again once the while is over");
    }
}

#[cfg(all(test, feature = "server"))]
mod server_tests {
    use axum::extract::FromRequest;
    use axum::response::IntoResponse;

    use super::*;

    /// Answers what does not read, naming the path, as a server would,
    /// to a caller carrying `x-known`; refuses any other `401`.
    struct Refuses;

    impl AnswersUnreadable for Refuses {
        type Caller = ();

        async fn caller(&self, headers: &axum::http::HeaderMap) -> Result<(), axum::response::Response> {
            match headers.contains_key("x-known") {
                true => Ok(()),
                false => Err((http::StatusCode::UNAUTHORIZED, "who are you").into_response()),
            }
        }

        fn other_shape(&self, (): (), path: &str, why: &str) -> axum::response::Response {
            (UNREADABLE_SHAPE, unreadable_shape(&format!("{path}: {why}"))).into_response()
        }
    }

    async fn read(body: &'static str) -> Result<Vec<u32>, (http::StatusCode, String)> {
        read_as(true, body).await
    }

    async fn read_as(known: bool, body: &'static str) -> Result<Vec<u32>, (http::StatusCode, String)> {
        let mut request = axum::http::Request::post("/v1/run/tag");
        if known {
            request = request.header("x-known", "1");
        }
        let request = request.body(axum::body::Body::from(body)).unwrap();
        match Json::<Vec<u32>>::from_request(request, &Refuses).await {
            Ok(Json(value)) => Ok(value),
            Err(answer) => {
                let status = answer.status();
                let body = axum::body::to_bytes(answer.into_body(), usize::MAX).await.unwrap();
                Err((status, String::from_utf8(body.to_vec()).unwrap()))
            }
        }
    }

    #[tokio::test]
    async fn a_body_of_another_shape_is_answered_by_its_server_and_one_that_is_not_json_is_a_400() {
        assert_eq!(read("[1]").await, Ok(vec![1]));
        let (status, body) = read(r#"{"a":1}"#).await.unwrap_err();
        assert_eq!(status, UNREADABLE_SHAPE);
        assert!(body.contains("/v1/run/tag: invalid type: map") && body.contains("`weft resync`"), "{body}");
        assert_eq!(read("[1,").await.unwrap_err().0, http::StatusCode::BAD_REQUEST);
    }

    /// A caller the server does not know gets its identity refusal for
    /// every body that does not read, JSON or not: it never learns the
    /// endpoint reads JSON, nor what shape.
    #[tokio::test]
    async fn a_stranger_only_ever_gets_the_identity_refusal() {
        for body in ["[1,", "not json", r#"{"a":1}"#] {
            assert_eq!(read_as(false, body).await, Err((http::StatusCode::UNAUTHORIZED, "who are you".into())), "{body}");
        }
        assert_eq!(read_as(false, "[1]").await, Ok(vec![1]), "a body that reads is the handler's to refuse");
    }
}
