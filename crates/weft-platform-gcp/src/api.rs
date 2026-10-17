//! Calling Google's REST APIs as the runtime's own service account.
//!
//! Every Google API weft drives (Cloud Run, Compute Engine, Cloud Build,
//! Cloud Tasks, Cloud Storage, IAM) is JSON over HTTPS with an access
//! token, and the slow ones answer with an operation to wait on. This is
//! that, once.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use serde_json::Value;

use crate::metadata::MetadataTokens;

/// How often an operation still running is looked at again.
const OPERATION_POLL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct Google {
    http: reqwest::Client,
    tokens: Arc<MetadataTokens>,
}

/// What an API answered when it answered with an error.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub body: String,
}

/// How much of an answer that is not Google's error shape (a proxy's
/// HTML page) an error shows.
const SHOWN_BODY: usize = 300;

/// An [`ApiError`] reads as one line, the way `weft status` and the alert
/// lines show it: the HTTP status, then Google's words ([`said`]) when the
/// body is Google's error shape, else the body itself on one line.
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error = serde_json::from_str::<Value>(&self.body).ok().and_then(|body| body.get("error").and_then(said));
        match error {
            Some(said) => write!(f, "Google answered {}: {said}", self.status),
            None => write!(f, "Google answered {}: {}", self.status, one_line(&self.body, SHOWN_BODY)),
        }
    }
}

/// What Google said in `error` (an API answer's `error`, or a failed
/// operation's) as one line: its status word, its message, then what its
/// details add ([`added_by`]). The message is a `google.rpc.Status`'s
/// `message`, or the messages of Compute Engine's `errors` list. `None`
/// for a value with no message, which is no such error.
fn said(error: &Value) -> Option<String> {
    let text = |v: &Value| v.get("message").and_then(Value::as_str).map(|m| one_line(m, usize::MAX)).filter(|m| !m.is_empty());
    let message = match text(error) {
        Some(message) => message,
        None => {
            let listed: Vec<String> = error.get("errors").and_then(Value::as_array).into_iter().flatten().filter_map(text).collect();
            if listed.is_empty() {
                return None;
            }
            listed.join("; ")
        }
    };
    let mut said = match error.get("status").and_then(Value::as_str) {
        Some(status) => format!("{status}: {message}"),
        None => message.clone(),
    };
    let added: Vec<String> = error.get("details").and_then(Value::as_array).into_iter().flatten().flat_map(|detail| added_by(detail, &message)).collect();
    if !added.is_empty() {
        said.push_str(&format!(" ({})", added.join("; ")));
    }
    Some(said)
}

/// What one of an error's `details` adds to its `message`: the permission
/// an `ErrorInfo` names, each field a `BadRequest` refuses with why, and
/// each violation of a `QuotaFailure` or a `PreconditionFailure`. Nothing
/// for a detail of any other type (a `Help`'s links, a `RequestInfo`'s
/// id).
fn added_by(detail: &Value, message: &str) -> Vec<String> {
    let kind = detail.get("@type").and_then(Value::as_str).and_then(|t| t.rsplit('.').next()).unwrap_or_default();
    let str_at = |v: &Value, key: &str| v.get(key).and_then(Value::as_str).map(|s| one_line(s, usize::MAX)).filter(|s| !s.is_empty());
    let each = |key: &str, say: &dyn Fn(&Value) -> Option<String>| -> Vec<String> {
        detail.get(key).and_then(Value::as_array).into_iter().flatten().filter_map(say).collect()
    };
    match kind {
        // A permission the message names is not said twice.
        "ErrorInfo" => detail
            .pointer("/metadata/permission")
            .and_then(Value::as_str)
            .filter(|p| !message.contains(p))
            .map(|p| format!("missing permission: {p}"))
            .into_iter()
            .collect(),
        "BadRequest" => each("fieldViolations", &|v| match (str_at(v, "field"), str_at(v, "description")) {
            (Some(field), Some(why)) => Some(format!("{field}: {why}")),
            (field, why) => field.or(why),
        }),
        "QuotaFailure" | "PreconditionFailure" => each("violations", &|v| str_at(v, "description").or_else(|| str_at(v, "subject"))),
        _ => Vec::new(),
    }
}

/// `text` with every run of whitespace one space, cut to `at_most`
/// characters.
fn one_line(text: &str, at_most: usize) -> String {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    match line.char_indices().nth(at_most) {
        Some((cut, _)) => format!("{}...", &line[..cut]),
        None => line,
    }
}

impl std::error::Error for ApiError {}

/// Whether `e` is Google answering `status`.
pub fn is_status(e: &anyhow::Error, status: u16) -> bool {
    e.chain().any(|c| c.downcast_ref::<ApiError>().is_some_and(|a| a.status == status))
}

impl Google {
    pub fn new(tokens: Arc<MetadataTokens>) -> Self {
        Self {
            http: reqwest::Client::builder().timeout(Duration::from_secs(60)).build().expect("reqwest client"),
            tokens,
        }
    }

    pub fn tokens(&self) -> &Arc<MetadataTokens> {
        &self.tokens
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Send `req` as this process's account, answering Google's response
    /// whatever its status: for a caller that reads an answer that is not
    /// JSON (Cloud Storage's objects and XML) or treats a status itself.
    pub async fn send_raw(&self, req: reqwest::RequestBuilder) -> anyhow::Result<reqwest::Response> {
        let token = self.tokens.access_token().await?;
        Ok(req.bearer_auth(token).send().await?)
    }

    /// `resp` when it is a success; its status and Google's words, as an
    /// [`ApiError`], otherwise.
    pub async fn success(resp: reqwest::Response) -> anyhow::Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status().as_u16();
        Err(ApiError { status, body: resp.text().await.unwrap_or_default() }.into())
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> anyhow::Result<Value> {
        let resp = Self::success(self.send_raw(req).await?).await?;
        let body = resp.text().await?;
        if body.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&body).with_context(|| format!("Google answered something that is not JSON: {body}"))
    }

    pub async fn get(&self, url: &str) -> anyhow::Result<Value> {
        self.send(self.http.get(url)).await.with_context(|| format!("GET {url}"))
    }

    /// `GET` with `query` as the URL's query string, encoded.
    pub async fn get_query(&self, url: &str, query: &[(&str, String)]) -> anyhow::Result<Value> {
        self.send(self.http.get(url).query(query)).await.with_context(|| format!("GET {url}"))
    }

    /// `GET`, with a 404 read as "not there".
    pub async fn get_opt(&self, url: &str) -> anyhow::Result<Option<Value>> {
        match self.get(url).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if is_status(&e, 404) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn post(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.post(url).json(body)).await.with_context(|| format!("POST {url}"))
    }

    pub async fn put(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.put(url).json(body)).await.with_context(|| format!("PUT {url}"))
    }

    pub async fn patch(&self, url: &str, body: &Value) -> anyhow::Result<Value> {
        self.send(self.http.patch(url).json(body)).await.with_context(|| format!("PATCH {url}"))
    }

    pub async fn put_bytes(&self, url: &str, content_type: &str, body: Vec<u8>) -> anyhow::Result<Value> {
        self.send(self.http.post(url).header("content-type", content_type).body(body))
            .await
            .with_context(|| format!("upload to {url}"))
    }

    /// `DELETE`, with a 404 read as "already gone".
    pub async fn delete(&self, url: &str) -> anyhow::Result<Option<Value>> {
        match self.send(self.http.delete(url)).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if is_status(&e, 404) => Ok(None),
            Err(e) => Err(e.context(format!("DELETE {url}"))),
        }
    }

    /// Wait for a long-running operation to end, failing with its error.
    /// `op` is what the call that started it answered; `base` is the
    /// API's root the operation's name is under.
    pub async fn wait(&self, base: &str, op: Value) -> anyhow::Result<Value> {
        let mut op = op;
        loop {
            let done = op.get("done").and_then(Value::as_bool).unwrap_or(false)
                || op.get("status").and_then(Value::as_str) == Some("DONE");
            if done {
                if let Some(err) = op.get("error").filter(|e| !e.is_null()) {
                    match said(err) {
                        Some(said) => anyhow::bail!("the operation failed: {said}"),
                        None => anyhow::bail!("the operation failed: {err}"),
                    }
                }
                return Ok(op);
            }
            let url = match (op.get("selfLink").and_then(Value::as_str), op.get("name").and_then(Value::as_str)) {
                (Some(link), _) => link.to_string(),
                (None, Some(name)) => format!("{}/{name}", base.trim_end_matches('/')),
                (None, None) => anyhow::bail!("an operation with no name to follow: {op}"),
            };
            tokio::time::sleep(OPERATION_POLL).await;
            op = self.get(&url).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cloud Run refusing to read a worker pool, the way Google answers a
    /// permission not granted yet: the permission is in the message, and
    /// again in the details.
    const RUN_DENIED: &str = r#"{
      "error": {
        "code": 403,
        "message": "Permission 'run.workerpools.get' denied on resource 'projects/acme/locations/europe-west1/workerPools/weft-holder' (or resource may not exist).",
        "status": "PERMISSION_DENIED",
        "details": [
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "IAM_PERMISSION_DENIED",
            "domain": "iam.googleapis.com",
            "metadata": { "permission": "run.workerpools.get", "resource": "projects/acme/locations/europe-west1/workerPools/weft-holder" }
          }
        ]
      }
    }"#;

    /// Cloud Tasks refusing to make a task, its message naming no
    /// permission and its details naming it.
    const TASKS_DENIED: &str = r#"{
      "error": {
        "code": 403,
        "message": "The principal (user or service account) lacks IAM permission for the resource \"projects/acme/locations/europe-west1/queues/weft-ticks\" (or the resource may not exist).",
        "status": "PERMISSION_DENIED",
        "details": [
          { "@type": "type.googleapis.com/google.rpc.Help", "links": [{ "url": "https://cloud.google.com/tasks/docs" }] },
          {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "IAM_PERMISSION_DENIED",
            "domain": "cloudtasks.googleapis.com",
            "metadata": { "permission": "cloudtasks.tasks.create" }
          }
        ]
      }
    }"#;

    fn shown(status: u16, body: &str) -> String {
        ApiError { status, body: body.into() }.to_string()
    }

    #[test]
    fn a_google_refusal_reads_as_one_line_of_its_words() {
        assert_eq!(
            shown(403, RUN_DENIED),
            "Google answered 403: PERMISSION_DENIED: Permission 'run.workerpools.get' denied on resource \
             'projects/acme/locations/europe-west1/workerPools/weft-holder' (or resource may not exist).",
            "the permission is said once"
        );
        assert_eq!(
            shown(403, TASKS_DENIED),
            "Google answered 403: PERMISSION_DENIED: The principal (user or service account) lacks IAM permission for the resource \
             \"projects/acme/locations/europe-west1/queues/weft-ticks\" (or the resource may not exist). \
             (missing permission: cloudtasks.tasks.create)",
            "a permission only the details name is added"
        );
    }

    /// A refusal whose message is generic says which field was wrong, and
    /// why, only in its details.
    #[test]
    fn a_bad_request_says_each_field_it_refused() {
        let body = r#"{"error": {"code": 400, "message": "Request contains an invalid argument.", "status": "INVALID_ARGUMENT", "details": [
            { "@type": "type.googleapis.com/google.rpc.BadRequest", "fieldViolations": [
                { "field": "spec.template.spec.containers[0].resources.limits.memory", "description": "Invalid value specified for memory." },
                { "description": "Only one container may be set." }
            ] },
            { "@type": "type.googleapis.com/google.rpc.QuotaFailure", "violations": [{ "subject": "project:acme", "description": "CPU quota exceeded in europe-west1." }] }
        ]}}"#;
        assert_eq!(
            shown(400, body),
            "Google answered 400: INVALID_ARGUMENT: Request contains an invalid argument. \
             (spec.template.spec.containers[0].resources.limits.memory: Invalid value specified for memory.; \
             Only one container may be set.; CPU quota exceeded in europe-west1.)"
        );
    }

    #[test]
    fn an_answer_that_is_not_googles_error_shape_reads_as_its_body_on_one_line() {
        assert_eq!(shown(502, "<html>\n  <body>Bad   gateway</body>\n</html>\n"), "Google answered 502: <html> <body>Bad gateway</body> </html>");
        assert_eq!(shown(404, ""), "Google answered 404: ");
        assert_eq!(shown(400, r#"{"error": {"code": 400}}"#), r#"Google answered 400: {"error": {"code": 400}}"#, "no message: the body as it is");
        assert_eq!(
            shown(404, r#"{"error": {"code": 404, "message": "  ", "status": "NOT_FOUND"}}"#),
            r#"Google answered 404: {"error": {"code": 404, "message": " ", "status": "NOT_FOUND"}}"#,
            "an empty message is no message"
        );
        let long = "x".repeat(SHOWN_BODY + 50);
        assert_eq!(shown(500, &long), format!("Google answered 500: {}...", "x".repeat(SHOWN_BODY)));
    }

    /// A failed operation's error is the same shape, usually with no
    /// status word.
    #[test]
    fn a_failed_operations_error_reads_as_its_message() {
        let error = serde_json::json!({ "code": 7, "message": "Permission denied on secret: projects/acme/secrets/s/versions/latest" });
        assert_eq!(said(&error).unwrap(), "Permission denied on secret: projects/acme/secrets/s/versions/latest");
        assert_eq!(said(&serde_json::json!({ "code": 13 })), None);
        assert_eq!(said(&serde_json::json!({ "code": 13, "message": "", "errors": [{ "message": "disk full" }] })).unwrap(), "disk full", "an empty message reads the list");
        let compute = serde_json::json!({ "errors": [
            { "code": "QUOTA_EXCEEDED", "message": "Quota 'GPUS_ALL_REGIONS' exceeded.  Limit: 0.0 globally." },
            { "code": "ZONE_RESOURCE_POOL_EXHAUSTED", "message": "The zone does not have enough resources." }
        ] });
        assert_eq!(
            said(&compute).unwrap(),
            "Quota 'GPUS_ALL_REGIONS' exceeded. Limit: 0.0 globally.; The zone does not have enough resources.",
            "a Compute Engine operation's errors, each message in turn"
        );
    }
}
