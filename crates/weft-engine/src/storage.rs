//! Worker-side storage data path: the worker's client of the broker's
//! runtime-file plane (`ctx.storage`).
//!
//! The worker holds NO bucket credentials. Every CONTROL call is an authenticated
//! HTTP call to the broker (the same broker the worker already uses for the journal
//! / tasks): the broker verifies the worker's identity, runs the key wall, enforces
//! quota, mints keys, and records metadata. But BYTES never flow through the broker.
//!
//! A put is MULTIPART, driven entirely in here (node code just calls `ctx.put`):
//! `upload/begin` mints the key (charging a known total size against the quota up
//! front), then the stream is sliced into parts; each part is reserved via
//! `upload/parts` (which returns a URL signed with the part's EXACT size; a stream
//! is quota-charged per part here), PUT directly to the bucket (with a bounded
//! retry that re-presigns via `upload/resume` on a dropped connection or expired
//! URL), and its etag reported via `upload/part-done`; `upload/complete` assembles
//! the file. Any unrecoverable failure aborts the upload (`upload/abort`, freeing
//! the quota reservation) and surfaces loud. A get is simpler: `download-url`
//! returns the metadata + a presigned GET URL, and the worker reads the bytes
//! DIRECTLY from the bucket. Presigned URLs are signed for the worker's internal
//! endpoint. A copy is the broker's own (`copy`): the bucket copies the object,
//! so nothing passes through the worker.
//!
//! There is no "ensure the storage exists" handshake and no retry-on-
//! unreachable dance: the broker is a long-lived install service, always up
//! like the journal, so an unreachable broker is a real failure surfaced to
//! the node, not a transient to paper over.

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use serde_json::Value;

use weft_core::storage::{
    ByteRange, ByteStream, DownloadUrlResponse, KeepTtl, LinkAnswer, LinkReach, ListFilesResponse, PresignedPart,
    StorageScope, StoredFile, StoredFileMeta, UploadBeginResponse, UploadPartsResponse, UploadResumeResponse,
};
use weft_core::error::{WeftError, WeftResult};
use weft_core::weft_type::WeftType;
use weft_core::RunId;

/// The run claim header the worker stamps on every storage call.
// SYNC: HDR_RUN_ID <-> crates/weft-broker/src/runtime_storage.rs (HDR_RUN_ID)
const HDR_RUN_ID: &str = "x-weft-run-id";

/// Run-parameterized storage surface the `ContextHandle` storage methods
/// delegate to. One impl per worker process; Layer-3 tests inject a fake.
#[async_trait]
pub trait WorkerStorageOps: Send + Sync {
    /// `identity` names what the file is a copy of; a put whose
    /// identity is already stored in `scope` answers the existing file
    /// and moves no bytes (see `StorageHandle::identified`).
    async fn put(
        &self,
        run_id: RunId,
        scope: &StorageScope,
        identity: Option<&str>,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<Value>;
    /// One attempt at overwriting the stored file at `key` with `data`
    /// (same key, scope, name, type and lifetime); see
    /// `ContextHandle::storage_replace` for the outcomes.
    async fn replace(
        &self,
        run_id: RunId,
        key: &str,
        expected_version: Option<u64>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<weft_core::storage::ReplaceOutcome>;
    async fn get(
        &self,
        run_id: RunId,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)>;
    async fn delete(&self, run_id: RunId, key: &str) -> WeftResult<()>;
    async fn list(&self, run_id: RunId, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>>;
    /// The file already stored under `identity` in `scope`, if any.
    async fn find(&self, run_id: RunId, scope: &StorageScope, identity: &str) -> WeftResult<Option<StoredFileMeta>>;
    async fn keep(&self, run_id: RunId, key: &str, ttl: KeepTtl) -> WeftResult<()>;
    /// A link to `key` for `reach`, living `ttl_secs` (`None`: the
    /// default), with the file's record.
    async fn link(&self, run_id: RunId, key: &str, ttl_secs: Option<u64>, reach: LinkReach) -> WeftResult<LinkAnswer>;
    /// A copy of `key` into `scope`, made inside the store.
    async fn copy(
        &self,
        run_id: RunId,
        key: &str,
        scope: &StorageScope,
        identity: Option<&str>,
        keep: Option<KeepTtl>,
    ) -> WeftResult<Value>;
    /// A slot a page uploads one file through.
    async fn upload_slot(&self, run_id: RunId, request: &weft_core::storage::slot::UploadSlotRequest) -> WeftResult<String>;
    /// The files uploaded through each claim's slot, received by `run_id`
    /// each for a place of its type, in the claims' order: all of them or
    /// none.
    async fn claim_uploads(&self, run_id: RunId, claims: &[(&str, &WeftType)]) -> WeftResult<Vec<Value>>;
}

/// Map a broker HTTP failure to a node-facing error. A transport failure
/// (broker unreachable) is a real NodeFiring error: the broker is a core
/// service, so its absence is a failure to surface, not a transient to retry
/// silently around.
fn http_err(context: &str, e: reqwest::Error) -> WeftError {
    WeftError::NodeFailed(format!("storage: {context}: {e}"))
}

async fn status_err(context: &str, resp: reqwest::Response) -> WeftError {
    let status = resp.status();
    let body = match body_text(context, resp).await {
        Ok(body) => body,
        Err(e) => return e,
    };
    if status == weft_broker_client::protocol::UNREADABLE_SHAPE {
        return WeftError::NodeFailed(weft_broker_client::WorkerOutdated { path: context.to_string(), said: body }.to_string());
    }
    match status.as_u16() {
        404 => WeftError::NodeFailed(format!("storage file not found: {body}")),
        401 | 403 => WeftError::NodeFailed(format!("storage denied: {body}")),
        413 => WeftError::NodeFailed(format!("storage quota exceeded: {body}")),
        _ => WeftError::NodeFailed(format!("storage: {context}: {status}: {body}")),
    }
}

/// The body of a broker answer. A body that cannot be read is a failure
/// of its own, never an empty message.
async fn body_text(context: &str, resp: reqwest::Response) -> WeftResult<String> {
    let status = resp.status();
    resp.text()
        .await
        .map_err(|e| WeftError::NodeFailed(format!("storage: {context}: {status}, and its body could not be read: {e}")))
}

/// The statuses a claim answers when the slot is no good for this call
/// (malformed, unknown, already received by another run, ended, too big,
/// or not of the type asked). Every other 4xx (the worker's own
/// credentials refused, a timeout, a rate limit) is not the caller's to
/// fix.
fn is_slot_refusal(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 400 | 404 | 409 | 410 | 413 | 422)
}

/// How many times a completion is asked before giving up: with the 1s
/// doubling backoff capped at 30s, about four minutes, near the broker's
/// lease after which its sweep drives the completion itself.
const COMPLETE_ATTEMPTS: u32 = 12;

/// What one answer to `upload/complete` means.
#[derive(Debug, PartialEq, Eq)]
enum CompleteStep {
    /// The file: done.
    Landed,
    /// Ask again: still landing (409 with `x-weft-completing`), or the
    /// broker failed without saying (5xx).
    Retry,
    /// The broker gave its final verdict and ended the upload (410).
    Ended,
    /// Refused before any completion was claimed (an incomplete upload,
    /// any other 4xx): the upload is aborted.
    Refused,
}

// SYNC: the marker <-> crates/weft-broker/src/runtime_storage.rs complete_response
// SYNC: completion backoff <-> crates/weft-cli/src/commands/assets.rs completing_wait
fn complete_step(status: reqwest::StatusCode, completing: bool) -> CompleteStep {
    if status.is_success() {
        CompleteStep::Landed
    } else if (status == reqwest::StatusCode::CONFLICT && completing) || status.is_server_error() {
        CompleteStep::Retry
    } else if status == reqwest::StatusCode::GONE {
        CompleteStep::Ended
    } else {
        CompleteStep::Refused
    }
}

/// The worker's broker runtime-file client.
pub struct WorkerStorage {
    broker_url: String,
    token: weft_broker_client::TokenSource,
    http: reqwest::Client,
}

// The request/response envelopes (the upload/* set, ListFilesResponse,
// PresignResponse, DownloadUrlResponse) are the shared worker<->broker
// contract; they live in `weft_core::storage` so this client and the broker
// share one definition.

impl WorkerStorage {
    /// `broker_url` is the broker the worker already talks to; `token` is
    /// the worker's identity for it (the broker resolves the tenant and
    /// project from it).
    pub fn new(broker_url: String, token: weft_broker_client::TokenSource) -> Arc<Self> {
        Arc::new(Self { broker_url, token, http: reqwest::Client::new() })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.broker_url.trim_end_matches('/'), path)
    }

    /// A request builder with the worker's identity + the run claim
    /// header.
    async fn authed(
        &self,
        req: reqwest::RequestBuilder,
        run_id: RunId,
    ) -> WeftResult<reqwest::RequestBuilder> {
        let bearer = self
            .token
            .read(self.broker_url.trim_end_matches('/'))
            .await
            .map_err(|e| WeftError::NodeFailed(format!("get an identity token for the broker: {e:#}")))?;
        let mut req = req.bearer_auth(bearer).header(HDR_RUN_ID, run_id.to_string());
        for (name, value) in self.token.headers() {
            req = req.header(name, value);
        }
        Ok(req)
    }

    /// GET a broker storage endpoint and deserialize the JSON response.
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        run_id: RunId,
        what: &str,
    ) -> WeftResult<T> {
        let resp = self
            .authed(self.http.get(self.url(path)), run_id)
            .await?
            .send()
            .await
            .map_err(|e| http_err(what, e))?;
        if !resp.status().is_success() {
            return Err(status_err(what, resp).await);
        }
        resp.json().await.map_err(|e| http_err(what, e))
    }

    /// POST a JSON body to a broker storage endpoint and deserialize the JSON
    /// response. `what` names the op for error context.
    async fn post_json<B: serde::Serialize + ?Sized, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        run_id: RunId,
        body: &B,
        what: &str,
    ) -> WeftResult<T> {
        let resp = self
            .authed(self.http.post(self.url(path)), run_id)
            .await?
            .json(body)
            .send()
            .await
            .map_err(|e| http_err(what, e))?;
        if !resp.status().is_success() {
            return Err(status_err(what, resp).await);
        }
        resp.json().await.map_err(|e| http_err(what, e))
    }

    /// POST a JSON body to a broker storage endpoint that answers with no
    /// content (part-done / abort).
    async fn post_no_content<B: serde::Serialize + ?Sized>(
        &self,
        path: &str,
        run_id: RunId,
        body: &B,
        what: &str,
    ) -> WeftResult<()> {
        let resp = self
            .authed(self.http.post(self.url(path)), run_id)
            .await?
            .json(body)
            .send()
            .await
            .map_err(|e| http_err(what, e))?;
        if !resp.status().is_success() {
            return Err(status_err(what, resp).await);
        }
        Ok(())
    }

    /// Slice the stream into parts and upload them for `key`: reserve each
    /// part (exact size), PUT it to its signed URL, report its etag.
    /// Bounded memory: at most one part is buffered.
    async fn upload_parts(
        &self,
        run_id: RunId,
        key: &str,
        part_size: u64,
        mut data: ByteStream,
    ) -> WeftResult<()> {
        use futures::StreamExt;
        let part_size = part_size as usize;
        let mut buf: Vec<u8> = Vec::new();
        // Parts are NAMED by their position, from 1, and this loop is the
        // only writer of this key's stream, so the part it is on is the part
        // it says it is on.
        let mut part_number: i32 = 1;
        while let Some(chunk) = data.next().await {
            let chunk = chunk.map_err(|e| {
                WeftError::NodeFailed(format!("storage: reading upload stream: {e}"))
            })?;
            buf.extend_from_slice(&chunk);
            while buf.len() >= part_size {
                let rest = buf.split_off(part_size);
                let part = bytes::Bytes::from(std::mem::replace(&mut buf, rest));
                self.upload_one_part(run_id, key, part_number, part).await?;
                part_number += 1;
            }
        }
        // The final short part, only if there are leftover bytes. A stream that
        // ended exactly on a part boundary flushes nothing; a stream that
        // produced NO bytes uploads ZERO parts (an empty object is not a
        // multipart part, which S3 cannot represent; `complete` writes it as a
        // plain empty object).
        if !buf.is_empty() {
            self.upload_one_part(run_id, key, part_number, bytes::Bytes::from(buf)).await?;
        }
        Ok(())
    }

    /// Upload the parts of the upload begun under `key`, then complete it.
    ///
    /// A failure while uploading parts aborts the upload, so its quota
    /// reservation is freed (idempotent: a quota rejection already aborted
    /// broker-side). Completion is different: once the broker claims it,
    /// the bytes may be landing and an abort is refused, so completion is
    /// retried until it answers for good ([`Self::complete_until_landed`]).
    async fn drive_or_abort(
        &self,
        run_id: RunId,
        key: &str,
        part_size: u64,
        data: ByteStream,
    ) -> WeftResult<StoredFile> {
        if let Err(e) = self.upload_parts(run_id, key, part_size, data).await {
            self.abort_upload(run_id, key).await;
            return Err(e);
        }
        let value = self.complete_until_landed(run_id, key).await?;
        StoredFile::from_value(&value).map_err(|e| {
            WeftError::NodeFailed(format!("storage: upload '{key}' completed but its answer is not a file: {e}"))
        })
    }

    async fn abort_upload(&self, run_id: RunId, key: &str) {
        if let Err(abort) = self
            .post_no_content(
                "/v1/storage/upload/abort",
                run_id,
                &weft_core::storage::UploadAbortRequest { key: key.to_string() },
                "abort upload",
            )
            .await
        {
            tracing::warn!(
                target: "weft_engine::storage",
                key = %key, error = %abort,
                "failed to abort interrupted upload; the broker sweep will reap it"
            );
        }
    }

    /// Ask the broker to complete `key` until it answers for good. Complete
    /// is idempotent, so a lost answer, a broker error, or the
    /// `x-weft-completing` "still landing" answer is asked again with
    /// backoff. A refusal before the broker claimed the completion (an
    /// incomplete upload) aborts it; a final verdict (410, the broker ended
    /// the upload) is reported as it is. An internal wait, so it is
    /// bounded: past it, the failure names the key and says the broker's
    /// sweep finishes or ends it.
    async fn complete_until_landed(&self, run_id: RunId, key: &str) -> WeftResult<Value> {
        let request = weft_core::storage::UploadCompleteRequest { key: key.to_string() };
        let mut delay = std::time::Duration::from_secs(1);
        let mut last = String::new();
        for _ in 0..COMPLETE_ATTEMPTS {
            let sent = async {
                self.authed(self.http.post(self.url("/v1/storage/upload/complete")), run_id)
                    .await?
                    .json(&request)
                    .send()
                    .await
                    .map_err(|e| http_err("upload complete", e))
            }
            .await;
            match sent {
                Err(e) => last = e.to_string(),
                Ok(resp) => match complete_step(resp.status(), resp.headers().contains_key(weft_core::storage::COMPLETING_HEADER)) {
                    CompleteStep::Landed => {
                        return resp.json().await.map_err(|e| http_err("upload complete", e));
                    }
                    CompleteStep::Retry => {
                        let status = resp.status();
                        last = match body_text("upload complete", resp).await {
                            Ok(body) => format!("{status}: {body}"),
                            Err(e) => e.to_string(),
                        };
                    }
                    CompleteStep::Ended => {
                        let e = status_err("upload complete", resp).await;
                        return Err(WeftError::NodeFailed(format!("storage: upload '{key}' was ended by the broker: {e}")));
                    }
                    CompleteStep::Refused => {
                        let e = status_err("upload complete", resp).await;
                        self.abort_upload(run_id, key).await;
                        return Err(e);
                    }
                },
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(std::time::Duration::from_secs(30));
        }
        Err(WeftError::NodeFailed(format!(
            "storage: upload '{key}' did not finish completing ({last}); the broker's sweep \
             finishes it or ends it, so look for the file again later"
        )))
    }

    /// Reserve one part (its URL comes back signed to exactly this size),
    /// PUT it, and record its etag.
    async fn upload_one_part(
        &self,
        run_id: RunId,
        key: &str,
        part_number: i32,
        bytes: bytes::Bytes,
    ) -> WeftResult<()> {
        let UploadPartsResponse { parts } = self
            .post_json(
                "/v1/storage/upload/parts",
                run_id,
                &weft_core::storage::UploadPartsRequest {
                    key: key.to_string(),
                    parts: vec![weft_core::storage::PartAsk {
                        part_number,
                        size_bytes: bytes.len() as u64,
                    }],
                },
                "reserve upload part",
            )
            .await?;
        let part = parts.into_iter().next().ok_or_else(|| {
            WeftError::NodeFailed("storage: part reservation returned no part".into())
        })?;
        self.put_part(run_id, key, part, bytes).await
    }

    /// PUT one reserved part to its signed URL with a bounded retry: a failed
    /// attempt (dropped connection, expired URL) gets a FRESH URL for exactly
    /// this part via `resume` and tries again. On success the etag (verbatim
    /// response header) is reported to the broker. Retries re-send the
    /// worker's own buffered bytes; nothing external is re-consumed.
    async fn put_part(
        &self,
        run_id: RunId,
        key: &str,
        mut part: PresignedPart,
        bytes: bytes::Bytes,
    ) -> WeftResult<()> {
        const ATTEMPTS: u32 = 3;
        let mut last_err = String::new();
        for attempt in 1..=ATTEMPTS {
            if attempt > 1 {
                let UploadResumeResponse { missing, .. } = self
                    .post_json(
                        "/v1/storage/upload/resume",
                        run_id,
                        &weft_core::storage::UploadResumeRequest { key: key.to_string() },
                        "resume upload",
                    )
                    .await?;
                match missing.into_iter().find(|p| p.part_number == part.part_number) {
                    Some(fresh) => part = fresh,
                    // Not missing anymore: a previous attempt landed AND was
                    // recorded (the done-report raced its own error path).
                    None => return Ok(()),
                }
            }
            match self.try_put_part(&part, &bytes).await {
                Ok(etag) => {
                    self.post_no_content(
                        "/v1/storage/upload/part-done",
                        run_id,
                        &weft_core::storage::PartDoneRequest {
                            key: key.to_string(),
                            part_number: part.part_number,
                            etag,
                        },
                        "record uploaded part",
                    )
                    .await?;
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        target: "weft_engine::storage",
                        key = %key, part = part.part_number, attempt, error = %e,
                        "part upload attempt failed"
                    );
                    last_err = e;
                }
            }
        }
        Err(WeftError::NodeFailed(format!(
            "storage: upload interrupted: part {} failed after {ATTEMPTS} attempts: {last_err}",
            part.part_number
        )))
    }

    /// One raw PUT of a part's bytes to its presigned URL. Returns the etag
    /// response header verbatim (quotes included). String error so the caller
    /// can fold attempts into one loud message.
    async fn try_put_part(&self, part: &PresignedPart, bytes: &bytes::Bytes) -> Result<String, String> {
        let resp = self
            .http
            .put(&part.url)
            .body(bytes.clone())
            .send()
            .await
            .map_err(|e| format!("PUT part to bucket: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.map_err(|e| format!("bucket rejected part: {status}, and its body could not be read: {e}"))?;
            return Err(format!("bucket rejected part: {status}: {body}"));
        }
        resp.headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| "bucket part response carried no etag".to_string())
    }
}

#[async_trait]
impl WorkerStorageOps for WorkerStorage {
    async fn put(
        &self,
        run_id: RunId,
        scope: &StorageScope,
        identity: Option<&str>,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<Value> {
        // Begin: the broker mints the key, charges a declared size against the
        // quota up front, and opens the multipart upload. The broker never
        // sees the bytes.
        // `resume` is not read here: it can only come back for a
        // CONTENT-ADDRESSED key (an asset, where two callers can hold the
        // same bytes), and a node's upload is run-scoped under a
        // freshly minted id, so nothing can already be part way up under it.
        let UploadBeginResponse { key, part_size, already_stored, resume: _ } = self
            .post_json(
                "/v1/storage/upload/begin",
                run_id,
                &weft_core::storage::UploadBeginRequest {
                    scope: scope.clone(),
                    mime_type: mime_type.to_string(),
                    filename: filename.to_string(),
                    keep,
                    declared_size,
                    identity: identity.map(str::to_string),
                },
                "upload begin",
            )
            .await?;
        // An identified put whose identity the scope already holds:
        // the broker answered the file it has, nothing to upload. The
        // stream is dropped unread (a URL fetch never pulls its body).
        if already_stored {
            let meta: StoredFileMeta = self.get_json(&format!("/v1/storage/meta/{key}"), run_id, "meta").await?;
            return Ok(StoredFile::from(&meta).to_value());
        }
        Ok(self.drive_or_abort(run_id, &key, part_size, data).await?.to_value())
    }

    async fn replace(
        &self,
        run_id: RunId,
        key: &str,
        expected_version: Option<u64>,
        declared_size: Option<u64>,
        data: ByteStream,
    ) -> WeftResult<weft_core::storage::ReplaceOutcome> {
        // The replacement is an ordinary upload under a key of its own,
        // written onto the replaced file's object; complete answers the
        // replaced file's value. The begin is where the broker refuses a
        // file another write holds (409) or that moved on from the
        // version this content was made from (412), before any byte
        // moves.
        let resp = self
            .authed(self.http.post(self.url("/v1/storage/upload/replace")), run_id)
            .await?
            .json(&weft_core::storage::UploadReplaceRequest {
                key: key.to_string(),
                declared_size,
                expected_version,
            })
            .send()
            .await
            .map_err(|e| http_err("replace begin", e))?;
        // SYNC: replace outcome statuses <-> crates/weft-broker/src/runtime_storage.rs map_err
        match resp.status() {
            reqwest::StatusCode::CONFLICT => return Ok(weft_core::storage::ReplaceOutcome::Busy),
            reqwest::StatusCode::PRECONDITION_FAILED => return Ok(weft_core::storage::ReplaceOutcome::Stale),
            status if !status.is_success() => return Err(status_err("replace begin", resp).await),
            _ => {}
        }
        let UploadBeginResponse { key: upload_key, part_size, .. } =
            resp.json().await.map_err(|e| http_err("replace begin", e))?;
        let file = self.drive_or_abort(run_id, &upload_key, part_size, data).await?;
        Ok(weft_core::storage::ReplaceOutcome::Replaced(file))
    }

    async fn get(
        &self,
        run_id: RunId,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)> {
        // 1. Ask the broker for the metadata + a presigned GET URL (bumps a kept
        //    file's expiry, 404s a missing file). No bytes through the broker.
        let resp = self
            .authed(self.http.get(self.url(&format!("/v1/storage/download-url/{key}"))), run_id)
            .await?
            .send()
            .await
            .map_err(|e| http_err("download-url", e))?;
        if !resp.status().is_success() {
            return Err(status_err("download-url", resp).await);
        }
        let DownloadUrlResponse { meta, url } =
            resp.json().await.map_err(|e| http_err("download-url response", e))?;
        // 2. Fetch the bytes DIRECTLY from the bucket, applying the Range here so a
        //    partial read never buffers the whole object anywhere.
        let mut req = self.http.get(&url);
        if let Some(r) = range {
            // Reject an inverted range loudly (end precedes start): a silent
            // 1-byte reinterpretation would hide a caller bug. An empty range
            // (end == start) is a valid zero-length read, short-circuited to an
            // empty body without hitting the bucket (`bytes=s-s-1` is invalid).
            let header = match r.end {
                Some(e) if e < r.start => {
                    return Err(WeftError::NodeFailed(format!(
                        "invalid byte range: end {e} precedes start {}",
                        r.start
                    )));
                }
                Some(e) if e == r.start => {
                    return Ok((meta, weft_core::storage::bytes_stream(bytes::Bytes::new())));
                }
                Some(e) => format!("bytes={}-{}", r.start, e - 1),
                None => format!("bytes={}-", r.start),
            };
            req = req.header("range", header);
        }
        let bytes_resp = req.send().await.map_err(|e| http_err("download from bucket", e))?;
        if !bytes_resp.status().is_success() {
            return Err(status_err("download from bucket", bytes_resp).await);
        }
        let stream: ByteStream = Box::pin(
            bytes_resp
                .bytes_stream()
                .map_err(|e| std::io::Error::other(format!("download stream: {e}"))),
        );
        Ok((meta, stream))
    }

    async fn delete(&self, run_id: RunId, key: &str) -> WeftResult<()> {
        let resp = self
            .authed(self.http.delete(self.url(&format!("/v1/storage/files/{key}"))), run_id)
            .await?
            .send()
            .await
            .map_err(|e| http_err("delete", e))?;
        if !resp.status().is_success() {
            return Err(status_err("delete", resp).await);
        }
        Ok(())
    }

    async fn find(&self, run_id: RunId, scope: &StorageScope, identity: &str) -> WeftResult<Option<StoredFileMeta>> {
        let out: weft_core::storage::IdentityLookupResponse = self
            .post_json(
                "/v1/storage/identity",
                run_id,
                &weft_core::storage::IdentityLookupRequest {
                    scope: scope.clone(),
                    identity: identity.to_string(),
                },
                "identity lookup",
            )
            .await?;
        Ok(out.file)
    }

    async fn list(&self, run_id: RunId, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
        let resp = self
            .authed(self.http.get(self.url("/v1/storage/list")), run_id)
            .await?
            .query(&[("scope", serde_json::to_string(scope).expect("scope serializes"))])
            .send()
            .await
            .map_err(|e| http_err("list", e))?;
        if !resp.status().is_success() {
            return Err(status_err("list", resp).await);
        }
        let out: ListFilesResponse = resp.json().await.map_err(|e| http_err("list response", e))?;
        Ok(out.files)
    }

    async fn keep(&self, run_id: RunId, key: &str, ttl: KeepTtl) -> WeftResult<()> {
        let resp = self
            .authed(self.http.post(self.url("/v1/storage/keep")), run_id)
            .await?
            .json(&weft_core::storage::KeepRequest { key: key.to_string(), ttl })
            .send()
            .await
            .map_err(|e| http_err("keep", e))?;
        if !resp.status().is_success() {
            return Err(status_err("keep", resp).await);
        }
        Ok(())
    }

    async fn link(&self, run_id: RunId, key: &str, ttl_secs: Option<u64>, reach: LinkReach) -> WeftResult<LinkAnswer> {
        self.post_json(
            "/v1/storage/link",
            run_id,
            &weft_core::storage::LinkRequest { key: key.to_string(), ttl_secs, reach },
            "link",
        )
        .await
    }

    async fn copy(
        &self,
        run_id: RunId,
        key: &str,
        scope: &StorageScope,
        identity: Option<&str>,
        keep: Option<KeepTtl>,
    ) -> WeftResult<Value> {
        self.post_json(
            "/v1/storage/copy",
            run_id,
            &weft_core::storage::CopyRequest {
                key: key.to_string(),
                scope: scope.clone(),
                identity: identity.map(str::to_string),
                keep,
            },
            "copy",
        )
        .await
    }

    async fn upload_slot(&self, run_id: RunId, request: &weft_core::storage::slot::UploadSlotRequest) -> WeftResult<String> {
        let out: weft_core::storage::slot::UploadSlotResponse =
            self.post_json("/v1/storage/upload-slot", run_id, request, "upload slot").await?;
        Ok(out.slot)
    }

    /// A refusal of any slot (no good, ended, another project's, already
    /// received, too big, not of the type its place takes, or nothing
    /// finished uploading through it) receives none of them, is the
    /// caller's to fix and comes back as an input error in the store's
    /// words. Anything else, the broker refusing the worker's own
    /// credentials included, is the node's failure.
    async fn claim_uploads(&self, run_id: RunId, claims: &[(&str, &WeftType)]) -> WeftResult<Vec<Value>> {
        use weft_core::storage::slot::{ClaimUploadRequest, ClaimUploadResponse, SlotClaim};
        let request = ClaimUploadRequest {
            claims: claims.iter().map(|(slot, fits)| SlotClaim { slot: slot.to_string(), fits: fits.wire_string() }).collect(),
        };
        let resp = self
            .authed(self.http.post(self.url("/v1/storage/upload/claim")), run_id)
            .await?
            .json(&request)
            .send()
            .await
            .map_err(|e| http_err("receive uploads", e))?;
        if is_slot_refusal(resp.status()) {
            return Err(WeftError::Input(body_text("receive uploads", resp).await?));
        }
        if !resp.status().is_success() {
            return Err(status_err("receive uploads", resp).await);
        }
        let answer: ClaimUploadResponse = resp.json().await.map_err(|e| http_err("receive uploads", e))?;
        if answer.files.len() != claims.len() {
            return Err(WeftError::NodeFailed(format!(
                "storage: receive uploads: asked for {} files, the broker answered {}",
                claims.len(),
                answer.files.len()
            )));
        }
        Ok(answer.files)
    }
}

// ---------- fake (tests) ----------

/// In-memory `WorkerStorageOps` for layer-3 tests: a map keyed by the
/// canonical storage key, with the same scope wall the broker enforces. Dumb:
/// state in a map, no business logic beyond the wall + keep flag.
#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeWorkerStorage;

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    use weft_core::storage::key::{self, CallerAuth};
    use weft_core::storage::FileLink;

    pub struct FakeWorkerStorage {
        /// The seeded caller identity the wall checks against (tenant t1,
        /// project p1, run c1), mirroring the broker's verdict.
        identity: CallerAuth,
        files: Mutex<BTreeMap<String, (StoredFileMeta, bytes::Bytes)>>,
        /// `(scope, identity)` of every identified put, to the key it
        /// minted.
        identities: Mutex<BTreeMap<(String, String), String>>,
        /// Monotonic id counter: the broker mints collision-free keys, so the
        /// fake must too. Deriving the id from `files.len()` reused a freed id
        /// after a delete (delete `f2`, next put reuses `f2`), which could
        /// silently overwrite the wrong entry and mask an ordering bug.
        next_id: Mutex<u64>,
    }

    impl FakeWorkerStorage {
        /// A fake bound to (tenant t1, project p1, run c1).
        pub fn new() -> Arc<Self> {
            Arc::new(Self {
                identity: CallerAuth::Worker {
                    tenant: "t1".into(),
                    project_id: "p1".into(),
                    run_id: Some("c1".into()),
                    instance: None,
                },
                files: Mutex::new(BTreeMap::new()),
                identities: Mutex::new(BTreeMap::new()),
                next_id: Mutex::new(0),
            })
        }

        fn enforce_wall(&self, key: &str) -> WeftResult<key::ParsedKey> {
            let parsed = key::parse_key(key)
                .map_err(|e| WeftError::NodeFailed(format!("storage: {e}")))?;
            key::check_key_access(&self.identity, &parsed)
                .map_err(|e| WeftError::NodeFailed(format!("storage denied: {e}")))?;
            Ok(parsed)
        }
    }

    #[async_trait]
    impl WorkerStorageOps for FakeWorkerStorage {
        async fn put(
            &self,
            _run_id: RunId,
            scope: &StorageScope,
            identity: Option<&str>,
            mime_type: &str,
            filename: &str,
            keep: Option<KeepTtl>,
            _declared_size: Option<u64>,
            data: ByteStream,
        ) -> WeftResult<Value> {
            // An identified put the scope already holds answers the
            // stored file, as the broker does; nothing is written.
            let identity_key = identity.map(|i| (format!("{scope:?}"), i.to_string()));
            if let Some(identity_key) = &identity_key {
                if let Some(existing) = self.identities.lock().get(identity_key) {
                    let files = self.files.lock();
                    let (meta, _) = files.get(existing).expect("an identified key is stored");
                    return Ok(StoredFile::from(meta).to_value());
                }
            }
            let id = {
                let mut n = self.next_id.lock();
                let id = *n;
                *n += 1;
                format!("f{id}")
            };
            let key = key::key_for_put(&self.identity, scope, &id)
                .map_err(|e| WeftError::NodeFailed(format!("storage denied: {e}")))?
                .to_key();
            let bytes = weft_core::storage::collect_stream(data)
                .await
                .map_err(|e| WeftError::NodeFailed(format!("storage: {e}")))?;
            let meta = StoredFileMeta {
                key: key.clone(),
                mime_type: mime_type.to_string(),
                size_bytes: bytes.len() as u64,
                filename: filename.to_string(),
                keep: keep.is_some() && matches!(scope, StorageScope::Run),
                expires_at_unix: None,
                keep_ttl_secs: keep.and_then(KeepTtl::secs),
                created_at_unix: 0,
                version: weft_core::storage::FIRST_FILE_VERSION,
            };
            let file = StoredFile::from(&meta);
            if let Some(identity_key) = identity_key {
                self.identities.lock().insert(identity_key, key.clone());
            }
            self.files.lock().insert(key, (meta, bytes));
            Ok(file.to_value())
        }

        async fn replace(
            &self,
            _run_id: RunId,
            key: &str,
            expected_version: Option<u64>,
            _declared_size: Option<u64>,
            data: ByteStream,
        ) -> WeftResult<weft_core::storage::ReplaceOutcome> {
            self.enforce_wall(key)?;
            let bytes = weft_core::storage::collect_stream(data)
                .await
                .map_err(|e| WeftError::NodeFailed(format!("storage: {e}")))?;
            let mut files = self.files.lock();
            let (meta, stored) = files
                .get_mut(key)
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            if expected_version.is_some_and(|v| v != meta.version) {
                return Ok(weft_core::storage::ReplaceOutcome::Stale);
            }
            meta.size_bytes = bytes.len() as u64;
            meta.version += 1;
            *stored = bytes;
            Ok(weft_core::storage::ReplaceOutcome::Replaced(StoredFile::from(&*meta)))
        }

        async fn find(&self, _run_id: RunId, scope: &StorageScope, identity: &str) -> WeftResult<Option<StoredFileMeta>> {
            let key = self.identities.lock().get(&(format!("{scope:?}"), identity.to_string())).cloned();
            Ok(key.and_then(|k| self.files.lock().get(&k).map(|(meta, _)| meta.clone())))
        }

        async fn get(
            &self,
            _run_id: RunId,
            key: &str,
            range: Option<ByteRange>,
        ) -> WeftResult<(StoredFileMeta, ByteStream)> {
            self.enforce_wall(key)?;
            let (meta, bytes) = self
                .files
                .lock()
                .get(key)
                .cloned()
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            let bytes = match range {
                None => bytes,
                Some(r) => {
                    // Mirror the real impl EXACTLY: reject an inverted range
                    // loudly, treat an empty range as a zero-length read, and
                    // clamp the end to the object length. Without this the fake
                    // panicked on `end < start` (slice start > end) where the
                    // real path silently reinterpreted it, so an L3 test would
                    // diverge from production.
                    if let Some(e) = r.end {
                        if e < r.start {
                            return Err(WeftError::NodeFailed(format!(
                                "invalid byte range: end {e} precedes start {}",
                                r.start
                            )));
                        }
                    }
                    let end = r.end.unwrap_or(bytes.len() as u64).min(bytes.len() as u64);
                    let start = r.start.min(end);
                    bytes.slice(start as usize..end as usize)
                }
            };
            Ok((meta, weft_core::storage::bytes_stream(bytes)))
        }

        async fn delete(&self, _run_id: RunId, key: &str) -> WeftResult<()> {
            self.enforce_wall(key)?;
            self.files
                .lock()
                .remove(key)
                .map(|_| ())
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))
        }

        async fn list(&self, _run_id: RunId, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
            let prefix = key::prefix_for_list(&self.identity, scope)
                .map_err(|e| WeftError::NodeFailed(format!("storage denied: {e}")))?;
            Ok(self
                .files
                .lock()
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, (m, _))| m.clone())
                .collect())
        }

        async fn keep(&self, _run_id: RunId, key: &str, ttl: KeepTtl) -> WeftResult<()> {
            self.enforce_wall(key)?;
            let mut files = self.files.lock();
            let (meta, _) = files
                .get_mut(key)
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            meta.keep = meta.keep || matches!(key::parse_key(key), Ok(p) if matches!(p.scope, key::KeyScope::Run { .. }));
            meta.keep_ttl_secs = ttl.secs();
            Ok(())
        }

        async fn link(&self, _run_id: RunId, key: &str, ttl_secs: Option<u64>, reach: LinkReach) -> WeftResult<LinkAnswer> {
            self.enforce_wall(key)?;
            let file = self
                .files
                .lock()
                .get(key)
                .map(|(meta, _)| meta.clone())
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            let ttl = ttl_secs.unwrap_or(weft_core::storage::DEFAULT_LINK_SECS);
            // The fake install is reached by nobody outside: its outside
            // link is the install's own address, which the internet does
            // not open.
            let link = match reach {
                LinkReach::Internal => FileLink { url: format!("http://fake-bucket/runtime/{key}?ttl={ttl}"), public: false },
                LinkReach::External => FileLink { url: format!("http://fake-install/files/runtime/{key}?ttl={ttl}"), public: false },
            };
            Ok(LinkAnswer { link, file })
        }

        async fn copy(
            &self,
            run_id: RunId,
            key: &str,
            scope: &StorageScope,
            identity: Option<&str>,
            keep: Option<KeepTtl>,
        ) -> WeftResult<Value> {
            self.enforce_wall(key)?;
            let (meta, bytes) = self
                .files
                .lock()
                .get(key)
                .cloned()
                .ok_or_else(|| WeftError::NodeFailed(format!("storage file not found: {key}")))?;
            self.put(run_id, scope, identity, &meta.mime_type, &meta.filename, keep, Some(meta.size_bytes), weft_core::storage::bytes_stream(bytes))
                .await
        }

        async fn upload_slot(&self, _run_id: RunId, request: &weft_core::storage::slot::UploadSlotRequest) -> WeftResult<String> {
            request.validate().map_err(WeftError::Input)?;
            Ok(format!("{}fake-slot", weft_core::storage::slot::SLOT_PREFIX))
        }

        async fn claim_uploads(&self, _run_id: RunId, claims: &[(&str, &WeftType)]) -> WeftResult<Vec<Value>> {
            let slots: Vec<&str> = claims.iter().map(|(slot, _)| *slot).collect();
            Err(WeftError::Input(format!("nothing was uploaded through '{}' (the fake storage takes no page's upload)", slots.join("', '"))))
        }
    }
}

#[cfg(test)]
mod complete_tests {
    use super::{complete_step, CompleteStep};
    use reqwest::StatusCode;

    #[test]
    fn only_the_marked_conflict_and_server_errors_are_asked_again() {
        assert_eq!(complete_step(StatusCode::OK, false), CompleteStep::Landed);
        assert_eq!(complete_step(StatusCode::CONFLICT, true), CompleteStep::Retry);
        assert_eq!(complete_step(StatusCode::INTERNAL_SERVER_ERROR, false), CompleteStep::Retry);
        assert_eq!(complete_step(StatusCode::GONE, false), CompleteStep::Ended);
        // An unmarked conflict is some other refusal, never "still landing".
        assert_eq!(complete_step(StatusCode::CONFLICT, false), CompleteStep::Refused);
        assert_eq!(complete_step(StatusCode::BAD_REQUEST, false), CompleteStep::Refused);
    }
}

#[cfg(test)]
mod claim_tests {
    use super::*;
    use axum::http::StatusCode;
    use weft_core::storage::slot::{ClaimUploadRequest, ClaimUploadResponse};

    /// A broker answering `upload/claim`: a claim for an `Audio` place is
    /// refused naming its slot (409), a slot named `short` gets an answer
    /// missing a file, a slot named `locked` is the worker's own
    /// credentials refused (401), and anything else is answered with one
    /// file per claim, keyed by its slot.
    async fn broker() -> Arc<WorkerStorage> {
        async fn claim(axum::Json(req): axum::Json<ClaimUploadRequest>) -> axum::response::Response {
            use axum::response::IntoResponse;
            if let Some(refused) = req.claims.iter().find(|c| c.fits == "Audio") {
                return (StatusCode::CONFLICT, format!("the file uploaded through '{}' does not fit here", refused.slot)).into_response();
            }
            if req.claims.iter().any(|c| c.slot == "locked") {
                return (StatusCode::UNAUTHORIZED, "who are you").into_response();
            }
            let files = req.claims.iter().filter(|c| c.slot != "short").map(|c| serde_json::json!({"key": c.slot, "fits": c.fits})).collect();
            axum::Json(ClaimUploadResponse { files }).into_response()
        }
        let app = axum::Router::new().route("/v1/storage/upload/claim", axum::routing::post(claim));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let token = weft_broker_client::TokenSource::worker(Arc::new(weft_platform_traits::identity::FixedToken("t".into())), "r1");
        WorkerStorage::new(format!("http://{addr}"), token)
    }

    /// Every slot of one claim goes to the broker in ONE request and the
    /// files come back in the claims' order; a refusal of any slot is the
    /// caller's input error in the broker's words, and an answer missing a
    /// file or the worker's credentials refused is the node's failure.
    #[tokio::test]
    async fn uploads_are_claimed_in_one_request_all_or_none() {
        let storage = broker().await;
        let ty = |t: &str| WeftType::parse(t).unwrap();
        let (image, video, audio) = (ty("Image"), ty("Video | Null"), ty("Audio"));
        let files = storage.claim_uploads(RunId::nil(), &[("weft-upload:a", &image), ("weft-upload:b", &video)]).await.unwrap();
        assert_eq!(
            files,
            [serde_json::json!({"key": "weft-upload:a", "fits": "Image"}), serde_json::json!({"key": "weft-upload:b", "fits": "Video | Null"})]
        );

        let refused = storage.claim_uploads(RunId::nil(), &[("weft-upload:a", &image), ("weft-upload:b", &audio)]).await.unwrap_err();
        assert!(matches!(&refused, WeftError::Input(why) if why.contains("'weft-upload:b' does not fit")), "{refused:?}");

        let short = storage.claim_uploads(RunId::nil(), &[("weft-upload:a", &image), ("short", &image)]).await.unwrap_err();
        assert!(matches!(&short, WeftError::NodeFailed(why) if why.contains("asked for 2 files, the broker answered 1")), "{short:?}");

        let locked = storage.claim_uploads(RunId::nil(), &[("locked", &image)]).await.unwrap_err();
        assert!(matches!(locked, WeftError::NodeFailed(_)), "{locked:?}");
    }
}
