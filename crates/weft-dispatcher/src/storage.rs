//! The dispatcher's storage control plane: the CLI `weft files` proxy to the
//! broker's runtime-file admin surface, plus the durable terminate-sweep queue.
//!
//! The broker is the single gatekeeper that owns the bucket + the
//! `runtime_file` metadata; the dispatcher carries no stored bytes of its own
//! (it reads a version's files on a link the broker signs, and its forwarder,
//! `crate::files_door`, passes requests on to the store unchanged). It is here
//! only to (1) front the CLI verbs (the CLI authenticates to the dispatcher, which
//! resolves the acting tenant and forwards to the broker as the control plane),
//! and (2) durably drive the terminate sweep: a worker can stall-then-die before
//! its eager sweep runs, so the write that ends a run that stored files of
//! its own enqueues a row, and this reaper drains it by asking the broker to
//! sweep the run's un-kept run files.

use anyhow::{Context, Result};
use weft_task_store::drain::DrainStep;

use weft_core::storage::{
    AdminLinkRequest, AssetUploadBeginRequest, AssetsHeldRequest, AssetsHeldResponse, LinkReader, ListFilesResponse,
    PartDoneRequest, PresignResult,
    StoredFileMeta, SweepRunRequest, SweepRunResponse, Tenanted, TenantScopeRequest, TenantUsage,
    UploadAbortRequest, UploadBeginResponse, UploadCompleteRequest, UploadPartsRequest,
    UploadPartsResponse, UploadResumeRequest, UploadResumeResponse, WipePrefixRequest,
    WipePrefixResponse,
};

use crate::state::DispatcherState;

// ---------- broker admin client ----------
//
// The wire envelopes live in `weft_core::storage` (single definition, shared
// with the broker's handlers), so the two ends cannot drift.

// The dispatcher's authenticated client of the broker's runtime-file admin
// surface: identity + URL joining live in `crate::role_client` (shared
// with the access-admin forwards and the listener); the typed retry
// classes below are this surface's own.

/// A sentinel in the error chain saying the broker answered 404 for a single-file
/// op. The api layer downcasts to this so a missing file surfaces as 404 to the
/// user instead of a blanket 500 (the broker's status class collapsed by `check`).
#[derive(Debug)]
pub struct StorageNotFound;

impl std::fmt::Display for StorageNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage object not found")
    }
}
impl std::error::Error for StorageNotFound {}

/// A broker 4xx other than 404: the broker understood the request and REFUSED
/// it (bad tenant/run/key shape, denied scope). Terminal for the request as
/// sent; retrying the identical request can never succeed. Carried typed through
/// the anyhow chain so retry loops (the sweep queue) can tell a dead request
/// from a transient broker fault.
#[derive(Debug)]
pub struct BrokerRejected {
    pub status: reqwest::StatusCode,
    /// The broker marked the refusal "this upload is being completed right
    /// now" ([`weft_core::storage::COMPLETING_HEADER`]); passed through so
    /// the caller can ask again instead of giving up.
    pub completing: bool,
}

impl std::fmt::Display for BrokerRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "broker rejected the request ({})", self.status)
    }
}

impl std::error::Error for BrokerRejected {}

/// The broker was out of reach, or said it could not serve right now (a
/// 502, 503 or 504): a fault that passes, which the person asks again on.
#[derive(Debug)]
pub struct StorageUnavailable;

impl std::fmt::Display for StorageUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage is out of reach right now")
    }
}

impl std::error::Error for StorageUnavailable {}

/// Whether `e` failed because storage could not be reached: the broker
/// answered it was unavailable ([`StorageUnavailable`]), or the request to it
/// never got through (no connection, a timeout).
pub fn storage_unreachable(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause.is::<StorageUnavailable>()
            || cause.downcast_ref::<reqwest::Error>().is_some_and(|r| r.is_connect() || r.is_timeout())
    })
}

async fn check(resp: reqwest::Response, what: &str) -> Result<reqwest::Response> {
    use reqwest::StatusCode;
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    let completing = resp.headers().contains_key(weft_core::storage::COMPLETING_HEADER);
    let body = resp.text().await.unwrap_or_default();
    if status == StatusCode::NOT_FOUND {
        // Preserve the 404 class through the anyhow chain: the api handler downcasts
        // to StorageNotFound and returns 404, not 500, for a missing file.
        return Err(anyhow::Error::new(StorageNotFound)
            .context(format!("broker storage {what} returned {status}: {body}")));
    }
    // Terminal client errors ONLY: the broker understood the request and refuses it
    // permanently (bad shape, denied scope, over quota, method/conflict). These are
    // BrokerRejected so the sweep queue stops retrying them.
    //
    // Deliberately NOT terminal: 401 UNAUTHORIZED. The broker returns 401 when it
    // cannot verify the caller's identity token, which includes a TRANSIENT fault
    // (the platform's signing keys momentarily unreachable, a token minted just
    // across a key rotation), not only a permanent refusal. Treating it as terminal
    // would permanently dead-letter a storage sweep on a passing blip and leak the
    // files. So 401 falls through to the transient bail below and is retried,
    // alongside 5xx.
    let terminal = matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::FORBIDDEN
            | StatusCode::PAYLOAD_TOO_LARGE
            | StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::CONFLICT
            | StatusCode::UNPROCESSABLE_ENTITY
    );
    if terminal {
        // A refusal, not a fault: typed so retry loops can stop retrying it.
        return Err(anyhow::Error::new(BrokerRejected { status, completing })
            .context(format!("broker storage {what} returned {status}: {body}")));
    }
    if matches!(status, StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT) {
        return Err(anyhow::Error::new(StorageUnavailable).context(format!("broker storage {what} returned {status}: {body}")));
    }
    // Everything else (401 auth-resolution fault, 429, 5xx, unexpected): transient,
    // retry.
    anyhow::bail!("broker storage {what} returned {status}: {body}")
}

/// POST one admin verb to the broker and parse its JSON response. Every
/// admin-surface call is this exact shape (identity bearer, JSON in, JSON out,
/// status classified by `check`), so it lives once.
async fn post_admin<Resp: serde::de::DeserializeOwned>(
    state: &DispatcherState,
    path: &str,
    what: &str,
    body: &impl serde::Serialize,
) -> Result<Resp> {
    let resp = state
        .broker
        .request(reqwest::Method::POST, path)
        .await?
        .json(body)
        .send()
        .await
        .with_context(|| format!("broker {what}"))?;
    check(resp, what).await?.json().await.with_context(|| format!("{what} parse"))
}

/// Same as `post_admin` for verbs whose success response carries no body.
async fn post_admin_unit(
    state: &DispatcherState,
    path: &str,
    what: &str,
    body: &impl serde::Serialize,
) -> Result<()> {
    let resp = state
        .broker
        .request(reqwest::Method::POST, path)
        .await?
        .json(body)
        .send()
        .await
        .with_context(|| format!("broker {what}"))?;
    check(resp, what).await?;
    Ok(())
}

/// Read one active file without changing its lifetime or minting a link.
pub async fn file_meta(state: &DispatcherState, key: &str) -> Result<StoredFileMeta> {
    let response = state
        .broker
        .request(reqwest::Method::GET, &format!("/v1/storage/admin/meta/{key}"))
        .await?
        .send()
        .await
        .context("read stored file metadata")?;
    check(response, "file metadata").await?.json().await.context("parse stored file metadata")
}

/// List one tenant's runtime files (the `weft files ls` surface).
pub async fn tenant_list(state: &DispatcherState, tenant: &str) -> Result<Vec<StoredFileMeta>> {
    let out: ListFilesResponse = post_admin(
        state,
        "/v1/storage/admin/tenant-list",
        "tenant-list",
        &TenantScopeRequest { tenant: tenant.to_string() },
    )
    .await?;
    Ok(out.files)
}

/// One tenant's footprint (the `weft files usage` surface).
pub async fn tenant_usage(state: &DispatcherState, tenant: &str) -> Result<TenantUsage> {
    post_admin(
        state,
        "/v1/storage/admin/tenant-usage",
        "tenant-usage",
        &TenantScopeRequest { tenant: tenant.to_string() },
    )
    .await
}

/// Publish current source references without changing node-created files' TTLs.
pub async fn set_asset_references(
    state: &DispatcherState,
    tenant: &str,
    references: weft_core::storage::AssetReferencesRequest,
) -> Result<weft_core::storage::AssetReferencesResponse> {
    post_admin(
        state,
        "/v1/storage/admin/asset-references",
        "update asset lifetimes",
        &Tenanted { tenant: tenant.into(), inner: references },
    ).await
}

/// Delete one file by its tenant-anchored key (`weft files rm <key>`).
pub async fn delete_key(state: &DispatcherState, key: &str) -> Result<()> {
    let resp = state
        .broker
        .request(reqwest::Method::DELETE, &format!("/v1/storage/admin/files/{key}"))
        .await?
        .send()
        .await
        .context("broker delete-key")?;
    check(resp, "delete-key").await?;
    Ok(())
}

/// A download link for one file, with its name and size, for `reader`:
/// a person's machine (the CLI, the editor), weft's own process, or
/// somebody outside reaching the install as a project's callers do. The
/// link goes straight to the store (or through the install's forwarder
/// for the outside), so a client may ask for any `Range` of it and resume.
pub async fn download_link(
    state: &DispatcherState,
    key: &str,
    ttl_secs: Option<u64>,
    reader: LinkReader,
) -> Result<PresignResult> {
    post_admin(
        state,
        "/v1/storage/admin/link",
        "link",
        &AdminLinkRequest { key: key.to_string(), ttl_secs, reader },
    )
    .await
}

/// The whole content of one stored file, read straight from the store on
/// a link the broker signed for this process. What a version build reads
/// its files with.
pub async fn read_file(state: &DispatcherState, key: &str) -> Result<Vec<u8>> {
    let link = download_link(state, key, None, LinkReader::Runtime).await?;
    let resp = state.http.get(&link.url).send().await.with_context(|| format!("read stored file {key}"))?;
    anyhow::ensure!(resp.status().is_success(), "the store answered {} reading stored file {key}", resp.status());
    let bytes = resp.bytes().await.with_context(|| format!("read stored file {key}"))?;
    anyhow::ensure!(
        bytes.len() as u64 == link.size_bytes,
        "stored file {key} came back {} bytes long, and the store records {}",
        bytes.len(),
        link.size_bytes
    );
    Ok(bytes.to_vec())
}

/// A version build's view of the storage plane (`crate::build::ProjectStorage`).
pub struct BrokerStorage<'a>(pub &'a DispatcherState);

#[async_trait::async_trait]
impl crate::build::ProjectStorage for BrokerStorage<'_> {
    async fn read(&self, key: &str) -> Result<Vec<u8>> {
        read_file(self.0, key).await
    }

    async fn meta(&self, key: &str) -> Result<StoredFileMeta> {
        file_meta(self.0, key).await
    }
}

/// Wipe a whole scope/tenant prefix (`weft files rm <prefix>` / project-delete).
pub async fn wipe_prefix(state: &DispatcherState, prefix: &str) -> Result<u64> {
    let out: WipePrefixResponse = post_admin(
        state,
        "/v1/storage/admin/wipe-prefix",
        "wipe-prefix",
        &WipePrefixRequest { prefix: prefix.to_string() },
    )
    .await?;
    Ok(out.wiped)
}

/// Terminate-sweep a run's un-kept run files: the broker reaps crashed
/// uploads now and stamps completed files with the post-run linger expiry.
async fn sweep_run(state: &DispatcherState, tenant: &str, run_id: &str) -> Result<SweepRunResponse> {
    post_admin(
        state,
        "/v1/storage/admin/sweep-run",
        "sweep-run",
        &SweepRunRequest { tenant: tenant.to_string(), run_id: run_id.to_string() },
    )
    .await
}

// ---------- asset upload proxy ----------
//
// The pre-build asset sync drives the broker's multipart upload contract
// through here: the dispatcher resolves the acting tenant (its api layer) and
// forwards each verb to the broker's admin upload surface. Bytes never pass
// through: the returned part URLs are presigned for the caller, which PUTs
// straight to the bucket.

/// Begin an ASSET upload (content-addressed: `content_hash` becomes the key
/// id) for `tenant`, the one the api layer resolved (never caller-claimed);
/// returns the minted key + part size.
pub async fn upload_begin(
    state: &DispatcherState,
    tenant: &str,
    req: AssetUploadBeginRequest,
) -> Result<UploadBeginResponse> {
    post_admin(
        state,
        "/v1/storage/admin/upload/begin",
        "upload-begin",
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Which of the named contents `tenant` already stores: a publish's diff.
pub async fn assets_held(
    state: &DispatcherState,
    tenant: &str,
    req: AssetsHeldRequest,
) -> Result<AssetsHeldResponse> {
    post_admin(
        state,
        "/v1/storage/admin/assets-held",
        "assets-held",
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Reserve + presign the next parts (browser-facing URLs).
pub async fn upload_parts(
    state: &DispatcherState,
    tenant: &str,
    req: UploadPartsRequest,
) -> Result<UploadPartsResponse> {
    post_admin(
        state,
        "/v1/storage/admin/upload/parts",
        "upload-parts",
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Record a landed part's etag.
pub async fn upload_part_done(
    state: &DispatcherState,
    tenant: &str,
    req: PartDoneRequest,
) -> Result<()> {
    post_admin_unit(
        state,
        "/v1/storage/admin/upload/part-done",
        "upload-part-done",
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Finalize the upload; returns the stored-file marker value the config holds.
pub async fn upload_complete(
    state: &DispatcherState,
    tenant: &str,
    req: UploadCompleteRequest,
) -> Result<serde_json::Value> {
    post_admin(
        state,
        "/v1/storage/admin/upload/complete",
        "upload-complete",
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Fresh browser-facing URLs for the parts that never landed.
pub async fn upload_resume(
    state: &DispatcherState,
    tenant: &str,
    req: UploadResumeRequest,
) -> Result<UploadResumeResponse> {
    post_admin(
        state,
        "/v1/storage/admin/upload/resume",
        "upload-resume",
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

/// Cancel an in-flight editor upload, freeing its reservation.
pub async fn upload_abort(
    state: &DispatcherState,
    tenant: &str,
    req: UploadAbortRequest,
) -> Result<()> {
    post_admin_unit(
        state,
        "/v1/storage/admin/upload/abort",
        "upload-abort",
        &Tenanted { tenant: tenant.to_string(), inner: req },
    )
    .await
}

// ---------- durable terminate-sweep queue ----------

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "storage_sweep",
    tables: &["storage_sweep"],
    ddl: &[r#"
        -- Durable terminate-sweep queue: a row per ended run that stored
        -- files of its own, whose un-kept run files still need sweeping.
        -- Inserted by the write that ends the run (`weft_record_batch`,
        -- `weft_journal::record::append_locked_in`), deleted by the sweep
        -- reaper once the broker confirmed the sweep.
        CREATE TABLE IF NOT EXISTS storage_sweep (
            run_id UUID PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            enqueued_at_unix BIGINT NOT NULL
        );
        "#,
        // Wake the sweep reaper when a run is queued, through the
        // announcement outbox like every other wake (`weft_task_store::announce`):
        // sent once the write commits, one wake for every row a flush
        // takes, since the reaper reads the whole queue.
        // SYNC: 'weft_storage_sweep' <-> crate::reaper::STORAGE_SWEEP_CHANNEL
        r#"CREATE OR REPLACE FUNCTION storage_sweep_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM weft_announce('weft_storage_sweep', '');
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS storage_sweep_notify_on_insert ON storage_sweep"#,
        r#"CREATE TRIGGER storage_sweep_notify_on_insert
            AFTER INSERT ON storage_sweep
            FOR EACH ROW
            EXECUTE FUNCTION storage_sweep_notify()"#,
    ],
    seed: &[],
};

/// Sweep-queue reaper: ask the broker to sweep each pending run's un-kept
/// run files. A row is removed only after the broker confirmed; a TRANSIENT
/// broker error (unreachable, 5xx) leaves the row and fails the pass once
/// every row was tried, so the loop tries again shortly a few times and then
/// writes the failure down for `weft status` (`weft_task_store::drain`); the
/// next run ending queued for a sweep wakes it, and every row is tried again
/// (the sweep is idempotent). A TERMINAL refusal (a 4xx: the broker understood and
/// rejected the request) is loud + dead-lettered: retrying the identical
/// request every tick forever would be a silent infinite loop over a row the
/// user can neither see nor clear, so the row is dropped with an error log
/// naming the run (the files, if any, remain reclaimable via `weft files`).
pub async fn process_sweep_queue(state: DispatcherState) -> Result<DrainStep> {
    let rows: Vec<(weft_core::RunId, String)> =
        sqlx::query_as("SELECT run_id, tenant_id FROM storage_sweep ORDER BY enqueued_at_unix")
            .fetch_all(&state.pg_pool)
            .await?;
    let mut deferred = Vec::new();
    for (run_id, tenant) in rows {
        match sweep_run(&state, &tenant, &run_id.to_string()).await {
            Ok(out) => {
                if out.swept > 0 || out.lingering > 0 {
                    tracing::info!(
                        target: "weft_dispatcher::storage",
                        %run_id, tenant = %tenant, swept = out.swept, lingering = out.lingering,
                        "terminate sweep: reaped crashed uploads, stamped completed \
                         un-kept run files with the post-run linger expiry"
                    );
                }
                sqlx::query("DELETE FROM storage_sweep WHERE run_id = $1")
                    .bind(run_id)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) if e.downcast_ref::<StorageNotFound>().is_some() => {
                // 404 from the broker means there was nothing to sweep for this
                // run (its files are already gone). That is terminal SUCCESS, not
                // a rejection: drop the row quietly (debug, not error).
                tracing::debug!(
                    target: "weft_dispatcher::storage",
                    %run_id, tenant = %tenant,
                    "terminate sweep found nothing to remove; clearing the queue row"
                );
                sqlx::query("DELETE FROM storage_sweep WHERE run_id = $1")
                    .bind(run_id)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) if e.downcast_ref::<BrokerRejected>().is_some() => {
                // The broker understood and permanently refuses this request (bad
                // shape / denied / over quota). Retrying it every tick forever would
                // be a silent infinite loop over a row nobody can clear, so drop it
                // with a loud error naming the run (any files stay reclaimable via
                // `weft files`).
                tracing::error!(
                    target: "weft_dispatcher::storage",
                    %run_id, tenant = %tenant, error = format!("{e:#}"),
                    "terminate sweep REJECTED by the broker; dropping the queue row \
                     (any remaining files for this run stay listable/deletable via \
                     the storage API)"
                );
                sqlx::query("DELETE FROM storage_sweep WHERE run_id = $1")
                    .bind(run_id)
                    .execute(&state.pg_pool)
                    .await?;
            }
            Err(e) => {
                // Transient (broker unreachable, apiserver blip surfaced as 401,
                // 5xx): keep the row, and fail the pass once the others went.
                tracing::warn!(
                    target: "weft_dispatcher::storage",
                    %run_id, tenant = %tenant, error = %e,
                    "terminate sweep deferred (transient broker/control-plane fault)"
                );
                deferred.push(format!("{run_id}: {e:#}"));
            }
        }
    }
    anyhow::ensure!(
        deferred.is_empty(),
        "the broker could not sweep the files of {} ended runs: {}",
        deferred.len(),
        deferred.join("; ")
    );
    Ok(DrainStep::Done)
}
