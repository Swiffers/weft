//! The runtime-file plane (`ctx.storage`): the files a running project
//! reads and writes (assets it fetches, run outputs, scratch). Bytes live
//! in the object-store bucket under the `runtime/` prefix; the file's
//! metadata (mime, name, size, keep/expiry) lives in the `runtime_file`
//! Postgres table. The broker is the single gatekeeper: a worker holds no
//! bucket credentials; the broker verifies the caller (the pure `key` wall),
//! charges the tenant's byte quota at reservation time, and mints presigned
//! URLs whose EXACT byte size is signed in, so the bucket itself enforces
//! every reservation. Bytes never transit the broker: uploads are multipart
//! (resumable, one signed URL per part) direct to the bucket.
//!
//! Why Postgres holds the metadata (not a sibling object next to each blob):
//! every interesting question (list a scope, a tenant's usage, am I over
//! quota, wipe a run's scratch) becomes one indexed query instead of a
//! bucket scan, so the plane scales with the number of tenants, not with the
//! number of stored files. The bucket holds ONLY opaque bytes.
//!
//! This is the runtime-file plane: the broker serves it directly. A
//! versioned-projects plane (pack a folder into a content tree, a version
//! graph) is built as a separate plane around this broker; this module is
//! only the runtime-file plane.

use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::PgPool;

use weft_core::storage::key::{CallerAuth, ParsedKey};
use weft_core::storage::{KeepTtl, PartAsk, PresignedPart, StorageScope, StoredFileMeta};
use weft_platform_traits::{ObjectStore, PresignAudience};

use crate::entitlement::{lock_tenant_storage, EntitlementSource};

/// Default TTL of a kept file or retired source asset (30 days). Access bumps
/// the expiry back to now + TTL, so an actively-used survivor never expires.
/// `KeepTtl::Default` resolves to this; defined beside `KeepTtl` in core.
pub use weft_core::storage::DEFAULT_KEEP_TTL_SECS;

/// How long an UN-KEPT completed run file lingers after its run terminates
/// before it expires. The terminate sweep stamps
/// `expires_at_unix = now + this` instead of deleting outright, so a user can
/// still open the files list and download a run's output right after the run
/// ends. It reads as gone once the deadline passes and the expiry loop
/// removes it then, and the stamped expiry is what the file lists surface as
/// the remaining lifetime.
pub const RUN_LINGER_TTL_SECS: i64 = 5 * 60;

/// Default lifetime of a presigned URL when the caller doesn't choose one.
pub const DEFAULT_PRESIGN_TTL_SECS: u64 = 15 * 60;

pub use weft_platform_traits::object_store::MAX_PRESIGN_TTL_SECS;

/// How long a 'pending' upload may sit with NO progress (no new part reserved)
/// before the sweep reaps it as abandoned: aborts its multipart upload, deletes
/// its row, frees its quota reservation. Long enough that a legitimately slow
/// upload (large parts on the default 15-min part-URL life, resumed after a
/// blip) is never reaped mid-flight, short enough that a crashed upload's
/// reservation doesn't hold quota hostage. Progress (a `parts` reservation)
/// refreshes the row's clock.
pub const PENDING_RESERVE_GRACE_SECS: i64 = 60 * 60;

/// Default part size for multipart uploads: 8 MiB. Every part is exactly this
/// size except the final one (which may be smaller). Above the 5 MiB floor
/// buckets impose on non-final parts, a 256 KiB multiple, and small enough
/// that a retry re-sends little. For a KNOWN total size the part size scales
/// up so the plan stays under the 10,000-part ceiling.
pub const DEFAULT_PART_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Hard ceiling on part numbers (the S3 multipart limit).
const MAX_PARTS: u64 = 10_000;

/// The part size for an upload: the default, scaled up (in whole MiB) when a
/// known total would otherwise exceed the part-count ceiling.
fn part_size_for(declared_size: Option<u64>) -> u64 {
    match declared_size {
        Some(total) if total > DEFAULT_PART_SIZE_BYTES * MAX_PARTS => {
            let mib = 1024 * 1024;
            // Smallest whole-MiB part size that fits `total` in MAX_PARTS parts.
            total.div_ceil(MAX_PARTS).div_ceil(mib) * mib
        }
        _ => DEFAULT_PART_SIZE_BYTES,
    }
}

/// The bucket prefix every runtime-file object lives under. The version
/// plane uses `chunks/` + `trees/`; the runtime plane uses `runtime/`, so
/// the two planes share one bucket without ever colliding.
const RUNTIME_PREFIX: &str = "runtime/";

/// The bucket object key for a canonical storage key: `runtime/<tenant>/...`.
/// The scope a key lives in: everything before its id. What an
/// identity is unique within (the same message fetched by two projects
/// is two files), and the expression the identity index is built on.
fn scope_prefix(key: &str) -> &str {
    key.rsplit_once('/').map(|(prefix, _)| prefix).unwrap_or(key)
}

fn object_key(key: &str) -> String {
    format!("{RUNTIME_PREFIX}{key}")
}

/// The loop that removes expired files ([`RuntimeStore::expire`]).
pub const FILE_EXPIRY_LOOP: &str = "runtime_file_expiry";

/// Announced when a write sets a file's expiry sooner than the expiry
/// loop may next look (as far as this broker copy knows): the loop looks
/// at once and books its next look at the soonest expiry.
pub const FILE_EXPIRY_CHANNEL: &str = "weft_file_expiry";

/// What wakes the expiry loop.
pub static FILE_EXPIRY_WAKE: &[weft_task_store::drain::WakeOn] = &[weft_task_store::drain::WakeOn::any(FILE_EXPIRY_CHANNEL)];

/// What the expiry loop does, in a person's words.
pub const FILE_EXPIRY_LABEL: &str = "expire stored files";

/// The pauses between the expiry loop's tries of one row's reap: three
/// tries in all, after which the row stays marked and the pass goes on. A
/// reap on a person's upload is tried once: it never waits on these.
const REAP_RETRY_PAUSES: [std::time::Duration; 2] = [std::time::Duration::from_millis(500), std::time::Duration::from_secs(2)];

/// SQL: the row is fenced for removal ('reaping') and nobody is reaping
/// it at `now_param` (a bind holding the store's clock): its claim lapsed
/// ([`CLAIM_LEASE_SECS`]) because its reap crashed, or its reap failed
/// and gave the claim back ([`RuntimeStore::mark_reap_failed`]). A claim
/// still running is some call's reap in progress, and nothing else takes
/// it.
fn reap_lapsed(now_param: &str) -> String {
    format!("(status = 'reaping' AND progressed_at_unix < {now_param} - {CLAIM_LEASE_SECS})")
}

/// SQL: fence row `$1` for removal ('reaping'), claimed at `$2` (the
/// store's clock), where `condition` (more `AND` clauses, or nothing) still
/// holds. Every flip to 'reaping' goes through here, so every reap holds a
/// claim ([`reap_lapsed`]).
fn fence_sql(condition: &str) -> String {
    format!("UPDATE runtime_file SET status = 'reaping', progressed_at_unix = $2 WHERE key = $1 {condition}")
}

/// SQL condition: the row has not expired by `now_param` (a bind holding
/// the store's clock). Every read of a file a person or a node sees
/// carries it, so a file is gone the moment it expires whenever the
/// expiry loop gets to it. Asset reads leave it out: an asset's life
/// follows the sources that reference it, and publishing brings it back.
fn unexpired(now_param: &str) -> String {
    format!("(expires_at_unix IS NULL OR expires_at_unix >= {now_param})")
}

/// A key's upload row in any status, as [`PendingUpload`] reads it; callers
/// add the status they accept.
const PENDING_ROW_SELECT_ANY: &str = "SELECT status, tenant_id, keep_ttl_secs, upload_id, part_size, declared_size, replaces \
     FROM runtime_file WHERE key = $1";

/// How long a claim on a row ('completing' or 'reaping', stamped in
/// `progressed_at_unix`) holds before another call takes the row over.
/// Only a claim whose process crashed or whose request was dropped
/// mid-way stays that long (a failed reap gives its claim back at once);
/// a live one finishes in a few
/// bucket calls. A completion driven early is harmless (two drives of one
/// upload agree), so for it this only spares work a live completion is
/// doing; a reap is never run twice at once.
pub const CLAIM_LEASE_SECS: i64 = 5 * 60;

/// The statement deleting the row at key `$1` that also matches
/// `condition`, for every path that ends an upload without completing
/// it. A replacement's multipart writes onto the replaced file's own
/// object, so by the time its row goes (an abort after a failed
/// completion, a reap) the file's bytes may already be the new ones:
/// the file moves to its next version either way, so a value read
/// before can never pass for the content now there.
/// Answers one row: whether the row deleted carried a failed reap's mark
/// (`reap_error`).
fn delete_upload_row(condition: &str) -> String {
    format!(
        "WITH gone AS (DELETE FROM runtime_file WHERE key = $1 {condition} RETURNING replaces, reap_error), \
         moved AS (UPDATE runtime_file SET version = version + 1 \
                   WHERE key IN (SELECT replaces FROM gone WHERE replaces IS NOT NULL)) \
         SELECT EXISTS (SELECT 1 FROM gone WHERE reap_error IS NOT NULL)"
    )
}

/// A `LIKE` pattern matching every key under `prefix`. `\` escapes any LIKE
/// metachar in the prefix (keys are tenant/scope/owner/id of validated segments,
/// so this is belt + suspenders, never a real escape need).
fn like_prefix(prefix: &str) -> String {
    format!("{}%", prefix.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))
}

/// Failure modes of a runtime-store operation, mapped to HTTP status by the
/// handler. Distinct so the worker can tell a real denial / not-found from a
/// quota rejection.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeStoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("denied: {0}")]
    Denied(String),
    #[error("invalid: {0}")]
    Invalid(String),
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),
    /// The content-addressed key already exists (an asset begin raced another
    /// sync, or the content is simply already uploaded). Distinguishable so
    /// the sync can treat "already active" as the idempotent success it is.
    #[error("conflict: {0}")]
    Conflict(String),
    /// A replacement named the version of the file it was based on, and
    /// the file has moved on since: the writer re-reads and tries again.
    #[error("stale: {0}")]
    Stale(String),
    /// A completion has claimed the upload and has not landed yet: the
    /// upload cannot change, and asking `complete` again is the way on.
    /// The HTTP layer marks it (`x-weft-completing`) so a client tells it
    /// apart from every other conflict without reading the text.
    #[error("completing: {0}")]
    Completing(String),
    /// A claimed completion whose outcome the bucket cannot account for:
    /// the upload was ended and its reservation freed. Final; re-upload.
    #[error("lost: {0}")]
    Lost(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// What one upload IS, independent of transport: scope + metadata + declared
/// size + (for the content-addressed asset scope) the explicit hash id.
/// Collapses `begin_upload`'s parameter list; both the worker data path and
/// the control-plane admin path build one of these from their wire envelope.
#[derive(Debug, Clone, Copy)]
pub struct UploadSpec<'a> {
    pub scope: &'a StorageScope,
    pub mime: &'a str,
    pub filename: &'a str,
    pub keep: Option<KeepTtl>,
    pub declared_size: Option<u64>,
    /// The sha256 id for `StorageScope::Asset` (required there, refused
    /// elsewhere); every other scope mints a uuid.
    pub content_hash: Option<&'a str>,
    /// What the file is a copy OF (a message id, a document id), so a
    /// second begin naming the same identity in the same scope answers
    /// the file already there. Refused on the asset scope, which is
    /// content-addressed already.
    pub identity: Option<&'a str>,
}

/// What a begin answered: a fresh reservation to upload into, or (asset
/// scope only) the content already stored ACTIVE under its hash, in which
/// case there is nothing to transfer and `key` is the existing file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginUpload {
    Ready { key: String, part_size: u64 },
    AlreadyStored { key: String },
    /// This content is already part way up, under this key: carry on with
    /// it ([`RuntimeStore::resume_upload`]) instead of starting again.
    ///
    /// Answered rather than refused because it is SAFE to carry on, even
    /// while another uploader is doing the same: the key is the content's
    /// hash, so both hold identical bytes, and a part is reserved by
    /// number, so naming part 3 twice reserves it once and both writers
    /// put the same bytes in it. Refusing instead (which is what this
    /// used to do) left the second publish of the same asset failing for
    /// as long as the first one's leftovers sat there, up to the hour the
    /// sweep takes to clear an abandoned upload.
    Resume { key: String, part_size: u64 },
}

/// Internal outcome of the begin's reservation transaction.
enum Reserved {
    Fresh,
    /// The file is already there, under this key: a content-addressed
    /// begin whose content is active (the minted key), or an identified
    /// begin whose identity the scope holds (that file's key).
    AlreadyActive(String),
    /// A content-addressed key whose upload is part way up, with the part
    /// size it was begun with.
    Pending(String, u64),
}

type StoreResult<T> = Result<T, RuntimeStoreError>;

fn validate_stream_layout(parts: &std::collections::BTreeMap<i32, u64>, part_size: u64) -> StoreResult<()> {
    for (index, (number, size)) in parts.iter().enumerate() {
        if *number != index as i32 + 1 {
            return Err(RuntimeStoreError::Invalid("stream parts must form a contiguous sequence starting at 1".into()));
        }
        if index + 1 < parts.len() && *size != part_size {
            return Err(RuntimeStoreError::Invalid(format!("stream part {number} is short; only the final part can be short")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod stream_layout_tests {
    use super::*;

    #[test]
    fn only_a_contiguous_prefix_with_a_short_final_part_is_valid() {
        let parts = |items: &[(i32, u64)]| items.iter().copied().collect();
        assert!(validate_stream_layout(&parts(&[(1, 10), (2, 3)]), 10).is_ok());
        assert!(validate_stream_layout(&parts(&[(2, 10)]), 10).is_err());
        assert!(validate_stream_layout(&parts(&[(1, 3), (2, 10)]), 10).is_err());
        assert!(validate_stream_layout(&parts(&[(1, 10), (3, 10)]), 10).is_err());
    }
}

/// The columns a [`FileRow`] reads, in every SELECT and RETURNING that
/// builds one, so a column added to the file's metadata is added once.
const FILE_ROW_COLUMNS: &str =
    "key, mime_type, filename, size_bytes, keep, expires_at_unix, keep_ttl_secs, created_at_unix, version";

/// The runtime-file plane's schema. The broker owns it (it is the only
/// reader/writer); the boot applies it with every other group, in one pass
/// over the install's one list (`weft_runtime::schema`). The canonical CREATE
/// lives here, edited in place; an existing database is carried to it by
/// a migration written with `./setup.sh --migration <name>` (the whole
/// contract is `weft_task_store::schema_guard`'s header).
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "runtime_file",
    tables: &["runtime_file", "runtime_file_part", "public_file_link", "asset_reference"],
    ddl: &[
        r#"
        -- One row per runtime file. `key` is the canonical
        -- `<tenant>/<scope>/<owner>/<id>` string (also the bucket object key
        -- under the `runtime/` prefix). `tenant_id` is the first key segment,
        -- denormalized so per-tenant usage + listing are indexed lookups.
        CREATE TABLE IF NOT EXISTS runtime_file (
            key                TEXT PRIMARY KEY,
            tenant_id          TEXT NOT NULL,
            mime_type          TEXT NOT NULL,
            filename           TEXT NOT NULL,
            size_bytes         BIGINT NOT NULL,
            -- Upload lifecycle. 'pending': the row was reserved at upload begin,
            -- BEFORE any bytes; it carries the multipart resume state below and
            -- its reserved_bytes are already charged against the tenant's byte
            -- quota. 'active': the upload completed (bytes assembled + sized).
            -- The row exists FIRST and bytes land SECOND, so the bucket never
            -- holds an object with no row; a 'pending' row whose upload never
            -- completed is reaped by the row-driven sweeps (which also abort its
            -- multipart upload). 'completing': a completion claimed the
            -- upload (its parts are final) and is having the bucket assemble
            -- the object; nothing may abort it, change its parts, or remove
            -- the file it replaces until it lands, and progressed_at_unix
            -- holds the claim time, from which the tenant's next upload drives
            -- a completion its caller abandoned. 'reaping': a call fenced the row for removal
            -- (writers and reads are locked out; the bucket state goes next,
            -- then the row), and progressed_at_unix holds the claim time; a
            -- reap that crashed or failed leaves the row in 'reaping', and
            -- once its claim lapsed (or a failed reap gave it back) the
            -- expiry loop, a wipe or the tenant's next upload takes it
            -- over. Only 'active' rows
            -- appear in user listings / gets; ALL statuses count toward the
            -- byte quota ('active' by size_bytes, others by reserved_bytes),
            -- which is what makes an in-flight upload unable to blow past the
            -- cap.
            status             TEXT NOT NULL DEFAULT 'active',
            -- True iff this run-scoped file is flagged to survive the
            -- terminate sweep. Always false in the other scopes, which
            -- outlive runs without a flag. Set at begin; a PENDING kept
            -- row is still sweepable (only kept ACTIVE files are spared).
            keep               BOOLEAN NOT NULL DEFAULT FALSE,
            -- Unix seconds at which the file expires (access-bumped), in
            -- any scope: a kept run file, a project/shared/instance
            -- file stored with a lifetime, a retired asset. NULL = no
            -- expiry. Set at complete, never on a pending row.
            expires_at_unix    BIGINT,
            -- The file's TTL so an access can recompute expiry. NULL when
            -- there is no expiry.
            keep_ttl_secs      BIGINT,
            created_at_unix    BIGINT NOT NULL,
            -- Multipart upload state, present on a 'pending' row (NULL once
            -- active). upload_id is the bucket's multipart handle (the resume
            -- handle); part_size is the fixed size of every non-final part.
            upload_id          TEXT,
            part_size          BIGINT,
            -- The total size declared at begin, NULL for an unknown-length
            -- stream. A declared upload's parts must slice exactly to it.
            declared_size      BIGINT,
            -- The bytes CHARGED against the tenant quota for this in-flight
            -- upload: the declared total (known size) or the running sum of
            -- reserved parts (stream). Every reserved part's exact size is
            -- signed into its URL, so the bucket enforces this number.
            reserved_bytes     BIGINT NOT NULL DEFAULT 0,
            -- Progress clock for the abandoned-pending reap: bumped whenever a
            -- part is reserved, so a long multi-part upload that is still
            -- moving is never reaped mid-flight. On a 'completing' or
            -- 'reaping' row, the time of its claim; 0 on a 'reaping' row
            -- whose reap failed, which gave its claim back.
            progressed_at_unix BIGINT NOT NULL DEFAULT 0,
            -- What the file is a copy OF, when the writer said (a message
            -- id, a document id at a provider): a begin naming an identity
            -- the scope already holds answers that file instead of minting
            -- another. NULL for a file that is its own thing.
            identity           TEXT,
            -- On a 'pending' row only: the key of the ACTIVE file this
            -- upload will overwrite (`StorageHandle::replace`). The upload
            -- writes straight to that file's object, which the bucket swaps
            -- in whole when the multipart completes, so readers see the old
            -- bytes until then and the new ones after. Completion folds the
            -- new size into that file's row and deletes this one. NULL for
            -- an upload that makes a new file.
            replaces           TEXT,
            -- On an 'active' row only: the key of the last replacement
            -- upload folded into this file (that upload's own row is gone
            -- once folded). A retried complete of that upload finds the
            -- file here and answers with it. NULL until a replacement lands.
            last_replacement   TEXT,
            -- Which write of this file's content the row describes: 1 when
            -- the file is made, and one more at the end of every
            -- replacement (folded, aborted or reaped alike, since the
            -- bytes may have been swapped either way). A key is never
            -- reused, so (key, version) names one content. Every stored
            -- file value carries it; a replacement that names the version
            -- it read is refused when the file has moved on since.
            version            BIGINT NOT NULL DEFAULT 1,
            -- On a 'reaping' row only: why a reap of it failed. While any
            -- row is marked, a failure naming them is written down for
            -- `weft status`. A failed reap gives its claim back: the expiry
            -- loop's next pass, a wipe or the tenant's next upload takes it
            -- again, and no look is booked for it until one does. The mark
            -- stays while that reap runs, so its record clears only once
            -- the row is gone.
            reap_error         TEXT
        );
        -- One replacement in flight per file: its multipart writes onto
        -- the file's own object, so a second one would race it there.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_one_replacement
            ON runtime_file(replaces) WHERE replaces IS NOT NULL;
        -- A replacement folds into exactly one file, and a key is never
        -- reused: the lookup a retried complete makes.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_last_replacement
            ON runtime_file(last_replacement) WHERE last_replacement IS NOT NULL;
        -- One file per identity per scope (the key minus its id): the
        -- lookup begin makes, and the guarantee that two runs fetching the
        -- same thing at once cannot both land.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_file_identity
            ON runtime_file((regexp_replace(key, '/[^/]+$', '')), identity)
            WHERE identity IS NOT NULL;
        -- One row per RESERVED part of a pending upload: the exact size signed
        -- into its URL, and the etag once the caller reports it landed (NULL =
        -- reserved but not yet landed, i.e. what resume re-presigns). Rows are
        -- deleted at complete; ON DELETE CASCADE ties them to the file row for
        -- every sweep/abort path.
        CREATE TABLE IF NOT EXISTS runtime_file_part (
            key           TEXT NOT NULL REFERENCES runtime_file(key) ON DELETE CASCADE,
            part_number   INT NOT NULL,
            size_bytes    BIGINT NOT NULL,
            etag          TEXT,
            PRIMARY KEY (key, part_number)
        );
        -- Per-tenant usage + listing range over the key prefix; the index on
        -- (tenant_id, key) serves the tenant-usage sum and the prefix list.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_tenant ON runtime_file(tenant_id);
        -- The expiry loop ranges files by their expiry.
        CREATE INDEX IF NOT EXISTS idx_runtime_file_expiry
            ON runtime_file(expires_at_unix) WHERE expires_at_unix IS NOT NULL;
        -- One row per minted PUBLIC RELAY link: the public
        -- `/public/files/{token}` route resolves the token here and
        -- streams the file. `fetch_url` is a presigned GET, signed for the
        -- endpoint the runtime does its own I/O on (the broker relays the
        -- bytes from it), for the same lifetime
        -- as the token. Rows expire with the link; every mint deletes
        -- the expired ones, so the table stays the size of the live
        -- link set. ON DELETE CASCADE ties a link to its file row, so a
        -- deleted/swept file takes its links with it and a live token
        -- can never point at bytes that are gone (a dead token is a
        -- clean 404, never a broken stream).
        CREATE TABLE IF NOT EXISTS public_file_link (
            token           TEXT PRIMARY KEY,
            key             TEXT NOT NULL REFERENCES runtime_file(key) ON DELETE CASCADE,
            mime_type       TEXT NOT NULL,
            filename        TEXT NOT NULL,
            fetch_url       TEXT NOT NULL,
            expires_at_unix BIGINT NOT NULL
        );
        -- Which projects reference which of the tenant's assets
        -- (`<tenant>/asset/<sha256>`): one row per (project, asset), the
        -- project's whole set replaced at every publish. An asset is one
        -- file per tenant whichever projects hold its content, so it lives
        -- while ANY project of the tenant has a row for it (no expiry), and
        -- the publish that removes its last row starts its countdown. No
        -- foreign key to `runtime_file`: a row records what the project's
        -- sources and versions name, which stays true when the file itself
        -- is gone (the next publish reports it missing).
        CREATE TABLE IF NOT EXISTS asset_reference (
            tenant_id   TEXT NOT NULL,
            project_id  TEXT NOT NULL,
            key         TEXT NOT NULL,
            PRIMARY KEY (tenant_id, project_id, key)
        );
        -- "Does any project still reference this asset?": the question a
        -- publish asks of every asset it stops referencing.
        CREATE INDEX IF NOT EXISTS idx_asset_reference_key ON asset_reference(key);
        "#,
    ],
    seed: &[],
};

/// A clock seam so the expiry math is testable without wall-clock. The broker
/// already carries a `weft_platform_traits::Clock`; the store takes one.
pub use weft_platform_traits::Clock;

/// The runtime-file store: PG metadata + bucket bytes, one gatekeeper.
pub struct RuntimeStore {
    pool: PgPool,
    bucket: Arc<dyn ObjectStore>,
    clock: Arc<dyn Clock>,
    /// The soonest expiry this copy rang the expiry loop for
    /// ([`Self::ring_for`]).
    rung: std::sync::Mutex<crate::notices::EarliestRung>,
}

/// One stored-file metadata row, the in-Rust shape of a `runtime_file` row.
#[derive(Debug, Clone, sqlx::FromRow)]
struct FileRow {
    key: String,
    mime_type: String,
    filename: String,
    size_bytes: i64,
    keep: bool,
    expires_at_unix: Option<i64>,
    keep_ttl_secs: Option<i64>,
    created_at_unix: i64,
    version: i64,
}

impl FileRow {
    fn to_meta(&self) -> StoredFileMeta {
        StoredFileMeta {
            key: self.key.clone(),
            mime_type: self.mime_type.clone(),
            size_bytes: self.size_bytes as u64,
            filename: self.filename.clone(),
            keep: self.keep,
            expires_at_unix: self.expires_at_unix,
            keep_ttl_secs: self.keep_ttl_secs.map(|s| s as u64),
            created_at_unix: self.created_at_unix,
            version: self.version as u64,
        }
    }
}

/// A resolved public-relay link: the headers the relay answers with and
/// the presigned internal URL it streams the bytes from.
#[derive(Debug)]
pub struct PublicLinkTarget {
    pub mime_type: String,
    pub filename: String,
    pub fetch_url: String,
}

/// What a sweep needs to know per row: spare it (kept ACTIVE file) or reap
/// its bucket state (aborting the in-flight upload of a pending row).
#[derive(Debug, sqlx::FromRow)]
struct SweepEntry {
    key: String,
    kept_active: bool,
    status: String,
    upload_id: Option<String>,
    /// The file a replacement upload overwrites: its multipart targets
    /// THAT file's object, so reaping it aborts the upload and must never
    /// delete the object (the file it replaces still owns it).
    replaces: Option<String>,
}

/// One in-flight upload's row: the metadata captured at begin plus the
/// multipart resume state. The in-Rust shape of a 'pending' or
/// 'completing' `runtime_file` row.
#[derive(Debug, Clone, sqlx::FromRow)]
struct PendingUpload {
    /// 'pending' while parts can still be reserved and reported,
    /// 'completing' once a completion has claimed it.
    status: String,
    tenant_id: String,
    keep_ttl_secs: Option<i64>,
    upload_id: Option<String>,
    part_size: i64,
    declared_size: Option<i64>,
    /// The active file this upload overwrites, when it is a replacement.
    replaces: Option<String>,
}

impl PendingUpload {
    /// The bucket object this upload writes to: the replaced file's own
    /// object for a replacement (the bucket swaps it in whole at
    /// completion), else the upload's own.
    fn object_key(&self, key: &str) -> String {
        object_key(self.replaces.as_deref().unwrap_or(key))
    }

    /// Refuse a change to an upload a completion has claimed: its parts
    /// are what the bucket is assembling.
    fn refuse_if_completing(&self, key: &str) -> StoreResult<()> {
        if self.status == "completing" {
            return Err(RuntimeStoreError::Completing(format!(
                "upload '{key}' is completing; it cannot change now. Call complete again to \
                 get the file once it lands"
            )));
        }
        Ok(())
    }

    /// The row as a reap sees it.
    fn sweep_entry(&self, key: &str) -> SweepEntry {
        SweepEntry {
            key: key.to_string(),
            kept_active: false,
            status: self.status.clone(),
            upload_id: self.upload_id.clone(),
            replaces: self.replaces.clone(),
        }
    }
}

/// What the bucket made of a claimed completion.
enum Bucket {
    /// The object is assembled, this many bytes.
    Assembled(u64),
    /// The final verdict: the upload cannot be completed. `overwritten`
    /// when a replaced file's object may no longer hold that file's bytes.
    Lost { why: String, overwritten: bool },
}

/// What [`RuntimeStore::fence_for_reap`] did with a row.
#[derive(Debug, PartialEq, Eq)]
enum Fence {
    /// Flipped to 'reaping': the caller reaps it.
    Fenced,
    /// No longer matching the sweep's condition.
    Spared,
    /// No row left under the key.
    Gone,
    /// Left alone because a completion is landing on it.
    Completing,
}

/// What [`RuntimeStore::lock_for_removal`] found under a key.
enum Removal {
    /// No row.
    Gone,
    /// A completion forbids the removal, for this reason.
    Blocked(String),
    /// The row is locked and may be removed.
    Free,
}

impl SweepEntry {
    /// A finished file's row as a reap sees it: an object, no upload.
    fn file(key: &str) -> Self {
        Self { key: key.to_string(), kept_active: false, status: "active".into(), upload_id: None, replaces: None }
    }
}

/// SQL fragment: the later of `param` (a bind like `$1`) and the newest
/// live public link's expiry for the row being updated. Every write to
/// `runtime_file.expires_at_unix` that could SHORTEN a deadline goes
/// through this, so no path (access bump, keep, linger stamp) can pull
/// a file's death forward under a live link: a minted link is a promise
/// the bytes stay fetchable for its stated lifetime.
fn expiry_honoring_links(param: &str) -> String {
    format!(
        "GREATEST({param}, COALESCE((SELECT MAX(l.expires_at_unix) FROM public_file_link l \
         WHERE l.key = runtime_file.key), 0))"
    )
}


impl RuntimeStore {
    pub fn new(pool: PgPool, bucket: Arc<dyn ObjectStore>, clock: Arc<dyn Clock>) -> Self {
        Self { pool, bucket, clock, rung: Default::default() }
    }

    /// A write just gave a file the expiry `at`: ring the expiry loop when
    /// no ring of this copy's covers it ([`crate::notices::EarliestRung`]).
    /// Only writes that can make an expiry sooner call this (a keep, a new
    /// lifetime, the linger stamp); an access pushes an expiry later and
    /// rings nothing. A ring that cannot be sent is logged: the loop then
    /// looks when its booking or a later ring says.
    async fn ring_for(&self, at: Option<i64>) {
        let Some(at) = at else { return };
        if !self.rung.lock().expect("expiry rung").needs(at, self.clock.now_unix()) {
            return;
        }
        match sqlx::query("SELECT pg_notify($1, '')").bind(FILE_EXPIRY_CHANNEL).execute(&self.pool).await {
            Ok(_) => self.rung.lock().expect("expiry rung").rang(at),
            Err(e) => tracing::error!(
                target: "weft_broker::runtime_store", error = %e, at,
                "could not ring the file expiry loop; a file expiring then is removed at the loop's next look"
            ),
        }
    }

    /// One tenant's live footprint (file_count, charged_bytes). Bytes count an
    /// ACTIVE file by its size and a PENDING upload by its reserved (quota-
    /// charged) bytes, so an in-flight upload is visible in usage the moment
    /// it reserves, exactly as the quota check sees it. An expired file
    /// counts for nothing. The quota check + the usage view read this, so the
    /// two can never disagree. The tenant's abandoned uploads are reclaimed
    /// first ([`Self::reclaim_abandoned`]), so they do not count either.
    pub async fn tenant_usage(&self, tenant: &str) -> Result<(u64, u64)> {
        self.reclaim_abandoned(tenant).await?;
        let now = self.clock.now_unix();
        let count = count_files(&self.pool, tenant, now).await?;
        let bytes = charged_bytes_for(&self.pool, tenant, now).await?;
        Ok((count, bytes))
    }

    /// Reclaim `tenant`'s abandoned uploads, with no lock held (a bucket
    /// call is never made under the tenant lock): drive every completion
    /// claimed longer than [`CLAIM_LEASE_SECS`] ago (its process crashed or
    /// its request was dropped), reap every pending upload with no progress
    /// for [`PENDING_RESERVE_GRACE_SECS`], and finish every reap whose claim
    /// lapsed ([`reap_lapsed`]), each tried once. Run when the tenant
    /// uploads again or asks what it stores: an abandoned upload only
    /// matters for what it holds of the tenant's quota. The bucket's rule
    /// that aborts unfinished multipart uploads after a day covers the
    /// bucket side of a tenant that never comes back.
    async fn reclaim_abandoned(&self, tenant: &str) -> Result<()> {
        let now = self.clock.now_unix();
        let stale: Vec<String> = sqlx::query_scalar(
            "SELECT key FROM runtime_file WHERE tenant_id = $1 AND status = 'completing' AND progressed_at_unix < $2",
        )
        .bind(tenant)
        .bind(now - CLAIM_LEASE_SECS)
        .fetch_all(&self.pool)
        .await
        .context("stale completion scan")?;
        for key in stale {
            if let Err(e) = self.drive_completion(&key, true).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %key, error = %e,
                    "a stale completion did not land; it stays marked and the tenant's next upload retries it"
                );
            }
        }
        // A 'reaping' row whose reap crashed or failed still holds the
        // tenant's quota, so it is finished here too.
        // `$2` is the store's clock, in the scan and in the fence alike.
        let abandoned = format!(
            "((status = 'pending' AND progressed_at_unix < $2 - {PENDING_RESERVE_GRACE_SECS}) OR {})",
            reap_lapsed("$2"),
        );
        let entries: Vec<SweepEntry> = sqlx::query_as(&format!(
            "SELECT key, FALSE AS kept_active, status, upload_id, replaces FROM runtime_file \
             WHERE tenant_id = $1 AND {abandoned}"
        ))
        .bind(tenant)
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .context("abandoned upload scan")?;
        for entry in &entries {
            // A pending row that progressed since the scan is no longer
            // abandoned, and a lapsed reap another call took over since is
            // claimed again: the fence's re-check spares both.
            if self.fence_for_reap(&entry.key, &format!("AND {abandoned}"), now).await? == Fence::Fenced {
                self.reap_with_tries(entry, &[]).await?;
            }
        }
        Ok(())
    }

    /// Would storing `incoming` more bytes push the tenant over their disk cap,
    /// counting their WHOLE account (every storage plane), read on `tx` under
    /// the caller's lock so the total is one fresh number. This is the single
    /// place the account-wide byte check is assembled; both upload entry points
    /// call it. `account_used_bytes` already includes this plane's charged
    /// bytes, so the check is just `total + incoming > cap`.
    async fn account_would_exceed(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        entitlements: &dyn EntitlementSource,
        tenant: &str,
        incoming: u64,
    ) -> Result<bool> {
        let total = entitlements.account_used_bytes(tx, tenant, self.clock.now_unix()).await?;
        Ok(entitlements.caps(tenant).await?.disk_bytes_would_exceed(total, incoming))
    }

    /// Re-wall a caller-supplied key: parse it through the grammar and confirm
    /// the caller may touch it. Every key-addressed upload verb (parts /
    /// part-done / complete / resume / abort) goes through here.
    fn wall_key(caller: &CallerAuth, key: &str) -> StoreResult<ParsedKey> {
        let parsed =
            weft_core::storage::key::parse_key(key).map_err(RuntimeStoreError::Denied)?;
        weft_core::storage::key::check_key_access(caller, &parsed)
            .map_err(RuntimeStoreError::Denied)?;
        Ok(parsed)
    }

    /// Begin a multipart upload: mint the key (tenant wall stamped in, the
    /// caller can't choose it), gate the file count, charge a KNOWN total size
    /// against the byte quota (a declared over-cap upload is rejected before a
    /// single byte can land anywhere), reserve the 'pending' row with the
    /// file's metadata, and open the bucket's multipart upload. Returns the
    /// key + the fixed part size the caller must slice to, or
    /// [`BeginUpload::AlreadyStored`] when a content-addressed (asset) begin
    /// names content that is already ACTIVE: same content = same asset, so
    /// the begin is that upload's idempotent success and there is nothing to
    /// transfer. A PENDING collision (another upload of this content is mid
    /// flight) stays a loud conflict.
    ///
    /// An unknown-length stream (`declared_size = None`) charges nothing here;
    /// each part is charged as it is reserved in [`Self::reserve_parts`].
    pub async fn begin_upload(
        &self,
        caller: &CallerAuth,
        spec: &UploadSpec<'_>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<BeginUpload> {
        let UploadSpec { scope, mime, filename, keep, declared_size, content_hash, identity } = *spec;
        if identity.is_some() && matches!(scope, StorageScope::Asset) {
            return Err(RuntimeStoreError::Invalid(
                "an identity does not apply to the asset scope, which is addressed by content".into(),
            ));
        }
        if identity.is_some_and(|i| i.is_empty() || i.len() > 512) {
            return Err(RuntimeStoreError::Invalid(
                "a file identity is a non-empty label of at most 512 characters".into(),
            ));
        }
        // A lifetime applies in every scope a node writes: on a run
        // file it also spares it from the end-of-run sweep, on the others
        // (which outlive runs already) it is simply when the file expires.
        // The asset scope's lifetime belongs to the source that references
        // it, so a node-chosen one is refused rather than dropped.
        if keep.is_some() && matches!(scope, StorageScope::Asset) {
            return Err(RuntimeStoreError::Invalid(
                "an asset's lifetime follows the source that references it; it takes no keep".into(),
            ));
        }
        // The file's id: the ASSET scope is content-addressed (the id IS the
        // sha256, supplied by the pre-build sync), every other scope mints a
        // uuid. A hash on a non-asset scope (or a missing/malformed hash on
        // the asset scope) is a caller bug, refused loud.
        let id = match (scope, content_hash) {
            (StorageScope::Asset, Some(hash)) => {
                if !weft_core::storage::is_content_hash(hash) {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "asset id must be a 64-hex sha256 content hash, got '{hash}'"
                    )));
                }
                hash.to_string()
            }
            (StorageScope::Asset, None) => {
                return Err(RuntimeStoreError::Invalid(
                    "asset uploads carry their content hash as the id".into(),
                ));
            }
            (_, Some(_)) => {
                return Err(RuntimeStoreError::Invalid(
                    "a content hash id only applies to the asset scope".into(),
                ));
            }
            (_, None) => uuid::Uuid::new_v4().to_string(),
        };
        let parsed = weft_core::storage::key::key_for_put(caller, scope, &id)
            .map_err(RuntimeStoreError::Denied)?;
        let key = parsed.to_key();
        let tenant = parsed.tenant.clone();
        let part_size = part_size_for(declared_size);
        // An asset starts on the default countdown the moment it completes:
        // an upload the sync never publishes (the build failed after the
        // transfer) would otherwise sit with no expiry and nothing to reap
        // it. Publishing (`set_asset_references`) clears the countdown for
        // every referenced asset, so a current source asset has no expiry.
        let ttl = match scope {
            StorageScope::Asset => Some(DEFAULT_KEEP_TTL_SECS),
            _ => keep.and_then(KeepTtl::secs),
        };

        // Before the reservation, with no lock held (both may call the
        // bucket): the tenant's abandoned uploads stop holding its quota, and
        // an expired file under this identity goes, so the identity is free
        // for this upload.
        self.reclaim_abandoned(&tenant).await.map_err(RuntimeStoreError::Other)?;
        if let Some(identity) = identity {
            self.reap_expired_identity(&key, identity).await.map_err(RuntimeStoreError::Other)?;
        }

        // Open the bucket multipart FIRST, with NO lock held and NO row yet, so we
        // have its id (the resume handle) in hand before we write the row. This is
        // what keeps "a committed pending row always carries its upload handle"
        // atomic: we never commit a row without its `upload_id`, and we never do
        // bucket I/O while holding the tenant lock (the gates + insert below do). If
        // anything after this point fails before the row commits, we abort this
        // multipart so the bucket is never left holding upload state no row points
        // at.
        let now = self.clock.now_unix();
        let upload_id = self
            .bucket
            .create_multipart(&object_key(&key))
            .await
            .context("runtime begin_upload: open multipart upload")
            .map_err(RuntimeStoreError::Other)?;

        // Gates + reservation, ATOMIC per tenant: the file-count gate, the
        // byte-quota check (known size), and the pending-row insert run in one
        // transaction under the tenant lock, so K concurrent begins serialize and
        // each sees the previous reservations. The row carries the file's metadata
        // (mime/filename/keep) AND its `upload_id` from the start; only the size and
        // expiry are finalized at complete. `keep` on a PENDING row does NOT spare
        // it from sweeps (they spare kept ACTIVE files only), so an abandoned
        // kept-file upload is still reaped. Any rejection/failure here aborts the
        // multipart opened above (`abort_reserve`), so a rejected begin leaves
        // nothing in the bucket.
        let reserve = async {
            let mut tx = self
                .pool
                .begin()
                .await
                .context("runtime begin_upload: begin reserve tx")
                .map_err(RuntimeStoreError::Other)?;
            lock_tenant_storage(&mut tx, &tenant)
                .await
                .map_err(RuntimeStoreError::Other)?;
            // A content-addressed (asset) key can legitimately collide: the
            // same content uploaded twice IS the same file. Check under the
            // lock: an ACTIVE row is this begin's idempotent success (nothing
            // to upload), a PENDING row is another upload of the same content
            // mid-flight (a loud conflict; rerun once it settles). Answered
            // structurally instead of via a raw PK violation.
            if content_hash.is_some() {
                let existing: Option<String> = sqlx::query_scalar(
                    "SELECT status FROM runtime_file WHERE key = $1",
                )
                .bind(&key)
                .fetch_optional(&mut *tx)
                .await
                .context("runtime begin_upload: check content-addressed key")
                .map_err(RuntimeStoreError::Other)?;
                match existing.as_deref() {
                    Some("active") => return Ok(Reserved::AlreadyActive(key.clone())),
                    // Part way up. The caller is handed the key and carries
                    // on with it; see `BeginUpload::Resume`. A row a sweep
                    // has already fenced for reaping ('reaping') is NOT
                    // resumable: its multipart is about to be aborted.
                    Some("pending") => {
                        let part_size: i64 = sqlx::query_scalar(
                            "SELECT part_size FROM runtime_file WHERE key = $1",
                        )
                        .bind(&key)
                        .fetch_one(&mut *tx)
                        .await
                        .context("runtime begin_upload: read the pending part size")
                        .map_err(RuntimeStoreError::Other)?;
                        return Ok(Reserved::Pending(key.clone(), part_size as u64));
                    }
                    Some(status) => {
                        return Err(RuntimeStoreError::Conflict(format!(
                            "asset '{key}' already exists ({status}); it is being cleared from \
                             the store, so this content's key frees itself shortly"
                        )));
                    }
                    None => {}
                }
            }
            // An identified begin: the same source stored twice in one
            // scope is one file. Checked under the lock, like the
            // content-addressed case: an ACTIVE row is this begin's
            // answer, a row still uploading (or being reaped) is a
            // conflict to retry once it settles. The scope is the key's
            // prefix (everything before the id), the same expression the
            // unique index is built on. A file that expired since it was
            // reaped above is not this begin's answer: it is on its way out.
            if let Some(identity) = identity {
                let existing: Option<(String, String)> = sqlx::query_as(&format!(
                    "SELECT key, CASE WHEN status = 'active' AND NOT {live} THEN 'expired' ELSE status END \
                     FROM runtime_file \
                     WHERE regexp_replace(key, '/[^/]+$', '') = $1 AND identity = $2",
                    live = unexpired("$3"),
                ))
                .bind(scope_prefix(&key))
                .bind(identity)
                .bind(now)
                .fetch_optional(&mut *tx)
                .await
                .context("runtime begin_upload: check identity")
                .map_err(RuntimeStoreError::Other)?;
                match existing {
                    Some((existing_key, status)) if status == "active" => {
                        return Ok(Reserved::AlreadyActive(existing_key));
                    }
                    Some((existing_key, status)) => {
                        return Err(RuntimeStoreError::Conflict(format!(
                            "a file with identity '{identity}' is already {status} in this scope \
                             ('{existing_key}'); retry once it settles"
                        )));
                    }
                    None => {}
                }
            }
            let count = count_files(&mut *tx, &tenant, now)
                .await
                .context("runtime begin_upload: count tenant files")
                .map_err(RuntimeStoreError::Other)?;
            let caps = entitlements
                .caps(&tenant)
                .await
                .context("runtime begin_upload: resolve tenant caps")
                .map_err(RuntimeStoreError::Other)?;
            if caps.file_count_would_exceed(count) {
                return Err(RuntimeStoreError::QuotaExceeded(format!(
                    "tenant '{tenant}' is at its file cap ({} files); delete files or raise the cap",
                    caps.file_cap
                )));
            }
            if let Some(declared) = declared_size {
                if self.account_would_exceed(&mut tx, entitlements, &tenant, declared)
                    .await
                    .map_err(RuntimeStoreError::Other)?
                {
                    return Err(RuntimeStoreError::QuotaExceeded(format!(
                        "tenant '{tenant}' would exceed its storage quota ({} bytes) by storing \
                         {declared} more",
                        caps.disk_bytes_cap
                    )));
                }
            }
            sqlx::query(
                "INSERT INTO runtime_file \
                 (key, tenant_id, mime_type, filename, size_bytes, status, keep, expires_at_unix, \
                  keep_ttl_secs, created_at_unix, upload_id, part_size, declared_size, \
                  reserved_bytes, progressed_at_unix, identity) \
                 VALUES ($1, $2, $3, $4, 0, 'pending', $5, NULL, $6, $7, $8, $9, $10, $11, $7, $12)",
            )
            .bind(&key)
            .bind(&tenant)
            .bind(mime)
            .bind(filename)
            // The flag is the end-of-run sweep's exemption, which only a
            // run file needs; elsewhere a lifetime is just a ttl.
            .bind(keep.is_some() && matches!(scope, StorageScope::Run))
            .bind(ttl.map(|s| s as i64))
            .bind(now)
            .bind(&upload_id)
            .bind(part_size as i64)
            .bind(declared_size.map(|s| s as i64))
            .bind(declared_size.unwrap_or(0) as i64)
            .bind(identity)
            .execute(&mut *tx)
            .await
            .context("runtime begin_upload: reserve pending row")
            .map_err(RuntimeStoreError::Other)?;
            tx.commit()
                .await
                .context("runtime begin_upload: commit reservation")
                .map_err(RuntimeStoreError::Other)?;
            Ok(Reserved::Fresh)
        }
        .await;

        // Whenever no fresh row committed (rejection, failure, or an
        // already-active asset), abort the multipart we opened so the bucket
        // is not left holding an upload no row references (the very invariant
        // this ordering exists to hold). Best-effort: on abort failure the
        // bucket lifecycle rule is the backstop.
        let abort_opened_multipart = || async {
            if let Err(ab) = self.bucket.abort_multipart(&object_key(&key), &upload_id).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %key, error = %ab,
                    "failed to abort multipart after an uncommitted begin; \
                     the bucket lifecycle rule will reap it"
                );
            }
        };
        match reserve {
            Ok(Reserved::Fresh) => Ok(BeginUpload::Ready { key, part_size }),
            Ok(Reserved::AlreadyActive(existing)) => {
                abort_opened_multipart().await;
                Ok(BeginUpload::AlreadyStored { key: existing })
            }
            // The multipart THIS begin opened is aborted: the caller is
            // going to carry on with the one already in flight, which has
            // its own upload id and its own part rows.
            Ok(Reserved::Pending(existing, part_size)) => {
                abort_opened_multipart().await;
                Ok(BeginUpload::Resume { key: existing, part_size })
            }
            Err(e) => {
                abort_opened_multipart().await;
                Err(e)
            }
        }
    }

    /// Begin overwriting the ACTIVE file `replaced` with new bytes: same
    /// key, same scope, name, type and lifetime; only the content and its
    /// size change. The upload is an ordinary pending one under a key of
    /// its own (so quota, resume, abort and every sweep treat it exactly
    /// like any upload), except that its multipart writes to the replaced
    /// file's OBJECT. The bucket swaps that object in whole when the
    /// multipart completes, so a reader sees the old bytes or the new ones,
    /// never a mix; completion then folds the new size into the replaced
    /// file's row ([`Self::fold_replacement`]).
    ///
    /// The declared size is charged up front like any upload, so for the
    /// length of the upload the tenant pays for both versions. There is no
    /// file-count gate: a replacement adds no file.
    ///
    /// One replacement of a file is in flight at a time (both write onto
    /// the file's own object): a begin while another is in flight is a
    /// [`RuntimeStoreError::Conflict`], to retry once it ends. With
    /// `expected_version` (an edit, which read the file first), a file
    /// whose version moved on since is [`RuntimeStoreError::Stale`]: no
    /// other replacement can start until this one ends, so the version
    /// checked here is the one the new bytes replace.
    pub async fn begin_replace(
        &self,
        caller: &CallerAuth,
        replaced: &str,
        declared_size: Option<u64>,
        expected_version: Option<u64>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<(String, u64)> {
        let target = Self::wall_key(caller, replaced)?;
        if matches!(target.scope, weft_core::storage::key::KeyScope::Asset) {
            return Err(RuntimeStoreError::Denied(
                "the asset scope is managed by the pre-build asset sync; an asset is never replaced \
                 by node code"
                    .into(),
            ));
        }
        let parsed = ParsedKey {
            tenant: target.tenant.clone(),
            scope: target.scope.clone(),
            id: uuid::Uuid::new_v4().to_string(),
        };
        let key = parsed.to_key();
        let replaced = target.to_key();
        let tenant = parsed.tenant.clone();
        let part_size = part_size_for(declared_size);
        let now = self.clock.now_unix();
        // Same ordering as `begin_upload`: the multipart first, with no
        // lock and no row, so a committed pending row always carries its
        // upload handle; anything that stops the row committing aborts it.
        let upload_id = self
            .bucket
            .create_multipart(&object_key(&replaced))
            .await
            .context("runtime begin_replace: open multipart upload")
            .map_err(RuntimeStoreError::Other)?;
        let reserve = async {
            let mut tx = self
                .pool
                .begin()
                .await
                .context("runtime begin_replace: begin reserve tx")
                .map_err(RuntimeStoreError::Other)?;
            lock_tenant_storage(&mut tx, &tenant).await.map_err(RuntimeStoreError::Other)?;
            let existing: Option<(String, String, i64)> = sqlx::query_as(&format!(
                "SELECT mime_type, filename, version FROM runtime_file WHERE key = $1 AND status = 'active' AND {live}",
                live = unexpired("$2"),
            ))
            .bind(&replaced)
            .bind(now)
            .fetch_optional(&mut *tx)
            .await
            .context("runtime begin_replace: read the replaced file")
            .map_err(RuntimeStoreError::Other)?;
            let Some((mime, filename, version)) = existing else {
                return Err(RuntimeStoreError::NotFound(replaced.clone()));
            };
            let in_flight: Option<String> =
                sqlx::query_scalar("SELECT key FROM runtime_file WHERE replaces = $1")
                    .bind(&replaced)
                    .fetch_optional(&mut *tx)
                    .await
                    .context("runtime begin_replace: look for a replacement in flight")
                    .map_err(RuntimeStoreError::Other)?;
            if in_flight.is_some() {
                return Err(RuntimeStoreError::Conflict(format!(
                    "'{replaced}' is being changed by another write; retry once it ends"
                )));
            }
            if let Some(expected) = expected_version {
                if expected as i64 != version {
                    return Err(RuntimeStoreError::Stale(format!(
                        "'{replaced}' is at version {version}, not the version {expected} this \
                         change was made from"
                    )));
                }
            }
            if let Some(declared) = declared_size {
                if self.account_would_exceed(&mut tx, entitlements, &tenant, declared)
                    .await
                    .map_err(RuntimeStoreError::Other)?
                {
                    let cap = entitlements
                        .caps(&tenant)
                        .await
                        .map(|c| c.disk_bytes_cap.to_string())
                        .unwrap_or_else(|_| "?".to_string());
                    return Err(RuntimeStoreError::QuotaExceeded(format!(
                        "tenant '{tenant}' would exceed its storage quota ({cap} bytes) by \
                         storing {declared} more while '{replaced}' is replaced; the old \
                         content counts until the new one lands"
                    )));
                }
            }
            sqlx::query(
                "INSERT INTO runtime_file \
                 (key, tenant_id, mime_type, filename, size_bytes, status, keep, expires_at_unix, \
                  keep_ttl_secs, created_at_unix, upload_id, part_size, declared_size, \
                  reserved_bytes, progressed_at_unix, identity, replaces) \
                 VALUES ($1, $2, $3, $4, 0, 'pending', FALSE, NULL, NULL, $5, $6, $7, $8, $9, $5, NULL, $10)",
            )
            .bind(&key)
            .bind(&tenant)
            .bind(&mime)
            .bind(&filename)
            .bind(now)
            .bind(&upload_id)
            .bind(part_size as i64)
            .bind(declared_size.map(|s| s as i64))
            .bind(declared_size.unwrap_or(0) as i64)
            .bind(&replaced)
            .execute(&mut *tx)
            .await
            .context("runtime begin_replace: reserve pending row")
            .map_err(RuntimeStoreError::Other)?;
            tx.commit()
                .await
                .context("runtime begin_replace: commit reservation")
                .map_err(RuntimeStoreError::Other)
        }
        .await;
        if let Err(e) = reserve {
            if let Err(ab) = self.bucket.abort_multipart(&object_key(&replaced), &upload_id).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %replaced, error = %ab,
                    "failed to abort multipart after an uncommitted replace; \
                     the bucket lifecycle rule will reap it"
                );
            }
            return Err(e);
        }
        Ok((key, part_size))
    }

    /// ASSEMBLY: create a stored file by concatenating EXISTING objects of
    /// this store, the bytes never leaving the process that runs it (bucket
    /// -> this process -> bucket, one part-sized buffer at a time). The same
    /// ledger lifecycle as a caller-driven upload (begin reservation, part
    /// rows, complete), so quota, sweeps, and listings see it identically;
    /// only the byte transport differs (direct `upload_part`, no presigning
    /// to an external caller).
    ///
    /// `sources` are `(object key, expected size)` pairs, concatenated in
    /// order; `spec.declared_size` must equal their sum (the reservation is
    /// exact) and a fetched object whose size disagrees aborts loudly. Any
    /// failure aborts the upload, freeing the reservation.
    pub async fn assemble(
        &self,
        caller: &CallerAuth,
        spec: &UploadSpec<'_>,
        sources: &[(String, u64)],
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<StoredFileMeta> {
        let total: u64 = sources.iter().map(|(_, s)| *s).sum();
        if spec.declared_size != Some(total) {
            return Err(RuntimeStoreError::Invalid(format!(
                "assemble: declared_size {:?} must equal the sources' total {total}",
                spec.declared_size
            )));
        }
        let (key, part_size) = match self.begin_upload(caller, spec, entitlements).await? {
            BeginUpload::Ready { key, part_size } => (key, part_size),
            // The content is already stored ACTIVE: assembling it again would
            // produce the same file, so the existing file's meta IS this
            // assembly's result (idempotent, like the begin itself).
            BeginUpload::AlreadyStored { key } => {
                let parsed = weft_core::storage::key::parse_key(&key)
                    .map_err(RuntimeStoreError::Denied)?;
                return self.meta(&parsed).await;
            }
            // An assembly is not resumable: it writes parts DIRECTLY (no
            // presigned URLs) from sources it walks in order, and the
            // in-flight upload under this key belongs to whoever opened it,
            // with its own multipart handle. Refused loudly rather than
            // joined; the content is addressed by its hash, so the other
            // upload is putting the same bytes there and this call's answer
            // arrives on the next attempt.
            BeginUpload::Resume { key, .. } => {
                return Err(RuntimeStoreError::Conflict(format!(
                    "'{key}' is already part way up; wait for that upload to finish or be \
                     cleared, then assemble again"
                )));
            }
        };
        let assembled = async {
            // The multipart handle, committed with the pending row by begin.
            let mut tx = self
                .pool
                .begin()
                .await
                .context("assemble: read pending row")
                .map_err(RuntimeStoreError::Other)?;
            let upload_id = Self::pending_row(&mut tx, &key)
                .await
                .map_err(RuntimeStoreError::Other)?
                .and_then(|p| p.upload_id)
                .ok_or_else(|| {
                    RuntimeStoreError::Other(anyhow::anyhow!(
                        "assemble: begin committed no upload handle for '{key}'"
                    ))
                })?;
            drop(tx);

            // Concatenate sources into exactly part_size-d parts (only the
            // final one may be smaller; `reserve_parts` validates the
            // slicing), each reserved in the ledger and uploaded directly.
            // Each source is size-checked up front and then read in
            // part-sized ranges, so memory stays bounded at ~one part
            // regardless of source-object and asset size.
            let mut buf: Vec<u8> = Vec::with_capacity(part_size as usize);
            // Parts are numbered from 1, in the order they are written.
            let mut part_number: i32 = 1;
            for (src, expected) in sources {
                let actual = self
                    .bucket
                    .size(src)
                    .await
                    .with_context(|| format!("assemble: stat source {src}"))
                    .map_err(RuntimeStoreError::Other)?
                    .ok_or_else(|| {
                        RuntimeStoreError::Invalid(format!(
                            "assemble: source object '{src}' does not exist"
                        ))
                    })?;
                if actual != *expected {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "assemble: source '{src}' is {actual} bytes, expected {expected}"
                    )));
                }
                let mut off: u64 = 0;
                while off < *expected {
                    let end = (*expected).min(off + part_size);
                    let bytes = self
                        .bucket
                        .get_range(src, off, end)
                        .await
                        .with_context(|| format!("assemble: read source {src} [{off}..{end})"))
                        .map_err(RuntimeStoreError::Other)?
                        .ok_or_else(|| {
                            RuntimeStoreError::Invalid(format!(
                                "assemble: source object '{src}' vanished mid-read"
                            ))
                        })?;
                    if bytes.is_empty() {
                        return Err(RuntimeStoreError::Invalid(format!(
                            "assemble: source '{src}' ended at {off} bytes, expected {expected}"
                        )));
                    }
                    off += bytes.len() as u64;
                    buf.extend_from_slice(&bytes);
                    while buf.len() as u64 >= part_size {
                        let chunk: Vec<u8> = buf.drain(..part_size as usize).collect();
                        self.assemble_part(caller, &key, &upload_id, part_number, chunk, entitlements)
                            .await?;
                        part_number += 1;
                    }
                }
            }
            if !buf.is_empty() {
                let chunk = std::mem::take(&mut buf);
                self.assemble_part(caller, &key, &upload_id, part_number, chunk, entitlements)
                    .await?;
            }
            self.complete_upload(caller, &key).await
        }
        .await;
        match assembled {
            Ok(meta) => Ok(meta),
            Err(e) => {
                // Free the reservation; nothing must linger on a failed assembly.
                if let Err(ab) = self.abort_upload(caller, &key).await {
                    tracing::error!(
                        target: "weft_broker::runtime_store",
                        key = %key, error = %ab,
                        "failed to abort assembly after error; the tenant's next upload reaps it, or \
                         drives it if a completion had claimed it"
                    );
                }
                Err(e)
            }
        }
    }

    /// One assembled part: ledger reservation (validates the slicing exactly
    /// like a caller-driven part) + direct upload + etag record.
    async fn assemble_part(
        &self,
        caller: &CallerAuth,
        key: &str,
        upload_id: &str,
        part_number: i32,
        chunk: Vec<u8>,
        entitlements: &dyn EntitlementSource,
    ) -> StoreResult<()> {
        let size = chunk.len() as u64;
        // The assembler counts its own parts: it is the only writer of this
        // key and it walks the sources in order, so the part it is on is the
        // part it says it is on.
        self.reserve_parts(
            caller,
            key,
            &[PartAsk { part_number, size_bytes: size }],
            entitlements,
            PresignAudience::Internal,
        )
        .await?;
        let etag = self
            .bucket
            .upload_part(&object_key(key), upload_id, part_number, bytes::Bytes::from(chunk))
            .await
            .context("assemble: direct part upload")
            .map_err(RuntimeStoreError::Other)?;
        self.record_part(caller, key, part_number, &etag).await
    }

    /// Reserve + presign the parts the caller NAMES. Each URL is signed with
    /// its part's exact size, so the bucket enforces the reservation
    /// byte-for-byte.
    ///
    /// The caller names the part number, and a part number IS a position:
    /// part `n` carries the file's bytes from `(n - 1) * part_size`. So
    /// reserving is idempotent, and that is the point. Asking for "the next
    /// part" meant two uploaders of the same key (the key is a content hash,
    /// so they hold identical bytes) each extended the reservation, together
    /// reserved more parts than the file has, and both then failed on a
    /// total that no longer added up, leaving the upload unfinishable until
    /// the hourly sweep removed it. Naming the part makes the second asker
    /// get the same part as the first, write the same bytes to it, and
    /// finish.
    ///
    /// A KNOWN-size upload was charged in full at begin, so its parts must
    /// slice exactly to the declared total (no re-charge here). An unknown-
    /// length stream is charged part-by-part under the tenant lock; the
    /// reservation that would cross the cap ABORTS the whole upload (frees
    /// everything) and returns QuotaExceeded, so a stream can never inch past
    /// the cap.
    pub async fn reserve_parts(
        &self,
        caller: &CallerAuth,
        key: &str,
        asks: &[PartAsk],
        entitlements: &dyn EntitlementSource,
        audience: PresignAudience,
    ) -> StoreResult<Vec<PresignedPart>> {
        Self::wall_key(caller, key)?;
        if asks.is_empty() {
            return Err(RuntimeStoreError::Invalid("no parts requested".into()));
        }
        let mut seen: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
        for ask in asks {
            if ask.part_number < 1 || ask.part_number as u64 > MAX_PARTS {
                return Err(RuntimeStoreError::Invalid(format!(
                    "part number {} is out of range: parts are numbered 1 to {MAX_PARTS}",
                    ask.part_number
                )));
            }
            if !seen.insert(ask.part_number) {
                return Err(RuntimeStoreError::Invalid(format!(
                    "part {} is named twice in one reservation",
                    ask.part_number
                )));
            }
        }
        let now = self.clock.now_unix();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime reserve_parts: begin tx")
            .map_err(RuntimeStoreError::Other)?;
        // The row is locked before the tenant: a completion claim (which
        // takes no tenant lock) then waits on this short transaction or
        // this one sees its mark, and no bucket call ever sits under it.
        let pending = Self::pending_row_locked(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| {
                RuntimeStoreError::Invalid(format!(
                    "no in-flight upload for key '{key}'; begin an upload first"
                ))
            })?;
        pending.refuse_if_completing(key)?;
        lock_tenant_storage(&mut tx, &pending.tenant_id)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let upload_id = pending.upload_id.clone().ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "pending row for '{key}' unexpectedly has no upload id (a committed \
                 pending row always carries one); abort this upload and begin again"
            ))
        })?;
        let part_size = pending.part_size as u64;

        // A part must be at least 1 byte: a multipart part can never be zero
        // (S3 rejects it), and an empty object uploads ZERO parts and is
        // written directly at complete.
        for ask in asks {
            if ask.size_bytes == 0 {
                return Err(RuntimeStoreError::Invalid(
                    "a part must be at least 1 byte; an empty object uploads zero parts".into(),
                ));
            }
            if ask.size_bytes > part_size {
                return Err(RuntimeStoreError::Invalid(format!(
                    "part {} is {} bytes, more than this upload's {part_size}-byte part size",
                    ask.part_number, ask.size_bytes
                )));
            }
        }
        // Which of the named parts are already reserved. A re-ask is the
        // whole point of naming parts, so it must not be charged twice: a
        // stream is charged per part as it goes, and charging a part the
        // caller already paid for would inch the tenant's usage up on every
        // retry.
        let already: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT part_number, size_bytes FROM runtime_file_part WHERE key = $1 ORDER BY part_number",
        )
        .bind(key)
        .fetch_all(&mut *tx)
        .await
        .context("runtime reserve_parts: read the parts already reserved")
        .map_err(RuntimeStoreError::Other)?;
        for ask in asks {
            if let Some((_, size)) = already.iter().find(|(number, _)| *number == ask.part_number) {
                if *size as u64 != ask.size_bytes {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "part {} was reserved as {size} bytes; its size cannot change to {}", ask.part_number, ask.size_bytes
                    )));
                }
            }
        }
        let incoming: u64 =
            asks.iter().filter(|a| !already.iter().any(|(number, _)| *number == a.part_number)).map(|a| a.size_bytes).sum();
        if let Some(declared) = pending.declared_size {
            // Known size: the layout is arithmetic, so each named part has
            // exactly one correct size and the reservation can be checked
            // against it without reading what else is reserved. Part `n`
            // starts at `(n - 1) * part_size`; it is a whole part unless the
            // file ends inside it.
            let declared = declared as u64;
            for ask in asks {
                let offset = (ask.part_number as u64 - 1) * part_size;
                if offset >= declared {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "part {} starts at byte {offset}, past the end of the declared \
                         {declared}-byte total",
                        ask.part_number
                    )));
                }
                let expected = part_size.min(declared - offset);
                if ask.size_bytes != expected {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "part {} starts at byte {offset} and must be {expected} bytes to slice \
                         the declared {declared}-byte total; got {}",
                        ask.part_number, ask.size_bytes
                    )));
                }
            }
        } else {
            let mut layout: std::collections::BTreeMap<_, _> = already.iter().map(|(number, size)| (*number, *size as u64)).collect();
            layout.extend(asks.iter().map(|ask| (ask.part_number, ask.size_bytes)));
            validate_stream_layout(&layout, part_size)?;
            // Stream: charge these parts now, under the lock. The account check
            // sums THIS plane's charged bytes (already including this upload's
            // reserved_bytes) plus the other plane's, both read on this tx.
            if self.account_would_exceed(&mut tx, entitlements, &pending.tenant_id, incoming)
                .await
                .map_err(RuntimeStoreError::Other)?
            {
                // The stream cannot fit: abort the WHOLE upload now (delete the
                // row in-tx so the freed charge is serialized; cascade drops the
                // parts), then abort the bucket's multipart upload. The caller
                // gets a loud quota error; nothing is left to clean.
                sqlx::query(&delete_upload_row("AND status = 'pending'"))
                    .bind(key)
                    .execute(&mut *tx)
                    .await
                    .context("runtime reserve_parts: delete over-quota upload")
                    .map_err(RuntimeStoreError::Other)?;
                tx.commit()
                    .await
                    .context("runtime reserve_parts: commit over-quota abort")
                    .map_err(RuntimeStoreError::Other)?;
                if let Err(abort) =
                    self.bucket.abort_multipart(&pending.object_key(key), &upload_id).await
                {
                    tracing::error!(
                        target: "weft_broker::runtime_store",
                        key = %key, error = %abort,
                        "failed to abort over-quota multipart upload; \
                         the bucket lifecycle rule will reap it"
                    );
                }
                // Best-effort cap readout for the message; the quota verdict
                // above already stands regardless.
                let cap_display = entitlements
                    .caps(&pending.tenant_id)
                    .await
                    .map(|c| c.disk_bytes_cap.to_string())
                    .unwrap_or_else(|_| "?".to_string());
                return Err(RuntimeStoreError::QuotaExceeded(format!(
                    "tenant '{}' would exceed its storage quota ({cap_display} bytes) by \
                     streaming {incoming} more; the upload was aborted",
                    pending.tenant_id
                )));
            }
            sqlx::query(
                "UPDATE runtime_file SET reserved_bytes = reserved_bytes + $2 \
                 WHERE key = $1 AND status = 'pending'",
            )
            .bind(key)
            .bind(incoming as i64)
            .execute(&mut *tx)
            .await
            .context("runtime reserve_parts: charge stream parts")
            .map_err(RuntimeStoreError::Other)?;
        }
        let mut reserved = Vec::with_capacity(asks.len());
        for ask in asks {
            // Idempotent by part number: a part already reserved keeps the
            // size it was reserved with, and this hands back a fresh URL for
            // it. That is what lets two uploaders of identical content, or
            // one uploader retrying, name the same part and agree.
            sqlx::query(
                "INSERT INTO runtime_file_part (key, part_number, size_bytes, etag) \
                 VALUES ($1, $2, $3, NULL) \
                 ON CONFLICT (key, part_number) DO NOTHING",
            )
            .bind(key)
            .bind(ask.part_number)
            .bind(ask.size_bytes as i64)
            .execute(&mut *tx)
            .await
            .context("runtime reserve_parts: insert part row")
            .map_err(RuntimeStoreError::Other)?;
            reserved.push((ask.part_number, ask.size_bytes));
        }
        // A reservation is progress: refresh the abandoned-pending clock. The
        // row is locked, so a sweep's fence waits for this commit and its
        // WHERE re-check then sees the bumped clock and spares the row; the
        // gate on 'pending' stays as an assertion of that.
        let alive = sqlx::query("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1 AND status = 'pending'")
            .bind(key)
            .bind(now)
            .execute(&mut *tx)
            .await
            .context("runtime reserve_parts: bump progress clock")
            .map_err(RuntimeStoreError::Other)?;
        if alive.rows_affected() == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "upload '{key}' was swept mid-flight (idle past the reserve grace); \
                 begin the upload again"
            )));
        }
        tx.commit()
            .await
            .context("runtime reserve_parts: commit reservations")
            .map_err(RuntimeStoreError::Other)?;

        // Presign AFTER the commit (no bucket I/O under the tenant lock). If a
        // presign fails here the reservations stand: the caller resumes (which
        // re-presigns exactly these parts), so nothing is stranded.
        let (offsets, _sizes, _missing) = self
            .part_offsets(key, &pending)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let mut parts = Vec::with_capacity(reserved.len());
        for (part_number, size) in reserved {
            let offset_bytes = *offsets.get(&part_number).ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "part {part_number} of '{key}' was just reserved and has no row to \
                     place it in the file; abort this upload and begin again"
                ))
            })?;
            let url = self
                .bucket
                .presign_part(
                    &pending.object_key(key),
                    &upload_id,
                    part_number,
                    size,
                    audience,
                    DEFAULT_PRESIGN_TTL_SECS,
                )
                .await
                .context("runtime reserve_parts: presign part (the reservation stands; resume the upload to re-presign)")
                .map_err(RuntimeStoreError::Other)?;
            parts.push(PresignedPart { part_number, size_bytes: size, offset_bytes, url });
        }
        Ok(parts)
    }

    /// Record a landed part's etag (the bucket's response header, verbatim).
    /// The part's size comes from OUR reservation, never from the caller.
    /// Idempotent: re-reporting a part overwrites its etag (a re-uploaded part
    /// number overwrites in the bucket too, so the latest etag is the truth).
    pub async fn record_part(
        &self,
        caller: &CallerAuth,
        key: &str,
        part_number: i32,
        etag: &str,
    ) -> StoreResult<()> {
        Self::wall_key(caller, key)?;
        if etag.is_empty() {
            return Err(RuntimeStoreError::Invalid("empty etag".into()));
        }
        // Under the upload row's lock: a completion claim reads the etags,
        // so a report must land before it or be refused after it, never
        // change an etag the bucket is being handed.
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime record_part: begin tx")
            .map_err(RuntimeStoreError::Other)?;
        let pending = Self::pending_row_locked(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| {
                RuntimeStoreError::Invalid(format!(
                    "upload '{key}' is no longer in flight (completed, aborted, or swept idle \
                     past the reserve grace); begin the upload again"
                ))
            })?;
        pending.refuse_if_completing(key)?;
        let updated = sqlx::query(
            "UPDATE runtime_file_part SET etag = $3 WHERE key = $1 AND part_number = $2",
        )
        .bind(key)
        .bind(part_number)
        .bind(etag)
        .execute(&mut *tx)
        .await
        .context("runtime record_part")
        .map_err(RuntimeStoreError::Other)?;
        if updated.rows_affected() == 0 {
            return Err(RuntimeStoreError::Invalid(format!(
                "part {part_number} of '{key}' was never reserved"
            )));
        }
        // A landed part is progress: refresh the abandoned-pending clock.
        sqlx::query("UPDATE runtime_file SET progressed_at_unix = $2 WHERE key = $1")
            .bind(key)
            .bind(self.clock.now_unix())
            .execute(&mut *tx)
            .await
            .context("runtime record_part: bump progress clock")
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime record_part: commit")
            .map_err(RuntimeStoreError::Other)?;
        Ok(())
    }

    /// Finalize an upload: every reserved part must have been reported done
    /// (and, for a known size, the parts must sum to the declared total).
    /// Idempotent on retry: a key that already completed returns the file
    /// it became ([`Self::completed_meta`]).
    ///
    /// Three steps, none holding a lock across a bucket call:
    /// 1. [`Self::claim_completion`] checks the parts and marks the row
    ///    'completing' in a short transaction. From then on an abort, a
    ///    part reservation or report, a delete of the replaced file and
    ///    every sweep see the mark and leave the upload alone.
    /// 2. [`Self::drive_completion`] has the bucket assemble the object.
    /// 3. [`Self::finish_completion`] folds the result into the rows.
    ///
    /// A complete that finds the row already 'completing' (a retry, or a
    /// second caller racing the first) drives the same completion: the
    /// drive is safe to run twice at once. A completion that fails without
    /// a verdict (the bucket unreachable, or refusing) stays 'completing'
    /// and answers [`RuntimeStoreError::Completing`]; the row never goes
    /// back to 'pending', since another drive may be landing its bytes. A
    /// crashed, dropped or refused completion is driven by the tenant's
    /// next upload or usage read once the claim is [`CLAIM_LEASE_SECS`]
    /// old ([`Self::reclaim_abandoned`]), and that drive gives the verdict.
    pub async fn complete_upload(
        &self,
        caller: &CallerAuth,
        key: &str,
    ) -> StoreResult<StoredFileMeta> {
        Self::wall_key(caller, key)?;
        self.claim_completion(key).await?;
        match self.drive_completion(key, false).await {
            // Past the claim, a failure that is not a verdict leaves the
            // upload 'completing': the caller asks again (or the sweep
            // finishes it), so it hears exactly that.
            Err(RuntimeStoreError::Other(e)) => {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %key, error = format!("{e:#}"),
                    "a claimed completion did not land yet; it stays 'completing'"
                );
                Err(RuntimeStoreError::Completing(format!(
                    "upload '{key}' is completing but has not landed yet; call complete again"
                )))
            }
            other => other,
        }
    }

    /// Step 1 of a completion: check every reserved part landed (and
    /// slices the declared total), then mark the row 'completing' with the
    /// claim time in `progressed_at_unix`. A replacement also locks the
    /// file it replaces, which must still be active: a delete or sweep of
    /// that file locks it too and then refuses on the mark, so the two
    /// cannot cross. A row already 'completing' is left as it is; a key
    /// with no in-flight row is answered by the drive (a retry or unknown).
    async fn claim_completion(&self, key: &str) -> StoreResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime complete_upload: begin claim")
            .map_err(RuntimeStoreError::Other)?;
        let Some(pending) = Self::pending_row_locked(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?
        else {
            return Ok(());
        };
        if pending.status == "completing" {
            return Ok(());
        }
        if let Some(replaced) = &pending.replaces {
            let status: Option<String> =
                sqlx::query_scalar("SELECT status FROM runtime_file WHERE key = $1 FOR UPDATE")
                    .bind(replaced)
                    .fetch_optional(&mut *tx)
                    .await
                    .context("runtime complete_upload: lock the replaced file")
                    .map_err(RuntimeStoreError::Other)?;
            if status.as_deref() != Some("active") {
                // The file was deleted or expired while the new bytes were
                // on their way: the replacement has nothing left to land
                // on, so it is ended here like an abort.
                sqlx::query(&fence_sql(""))
                    .bind(key)
                    .bind(self.clock.now_unix())
                    .execute(&mut *tx)
                    .await
                    .context("runtime complete_upload: fence an orphaned replacement")
                    .map_err(RuntimeStoreError::Other)?;
                tx.commit()
                    .await
                    .context("runtime complete_upload: commit orphaned replacement fence")
                    .map_err(RuntimeStoreError::Other)?;
                self.reap_fenced(&pending.sweep_entry(key)).await.map_err(RuntimeStoreError::Other)?;
                return Err(RuntimeStoreError::NotFound(format!(
                    "'{replaced}' was removed while its replacement '{key}' was uploading; the \
                     replacement was cancelled"
                )));
            }
        }
        let parts = Self::completion_parts(&mut *tx, key).await?;
        if parts.is_empty() {
            // A declared NON-zero size with no parts is an incomplete upload,
            // not an empty file.
            if let Some(declared) = pending.declared_size.filter(|d| *d > 0) {
                return Err(RuntimeStoreError::Invalid(format!(
                    "upload '{key}' declared {declared} bytes but no parts were uploaded; \
                     resume the upload to finish it, or abort it"
                )));
            }
        } else {
            let missing: Vec<i32> =
                parts.iter().filter(|(_, etag, _)| etag.is_none()).map(|(n, _, _)| *n).collect();
            if !missing.is_empty() {
                return Err(RuntimeStoreError::Invalid(format!(
                    "upload '{key}' is incomplete: parts {missing:?} were never uploaded; \
                     resume the upload to finish them, or abort it"
                )));
            }
            let total: u64 = parts.iter().map(|(_, _, s)| *s as u64).sum();
            if let Some(declared) = pending.declared_size {
                if total != declared as u64 {
                    return Err(RuntimeStoreError::Invalid(format!(
                        "upload '{key}' reserved {total} bytes of a declared {declared}; \
                         upload the remaining parts before completing"
                    )));
                }
            }
        }
        sqlx::query(
            "UPDATE runtime_file SET status = 'completing', progressed_at_unix = $2 \
             WHERE key = $1 AND status = 'pending'",
        )
        .bind(key)
        .bind(self.clock.now_unix())
        .execute(&mut *tx)
        .await
        .context("runtime complete_upload: mark completing")
        .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime complete_upload: commit claim")
            .map_err(RuntimeStoreError::Other)?;
        Ok(())
    }

    /// An upload's part rows, ascending: (number, etag once landed, size).
    async fn completion_parts<'e>(
        executor: impl sqlx::PgExecutor<'e>,
        key: &str,
    ) -> StoreResult<Vec<(i32, Option<String>, i64)>> {
        sqlx::query_as(
            "SELECT part_number, etag, size_bytes FROM runtime_file_part \
             WHERE key = $1 ORDER BY part_number",
        )
        .bind(key)
        .fetch_all(executor)
        .await
        .context("runtime complete_upload: read parts")
        .map_err(RuntimeStoreError::Other)
    }

    /// Steps 2 and 3 of a completion, for a row marked 'completing': get
    /// the object assembled ([`Self::bucket_completion`]), then fold it into
    /// the rows ([`Self::finish_completion`]). Safe to run while another
    /// drive of the same upload runs: both see the multipart open, the
    /// bucket completes it once, and the other drive's completion fails
    /// with `NoSuchUpload`, after which it finds the upload gone and reads
    /// the object; the rows fold once. With no 'completing' row, answers as
    /// a retry of a finished completion.
    ///
    /// `recovering` is the drive of a claim past its lease
    /// ([`Self::reclaim_abandoned`]): a bucket refusal then is final (the
    /// claim's own drive already had its chance), where a live drive's
    /// refusal is left for that one.
    async fn drive_completion(&self, key: &str, recovering: bool) -> StoreResult<StoredFileMeta> {
        let row = sqlx::query_as::<_, PendingUpload>(&format!(
            "{PENDING_ROW_SELECT_ANY} AND status = 'completing'"
        ))
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .context("runtime complete_upload: read the completing row")
        .map_err(RuntimeStoreError::Other)?;
        let Some(pending) = row else {
            return Self::completed_meta(&self.pool, key).await?.ok_or_else(|| {
                RuntimeStoreError::NotFound(format!(
                    "no upload in flight for key '{key}' (never begun, aborted, swept, or a \
                     replacement a later one has since overtaken)"
                ))
            });
        };
        // The parts cannot change now: reserving and reporting refuse a
        // 'completing' row.
        let parts = Self::completion_parts(&self.pool, key).await?;
        let total: u64 = parts.iter().map(|(_, _, s)| *s as u64).sum();
        match self.bucket_completion(key, &pending, &parts, total, recovering).await? {
            Bucket::Assembled(actual) => self.finish_completion(key, &pending, total, actual).await,
            Bucket::Lost { why, overwritten } => self.lose_completion(key, &pending, &why, overwritten).await,
        }
    }

    /// Step 2: what the bucket made of the upload.
    ///
    /// The bucket is asked first whether the multipart is still open
    /// (`multipart_exists`), so a completion is never sent for an upload
    /// already completed. Open: complete it. Gone: since nothing aborts an
    /// upload marked 'completing', a drive completed it, and the object's
    /// current size is read; an object of any other size means it was
    /// aborted outside weft (the bucket's incomplete-upload lifecycle
    /// rule), or completed wrong, and that is a [`Bucket::Lost`] verdict.
    /// A completion the bucket refuses while the upload stays open is an
    /// error (the row stays 'completing') unless `recovering`, where it is
    /// final. An error from the bucket itself is never a verdict.
    async fn bucket_completion(
        &self,
        key: &str,
        pending: &PendingUpload,
        parts: &[(i32, Option<String>, i64)],
        total: u64,
        recovering: bool,
    ) -> StoreResult<Bucket> {
        let object = pending.object_key(key);
        if parts.is_empty() {
            // Empty object: multipart cannot make a zero-byte object (a
            // part is never empty), so the (empty) multipart is dropped and
            // the object written directly. Both steps are safe to repeat.
            if let Some(upload_id) = &pending.upload_id {
                self.bucket
                    .abort_multipart(&object, upload_id)
                    .await
                    .context("runtime complete_upload: abort empty multipart")
                    .map_err(RuntimeStoreError::Other)?;
            }
            self.bucket
                .put(&object, bytes::Bytes::new())
                .await
                .context("runtime complete_upload: write empty object")
                .map_err(RuntimeStoreError::Other)?;
            return Ok(Bucket::Assembled(0));
        }
        let Some(upload_id) = pending.upload_id.clone() else {
            // Parts with no multipart handle: nothing can assemble them, and
            // nothing was ever written to the object.
            return Ok(Bucket::Lost {
                why: "its row carries parts but no multipart upload id".into(),
                overwritten: false,
            });
        };
        let open = self
            .bucket
            .multipart_exists(&object, &upload_id)
            .await
            .context("runtime complete_upload: ask the bucket whether the upload is open")
            .map_err(RuntimeStoreError::Other)?;
        if open {
            let etags: Vec<(i32, String)> = parts
                .iter()
                .map(|(n, etag, _)| (*n, etag.clone().expect("a claimed upload has every etag")))
                .collect();
            match self.bucket.complete_multipart(&object, &upload_id, &etags).await {
                Ok(size) => return Ok(Bucket::Assembled(size)),
                Err(refused) => {
                    let still_open = self
                        .bucket
                        .multipart_exists(&object, &upload_id)
                        .await
                        .context("runtime complete_upload: ask the bucket whether a failed completion left the upload open")
                        .map_err(RuntimeStoreError::Other)?;
                    if still_open {
                        if recovering {
                            // Refused again past the lease, still open: the
                            // multipart never completed, so the object was
                            // never touched.
                            return Ok(Bucket::Lost {
                                why: format!("the bucket refuses to complete it: {refused:#}"),
                                overwritten: false,
                            });
                        }
                        return Err(RuntimeStoreError::Other(refused.context(format!(
                            "runtime complete_upload: the bucket refused to complete '{key}'; it \
                             stays 'completing' and the tenant's next upload after the lease retries it"
                        ))));
                    }
                    // Gone after a failure: a concurrent drive completed it.
                }
            }
        }
        let size = self
            .bucket
            .size(&object)
            .await
            .context("runtime complete_upload: read the completed object's size")
            .map_err(RuntimeStoreError::Other)?;
        match size {
            Some(size) if size == total => Ok(Bucket::Assembled(size)),
            other => {
                // For a replacement the object is the replaced file's: it
                // still holds that file's bytes when its size is the one the
                // file's row records, else something rewrote it.
                let overwritten = match &pending.replaces {
                    None => true,
                    Some(replaced) => {
                        let recorded: Option<i64> = sqlx::query_scalar(
                            "SELECT size_bytes FROM runtime_file WHERE key = $1",
                        )
                        .bind(replaced)
                        .fetch_optional(&self.pool)
                        .await
                        .context("runtime complete_upload: read the replaced file's size")
                        .map_err(RuntimeStoreError::Other)?;
                        recorded.map(|r| r as u64) != other
                    }
                };
                Ok(Bucket::Lost {
                    why: format!(
                        "the multipart is gone from the bucket but the object is {other:?} bytes, \
                         not the {total} it was completing to (aborted outside weft, likely by the \
                         bucket's incomplete-upload lifecycle rule, or completed wrong)"
                    ),
                    overwritten,
                })
            }
        }
    }

    /// The final verdict on a completion the bucket cannot account for:
    /// logged as an error naming the key and why, and the upload fenced to
    /// 'reaping' and reaped like an abort, freeing its reservation. A
    /// replacement's file keeps its row when its object was never
    /// overwritten (the multipart never completed, so the bucket never
    /// swapped the object); when it may have been (`overwritten`), the file
    /// goes too, so no row states a size its object does not have.
    async fn lose_completion(
        &self,
        key: &str,
        pending: &PendingUpload,
        why: &str,
        overwritten: bool,
    ) -> StoreResult<StoredFileMeta> {
        tracing::error!(
            target: "weft_broker::runtime_store",
            key = %key, replaces = ?pending.replaces, why = %why,
            "a completion cannot be accounted for; the upload is ended"
        );
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime complete_upload: begin verdict")
            .map_err(RuntimeStoreError::Other)?;
        let mut doomed = vec![pending.sweep_entry(key)];
        if overwritten {
            if let Some(replaced) = &pending.replaces {
                doomed.push(SweepEntry::file(replaced));
            }
        }
        let now = self.clock.now_unix();
        for entry in &doomed {
            sqlx::query(&fence_sql(""))
                .bind(&entry.key)
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("runtime complete_upload: fence a lost completion")
                .map_err(RuntimeStoreError::Other)?;
        }
        tx.commit()
            .await
            .context("runtime complete_upload: commit verdict")
            .map_err(RuntimeStoreError::Other)?;
        // The upload's own row first (it ends the multipart), then the
        // replaced file's (it deletes the object both share).
        for entry in &doomed {
            if let Err(e) = self.reap_fenced(entry).await {
                tracing::error!(
                    target: "weft_broker::runtime_store",
                    key = %entry.key, error = %e,
                    "failed to reap a lost completion; its row stays 'reaping' and the expiry loop's next pass, a wipe or the \
                     tenant's next upload tries it again"
                );
            }
        }
        let lost = match (&pending.replaces, overwritten) {
            (Some(replaced), true) => format!("; the file it replaced, '{replaced}', is gone with it"),
            (Some(replaced), false) => format!("; '{replaced}' keeps its old content"),
            (None, _) => String::new(),
        };
        Err(RuntimeStoreError::Lost(format!("upload '{key}' could not be completed ({why}){lost}; upload the file again")))
    }

    /// Step 3: fold an assembled object into the rows, in one short
    /// transaction locking the upload's row and, for a replacement, the
    /// file it replaces. A new upload flips active ([`Self::flip_active`]);
    /// a replacement folds into its file ([`Self::fold_replacement`]).
    ///
    /// A row no longer 'completing' means a concurrent drive folded it
    /// first, and its result is the answer.
    ///
    /// An object whose size differs from the reservation (signed into
    /// every part's URL, so only a bucket anomaly) was never charged: it
    /// gets the [`Self::lose_completion`] verdict, which fences the rows
    /// before removing the object, so no row ever points at a deleted one.
    async fn finish_completion(
        &self,
        key: &str,
        pending: &PendingUpload,
        total: u64,
        actual: u64,
    ) -> StoreResult<StoredFileMeta> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime complete_upload: begin fold")
            .map_err(RuntimeStoreError::Other)?;
        let row = Self::pending_row_locked(&mut tx, key).await.map_err(RuntimeStoreError::Other)?;
        if row.is_none_or(|r| r.status != "completing") {
            let done = Self::completed_meta(&mut *tx, key).await?;
            return done.ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "the bucket completed '{key}' but its row is gone and no file records it; \
                     the object at '{}' has no row",
                    pending.object_key(key)
                ))
            });
        }
        if let Some(replaced) = &pending.replaces {
            sqlx::query("SELECT 1 FROM runtime_file WHERE key = $1 FOR UPDATE")
                .bind(replaced)
                .execute(&mut *tx)
                .await
                .context("runtime complete_upload: lock the replaced file")
                .map_err(RuntimeStoreError::Other)?;
        }
        if actual != total {
            drop(tx);
            return self
                .lose_completion(
                    key,
                    pending,
                    &format!("the assembled object is {actual} bytes but {total} were reserved"),
                    true,
                )
                .await;
        }
        let now = self.clock.now_unix();
        // A replaced file's expiry is renewed, never made sooner. It is
        // rung all the same: the expiry loop leaves a file out of its next
        // look while its replacement completes, so no look may be booked
        // for it.
        let meta = match &pending.replaces {
            Some(replaced) => Self::fold_replacement(&mut tx, key, replaced, actual, now).await?,
            None => Self::flip_active(&mut tx, key, pending, actual, now).await?,
        };
        tx.commit()
            .await
            .context("runtime complete_upload: commit fold")
            .map_err(RuntimeStoreError::Other)?;
        self.ring_for(meta.expires_at_unix).await;
        Ok(meta)
    }

    /// Fold a completed replacement into the file it replaced: the bucket
    /// already swapped that file's object for the new bytes, so its row
    /// takes the new size (and the charge that goes with it), its expiry is
    /// renewed (a replace is an access), it records this upload as its last
    /// replacement (what a retried complete finds), and the replacement's
    /// own row goes, its charge and part rows with it. The replaced file is
    /// active: nothing removes a file whose replacement is completing.
    async fn fold_replacement(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        key: &str,
        replaced: &str,
        actual: u64,
        now: i64,
    ) -> StoreResult<StoredFileMeta> {
        sqlx::query("DELETE FROM runtime_file WHERE key = $1 AND status = 'completing'")
            .bind(key)
            .execute(&mut **tx)
            .await
            .context("runtime replace: drop the replacement's row")
            .map_err(RuntimeStoreError::Other)?;
        let folded: FileRow = sqlx::query_as::<_, FileRow>(&format!(
            "UPDATE runtime_file SET size_bytes = $2, reserved_bytes = $2, version = version + 1, \
                 last_replacement = $4, \
                 expires_at_unix = CASE WHEN keep_ttl_secs IS NULL THEN expires_at_unix ELSE {} END \
             WHERE key = $1 AND status = 'active' \
             RETURNING {FILE_ROW_COLUMNS}",
            expiry_honoring_links("$3 + keep_ttl_secs")
        ))
        .bind(replaced)
        .bind(actual as i64)
        .bind(now)
        .bind(key)
        .fetch_optional(&mut **tx)
        .await
        .context("runtime replace: fold the new size into the replaced file")
        .map_err(RuntimeStoreError::Other)?
        .ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "'{replaced}' is no longer active although its replacement '{key}' was \
                 completing, which every removal refuses; its object now holds the new bytes"
            ))
        })?;
        Ok(folded.to_meta())
    }

    /// Flip a completed upload's row to active with its final size and drop
    /// its part rows. The expiry is stamped NOW (completion is when a kept
    /// file starts existing), unless the row already carries one: the
    /// run sweep's linger stamp on an upload still completing when
    /// its run ended ([`Self::sweep_run`]). `reserved_bytes` is set to the
    /// final size so the tenant's charged sum is identical before and after
    /// the flip.
    async fn flip_active(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        key: &str,
        pending: &PendingUpload,
        actual: u64,
        now: i64,
    ) -> StoreResult<StoredFileMeta> {
        let expires_at = pending.keep_ttl_secs.map(|s| now + s);
        let flipped: FileRow = sqlx::query_as(&format!(
            "UPDATE runtime_file SET \
               size_bytes = $2, status = 'active', expires_at_unix = COALESCE(expires_at_unix, $3), \
               upload_id = NULL, part_size = NULL, declared_size = NULL, reserved_bytes = $2 \
             WHERE key = $1 AND status = 'completing' \
             RETURNING {FILE_ROW_COLUMNS}"
        ))
        .bind(key)
        .bind(actual as i64)
        .bind(expires_at)
        .fetch_one(&mut **tx)
        .await
        .context("runtime complete_upload: flip active (the row is locked by this transaction)")
        .map_err(RuntimeStoreError::Other)?;
        sqlx::query("DELETE FROM runtime_file_part WHERE key = $1")
            .bind(key)
            .execute(&mut **tx)
            .await
            .context("runtime complete_upload: drop part rows")
            .map_err(RuntimeStoreError::Other)?;
        Ok(flipped.to_meta())
    }

    /// Where each of this upload's parts starts in the file.
    ///
    /// Known-size uploads allow reserving parts out of order. Their offsets
    /// come from the declared layout, never from which rows arrived first.
    /// Streams reserve a contiguous prefix and only their final part is short.
    #[allow(clippy::type_complexity)]
    async fn part_offsets(
        &self,
        key: &str,
        pending: &PendingUpload,
    ) -> anyhow::Result<(std::collections::BTreeMap<i32, u64>, std::collections::BTreeMap<i32, u64>, Vec<(i32, i64)>)> {
        let rows: Vec<(i32, i64, bool)> = sqlx::query_as(
            "SELECT part_number, size_bytes, etag IS NULL FROM runtime_file_part \
             WHERE key = $1 ORDER BY part_number",
        )
        .bind(key)
        .fetch_all(&self.pool)
        .await
        .context("runtime part_offsets: read part sizes")?;
        let mut at = 0u64;
        let mut offsets = std::collections::BTreeMap::new();
        let mut sizes = std::collections::BTreeMap::new();
        let mut missing = Vec::new();
        for (number, size, unfinished) in rows {
            offsets.insert(number, if pending.declared_size.is_some() {
                (number as u64 - 1) * pending.part_size as u64
            } else { at });
            sizes.insert(number, size as u64);
            if unfinished { missing.push((number, size)); }
            at += size as u64;
        }
        Ok((offsets, sizes, missing))
    }

    /// Resume an interrupted upload: re-presign exactly the reserved parts
    /// that were never reported done. Returns the upload's part size + those
    /// parts. (A part that was uploaded but whose done-report was lost is
    /// simply re-uploaded: its etag is only trusted from our own records.)
    pub async fn resume_upload(
        &self,
        caller: &CallerAuth,
        key: &str,
        entitlements: &dyn EntitlementSource,
        audience: PresignAudience,
    ) -> StoreResult<(u64, Vec<PresignedPart>, u64)> {
        Self::wall_key(caller, key)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime resume_upload: begin read tx")
            .map_err(RuntimeStoreError::Other)?;
        let pending = Self::pending_row(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime resume_upload: commit read")
            .map_err(RuntimeStoreError::Other)?;
        if let Some(pending) = &pending {
            pending.refuse_if_completing(key)?;
        }
        let Some(pending) = pending else {
            return match self.row(key).await.map_err(RuntimeStoreError::Other)? {
                Some(_) => Err(RuntimeStoreError::Invalid(format!(
                    "upload '{key}' already completed; nothing to resume"
                ))),
                None => Err(RuntimeStoreError::NotFound(format!(
                    "no upload in flight for key '{key}'"
                ))),
            };
        };
        let upload_id = pending.upload_id.clone().ok_or_else(|| {
            RuntimeStoreError::Other(anyhow::anyhow!(
                "pending row for '{key}' unexpectedly has no upload id (a committed \
                 pending row always carries one); abort this upload and begin again"
            ))
        })?;
        // Missing parts and the resume position must describe the same rows.
        // A reservation arriving between separate reads could otherwise move
        // the reader past bytes that neither publisher has uploaded.
        let (offsets, sizes, missing) = self
            .part_offsets(key, &pending)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let mut parts = Vec::with_capacity(missing.len());
        if let Some(declared) = pending.declared_size {
            // Reservation order can leave holes before the highest part. Fill
            // them through normal reservation before returning the resume
            // prefix, so a forward-only reader never skips unreserved bytes.
            let highest = sizes.keys().next_back().copied().unwrap_or(0);
            let gaps: Vec<_> = (1..=highest).filter(|number| !sizes.contains_key(number)).map(|number| {
                let offset = (number as u64 - 1) * pending.part_size as u64;
                PartAsk { part_number: number, size_bytes: (pending.part_size as u64).min(declared as u64 - offset) }
            }).collect();
            if !gaps.is_empty() {
                parts.extend(self.reserve_parts(caller, key, &gaps, entitlements, audience).await?);
            }
        }
        for (part_number, size) in missing {
            let offset_bytes = *offsets.get(&part_number).ok_or_else(|| {
                RuntimeStoreError::Other(anyhow::anyhow!(
                    "part {part_number} of '{key}' is missing its own row, so where its bytes \
                     belong in the file cannot be stated; abort this upload and begin again"
                ))
            })?;
            let url = self
                .bucket
                .presign_part(
                    &pending.object_key(key),
                    &upload_id,
                    part_number,
                    size as u64,
                    audience,
                    DEFAULT_PRESIGN_TTL_SECS,
                )
                .await
                .context("runtime resume_upload: presign missing part")
                .map_err(RuntimeStoreError::Other)?;
            parts.push(PresignedPart { part_number, size_bytes: size as u64, offset_bytes, url });
        }
        // Everything already carved into parts, which is where new ones
        // begin. `offsets` holds every part row, so the end of the last
        // one plus its size is that length.
        let reserved_bytes = offsets
            .iter()
            .next_back()
            .map(|(number, at)| at + sizes.get(number).copied().unwrap_or(0))
            .unwrap_or(0);
        parts.sort_by_key(|part| part.part_number);
        Ok((pending.part_size as u64, parts, reserved_bytes))
    }

    /// Cancel an in-flight upload. Idempotent: a key with no in-flight
    /// upload and no file is already in the aborted state. A COMPLETED file
    /// is not abortable (delete it instead), and neither is an upload a
    /// completion has claimed: the bucket may already hold its object.
    ///
    /// The row is fenced to 'reaping' first, under its lock, so no
    /// completion can claim it afterwards; then the bucket's multipart is
    /// aborted and the row deleted (freeing the reservation; the part rows
    /// cascade), exactly like a sweep's reap. A crash in between leaves a
    /// 'reaping' row the tenant's next upload finishes.
    pub async fn abort_upload(&self, caller: &CallerAuth, key: &str) -> StoreResult<()> {
        Self::wall_key(caller, key)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime abort_upload: begin tx")
            .map_err(RuntimeStoreError::Other)?;
        let pending = Self::pending_row_locked(&mut tx, key)
            .await
            .map_err(RuntimeStoreError::Other)?;
        let Some(pending) = pending else {
            drop(tx);
            return match self.row(key).await.map_err(RuntimeStoreError::Other)? {
                Some(_) => Err(RuntimeStoreError::Invalid(format!(
                    "'{key}' already completed; delete the file instead of aborting"
                ))),
                None => Ok(()),
            };
        };
        pending.refuse_if_completing(key)?;
        sqlx::query(&fence_sql(""))
            .bind(key)
            .bind(self.clock.now_unix())
            .execute(&mut *tx)
            .await
            .context("runtime abort_upload: fence the row")
            .map_err(RuntimeStoreError::Other)?;
        tx.commit()
            .await
            .context("runtime abort_upload: commit fence")
            .map_err(RuntimeStoreError::Other)?;
        self.reap_fenced(&pending.sweep_entry(key))
            .await
            .context("runtime abort_upload (the row stays 'reaping'; the tenant's next upload finishes it)")
            .map_err(RuntimeStoreError::Other)
    }

    /// The in-flight upload row ('pending' or 'completing') for a key, read
    /// inside an open transaction. None when the key has none (active,
    /// being reaped, or unknown).
    async fn pending_row(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        key: &str,
    ) -> Result<Option<PendingUpload>> {
        sqlx::query_as::<_, PendingUpload>(&format!("{PENDING_ROW_SELECT_ANY} AND status IN ('pending', 'completing')"))
            .bind(key)
            .fetch_optional(&mut **tx)
            .await
            .context("read pending upload row")
    }

    /// [`Self::pending_row`], holding the row locked until `tx` ends. Every
    /// transaction that changes an upload's status or parts takes this
    /// lock first, so they apply one after another.
    async fn pending_row_locked(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        key: &str,
    ) -> Result<Option<PendingUpload>> {
        sqlx::query_as::<_, PendingUpload>(&format!(
            "{PENDING_ROW_SELECT_ANY} AND status IN ('pending', 'completing') FOR UPDATE"
        ))
            .bind(key)
            .fetch_optional(&mut **tx)
            .await
            .context("lock pending upload row")
    }

    /// The file an upload key completed into, for a complete that finds its
    /// pending row gone (a retry, or the loser of two concurrent completes):
    /// the key's own ACTIVE row for a new file, or for a replacement the
    /// active file it last folded into. None when the key never completed
    /// (unknown, swept, aborted) or a later replacement of the same file
    /// has folded since.
    async fn completed_meta<'e>(
        executor: impl sqlx::PgExecutor<'e>,
        key: &str,
    ) -> StoreResult<Option<StoredFileMeta>> {
        let row: Option<FileRow> = sqlx::query_as(&format!(
            "SELECT {FILE_ROW_COLUMNS} FROM runtime_file \
             WHERE (key = $1 OR last_replacement = $1) AND status = 'active'"
        ))
        .bind(key)
        .fetch_optional(executor)
        .await
        .context("runtime complete_upload: read existing row")
        .map_err(RuntimeStoreError::Other)?;
        Ok(row.as_ref().map(FileRow::to_meta))
    }

    /// The metadata row for an ACTIVE (finalized) file, or None if absent, still
    /// pending, or expired. Every user-facing read (meta/get/download/keep/presign)
    /// goes through here, so a half-uploaded 'pending' file reads as not-found
    /// until it finalizes. Access does NOT bump a kept file's expiry here (a
    /// metadata peek is not an access); the byte `get`/`download_url` bumps it.
    async fn row(&self, key: &str) -> Result<Option<FileRow>> {
        sqlx::query_as::<_, FileRow>(&format!(
            "SELECT {FILE_ROW_COLUMNS} FROM runtime_file WHERE key = $1 AND status = 'active' AND {live}",
            live = unexpired("$2"),
        ))
        .bind(key)
        .bind(self.clock.now_unix())
        .fetch_optional(&self.pool)
        .await
        .context("runtime row")
    }

    /// The ACTIVE file stored under `identity` in the caller's `scope`,
    /// if any: the read half of an identified put, so a caller can ask
    /// before it fetches anything. The scope is the same prefix the
    /// begin's collision check and the identity index use.
    pub async fn find_identity(
        &self,
        caller: &CallerAuth,
        scope: &StorageScope,
        identity: &str,
    ) -> StoreResult<Option<StoredFileMeta>> {
        let prefix = weft_core::storage::key::prefix_for_list(caller, scope)
            .map_err(RuntimeStoreError::Denied)?;
        let row = sqlx::query_as::<_, FileRow>(&format!(
            "SELECT {FILE_ROW_COLUMNS} FROM runtime_file \
             WHERE regexp_replace(key, '/[^/]+$', '') = $1 AND identity = $2 AND status = 'active' AND {live}",
            live = unexpired("$3"),
        ))
        .bind(prefix.trim_end_matches('/'))
        .bind(identity)
        .bind(self.clock.now_unix())
        .fetch_optional(&self.pool)
        .await
        .context("runtime find_identity")
        .map_err(RuntimeStoreError::Other)?;
        Ok(row.as_ref().map(FileRow::to_meta))
    }

    /// Metadata only (no access bump). The caller has already passed the wall.
    pub async fn meta(&self, parsed: &ParsedKey) -> StoreResult<StoredFileMeta> {
        let key = parsed.to_key();
        self.row(&key)
            .await
            .map_err(RuntimeStoreError::Other)?
            .map(|r| r.to_meta())
            .ok_or_else(|| RuntimeStoreError::NotFound(key))
    }

    /// Read an active, unexpired file and renew its expiry from its current
    /// TTL, then push an expiry that is still short of `cover_until` to it:
    /// a link handed out for the file lives that long, and the bytes must
    /// outlive it. Leaves no expiry on a file that has none. Never shortens
    /// below a live public link's expiry (a minted link is a promise the
    /// bytes stay fetchable for its stated lifetime). An expired file is
    /// never brought back to life: it reads as gone.
    async fn bump_expiry(&self, key: &str, cover_until: i64) -> Result<Option<FileRow>> {
        // Read the TTL from the locked row, not an earlier metadata read:
        // source publication may have protected or retired this asset since
        // that read. A reaper that already claimed the row wins over access.
        sqlx::query_as::<_, FileRow>(&format!(
            "UPDATE runtime_file SET expires_at_unix = CASE \
                 WHEN expires_at_unix IS NULL THEN NULL \
                 WHEN keep_ttl_secs IS NULL THEN GREATEST(expires_at_unix, $3) \
                 ELSE GREATEST({}, $3) END \
             WHERE key = $2 AND status = 'active' AND {live} \
             RETURNING {FILE_ROW_COLUMNS}",
            expiry_honoring_links("$1 + keep_ttl_secs"),
            live = unexpired("$1"),
        ))
        .bind(self.clock.now_unix())
        .bind(key)
        .bind(cover_until)
        .fetch_optional(&self.pool)
        .await
        .context("bump expiry")
    }

    /// Delete a file (object + row). A missing row is a not-found so the caller
    /// learns the key was already gone.
    ///
    /// Delete a finished file: fence its row to 'reaping' (under its lock),
    /// then remove the object, then the row, the same three steps as every
    /// reap, so a crash in between leaves a 'reaping' row the tenant's next
    /// upload finishes and never a row pointing at a deleted object. A file whose
    /// replacement is completing is refused: the bucket may be writing its
    /// object.
    pub async fn delete(&self, parsed: &ParsedKey) -> StoreResult<()> {
        let key = parsed.to_key();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("runtime delete: begin tx")
            .map_err(RuntimeStoreError::Other)?;
        if let Removal::Blocked(blocker) = Self::lock_for_removal(&mut tx, &key).await.map_err(RuntimeStoreError::Other)? {
            return Err(RuntimeStoreError::Completing(format!(
                "'{key}' cannot be deleted while {blocker}; delete it once that lands"
            )));
        }
        let fenced = sqlx::query(&fence_sql("AND status = 'active'"))
            .bind(&key)
            .bind(self.clock.now_unix())
            .execute(&mut *tx)
            .await
            .context("runtime delete: fence the row")
            .map_err(RuntimeStoreError::Other)?;
        if fenced.rows_affected() == 0 {
            return Err(RuntimeStoreError::NotFound(key));
        }
        tx.commit()
            .await
            .context("runtime delete: commit fence")
            .map_err(RuntimeStoreError::Other)?;
        self.reap_fenced(&SweepEntry::file(&key))
            .await
            .context("runtime delete (the row stays 'reaping'; the tenant's next upload finishes it)")
            .map_err(RuntimeStoreError::Other)
    }

    /// Lock a row before fencing it for removal, and say whether it is
    /// there and what forbids the removal: the row is an upload a
    /// completion has claimed, or a file whose replacement a completion
    /// has claimed (the bucket may be writing its object). The check runs
    /// as its own statement after the lock, so it sees a claim that
    /// committed while this waited; a claim arriving later locks the same
    /// file and finds it fenced.
    async fn lock_for_removal(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, key: &str) -> Result<Removal> {
        let status: Option<String> = sqlx::query_scalar("SELECT status FROM runtime_file WHERE key = $1 FOR UPDATE")
            .bind(key)
            .fetch_optional(&mut **tx)
            .await
            .context("lock a row for removal")?;
        match status.as_deref() {
            None => return Ok(Removal::Gone),
            Some("completing") => return Ok(Removal::Blocked(format!("its upload '{key}' is completing"))),
            Some(_) => {}
        }
        let replacement: Option<String> = sqlx::query_scalar(
            "SELECT key FROM runtime_file WHERE replaces = $1 AND status = 'completing'",
        )
        .bind(key)
        .fetch_optional(&mut **tx)
        .await
        .context("look for a completing replacement")?;
        Ok(match replacement {
            Some(r) => Removal::Blocked(format!("its replacement '{r}' is completing")),
            None => Removal::Free,
        })
    }

    /// Fence one row for a sweep or wipe: lock it, skip it when it is gone
    /// or a completion forbids the removal ([`Self::lock_for_removal`]),
    /// else flip it to 'reaping', claimed at `now`, where `condition`
    /// ([`fence_sql`]'s, `$2` being `now`) still holds.
    async fn fence_for_reap(&self, key: &str, condition: &str, now: i64) -> Result<Fence> {
        let mut tx = self.pool.begin().await.context("fence: begin tx")?;
        match Self::lock_for_removal(&mut tx, key).await? {
            Removal::Gone => return Ok(Fence::Gone),
            Removal::Blocked(blocker) => {
                tracing::info!(target: "weft_broker::runtime_store", key = %key, "not reaped while {blocker}");
                return Ok(Fence::Completing);
            }
            Removal::Free => {}
        }
        let fenced = sqlx::query(&fence_sql(condition))
            .bind(key)
            .bind(now)
            .execute(&mut *tx)
            .await
            .context("fence")?
            .rows_affected();
        tx.commit().await.context("fence: commit")?;
        Ok(if fenced > 0 { Fence::Fenced } else { Fence::Spared })
    }

    /// Replace the set of the tenant's assets `project` references. An asset
    /// any project of the tenant references has no expiry; one whose LAST
    /// reference this publish removes starts the default access-renewed TTL
    /// once (repeated publishes never restart it), and referencing it again
    /// clears that countdown. Every project's publish runs under the tenant
    /// lock, so two projects publishing at once agree on who references what.
    ///
    /// `keys` must all be present (a missing one fails the update and
    /// changes nothing); `kept` are referenced when present and handed back
    /// when not, so a version whose file is gone never blocks a build.
    pub async fn set_asset_references(
        &self,
        tenant: &str,
        project: &str,
        keys: &[String],
        kept: &[String],
    ) -> StoreResult<Vec<String>> {
        if !weft_core::storage::key::valid_segment(project) {
            return Err(RuntimeStoreError::Invalid(format!("'{project}' is not a valid project id")));
        }
        for key in keys.iter().chain(kept) {
            let parsed = weft_core::storage::key::parse_key(key).map_err(RuntimeStoreError::Invalid)?;
            if parsed.tenant != tenant || parsed.scope != weft_core::storage::key::KeyScope::Asset {
                return Err(RuntimeStoreError::Denied(
                    "asset references must name the acting tenant's assets".into(),
                ));
            }
        }
        let mut tx = self.pool.begin().await.context("asset references transaction")?;
        lock_tenant_storage(&mut tx, tenant).await?;
        let before: Vec<String> = sqlx::query_scalar(
            "SELECT key FROM asset_reference WHERE tenant_id = $1 AND project_id = $2",
        ).bind(tenant).bind(project).fetch_all(&mut *tx).await.context("read the project's asset references")?;
        // Lock every file this publish touches before checking presence, so
        // expiry cannot remove a referenced file between validation and
        // protection. A reaper that won first makes this update fail without
        // changing any reference.
        let touched: Vec<&String> = keys.iter().chain(kept).chain(&before).collect();
        let available: Vec<String> = sqlx::query_scalar(
            "SELECT key FROM runtime_file WHERE key = ANY($1) AND status = 'active' ORDER BY key FOR UPDATE",
        ).bind(&touched).fetch_all(&mut *tx).await.context("lock the referenced assets")?;
        let available: std::collections::HashSet<&str> = available.iter().map(String::as_str).collect();
        if let Some(missing) = keys.iter().find(|key| !available.contains(key.as_str())) {
            return Err(RuntimeStoreError::NotFound(missing.clone()));
        }
        let (kept_present, missing): (Vec<String>, Vec<String>) = kept.iter().cloned()
            .partition(|key| available.contains(key.as_str()));
        let referenced: Vec<String> = keys.iter().cloned().chain(kept_present)
            .collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        sqlx::query("DELETE FROM asset_reference WHERE tenant_id = $1 AND project_id = $2")
            .bind(tenant).bind(project).execute(&mut *tx).await.context("clear the project's asset references")?;
        sqlx::query(
            "INSERT INTO asset_reference (tenant_id, project_id, key) SELECT $1, $2, unnest($3::text[])",
        ).bind(tenant).bind(project).bind(&referenced).execute(&mut *tx).await.context("record the project's asset references")?;
        sqlx::query(
            "UPDATE runtime_file SET expires_at_unix = NULL, keep_ttl_secs = NULL \
             WHERE key = ANY($1) AND status = 'active'",
        ).bind(&referenced).execute(&mut *tx).await.context("keep the referenced assets")?;
        // What this project stopped referencing, and no other project
        // references either, starts its countdown.
        let countdown = self.clock.now_unix() + DEFAULT_KEEP_TTL_SECS as i64;
        let retired = sqlx::query(&format!(
            "UPDATE runtime_file SET \
                 expires_at_unix = COALESCE(expires_at_unix, {}), \
                 keep_ttl_secs = COALESCE(keep_ttl_secs, $3) \
             WHERE key = ANY($1) AND status = 'active' \
               AND NOT EXISTS (SELECT 1 FROM asset_reference r WHERE r.key = runtime_file.key)",
            expiry_honoring_links("$2")
        ))
        .bind(&before)
        .bind(countdown)
        .bind(DEFAULT_KEEP_TTL_SECS as i64)
        .execute(&mut *tx).await.context("retire the unreferenced assets")?;
        tx.commit().await.context("commit the project's asset references")?;
        if retired.rows_affected() > 0 {
            self.ring_for(Some(countdown)).await;
        }
        Ok(missing)
    }

    /// Which of `hashes` the tenant stores whole: `content hash -> key`. An
    /// upload still in flight is not held (its begin answers how to resume
    /// it). Every hash must be a content hash.
    pub async fn held_assets(
        &self,
        tenant: &str,
        hashes: &[String],
    ) -> StoreResult<std::collections::BTreeMap<String, String>> {
        let mut wanted = std::collections::BTreeMap::new();
        for hash in hashes {
            let key = ParsedKey::asset(tenant, hash).map_err(RuntimeStoreError::Invalid)?.to_key();
            wanted.insert(key, hash.clone());
        }
        let keys: Vec<&String> = wanted.keys().collect();
        let held: Vec<String> = sqlx::query_scalar(
            "SELECT key FROM runtime_file WHERE key = ANY($1) AND status = 'active'",
        )
        .bind(&keys)
        .fetch_all(&self.pool)
        .await
        .context("look up held assets")
        .map_err(RuntimeStoreError::Other)?;
        Ok(held.into_iter().map(|key| (wanted[&key].clone(), key)).collect())
    }

    /// List every ACTIVE file under a key prefix (a scope, or a whole tenant). The
    /// prefix is produced by the wall (`prefix_for_list` / `tenant_prefix`), so it
    /// is always tenant-anchored. Pending (half-uploaded) files are excluded: they
    /// are not real files a user can see yet; so are expired ones.
    pub async fn list(&self, prefix: &str) -> Result<Vec<StoredFileMeta>> {
        let pattern = like_prefix(prefix);
        let rows = sqlx::query_as::<_, FileRow>(&format!(
            "SELECT {FILE_ROW_COLUMNS} FROM runtime_file \
             WHERE key LIKE $1 ESCAPE '\\' AND status = 'active' AND {live} ORDER BY key",
            live = unexpired("$2"),
        ))
        .bind(&pattern)
        .bind(self.clock.now_unix())
        .fetch_all(&self.pool)
        .await
        .context("runtime list")?;
        Ok(rows.iter().map(FileRow::to_meta).collect())
    }


    /// Every key under a prefix REGARDLESS of status (active + pending), with
    /// what a sweep needs: is it a kept ACTIVE file (spared by the terminate
    /// sweep), and the in-flight upload id to abort (pending rows). Sweeps use
    /// this (not `list`) so a half-uploaded file's bucket state is cleaned
    /// too; the bucket never keeps state whose row a sweep removed.
    async fn keys_under(&self, prefix: &str) -> Result<Vec<SweepEntry>> {
        let pattern = like_prefix(prefix);
        sqlx::query_as::<_, SweepEntry>(
            "SELECT key, (keep AND status = 'active') AS kept_active, status, upload_id, replaces \
             FROM runtime_file WHERE key LIKE $1 ESCAPE '\\'",
        )
        .bind(&pattern)
        .fetch_all(&self.pool)
        .await
        .context("runtime keys_under")
    }

    /// Steps 2+3 of a fenced reap (the caller has already flipped the row to
    /// 'reaping'): remove the bucket state, then the row. If the bucket reap
    /// fails, the row is marked ([`Self::mark_reap_failed`]), the error
    /// propagates, and the row STAYS in 'reaping', which the sweeps take
    /// over (at once once marked, once its claim lapsed after a crash). So
    /// a crash between the steps never strands an object without a row
    /// (nothing would ever revisit it: all reclaim paths are row-driven)
    /// nor a charged row pointing at deleted bytes; the only residue is a
    /// 'reaping' row that the tenant's next upload (or the expiry loop's
    /// next pass) finishes.
    async fn reap_fenced(&self, entry: &SweepEntry) -> Result<()> {
        self.try_reap(entry, &[]).await?
    }

    /// Reap a fenced row ([`Self::reap_fenced`]), trying again after each
    /// of `pauses`, still under this call's claim. When every try failed,
    /// the row stays 'reaping', marked, and `false` is answered: the
    /// caller goes on with its other rows.
    async fn reap_with_tries(&self, entry: &SweepEntry, pauses: &[std::time::Duration]) -> Result<bool> {
        Ok(self.try_reap(entry, pauses).await?.is_ok())
    }

    /// [`Self::reap_with_tries`], answering a bucket reap that failed every
    /// try (the row then marked) apart from a failure of the database:
    /// `Ok(Err(_))` then, with the last try's error.
    async fn try_reap(&self, entry: &SweepEntry, pauses: &[std::time::Duration]) -> Result<Result<()>> {
        let mut pauses = pauses.iter();
        while let Err(e) = self.reap_bucket_state(entry).await {
            match pauses.next() {
                Some(pause) => tokio::time::sleep(*pause).await,
                None => {
                    self.mark_reap_failed(&entry.key, &e).await?;
                    return Ok(Err(e));
                }
            }
        }
        let was_marked: bool = sqlx::query_scalar(&delete_upload_row(""))
            .bind(&entry.key)
            .fetch_one(&self.pool)
            .await
            .context("delete reaped row")?;
        if was_marked {
            self.record_reap_failures().await?;
        }
        Ok(Ok(()))
    }

    /// Remove a doomed key's bucket state: abort its in-flight multipart
    /// upload (if any; idempotent) and delete its object (idempotent). Every
    /// sweep/wipe path funnels through here so no path can forget the abort.
    /// A replacement upload's multipart lives on the object of the file it
    /// replaces, which that file still owns: only the upload is aborted.
    async fn reap_bucket_state(&self, entry: &SweepEntry) -> Result<()> {
        let key = &entry.key;
        let object = object_key(entry.replaces.as_deref().unwrap_or(key));
        if let Some(id) = &entry.upload_id {
            self.bucket
                .abort_multipart(&object, id)
                .await
                .with_context(|| format!("abort in-flight upload for {key}"))?;
        }
        if entry.replaces.is_none() {
            self.bucket
                .delete(&object)
                .await
                .with_context(|| format!("delete object {key}"))?;
        }
        Ok(())
    }

    /// Set how long a file lives from now on, access-renewed. On a
    /// run file this also flags it to survive the terminate sweep;
    /// in the scopes that outlive runs already it is only the lifetime
    /// (`KeepTtl::Never` there clears one). An asset's lifetime follows
    /// the source, so it is refused.
    pub async fn keep(&self, parsed: &ParsedKey, ttl: KeepTtl) -> StoreResult<StoredFileMeta> {
        if matches!(parsed.scope, weft_core::storage::key::KeyScope::Asset) {
            return Err(RuntimeStoreError::Invalid(
                "an asset's lifetime follows the source that references it; it takes no keep".into(),
            ));
        }
        let run_scoped = matches!(parsed.scope, weft_core::storage::key::KeyScope::Run { .. });
        let key = parsed.to_key();
        let ttl_secs = ttl.secs();
        let now = self.clock.now_unix();
        let expires_at = ttl_secs.map(|s| now + s as i64);
        // KeepTtl::Never binds NULL (never expires, covers links
        // trivially); a finite deadline never undercuts a live link. A file
        // that already expired stays expired.
        let row: Option<FileRow> = sqlx::query_as::<_, FileRow>(&format!(
            "UPDATE runtime_file SET keep = keep OR $4, keep_ttl_secs = $1, \
                 expires_at_unix = CASE WHEN $2::bigint IS NULL THEN NULL ELSE {} END \
             WHERE key = $3 AND status <> 'reaping' AND {live} \
             RETURNING {FILE_ROW_COLUMNS}",
            expiry_honoring_links("$2"),
            live = unexpired("$5"),
        ))
        .bind(ttl_secs.map(|s| s as i64))
        .bind(expires_at)
        .bind(&key)
        .bind(run_scoped)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("runtime keep")
        .map_err(RuntimeStoreError::Other)?;
        let row = row.ok_or_else(|| RuntimeStoreError::NotFound(key))?;
        self.ring_for(row.expires_at_unix).await;
        Ok(row.to_meta())
    }

    /// Mint a presigned GET URL signed for the bucket's PUBLIC endpoint, valid
    /// for a clamped TTL: the browser's download lane, and the direct link when
    /// the operator declared that endpoint internet-reachable. The caller
    /// streams the bytes directly from the bucket; the broker never proxies
    /// them. Minting counts as access (bumps the expiry), and a missing file
    /// fails the mint rather than handing out a 404 URL. A node body's own
    /// link is `download_url` with the Internal audience: the public endpoint
    /// of a local install is the host's loopback, which a process cannot reach.
    pub async fn presign(&self, parsed: &ParsedKey, ttl_secs: Option<u64>) -> StoreResult<String> {
        Ok(self.presign_get(parsed, PresignAudience::External, ttl_secs, true).await?.1)
    }

    /// Mint a PUBLIC RELAY link token for a file: the returned token
    /// resolves at the public `/public/files/{token}` route, which
    /// streams the bytes. The row carries the metadata
    /// plus a presigned fetch URL the relay (the broker itself) reads
    /// from, signed for the runtime's own endpoint and the token's own
    /// lifetime. Minting counts as access (bumps a kept file's
    /// expiry), and a minted link is a PROMISE: a file already carrying
    /// an expiry gets it pushed past the link's ([`Self::presign_get`]), so
    /// the bytes outlive every live token (a file with no expiry outlives
    /// it trivially, and the terminate sweep's linger stamp honors live
    /// links too).
    pub async fn mint_public_link(
        &self,
        parsed: &ParsedKey,
        ttl_secs: Option<u64>,
    ) -> StoreResult<String> {
        let (meta, fetch_url, link_expiry) =
            self.presign_get(parsed, PresignAudience::Runtime, ttl_secs, true).await?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        // Opportunistic sweep: expired links are dead weight nobody can
        // act on, deleted on every mint so the table never accumulates.
        sqlx::query("DELETE FROM public_file_link WHERE expires_at_unix < $1")
            .bind(self.clock.now_unix())
            .execute(&self.pool)
            .await
            .context("public_file_link sweep")
            .map_err(RuntimeStoreError::Other)?;
        sqlx::query(
            "INSERT INTO public_file_link
               (token, key, mime_type, filename, fetch_url, expires_at_unix)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&token)
        .bind(parsed.to_key())
        .bind(&meta.mime_type)
        .bind(&meta.filename)
        .bind(&fetch_url)
        .bind(link_expiry)
        .execute(&self.pool)
        .await
        .context("public_file_link insert")
        .map_err(RuntimeStoreError::Other)?;
        Ok(token)
    }

    /// Resolve a public-relay token to its file: the metadata for the
    /// response headers plus the presigned internal fetch URL minted
    /// with it. `None` for a missing OR expired token (the two must be
    /// indistinguishable to the outside).
    pub async fn resolve_public_link(&self, token: &str) -> Result<Option<PublicLinkTarget>> {
        let row: Option<(String, String, String, i64)> = sqlx::query_as(
            "SELECT mime_type, filename, fetch_url, expires_at_unix
             FROM public_file_link WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .context("public_file_link lookup")?;
        Ok(row.and_then(|(mime_type, filename, fetch_url, expires)| {
            (expires >= self.clock.now_unix()).then_some(PublicLinkTarget {
                mime_type,
                filename,
                fetch_url,
            })
        }))
    }

    /// Mint a presigned GET URL a WORKER uses to read a runtime file's bytes
    /// DIRECTLY from the bucket, plus its metadata. Bytes never transit the broker.
    /// Signed for the INTERNAL endpoint (the worker is internal). Counts as access
    /// (bumps a kept file's expiry), like the old streaming get did. With
    /// `hands_out`, the URL is handed to somebody to fetch later (a link a
    /// node passes on), so the file outlives it; a read made now needs no
    /// such promise.
    pub async fn download_url(
        &self,
        parsed: &ParsedKey,
        audience: PresignAudience,
        ttl_secs: Option<u64>,
        hands_out: bool,
    ) -> StoreResult<(StoredFileMeta, String)> {
        let (meta, url, _) = self.presign_get(parsed, audience, ttl_secs, hands_out).await?;
        Ok((meta, url))
    }

    /// Shared body of the presigned-GET mints: clamp the TTL, load the row
    /// (404 if absent or expired) bumping its expiry (minting IS an access)
    /// and, for a URL handed out (`hands_out`), pushing the expiry past the
    /// URL's life (a file on an expiry clock, a lingering run output among
    /// them, outlives every URL handed out for it), sign for `audience`.
    /// Returns the file's metadata, the URL, and when the URL stops working
    /// (unix seconds).
    async fn presign_get(
        &self,
        parsed: &ParsedKey,
        audience: PresignAudience,
        ttl_secs: Option<u64>,
        hands_out: bool,
    ) -> StoreResult<(StoredFileMeta, String, i64)> {
        let key = parsed.to_key();
        let ttl = ttl_secs.unwrap_or(DEFAULT_PRESIGN_TTL_SECS).clamp(1, MAX_PRESIGN_TTL_SECS);
        let until = self.clock.now_unix() + ttl as i64;
        let row = self
            .bump_expiry(&key, if hands_out { until } else { 0 })
            .await
            .map_err(RuntimeStoreError::Other)?
            .ok_or_else(|| RuntimeStoreError::NotFound(key.clone()))?;
        let url = self
            .bucket
            .presign_get(&object_key(&key), audience, ttl)
            .await
            .context("runtime presign GET")
            .map_err(RuntimeStoreError::Other)?;
        Ok((row.to_meta(), url, until))
    }

    /// Wipe every file under a wall-validated prefix (a `weft files rm` of a
    /// scope, or a tenant-delete). Deletes rows + objects; returns the count.
    /// The prefix MUST have passed `validate_wipe_prefix` at the edge.
    pub async fn wipe_prefix(&self, prefix: &str) -> Result<u64> {
        // ALL keys under the prefix (active + pending), so a half-uploaded
        // file's bucket state (in-flight upload + any object) is wiped too.
        // Same fenced three-step reap as the sweeps (fence the row to
        // 'reaping', reap bucket, delete row): a crash mid-wipe leaves only
        // 'reaping' rows the tenant's next upload finishes, never a live
        // row pointing at deleted bytes. The wipe is unconditional (the whole
        // prefix dies), so the fence has no doom re-check; it exists to
        // lock out writers/readers and make the residue self-healing. A row
        // nobody is reaping (a reap that failed or crashed) is taken over
        // like any other.
        //
        // Two things a wipe leaves: a completion in flight (the bucket may
        // be writing that object) and a row another call is reaping right
        // now (its claim is live). The wipe finishes everything else, then
        // fails naming how many it left, so a second wipe once they are
        // done takes what is still there.
        let now = self.clock.now_unix();
        let mut removed = 0;
        let mut left = 0;
        for entry in self.keys_under(prefix).await? {
            match self
                .fence_for_reap(&entry.key, &format!("AND (status <> 'reaping' OR {})", reap_lapsed("$2")), now)
                .await?
            {
                Fence::Fenced => {}
                Fence::Gone => continue,
                Fence::Spared | Fence::Completing => {
                    left += 1;
                    continue;
                }
            }
            self.reap_fenced(&entry).await?;
            removed += 1;
        }
        // A whole-tenant wipe takes the tenant's asset references with its
        // assets; every narrower prefix names no asset, so this matches none.
        sqlx::query("DELETE FROM asset_reference WHERE key LIKE $1 ESCAPE '\\'")
            .bind(like_prefix(prefix))
            .execute(&self.pool)
            .await
            .context("drop the wiped assets' references")?;
        if left > 0 {
            anyhow::bail!(
                "wiped {removed} files under '{prefix}' and left {left} that a completing upload or another \
                 removal in progress holds; wipe again once they are done"
            );
        }
        Ok(removed)
    }

    /// Terminate sweep: close out a run's un-kept run files (the
    /// `<tenant>/run/<run_id>/` prefix, kept files excepted).
    ///
    /// - A COMPLETED un-kept file is not deleted here: it gets
    ///   `expires_at_unix = now + RUN_LINGER_TTL_SECS` stamped, so the user
    ///   can still list/download a run's output right after the run ends; the
    ///   expiry loop removes it once the linger passes (the stamp rings it,
    ///   [`Self::ring_for`]). The stamped deadline
    ///   is what the file lists surface as the remaining lifetime.
    /// - A PENDING row (a crashed/abandoned upload) is reaped immediately,
    ///   in-flight multipart aborted: a half-uploaded file has nothing worth
    ///   downloading (even a kept-flagged one: keep only spares a COMPLETED
    ///   file). A leftover 'reaping' row whose claim lapsed is retried.
    ///
    /// Returns `(reaped, lingering)`: rows removed now vs stamped to expire.
    pub async fn sweep_run(&self, tenant: &str, run_id: &str) -> Result<(u64, u64)> {
        // Rendered through the key grammar (never hand-built): validates both
        // segments and keeps the scope tag single-sourced.
        let prefix = weft_core::storage::key::run_prefix(tenant, run_id)
            .map_err(|e| anyhow::anyhow!("sweep_run: {e}"))?;
        let mut reaped = 0;
        let mut lingering = 0;
        for entry in self.keys_under(&prefix).await? {
            if entry.kept_active {
                continue;
            }
            // A completion in flight lands first (driving it here is safe
            // even if its own caller is still driving it), so the file it
            // becomes gets the linger stamp below like any finished file.
            if entry.status == "completing" {
                if let Err(e) = self.drive_completion(&entry.key, false).await {
                    tracing::error!(
                        target: "weft_broker::runtime_store",
                        key = %entry.key, error = %e,
                        "a run's completing upload did not land; it gets the linger \
                         stamp now and the tenant's next upload lands it"
                    );
                }
            }
            if entry.status == "active" || entry.status == "completing" {
                // Completed un-kept file: stamp the linger deadline. A
                // completion still landing is stamped too, and its fold keeps
                // the stamp ([`Self::flip_active`]), so a file that lands after
                // its run ended lingers like the run's other files. The guard
                // re-checks the row is still un-kept (a keep that
                // landed since the scan wins and the file survives untouched),
                // and only stamps a NULL expiry so a re-delivered terminate
                // sweep (the queue is idempotent) can't keep pushing the
                // deadline out. The deadline honors any live public link on
                // the file (a minted link is a promise the bytes stay
                // fetchable for its stated lifetime).
                let stamped: Option<Option<i64>> = sqlx::query_scalar(&format!(
                    "UPDATE runtime_file SET expires_at_unix = {} \
                     WHERE key = $1 AND status IN ('active', 'completing') AND NOT keep \
                       AND expires_at_unix IS NULL \
                     RETURNING expires_at_unix",
                    expiry_honoring_links("$2")
                ))
                .bind(&entry.key)
                .bind(self.clock.now_unix() + RUN_LINGER_TTL_SECS)
                .fetch_optional(&self.pool)
                .await
                .context("linger stamp")?;
                if let Some(at) = stamped {
                    lingering += 1;
                    // The expiry loop must look by the linger's end; one
                    // ring covers the run's other files, stamped later.
                    self.ring_for(at).await;
                }
                continue;
            }
            // FENCE first: flip the row to 'reaping', atomically re-checking it
            // is still a pending row or a reap whose claim lapsed (a completion
            // that landed since the scan wins: the row is 'active' now and the
            // clause spares it; so does another call's reap in progress).
            // The flip locks out every writer and reader (record_part/reserve
            // gate on 'pending'; keep/get/download gate on 'active'), so the
            // bucket reap below can never race an in-flight upload, and a
            // crash at any point leaves a 'reaping' row this sweep's retry
            // (and the tenant's next upload) re-finds. Neither delete-object-first (a
            // charged row pointing at gone bytes on crash) nor
            // delete-row-first (an orphan object nothing row-driven ever
            // revisits) has that property.
            if self
                .fence_for_reap(&entry.key, &format!("AND (status = 'pending' OR {})", reap_lapsed("$2")), self.clock.now_unix())
                .await?
                != Fence::Fenced
            {
                continue;
            }
            self.reap_fenced(&entry).await?;
            reaped += 1;
        }
        Ok((reaped, lingering))
    }

    /// The expiry loop's pass: remove every file whose expiry passed (the
    /// store's clock) and finish every reap nobody is working on (a crash
    /// or a failure left it 'reaping'), then say when it next has a row to
    /// look at. Each row is fenced first (flipped to 'reaping' under its
    /// lock, re-checking it is still doomed: an access that renewed it
    /// since the scan spares it, and so does another call's reap in
    /// progress), so the bucket reap never races a writer, a reader or
    /// another reap. A row whose reap keeps failing is marked
    /// ([`Self::mark_reap_failed`]) and does not hold up the others.
    ///
    /// The next look is booked at the soonest of two times: the expiry of
    /// an active file, leaving out a file whose replacement is completing
    /// (it cannot go until the replacement lands, and landing rings,
    /// [`Self::finish_completion`]), and the lapse of a claim some call
    /// holds on a 'reaping' row (if that call dies, its row is taken
    /// then), a marked row a call took over included. A marked row nobody
    /// took over holds no claim and books nothing: its reap failed, and it
    /// waits for the next pass, a wipe or the tenant's next upload.
    pub async fn expire(&self) -> Result<weft_task_store::drain::DrainStep> {
        use weft_task_store::drain::DrainStep;
        let now = self.clock.now_unix();
        // `$1` in the scan and `$2` in the fence are the store's clock.
        let doomed = |now: &str| format!("((status = 'active' AND expires_at_unix < {now}) OR {})", reap_lapsed(now));
        let entries: Vec<SweepEntry> = sqlx::query_as(&format!(
            "SELECT key, FALSE AS kept_active, status, upload_id, replaces FROM runtime_file WHERE {}",
            doomed("$1")
        ))
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .context("expired file scan")?;
        let mut removed = 0;
        for entry in &entries {
            let fence = self.fence_for_reap(&entry.key, &format!("AND {}", doomed("$2")), now).await?;
            if fence == Fence::Fenced && self.reap_with_tries(entry, &REAP_RETRY_PAUSES).await? {
                removed += 1;
            }
        }
        if removed > 0 {
            tracing::info!(target: "weft_broker::runtime_store", removed, "expired stored files removed");
        }
        let next: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT MIN(at) FROM ( \
                 SELECT f.expires_at_unix AS at FROM runtime_file f \
                  WHERE f.status = 'active' AND f.expires_at_unix IS NOT NULL \
                    AND NOT EXISTS (SELECT 1 FROM runtime_file r WHERE r.replaces = f.key AND r.status = 'completing') \
               UNION ALL \
                 SELECT progressed_at_unix + {CLAIM_LEASE_SECS} FROM runtime_file \
                  WHERE status = 'reaping' AND progressed_at_unix > 0 \
             ) due"
        ))
        .fetch_one(&self.pool)
        .await
        .context("next file expiry")?;
        Ok(match next {
            // An expiry and a claim's lapse are a second's mark: the row
            // is doomed once it passed.
            Some(at) => DrainStep::RetryIn(std::time::Duration::from_secs(u64::try_from(at + 1 - now).unwrap_or(0))),
            None => DrainStep::Done,
        })
    }

    /// Mark a fenced row whose bucket reap failed with `error`, give its
    /// claim back (nobody is working on it any more, so the expiry loop's
    /// next pass, a wipe or the tenant's next upload takes it), and write
    /// the failure down ([`Self::record_reap_failures`]).
    async fn mark_reap_failed(&self, key: &str, error: &anyhow::Error) -> Result<()> {
        tracing::error!(
            target: "weft_broker::runtime_store", key = %key, error = %format!("{error:#}"),
            "a stored file could not be removed from the bucket; the expiry loop's next pass, a wipe or the tenant's \
             next upload tries it again"
        );
        sqlx::query("UPDATE runtime_file SET reap_error = $2, progressed_at_unix = 0 WHERE key = $1 AND status = 'reaping'")
            .bind(key)
            .bind(weft_task_store::unanswered::pg_text(&format!("{error:#}")))
            .execute(&self.pool)
            .await
            .context("mark a row whose reap failed")?;
        self.record_reap_failures().await
    }

    /// Write down, as the failure `weft status` shows (`Callee::Reaps`, a
    /// record of its own: the expiry loop writes and clears its own passes'
    /// failures), every row a reap failed on, or clear it once none is
    /// left. Run when a row is marked and when a marked row is removed,
    /// after that change committed. Two of these run one after the other
    /// (a transaction lock), so the last one reads every change made
    /// before it and what it writes stands.
    async fn record_reap_failures(&self) -> Result<()> {
        let mut tx = self.pool.begin().await.context("record failed reaps: begin tx")?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('weft:runtime-file-reaps', 0))")
            .execute(&mut *tx)
            .await
            .context("wait for another record of failed reaps")?;
        let (failing, shown): (i64, Option<String>) = sqlx::query_as(
            "SELECT COUNT(*), string_agg(key || ': ' || reap_error, '; ' ORDER BY key) FILTER (WHERE n <= 5) \
             FROM (SELECT key, reap_error, row_number() OVER (ORDER BY key) AS n FROM runtime_file \
                   WHERE status = 'reaping' AND reap_error IS NOT NULL) marked",
        )
        .fetch_one(&mut *tx)
        .await
        .context("read the rows a reap failed on")?;
        let callee = weft_task_store::unanswered::Callee::Reaps;
        let alerts = if failing == 0 {
            weft_task_store::unanswered::answered(&mut *tx, callee).await?;
            weft_task_store::unanswered::Alerts::default()
        } else {
            let error = format!(
                "{failing} stored file(s) could not be removed from the bucket ({}{}); they hold their tenant's \
                 quota until removed",
                shown.unwrap_or_default(),
                if failing > 5 { "; ..." } else { "" },
            );
            weft_task_store::unanswered::failed(&mut *tx, callee, None, &error).await?
        };
        tx.commit().await.context("record failed reaps: commit")?;
        alerts.log();
        Ok(())
    }

    /// Remove the active file stored under `identity` in `key`'s scope when
    /// it expired, so an upload of the same thing takes its place. A
    /// file whose replacement is completing is left (the begin then says
    /// it is expired and to retry).
    async fn reap_expired_identity(&self, key: &str, identity: &str) -> Result<()> {
        let now = self.clock.now_unix();
        let expired: Option<SweepEntry> = sqlx::query_as(
            "SELECT key, FALSE AS kept_active, status, upload_id, replaces FROM runtime_file \
             WHERE regexp_replace(key, '/[^/]+$', '') = $1 AND identity = $2 \
               AND status = 'active' AND expires_at_unix < $3",
        )
        .bind(scope_prefix(key))
        .bind(identity)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("look for an expired file under this identity")?;
        let Some(entry) = expired else { return Ok(()) };
        // Tried once, on the caller's upload: a bucket that refuses fails it.
        let fence = self.fence_for_reap(&entry.key, "AND status = 'active' AND expires_at_unix < $2", now).await?;
        if fence == Fence::Fenced {
            self.reap_fenced(&entry).await?;
        }
        Ok(())
    }
}

/// The ONE definition of a tenant's charged bytes in the runtime-file plane:
/// an active file counts by its size (nothing once it expired by `now`, the
/// store's clock), any other row by its quota-charged reservation. Every
/// quota check and usage view that needs the runtime-file footprint reads
/// through here so the numbers can never disagree.
pub async fn charged_bytes_for<'e, E>(executor: E, tenant: &str, now: i64) -> Result<u64>
where
    E: sqlx::PgExecutor<'e>,
{
    let bytes: Option<i64> = sqlx::query_scalar(&format!(
        "SELECT COALESCE(SUM(CASE WHEN status <> 'active' THEN reserved_bytes \
                                  WHEN {live} THEN size_bytes ELSE 0 END), 0)::BIGINT \
         FROM runtime_file WHERE tenant_id = $1",
        live = unexpired("$2"),
    ))
    .bind(tenant)
    .bind(now)
    .fetch_one(executor)
    .await
    .context("read tenant charged bytes")?;
    Ok(bytes.unwrap_or(0) as u64)
}

/// How many files a tenant holds against its file cap: every row but an
/// active file expired by `now`.
async fn count_files<'e, E>(executor: E, tenant: &str, now: i64) -> Result<u64>
where
    E: sqlx::PgExecutor<'e>,
{
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM runtime_file WHERE tenant_id = $1 AND (status <> 'active' OR {live})",
        live = unexpired("$2"),
    ))
    .bind(tenant)
    .bind(now)
    .fetch_one(executor)
    .await
    .context("count tenant files")?;
    Ok(count as u64)
}

