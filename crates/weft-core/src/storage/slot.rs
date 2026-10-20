//! The upload slot: how a page's visitor sends a file of any size
//! straight to storage, and how the route the page calls next receives it.
//!
//! A program asks for a slot (`StorageHandle::upload_slot`, the
//! `UploadSlot` node): a signed grant naming the project, the scope the
//! file lands in, how big and of what type it may be, and until when it
//! works. The page hands it to `uploadFile` (`@weft/connect`), which runs
//! the four-step upload through the install (begin, parts, part-done,
//! complete) with each part PUT straight to storage, and answers the
//! slot. The page then sends the slot in its call to a route, in a place
//! declared as a file, and the route receives the stored file
//! (`StorageHandle::claim_uploads`, every slot of one call together or
//! none). A slot uploads one file and is claimed once; an upload nobody claims goes when its slot ends.
//!
//! The grant is a signed token ([`crate::signed_token`]) keyed from the
//! install's secret, so only the broker mints and reads one, and a page
//! can neither forge a slot nor stretch one.

use serde::{Deserialize, Serialize};

use super::{KeepTtl, StorageScope};
use crate::signed_token::{self, SignedClaims};

/// What an upload slot is written as, in a page's call to a route:
/// `weft-upload:<token>`.
// SYNC: SLOT_PREFIX <-> packages/weft-connect/src/core/upload.ts SLOT_PREFIX
pub const SLOT_PREFIX: &str = "weft-upload:";

/// The longest a slot works: seven days, as long as a link to the parts
/// it signs can live.
pub const MAX_SLOT_MINUTES: u64 = 7 * 24 * 60;

/// What a token that is no good is called in the refusals.
const NOUN: &str = "upload slot";

/// `POST /v1/storage/upload-slot`: a slot for one upload into `scope`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadSlotRequest {
    /// The most the file may weigh.
    pub max_bytes: u64,
    /// The file type the upload must be (`Video`, `Image | Video`,
    /// `File`), held to its first bytes once it landed.
    pub accepts: String,
    /// How long the slot works, from now: the upload and the claim both
    /// happen before it ends.
    pub minutes: u64,
    /// Where the file lands: `project` (and `instance`) when it is stored,
    /// `run` in the run that claims it. Never `shared` or `asset`.
    pub scope: StorageScope,
    /// How long the file lives once claimed; `None` for its scope's own
    /// answer (until deleted outside `run`, swept with its run inside it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep: Option<KeepTtl>,
}

impl UploadSlotRequest {
    /// Refuse what no slot can be, naming the setting.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_bytes == 0 {
            return Err("an upload slot's largest size is at least one byte".into());
        }
        if self.minutes == 0 || self.minutes > MAX_SLOT_MINUTES {
            return Err(format!("an upload slot works 1 to {MAX_SLOT_MINUTES} minutes (7 days), not {}", self.minutes));
        }
        accepted_type(&self.accepts)?;
        match self.scope {
            StorageScope::Project | StorageScope::Instance { .. } | StorageScope::Run => Ok(()),
            StorageScope::Shared { .. } | StorageScope::Asset => {
                Err("an uploaded file lands in the project, an instance or the run that receives it".into())
            }
        }
    }
}

/// The file type a slot's `accepts` names, refused unless it is one a
/// file can be (`Image`, `Video | Audio`, `File`).
pub fn accepted_type(accepts: &str) -> Result<crate::weft_type::WeftType, String> {
    let ty = crate::weft_type::WeftType::try_parse(accepts).map_err(|why| format!("'{accepts}' is not a type: {why}"))?;
    if !ty.is_file_valued() {
        return Err(format!("an upload is a file, so the type it accepts is a file type (Image, Video, Audio, File...), not {accepts}"));
    }
    Ok(ty)
}

/// The answer to an [`UploadSlotRequest`]: the slot, as a page hands it
/// on (`weft-upload:<token>`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadSlotResponse {
    pub slot: String,
}

/// What a slot grants, signed into it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadSlotClaims {
    /// The slot's own id: what its one upload is recorded under, and so
    /// what makes it single use.
    pub sid: String,
    pub tenant: String,
    pub project: String,
    /// Where the file lands, an instance already named.
    pub scope: StorageScope,
    pub max_bytes: u64,
    pub accepts: String,
    /// Unix seconds at which the slot stops working.
    pub exp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep: Option<KeepTtl>,
}

impl SignedClaims for UploadSlotClaims {
    fn exp(&self) -> i64 {
        self.exp
    }
}

/// The key slots are signed with, derived from the install's secret.
#[derive(Clone)]
pub struct SlotKey(Vec<u8>);

impl std::fmt::Debug for SlotKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SlotKey(..)")
    }
}

impl SlotKey {
    pub fn of(install_secret: &[u8]) -> Self {
        Self(signed_token::derive_key(install_secret, "upload-slot"))
    }

    /// The slot for `claims`, as a page hands it on.
    pub fn mint(&self, claims: &UploadSlotClaims) -> String {
        format!("{SLOT_PREFIX}{}", signed_token::mint(&self.0, claims))
    }

    /// What the slot `text` grants, or why it is no good (not a slot,
    /// forged, ended). The reasons are safe to show the page.
    pub fn read(&self, text: &str, now_unix: i64) -> Result<UploadSlotClaims, String> {
        let token = text
            .strip_prefix(SLOT_PREFIX)
            .ok_or_else(|| format!("an upload slot starts with '{SLOT_PREFIX}'"))?;
        signed_token::validate(&self.0, token, now_unix, NOUN)
    }
}

/// Whether `text` is written as an upload slot (the place a page sends one
/// in is then answered with the file it names).
pub fn is_slot(text: &str) -> bool {
    text.starts_with(SLOT_PREFIX)
}

/// A browser's upload verb: the slot it uploads through, around the
/// verb's own body (the same bodies the worker's uploads send).
// SYNC: Slotted <-> packages/weft-connect/src/core/upload.ts (the verb bodies)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Slotted<T> {
    pub slot: String,
    #[serde(flatten)]
    pub inner: T,
}

/// `POST <install>/files/upload/begin`: start the slot's one upload. The
/// size is known up front (a browser holds the whole file) and held to the
/// slot's largest. `sha256` is the browser's hash of the file: when the
/// install already holds a file it hashed itself with that content, the
/// upload is made from it inside storage and no byte is sent.
// SYNC: SlotBeginRequest <-> packages/weft-connect/src/core/upload.ts SlotBegin
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SlotBeginRequest {
    pub filename: String,
    /// The type the browser states (`File.type`); empty when it has none,
    /// and then the file's first bytes name it.
    #[serde(default)]
    pub mime_type: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// `POST /v1/storage/upload/claim`: the files some slots uploaded,
/// received by the run asking (each slot once). Every slot of one call is
/// received together or none is: one refused slot leaves the others open
/// for a corrected call, which is a new run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimUploadRequest {
    pub claims: Vec<SlotClaim>,
}

/// One slot of a [`ClaimUploadRequest`]. `fits` is the type of the place
/// that receives its file (`Image`, `Video | Null`): a file that does not
/// fit is refused BEFORE it is received, so the slot stays open for a call
/// that sends it where it fits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotClaim {
    pub slot: String,
    pub fits: String,
}

/// The answer to a [`ClaimUploadRequest`]: each slot's stored-file value,
/// in the order of the claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimUploadResponse {
    pub files: Vec<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(scope: StorageScope) -> UploadSlotRequest {
        UploadSlotRequest { max_bytes: 10, accepts: "Video".into(), minutes: 60, scope, keep: None }
    }

    fn claims(exp: i64) -> UploadSlotClaims {
        UploadSlotClaims {
            sid: "s1".into(),
            tenant: "t".into(),
            project: "p".into(),
            scope: StorageScope::Project,
            max_bytes: 10,
            accepts: "Video".into(),
            exp,
            keep: None,
        }
    }

    #[test]
    fn a_slot_lands_a_file_in_the_project_an_instance_or_the_run() {
        for scope in [StorageScope::Project, StorageScope::instance(), StorageScope::Run] {
            assert!(ask(scope).validate().is_ok());
        }
        assert!(ask(StorageScope::Shared { name: "x".into() }).validate().is_err());
        assert!(ask(StorageScope::Asset).validate().is_err());
        assert!(UploadSlotRequest { minutes: 0, ..ask(StorageScope::Project) }.validate().is_err());
        assert!(UploadSlotRequest { minutes: MAX_SLOT_MINUTES + 1, ..ask(StorageScope::Project) }.validate().is_err());
        assert!(UploadSlotRequest { max_bytes: 0, ..ask(StorageScope::Project) }.validate().is_err());
    }

    #[test]
    fn a_slot_accepts_a_file_type_only() {
        for good in ["Video", "Image | Video", "File", "Media"] {
            assert!(accepted_type(good).is_ok(), "{good}");
        }
        for bad in ["String", "Video | String", "List[Video]", "nonsense ["] {
            assert!(accepted_type(bad).is_err(), "{bad}");
        }
    }

    /// A slot reads back what it was minted with until it ends, and
    /// another install's slot, a stretched one or a plain string reads as
    /// no slot.
    #[test]
    fn a_slot_is_only_what_this_install_signed() {
        let key = SlotKey::of(b"secret");
        let slot = key.mint(&claims(100));
        assert!(is_slot(&slot));
        assert_eq!(key.read(&slot, 99).unwrap(), claims(100));
        assert!(key.read(&slot, 100).unwrap_err().contains("expired"));
        assert!(SlotKey::of(b"other").read(&slot, 0).is_err());
        let token = slot.strip_prefix(SLOT_PREFIX).unwrap();
        assert!(key.read(token, 0).unwrap_err().contains(SLOT_PREFIX));
        let (head, sig) = token.rsplit_once('.').unwrap();
        let stretched = signed_token::mint(b"secret", &claims(10_000));
        let (_, other_payload) = stretched.split_once('.').unwrap();
        let (other_payload, _) = other_payload.split_once('.').unwrap();
        let forged = format!("{SLOT_PREFIX}v1.{other_payload}.{sig}");
        assert!(key.read(&forged, 0).is_err(), "{head}");
    }

    /// The verb bodies read exactly what `uploadFile` in
    /// `packages/weft-connect/src/core/upload.ts` sends: the slot beside
    /// the verb's own camelCase fields.
    #[test]
    fn the_browsers_verb_bodies_read_as_sent() {
        let begin: Slotted<SlotBeginRequest> = serde_json::from_value(serde_json::json!({
            "slot": "weft-upload:t",
            "filename": "clip.mp4",
            "mimeType": "video/mp4",
            "sizeBytes": 12,
            "sha256": "ab12",
        }))
        .unwrap();
        assert_eq!(begin.slot, "weft-upload:t");
        assert_eq!(
            (begin.inner.filename.as_str(), begin.inner.mime_type.as_str(), begin.inner.size_bytes, begin.inner.sha256.as_deref()),
            ("clip.mp4", "video/mp4", 12, Some("ab12"))
        );
        // `check` sends the same file without its hash, and a file the
        // browser names no type for states an empty one.
        let check: Slotted<SlotBeginRequest> = serde_json::from_value(serde_json::json!({
            "slot": "weft-upload:t",
            "filename": "notes",
            "mimeType": "",
            "sizeBytes": 3,
        }))
        .unwrap();
        assert_eq!((check.inner.mime_type.as_str(), check.inner.sha256), ("", None));

        let done: Slotted<crate::storage::PartDoneRequest> = serde_json::from_value(serde_json::json!({
            "slot": "weft-upload:t",
            "key": "t/project/p/f1",
            "partNumber": 2,
            "etag": "\"9a0364b9e99bb480dd25e1f0284c8555\"",
        }))
        .unwrap();
        assert_eq!(done.slot, "weft-upload:t");
        assert_eq!(
            (done.inner.key.as_str(), done.inner.part_number, done.inner.etag.as_str()),
            ("t/project/p/f1", 2, "\"9a0364b9e99bb480dd25e1f0284c8555\"")
        );
    }

    /// A claim names, for every slot, the type of the place receiving its
    /// file, and a slot that names none is no claim; the answer is the
    /// files in the claims' order.
    #[test]
    fn a_claim_carries_the_type_each_slot_must_fit() {
        let claim = ClaimUploadRequest {
            claims: vec![
                SlotClaim { slot: "weft-upload:a".into(), fits: "Image | Null".into() },
                SlotClaim { slot: "weft-upload:b".into(), fits: "Video".into() },
            ],
        };
        let wire = serde_json::to_value(&claim).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({"claims": [
                {"slot": "weft-upload:a", "fits": "Image | Null"},
                {"slot": "weft-upload:b", "fits": "Video"},
            ]})
        );
        let back: ClaimUploadRequest = serde_json::from_value(wire).unwrap();
        assert_eq!(back.claims, claim.claims);
        assert!(serde_json::from_value::<ClaimUploadRequest>(serde_json::json!({"claims": [{"slot": "weft-upload:t"}]})).is_err());
        assert!(serde_json::from_value::<ClaimUploadRequest>(serde_json::json!({"slot": "weft-upload:t", "fits": "File"})).is_err());

        let answer = ClaimUploadResponse { files: vec![serde_json::json!({"key": "a"}), serde_json::json!({"key": "b"})] };
        let back: ClaimUploadResponse = serde_json::from_value(serde_json::to_value(&answer).unwrap()).unwrap();
        assert_eq!(back.files, answer.files);
    }
}
