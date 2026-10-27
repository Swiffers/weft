//! What the install's image endpoints answer and the CLI reads: one Rust
//! type per answer, so the two ends cannot drift. And the record of which
//! shared images each local install uses ([`ImageRefs`]), written by its
//! start and read by the machine-wide sweeps.

use serde::{Deserialize, Serialize};

/// `POST /images/prune`: which images to prune. No project is every
/// project's; a project is only that one's.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PruneRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<uuid::Uuid>,
}

/// `POST /images/prune`: what a prune did. Every field is required, so an
/// answer missing one is a dispatcher the CLI does not match, never an
/// empty list.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PruneReport {
    pub removed: Vec<String>,
    /// Images a container still runs from (a removed project's unit
    /// whose teardown has not finished), or a build in progress is about
    /// to register; the next prune takes them.
    pub in_use: Vec<String>,
    /// Images that could not be deleted, with why; the next prune tries
    /// them again.
    pub failed: Vec<(String, String)>,
}

/// What one local install uses of what every install on the machine
/// shares, kept in its folder (`<install dir>/image-refs`) so a
/// machine-wide cleanup keeps every install's and drops only what none of
/// them uses. A `weft daemon start` writes it whole before it builds or
/// pulls anything it names.
///
/// It is a record between versions of weft: one install per branch means
/// installs of older wefts may sit beside this one, reading every
/// install's record with this same shape. This weft fills only `runtime`
/// and `standard_worker`, and writes the other two empty; an older weft
/// fills them with the Docker builder bases and BuildKit compile caches
/// its image builds use, which the sweeps keep for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImageRefs {
    /// `<registry>/weft-runtime:<hash>`, what its runtime and its infra
    /// units' agents run.
    pub runtime: String,
    /// The standard worker: the worker of the whole base catalog. On this
    /// weft the program `weft-worker:<hash>` compiled on the machine; an
    /// older weft names a registry image.
    pub standard_worker: String,
    /// The Docker builder bases an older weft's worker images compile on
    /// (`<registry>/weft-builder-base:<hash>`). Empty from this weft.
    pub builder_bases: std::collections::BTreeSet<String>,
    /// The BuildKit compile caches an older weft's worker image builds
    /// mount, by key. Empty from this weft.
    pub compile_cache_keys: std::collections::BTreeSet<String>,
}

/// The record's file name in an install's folder.
pub const IMAGE_REFS_FILE: &str = "image-refs";

impl ImageRefs {
    /// The record in the install folder `dir`, `None` when it has none.
    pub fn load(dir: &std::path::Path) -> anyhow::Result<Option<Self>> {
        use anyhow::Context as _;
        let path = dir.join(IMAGE_REFS_FILE);
        match std::fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).map(Some).with_context(|| {
                format!("{} is not a valid image record; a `weft daemon start` of its install writes it anew", path.display())
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
        }
    }

    /// Make this the record of the install folder `dir`. Written whole and
    /// renamed into place, so a sweep reading it sees the old record or the
    /// new one, never half of one.
    pub fn save(&self, dir: &std::path::Path) -> anyhow::Result<()> {
        let _turn = Self::turn(dir)?;
        self.write(dir)
    }

    fn write(&self, dir: &std::path::Path) -> anyhow::Result<()> {
        use anyhow::Context as _;
        let bytes = serde_json::to_vec_pretty(self)?;
        crate::infra::write_whole(dir, IMAGE_REFS_FILE, &bytes, 0o644)
            .with_context(|| format!("write {}", dir.join(IMAGE_REFS_FILE).display()))
    }

    /// The record of the install folder `dir`, read under its lock, with
    /// the lock: while the caller keeps the returned file, no start replaces
    /// the record (and no build of an older weft adds to it). A machine-wide
    /// sweep holds every install's this way while it checks and removes, so
    /// what is recorded meanwhile is never removed by it.
    ///
    /// Makes nothing: a folder with no lock file has no record either (a
    /// start makes the lock before it writes the record), so it is `None`
    /// with no lock, and a sweep never brings back a folder a removal just
    /// took.
    /// `on_wait` is said once when someone holds it. Blocking: an async
    /// caller runs it on its blocking pool.
    pub fn load_held(dir: &std::path::Path, on_wait: impl FnOnce()) -> anyhow::Result<(Option<Self>, Option<std::fs::File>)> {
        use anyhow::Context as _;
        let path = Self::lock_path(dir);
        let Some(turn) = crate::file_lock::hold_existing(&path, on_wait).with_context(|| format!("lock {}", path.display()))? else {
            return Ok((None, None));
        };
        Ok((Self::load(dir)?, Some(turn)))
    }

    /// The lock every change to the record of `dir` holds, beside it: this
    /// weft's start, and an older weft's builds, which add to the record
    /// with a read and a write.
    fn turn(dir: &std::path::Path) -> anyhow::Result<std::fs::File> {
        use anyhow::Context as _;
        let path = Self::lock_path(dir);
        crate::file_lock::hold(&path, || {}).with_context(|| format!("lock {}", path.display()))
    }

    fn lock_path(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join(format!("{IMAGE_REFS_FILE}.lock"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(n: u8) -> ImageRefs {
        ImageRefs {
            runtime: format!("ghcr.io/weavemindai/weft-runtime:r{n}"),
            standard_worker: format!("ghcr.io/weavemindai/weft-worker:w{n}"),
            builder_bases: [format!("ghcr.io/weavemindai/weft-builder-base:b{n}")].into(),
            compile_cache_keys: [format!("k{n}")].into(),
        }
    }

    #[test]
    fn a_record_is_written_whole_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(ImageRefs::load(dir.path()).unwrap(), None);
        record(1).save(dir.path()).unwrap();
        assert_eq!(ImageRefs::load(dir.path()).unwrap(), Some(record(1)));
        let on_disk: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.path().join(IMAGE_REFS_FILE)).unwrap()).unwrap();
        assert_eq!(on_disk["builderBases"], serde_json::json!(["ghcr.io/weavemindai/weft-builder-base:b1"]));
        assert_eq!(on_disk["compileCacheKeys"], serde_json::json!(["k1"]));
        let left: Vec<String> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        assert!(left.iter().all(|f| !f.ends_with(".tmp")), "no partial file left behind: {left:?}");
        std::fs::write(dir.path().join(IMAGE_REFS_FILE), "{}").unwrap();
        let error = format!("{:#}", ImageRefs::load(dir.path()).unwrap_err());
        assert!(error.contains("weft daemon start"), "a record missing a field fails loud, naming the fix: {error}");
    }
}
