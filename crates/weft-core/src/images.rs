//! What the install's image endpoints answer and the CLI reads: one Rust
//! type per answer, so the two ends cannot drift. And the record of which
//! shared images each local install uses ([`ImageRefs`]), written by its
//! start and its builds and read by the machine-wide sweeps.

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

/// The images and compile caches one local install uses, kept in its
/// folder (`<install dir>/image-refs`) so a machine-wide cleanup keeps
/// every install's and drops only what none of them uses.
///
/// The builder base, the runtime image, the standard worker and the
/// worker compile cache are content-addressed and shared by every install
/// on the machine: two installs built from the same source use the same
/// ones, two built from different branches use different ones side by
/// side. A `weft daemon start` writes the record whole before it builds
/// or pulls anything it names; every worker build adds the builder base
/// and the compile cache it picked, since those follow the live checkout
/// and move when it does (a pull, a branch switch) while the install
/// runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImageRefs {
    /// `<registry>/weft-runtime:<hash>`, what its runtime runs.
    pub runtime: String,
    /// `<registry>/weft-worker:<hash>`, the worker of the whole base
    /// catalog.
    pub standard_worker: String,
    /// `<registry>/weft-builder-base:<hash>`: what its workers and the
    /// node-test images built for it compile on.
    pub builder_bases: std::collections::BTreeSet<String>,
    /// The BuildKit compile caches its worker builds mount, by key
    /// (`weft_compiler::hash::compute_worker_cache_key`).
    pub compile_cache_keys: std::collections::BTreeSet<String>,
}

/// What one image build uses of what an install keeps on this machine:
/// the builder base it builds FROM and the compile cache it mounts, both
/// picked from the live checkout when it is staged. A build using neither
/// (an infra image) carries the default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuildUses {
    pub builder_base: Option<String>,
    pub compile_cache_key: Option<String>,
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

    /// Make this the record of the install folder `dir` (a start: what
    /// earlier builds added goes, since the runtime they built for is
    /// replaced). Written whole and renamed into place, so a sweep reading
    /// it sees the old record or the new one, never half of one.
    pub fn save(&self, dir: &std::path::Path) -> anyhow::Result<()> {
        let _turn = Self::turn(dir, || {})?;
        self.write(dir)
    }

    /// Add what a build on this machine uses to the record of the install
    /// folder `dir`: the builder base it builds FROM, the compile cache it
    /// mounts. An install with no record keeps everything of every kind
    /// already (a sweep removes nothing while one exists), so there is
    /// nothing to add to and nothing is written. The read and the write
    /// happen under the record's lock, so two builds adding at once both
    /// land.
    /// `on_wait` is said once when a cleanup holds the record (for one
    /// removal at most). Blocking: an async caller runs it on its blocking
    /// pool.
    pub fn add_build(dir: &std::path::Path, uses: &BuildUses, on_wait: impl FnOnce()) -> anyhow::Result<()> {
        if !dir.join(IMAGE_REFS_FILE).exists() {
            return Ok(());
        }
        let _turn = Self::turn(dir, on_wait)?;
        let Some(mut record) = Self::load(dir)? else { return Ok(()) };
        let added_base = uses.builder_base.as_ref().is_some_and(|b| record.builder_bases.insert(b.clone()));
        let added_key = uses.compile_cache_key.as_ref().is_some_and(|k| record.compile_cache_keys.insert(k.clone()));
        if added_base || added_key {
            record.write(dir)?;
        }
        Ok(())
    }

    fn write(&self, dir: &std::path::Path) -> anyhow::Result<()> {
        use anyhow::Context as _;
        let bytes = serde_json::to_vec_pretty(self)?;
        crate::infra::write_whole(dir, IMAGE_REFS_FILE, &bytes)
            .with_context(|| format!("write {}", dir.join(IMAGE_REFS_FILE).display()))
    }

    /// The record of the install folder `dir`, read under its lock, with
    /// the lock: while the caller keeps the returned file, no build adds to
    /// the record and no start replaces it. A machine-wide sweep holds every
    /// install's this way while it checks and removes, so what a build
    /// records meanwhile is never removed by it: the build waits, records,
    /// then builds (pulling or building again what the sweep took).
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

    /// The lock every change to the record of `dir` holds, beside it (a
    /// change is a read and a write, and two at once would lose one).
    fn turn(dir: &std::path::Path, on_wait: impl FnOnce()) -> anyhow::Result<std::fs::File> {
        use anyhow::Context as _;
        let path = Self::lock_path(dir);
        crate::file_lock::hold(&path, on_wait).with_context(|| format!("lock {}", path.display()))
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

    #[test]
    fn a_build_adds_what_it_picked_and_a_start_replaces_it_all() {
        let dir = tempfile::tempdir().unwrap();
        ImageRefs::add_build(dir.path(), &BuildUses { builder_base: Some("b9".to_string()), compile_cache_key: Some("k9".to_string()) }, || {}).unwrap();
        assert_eq!(ImageRefs::load(dir.path()).unwrap(), None, "an install with no record keeps everything; nothing is written");

        record(1).save(dir.path()).unwrap();
        ImageRefs::add_build(dir.path(), &BuildUses { builder_base: Some("ghcr.io/weavemindai/weft-builder-base:b2".to_string()), compile_cache_key: Some("k2".to_string()) }, || {}).unwrap();
        ImageRefs::add_build(dir.path(), &BuildUses { builder_base: None, compile_cache_key: Some("k2".to_string()) }, || {}).unwrap();
        ImageRefs::add_build(dir.path(), &BuildUses { builder_base: None, compile_cache_key: Some("k3".to_string()) }, || {}).unwrap();
        let grown = ImageRefs::load(dir.path()).unwrap().unwrap();
        assert_eq!(
            grown.builder_bases,
            ["ghcr.io/weavemindai/weft-builder-base:b1".to_string(), "ghcr.io/weavemindai/weft-builder-base:b2".to_string()].into()
        );
        assert_eq!(grown.compile_cache_keys, ["k1".to_string(), "k2".to_string(), "k3".to_string()].into());
        assert_eq!((grown.runtime, grown.standard_worker), (record(1).runtime, record(1).standard_worker));

        record(4).save(dir.path()).unwrap();
        assert_eq!(ImageRefs::load(dir.path()).unwrap(), Some(record(4)));
    }

    /// Builds adding side by side all land: each read-modify-write holds
    /// the record's lock.
    #[test]
    fn adds_at_once_all_land() {
        let dir = tempfile::tempdir().unwrap();
        record(0).save(dir.path()).unwrap();
        let threads: Vec<_> = (0..16)
            .map(|n| {
                let dir = dir.path().to_path_buf();
                std::thread::spawn(move || ImageRefs::add_build(&dir, &BuildUses { builder_base: None, compile_cache_key: Some(format!("key{n}")) }, || {}).unwrap())
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(ImageRefs::load(dir.path()).unwrap().unwrap().compile_cache_keys.len(), 17);
    }
}
