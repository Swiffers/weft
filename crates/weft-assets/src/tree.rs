//! A project folder on disk as a version sees it: which files the walk
//! takes ([`walk_folder`], over the rule in
//! `weft_core::project::coverage::covers`), what each one hashes to
//! ([`disk_manifest`], remembering hashes in a [`HashCache`] so an
//! unchanged file is not read again), and how a file arriving from
//! elsewhere is put in place or taken out ([`write_in_place`],
//! [`remove_in_place`]). The CLI's snapshot and a switch read the folder
//! through this, and so does the local runtime's folder sync, so the two
//! can never disagree about what a project is made of.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use weft_core::project::coverage::{covers, is_conflict_copy};
use weft_core::project::hash::Manifest;

/// What one walk of a project folder found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Walked {
    /// Every covered file, project-relative with forward slashes, sorted.
    pub files: Vec<String>,
    /// Every conflict copy the walk passed (a file the sync wrote beside
    /// one both sides changed), sorted. Never in `files`: a copy is the
    /// person's to look at and delete, not part of a version.
    pub conflicts: Vec<String>,
}

/// Walk the project at `root`: its covered files, and the conflict copies
/// sitting among them.
pub fn walk_folder(root: &Path) -> Result<Walked> {
    let mut out = Walked::default();
    walk(root, root, &mut out)?;
    out.files.sort();
    out.conflicts.sort();
    Ok(out)
}

/// Every covered file under `root`, project-relative with forward
/// slashes, sorted: [`walk_folder`]'s `files`.
pub fn covered_paths(root: &Path) -> Result<Vec<String>> {
    Ok(walk_folder(root)?.files)
}

fn walk(root: &Path, dir: &Path, out: &mut Walked) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let raw = path.strip_prefix(root).expect("under root");
        let Some(rel) = raw.to_str() else {
            // A path whose bytes are not UTF-8 cannot be a manifest key.
            // `to_string_lossy` would have spelled it with replacement
            // characters, and that spelling decides what is IN the
            // version: the file would be published under a name that is
            // not its name, read as both added and removed by every
            // dirty check (the disk name never equals the lossy one), and
            // restored beside the original rather than over it.
            //
            // Refused by name, but only when a version would actually
            // take it. Such a name in a folder no version reads is not
            // this walk's business, and failing every snapshot over a
            // file that would never be recorded is worse than passing it
            // over.
            let lossy = raw.to_string_lossy().replace('\\', "/");
            let wanted = if path.is_dir() { !prunes_whole_tree(&lossy) } else { covers(&lossy) };
            if wanted {
                bail!(
                    "'{}' is not a name weft can record in a version (its bytes are not UTF-8); \
                     rename it, or move it out of the project",
                    path.display()
                );
            }
            continue;
        };
        let rel = rel.replace('\\', "/");
        let is_symlink = entry.file_type()?.is_symlink();
        // `is_dir` FOLLOWS a symlink, so this has to be asked before any
        // question about what the entry is.
        if path.is_dir() && !is_symlink {
            // Prune whole trees the rule never covers, so a large
            // `.weft/target` is never walked.
            let cargo_output = is_cargo_output(&path);
            // Pruning asks `covers` about the directory as a path, so
            // the two cannot disagree: spelled out again here, `.weft`
            // without its slash also pruned a folder called
            // `.weft-notes` that `covers` would have included, and only
            // the walk's opinion reaches the disk. The one thing `covers`
            // cannot answer is `cargo_output`, which is a fact about
            // what sits next to the directory rather than about its path.
            if !(cargo_output || prunes_whole_tree(&rel)) {
                walk(root, &path, out)?;
            }
        } else if is_symlink {
            // A link where a version's own file or folder would be is
            // refused by name. Followed, a link pointing at a folder above
            // it is an endless walk (a stack overflow, with no message),
            // and one pointing OUT of the project pulls somebody else's
            // files in under a path inside it, which a switch would later
            // write back THROUGH.
            //
            // A link anywhere else is nobody's business here, and most
            // links in a project are exactly that: a symlinked `README.md`
            // or `docker-compose.yml` at the root is ordinary, and no
            // version would ever take it. So the question asked matches
            // what the link IS. `path.is_dir()` follows the link, which is
            // safe to ASK (it recurses nothing) and is the only way to know
            // which of the two questions applies.
            let refuse = if path.is_dir() { !prunes_whole_tree(&rel) } else { covers(&rel) };
            if refuse {
                bail!(
                    "'{rel}' is a symlink, and a version is the files themselves: a link can \
                     point out of the project or back at a folder above it, and switching back \
                     would write through it. Replace it with the file, or move it out of the \
                     project"
                );
            }
        } else if covers(&rel) {
            out.files.push(rel);
        } else if entry.file_name().to_str().is_some_and(is_conflict_copy) {
            out.conflicts.push(rel);
        }
    }
    Ok(())
}

/// Whether the directory at `dir` is Cargo's build output: a `target/`
/// with a `Cargo.toml` beside it. A fact about the disk, not the path,
/// which is why the pure `covers` cannot make this call and the walk
/// does.
fn is_cargo_output(dir: &Path) -> bool {
    dir.file_name().is_some_and(|name| name == "target") && dir.parent().is_some_and(|parent| parent.join("Cargo.toml").is_file())
}

/// Whether the walk takes the file at `rel` (project-relative, forward
/// slashes) into a version: `covers` says so, and no folder on its way
/// is Cargo's build output. The one answer to "is this a version's file"
/// for a path the walk did not list itself.
pub fn walk_takes(root: &Path, rel: &str) -> bool {
    if !covers(rel) {
        return false;
    }
    let mut dir = root.to_path_buf();
    let mut parts: Vec<&str> = rel.split('/').collect();
    parts.pop();
    parts.into_iter().all(|part| {
        dir.push(part);
        !is_cargo_output(&dir)
    })
}

/// Whether a DIRECTORY holds nothing a version covers, so the walk can
/// skip it whole.
///
/// Asks `covers` rather than restating it: a path under this directory
/// can only be covered if the directory itself could be, and the two
/// spellings drifting apart is how a folder ends up silently in or out
/// of a version.
fn prunes_whole_tree(rel: &str) -> bool {
    // A directory's own path is not a file path, so `covers` is asked
    // about a file inside it. `.weft` is covered nowhere; `nodes` is
    // covered only through its contents.
    !covers(&format!("{rel}/x.weft")) && !covers(&format!("{rel}/x"))
}

/// Open the covered file `rel` under `root` for one read, `None` when it
/// was deleted since the walk named it ([`walk_folder`]): another person
/// or agent removing a file mid-walk is the disk changing, not an error.
pub fn open_covered(root: &Path, rel: &str) -> Result<Option<std::fs::File>> {
    let path = root.join(rel);
    match std::fs::File::open(&path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

// ----- what the files hash to --------------------------------------------

/// The project at `root` as it is on disk now: what each covered file
/// hashes to, and the conflict copies sitting among them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiskState {
    pub manifest: Manifest,
    pub conflicts: Vec<String>,
}

/// How long a file's modification time stays too close to the moment it
/// was hashed to vouch for the hash. A file written again within the same
/// tick of a coarse clock (two seconds on FAT, one on some network
/// filesystems) keeps the time it had, so its size and time can match
/// while its bytes do not. Git's racy-clean rule, with the widest tick.
const RACY_WINDOW_NS: u64 = 2_000_000_000;

/// The hashes [`disk_manifest`] took last time, keyed by project-relative
/// path, kept at `<root>/.weft/hash-cache.json`. It only ever saves a
/// read: an entry is used when the file's size and modification time are
/// what they were and that time is safely older than the hash, and every
/// other file is read again. So a cache that is missing, unreadable or
/// from another shape is an empty one, never an error.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashCache {
    entries: BTreeMap<String, CacheEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CacheEntry {
    size: u64,
    mtime_ns: u64,
    /// When the hash was taken, read BEFORE the file was looked at, so a
    /// write landing while it was read is never counted as older.
    recorded_at_ns: u64,
    sha256: String,
}

impl CacheEntry {
    /// Whether this entry still answers for a file of `size` modified at
    /// `mtime_ns`.
    fn vouches_for(&self, size: u64, mtime_ns: u64) -> bool {
        self.size == size && self.mtime_ns == mtime_ns && mtime_ns < self.recorded_at_ns.saturating_sub(RACY_WINDOW_NS)
    }
}

fn cache_file(root: &Path) -> PathBuf {
    root.join(".weft").join("hash-cache.json")
}

impl HashCache {
    /// The cache saved under `root`, or an empty one when there is none
    /// or it does not read.
    pub fn load(root: &Path) -> Self {
        std::fs::read(cache_file(root))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    /// Write the cache under `root`, whole or not at all: a reader never
    /// sees half a file.
    pub fn save(&self, root: &Path) -> Result<()> {
        let path = cache_file(root);
        let dir = path.parent().expect("the cache file has a folder");
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let mut temp = tempfile::NamedTempFile::new_in(dir).with_context(|| format!("create a file in {}", dir.display()))?;
        serde_json::to_writer(&mut temp, self).context("write the hash cache")?;
        temp.persist(&path).with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }
}

/// `t` in nanoseconds since the epoch, `None` when it does not fit (a
/// time before 1970, or past 2554).
fn epoch_ns(t: SystemTime) -> Option<u64> {
    t.duration_since(UNIX_EPOCH).ok().and_then(|d| u64::try_from(d.as_nanos()).ok())
}

/// The project at `root` as it is on disk, hashing only the files `cache`
/// cannot vouch for, and leaving `cache` holding exactly the files on
/// disk now (a path that is gone loses its entry). A file deleted between
/// the walk and its read is one the disk no longer has
/// ([`open_covered`]), as it would be a moment later.
pub fn disk_manifest(root: &Path, cache: &mut HashCache) -> Result<DiskState> {
    let walked = walk_folder(root)?;
    let mut manifest = Manifest::new();
    let mut entries = BTreeMap::new();
    for rel in walked.files {
        let recorded_at_ns = epoch_ns(SystemTime::now()).context("the clock is before 1970")?;
        let Some(file) = open_covered(root, &rel)? else { continue };
        let read = |e: std::io::Error| anyhow::Error::new(e).context(format!("read {}", root.join(&rel).display()));
        let meta = file.metadata().map_err(read)?;
        let size = meta.len();
        let mtime_ns = meta.modified().ok().and_then(epoch_ns);
        let kept = mtime_ns.and_then(|mtime_ns| cache.entries.get(&rel).filter(|e| e.vouches_for(size, mtime_ns)));
        let entry = match kept {
            Some(entry) => entry.clone(),
            None => {
                let (sha256, _) = crate::hash_reader(file).with_context(|| format!("read {}", root.join(&rel).display()))?;
                let Some(mtime_ns) = mtime_ns else {
                    // A time the cache cannot hold is a file it never
                    // vouches for: hashed every time.
                    manifest.insert(rel, sha256);
                    continue;
                };
                CacheEntry { size, mtime_ns, recorded_at_ns, sha256 }
            }
        };
        manifest.insert(rel.clone(), entry.sha256.clone());
        entries.insert(rel, entry);
    }
    cache.entries = entries;
    Ok(DiskState { manifest, conflicts: walked.conflicts })
}

/// The manifest of the project at `root` as it is on disk, every file
/// read: [`disk_manifest`] with nothing remembered.
pub fn local_manifest(root: &Path) -> Result<Manifest> {
    Ok(disk_manifest(root, &mut HashCache::default())?.manifest)
}

// ----- putting files in place ---------------------------------------------

/// Refuse to write `rel` under `root` when it could land anywhere but
/// there: a path that is absolute or climbs (`Path::join` with an absolute
/// path THROWS THE ROOT AWAY, and `..` walks out of it), or one with a
/// symbolic link on its way, which a write would go through.
pub fn validate_destination(root: &Path, rel: &str) -> Result<()> {
    if rel.is_empty() || !Path::new(rel).components().all(|part| matches!(part, Component::Normal(_))) {
        bail!("cannot write '{rel}': it is not a path inside the project");
    }
    let mut full = root.to_path_buf();
    for part in Path::new(rel).components() {
        full.push(part);
        match std::fs::symlink_metadata(&full) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("cannot write '{rel}': {} is a symbolic link", full.display());
            }
            Ok(_) => {}
            Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => break,
            Err(error) => return Err(error).with_context(|| format!("inspect {}", full.display())),
        }
    }
    Ok(())
}

/// Where the incoming bytes of `rel` are written before [`write_in_place`]
/// moves them in: a fresh file under `<root>/.weft/incoming/`. Not beside
/// the target, because the walk takes any name in a covered folder, so a
/// half-written file there would be hashed into a version; `.weft/` is
/// never walked, and it sits on the project's own disk, so the move is
/// one rename.
pub fn temp_for(root: &Path, rel: &str) -> Result<tempfile::NamedTempFile> {
    validate_destination(root, rel)?;
    let dir = root.join(".weft").join("incoming");
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    tempfile::NamedTempFile::new_in(&dir).with_context(|| format!("create a file in {} for {rel}", dir.display()))
}

/// Move the fully written file at `from` ([`temp_for`]) to `rel` under
/// `root` in one rename, so a reader sees the old file or the new one and
/// never part of either. Creates the folders on the way, and checks the
/// path again right before the move: a symbolic link planted while the
/// bytes arrived would be written through.
pub fn write_in_place(root: &Path, rel: &str, from: &Path) -> Result<()> {
    validate_destination(root, rel)?;
    let full = root.join(rel);
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {} for {rel}", parent.display()))?;
    }
    std::fs::rename(from, &full).with_context(|| format!("move {} to {}", from.display(), full.display()))
}

/// Remove `rel` under `root`, and the folders that leaves empty, up to
/// the root: a file whose folder was created with it otherwise leaves the
/// folder behind, and the tree does not look like what it should be. A
/// file already gone is the asked-for state.
pub fn remove_in_place(root: &Path, rel: &str) -> Result<()> {
    validate_destination(root, rel)?;
    let full = root.join(rel);
    match std::fs::remove_file(&full) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("remove {}", full.display())),
    }
    let mut parent = full.parent().map(Path::to_path_buf);
    while let Some(dir) = parent {
        if dir == root || !dir.starts_with(root) {
            break;
        }
        if std::fs::remove_dir(&dir).is_err() {
            break; // not empty, which is the ordinary case
        }
        parent = dir.parent().map(Path::to_path_buf);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(root: &Path, rel: &str, bytes: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, bytes).expect("write");
    }

    /// The walk's one disk-aware call: a `target/` is cargo's exactly
    /// when a `Cargo.toml` sits beside it. Told wrong in either
    /// direction it costs real files: thousands of build artifacts
    /// hashed and re-deleted on a switch, or somebody's writing dropped
    /// out of the version that was supposed to keep it.
    #[test]
    fn a_target_folder_is_build_output_only_next_to_a_cargo_toml() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(root, "weft.toml", "x");
        write(root, "nodes/pkg/Cargo.toml", "x");
        write(root, "nodes/pkg/target/debug/build.log", "x");
        write(root, "nodes/pkg/node_modules/left-pad/index.js", "x");
        write(root, "prompts/target/tone.txt", "x");
        write(root, "nodes/pkg/__pycache__/helper.cpython-312.pyc", "x");
        write(root, "scripts/tool.pyc", "x");

        let paths = covered_paths(root).expect("walk");
        assert!(!paths.iter().any(|p| p.contains("pycache") || p.ends_with(".pyc")), "{paths:?}");
        assert!(paths.contains(&"prompts/target/tone.txt".to_string()), "{paths:?}");
        assert!(paths.contains(&"nodes/pkg/Cargo.toml".to_string()), "{paths:?}");
        assert!(!paths.iter().any(|p| p.starts_with("nodes/pkg/target/")), "{paths:?}");
        assert!(!paths.iter().any(|p| p.contains("node_modules")), "{paths:?}");
        assert!(walk_takes(root, "prompts/target/tone.txt"));
        assert!(!walk_takes(root, "nodes/pkg/target/debug/build.log"));
    }

    /// The walk prunes exactly what `covers` excludes. Two spellings of
    /// one rule is how a folder ends up silently in or out of a version,
    /// and only the walk's opinion reaches the disk.
    #[test]
    fn the_walk_prunes_what_the_rule_excludes_and_nothing_else() {
        // Every hidden top-level folder is tooling, not a version's files.
        for rel in [".weft", ".git", ".github", ".vscode", ".weft-notes", "target", "nodes/a/node_modules", "nodes/a/__pycache__"] {
            assert!(prunes_whole_tree(rel), "{rel} holds nothing a version covers");
        }
        for rel in ["nodes", "nodes/base_catalog", "prompts", "scripts", "sql", "assets", "examples", "layouts"] {
            assert!(!prunes_whole_tree(rel), "{rel} can hold covered files");
        }
    }

    /// A symlink is refused by name rather than followed: one pointing at
    /// a folder above it is an endless walk, and one pointing out of the
    /// project pulls files in under a path inside it, which a switch
    /// would write through.
    #[test]
    fn a_symlink_in_the_tree_is_refused_by_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(root, "weft.toml", "x");
        write(root, "prompts/real.txt", "x");
        #[cfg(unix)]
        {
            // A link a version would never take is nobody's business here,
            // and most links in a project are exactly that: refusing them
            // would refuse an ordinary project.
            std::fs::create_dir_all(root.join(".weft")).expect("mkdir");
            std::os::unix::fs::symlink(root, root.join(".weft/loop")).expect("symlink");
            std::os::unix::fs::symlink(root.join("weft.toml"), root.join("README.md")).expect("symlink");
            std::fs::create_dir_all(root.join("docs")).expect("mkdir");
            std::os::unix::fs::symlink(root.join("weft.toml"), root.join("docs/notes.txt")).expect("symlink");
            covered_paths(root).expect("a link a version would not take is passed over");

            std::os::unix::fs::symlink(root, root.join("prompts/loop")).expect("symlink");
            let err = covered_paths(root).expect_err("a symlink is refused");
            assert!(err.to_string().contains("prompts/loop"), "{err}");
            assert!(err.to_string().contains("symlink"), "{err}");
        }
    }

    #[test]
    fn the_walk_lists_conflict_copies_apart_from_the_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(root, "main.weft", "x");
        write(root, "main.weft-conflict-1a2b3c4d.weft", "y");
        write(root, "prompts/tone.txt", "x");
        write(root, "prompts/tone.weft-conflict-1a2b3c4d.txt", "y");
        // Inside a folder the walk never enters, nobody's business.
        write(root, ".git/x.weft-conflict-1a2b3c4d", "y");

        let walked = walk_folder(root).expect("walk");
        assert_eq!(walked.files, vec!["main.weft", "prompts/tone.txt"]);
        assert_eq!(walked.conflicts, vec!["main.weft-conflict-1a2b3c4d.weft", "prompts/tone.weft-conflict-1a2b3c4d.txt"]);
        let state = disk_manifest(root, &mut HashCache::default()).expect("manifest");
        assert_eq!(state.manifest.keys().collect::<Vec<_>>(), vec!["main.weft", "prompts/tone.txt"]);
        assert_eq!(state.conflicts, walked.conflicts);
    }

    fn sha(bytes: &str) -> String {
        crate::hash_reader(bytes.as_bytes()).expect("hash").0
    }

    /// An entry recorded long after the file last changed, with a hash the
    /// file does not have: only a cache that trusted it would answer it.
    fn planted(root: &Path, rel: &str, hash: &str) -> CacheEntry {
        let meta = std::fs::metadata(root.join(rel)).expect("stat");
        let mtime_ns = epoch_ns(meta.modified().expect("mtime")).expect("ns");
        CacheEntry { size: meta.len(), mtime_ns, recorded_at_ns: mtime_ns + 10 * RACY_WINDOW_NS, sha256: hash.into() }
    }

    #[test]
    fn the_cache_trusts_an_old_unchanged_file_and_rehashes_a_changed_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(root, "main.weft", "one");
        let mut cache = HashCache::default();
        cache.entries.insert("main.weft".into(), planted(root, "main.weft", "remembered"));
        let state = disk_manifest(root, &mut cache).expect("manifest");
        assert_eq!(state.manifest["main.weft"], "remembered", "an old unchanged file is not read");

        // A different size is a different file, whatever its time says.
        write(root, "main.weft", "three");
        let state = disk_manifest(root, &mut cache).expect("manifest");
        assert_eq!(state.manifest["main.weft"], sha("three"));
        assert_eq!(cache.entries["main.weft"].sha256, sha("three"));
    }

    #[test]
    fn the_cache_distrusts_a_file_changed_close_to_its_hash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(root, "main.weft", "one");
        let mut cache = HashCache::default();
        let mut entry = planted(root, "main.weft", "remembered");
        // Same size and time, but the hash was taken within a clock tick
        // of the write: the file may have changed again without its time
        // moving.
        entry.recorded_at_ns = entry.mtime_ns + RACY_WINDOW_NS / 2;
        cache.entries.insert("main.weft".into(), entry);
        let state = disk_manifest(root, &mut cache).expect("manifest");
        assert_eq!(state.manifest["main.weft"], sha("one"));
    }

    #[test]
    fn the_cache_drops_removed_paths_and_survives_a_save() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(root, "main.weft", "one");
        write(root, "prompts/gone.txt", "two");
        let mut cache = HashCache::load(root);
        assert_eq!(cache, HashCache::default(), "no file is an empty cache");
        disk_manifest(root, &mut cache).expect("manifest");
        assert_eq!(cache.entries.len(), 2);

        std::fs::remove_file(root.join("prompts/gone.txt")).expect("remove");
        disk_manifest(root, &mut cache).expect("manifest");
        assert_eq!(cache.entries.keys().collect::<Vec<_>>(), vec!["main.weft"]);

        cache.save(root).expect("save");
        assert_eq!(HashCache::load(root), cache);
        // The cache lives where the walk never looks.
        assert_eq!(covered_paths(root).expect("walk"), vec!["main.weft"]);
        std::fs::write(root.join(".weft/hash-cache.json"), "not json").expect("write");
        assert_eq!(HashCache::load(root), HashCache::default(), "an unreadable file is an empty cache");
    }

    #[test]
    fn a_file_is_moved_in_whole_and_removed_with_the_folders_it_emptied() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(root, "nodes/keep/mod.rs", "x");

        let mut temp = temp_for(root, "nodes/new/deep/mod.rs").expect("temp");
        temp.write_all(b"incoming").expect("write");
        assert!(covered_paths(root).expect("walk") == vec!["nodes/keep/mod.rs"], "a file still arriving is not in the walk");
        let temp = temp.into_temp_path();
        write_in_place(root, "nodes/new/deep/mod.rs", &temp).expect("move in");
        assert_eq!(std::fs::read_to_string(root.join("nodes/new/deep/mod.rs")).expect("read"), "incoming");

        remove_in_place(root, "nodes/new/deep/mod.rs").expect("remove");
        assert!(!root.join("nodes/new").exists(), "the folders it emptied are gone");
        assert!(root.join("nodes/keep/mod.rs").is_file(), "a folder still holding a file stays");
        remove_in_place(root, "nodes/new/deep/mod.rs").expect("a file already gone is fine");
        assert!(root.is_dir(), "the root itself is never removed");
    }

    #[test]
    fn a_path_out_of_the_project_or_through_a_link_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        for bad in ["../escape.txt", "/etc/passwd", "prompts/../../x"] {
            assert!(validate_destination(root, bad).is_err(), "{bad}");
        }
        #[cfg(unix)]
        {
            let outside = tempfile::tempdir().expect("tempdir");
            std::os::unix::fs::symlink(outside.path(), root.join("prompts")).expect("symlink");
            let from = root.join("incoming");
            std::fs::write(&from, "x").expect("write");
            let err = write_in_place(root, "prompts/payload", &from).expect_err("refused");
            assert!(err.to_string().contains("symbolic link"), "{err}");
            assert!(remove_in_place(root, "prompts/payload").is_err());
            assert!(temp_for(root, "prompts/payload").is_err());
            assert!(!outside.path().join("payload").exists());
        }
    }
}
