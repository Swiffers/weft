//! The project's files as one command reads them: every file a version
//! covers (`versions::covers`), read once when the command starts. The
//! command compiles from it, publishes it as the version and builds that,
//! so what other people or agents write to disk meanwhile never reaches
//! it: the build is the snapshot whatever the disk says by then, and the
//! command ends by naming the files that moved after it was taken
//! ([`Snapshot::changed_on_disk`]), so nobody believes an edit is live
//! when it is not.
//!
//! The bytes are kept in memory, except a file larger than
//! [`SPILL_ABOVE`] (a video under `assets/`) and every file past
//! [`MEMORY_BUDGET`] in all, each copied once into an anonymous file under
//! the project's own `.weft/` and read back from there. Files are read
//! smallest first, so the program's text (the files the compile reads
//! again and again) stays in memory, and a folder of media never holds
//! gigabytes in the command's memory.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use weft_assets::AssetSource;
use weft_catalog::{NodeTreeEntryKind, SourceTree};
use weft_core::project::hash::Manifest;

use crate::client::DispatcherClient;

/// The size above which a file's bytes are kept on disk rather than in
/// memory (see the module doc).
const SPILL_ABOVE: u64 = 16 * 1024 * 1024;

/// How many bytes the snapshot keeps in memory in all; the files read
/// after that are kept on disk too (see the module doc).
const MEMORY_BUDGET: u64 = 256 * 1024 * 1024;

/// See the module doc.
pub struct Snapshot {
    root: PathBuf,
    /// When it was taken, for the line that says what a build is built
    /// from.
    taken_at: chrono::DateTime<chrono::Local>,
    /// Project-relative path (forward slashes) to the file.
    files: BTreeMap<String, File>,
    /// Every directory that holds a file, project-relative, the root
    /// itself as the empty path.
    dirs: BTreeSet<String>,
}

struct File {
    hash: String,
    size: u64,
    mime: String,
    bytes: Bytes,
}

#[derive(Clone)]
enum Bytes {
    Memory(Arc<[u8]>),
    /// The anonymous copy under `.weft/`, read at offsets so readers never
    /// share a position.
    Spilled(Arc<std::fs::File>),
}

impl Snapshot {
    /// Read every covered file under `root` once. A file deleted between
    /// the walk and its read is not in the snapshot: it was gone by the
    /// time the snapshot reached it.
    pub fn take(root: &Path) -> Result<Self> {
        let taken_at = chrono::Local::now();
        let mut files = BTreeMap::new();
        let mut in_memory = 0u64;
        // Smallest first, so the budget goes to the many small files (see
        // the module doc). A size is only the order: the bytes read are
        // what the snapshot holds.
        let mut by_size: Vec<(u64, String)> = Vec::new();
        for rel in super::versions::covered_paths(root)? {
            let Some(file) = super::versions::open_covered(root, &rel)? else { continue };
            let size = file.metadata().with_context(|| format!("read {}", root.join(&rel).display()))?.len();
            by_size.push((size, rel));
        }
        by_size.sort();
        for (_, rel) in by_size {
            let Some(file) = super::versions::open_covered(root, &rel)? else { continue };
            let path = root.join(&rel);
            let read = read_file(root, &rel, file, MEMORY_BUDGET.saturating_sub(in_memory))
                .with_context(|| format!("read {}", path.display()))?;
            if let Bytes::Memory(bytes) = &read.bytes {
                in_memory += bytes.len() as u64;
            }
            files.insert(rel, read);
        }
        Ok(Self::of(root.to_path_buf(), taken_at, files))
    }

    fn of(root: PathBuf, taken_at: chrono::DateTime<chrono::Local>, files: BTreeMap<String, File>) -> Self {
        let mut dirs = BTreeSet::from([String::new()]);
        for rel in files.keys() {
            let mut at = rel.as_str();
            while let Some((parent, _)) = at.rsplit_once('/') {
                dirs.insert(parent.to_string());
                at = parent;
            }
        }
        Self { root, taken_at, files, dirs }
    }

    /// Keep `bytes` as the file at `path` (absolute, under the root): a
    /// file this command wrote itself (`weft run --save`'s example) is part
    /// of what it read, so its version holds it whatever the disk says
    /// next. A path outside the root, or one no version covers, is not.
    pub fn record(&mut self, path: &Path, bytes: Vec<u8>) -> Result<()> {
        let rel = self.key(path).filter(|rel| super::versions::covers(rel));
        let Some(rel) = rel else { return Ok(()) };
        let (hash, size, head) = weft_assets::hash_reader_peeking(bytes.as_slice())?;
        let file = File { hash, size, mime: weft_assets::media_type(&head, &rel), bytes: Bytes::Memory(bytes.into()) };
        let mut at = rel.as_str();
        while let Some((parent, _)) = at.rsplit_once('/') {
            self.dirs.insert(parent.to_string());
            at = parent;
        }
        self.files.insert(rel, file);
        Ok(())
    }

    pub fn taken_at(&self) -> chrono::DateTime<chrono::Local> {
        self.taken_at
    }

    /// What a version of these files records: each path's content hash.
    pub fn manifest(&self) -> Manifest {
        self.files.iter().map(|(rel, file)| (rel.clone(), file.hash.clone())).collect()
    }

    /// The project these files make: its `weft.toml` as the snapshot reads.
    pub fn project(&self) -> Result<weft_compiler::project::Project> {
        weft_compiler::project::Project::load_in(&self.root, self).map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Publish every file into the tenant's assets and answer the manifest
    /// a version records. `tell` says on stderr how far the uploads have
    /// got (never under `--json`, whose output is the program's).
    pub async fn publish(&self, client: &DispatcherClient, tell: bool) -> Result<Manifest> {
        let hashed: Vec<weft_assets::HashedFile> = self
            .files
            .iter()
            .map(|(rel, file)| weft_assets::HashedFile {
                path: rel.clone(),
                hash: file.hash.clone(),
                size: file.size,
                mime: file.mime.clone(),
            })
            .collect();
        let store = if tell {
            super::assets::DispatcherStore::telling(client)
        } else {
            super::assets::DispatcherStore::new(client)
        };
        weft_assets::publish_hashed(&hashed, self, &store).await.context("publish the version's files")?;
        Ok(self.manifest())
    }

    /// The covered files that differ on disk now from this snapshot
    /// (changed, added, or removed since), sorted. A file deleted while
    /// this looks is one removed.
    pub fn changed_on_disk(&self) -> Result<Vec<String>> {
        Ok(changed_between(&self.manifest(), &super::versions::local_manifest(&self.root)?))
    }

    /// The bytes of the file at `rel`, whole.
    fn bytes(&self, rel: &str) -> std::io::Result<Vec<u8>> {
        let file = self.files.get(rel).ok_or_else(|| not_found(rel))?;
        let mut out = Vec::with_capacity(file.size as usize);
        reader(&file.bytes, file.size).read_to_end(&mut out)?;
        Ok(out)
    }

    /// `path` (absolute, under the root) as a key: project-relative with
    /// forward slashes, `.` and `..` resolved on the spelling. `None` for a
    /// path outside the root.
    fn key(&self, path: &Path) -> Option<String> {
        let rel = path.strip_prefix(&self.root).ok()?;
        let mut parts: Vec<String> = Vec::new();
        for component in rel.components() {
            match component {
                Component::CurDir => {}
                Component::ParentDir => {
                    parts.pop()?;
                }
                Component::Normal(part) => parts.push(part.to_str()?.to_string()),
                Component::RootDir | Component::Prefix(_) => return None,
            }
        }
        Some(parts.join("/"))
    }
}

/// The paths whose hash differs between `then` and `now`, either side
/// missing included, sorted.
fn changed_between(then: &Manifest, now: &Manifest) -> Vec<String> {
    then.keys()
        .chain(now.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|path| then.get(*path) != now.get(*path))
        .cloned()
        .collect()
}

/// One file read, at `rel`: hashed, its media type named
/// (`weft_assets::media_type`), its bytes kept, in memory when it fits in
/// what is `left` of the budget and under [`SPILL_ABOVE`].
fn read_file(root: &Path, rel: &str, mut file: std::fs::File, left: u64) -> Result<File> {
    let size = file.metadata()?.len();
    let path = root.join(rel);
    let bytes = if size > SPILL_ABOVE || size > left {
        let scratch = root.join(".weft");
        std::fs::create_dir_all(&scratch).with_context(|| format!("create {}", scratch.display()))?;
        let mut copy = tempfile::tempfile_in(&scratch)
            .with_context(|| format!("create a copy of {} under {}", path.display(), scratch.display()))?;
        std::io::copy(&mut file, &mut copy)?;
        Bytes::Spilled(Arc::new(copy))
    } else {
        let mut bytes = Vec::with_capacity(size as usize);
        file.read_to_end(&mut bytes)?;
        Bytes::Memory(bytes.into())
    };
    // The size is what was read, which a file growing meanwhile moved.
    let size = match &bytes {
        Bytes::Memory(bytes) => bytes.len() as u64,
        Bytes::Spilled(copy) => copy.metadata()?.len(),
    };
    let (hash, _, head) = weft_assets::hash_reader_peeking(reader(&bytes, size))?;
    Ok(File { hash, size, mime: weft_assets::media_type(&head, rel), bytes })
}

fn not_found(rel: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("{rel} was not in the project when this command read it at its start"),
    )
}

/// A reader over a file's kept bytes, with a position of its own.
fn reader(bytes: &Bytes, size: u64) -> KeptReader {
    KeptReader { bytes: bytes.clone(), size, at: 0 }
}

struct KeptReader {
    bytes: Bytes,
    size: u64,
    at: u64,
}

impl Read for KeptReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = match &self.bytes {
            Bytes::Memory(bytes) => {
                let start = (self.at as usize).min(bytes.len());
                let n = buf.len().min(bytes.len() - start);
                buf[..n].copy_from_slice(&bytes[start..start + n]);
                n
            }
            Bytes::Spilled(copy) => {
                let left = self.size.saturating_sub(self.at) as usize;
                let want = buf.len().min(left);
                std::os::unix::fs::FileExt::read_at(copy.as_ref(), &mut buf[..want], self.at)?
            }
        };
        self.at += n as u64;
        Ok(n)
    }
}

impl Seek for KeptReader {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        let at = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(back) => self.size.checked_add_signed(back),
            SeekFrom::Current(by) => self.at.checked_add_signed(by),
        };
        self.at = at.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "seek before the start"))?;
        Ok(self.at)
    }
}

impl SourceTree for Snapshot {
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        let rel = self.key(dir).filter(|rel| self.dirs.contains(rel)).ok_or_else(|| not_found(&dir.to_string_lossy()))?;
        let prefix = if rel.is_empty() { String::new() } else { format!("{rel}/") };
        let children: BTreeSet<&str> = self
            .files
            .keys()
            .filter_map(|path| path.strip_prefix(prefix.as_str()))
            .map(|rest| rest.split('/').next().unwrap_or(rest))
            .collect();
        Ok(children.into_iter().map(|child| dir.join(child)).collect())
    }

    fn entry_kind(&self, path: &Path) -> std::io::Result<NodeTreeEntryKind> {
        match self.key(path) {
            Some(rel) if self.files.contains_key(&rel) => Ok(NodeTreeEntryKind::File),
            Some(rel) if self.dirs.contains(&rel) => Ok(NodeTreeEntryKind::Dir),
            _ => Err(not_found(&path.to_string_lossy())),
        }
    }

    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        let rel = self.key(path).ok_or_else(|| not_found(&path.to_string_lossy()))?;
        self.bytes(&rel)
    }

    fn canonicalize(&self, path: &Path) -> std::io::Result<PathBuf> {
        match self.key(path) {
            Some(rel) if self.files.contains_key(&rel) || self.dirs.contains(&rel) => {
                Ok(if rel.is_empty() { self.root.clone() } else { self.root.join(rel) })
            }
            _ => Err(not_found(&path.to_string_lossy())),
        }
    }
}

impl AssetSource for Snapshot {
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>> {
        let file = self.files.get(path).ok_or_else(|| not_found(path))?;
        Ok(Box::new(reader(&file.bytes, file.size)))
    }

    /// The kept bytes themselves: nothing can change them under the
    /// caller, so no copy is made.
    fn snapshot(&self, path: &str) -> Result<Box<dyn weft_assets::AssetReader>> {
        let file = self.files.get(path).ok_or_else(|| not_found(path))?;
        Ok(Box::new(reader(&file.bytes, file.size)))
    }
}

/// What a command resolves its `@file` and `@asset` markers from
/// ([`Snapshot::then_disk`]).
pub struct SnapshotFirst<'a> {
    snapshot: &'a Snapshot,
    disk: super::assets::DiskSource,
}

impl Snapshot {
    /// The files a command resolves its `@file` and `@asset` markers from
    /// (the build's `@asset` refs, a run's handed values): a project file a
    /// version covers is read from the snapshot, like everything else the
    /// command reads (one that appeared after it is not there); any other
    /// path (one outside the project, which an `@asset` may name by its
    /// absolute path, or a file no version covers) from the disk where it
    /// is.
    pub fn then_disk(&self) -> SnapshotFirst<'_> {
        SnapshotFirst { snapshot: self, disk: super::assets::DiskSource::new(self.root.clone()) }
    }
}

impl SnapshotFirst<'_> {
    /// The project's root, which a marker's path is spelled from.
    pub fn root(&self) -> &Path {
        &self.snapshot.root
    }

    /// `path` (absolute) as a snapshot key when the snapshot answers for
    /// it: a file inside the project that a version covers, which is read
    /// from the snapshot or not at all (one written after the snapshot is
    /// not this command's). `None` for any other path, read from the disk.
    fn covered(&self, path: &Path) -> Option<String> {
        self.snapshot.key(path).filter(|rel| super::versions::walk_takes(&self.snapshot.root, rel))
    }

    /// The text of the file at `path` (absolute), from wherever
    /// [`Self::covered`] says.
    pub fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
        match self.covered(path) {
            Some(rel) => self.snapshot.read_to_string(&self.snapshot.root.join(rel)),
            None => std::fs::read_to_string(path),
        }
    }
}

impl weft_compiler::FileReader for SnapshotFirst<'_> {
    fn read_spelled(&self, spelled: &weft_compiler::SpelledPath, relative: &Path) -> Result<weft_compiler::ResolvedFile, String> {
        match self.covered(&spelled.path) {
            Some(_) => {
                let content =
                    self.read_to_string(&spelled.path).map_err(|e| format!("path {relative:?} cannot be read: {e}"))?;
                Ok(weft_compiler::ResolvedFile { identity: spelled.path.clone(), content })
            }
            None => weft_compiler::FileReader::read_spelled(&weft_compiler::DiskFileReader, spelled, relative),
        }
    }

    /// The root is the one identity asked for, and the snapshot's is the
    /// disk's own (`Project::find` canonicalized it).
    fn identity(&self, path: &Path) -> Result<PathBuf, String> {
        weft_compiler::FileReader::identity(&weft_compiler::DiskFileReader, path)
    }
}

impl AssetSource for SnapshotFirst<'_> {
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>> {
        match self.covered(&self.snapshot.root.join(path)) {
            Some(rel) => self.snapshot.open(&rel),
            None => self.disk.open(path),
        }
    }

    fn snapshot(&self, path: &str) -> Result<Box<dyn weft_assets::AssetReader>> {
        match self.covered(&self.snapshot.root.join(path)) {
            Some(rel) => self.snapshot.snapshot(&rel),
            None => self.disk.snapshot(path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(root: &Path, rel: &str, body: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    /// What the snapshot read is what every reader of it sees: a file
    /// changed on disk afterwards reads as it was, and is named as changed;
    /// one added or removed is named too.
    #[test]
    fn a_file_changed_after_the_snapshot_is_not_read_and_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        write(&root, "weft.toml", "[package]\nname = 'x'\nid = '00000000-0000-0000-0000-000000000001'\n");
        write(&root, "src/main.weft", "a = Text { value: \"one\" }\n");
        write(&root, "prompts/judge.md", "be fair\n");
        write(&root, "prompts/gone.md", "bye\n");
        let snapshot = Snapshot::take(&root).unwrap();

        write(&root, "prompts/judge.md", "be harsh\n");
        write(&root, "prompts/new.md", "hi\n");
        std::fs::remove_file(root.join("prompts/gone.md")).unwrap();

        assert_eq!(snapshot.read_to_string(&root.join("prompts/judge.md")).unwrap(), "be fair\n");
        assert_eq!(snapshot.read_to_string(&root.join("prompts/gone.md")).unwrap(), "bye\n");
        assert!(snapshot.entry_kind(&root.join("prompts/new.md")).is_err());
        assert_eq!(snapshot.changed_on_disk().unwrap(), ["prompts/gone.md", "prompts/judge.md", "prompts/new.md"]);
        let mut opened = String::new();
        snapshot.open("prompts/judge.md").unwrap().read_to_string(&mut opened).unwrap();
        assert_eq!(opened, "be fair\n");
    }

    /// A covered file that appeared after the snapshot is not the
    /// command's, whoever reads it; a file no version covers is the disk's.
    #[test]
    fn a_covered_file_newer_than_the_snapshot_is_not_read_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        write(&root, "weft.toml", "[package]\nname = 'x'\n");
        let snapshot = Snapshot::take(&root).unwrap();
        write(&root, "examples/late.json", "{}");
        write(&root, "notes/free.md", "mine");
        let files = snapshot.then_disk();
        assert!(files.read_to_string(&root.join("examples/late.json")).is_err());
        assert!(files.open("examples/late.json").is_err());
        assert_eq!(files.read_to_string(&root.join("notes/free.md")).unwrap(), "mine");
        // What the walk never takes (a node's own Cargo build output) is
        // the disk's too, though its path reads as covered.
        write(&root, "nodes/tool/Cargo.toml", "[package]\n");
        write(&root, "nodes/tool/target/out.bin", "built");
        assert_eq!(files.read_to_string(&root.join("nodes/tool/target/out.bin")).unwrap(), "built");
    }

    /// The tree a snapshot answers: directories implied by the files, the
    /// root among them, children listed once, spellings with `..` resolved
    /// and anything outside the root absent.
    #[test]
    fn the_snapshot_answers_like_a_directory_tree() {
        let snapshot = Snapshot::of(
            PathBuf::from("/p"),
            chrono::Local::now(),
            ["nodes/a/metadata.json", "nodes/a/mod.rs", "nodes/b/pkg/package.toml", "weft.toml"]
                .into_iter()
                .map(|rel| {
                    let body: Arc<[u8]> = Arc::from(rel.as_bytes());
                    (rel.to_string(), File { hash: rel.into(), size: body.len() as u64, mime: "text/plain".into(), bytes: Bytes::Memory(body) })
                })
                .collect(),
        );
        let p = Path::new;
        assert_eq!(snapshot.read_dir(p("/p")).unwrap(), [p("/p/nodes"), p("/p/weft.toml")]);
        assert_eq!(snapshot.read_dir(p("/p/nodes")).unwrap(), [p("/p/nodes/a"), p("/p/nodes/b")]);
        assert!(matches!(snapshot.entry_kind(p("/p/nodes/b")).unwrap(), NodeTreeEntryKind::Dir));
        assert!(matches!(snapshot.entry_kind(p("/p/nodes/a/mod.rs")).unwrap(), NodeTreeEntryKind::File));
        assert!(snapshot.entry_kind(p("/p/nodes/c")).is_err());
        assert!(snapshot.read_dir(p("/p/weft.toml")).is_err(), "a file is no directory");
        assert_eq!(snapshot.canonicalize(p("/p/nodes/b/../a/./mod.rs")).unwrap(), p("/p/nodes/a/mod.rs"));
        assert_eq!(snapshot.canonicalize(p("/p")).unwrap(), p("/p"));
        assert!(snapshot.canonicalize(p("/p/../q/weft.toml")).is_err(), "outside the root");
        assert_eq!(snapshot.read(p("/p/nodes/a/mod.rs")).unwrap(), b"nodes/a/mod.rs");
    }

    /// A file that fits under the per-file bound still goes to disk once
    /// the budget for the whole snapshot is spent.
    #[test]
    fn a_file_past_the_budget_is_kept_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        write(&root, "assets/small.txt", "tiny");
        let file = std::fs::File::open(root.join("assets/small.txt")).unwrap();
        assert!(matches!(read_file(&root, "assets/small.txt", file, 2).unwrap().bytes, Bytes::Spilled(_)));
        let file = std::fs::File::open(root.join("assets/small.txt")).unwrap();
        let kept = read_file(&root, "assets/small.txt", file, 4).unwrap();
        assert!(matches!(kept.bytes, Bytes::Memory(_)));
        assert!(kept.mime.starts_with("text/plain"), "{}", kept.mime);
    }

    /// A file larger than the memory bound reads back whole, from any
    /// position, through two readers at once.
    #[test]
    fn a_spilled_file_reads_back_whole_from_any_position() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let big: Vec<u8> = (0..SPILL_ABOVE + 10).map(|i| (i % 251) as u8).collect();
        std::fs::create_dir_all(root.join("assets")).unwrap();
        std::fs::write(root.join("assets/clip.bin"), &big).unwrap();
        let snapshot = Snapshot::take(&root).unwrap();
        let file = &snapshot.files["assets/clip.bin"];
        assert!(matches!(file.bytes, Bytes::Spilled(_)));
        assert_eq!(file.hash, weft_core::project::hash::sha256_hex(&big));
        let mut first = reader(&file.bytes, file.size);
        let mut second = reader(&file.bytes, file.size);
        second.seek(SeekFrom::End(-10)).unwrap();
        let mut tail = Vec::new();
        second.read_to_end(&mut tail).unwrap();
        assert_eq!(tail, big[big.len() - 10..]);
        let mut whole = Vec::new();
        first.read_to_end(&mut whole).unwrap();
        assert_eq!(whole, big);
    }
}
