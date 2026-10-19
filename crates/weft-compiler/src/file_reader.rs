//! The filesystem view the compiler reads `@file`/`@include` content through.
//!
//! `@include("path")` names external content relative to the referencing
//! file's directory, `@file("path")` relative to the project root, and
//! either one starting `__catalog/` relative to the project's `nodes/`.
//! Resolving them is two steps: turn `(base, relative_path)` into a
//! canonical identity (with a containment check), then read that
//! identity's bytes. Today there are two backings for those bytes:
//!
//!   - disk (the CLI on a laptop; a project folder unpacked to a temp dir and
//!     compiled from disk the same way),
//!   - in-memory map (the browser WASM parse path, and tests).
//!
//! The compile pipeline (parse, flatten, enrich, validate) is otherwise pure, so
//! the only thing that varies across the two is HOW a referenced path becomes
//! content. `FileReader` is that one seam; `CompileFs` bundles a reader with the
//! current resolution anchor (`base`) and is what the compiler threads, so the
//! reader and the anchor (which are never independent: a reader can't resolve a
//! relative path without an anchor, an anchor can't read without a reader) travel
//! as one concept.

use std::path::{Component, Path, PathBuf};

/// A resolved external reference: its canonical identity plus its content.
///
/// `identity` is the backing-agnostic key for the resolved path. The compiler
/// uses it for two things, so every backing must produce one that supports both:
///   - `@include` cycle detection (is this identity already on the resolve
///     stack?), which needs identities to compare equal iff they name the same
///     file,
///   - deriving the base for content referenced INSIDE the resolved file
///     (`identity.parent()`), so a nested `@file`/`@include` resolves against the
///     included file's own directory, not the parent's.
#[derive(Debug)]
pub struct ResolvedFile {
    pub identity: PathBuf,
    pub content: String,
}

/// Where a marker's path lands, worked out from its spelling alone,
/// before any backing is asked about the file.
#[derive(Debug)]
pub struct SpelledPath {
    /// The path joined onto its anchor and normalized, inside `root`.
    pub path: PathBuf,
    /// The project root, which the file read may not leave either (the
    /// disk backing follows symlinks). A `__catalog/` path is held to
    /// `nodes/` on its spelling alone ([`catalog_path`]); the file it
    /// lands on is held to the project, like `nodes/...` written out, so
    /// the two spellings of one path read the same file in every marker.
    pub root: PathBuf,
}

/// The refusal for a path, as written, that leaves the project root.
pub fn escapes_root(relative: &Path) -> String {
    format!("path {relative:?} escapes the project root")
}

/// Resolve and read the content named by `relative` against `base`.
///
/// `base` is the directory the reference is relative to (the referencing file's
/// own directory). The error is a human-facing string the caller wraps in a
/// `CompileError` at the field's span: no error type, because every backing's
/// failure ("escapes the root", "not found", "is not valid UTF-8") is just a
/// message pointed at a source line.
pub trait FileReader {
    /// Read the file `relative` names from `base`, refusing anything
    /// outside `root` (the project). The two differ for an `@include`,
    /// whose path is relative to the file that writes it: the program
    /// lives in `src/`, so `@include("../lib/x.weft")` in
    /// `src/billing/charge.weft` climbs one level and lands inside the
    /// project. A `@file` / `@asset` path is relative to the project root
    /// wherever it is written, so for those the two are the same. A path
    /// that starts with `__catalog/` is the exception to both: it is read
    /// from the project's `nodes/`, and its spelling may not climb out of
    /// it ([`catalog_path`]).
    ///
    /// Every backing resolves through this one method, so the rule is
    /// checked on the spelling, before the file is touched: a `../` that
    /// climbs out is refused as such whether or not something is there
    /// (a typo'd path used to report "no such file" instead of the rule).
    fn resolve_and_read(&self, root: &Path, base: &Path, relative: &Path) -> Result<ResolvedFile, String> {
        let root = self.identity(root)?;
        let path = match catalog_path(relative) {
            Some(path) => root.join(path.map_err(|why| format!("path {relative:?} {why}"))?),
            None => {
                let base = self.identity(base)?;
                // A `..` that merely balances out a base component
                // (`proj/../secret.txt`) survives the climb check yet
                // leaves the root, so the joined path is checked against
                // the root as well.
                normalize_lexical(&base.join(relative)).ok_or_else(|| escapes_root(relative))?
            }
        };
        if !path.starts_with(&root) {
            return Err(escapes_root(relative));
        }
        let spelled = SpelledPath { path, root };
        self.read_spelled(&spelled, relative)
    }

    /// Read the file at `spelled.path`, already checked to sit inside
    /// `spelled.root` as spelled. `relative` is the path as written, for
    /// the messages.
    fn read_spelled(&self, spelled: &SpelledPath, relative: &Path) -> Result<ResolvedFile, String>;

    /// The identity `resolve_and_read` would give `path` (the disk backing
    /// canonicalizes, the map backing normalizes lexically), so a caller can
    /// express one identity relative to another: an included file's
    /// `@file("x")` becomes `components/x` under the project root.
    fn identity(&self, path: &Path) -> Result<PathBuf, String>;
}

/// The path a `__catalog/` marker path names from the project root
/// (`__catalog/base_catalog/computer/agent.weft` is
/// `nodes/base_catalog/computer/agent.weft`), or `None` for a path that
/// does not start with that segment. Its rest is normalized on its own,
/// so any `..` that climbs out of `nodes/` is refused, even one that
/// comes back in (`__catalog/x/../../nodes/y`); the refusal is the
/// reason alone, [`CATALOG_ESCAPE`], for the caller to put after the
/// path the way its own messages name one.
///
/// THE prefix rule: every backing reaches it through
/// [`FileReader::resolve_and_read`] (`@include`), and every `@file` /
/// `@asset` through `file_ref::spell_from_root`, which spells the
/// marker from the root before any read.
// SYNC: catalog_path <-> extension-vscode/src/includePath.ts includeRelativeToFile
pub fn catalog_path(written: &Path) -> Option<Result<PathBuf, &'static str>> {
    let mut components = written.components();
    match components.next() {
        Some(Component::Normal(first)) if first == crate::project::CATALOG_PREFIX => {}
        _ => return None,
    }
    Some(
        normalize_lexical(components.as_path())
            .map(|rest| Path::new(crate::project::NODES_DIR).join(rest))
            .ok_or(CATALOG_ESCAPE),
    )
}

/// Why a `__catalog/` path that climbs out of `nodes/` is refused.
pub const CATALOG_ESCAPE: &str =
    "escapes the project's nodes/ folder: a `__catalog/` path is read from nodes/ and has to stay inside it";

/// The on-disk backing. Canonicalizes the spelled path (resolving
/// symlinks), rejects anything that lands outside the root, then reads
/// the bytes.
///
/// The project tree is TRUSTED: this runs at build time on the user's own
/// project, and referenced files are part of that project. The containment
/// check guards against an accidental `../` typo pulling a file from outside
/// the project into the build. It is robust for that: `canonicalize`
/// resolves `..` and follows symlinks before the prefix check, so any
/// path that lands outside the root (including via a symlink) is rejected.
/// (`@asset` is different: it may name a file anywhere on the machine
/// by an absolute path, read from where it sits and uploaded at compile.)
pub struct DiskFileReader;

impl FileReader for DiskFileReader {
    fn read_spelled(&self, spelled: &SpelledPath, relative: &Path) -> Result<ResolvedFile, String> {
        let identity = spelled
            .path
            .canonicalize()
            .map_err(|e| format!("path {relative:?} cannot be read: {e}"))?;
        if !identity.starts_with(&spelled.root) {
            return Err(escapes_root(relative));
        }
        let content = std::fs::read_to_string(&identity)
            .map_err(|e| format!("path {relative:?} cannot be read: {e}"))?;
        Ok(ResolvedFile { identity, content })
    }

    fn identity(&self, path: &Path) -> Result<PathBuf, String> {
        path.canonicalize()
            .map_err(|e| format!("project root {path:?} is unreadable: {e}"))
    }
}

/// An in-memory backing: a path-keyed map of content. Used by the browser WASM
/// parse path (the editor feeds it the project's open files) and by tests that
/// exercise `@file`/`@include` without touching a real filesystem.
///
/// Resolution is LEXICAL (no filesystem): the spelled path is both the
/// identity and the map key, and its containment was checked on the
/// spelling, the same check the disk backing makes before it touches the
/// disk, so a `@file("../x")` at the root fails identically on both
/// backings. Keys in the map must be the normalized form the compiler
/// resolves to (callers build the map from the same paths the source
/// references).
pub struct MapFileReader {
    files: std::collections::BTreeMap<PathBuf, String>,
}

impl MapFileReader {
    pub fn new(files: std::collections::BTreeMap<PathBuf, String>) -> Self {
        Self { files }
    }
}

impl FileReader for MapFileReader {
    fn read_spelled(&self, spelled: &SpelledPath, relative: &Path) -> Result<ResolvedFile, String> {
        match self.files.get(&spelled.path) {
            Some(content) => Ok(ResolvedFile {
                identity: spelled.path.clone(),
                content: content.clone(),
            }),
            None => Err(format!("path {relative:?} not found")),
        }
    }

    fn identity(&self, path: &Path) -> Result<PathBuf, String> {
        normalize_lexical(path).ok_or_else(|| format!("path {path:?} escapes the project root"))
    }
}

/// Lexically normalize a path: collapse `.` and resolve `..` against the
/// preceding component, WITHOUT touching the filesystem. Returns `None` if a
/// `..` would climb above the path's root (the lexical equivalent of escaping
/// the containment root), so a virtual backing rejects the same escapes the disk
/// backing's canonicalize-then-prefix-check rejects.
pub(crate) fn normalize_lexical(path: &Path) -> Option<PathBuf> {
    let mut out: Vec<Component> = Vec::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => match out.last() {
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                // `..` at the start, or after a root/prefix, escapes upward.
                _ => return None,
            },
            other => out.push(other),
        }
    }
    Some(out.iter().collect())
}

/// The filesystem view threaded through the compile pipeline: a reader, the
/// project root, and the current resolution anchor. Replaces the bare
/// `base_dir: Option<&Path>` the pipeline used to thread, unifying "where do
/// I resolve relative paths" and "how do I read them" into one value (they
/// are never independent).
///
/// `root` is the project (the folder holding `weft.toml`): every path a
/// marker names must land inside it, `@file` / `@asset` paths are relative
/// to it wherever they are written, and every path that leaves the
/// compiler is spelled from it. `base` is the directory of the file being
/// compiled, what an `@include` path is joined onto: `src/` for the
/// program, an included file's own directory inside it. Both are `None`
/// when compiling outside any project (an unsaved buffer, a bare snippet):
/// a `@file`/`@include` then has no anchor and is a compile error. Entering
/// an included file produces a fresh `CompileFs` (`anchored_at`) carrying
/// the SAME reader and root and the included file's directory as the anchor.
#[derive(Clone, Copy)]
pub struct CompileFs<'a> {
    pub reader: &'a dyn FileReader,
    pub root: Option<&'a Path>,
    pub base: Option<&'a Path>,
}

/// The default reader for callers that don't inject one: read from disk. A
/// `'static` so `CompileFs::disk`/`none` can borrow it without the caller
/// holding an instance.
static DISK_READER: DiskFileReader = DiskFileReader;

impl<'a> CompileFs<'a> {
    /// Disk-backed view of the project at `root`, anchored there too: for a
    /// file that sits at the root (a test fixture, a standalone file). A
    /// program under `src/` follows with `anchored_at`.
    pub fn disk(root: &'a Path) -> Self {
        Self {
            reader: &DISK_READER,
            root: Some(root),
            base: Some(root),
        }
    }

    /// Disk-backed view with NO anchor: compiling outside a project. Any
    /// `@file`/`@include` is a compile error (nothing to resolve against).
    pub fn none() -> Self {
        Self {
            reader: &DISK_READER,
            root: None,
            base: None,
        }
    }

    /// View with an explicit reader (the in-memory map the browser parse path
    /// uses) of the project at `root`, anchored there.
    pub fn with_reader(reader: &'a dyn FileReader, root: Option<&'a Path>) -> Self {
        Self { reader, root, base: root }
    }

    /// The same project seen from `base`: the directory of the file whose
    /// markers are being resolved (`src/` for the program, an included
    /// file's own directory once the compiler has entered it).
    pub fn anchored_at(&self, base: Option<&'a Path>) -> Self {
        Self {
            reader: self.reader,
            root: self.root,
            base,
        }
    }
}

#[cfg(test)]
#[path = "tests/file_reader_tests.rs"]
mod tests;
