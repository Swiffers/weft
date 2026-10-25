//! Which of a project's files make a version: every `*.weft` (an
//! `@include`d file is program text), `weft.toml`, `src/**` (the
//! compiler's second node root: a node beside the code is source like
//! any other), `nodes/**` (the standard library under
//! `nodes/base_catalog/` included: it is the project's own, edited or
//! left out as the project chose), `prompts/**`, `scripts/**`, `sql/**`,
//! `assets/**`, `examples/**` (a switch back restores the examples that
//! existed then) and `layouts/**` (where the graph's boxes sit: a switch
//! back puts them where they were). The rule is pure, over a path; the
//! walk that applies it to a folder is `weft_assets::tree`.

/// Whether a project-relative path (forward slashes) is part of a
/// version. The one rule; every walk, every sync and every check reads it.
pub fn covers(rel: &str) -> bool {
    let first = rel.split('/').next().unwrap_or("");
    // A HIDDEN top-level entry is tooling, never a version's files:
    // `.weft` and `.git`, and equally `.github`, `.vscode`, `.idea`,
    // `.gitignore`, `.env`. Said as one rule about the whole entry,
    // hidden files included, so `.weft` itself never comes back in
    // through the `ends_with(".weft")` rule below, and a directory
    // answers the same spelled with or without its trailing slash.
    if first.starts_with('.') {
        return false;
    }
    if rel.starts_with("target/") {
        return false;
    }
    // `node_modules` is never source, at any depth: a node that carries
    // a JS package has its own, and an ordinary `pnpm install` inside a
    // node would put thousands of files through the hash, the publish
    // and every switch. The name is npm's, so nothing else is called
    // that.
    //
    // `target` is excluded at the ROOT only, just above: the project
    // root is where weft's own build puts a Cargo workspace, so a
    // top-level `target/` is build output every time. Deeper down it is
    // an ordinary word, and a node package, a prompts folder or a
    // scripts folder may legitimately be called it, so it is NOT
    // excluded by name there: silently dropping source from a version
    // loses it on a switch back. The walk tells the two apart, because
    // it can see whether a `Cargo.toml` sits beside the directory.
    if rel.split('/').any(|seg| seg == "node_modules") {
        return false;
    }
    if rel.split('/').any(is_python_cache) {
        return false;
    }
    // A copy the sync wrote beside a file both sides changed is the
    // person's to look at and delete, never part of a version: taken in,
    // a conflict copy of `main.weft` would be compiled as a second
    // program file.
    if rel.rsplit('/').next().is_some_and(is_conflict_copy) {
        return false;
    }
    if rel.ends_with(".weft") || rel == "weft.toml" {
        return true;
    }
    matches!(first, "src" | "nodes" | "prompts" | "scripts" | "sql" | "assets" | "examples" | "layouts")
}

// SYNC: is_python_cache <-> .dockerignore (the `__pycache__` / `*.pyc` / `.pytest_cache` lines)
/// True if an entry of this name is what Python writes beside a module
/// the first time it imports it (`__pycache__/`, a `.pyc`), or what
/// pytest keeps between its runs (`.pytest_cache/`, rewritten by every
/// run). Never source, and leaving it in would make a machine that ran a
/// node, or its tests, hash the same tree differently from one that did
/// not, and build again for nothing. Asked at any depth, by every walk
/// that decides what a project or a node is made of.
pub fn is_python_cache(name: &str) -> bool {
    name == "__pycache__" || name == ".pytest_cache" || name.ends_with(".pyc")
}

/// What every conflict copy's file name carries ([`conflict_copy_name`]).
const CONFLICT_MARK: &str = ".weft-conflict-";

/// Whether a file NAME (no folder) is a conflict copy the sync wrote.
pub fn is_conflict_copy(name: &str) -> bool {
    name.contains(CONFLICT_MARK)
}

/// Where the sync writes the version of `path` it could not put in place
/// because the file on disk changed too: beside it, named after the file
/// and the first characters of the incoming content's hash, keeping the
/// extension so an editor still opens it as what it is
/// (`prompts/tone.txt` -> `prompts/tone.weft-conflict-1a2b3c4d.txt`).
pub fn conflict_copy_name(path: &str, incoming_hash: &str) -> String {
    let (dir, name) = match path.rsplit_once('/') {
        Some((dir, name)) => (Some(dir), name),
        None => (None, path),
    };
    let short = &incoming_hash[..incoming_hash.len().min(8)];
    // A name whose only dot leads it (`.env`) has no extension.
    let named = match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => format!("{stem}{CONFLICT_MARK}{short}.{ext}"),
        _ => format!("{name}{CONFLICT_MARK}{short}"),
    };
    match dir {
        Some(dir) => format!("{dir}/{named}"),
        None => named,
    }
}

/// The file a conflict copy was written beside ([`conflict_copy_name`]
/// read backwards), or `None` when `copy` is no conflict copy.
pub fn conflict_copy_origin(copy: &str) -> Option<String> {
    let (dir, name) = match copy.rsplit_once('/') {
        Some((dir, name)) => (Some(dir), name),
        None => (None, copy),
    };
    let (stem, after) = name.rsplit_once(CONFLICT_MARK)?;
    let (short, ext) = match after.split_once('.') {
        Some((short, ext)) => (short, Some(ext)),
        None => (after, None),
    };
    if stem.is_empty() || short.is_empty() || !short.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let origin = match ext {
        Some(ext) => format!("{stem}.{ext}"),
        None => stem.to_string(),
    };
    Some(match dir {
        Some(dir) => format!("{dir}/{origin}"),
        None => origin,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_covers_program_files_and_layouts_and_not_tooling() {
        for yes in [
            "main.weft",
            "triage.weft",
            "weft.toml",
            "nodes/mine/mod.rs",
            "nodes/pkg/metadata.json",
            "prompts/p.txt",
            "scripts/s.py",
            "sql/q.sql",
            "assets/pic.png",
            "examples/angry.json",
            "layouts/main.json",
            "sub/dir/inc.weft",
            // `target` is an ordinary word below the root.
            "nodes/target/mod.rs",
            "prompts/target/tone.txt",
            "src/greeter/mod.rs",
            "src/greeter/metadata.json",
            "src/notes.md",
            "nodes/base_catalog/x/mod.rs",
            "nodes/base_catalog/computer/agent.weft",
        ] {
            assert!(covers(yes), "{yes} is part of a version");
        }
        for no in [
            ".weft/target/x",
            ".git/HEAD",
            ".github/workflows/ci.weft",
            ".vscode/settings.json",
            "target/debug/x",
            "README.md",
            "notes.txt",
            "nodes/bridge/node_modules/left-pad/index.js",
            "nodes/pkg/__pycache__/helper.cpython-312.pyc",
            "nodes/pkg/images/app/.pytest_cache/v/cache/lastfailed",
            "scripts/tool.pyc",
            "front/src/routes/+page.svelte",
            "main.weft-conflict-1a2b3c4d.weft",
            "prompts/tone.weft-conflict-1a2b3c4d.txt",
        ] {
            assert!(!covers(no), "{no} is not part of a version");
        }
    }

    #[test]
    fn a_conflict_copy_sits_beside_its_file_keeps_its_extension_and_is_never_covered() {
        let hash = "1a2b3c4d5e6f".repeat(5);
        assert_eq!(conflict_copy_name("prompts/tone.txt", &hash), "prompts/tone.weft-conflict-1a2b3c4d.txt");
        assert_eq!(conflict_copy_name("main.weft", &hash), "main.weft-conflict-1a2b3c4d.weft");
        assert_eq!(conflict_copy_name("nodes/a/Makefile", &hash), "nodes/a/Makefile.weft-conflict-1a2b3c4d");
        assert_eq!(conflict_copy_name("assets/.env", &hash), "assets/.env.weft-conflict-1a2b3c4d");
        for path in ["prompts/tone.txt", "main.weft", "nodes/a/Makefile", "layouts/main.json"] {
            assert!(covers(path));
            assert!(!covers(&conflict_copy_name(path, &hash)), "the copy of {path} is not in a version");
            assert_eq!(conflict_copy_origin(&conflict_copy_name(path, &hash)).as_deref(), Some(path), "the copy names its file");
        }
        assert_eq!(conflict_copy_origin("assets/.env.weft-conflict-1a2b3c4d").as_deref(), Some("assets/.env"));
        assert_eq!(conflict_copy_origin("prompts/tone.txt"), None, "a file that is no copy names nothing");
    }
}
