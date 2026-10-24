use super::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// ── MapFileReader (Layer 1, pure: no filesystem) ────────────────────────────

fn map(entries: &[(&str, &str)]) -> MapFileReader {
    let mut files = BTreeMap::new();
    for (k, v) in entries {
        files.insert(PathBuf::from(k), (*v).to_string());
    }
    MapFileReader::new(files)
}

#[test]
fn map_reads_content_and_identity() {
    let reader = map(&[("proj/prompts/system.txt", "be helpful")]);
    let resolved = reader
        .resolve_and_read(Path::new("proj"), Path::new("proj/prompts"), Path::new("system.txt"))
        .unwrap();
    assert_eq!(resolved.content, "be helpful");
    // Identity is the normalized join, the key downstream uses for cycle
    // detection and for deriving the included file's directory.
    assert_eq!(resolved.file.identity, PathBuf::from("proj/prompts/system.txt"));
}

#[test]
fn map_resolves_dot_and_parent_within_root() {
    let reader = map(&[("proj/a.txt", "A")]);
    // `sub/../a.txt` from base `proj` normalizes to `proj/a.txt`: a `..` that
    // stays within the path is fine (it cancels a real component).
    let resolved = reader
        .resolve_and_read(Path::new("proj"), Path::new("proj"), Path::new("sub/../a.txt"))
        .unwrap();
    assert_eq!(resolved.file.identity, PathBuf::from("proj/a.txt"));
    assert_eq!(resolved.content, "A");
}

#[test]
fn map_missing_file_errors() {
    let reader = map(&[("proj/a.txt", "A")]);
    let err = reader
        .resolve_and_read(Path::new("proj"), Path::new("proj"), Path::new("b.txt"))
        .unwrap_err();
    assert!(err.contains("not found"), "got: {err}");
}

#[test]
fn map_rejects_escape_above_root() {
    // `../secret.txt` from base `proj` climbs above `proj`, the lexical
    // equivalent of the disk backing's canonicalize-then-prefix-check escape.
    let reader = map(&[("secret.txt", "leak")]);
    let err = reader
        .resolve_and_read(Path::new("proj"), Path::new("proj"), Path::new("../secret.txt"))
        .unwrap_err();
    assert!(err.contains("escapes"), "got: {err}");
}

// ── normalize_lexical (Layer 1, pure) ───────────────────────────────────────

#[test]
fn normalize_collapses_cur_and_parent() {
    assert_eq!(
        normalize_lexical(Path::new("a/./b/../c")),
        Some(PathBuf::from("a/c"))
    );
}

#[test]
fn normalize_rejects_climb_above_relative_root() {
    assert_eq!(normalize_lexical(Path::new("../x")), None);
    assert_eq!(normalize_lexical(Path::new("a/../../x")), None);
}

// ── Disk / map parity on escape (Layer 3 for disk, Layer 1 for map) ──────────

#[test]
fn disk_and_map_reject_the_same_escape() {
    // Both backings reject a `../` that leaves the root, so a `@file("../x")`
    // fails identically whether the project is on disk or in an in-memory map.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(dir.path().join("secret.txt"), "leak").unwrap();

    let disk_err = DiskFileReader
        .resolve_and_read(&root, &root, Path::new("../secret.txt"))
        .unwrap_err();
    assert!(disk_err.contains("escapes"), "disk: {disk_err}");

    let map_err = map(&[("secret.txt", "leak")])
        .resolve_and_read(Path::new("project"), Path::new("project"), Path::new("../secret.txt"))
        .unwrap_err();
    assert!(map_err.contains("escapes"), "map: {map_err}");
}

#[test]
fn a_climb_that_stays_inside_the_root_is_allowed_on_both_backings() {
    // An `@include` path is relative to the file that writes it, and the
    // program lives in `src/`, so `@include("../lib/x.weft")` climbs one
    // level and lands inside the project: the wall is the root, not the
    // writing file's own directory.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::create_dir_all(root.join("assets")).unwrap();
    std::fs::write(root.join("assets/x.txt"), "inside").unwrap();
    let disk = DiskFileReader
        .resolve_and_read(&root, &root.join("src"), Path::new("../assets/x.txt"))
        .unwrap();
    assert_eq!(disk.content, "inside");

    let map = map(&[("project/assets/x.txt", "inside")])
        .resolve_and_read(Path::new("project"), Path::new("project/src"), Path::new("../assets/x.txt"))
        .unwrap();
    assert_eq!(map.content, "inside");
    assert_eq!(map.file.identity, PathBuf::from("project/assets/x.txt"));
}

// ── The `__catalog/` prefix (Layer 1 on the map, Layer 3 on disk) ──────────

#[test]
fn a_catalog_path_is_read_from_nodes_wherever_it_is_written() {
    let reader = map(&[
        ("proj/nodes/base_catalog/computer/agent.weft", "stdlib"),
        ("proj/nodes/my_pkg/x.weft", "own"),
    ]);
    // From the program, from deep in `src/`, and from `nodes/` itself:
    // the anchor never matters.
    for base in ["proj/src", "proj/src/billing/stripe", "proj/nodes/my_pkg"] {
        let stdlib = reader
            .resolve_and_read(Path::new("proj"), Path::new(base), Path::new("__catalog/base_catalog/computer/agent.weft"))
            .unwrap();
        assert_eq!(stdlib.file.identity, PathBuf::from("proj/nodes/base_catalog/computer/agent.weft"));
        assert_eq!(stdlib.content, "stdlib");
        let own = reader
            .resolve_and_read(Path::new("proj"), Path::new(base), Path::new("__catalog/my_pkg/x.weft"))
            .unwrap();
        assert_eq!(own.content, "own");
    }
    // Only the first segment is the prefix: one further in is a folder.
    assert_eq!(catalog_path(Path::new("lib/__catalog/x.weft")), None);
    assert_eq!(catalog_path(Path::new("./__catalog/x.weft")), None);
    assert_eq!(catalog_path(Path::new("__catalog/a/../x.weft")), Some(Ok(PathBuf::from("nodes/x.weft"))));
}

#[test]
fn a_catalog_path_that_climbs_out_of_nodes_is_refused_on_both_backings() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::create_dir_all(root.join("nodes")).unwrap();
    std::fs::write(root.join("src/x.weft"), "Group {}").unwrap();
    let reader = map(&[("project/src/x.weft", "Group {}"), ("project/nodes/x.weft", "Group {}")]);
    // Into the project beside `nodes/`, and out then back in: both are
    // refused, the file being there or not.
    for written in ["__catalog/../src/x.weft", "__catalog/a/../../nodes/x.weft", "__catalog/../../etc/passwd"] {
        let expected = format!(
            "path {:?} escapes the project's nodes/ folder: a `__catalog/` path is read from nodes/ and has to stay inside it",
            Path::new(written)
        );
        let disk = DiskFileReader.resolve_and_read(&root, &root.join("src"), Path::new(written)).unwrap_err();
        assert_eq!(disk, expected);
        let in_map = reader
            .resolve_and_read(Path::new("project"), Path::new("project/src"), Path::new(written))
            .unwrap_err();
        assert_eq!(in_map, expected);
    }
}

#[test]
fn a_catalog_path_reads_what_its_nodes_spelling_reads() {
    // `__catalog/x` and `nodes/x` are one file in every marker: a link
    // under `nodes/` into the project is followed like any link in it
    // (an `@file("nodes/linked/x.weft")` reads it too), and one out of the
    // project is refused like any read out of it.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("project");
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::create_dir_all(root.join("nodes")).unwrap();
    std::fs::write(root.join("src/x.weft"), "Group {}").unwrap();
    std::fs::write(dir.path().join("outside.weft"), "Group {}").unwrap();
    std::os::unix::fs::symlink(root.join("src"), root.join("nodes/linked")).unwrap();
    std::os::unix::fs::symlink(dir.path(), root.join("nodes/away")).unwrap();
    let read = DiskFileReader
        .resolve_and_read(&root, &root.join("src"), Path::new("__catalog/linked/x.weft"))
        .unwrap();
    assert_eq!(read.content, "Group {}");
    let err = DiskFileReader
        .resolve_and_read(&root, &root.join("src"), Path::new("__catalog/away/outside.weft"))
        .unwrap_err();
    assert!(err.contains("escapes the project root"), "{err}");
}

#[cfg(unix)]
#[test]
fn a_root_spelled_through_a_symlink_gives_the_same_path_from_the_root() {
    // A project compiled from a symlinked folder (macOS's `/var` is one)
    // names its files exactly as one compiled from the real folder: the
    // path from the root is what a body id is built from, so the two
    // compiles give an included file's nodes the same ids.
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    std::fs::create_dir_all(real.join("src")).unwrap();
    std::fs::write(real.join("src/setup.weft"), "Group() {}").unwrap();
    let linked = dir.path().join("linked");
    std::os::unix::fs::symlink(&real, &linked).unwrap();
    for root in [&real, &linked] {
        let resolved = DiskFileReader
            .resolve_and_read(root, &root.join("src"), Path::new("setup.weft"))
            .unwrap();
        assert_eq!(resolved.file.from_root, PathBuf::from("src/setup.weft"));
        assert_eq!(crate::source_name::body_id(&resolved.file.from_root), "@src:setup");
    }
}
