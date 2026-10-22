//! A weft checkout on this machine: how to tell one, find the one a
//! process stands in, and read which branch it is on. The branch is what
//! picks the install a command acts on ([`crate::infra::Install::of_checkout`]),
//! and the checkout is what the stdlib catalog and the worker builds read
//! (`weft_catalog::weft_repo_root`).

use std::path::{Path, PathBuf};

/// Whether `p` is a weft checkout, judged by what the consumers of the
/// root actually read: this workspace's manifests + lockfile (staged into
/// every worker build context) and the stdlib catalog. One predicate for
/// every place that asks, so they cannot drift on what "a checkout" means.
// SYNC: is_weft_checkout <-> install.sh (is_weft_checkout)
pub fn is_weft_checkout(p: &Path) -> bool {
    p.join("crates/weft-catalog").is_dir()
        && p.join("Cargo.toml").is_file()
        && p.join("Cargo.lock").is_file()
        && p.join("catalog").is_dir()
}

/// The weft checkout `dir` is inside (`dir` itself or the nearest folder
/// above it that is one), `None` outside any.
pub fn enclosing(dir: &Path) -> Option<PathBuf> {
    dir.ancestors().find(|d| is_weft_checkout(d)).map(Path::to_path_buf)
}

/// The branch the checkout at `root` is on, `Ok(None)` on a detached HEAD
/// (or a HEAD pointing at anything but a branch). Read from git's own
/// files, so it needs no `git` binary: `<root>/.git` is the git folder in
/// a main checkout, and a file `gitdir: <path>` in a worktree. A checkout
/// with no `.git` at all (an unpacked archive) is an error: it has no
/// branch to pick an install by.
pub fn branch(root: &Path) -> Result<Option<String>, String> {
    let dot_git = root.join(".git");
    let git_dir = if dot_git.is_dir() {
        dot_git
    } else {
        let pointer = std::fs::read_to_string(&dot_git).map_err(|e| {
            format!("{} is not a git checkout ({}: {e}), so it is on no branch", root.display(), dot_git.display())
        })?;
        let target = pointer
            .trim()
            .strip_prefix("gitdir:")
            .map(str::trim)
            .ok_or_else(|| format!("{} does not start with `gitdir:`", dot_git.display()))?;
        // A relative gitdir is relative to the folder holding `.git`.
        root.join(target)
    };
    let head_file = git_dir.join("HEAD");
    let head = std::fs::read_to_string(&head_file).map_err(|e| format!("cannot read {}: {e}", head_file.display()))?;
    let branch = head.trim().strip_prefix("ref: refs/heads/").map(str::to_string);
    // A repository keeping its refs in a reftable (`extensions.refStorage
    // = reftable`) writes this fixed stub in HEAD and keeps the real one in
    // the table, which this does not read.
    if branch.as_deref() == Some(REFTABLE_HEAD_STUB) {
        return Err(format!(
            "{} keeps its refs in a reftable, whose current branch weft cannot read from {}; name the install \
             with WEFT_INSTALL=<name> (`default` for the main one)",
            root.display(),
            head_file.display()
        ));
    }
    Ok(branch)
}

/// What git writes in HEAD when the real one is in a reftable.
const REFTABLE_HEAD_STUB: &str = ".invalid";

#[cfg(test)]
mod tests {
    use super::*;

    fn checkout(at: &Path) {
        std::fs::create_dir_all(at.join("crates/weft-catalog")).unwrap();
        std::fs::create_dir_all(at.join("catalog")).unwrap();
        std::fs::write(at.join("Cargo.toml"), "").unwrap();
        std::fs::write(at.join("Cargo.lock"), "").unwrap();
    }

    /// The predicate demands everything the consumers read: the crate
    /// dir, the workspace manifests, and the catalog. Any one missing
    /// means "not a checkout", or a deeper consumer would fail with a
    /// worse message later.
    #[test]
    fn a_checkout_needs_crates_manifests_and_catalog() {
        for missing in ["crates/weft-catalog", "Cargo.toml", "Cargo.lock", "catalog"] {
            let dir = tempfile::tempdir().unwrap();
            checkout(dir.path());
            assert!(is_weft_checkout(dir.path()));
            let p = dir.path().join(missing);
            if p.is_dir() {
                std::fs::remove_dir_all(&p).unwrap();
            } else {
                std::fs::remove_file(&p).unwrap();
            }
            assert!(!is_weft_checkout(dir.path()), "should fail without {missing}");
        }
    }

    #[test]
    fn the_enclosing_checkout_is_found_from_any_folder_inside_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("weft");
        checkout(&root);
        std::fs::create_dir_all(root.join("crates/x/src")).unwrap();
        assert_eq!(enclosing(&root.join("crates/x/src")), Some(root.clone()));
        assert_eq!(enclosing(&root), Some(root));
        assert_eq!(enclosing(dir.path()), None);
    }

    #[test]
    fn the_branch_is_read_from_a_main_checkout_a_worktree_and_a_detached_head() {
        let dir = tempfile::tempdir().unwrap();
        let main = dir.path().join("main");
        std::fs::create_dir_all(main.join(".git/worktrees/wt")).unwrap();
        std::fs::write(main.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        assert_eq!(branch(&main).unwrap().as_deref(), Some("main"));

        // A worktree's `.git` is a file pointing at its own git folder,
        // relative here as git writes it with `worktree.useRelativePaths`.
        let wt = dir.path().join("wt");
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(wt.join(".git"), "gitdir: ../main/.git/worktrees/wt\n").unwrap();
        std::fs::write(main.join(".git/worktrees/wt/HEAD"), "ref: refs/heads/fix/journal-holes\n").unwrap();
        assert_eq!(branch(&wt).unwrap().as_deref(), Some("fix/journal-holes"));

        std::fs::write(main.join(".git/worktrees/wt/HEAD"), "60bdaf09a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6\n").unwrap();
        assert_eq!(branch(&wt).unwrap(), None);

        std::fs::write(main.join(".git/HEAD"), "ref: refs/heads/.invalid\n").unwrap();
        assert!(branch(&main).unwrap_err().contains("reftable"));

        let bare = dir.path().join("unpacked");
        std::fs::create_dir_all(&bare).unwrap();
        assert!(branch(&bare).unwrap_err().contains("not a git checkout"));
    }
}
