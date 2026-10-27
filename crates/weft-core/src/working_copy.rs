//! Working copies: the places a project's code is being edited, which the
//! install holds.
//!
//! The install's version tree is where a project's code lives. A working
//! copy is one place it is being edited: a folder on somebody's machine
//! (`folder`), or the copy a program writes into with no folder at all
//! (`program`). Each one has a head (the version it was last stamped as,
//! and the run the next `--seed` inherits from) and its **working
//! manifest**: its files as they are right now, `path -> sha256`, the same
//! shape a version has. Editing moves the working manifest and never the
//! history; an action that does something (a build, a run, a checkpoint)
//! first stamps it into a version.
//!
//! A folder and its working copy are kept the same both ways by a syncer
//! (the local runtime watching the folder, or the CLI before a command):
//! [`plan_sync`] is what it decides, from three manifests. What the folder
//! held the last time the two agreed is the working copy's `synced` map, so
//! a syncer can tell a change made on disk from one made in the install.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::project::hash::Manifest;
use crate::RunId;

/// A working copy's id.
pub type WorkingCopyId = uuid::Uuid;

/// On the 404 `GET /projects/{id}/working-copies/folder` answers when the
/// folder is no working copy yet, so a caller registering it can tell
/// that apart from a project it may not see.
pub const NO_WORKING_COPY_HEADER: &str = "x-weft-no-working-copy";

/// What kind of place a working copy is.
// SYNC: WorkingCopyKind <-> crates/weft-dispatcher/src/working_copies.rs (the `kind` column's CHECK), extension-vscode/src/sidebar/version-tree.ts WorkingCopy.kind
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkingCopyKind {
    /// A folder on a machine, kept in sync with its files.
    Folder,
    /// The copy programs write into, with no folder behind it. A project
    /// has at most one.
    Program,
}

impl WorkingCopyKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkingCopyKind::Folder => "folder",
            WorkingCopyKind::Program => "program",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "folder" => Ok(WorkingCopyKind::Folder),
            "program" => Ok(WorkingCopyKind::Program),
            other => Err(format!("'{other}' is no kind of working copy")),
        }
    }
}

/// A working copy's head: git's HEAD and nothing more. The version its
/// next stamp parents on, and the run the next `--seed` inherits from
/// (none when head is a bare version).
// SYNC: Head <-> extension-vscode/src/sidebar/version-tree.ts WorkingCopy (headVersion, headRun)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Head {
    pub head_version: Option<String>,
    pub head_run: Option<RunId>,
}

/// One working copy, as the tree and every working-copy answer show it.
// SYNC: WorkingCopy <-> extension-vscode/src/sidebar/version-tree.ts WorkingCopy
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkingCopy {
    pub id: WorkingCopyId,
    pub kind: WorkingCopyKind,
    /// The machine a folder is on (its host name), and the folder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(flatten)]
    pub head: Head,
    /// The id the working manifest would have as a version
    /// (`manifest_version_id`): equal to `headVersion` when nothing changed
    /// since the last stamp.
    pub working_version: String,
    /// How many times the working manifest changed: what a syncer holds
    /// its reading against.
    pub revision: u64,
    /// The conflict copies the last sync found in the folder, each
    /// project-relative (`prompts/tone.weft-conflict-1a2b3c4d.txt`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,
    /// When it last changed, unix seconds.
    pub updated_at: u64,
}

impl WorkingCopy {
    /// Whether its files changed since its head was stamped.
    pub fn is_changed(&self) -> bool {
        self.head.head_version.as_deref() != Some(self.working_version.as_str())
    }
}

/// What a syncer reads of a working copy before it decides anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkingState {
    pub copy: WorkingCopy,
    /// The working manifest itself.
    pub working: Manifest,
    /// What the folder held, path by path, the last time a syncer saw it
    /// and the working copy agree. Empty for a program's copy.
    pub synced: Manifest,
}

/// How a change expects the path it changes to stand in the working
/// manifest: a writer that read the path before deciding names what it
/// read, and the change is refused if somebody moved it since.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "expect", rename_all = "snake_case")]
pub enum Expect {
    /// Whatever stands there now (a program writing a file outright).
    Any,
    /// Nothing: the path holds no file.
    Absent,
    /// This content.
    Hash { hash: String },
}

impl Expect {
    /// What a writer that read `now` expects.
    pub fn read(now: Option<&String>) -> Self {
        match now {
            Some(hash) => Expect::Hash { hash: hash.clone() },
            None => Expect::Absent,
        }
    }

    /// Whether `now` is what this expects.
    pub fn holds(&self, now: Option<&String>) -> bool {
        match self {
            Expect::Any => true,
            Expect::Absent => now.is_none(),
            Expect::Hash { hash } => now == Some(hash),
        }
    }
}

/// One change to a working manifest: `path` takes the content `hash`, or
/// goes (`None`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkingChange {
    pub path: String,
    pub hash: Option<String>,
    pub expected: Expect,
}

/// What a syncer hands back after a pass: the changes the folder brings
/// to the working manifest, what the folder holds now for the paths it
/// looked at (`None`: no file there), and every conflict copy it saw on
/// disk (replacing the list the working copy kept). Applied whole or not
/// at all.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncReport {
    #[serde(default)]
    pub changes: Vec<WorkingChange>,
    #[serde(default)]
    pub synced: BTreeMap<String, Option<String>>,
    #[serde(default)]
    pub conflicts: Vec<String>,
}

/// What applying changes answered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "outcome")]
pub enum Applied {
    /// Every change landed; the working copy is now this.
    Applied { copy: WorkingCopy },
    /// Nothing landed: these paths do not stand as their change expected.
    /// Read the working copy again and decide again.
    Stale { paths: Vec<String> },
}

/// `POST /projects/{id}/working-copies/folder`: the folder working copy
/// at `path` on `host`, made the first time with the folder's own files
/// (`manifest`, every blob already stored) as its working manifest and
/// the project's most recently changed head as its head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterFolder {
    pub host: String,
    pub path: String,
    pub manifest: Manifest,
}

/// Which working copy a call means: one by its id, or the project's
/// default, which is its most recently changed folder (so a person's
/// folder follows what a program writes for it) or, with none, the copy
/// programs write into.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkingCopyRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_copy: Option<WorkingCopyId>,
}

/// `POST /projects/{id}/working-copies/changes`: change files of a
/// working copy (a program writing code). Every blob a change names is
/// already stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeFiles {
    #[serde(flatten)]
    pub working_copy: WorkingCopyRef,
    pub changes: Vec<WorkingChange>,
}

/// One file a program writes into a working copy ([`WriteFiles`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileWrite {
    pub path: String,
    #[serde(flatten)]
    pub content: FileContent,
    /// How the path must stand before the write lands; anything (a program
    /// writing a file outright) when left out.
    #[serde(default = "expect_any")]
    pub expected: Expect,
}

fn expect_any() -> Expect {
    Expect::Any
}

/// What a [`FileWrite`] puts at its path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "content", rename_all = "snake_case")]
pub enum FileContent {
    /// These bytes, as text: code, a prompt, an example.
    Text { text: String },
    /// The bytes of a file the program stored (`ctx.storage(..)`), by its
    /// key: what a large or binary file is written from.
    Stored { key: String },
    /// No file: the path goes.
    Removed,
}

/// The most bytes of text one [`WriteFiles`] carries in itself: it travels
/// as a task row, which is no place for a large file. Past it, a program
/// stores the file first and writes it by key ([`FileContent::Stored`]).
pub const MAX_WRITTEN_TEXT_BYTES: usize = 4 * 1024 * 1024;

/// A program writing files into a working copy (named, or the project's
/// default): every file lands, or none does.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteFiles {
    #[serde(flatten)]
    pub working_copy: WorkingCopyRef,
    pub files: Vec<FileWrite>,
}

/// Where a switch goes: a run (its version, with the run as head's run,
/// so the next `--seed` inherits from it) or a bare version.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwitchTo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<RunId>,
}

/// `POST /projects/{id}/versions/switch`: move a working copy (named, or
/// the project's default) to a version or a run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwitchRequest {
    #[serde(flatten)]
    pub working_copy: WorkingCopyRef,
    #[serde(flatten)]
    pub to: SwitchTo,
}

/// What a switch answered: the version the files were before it (stamped
/// first, so nothing is lost and `switch <saved>` comes back; none when the
/// working copy held no file), and where the working copy is now.
// SYNC: Switched <-> extension-vscode/src/sidebar/version-tree.ts Switched
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Switched {
    pub saved: Option<String>,
    pub copy: WorkingCopy,
}

/// What forgetting a working copy answered: the version its files were
/// recorded as before it went (none when it held no file), so nothing it
/// held is lost and `switch <saved>` brings them to another copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Forgotten {
    pub saved: Option<String>,
}

// ----- what a syncer decides ----------------------------------------------

/// What one sync pass does, decided from three manifests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncPlan {
    /// What the folder brings to the working manifest: `path -> content`
    /// (`None`: the file went), each expecting the path to stand as it was
    /// read.
    pub push: Vec<WorkingChange>,
    /// What the working copy brings to the folder: `path -> content`
    /// (`None`: remove the file), written only while the file on disk still
    /// holds what `synced` says.
    pub write: BTreeMap<String, Option<String>>,
    /// Paths both sides changed. The folder's file stays and goes to the
    /// working copy (in `push`); the incoming content, when there is one,
    /// is written beside it as a conflict copy.
    pub conflicts: Vec<Conflict>,
    /// What the folder will hold for every path this pass decided about,
    /// once it is carried out.
    pub synced: BTreeMap<String, Option<String>>,
}

impl SyncPlan {
    /// Whether the pass has anything to do.
    pub fn is_empty(&self) -> bool {
        self.push.is_empty() && self.write.is_empty() && self.conflicts.is_empty() && self.synced.is_empty()
    }
}

/// A path both the folder and the working copy changed since they last
/// agreed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub path: String,
    /// What the folder holds there and keeps (`None`: it removed the file,
    /// and the removal is what goes to the working copy).
    pub folder: Option<String>,
    /// What the working copy holds there (`None`: it removed the file, so
    /// there is no copy to write).
    pub incoming: Option<String>,
    /// Where the incoming content is written beside the folder's file.
    pub copy_path: Option<String>,
}

/// Decide one sync pass from what the folder holds now (`disk`), what it
/// held the last time it and the working copy agreed (`synced`), and the
/// working manifest (`working`). For each path:
///
/// - all three agree, or disk and working agree: nothing to move (the
///   path is recorded as synced when `synced` lagged);
/// - only the disk moved: the folder's content goes to the working copy;
/// - only the working copy moved: its content is written to the folder;
/// - both moved, differently: a conflict. The folder's file is kept and
///   goes to the working copy, so the working copy is what the person sees
///   in their editor, and the incoming content is written beside it.
pub fn plan_sync(disk: &Manifest, synced: &Manifest, working: &Manifest) -> SyncPlan {
    let mut plan = SyncPlan::default();
    let paths: std::collections::BTreeSet<&String> = disk.keys().chain(synced.keys()).chain(working.keys()).collect();
    for path in paths {
        let (d, s, w) = (disk.get(path), synced.get(path), working.get(path));
        if d == w {
            if s != d {
                plan.synced.insert(path.clone(), d.cloned());
            }
            continue;
        }
        let disk_moved = d != s;
        let working_moved = w != s;
        match (disk_moved, working_moved) {
            (true, false) => {
                plan.push.push(WorkingChange { path: path.clone(), hash: d.cloned(), expected: Expect::read(w) });
                plan.synced.insert(path.clone(), d.cloned());
            }
            (false, true) => {
                plan.write.insert(path.clone(), w.cloned());
                plan.synced.insert(path.clone(), w.cloned());
            }
            // `d != w` with neither moved cannot be: both equal `s`.
            (false, false) => unreachable!("disk and working agree with synced, so with each other"),
            (true, true) => {
                plan.push.push(WorkingChange { path: path.clone(), hash: d.cloned(), expected: Expect::read(w) });
                plan.synced.insert(path.clone(), d.cloned());
                plan.conflicts.push(Conflict {
                    path: path.clone(),
                    folder: d.cloned(),
                    incoming: w.cloned(),
                    copy_path: w.map(|hash| crate::project::coverage::conflict_copy_name(path, hash)),
                });
            }
        }
    }
    plan
}

/// The working manifest `changes` make of `working`, or the paths whose
/// change does not find what it expected (nothing applied then).
pub fn apply_changes(working: &Manifest, changes: &[WorkingChange]) -> Result<Manifest, Vec<String>> {
    let stale: Vec<String> = changes
        .iter()
        .filter(|change| !change.expected.holds(working.get(&change.path)))
        .map(|change| change.path.clone())
        .collect();
    if !stale.is_empty() {
        return Err(stale);
    }
    let mut out = working.clone();
    for change in changes {
        match &change.hash {
            Some(hash) => out.insert(change.path.clone(), hash.clone()),
            None => out.remove(&change.path),
        };
    }
    Ok(out)
}

/// `synced` with a pass's report of what the folder holds folded in.
pub fn fold_synced(synced: &Manifest, report: &BTreeMap<String, Option<String>>) -> Manifest {
    let mut out = synced.clone();
    for (path, now) in report {
        match now {
            Some(hash) => out.insert(path.clone(), hash.clone()),
            None => out.remove(path),
        };
    }
    out
}

/// Whether this machine hears no file changes in the folder at `path`
/// (`on_wsl`: it runs on WSL, [`on_wsl`]). On WSL, a Windows drive mounted
/// under `/mnt/<letter>/` reports none to Linux at all, so whatever waits
/// on changes there (a working copy kept in sync, a folder watch) would
/// never hear one; each refuses such a folder in its own words.
pub fn hears_no_changes(path: &str, on_wsl: bool) -> bool {
    let Some(drive) = path.strip_prefix("/mnt/").and_then(|rest| rest.split('/').next()) else {
        return false;
    };
    on_wsl && drive.len() == 1 && drive.chars().all(|c| c.is_ascii_alphabetic())
}

/// Whether this process runs on WSL, as its kernel says (read once).
pub fn on_wsl() -> bool {
    static ON_WSL: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON_WSL.get_or_init(|| {
        ["/proc/sys/kernel/osrelease", "/proc/version"]
            .iter()
            .any(|file| std::fs::read_to_string(file).is_ok_and(|text| text.to_ascii_lowercase().contains("microsoft")))
    })
}

/// This machine's host name: which machine a folder working copy is on,
/// read the same way by everything that registers or watches one (the
/// CLI, the local runtime's folder sync). Linux reads it from the kernel
/// (`/proc/sys/kernel/hostname`); elsewhere `hostname` says it.
pub fn this_machine() -> Result<String, String> {
    let raw = if std::path::Path::new("/proc/sys/kernel/hostname").exists() {
        std::fs::read("/proc/sys/kernel/hostname").map_err(|e| format!("read this machine's host name: {e}"))?
    } else {
        let out = std::process::Command::new("hostname").output().map_err(|e| format!("run `hostname`: {e}"))?;
        if !out.status.success() {
            return Err(format!("`hostname` failed ({}): {}", out.status, String::from_utf8_lossy(&out.stderr).trim()));
        }
        out.stdout
    };
    machine_name_of(&raw)
}

/// The host name `raw` spells (its trailing line end dropped), refusing
/// one that is empty or not UTF-8: it names a folder's machine to the
/// install, so it is never spelled approximately.
fn machine_name_of(raw: &[u8]) -> Result<String, String> {
    let name = std::str::from_utf8(raw)
        .map_err(|_| "this machine's host name is not UTF-8, so it cannot name the machine a working copy is on".to_string())?
        .trim_end_matches(['\n', '\r']);
    if name.is_empty() {
        return Err("this machine has no host name, which names the machine a working copy is on; set one (`hostnamectl set-hostname <name>`)".into());
    }
    Ok(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(entries: &[(&str, &str)]) -> Manifest {
        entries.iter().map(|(p, h)| (p.to_string(), h.to_string())).collect()
    }

    fn hash(c: char) -> String {
        c.to_string().repeat(64)
    }

    #[test]
    fn a_path_only_the_folder_changed_goes_to_the_working_copy() {
        let plan = plan_sync(&m(&[("a", "2")]), &m(&[("a", "1")]), &m(&[("a", "1")]));
        assert_eq!(plan.push, vec![WorkingChange { path: "a".into(), hash: Some("2".into()), expected: Expect::Hash { hash: "1".into() } }]);
        assert!(plan.write.is_empty() && plan.conflicts.is_empty());
        assert_eq!(plan.synced, [("a".to_string(), Some("2".to_string()))].into());
        // A file removed on disk goes too, and one made on disk comes in.
        let plan = plan_sync(&m(&[("new", "9")]), &m(&[("gone", "1")]), &m(&[("gone", "1")]));
        let pushed: Vec<(&str, Option<&str>)> = plan.push.iter().map(|c| (c.path.as_str(), c.hash.as_deref())).collect();
        assert_eq!(pushed, vec![("gone", None), ("new", Some("9"))]);
        assert_eq!(plan.push[1].expected, Expect::Absent);
    }

    #[test]
    fn a_path_only_the_working_copy_changed_is_written_to_the_folder() {
        let plan = plan_sync(&m(&[("a", "1"), ("b", "1")]), &m(&[("a", "1"), ("b", "1")]), &m(&[("a", "2"), ("c", "3")]));
        assert!(plan.push.is_empty() && plan.conflicts.is_empty());
        assert_eq!(plan.write, [("a".to_string(), Some("2".to_string())), ("b".to_string(), None), ("c".to_string(), Some("3".to_string()))].into());
        assert_eq!(plan.synced, plan.write);
    }

    #[test]
    fn a_path_both_changed_keeps_the_folders_file_and_writes_the_other_beside_it() {
        let (s, d, w) = (hash('1'), hash('2'), hash('3'));
        let plan = plan_sync(&m(&[("p/t.txt", &d)]), &m(&[("p/t.txt", &s)]), &m(&[("p/t.txt", &w)]));
        assert_eq!(plan.push, vec![WorkingChange { path: "p/t.txt".into(), hash: Some(d.clone()), expected: Expect::Hash { hash: w.clone() } }]);
        assert!(plan.write.is_empty());
        assert_eq!(
            plan.conflicts,
            vec![Conflict {
                path: "p/t.txt".into(),
                folder: Some(d.clone()),
                incoming: Some(w.clone()),
                copy_path: Some("p/t.weft-conflict-33333333.txt".into())
            }]
        );
        // The folder removed what the working copy changed: the removal
        // stays, and the change is written beside where the file was.
        let plan = plan_sync(&Manifest::new(), &m(&[("p/t.txt", &s)]), &m(&[("p/t.txt", &w)]));
        assert_eq!(plan.conflicts[0].folder, None);
        assert_eq!(plan.conflicts[0].copy_path.as_deref(), Some("p/t.weft-conflict-33333333.txt"));
        assert_eq!(plan.push[0].hash, None, "the removal goes up");
        // The working copy removed what the folder edited: the edit stays,
        // and there is nothing to write beside it.
        let plan = plan_sync(&m(&[("a", &d)]), &m(&[("a", &s)]), &Manifest::new());
        assert_eq!(plan.conflicts, vec![Conflict { path: "a".into(), folder: Some(d.clone()), incoming: None, copy_path: None }]);
        assert_eq!(plan.push[0].expected, Expect::Absent);
    }

    #[test]
    fn sides_that_already_agree_only_record_it() {
        let plan = plan_sync(&m(&[("a", "2")]), &m(&[("a", "1")]), &m(&[("a", "2")]));
        assert!(plan.push.is_empty() && plan.write.is_empty() && plan.conflicts.is_empty());
        assert_eq!(plan.synced, [("a".to_string(), Some("2".to_string()))].into());
        assert!(plan_sync(&m(&[("a", "1")]), &m(&[("a", "1")]), &m(&[("a", "1")])).is_empty());
    }

    #[test]
    fn a_change_lands_only_where_it_finds_what_it_expected() {
        let working = m(&[("a", "1"), ("b", "2")]);
        let change = |path: &str, hash: Option<&str>, expected: Expect| WorkingChange { path: path.into(), hash: hash.map(str::to_string), expected };
        let applied = apply_changes(&working, &[
            change("a", Some("9"), Expect::Hash { hash: "1".into() }),
            change("b", None, Expect::Any),
            change("c", Some("3"), Expect::Absent),
        ]).unwrap();
        assert_eq!(applied, m(&[("a", "9"), ("c", "3")]));
        let stale = apply_changes(&working, &[
            change("a", Some("9"), Expect::Hash { hash: "0".into() }),
            change("b", Some("8"), Expect::Absent),
            change("c", Some("3"), Expect::Absent),
        ]).unwrap_err();
        assert_eq!(stale, vec!["a", "b"]);
    }

    /// The wire shapes, as the CLI and the editor read them.
    #[test]
    fn the_wire_shapes_round_trip_in_camel_case() {
        let copy = WorkingCopy {
            id: uuid::Uuid::nil(),
            kind: WorkingCopyKind::Folder,
            host: Some("box".into()),
            path: Some("/home/me/shop".into()),
            head: Head { head_version: Some("v1".into()), head_run: None },
            working_version: "v2".into(),
            revision: 3,
            conflicts: vec![],
            updated_at: 7,
        };
        let wire = serde_json::to_value(&copy).unwrap();
        assert_eq!(wire["headVersion"], "v1");
        assert_eq!(wire["workingVersion"], "v2");
        assert_eq!(wire["kind"], "folder");
        assert!(wire.get("conflicts").is_none(), "{wire}");
        assert_eq!(serde_json::from_value::<WorkingCopy>(wire).unwrap(), copy);
        assert!(copy.is_changed());

        let report = SyncReport {
            changes: vec![WorkingChange { path: "a".into(), hash: None, expected: Expect::Hash { hash: "1".into() } }],
            synced: [("a".to_string(), None)].into(),
            conflicts: vec!["a.weft-conflict-11111111".into()],
        };
        let wire = serde_json::to_value(&report).unwrap();
        assert_eq!(wire["changes"][0], serde_json::json!({ "path": "a", "hash": null, "expected": { "expect": "hash", "hash": "1" } }));
        assert_eq!(serde_json::from_value::<SyncReport>(wire).unwrap(), report);
        let stale = serde_json::to_value(Applied::Stale { paths: vec!["a".into()] }).unwrap();
        assert_eq!(stale, serde_json::json!({ "outcome": "stale", "paths": ["a"] }));
    }

    #[test]
    fn only_a_windows_drive_on_wsl_hears_no_changes() {
        assert!(hears_no_changes("/mnt/c/Users/me/project", true), "a Windows drive on WSL");
        assert!(hears_no_changes("/mnt/D", true), "the drive itself");
        assert!(!hears_no_changes("/mnt/c/Users/me/project", false), "a real /mnt/c on Linux is a folder like any other");
        assert!(!hears_no_changes("/home/me/project", true));
        assert!(!hears_no_changes("/mnt/data/project", true), "a mount that is not a drive letter");
        assert!(!hears_no_changes("/mnt", true));
    }

    #[test]
    fn a_machine_name_is_read_whole_and_refused_when_empty_or_not_utf8() {
        assert_eq!(machine_name_of(b"box\n").unwrap(), "box");
        assert!(machine_name_of(&[0x66, 0xff]).unwrap_err().contains("not UTF-8"));
        assert!(machine_name_of(b"\n").unwrap_err().contains("no host name"));
        assert!(!this_machine().unwrap().is_empty());
    }
}
