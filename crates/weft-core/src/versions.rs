//! What the install's version-tree endpoints answer and the CLI reads
//! (`/projects/{id}/versions/...`): one Rust type per message, so the
//! two ends cannot drift.

use serde::{Deserialize, Serialize};

use crate::project::hash::Manifest;
use crate::run_spec::RunSpec;
use crate::working_copy::{WorkingCopy, WorkingCopyRef};
use crate::RunId;

/// The paths that differ between two versions: added, removed, and
/// changed, sorted. What `weft tree` prints next to a version.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

/// What changed from `parent` to `child`.
pub fn manifest_diff(parent: &Manifest, child: &Manifest) -> ManifestDiff {
    let mut diff = ManifestDiff::default();
    for (path, hash) in child {
        match parent.get(path) {
            None => diff.added.push(path.clone()),
            Some(h) if h != hash => diff.changed.push(path.clone()),
            Some(_) => {}
        }
    }
    for path in parent.keys() {
        if !child.contains_key(path) {
            diff.removed.push(path.clone());
        }
    }
    diff
}

// SYNC: VersionSummary <-> extension-vscode/src/sidebar/version-tree.ts VersionSummary
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionSummary {
    pub id: String,
    pub parent_id: Option<String>,
    pub label: Option<String>,
    pub created_at: u64,
    /// What changed against the parent (empty on a root).
    pub diff: ManifestDiff,
    pub manifest: Manifest,
    /// How many runs the project's triggers started on this version (each
    /// one too many to list), and the newest.
    #[serde(default)]
    pub trigger_runs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_trigger_run: Option<RunId>,
}

// SYNC: VersionRun <-> extension-vscode/src/sidebar/version-tree.ts VersionRun
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionRun {
    pub run_id: RunId,
    pub version_id: String,
    /// The program this run executed. `weft freeze` records it on the
    /// example so a later reader can tell which definition the frozen
    /// wires came from.
    pub definition_hash: String,
    pub seed_run_id: Option<RunId>,
    pub stale: Vec<String>,
    pub spec: Option<RunSpec>,
    pub example: Option<String>,
    /// When it started, as the version tree recorded it: there whether
    /// or not its record still is.
    pub started_at: u64,
    /// The run as the runs listing shows it (where it stands, when it
    /// ended, who stopped it, what it skipped), or `None` when its record
    /// is gone.
    // SYNC: VersionRun.listing <-> extension-vscode/src/sidebar/version-tree.ts VersionRun.listing
    pub listing: Option<crate::program::RunSummary>,
}

/// `GET /projects/{id}/versions/tree`: the project's working copies (each
/// with its head), the versions some activation's listeners run, and
/// every version and run started by hand.
// SYNC: VersionTree <-> extension-vscode/src/sidebar/version-tree.ts TreeJson
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionTree {
    pub working_copies: Vec<WorkingCopy>,
    /// Every version some activation's listeners run (one per trigger and
    /// owner at most, deduplicated, sorted). Empty while nothing listens.
    #[serde(default)]
    pub activated_versions: Vec<String>,
    pub versions: Vec<VersionSummary>,
    pub runs: Vec<VersionRun>,
}

/// `GET /projects/{id}/versions/running`: the files of the program this
/// install holds, and the version they are.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningSource {
    pub version: String,
    pub manifest: Manifest,
}

/// What a started run answers (`POST /projects/{id}/versions/run`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStarted {
    pub run_id: RunId,
    pub version: String,
    pub seed: Option<RunId>,
    /// Nodes taken from the seed, sorted.
    pub inherited: Vec<String>,
    /// Nodes this run executes, sorted.
    pub ran: Vec<String>,
    pub warnings: Vec<String>,
}

/// `POST /projects/{id}/versions`: record a working copy's files as a
/// version under its head (no run, no build) and move its head there.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointRequest {
    #[serde(flatten)]
    pub working_copy: WorkingCopyRef,
    #[serde(default)]
    pub label: Option<String>,
    /// Start a new tree: the version parents on nothing.
    #[serde(default)]
    pub root: bool,
}

/// What a checkpoint answers: the version the files are.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionUpsert {
    pub version: String,
    /// `false` when the project already had this exact code.
    pub created: bool,
    pub parent: Option<String>,
}

/// `POST /projects/{id}/versions/runs`: the run that records itself.
/// What it runs is a version the build that registered `definition_hash`
/// stamped; what it starts with is [`RunShape`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionRunRequest {
    /// The working copy the run is started from: its head's run is what
    /// `seed` inherits from, and its head moves to the run.
    #[serde(flatten)]
    pub working_copy: WorkingCopyRef,
    /// The version to run.
    pub version: String,
    /// The build the caller just had registered; the run refuses if the
    /// project's registered build moved since.
    pub definition_hash: String,
    pub binary_hash: String,
    /// Seed from head's run (`weft run --seed`).
    #[serde(default)]
    pub seed: bool,
    /// Seed from this run instead (`weft run --seed=<run id>`): any
    /// settled run of the project, whatever started it, of any version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed_run: Option<RunId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seed_until: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seed_before: Vec<String>,
    #[serde(flatten)]
    pub shape: RunShape,
}

/// What a run starts with: a saved example of the version it runs, edited
/// by `flags`, or the flags alone (none: the whole program). The install
/// reads the example from the version and resolves every `@file` and
/// `@asset` in it from the version's files, so the run starts on what the
/// version holds wherever it is asked from.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunShape {
    /// `examples/<name>.json` of the version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub example: Option<String>,
    #[serde(default, skip_serializing_if = "crate::run_flags::RunFlags::is_empty")]
    pub flags: crate::run_flags::RunFlags,
}

/// `PUT /projects/{id}/versions/runs/{run_id}`: change what a
/// recorded run remembers. A field left out is kept.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunUpdate {
    /// Absent: keep it. `null`: clear it. A name: the saved example
    /// this run was frozen as.
    #[serde(default, deserialize_with = "present", skip_serializing_if = "Option::is_none")]
    pub example: Option<Option<String>>,
}

/// A field the caller may leave alone, set, or clear.
///
/// `None` is "the key was absent, keep what is recorded"; `Some(None)`
/// is an explicit `null`, "clear it".
fn present<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(T::deserialize(d)?))
}

/// What `weft prune <version>` removes: the subtree's versions, every
/// run under them, and the blobs no surviving manifest names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrunePlan {
    pub versions: Vec<String>,
    pub runs: Vec<RunId>,
    /// `sha256` of every blob only the pruned versions held.
    pub blobs: Vec<String>,
}

/// `DELETE /projects/{id}/versions/{version}`: the plan, and whether it
/// was carried out (`false` under `?plan=true`). After a delete the plan
/// is what was actually removed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneResponse {
    pub plan: PrunePlan,
    pub deleted: bool,
}

/// `POST /projects/{id}/versions/sweep`: the bare versions it dropped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepResponse {
    pub swept: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_diff_names_every_kind_of_change() {
        let manifest = |entries: &[(&str, &str)]| -> Manifest {
            entries.iter().map(|(p, h)| (p.to_string(), h.to_string())).collect()
        };
        let parent = manifest(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let child = manifest(&[("a", "1"), ("b", "X"), ("d", "4")]);
        let diff = manifest_diff(&parent, &child);
        assert_eq!(diff.added, vec!["d"]);
        assert_eq!(diff.removed, vec!["c"]);
        assert_eq!(diff.changed, vec!["b"]);
    }

    #[test]
    fn a_run_update_tells_keep_from_clear_from_set() {
        let read = |v: serde_json::Value| serde_json::from_value::<RunUpdate>(v).unwrap().example;
        assert_eq!(read(serde_json::json!({})), None);
        assert_eq!(read(serde_json::json!({ "example": null })), Some(None));
        assert_eq!(read(serde_json::json!({ "example": "x" })), Some(Some("x".into())));
        let keep = serde_json::to_value(RunUpdate { example: None }).unwrap();
        assert_eq!(keep, serde_json::json!({}));
        let set = serde_json::to_value(RunUpdate { example: Some(Some("x".into())) }).unwrap();
        assert_eq!(set, serde_json::json!({ "example": "x" }));
    }

    #[test]
    fn a_started_run_and_the_head_name_their_fields_in_camel_case() {
        let started = RunStarted {
            run_id: RunId::nil(),
            version: "v1".into(),
            seed: None,
            inherited: vec![],
            ran: vec!["n".into()],
            warnings: vec![],
        };
        let wire = serde_json::to_value(&started).unwrap();
        assert_eq!(wire["runId"], "00000000-0000-0000-0000-000000000000");
        assert!(wire.get("run_id").is_none(), "{wire}");
        let tree = VersionTree { working_copies: vec![], activated_versions: vec!["v1".into()], versions: vec![], runs: vec![] };
        assert_eq!(
            serde_json::to_value(&tree).unwrap(),
            serde_json::json!({ "workingCopies": [], "activatedVersions": ["v1"], "versions": [], "runs": [] })
        );
    }

    #[test]
    fn a_run_request_round_trips_its_camel_case_keys() {
        let body = VersionRunRequest {
            working_copy: WorkingCopyRef { working_copy: Some(uuid::Uuid::nil()) },
            version: "v".into(),
            definition_hash: "d".into(),
            binary_hash: "b".into(),
            seed: true,
            seed_run: Some(RunId::nil()),
            seed_until: vec!["n".into()],
            seed_before: Vec::new(),
            shape: RunShape {
                example: Some("e".into()),
                flags: crate::run_flags::RunFlags { target: vec!["t".into()], ..Default::default() },
            },
        };
        let wire = serde_json::to_value(&body).unwrap();
        for key in ["workingCopy", "definitionHash", "binaryHash", "seedRun", "seedUntil", "example", "flags"] {
            assert!(wire.get(key).is_some(), "{key} in {wire}");
        }
        assert!(wire.get("seedBefore").is_none(), "an empty list is left out: {wire}");
        let back: VersionRunRequest = serde_json::from_value(wire).unwrap();
        assert_eq!(back, body);
        // A plain run of the whole program says nothing but what it runs.
        let plain = VersionRunRequest { version: "v".into(), definition_hash: "d".into(), binary_hash: "b".into(), ..Default::default() };
        assert_eq!(
            serde_json::to_value(&plain).unwrap(),
            serde_json::json!({ "version": "v", "definitionHash": "d", "binaryHash": "b", "seed": false })
        );
    }
}
