//! Layer-4: the sensitive edges of the version tree, the ones where
//! getting it wrong costs a person their files, their money, or their
//! ability to run the project again.
//!
//! The happy paths live in `version_tree.rs` and `version_tree_grow.rs`.
//! What is here is the set that a unit test cannot honestly prove,
//! because the thing at risk is a real file on disk, a real row in the
//! tree, or two real sessions arriving at once:
//!
//!   - freezing over an example file that does not parse (it used to be
//!     read as "no file here" and overwritten);
//!   - a frozen example whose new run fails, with the old evidence
//!     preserved and the failure available for review;
//!   - build output inside a node, which used to be hashed into every
//!     version and deleted file by file on a branch back;
//!   - two checkouts of one project checkpointing at once, each its own
//!     working copy with its own head, where a lost update would silently
//!     drop one session's version;
//!   - a project id in a spelling the database does not store, which
//!     used to authorize and then match nothing, so a cancel answered
//!     success having done nothing.
#![cfg(feature = "e2e")]

use crate::common;

use common::{my_copy, run_id_of, fire_until_run, graph_trigger_hold, tree_of, version_ids, HOLD};
use serde_json::Value;
use weft_e2e::fakes::{PollFake, SseFake};
use weft_e2e::status::{self, STATUS_DEADLINE};
use weft_e2e::{ensure, project::Project, run, SettledRun};

/// An `examples/<name>.json` that exists but does not parse is a file
/// somebody wrote and mistyped. Freezing over it would destroy the only
/// copy of the scope and kicks they meant to keep.
#[tokio::test]
async fn an_unparseable_example_file_is_never_overwritten() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?.completed()?;

    // A spec with one character wrong: valid-looking, not valid JSON.
    let hand_written = "{ \"name\": \"chain\", \"target\": [\"out\"],, }";
    project.write_file("examples/chain.json", hand_written)?;

    let refused = project.weft_refused(&["freeze", "chain"]).await?;
    anyhow::ensure!(
        refused.contains("parse") && refused.contains("chain.json"),
        "the refusal has to name the file and say it does not parse: {refused}"
    );
    anyhow::ensure!(
        project.read_file("examples/chain.json")? == hand_written,
        "the file must be exactly as the person left it"
    );

    // And with the typo fixed, the same command works, so the refusal is
    // about the file rather than about freezing.
    project.write_file("examples/chain.json", "{ \"name\": \"chain\" }")?;
    project.weft(&["freeze", "chain"]).await?;
    let frozen: Value = serde_json::from_str(&project.read_file("examples/chain.json")?)?;
    anyhow::ensure!(frozen["expected"]["wires"].is_array(), "now it froze: {frozen}");

    project.finish().await
}

/// A failed run remains a failure; comparison does not invent a verdict
/// or replace accepted evidence.
#[tokio::test]
async fn a_frozen_example_with_a_failed_new_run_preserves_accepted_evidence() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;

    let stdout = project.weft(&["run", "--json", "--target", "out"]).await?;
    let run_id = run_id_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), run_id).await?.completed()?;
    project.weft(&["freeze", "chain", &run_id.to_string()]).await?;
    let accepted = project.read_file("examples/chain.json")?;

    // Now make the run itself fail: `src` is text, and a Cast to Number
    // fails loudly on a value that does not fit.
    let main = project.read_file("src/main.weft")?;
    project.set_main(&main.replace("mid = Cast -> (value: String)", "mid = Cast -> (value: Number)"))?;

    let stdout = project.weft(&["run", "chain", "--json"]).await?;
    let failed_run_id = run_id_of(&stdout)?;
    SettledRun::observe(project.dispatcher(), failed_run_id).await?
        .failed_with("cannot cast")?;
    let tree = tree_of(&project).await?;
    let failed = my_copy(&tree)?["headRun"].as_str().expect("failed run recorded");
    anyhow::ensure!(failed == failed_run_id.to_string(), "the failed run must be the recorded head run");
    let row = tree["runs"].as_array().unwrap().iter().find(|row| row["runId"] == failed).unwrap();
    anyhow::ensure!(row["listing"]["status"] == "failed", "run failure is retained: {row}");
    let diff: Value = serde_json::from_str(project.weft(&["diff", failed, "example:chain", "--json"]).await?.trim())?;
    anyhow::ensure!(!diff["differing"].as_array().unwrap().is_empty(), "failure outputs remain reviewable: {diff}");
    anyhow::ensure!(project.read_file("examples/chain.json")? == accepted, "a failed run never overwrites accepted evidence");

    project.finish().await
}

/// A node that carries its own package has build output beside it.
/// Covering it put thousands of files through the hash and the publish,
/// made a folder nobody edited read as changed, and deleted them one by
/// one on a branch back. A directory that is genuinely
/// somebody's source, even one called `target`, still counts: what
/// tells the two apart is a `Cargo.toml` sitting beside it.
#[tokio::test]
async fn build_output_inside_a_node_is_not_part_of_a_version() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;

    // Build output, and two folders that merely look like it. What
    // makes `nodes/pkg/target` cargo's is the `Cargo.toml` beside it;
    // `prompts/target` has none, so it is somebody's writing.
    project.write_file("nodes/pkg/node_modules/left-pad/index.js", "module.exports = 1;\n")?;
    project.write_file("nodes/pkg/Cargo.toml", "[package]\nname = \"pkg\"\n")?;
    project.write_file("nodes/pkg/target/debug/build.log", "stale build output\n")?;
    project.write_file("prompts/target/tone.txt", "a prompt folder called target\n")?;

    // The first gesture in this project is the checkpoint: nothing has
    // run, nothing is built, and a save point still has to work.
    let first = project.weft(&["checkpoint", "--json"]).await?;
    let first: Value = serde_json::from_str(first.trim())?;
    let version = first["version"].as_str().unwrap_or_default().to_string();
    anyhow::ensure!(!version.is_empty(), "checkpoint answers a version: {first}");

    let tree = tree_of(&project).await?;
    let versions: &[Value] = tree["versions"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    let manifest = versions
        .iter()
        .find(|v| v["id"] == serde_json::json!(version))
        .map(|v| v["manifest"].clone())
        .unwrap_or(Value::Null);
    let paths: Vec<String> = match manifest.as_object() {
        Some(m) => m.keys().cloned().collect(),
        None => Vec::new(),
    };
    anyhow::ensure!(
        !paths.iter().any(|p| p.contains("node_modules")),
        "a node's installed packages are not part of a version: {paths:?}"
    );
    anyhow::ensure!(
        !paths.iter().any(|p| p.contains("nodes/pkg/target/")),
        "cargo's output beside a node is not part of a version: {paths:?}"
    );
    anyhow::ensure!(
        paths.iter().any(|p| p == "prompts/target/tone.txt"),
        "a source folder called `target` IS part of a version: {paths:?}"
    );
    anyhow::ensure!(
        paths.iter().any(|p| p == "nodes/pkg/Cargo.toml"),
        "and the file that marks the crate is source too: {paths:?}"
    );

    // A folder nobody edited reads as unedited: build output must not
    // make the branch keep anything new as a version.
    let branched: Value = serde_json::from_str(project.weft(&["branch", &version, "--json"]).await?.trim())?;
    anyhow::ensure!(branched["saved"] == serde_json::json!(version), "nothing new to keep: {branched}");
    let tree = tree_of(&project).await?;
    anyhow::ensure!(version_ids(&tree) == vec![version.clone()], "the branch recorded no version: {tree}");
    anyhow::ensure!(my_copy(&tree)?["workingVersion"] == serde_json::json!(version), "the folder is still that version: {tree}");
    // And the branch back left the build output alone.
    anyhow::ensure!(
        project.has_file("nodes/pkg/node_modules/left-pad/index.js"),
        "a branch must not delete a node's installed packages"
    );
    anyhow::ensure!(project.has_file("prompts/target/tone.txt"), "nor its source");

    project.finish().await
}

/// Two sessions checkpointing at once, from two checkouts of one
/// project with DIFFERENT edits.
///
/// Each checkout is a working copy of its own, with its own head, so the
/// two never fight over one head: both versions land, each becomes the
/// head of the checkout that recorded it, and every version parents on
/// one the tree holds. A lost update would show as a checkpoint that
/// answered a version the tree does not hold, or as a head left behind.
/// Running both from one directory would not test this: they would agree
/// on the same content and so on the same version.
#[tokio::test]
async fn two_sessions_checkpointing_at_once_leave_one_coherent_tree() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;
    // The version both sessions start from. It is also what registers
    // the project and the first folder: nothing has run here.
    project.weft(&["checkpoint", "--json"]).await?;

    // A second checkout of the SAME project, and a different edit in
    // each, so the two checkpoints are two versions rather than one.
    let other = project.second_checkout_with_catalog().await?;
    project.write_file("prompts/greeting.txt", "from the first session\n")?;
    std::fs::write(other.join("prompts/greeting.txt"), "from the second session\n")?;

    let (a, b) = tokio::join!(
        weft_e2e::client::cli(project.dispatcher(), project.dir(), &["checkpoint", "first", "--json"]),
        weft_e2e::client::cli(project.dispatcher(), &other, &["checkpoint", "second", "--json"]),
    );
    let (a, b) = (a?, b?);

    anyhow::ensure!(
        a.success && b.success,
        "both checkpoints must land:\nfirst: {} {}\nsecond: {} {}",
        a.stdout, a.stderr, b.stdout, b.stderr
    );

    // Every version parents on one the tree holds: no lineage dangles.
    let tree = tree_of(&project).await?;
    let ids = version_ids(&tree);
    for v in tree["versions"].as_array().map(Vec::as_slice).unwrap_or(&[]) {
        if let Some(parent) = v["parentId"].as_str() {
            anyhow::ensure!(
                ids.contains(&parent.to_string()),
                "version {} parents on {parent}, which the tree does not hold: {ids:?}",
                v["id"]
            );
        }
    }
    // Each version is actually recorded, not just answered, and each is
    // the head of the checkout that recorded it.
    let mut landed = Vec::new();
    for out in [&a, &b] {
        let answer: Value = serde_json::from_str(out.stdout.trim())
            .map_err(|e| anyhow::anyhow!("a checkpoint answers JSON ({e}): {}", out.stdout))?;
        let version = answer["version"].as_str().ok_or_else(|| anyhow::anyhow!("a checkpoint names its version: {answer}"))?;
        anyhow::ensure!(answer["created"] == serde_json::json!(true), "two different edits are two new versions: {answer}");
        landed.push(version.to_string());
    }
    anyhow::ensure!(landed[0] != landed[1], "two different edits are two versions: {landed:?}");
    for version in &landed {
        anyhow::ensure!(ids.contains(version), "a checkpoint that answered {version} recorded it: {ids:?}");
    }
    let first_head = my_copy(&tree)?["headVersion"].clone();
    anyhow::ensure!(first_head == serde_json::json!(landed[0]), "the first checkout's head is its version: {tree}");
    let other_tree: Value = serde_json::from_str(
        weft_e2e::client::cli_ok(project.dispatcher(), &other, &["tree", "--json"]).await?.trim(),
    )?;
    let second_head = my_copy(&other_tree)?["headVersion"].clone();
    anyhow::ensure!(second_head == serde_json::json!(landed[1]), "the second checkout's head is its version: {other_tree}");
    anyhow::ensure!(
        other_tree["workingCopies"].as_array().map(Vec::len) == Some(2),
        "two checkouts are two working copies of the project: {other_tree}"
    );

    project.finish().await
}

/// A project id is a uuid, and a uuid has several legal spellings while
/// the database stores exactly one. A verb that authorized on the
/// parsed id and then queried on the caller's own text matched nothing
/// and answered success, so a cancel reported that it had cancelled and
/// nothing had been cancelled.
///
/// A deactivate with `running: cancel` cancels the runs its triggers
/// fired that are going, so the test fires the trigger, holds the run
/// open, and takes the trigger down with the id spelled the way a
/// hand-written script or an id copied out of a UI often is.
#[tokio::test]
async fn a_cancel_on_an_uppercase_project_id_really_cancels() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("lifecycle", disp.clone()).await?;
    let gate = PollFake::start(HOLD).await?;
    let feed = SseFake::start().await?;
    project.add_node_from_fixture("human_form", "test_sse_trigger")?;
    project.set_main(&graph_trigger_hold(&feed.url(), "go", &gate.url()))?;
    project.activate().await?;
    let pid = project.id();
    let before = run::runs(&disp, &pid).await?;
    let run_id = fire_until_run(&feed, &disp, &pid, "go", &before).await?;
    run::wait_for_status(&disp, run_id, "running").await?;

    let shouted = pid.to_string().to_uppercase();
    let (code, body) = disp
        .post_raw(&format!("/projects/{shouted}/deactivate"), &serde_json::json!({ "running": "cancel" }))
        .await?;
    anyhow::ensure!(code.is_success(), "deactivate answered HTTP {code}: {body}");
    let answer: weft_core::deactivation::DeactivateResponse = serde_json::from_str(&body)?;
    anyhow::ensure!(answer.report.runs == 1, "the deactivate reached the run going: {body}");

    // The claim it just made has to be true: it used to answer success
    // having matched no rows at all.
    let settled = SettledRun::observe(&disp, run_id).await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "deactivate answered that it cancelled a run, so the run must actually be cancelled; got {}",
        settled.status
    );
    status::wait_until_status(&disp, &pid, "inactive", STATUS_DEADLINE).await?;

    project.finish().await
}
