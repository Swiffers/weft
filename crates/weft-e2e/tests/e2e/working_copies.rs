//! Layer-4: a folder is a working copy the install keeps in sync, and a
//! program edits a project's code through the same working copies.
//!
//! Three contracts, each one a real folder on disk and a real install:
//!
//!   - a program writes a file into ANOTHER project's code with
//!     `ctx.project(id).code()`, builds and runs it, and the run shows
//!     what it wrote; the person's folder of that project gets the file
//!     too, written by the runtime's sync;
//!   - a save in a registered folder reaches its working copy with no
//!     weft command run (read over the dispatcher's route, which syncs
//!     nothing, so it is the runtime's own sync that landed it), and
//!     `weft run` then runs that code with nothing left to send;
//!   - `weft branch <older version>` keeps the folder's unrecorded work
//!     as a version and prints the command that comes back, and that
//!     command brings the files back exactly;
//!   - a file changed both in the folder and in its working copy keeps
//!     the folder's and writes the other beside it, and `weft resolve`
//!     settles it either way;
//!   - a folder that is gone keeps its head's versions from a prune
//!     until `weft branch --copy` moves it or `weft forget-copy` removes
//!     it, keeping its files as a version.
#![cfg(feature = "e2e")]

use std::time::Duration;

use crate::common;

use common::{my_copy, run_id_of, tree_of, version_ids, working_copies};
use serde_json::{json, Value};
use uuid::Uuid;
use weft_core::working_copy::WorkingCopy;
use weft_e2e::{ensure, project::Project, run, SettledRun};

/// How long the runtime's sync gets to carry a change between a folder
/// and its working copy: it passes about a second after the folder goes
/// quiet, or at once when the install says the working copy moved.
const SYNC_DEADLINE: Duration = Duration::from_secs(30);

/// The working copy `id` of `project`, as the install holds it now.
async fn copy_of(project: &Project, id: Uuid) -> anyhow::Result<WorkingCopy> {
    working_copies(project.dispatcher(), &project.id())
        .await?
        .into_iter()
        .find(|copy| copy.id == id)
        .ok_or_else(|| anyhow::anyhow!("project {} has no working copy {id}", project.id()))
}

/// The id of the folder's own working copy, registering the folder if
/// nothing has yet (reading the tree syncs, and the first sync registers).
async fn folder_copy_id(project: &Project) -> anyhow::Result<Uuid> {
    let tree = tree_of(project).await?;
    Ok(tree["workingCopy"].as_str().unwrap_or_default().parse()?)
}

#[tokio::test]
async fn a_program_writes_another_projects_code_and_runs_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let target = Project::prepare("version_tree", disp.clone()).await?;
    // The person's folder of the target is its working copy: the
    // program's write lands there, and the runtime writes it to disk.
    target.weft(&["checkpoint", "--json"]).await?;
    let folder = folder_copy_id(&target).await?;

    let mut writer = Project::prepare("program_edits_code", disp.clone()).await?;
    writer.substitute_in_main("__E2E_TOKEN__", &target.id().to_string())?;
    let started = run::start(&mut writer).await?;
    // The writer's node waits for the target's build before it starts
    // the run, so it gets longer than a plain run.
    let settled = SettledRun::observe_within(&disp, started, Duration::from_secs(240)).await?;
    settled.completed()?;
    let target_run: Uuid = settled
        .output_of("write")
        .and_then(|out| out["runId"].as_str().map(str::to_string))
        .ok_or_else(|| anyhow::anyhow!("the writer handed on no run id: {:?}", settled.output_of("write")))?
        .parse()?;

    // The run is of the code the program wrote.
    let written = "written by another project's program";
    SettledRun::observe(&disp, target_run).await?.completed()?.assert_input("out", "data", &json!(written))?;

    // It ran on the person's folder copy, whose head is now that run.
    let copy = copy_of(&target, folder).await?;
    anyhow::ensure!(copy.head.head_run == Some(target_run), "the run moved the folder copy's head: {copy:?}");
    // And the runtime wrote the file into the folder, with no command run
    // there, beside the file it replaced, which the program copied by its
    // hash.
    weft_e2e::poll_until("the program's write to reach the target's folder", SYNC_DEADLINE, Duration::from_millis(250), || async {
        Ok((target.read_file("prompts/greeting.txt").unwrap_or_default() == written).then_some(()))
    })
    .await?;
    let before = target.read_file("prompts/before.txt")?;
    anyhow::ensure!(before == "hello", "the replaced file is kept, byte for byte: {before:?}");

    writer.finish().await?;
    target.finish().await
}

#[tokio::test]
async fn a_save_in_the_folder_reaches_its_working_copy_and_weft_run_runs_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;
    // Registers the project and the folder.
    project.weft(&["checkpoint", "--json"]).await?;
    let folder = folder_copy_id(&project).await?;
    let before = copy_of(&project, folder).await?;

    // A save, as an editor makes it. Nothing below runs a command in the
    // folder until `weft run`: the route the copy is read through syncs
    // nothing, so what it shows is the runtime's own doing.
    let saved = "saved in an editor";
    project.write_file("prompts/greeting.txt", saved)?;
    let landed = weft_e2e::poll_until("the runtime's sync to land the save", SYNC_DEADLINE, Duration::from_millis(250), || async {
        let copy = copy_of(&project, folder).await?;
        Ok((copy.working_version != before.working_version).then_some(copy))
    })
    .await?;
    anyhow::ensure!(landed.is_changed(), "the save is not a version until something stamps it: {landed:?}");
    anyhow::ensure!(landed.revision > before.revision, "the save moved the working copy: {before:?} then {landed:?}");

    // `weft run` runs exactly that code, and had nothing to send: a push
    // of its own would have moved the working copy's revision.
    let run_id = run_id_of(&project.weft(&["run", "--json", "--target", "out"]).await?)?;
    SettledRun::observe(project.dispatcher(), run_id).await?.completed()?.assert_input("out", "data", &json!(saved))?;
    let after = copy_of(&project, folder).await?;
    anyhow::ensure!(after.revision == landed.revision, "the run found the save already in the install: {landed:?} then {after:?}");
    anyhow::ensure!(
        after.head.head_version.as_deref() == Some(landed.working_version.as_str()) && after.head.head_run == Some(run_id),
        "the run stamped the saved files and is head's run: {after:?}"
    );

    project.finish().await
}

#[tokio::test]
async fn a_branch_keeps_the_work_as_a_version_and_its_come_back_line_restores_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;
    let base: Value = serde_json::from_str(project.weft(&["checkpoint", "base", "--json"]).await?.trim())?;
    let v1 = base["version"].as_str().unwrap_or_default().to_string();

    // Work nobody recorded: an edit and a new file.
    let edited = "work in progress";
    let added = "a file only the work has";
    project.write_file("prompts/greeting.txt", edited)?;
    project.write_file("prompts/notes.txt", added)?;

    // Branching away keeps it, and says how to come back.
    let said = project.weft(&["branch", "base"]).await?;
    anyhow::ensure!(said.contains(&format!("this folder is now at version {}", &v1[..8])), "{said}");
    let saved_short = said
        .split("your files as they were are version ")
        .nth(1)
        .and_then(|rest| rest.split(';').next())
        .map(str::trim)
        .ok_or_else(|| anyhow::anyhow!("the branch printed no way back: {said}"))?
        .to_string();
    anyhow::ensure!(said.contains(&format!("branch {saved_short}` comes back to them")), "the way back is a command: {said}");
    anyhow::ensure!(project.read_file("prompts/greeting.txt")? == "hello", "the base's file is back on disk");
    anyhow::ensure!(!project.has_file("prompts/notes.txt"), "a file the base never held is gone");

    let tree = tree_of(&project).await?;
    let saved = version_ids(&tree)
        .into_iter()
        .find(|id| id.starts_with(&saved_short))
        .ok_or_else(|| anyhow::anyhow!("the kept work {saved_short} is a version of the tree: {tree}"))?;
    let row = tree["versions"].as_array().unwrap().iter().find(|v| v["id"] == json!(saved)).cloned().unwrap_or_default();
    anyhow::ensure!(row["parentId"] == json!(v1), "the work parents on where it was made: {row}");
    anyhow::ensure!(
        row["diff"]["changed"] == json!(["prompts/greeting.txt"]) && row["diff"]["added"] == json!(["prompts/notes.txt"]),
        "the kept version names the work: {row}"
    );
    let copy = my_copy(&tree)?;
    anyhow::ensure!(copy["headVersion"] == json!(v1) && copy["workingVersion"] == json!(v1), "the folder is the base: {tree}");

    // The line it printed comes back, byte for byte, and leaving the
    // unchanged base records nothing new.
    let back: Value = serde_json::from_str(project.weft(&["branch", &saved_short, "--json"]).await?.trim())?;
    anyhow::ensure!(back["saved"] == json!(v1) && back["copy"]["headVersion"] == json!(saved), "{back}");
    anyhow::ensure!(project.read_file("prompts/greeting.txt")? == edited, "the edit is back exactly");
    anyhow::ensure!(project.read_file("prompts/notes.txt")? == added, "the new file is back exactly");
    let tree = tree_of(&project).await?;
    anyhow::ensure!(version_ids(&tree).len() == 2, "the base and the work, nothing else: {tree}");
    anyhow::ensure!(my_copy(&tree)?["workingVersion"] == json!(saved), "the folder is the work again: {tree}");

    project.finish().await
}

#[tokio::test]
async fn a_gone_folder_is_moved_or_forgotten_so_a_prune_goes() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp.clone()).await?;
    let base: Value = serde_json::from_str(project.weft(&["checkpoint", "base", "--json"]).await?.trim())?;
    let v1 = base["version"].as_str().unwrap_or_default().to_string();
    project.write_file("prompts/greeting.txt", "louder")?;
    project.weft(&["checkpoint", "loud", "--json"]).await?;
    let mine = folder_copy_id(&project).await?;

    // A second folder of the project registers on the newest head, then
    // goes away with the head still on `loud`.
    let second = project.second_checkout()?;
    let out = weft_e2e::client::cli(&disp, &second, &["tree"]).await?;
    anyhow::ensure!(out.success, "the second folder registers: {}", out.stderr);
    let copies = working_copies(&disp, &project.id()).await?;
    let other = copies.iter().find(|c| c.id != mine).ok_or_else(|| anyhow::anyhow!("no second working copy: {copies:?}"))?.clone();
    let path = other.path.clone().unwrap_or_default();
    std::fs::remove_dir_all(&second)?;
    project.weft(&["branch", "base"]).await?;

    // The prune is refused naming that folder and both ways out.
    let refused = project.weft_refused(&["prune", "loud", "--yes"]).await?;
    anyhow::ensure!(
        refused.contains(&format!("--copy {path}")) && refused.contains(&format!("weft forget-copy {path}")),
        "{refused}"
    );

    // Moving it away from here lets the prune go.
    let moved: Value = serde_json::from_str(project.weft(&["branch", "base", "--copy", &path, "--json"]).await?.trim())?;
    anyhow::ensure!(moved["copy"]["id"] == json!(other.id) && moved["copy"]["headVersion"] == json!(v1), "{moved}");
    anyhow::ensure!(project.read_file("prompts/greeting.txt")? == "hello", "this folder stayed where it was");
    let pruned: Value = serde_json::from_str(project.weft(&["prune", "loud", "--yes", "--json"]).await?.trim())?;
    anyhow::ensure!(pruned["deleted"] == json!(true), "{pruned}");

    // Forgetting it keeps its files as a version and removes it.
    let forgot: Value = serde_json::from_str(project.weft(&["forget-copy", &path, "--json"]).await?.trim())?;
    anyhow::ensure!(forgot["saved"] == json!(v1), "its files were the base: {forgot}");
    let left = working_copies(&disp, &project.id()).await?;
    anyhow::ensure!(left.iter().all(|c| c.id != other.id), "the copy is gone: {left:?}");
    let here = left.iter().find(|c| c.id == mine).and_then(|c| c.path.clone()).unwrap_or_default();
    let refused = project.weft_refused(&["forget-copy", &here]).await?;
    anyhow::ensure!(refused.contains("this folder's working copy"), "{refused}");

    project.finish().await
}

/// The working copy `id` with its files, as the install holds it now.
async fn state_of(project: &Project, id: Uuid) -> anyhow::Result<weft_core::working_copy::WorkingState> {
    project.dispatcher().get_json(&format!("/projects/{}/working-copies/state?workingCopy={id}", project.id())).await
}

/// Change `path` in the working copy `id` to the stored `hash`, as a
/// program's write would, while the folder holds something else of its
/// own: the next sync finds both sides moved.
async fn change_in_the_install(project: &Project, id: Uuid, path: &str, hash: &str, read: &str) -> anyhow::Result<()> {
    let body = json!({ "workingCopy": id, "changes": [{ "path": path, "hash": hash, "expected": { "expect": "hash", "hash": read } }] });
    let applied: Value = project.dispatcher().post_json(&format!("/projects/{}/working-copies/changes", project.id()), &body).await?;
    // The runtime's sync waits for the folder to go quiet, so the save is
    // not up yet and the change still finds what it read.
    anyhow::ensure!(applied["outcome"] == json!("applied"), "the install took the change before the save went up: {applied}");
    Ok(())
}

#[tokio::test]
async fn a_conflict_keeps_both_files_and_resolve_settles_it_either_way() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("version_tree", disp).await?;
    const FILE: &str = "prompts/greeting.txt";
    project.weft(&["checkpoint", "base", "--json"]).await?;
    let folder = folder_copy_id(&project).await?;
    let hello = state_of(&project, folder).await?.working[FILE].clone();
    project.write_file(FILE, "louder")?;
    project.weft(&["checkpoint", "loud", "--json"]).await?;
    let louder = state_of(&project, folder).await?.working[FILE].clone();

    // Both sides move: a save here, and the install's copy back to "hello".
    project.write_file(FILE, "mine")?;
    change_in_the_install(&project, folder, FILE, &hello, &louder).await?;
    let conflicted = weft_e2e::poll_until("the sync to find the conflict", SYNC_DEADLINE, Duration::from_millis(250), || async {
        let copy = copy_of(&project, folder).await?;
        Ok((!copy.conflicts.is_empty()).then_some(copy))
    })
    .await?;
    let beside = conflicted.conflicts[0].clone();
    anyhow::ensure!(project.read_file(FILE)? == "mine", "the folder's file is kept");
    anyhow::ensure!(project.read_file(&beside)? == "hello", "the install's is beside it as {beside}");

    // Taking the install's side puts it in place and the copy goes.
    let resolved: Value = serde_json::from_str(project.weft(&["resolve", FILE, "--keep", "incoming", "--json"]).await?.trim())?;
    anyhow::ensure!(resolved["resolved"] == json!([FILE]), "{resolved}");
    anyhow::ensure!(project.read_file(FILE)? == "hello" && !project.has_file(&beside), "the install's file is in place");
    let state = state_of(&project, folder).await?;
    anyhow::ensure!(state.working[FILE] == hello && state.copy.conflicts.is_empty(), "the working copy agrees: {state:?}");

    // And keeping the folder's drops the copy and sends the folder's up.
    project.write_file(FILE, "mine again")?;
    change_in_the_install(&project, folder, FILE, &louder, &hello).await?;
    let conflicted = weft_e2e::poll_until("the second conflict", SYNC_DEADLINE, Duration::from_millis(250), || async {
        let copy = copy_of(&project, folder).await?;
        Ok((!copy.conflicts.is_empty()).then_some(copy))
    })
    .await?;
    let beside = conflicted.conflicts[0].clone();
    project.weft(&["resolve", "--keep", "folder"]).await?;
    anyhow::ensure!(project.read_file(FILE)? == "mine again" && !project.has_file(&beside), "the folder's file stays, alone");
    let state = state_of(&project, folder).await?;
    anyhow::ensure!(state.working[FILE] != louder && state.copy.conflicts.is_empty(), "the folder's file went up: {state:?}");

    project.finish().await
}
