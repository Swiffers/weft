//! A seeded run never hands a node that runs again a saved output naming a
//! file that has changed since. Every stored file value carries the
//! version of the content it names, so the saved result of a node that
//! made a file says which write it handed out; once a later node (or
//! another run) edits the file in place, sending that result on would hand
//! over a file that no longer holds what the node made. A reused result
//! that nothing running reads hands nothing over, so it is no reason to
//! refuse.
//!
//! Shape: the `seed_moved_file` fixture starts a conversation file and
//! adds a message to it in a second node. The first run completes. A
//! seeded run that reuses both nodes runs nothing and goes ahead. Once the
//! second node's code changes it runs again, reading the first node's
//! saved file, and the seeded run is refused before it starts, naming the
//! node, the port, the file and both ways out.
#![cfg(feature = "e2e")]

use crate::common;

use common::{inherited, run_id_of};

use weft_e2e::{ensure, project::Project, run::SettledRun};

#[tokio::test]
async fn a_seed_whose_file_moved_on_is_refused() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("seed_moved_file", disp).await?;

    let stdout = project.weft(&["run", "--json"]).await?;
    let first = SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?;
    first.completed()?;

    // Both reused, nothing runs: the stale file reaches nothing.
    let stdout = project.weft(&["run", "--json", "--seed"]).await?;
    let reused = SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?;
    reused.completed()?;
    anyhow::ensure!(inherited(&reused, "start") && inherited(&reused, "more"), "a run of unchanged code reuses both nodes");

    // `more` changed, so it runs again and would read `start`'s saved
    // file, which `more` moved on to version 2 in the first run.
    let main = std::fs::read_to_string(project.dir().join("src/main.weft"))?;
    anyhow::ensure!(main.contains(r#"text: "two""#), "the fixture's `more` says \"two\":\n{main}");
    project.write_file("src/main.weft", &main.replace(r#"text: "two""#, r#"text: "two, edited""#))?;
    let refused = project.weft_refused(&["run", "--json", "--seed"]).await?;
    for part in [
        "--seed would reuse what 'start' produced on 'historyFile'",
        "'conversation.json', has changed since",
        "version 1",
        "version 2",
        "--emit start=",
        "--seed-before start",
    ] {
        anyhow::ensure!(refused.contains(part), "the refusal names {part:?}:\n{refused}");
    }

    project.finish().await
}
