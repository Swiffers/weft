//! A seeded run never reuses a node whose saved output names a file that
//! has changed since. Every stored file value carries the version of the
//! content it names, so the saved result of a node that made a file says
//! which write it handed out; once a later node (or another run) edits the
//! file in place, reusing that result would hand on a file that no longer
//! holds what the node made.
//!
//! Shape: the `seed_moved_file` fixture starts a conversation file and
//! adds a message to it in a second node. The first run completes; a
//! seeded run, which would reuse both nodes, is refused before it starts,
//! naming the node, the port, the file and both ways out.
#![cfg(feature = "e2e")]

use crate::common;

use common::run_id_of;

use weft_e2e::{ensure, project::Project, run::SettledRun};

#[tokio::test]
async fn a_seed_whose_file_moved_on_is_refused() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("seed_moved_file", disp).await?;

    let stdout = project.weft(&["run", "--json"]).await?;
    let first = SettledRun::observe(project.dispatcher(), run_id_of(&stdout)?).await?;
    first.completed()?;

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
