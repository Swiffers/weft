//! A folder on the person's machine as a trigger (`FolderWatch`), and the
//! worker it fires as a process on that machine: the run opens the file
//! the fire names, in the project's folder.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, project::Project, run, SettledRun};

/// A file created in the watched folder fires one run, which reads the
/// file at the path the fire carries and runs in the project's own folder.
#[tokio::test]
async fn a_file_in_a_watched_folder_fires_a_run_that_reads_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("folder_watch", disp.clone()).await?;
    let folder = tempfile::tempdir()?;
    let folder_path = folder.path().canonicalize()?;
    project.substitute_in_main("/__E2E_FOLDER__", folder_path.to_str().expect("a temp folder's path is UTF-8"))?;
    project.activate().await?;
    let pid = project.id();

    let before = run::runs(&disp, &pid).await?;
    std::fs::write(folder_path.join("note.txt"), "hello from the machine")?;
    let run_id = run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_input("text", "data", &json!("hello from the machine"))?;
    let cwd = settled.input_of("cwd").and_then(|i| i.get("data").and_then(|d| d.as_str().map(str::to_string)));
    let cwd = cwd.ok_or_else(|| anyhow::anyhow!("the run said no folder: {:?}", settled.input_of("cwd")))?;
    anyhow::ensure!(
        std::path::Path::new(&cwd).canonicalize()? == project.dir().canonicalize()?,
        "the worker runs in the project's folder {}, not {cwd}",
        project.dir().display()
    );

    project.finish().await
}
