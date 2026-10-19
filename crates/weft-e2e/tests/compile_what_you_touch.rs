//! Layer-4: each command cares only about what it touches. The
//! `compile_what_you_touch` fixture has two routes, `back` answering
//! through `lib/stage.weft`. With that file broken, `front` still
//! activates and answers (the broken part printed as a warning) and `back`
//! is refused naming the file; and a file rewritten over and over while a
//! resync runs does not fail it: the resync builds the files as it first
//! read them, and names the one that moved.
#![cfg(feature = "e2e")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{client, ensure, live, project::Project};

#[tokio::test]
async fn a_broken_include_stops_only_the_trigger_that_needs_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("compile_what_you_touch", disp.clone()).await?;
    let base = project.unique_live_path()?;
    // Somebody else's half-written edit: the group never closes.
    project.write_file("src/lib/stage.weft", "Group(text: String) -> (body: JsonDict) {\n  shape = ExecPython(\n")?;

    let out = client::cli(&disp, project.dir(), &["activate", "--trigger", "front"]).await?;
    anyhow::ensure!(out.success, "activating front failed:\n{}\n{}", out.stdout, out.stderr);
    anyhow::ensure!(
        out.stderr.contains("does not compile") && out.stderr.contains("stage.weft"),
        "the broken file is named as a warning:\n{}",
        out.stderr
    );
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/front"), &[], &json!({ "text": "hi" })).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(serde_json::from_slice::<Value>(&body)?, json!({ "heard": "hi" }));

    let refused = project.weft_refused(&["activate", "--trigger", "back"]).await?;
    anyhow::ensure!(
        refused.contains("needs parts of the program that do not compile") && refused.contains("stage.weft"),
        "the refusal is the cut's, naming the file `back` needs:\n{refused}"
    );
    project.finish().await
}

#[tokio::test]
async fn a_file_edited_during_a_resync_is_named_and_does_not_fail_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("compile_what_you_touch", disp.clone()).await?;
    project.unique_live_path()?;
    project.activate().await?;

    // A prompt specialist rewriting the prompt the whole time the resync
    // runs: every write after the resync read its files is one its build
    // does not hold.
    let note = project.dir().join("prompts/note.md");
    let stop = Arc::new(AtomicBool::new(false));
    let writer = std::thread::spawn({
        let stop = stop.clone();
        move || -> std::io::Result<()> {
            let mut edit = 0u64;
            while !stop.load(Ordering::Relaxed) {
                edit += 1;
                std::fs::write(&note, format!("Be brief, take {edit}.\n"))?;
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Ok(())
        }
    });
    let out = client::cli(&disp, project.dir(), &["resync"]).await;
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("the writer thread")?;
    let out = out?;
    anyhow::ensure!(out.success, "the resync failed:\n{}\n{}", out.stdout, out.stderr);
    anyhow::ensure!(
        out.stderr.contains("changed after that") && out.stderr.contains("prompts/note.md"),
        "the moved file is named:\n{}",
        out.stderr
    );
    project.finish().await
}
