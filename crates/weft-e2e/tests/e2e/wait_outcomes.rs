//! `weft wait` and `weft run` say how a run ended in their exit code: one
//! code per outcome, the same code from both for a run that failed.
//!
//! The codes are `weft_cli`'s `commands::wait::Outcome::code`; the CLI is a
//! binary, so they are written out here.
#![cfg(feature = "e2e")]

use weft_e2e::{client::cli, ensure, project::Project, run};

const COMPLETED: i32 = 0;
const FAILED_OR_CANCELLED: i32 = 10;
const WAITING_FOR_INPUT: i32 = 11;
const TIMED_OUT: i32 = 12;

/// `weft wait <run>` in `project`, answering its exit code and its output.
async fn wait(project: &Project, args: &[&str]) -> anyhow::Result<(i32, String)> {
    let mut all = vec!["wait"];
    all.extend_from_slice(args);
    let out = cli(project.dispatcher(), project.dir(), &all).await?;
    let code = out.status.ok_or_else(|| anyhow::anyhow!("`{}` was killed by a signal", out.invocation))?;
    Ok((code, format!("{}{}", out.stdout, out.stderr)))
}

/// A completed run exits 0, and waiting again on the run that already
/// ended answers at once from its history.
#[tokio::test]
async fn wait_exits_zero_for_a_completed_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("plain", disp).await?;
    let run_id = run::start(&mut project).await?.to_string();

    let (code, out) = wait(&project, &[&run_id]).await?;
    anyhow::ensure!(code == COMPLETED, "exit {code}: {out}");
    anyhow::ensure!(out.contains("completed"), "{out}");

    let (code, out) = wait(&project, &[&run_id, "--timeout", "5s"]).await?;
    anyhow::ensure!(code == COMPLETED, "a run that already ended is answered at once; exit {code}: {out}");

    project.finish().await
}

#[tokio::test]
async fn wait_exits_non_zero_for_a_failed_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("run_fails", disp).await?;
    let run_id = run::start(&mut project).await?.to_string();

    let (code, out) = wait(&project, &[&run_id]).await?;
    anyhow::ensure!(code == FAILED_OR_CANCELLED, "exit {code}: {out}");
    anyhow::ensure!(out.contains("failed") && out.contains("this run fails on purpose"), "{out}");

    project.finish().await
}

/// A run parked on a timer: `--waiting-for-input` returns at the pause, a
/// short timeout returns with the run still going, and once the run is
/// stopped the wait answers that it was cancelled.
#[tokio::test]
async fn wait_returns_at_a_pause_only_when_asked() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("wait_format", disp).await?;
    project.set_node_config("hold", "seconds", "600")?;
    let run_id = run::start(&mut project).await?.to_string();

    let (code, out) = wait(&project, &[&run_id, "--waiting-for-input"]).await?;
    anyhow::ensure!(code == WAITING_FOR_INPUT, "exit {code}: {out}");

    let (code, out) = wait(&project, &[&run_id, "--timeout", "3s"]).await?;
    anyhow::ensure!(code == TIMED_OUT, "without the flag a paused run is still going; exit {code}: {out}");
    anyhow::ensure!(out.contains(&format!("weft wait {run_id}")), "the timeout says how to keep waiting: {out}");

    project.weft(&["stop", &run_id]).await?;
    let (code, out) = wait(&project, &[&run_id]).await?;
    anyhow::ensure!(code == FAILED_OR_CANCELLED, "exit {code}: {out}");
    anyhow::ensure!(out.contains("cancelled"), "{out}");

    project.finish().await
}

/// `weft run` follows its run to the end and exits with the code `weft
/// wait` gives the same ending.
#[tokio::test]
async fn run_exits_non_zero_when_its_run_fails() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let project = Project::prepare("run_fails", disp).await?;

    let out = cli(project.dispatcher(), project.dir(), &["run"]).await?;
    anyhow::ensure!(
        out.status == Some(FAILED_OR_CANCELLED),
        "exit {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        out.status,
        out.stdout,
        out.stderr
    );

    project.finish().await
}
