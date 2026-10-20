//! A node that started an outside job waits for it without holding a
//! worker: it parks on a poll of the job's status address, the listener
//! polls it on the node's behalf, and the run resumes with the first answer
//! whose status says done.
//!
//! The job fake says "still running" twice, then "done". So the run
//! completing with the job's result proves the polls that did not match were
//! held back, the matching one resumed the run, and its response is what
//! `await_signal` returned. The read count staying put afterwards proves the
//! polling stopped once the run had its answer, and the job hearing no
//! cancel proves the last call goes only when the run is cancelled.
//!
//! A run cancelled while it waits tells the job once, at the job's own
//! cancel address, and its polls stop.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::JobFake, project::Project, run};

/// The await_poll project, pointed at `job`'s status and cancel addresses.
async fn waiting_on(job: &JobFake) -> anyhow::Result<Project> {
    let disp = ensure::up().await?;
    let project = Project::prepare("await_poll", disp).await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &job.url())?;
    project.substitute_in_main("__E2E_CANCEL_URL__", &job.cancel_url())?;
    Ok(project)
}

#[tokio::test]
async fn a_node_waiting_on_a_job_resumes_when_the_job_is_done() -> anyhow::Result<()> {
    let job = JobFake::start(2, "https://cdn.example/out.mp4").await?;
    let mut project = waiting_on(&job).await?;

    let settled = run::run_and_settle(&mut project).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!("https://cdn.example/out.mp4"))?;
    let reads = job.reads();
    anyhow::ensure!(reads >= 3, "the job was read {reads} times; two \"still running\" answers and the done one make three");

    // Longer than the 5 second poll interval: no poll follows the answer.
    tokio::time::sleep(Duration::from_secs(7)).await;
    anyhow::ensure!(job.reads() == reads, "the status was read again after the run had its answer ({reads} then {})", job.reads());
    anyhow::ensure!(job.cancels() == 0, "a job whose wait was answered was told to cancel");

    project.finish().await
}

#[tokio::test]
async fn a_run_cancelled_while_it_waits_on_a_job_tells_the_job_once() -> anyhow::Result<()> {
    // Never done: only the cancel ends the wait.
    let job = JobFake::start(usize::MAX, "never").await?;
    let mut project = waiting_on(&job).await?;
    let run_id = run::start(&mut project).await?;
    let polled = || async { Ok((job.reads() >= 1).then_some(())) };
    weft_e2e::poll_until("the job is polled", Duration::from_secs(60), Duration::from_millis(200), polled).await?;

    project.weft(&["stop", &run_id.to_string()]).await?;
    run::wait_for_status(project.dispatcher(), run_id, "cancelled").await?;
    let told = || async { Ok((job.cancels() >= 1).then_some(())) };
    weft_e2e::poll_until("the job hears the cancel", Duration::from_secs(60), Duration::from_millis(200), told).await?;

    // Longer than the 5 second poll interval: no poll and no second cancel
    // follow the one the cancel sent.
    let reads = job.reads();
    tokio::time::sleep(Duration::from_secs(7)).await;
    anyhow::ensure!(job.cancels() == 1, "the job was told to cancel {} times", job.cancels());
    anyhow::ensure!(job.reads() == reads, "the status was read again after the cancel ({reads} then {})", job.reads());

    project.finish().await
}
