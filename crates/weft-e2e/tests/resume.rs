//! Platform: a worker crashes mid-run and the job resumes on a FRESH
//! worker. This is the core local-reliability guarantee (a single machine's
//! worker dies; parked work must survive).
//!
//! Shape: reuse the `human_form` fixture (an event starts a flow that suspends
//! at a HumanQuery). While suspended, KILL the worker container from the host (a
//! fake crash). Then answer the form: the resume task is NOT pinned to the dead
//! worker,
//! so a worker (fresh, re-seeded from the journal) picks it up and completes.
//!
//! Proof is by INFERENCE, and airtight: we kill the ONLY live worker while the
//! run is parked at the form (before we send the answer, so it provably
//! had not finished), then assert it still completed with the right approval. A
//! dead worker cannot finish a job, so a fresh one must have resumed it. We do
//! NOT additionally fingerprint the new worker: a respawn reuses the worker's
//! deterministic container name, so no marker tells the two apart.
//!
//! Why this fixture: only a resumable park (a signal/form) survives a worker
//! death; a node holding a live-caller connection is terminally
//! cancelled when its worker dies (the external connection died with it), so it is
//! the wrong shape for a resume test. A HumanQuery suspension is unpinned and
//! resumes anywhere.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, human, platform::Platform, project::Project, run, SettledRun};

#[tokio::test]
async fn worker_crash_resumes_on_fresh_worker() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    // Bring up the entry feed + activate (a HumanQuery resume only dispatches on
    // an active project; activation also brings up the form-routing listener).
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Start a fresh run that runs to HumanQuery and suspends.
    let before = run::runs(&disp, &pid).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;

    // Wait until the run is genuinely parked at the form (the worker has
    // registered the suspension), THEN kill its worker to fake a crash. We kill
    // every worker container of the project; an empty kill set would mean the
    // worker already idle-exited (also a valid resume path), but we assert at
    // least one was live so this test exercises the abrupt-crash path on
    // purpose, not by accident.
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    let killed = platform.kill_workers(&pid).await?;
    assert!(
        !killed.is_empty(),
        "expected a live worker to kill while the run was suspended at the form; \
         killed none (the worker idle-exited before we caught it). Widen the window."
    );

    // Answer the form. The resume task is not pinned to the dead worker, so a
    // worker (fresh, re-seeded from the journal) picks it up and finishes.
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;

    // The proof of resume-across-a-crash is by inference, and it is airtight: we
    // killed the ONLY live worker while the run was parked at the form
    // (before we sent the answer, so it provably had not finished), yet the
    // run still completed with the correct approval. A dead worker cannot
    // finish a job; a fresh one re-seeded from the journal must have. We do NOT
    // additionally fingerprint the new worker replica: a respawn reuses the
    // worker's deterministic name and the dead row is GC'd within seconds, so
    // any "a different replica did it" marker is timing-flaky (see platform.rs).
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(true))?;

    project.finish().await
}

/// A run paused on a build whose worker can no longer start ends the
/// first time it is handed to a worker, never retried, and says how to
/// pick it back up: `weft resync` for the triggers, `weft run --seed <run
/// id>` for the run.
///
/// Shape: the run pauses at the form on build A, the project moves on to
/// build B, and A's image is deleted from the machine the way a cleanup
/// outside weft deletes it (weft's own `weft clean --images` keeps the
/// image of every run that has not ended). Answering the form hands the
/// run to a worker of A, which cannot start.
#[tokio::test]
async fn a_paused_run_whose_build_cannot_start_ends_once_naming_the_way_back() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let platform = Platform::connect(&disp).await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    // Each build of this test is its own (a node's code is part of its
    // image's hash), so the image deleted below is no other test's.
    let node = project.dir().join("nodes/test_sse_trigger/mod.rs");
    let original = std::fs::read_to_string(&node)?;
    std::fs::write(&node, format!("{original}\n// build A {}\n", uuid::Uuid::new_v4()))?;

    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;
    let before = run::runs(&disp, &pid).await?;
    feed.wait_for_subscriber(Duration::from_secs(60)).await?;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let run_id = run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;

    // A's image, read off the worker running it.
    let hash = platform.run_binary_hash(&run_id).await?;
    let images = platform.worker_images(&pid).await?;
    let old = images
        .iter()
        .find(|image| image.ends_with(&hash))
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no live worker runs the run's build {hash} (workers run {images:?})"))?;

    // The project moves on to build B; the paused run stays on A.
    std::fs::write(&node, format!("{original}\n// build B {}\n", uuid::Uuid::new_v4()))?;
    project.build().await?;
    anyhow::ensure!(platform.run_binary_hash(&run_id).await? == hash, "a build moves no paused run");

    platform.kill_workers(&pid).await?;
    platform.remove_image(&old).await?;

    // Answering hands the run to a worker of A. It cannot start, so the
    // run ends at once: a retry would leave it going until this times out.
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;
    let settled = SettledRun::observe_within(&disp, run_id, Duration::from_secs(120)).await?;
    anyhow::ensure!(settled.status == "cancelled", "the run ends, it is {}", settled.status);
    let reason = settled.cancel_reason().unwrap_or_default();
    anyhow::ensure!(
        reason.contains("`weft resync`") && reason.contains(&format!("`weft run --seed {run_id}`")),
        "the ending names the way back: {reason}"
    );
    anyhow::ensure!(settled.input_of("out").is_none(), "nothing of the run went on past the form");

    project.finish().await
}
