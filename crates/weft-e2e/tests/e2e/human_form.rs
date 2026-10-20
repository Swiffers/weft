//! Human-in-the-loop: an event starts a flow on an active project; a mid-flow
//! HumanQuery suspends; the rig answers it over HTTP (no browser) and asserts
//! the run resumed with the approval.
#![cfg(feature = "e2e")]

use std::time::Duration;

use serde_json::json;
use weft_e2e::{ensure, fakes::SseFake, human, project::Project, run, SettledRun};

use crate::common;
use common::{inherited, run_id_of};

#[tokio::test]
async fn human_query_resumes_with_approval() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();

    // Stand up the fake feed that the entry trigger subscribes to, then activate
    // (a HumanQuery resume only dispatches on an active project; activation also
    // brings up the listener that routes the form submission).
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;

    // Snapshot existing runs (activation creates a TriggerSetup run) so we
    // can identify the genuine Fire run the event creates.
    let before = run::runs(&disp, &pid).await?;

    // Push an event to START a fresh run; it runs to HumanQuery and
    // suspends. (Brief settle so the listener's SSE connect is live first.)
    tokio::time::sleep(Duration::from_secs(2)).await;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;

    // Play the human: wait for the query form, approve it.
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;

    // The suspended run resumes and completes; approval flows to Debug as true.
    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_input("out", "data", &json!(true))?;

    project.finish().await
}

/// A person skipping the question: the HumanQuery ends skipped (an
/// approve/reject field included), its outputs close, what reads them
/// skips, and the run still completes.
#[tokio::test]
async fn a_skipped_question_skips_its_node_and_the_run_completes() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;
    let before = run::runs(&disp, &pid).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let run_id =
        run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;

    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    human::skip_form(&disp, &review).await?;

    let settled = SettledRun::observe(&disp, run_id).await?;
    settled.completed()?;
    settled.assert_skip_reason("review", "wait_skipped")?;
    settled.assert_skipped("out")?;

    project.finish().await
}

/// A run seeded from a run an event started, re-running the question and
/// what follows it: the question's code is what it was, so it takes the
/// answer the person gave there instead of asking again, and the run
/// completes with nobody answering.
#[tokio::test]
async fn a_seeded_run_reuses_the_answer_a_person_gave() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("human_form", disp.clone()).await?;
    let pid = project.id();
    let feed = SseFake::start().await?;
    project.substitute_in_main("__E2E_FAKE_URL__", &feed.url())?;
    project.activate().await?;
    let before = run::runs(&disp, &pid).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    feed.push_event("go", &json!({ "value": "the change" }).to_string());
    let answered = run::wait_for_triggered_run(&disp, &pid, &before, Duration::from_secs(60)).await?;
    let review = human::wait_for_form_by_node(&disp, &pid, "review").await?;
    human::answer_form(&disp, &review, &json!({ "decision": "approve" })).await?;
    SettledRun::observe(&disp, answered).await?.completed()?;

    let answered = answered.to_string();
    let seed = format!("--seed={answered}");
    let stdout = project.weft(&["run", "--json", &seed, "--seed-before", "review"]).await?;
    let seeded = SettledRun::observe_within(&disp, run_id_of(&stdout)?, Duration::from_secs(120)).await?;
    seeded.completed()?;
    anyhow::ensure!(inherited(&seeded, "start"), "the event's value is reused");
    anyhow::ensure!(!inherited(&seeded, "review"), "the question runs again");
    seeded.assert_input("out", "data", &json!(true))?;

    project.finish().await
}
