//! Routes whose runs cannot pause: a wait holds in the worker while the
//! caller is on the line (or the run is unrecorded), its answer carries the
//! run on, and a wait that outlasts the route's `holdSecs` is given up,
//! failing the call (its reason goes on the run, never to the caller).
#![cfg(feature = "e2e")]

use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{client::poll_until, ensure, live, project::Project};

#[tokio::test]
async fn a_route_holds_its_wait_and_gives_up_after_its_hold() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("route_hold", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let pid = project.id();

    // Tied to its caller, and unrecorded: the timer answers the wait in
    // the worker, and the caller gets the reply after it.
    for path in ["tied", "unrecorded"] {
        let started = Instant::now();
        let (status, _, body) = live::http_request(&disp, Method::GET, &format!("{base}/{path}"), &[], None).await?;
        assert_eq!(status, 200, "{path}: {}", String::from_utf8_lossy(&body));
        assert_eq!(serde_json::from_slice::<Value>(&body)?, json!({ "state": "waited" }), "{path}");
        assert!(started.elapsed() >= Duration::from_secs(1), "{path}: the reply came after the wait");
    }

    // A thirty second timer under a one second hold: given up, a time
    // limit (504), long before the timer. The caller is told only that
    // the run failed and which run it was; why is on the run.
    let started = Instant::now();
    let (status, _, body) = live::http_request(&disp, Method::GET, &format!("{base}/impatient"), &[], None).await?;
    let said = String::from_utf8_lossy(&body).to_string();
    assert_eq!(status.as_u16(), 504, "{said}");
    assert!(started.elapsed() < Duration::from_secs(20), "given up after its hold, not the timer");
    let told: Value = serde_json::from_slice(&body)?;
    let run_id = told["runId"].as_str().unwrap_or_default().to_string();
    assert_eq!(told, json!({ "error": "Something went wrong", "runId": run_id }), "{said}");

    // Its run lists failed, with the reason on it for its owner. Its
    // ending is written after the caller heard, so wait for the row to
    // leave `running`.
    let failed = poll_until("the given-up run to end", Duration::from_secs(60), Duration::from_millis(250), || {
        let disp = disp.clone();
        let run_id = run_id.clone();
        async move {
            let page: Value = disp.get_json(&format!("/runs?project_id={pid}&phase=fire&limit=50")).await?;
            Ok(page["runs"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .find(|row| row["runId"] == run_id.as_str() && row["status"] != "running"))
        }
    })
    .await?;
    assert_eq!(failed["status"], "failed", "{failed}");
    assert_eq!(failed["entryNode"], "impatient", "{failed}");
    assert!(
        failed["error"].as_str().is_some_and(|e| e.contains("gave up its wait") && e.contains("holdSecs")),
        "the reason is on the run: {failed}"
    );

    project.finish().await
}
