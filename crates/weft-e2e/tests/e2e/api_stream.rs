//! Streaming with zero custom Rust: a bus behind a route, framed as
//! server-sent events and as ndjson, read whole by the rig. Plus the
//! lifetime of a streaming run: a caller who hangs up on a feed that has
//! gone quiet still ends it. And a producer failing: before its first
//! piece unhandled (a plain failure naming the run), before it with a
//! backup branch on `_error`, and halfway (the stream's error marker).
#![cfg(feature = "e2e")]

use reqwest::Method;
use weft_e2e::run::SettledRun;
use weft_e2e::{ensure, live, project::Project};

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

/// The run an answer names in its `Weft-Run-Id` header, which every
/// answer of a route's run carries.
fn run_of(headers: &reqwest::header::HeaderMap) -> anyhow::Result<uuid::Uuid> {
    let value = header(headers, weft_core::caller::RUN_ID_HEADER)
        .ok_or_else(|| anyhow::anyhow!("the answer names no run: {headers:?}"))?;
    Ok(value.parse()?)
}

/// The run `run_id` once it settled. A route's run reaches its record a
/// moment after its answer; the wait covers that.
async fn settled(disp: &weft_e2e::client::Dispatcher, run_id: uuid::Uuid) -> anyhow::Result<SettledRun> {
    SettledRun::observe_within(disp, run_id, std::time::Duration::from_secs(120)).await
}

#[tokio::test]
async fn a_bus_streams_as_sse_and_ndjson() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/sse"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "content-type"), Some("text/event-stream"));
    assert_eq!(String::from_utf8_lossy(&body), "data: t1\n\ndata: t2\n\ndata: t3\n\n");
    let streamed = run_of(&headers)?;
    settled(&disp, streamed).await?.completed()?;

    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/lines"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "content-type"), Some("application/x-ndjson"));
    assert_eq!(header(&headers, "x-run"), Some("e2e"), "the Stream's own headers ride the head");
    assert_eq!(String::from_utf8_lossy(&body), "\"t1\"\n\"t2\"\n\"t3\"\n");

    project.finish().await
}

/// A feed that says one thing and then goes quiet, and a caller who hangs
/// up. The run behind the route is cancelled without the program writing
/// another byte: the worker watches the connection itself, and the
/// keepalive the framing ignores is the second way it finds out. Before
/// this, only a real write could discover the caller had left, so a
/// watched table nobody touched held its run open for ever, one per
/// connection.
#[tokio::test]
async fn a_caller_leaving_a_quiet_stream_ends_its_run() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = weft_e2e::run::runs(&disp, &project.id()).await?;

    let first = live::stream_first_chunk_then_hang_up(&disp, &format!("{base}/quiet")).await?;
    assert_eq!(String::from_utf8_lossy(&first), "data: first\n\n");

    // The caller is gone and the program has nothing more to say. The run
    // must end anyway, and end as the caller's doing.
    let run_ids = weft_e2e::run::wait_for_triggered_runs(
        &disp,
        &project.id(),
        &before,
        1,
        std::time::Duration::from_secs(60),
    )
    .await?;
    let settled = weft_e2e::run::SettledRun::observe_within(
        &disp,
        run_ids[0],
        std::time::Duration::from_secs(120),
    )
    .await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "the run should end with its caller, got {} ({:?})",
        settled.status,
        settled.cancel_reason()
    );

    project.finish().await
}

/// A caller that is THERE but has stopped reading: the socket stays
/// open and its machine keeps acknowledging, it just never consumes.
///
/// Deliberately NOT asserting the run ends. This is the case the
/// socket-level bound cannot see and should not: the far side is
/// answering, so by every measure available it is alive, and cutting it
/// would be cutting a slow reader. What this pins is that the exchange
/// survives it rather than erroring, and the run ends the moment the
/// caller actually goes.
///
/// The case that DOES matter, a caller whose machine stops answering,
/// cannot be produced from a healthy local socket: the kernel on this
/// side keeps acknowledging whatever we do in user space. Proving it
/// needs the packets dropped underneath us (a firewall rule, a severed
/// network), which is a harness this rig does not have. That gap is
/// real and worth closing; it is not something this test can fake.
#[tokio::test]
async fn a_caller_that_stops_reading_does_not_break_the_exchange() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = weft_e2e::run::runs(&disp, &project.id()).await?;

    {
        // Held, then dropped: while held the caller is present and
        // silent, and dropping it is the caller finally leaving.
        let _reading_nothing =
            live::stream_first_chunk_then_stop_reading(&disp, &format!("{base}/quiet")).await?;
    }

    let run_ids = weft_e2e::run::wait_for_triggered_runs(
        &disp,
        &project.id(),
        &before,
        1,
        std::time::Duration::from_secs(60),
    )
    .await?;
    let settled = weft_e2e::run::SettledRun::observe_within(
        &disp,
        run_ids[0],
        std::time::Duration::from_secs(120),
    )
    .await?;
    anyhow::ensure!(
        settled.status == "cancelled",
        "the run ends once the caller really goes, got {} ({:?})",
        settled.status,
        settled.cancel_reason()
    );

    project.finish().await
}

/// Many callers arriving and abandoning the same feed in a row, which is
/// what a person refreshing a page does. The count that matters is not
/// whether ONE run ended, it is whether the standing population comes
/// back to nothing: a leak of one per connection is invisible in a
/// single-run test and obvious here.
#[tokio::test]
async fn abandoned_streams_do_not_pile_up() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let before = weft_e2e::run::runs(&disp, &project.id()).await?;

    const CALLERS: usize = 4;
    for _ in 0..CALLERS {
        let first =
            live::stream_first_chunk_then_hang_up(&disp, &format!("{base}/quiet")).await?;
        anyhow::ensure!(!first.is_empty(), "each caller got its first event");
    }

    let run_ids = weft_e2e::run::wait_for_triggered_runs(
        &disp,
        &project.id(),
        &before,
        CALLERS,
        std::time::Duration::from_secs(120),
    )
    .await?;
    for run_id in run_ids {
        let settled = weft_e2e::run::SettledRun::observe_within(
            &disp,
            run_id,
            std::time::Duration::from_secs(120),
        )
        .await?;
        anyhow::ensure!(
            settled.status == "cancelled",
            "every abandoned stream must end, {run_id} is {} ({:?})",
            settled.status,
            settled.cancel_reason()
        );
    }

    project.finish().await
}

/// A producer that fails before its first piece, with nothing handling it:
/// the run fails without answering, and its caller is told only that it
/// failed and which run it was, as JSON under a `500`. The whole message
/// stays on the run, where its owner reads it.
#[tokio::test]
async fn a_run_that_fails_unanswered_tells_its_caller_plainly() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/refused"), &[], None).await?;
    assert_eq!(status, 500, "{}", String::from_utf8_lossy(&body));
    assert_eq!(header(&headers, "content-type"), Some("application/json"));
    let run_id = run_of(&headers)?;
    let told: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(told, serde_json::json!({ "error": "Something went wrong", "runId": run_id.to_string() }));
    settled(&disp, run_id).await?.failed_with("the stand-in model went away after 0 tick(s)")?;

    project.finish().await
}

/// The same failure with a backup branch gated on the producer's
/// `_error`: no bus opened, so the stream never starts, and the backup
/// answers the caller. The run completes.
#[tokio::test]
async fn a_producer_failing_before_its_first_piece_is_answered_by_a_backup() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/backup"), &[], None).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&body)?, serde_json::json!("the backup answered"));
    let run = settled(&disp, run_of(&headers)?).await?;
    run.completed()?;
    run.assert_completed("fallback")?;

    project.finish().await
}

/// A producer that breaks off after two pieces: the caller gets what
/// went out, then the format's error marker (an SSE `error` event with
/// the plain failure and the run's id), never a short stream passed off
/// as whole.
#[tokio::test]
async fn a_stream_that_breaks_off_ends_with_its_error_marker() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("api_stream", disp.clone()).await?;
    let base = project.unique_live_path()?;
    project.activate().await?;
    let (status, headers, body) =
        live::http_request(&disp, Method::GET, &format!("{base}/broken"), &[], None).await?;
    assert_eq!(status, 200, "the head went out with the first piece");
    assert_eq!(header(&headers, "content-type"), Some("text/event-stream"));
    let run_id = run_of(&headers)?;
    assert_eq!(
        String::from_utf8_lossy(&body),
        format!(
            "data: t1\n\ndata: t2\n\n\nevent: error\ndata: {{\"error\":\"Something went wrong\",\"runId\":\"{run_id}\"}}\n\n"
        )
    );
    settled(&disp, run_id).await?.failed_with("the stand-in model went away after 2 tick(s)")?;

    project.finish().await
}
