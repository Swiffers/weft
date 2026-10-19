//! Layer-4: a node package that does not compile is left out of the
//! worker, and only what uses it is refused. The `left_out_package`
//! fixture's `Shout` node (nodes/shout/) has code that does not compile:
//! the build still makes a worker, the trigger that never reaches Shout
//! activates and answers, and the one that does is refused, naming the
//! package and its first error.
#![cfg(feature = "e2e")]

use reqwest::Method;
use serde_json::{json, Value};
use weft_e2e::{client, ensure, live, project::Project};

#[tokio::test]
async fn a_package_that_does_not_compile_stops_only_the_trigger_that_uses_it() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let mut project = Project::prepare("left_out_package", disp.clone()).await?;
    let base = project.unique_live_path()?;

    let out = client::cli(&disp, project.dir(), &["activate", "--trigger", "front"]).await?;
    anyhow::ensure!(out.success, "activating front failed:\n{}\n{}", out.stdout, out.stderr);
    anyhow::ensure!(
        out.stderr.contains("package 'shout' didn't compile"),
        "the left-out package is named as a warning:\n{}",
        out.stderr
    );
    let (status, _, body) =
        live::http_json(&disp, Method::POST, &format!("{base}/front"), &[], &json!({ "text": "hi" })).await?;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(serde_json::from_slice::<Value>(&body)?, json!({ "heard": "hi" }));

    let refused = project.weft_refused(&["activate", "--trigger", "back"]).await?;
    anyhow::ensure!(
        refused.contains("package 'shout' didn't compile") && refused.contains("mismatched types"),
        "the refusal names the package and its first error:\n{refused}"
    );
    project.finish().await
}
