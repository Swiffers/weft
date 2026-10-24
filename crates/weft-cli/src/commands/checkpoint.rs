//! `weft checkpoint [<label>]`: record this folder's files as a version
//! under its working copy's head, with no run and no build, and move the
//! head there. The gesture outside any run: a point to branch back to.

use anyhow::Context;
use weft_core::versions::{CheckpointRequest, VersionUpsert};

use super::Ctx;

pub async fn run(ctx: Ctx, label: Option<String>, root: bool) -> anyhow::Result<()> {
    // A checkpoint is a save point, so it works as the first thing a
    // person ever does in a project: syncing declares the project and
    // registers the folder. Source only, no image.
    let here = super::copy::this_copy(&ctx, None).await?;
    let body = CheckpointRequest { working_copy: here.named(), label: label.clone(), root };
    let resp = here
        .client
        .post_json(&format!("/projects/{}/versions", here.project_id), &serde_json::to_value(&body)?)
        .await
        .context("checkpoint")?;
    let upsert: VersionUpsert = serde_json::from_value(resp.clone()).context("read the checkpoint answer")?;
    if ctx.json_out(&resp)? {
        return Ok(());
    }
    let short = super::versions::short(&upsert.version);
    match (upsert.created, &label) {
        (true, Some(l)) => println!("checkpoint {short} ({l})"),
        (true, None) => println!("checkpoint {short}"),
        (false, Some(l)) => println!("already at {short}; labelled {l}"),
        (false, None) => println!("already at {short}"),
    }
    Ok(())
}
