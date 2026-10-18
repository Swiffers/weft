use super::Ctx;

pub async fn run(ctx: Ctx, run_id: String) -> anyhow::Result<()> {
    let run_id = super::resolve_run_id(&ctx, &run_id).await?;
    ctx.client()?.post_empty(&format!("/runs/{run_id}/cancel")).await?;
    if ctx.json_out(&serde_json::json!({ "cancelled": run_id }))? {
        return Ok(());
    }
    println!("cancelled {run_id}");
    Ok(())
}
