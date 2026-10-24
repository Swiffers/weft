//! `weft wake <run_id> <node>`: resolve a pure time wait now. Refused
//! for any wait expecting a value, naming the kind.

use super::Ctx;

pub async fn run(ctx: Ctx, run_id: String, node: String, iteration: Option<String>) -> anyhow::Result<()> {
    let run_id = super::resolve_run_id(&ctx, &run_id).await?;
    // The node is named the way the program spells it (`sweep.key`, the
    // whole path from the entry file), and the dispatcher reads it against
    // the program the run ran, which is not always the one in this folder
    // any more. A node waiting in several loop iterations takes the one
    // `--iteration` names.
    let query = iteration.as_deref().map(|it| format!("?iteration={}", super::runs::query_escaped(it))).unwrap_or_default();
    ctx.client()?.post_empty(&format!("/runs/{run_id}/wake/{}{query}", super::runs::query_escaped(&node))).await?;
    if ctx.json_out(&serde_json::json!({ "runId": run_id, "node": node, "iteration": iteration }))? {
        return Ok(());
    }
    println!("woke {node} in {}", super::versions::short(&run_id));
    Ok(())
}
