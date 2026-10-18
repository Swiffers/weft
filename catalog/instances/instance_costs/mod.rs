//! InstanceCosts: what one instance's runs cost, narrowed by the optional
//! filters, with the measured total and how many calls it is missing.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::program::PaidBy;
use weft::{node_bail, RunContext, RunId, Node, NodeManifest, WeftError, WeftResult};

use super::lifecycle::instance;

#[derive(NodeManifest)]
pub struct InstanceCostsNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for InstanceCostsNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let mut query = ctx.costs().instance(instance(&ctx)?);
        // Never skipped when blank: a run id that did not arrive must not
        // widen the read to every run of the instance.
        if let Some(given) = ctx.inputs.opt::<String>("run")? {
            let run: RunId = given.trim().parse().map_err(|_| {
                WeftError::Input(format!("run takes a run id (the one GetRunId gives), so it cannot be '{given}'"))
            })?;
            query = query.run(run);
        }
        if let Some(service) = ctx.inputs.opt::<String>("service")?.filter(|s| !s.trim().is_empty()) {
            query = query.service(service);
        }
        if let Some(node) = ctx.inputs.opt::<String>("node")?.filter(|s| !s.trim().is_empty()) {
            query = query.node(node);
        }
        // Read typed: a word outside the enum fails naming the input, the
        // word, and the words it takes.
        if let Some(paid_by) = ctx.inputs.opt::<PaidBy>("paidBy")? {
            query = query.paid_by(paid_by);
        }
        if let Some(since) = ctx.inputs.opt::<f64>("since")? {
            if !(since >= 0.0) {
                node_bail!("since is a moment in unix seconds, so it cannot be {since}");
            }
            query = query.since(since as u64);
        }
        let records = query.list().await?;
        // Folded from 0.0: `Sum` for f64 starts at -0.0, so an instance
        // with no records would read a total of -0.
        let total: f64 = records.iter().filter_map(|r| r.amount_usd).fold(0.0, |sum, amount| sum + amount);
        let unpriced = records.iter().filter(|r| r.amount_usd.is_none()).count();
        ctx.pulse_downstream(
            NodeOutput::new()
                .set_serialized("records", &records)?
                .set("totalUsd", total)
                .set("unpricedCalls", unpriced),
        )
        .await
    }
}
