//! AwaitJob: wait for an outside job to finish without holding a worker.
//!
//! Parks on a `PollEndpoint` over the job's status address. The listener
//! polls it, the first poll at once, and the first answer whose `status`
//! reads COMPLETED or FAILED resumes the run as the value `await_signal`
//! returns. The node emits the job's `result`, or fails with its `error`.
//! With a `cancelUrl`, a cancel of the run while it waits sends the job a
//! `PUT` there.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::signal::{CancelCall, PollEndpoint, Predicate};
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct AwaitJobNode;

#[async_trait]
impl Node for AwaitJobNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let url: String = ctx.inputs.get("url")?;
        let poll = PollEndpoint {
            url,
            interval_secs: 5,
            filters: vec![Predicate::regex("status", "^(COMPLETED|FAILED)$")],
            ..Default::default()
        };
        let answer = match ctx.inputs.opt::<String>("cancelUrl")? {
            Some(cancel) => ctx.await_signal_with_cancel(poll, CancelCall::put(cancel)).await?,
            None => ctx.await_signal(poll).await?,
        };
        if answer["status"] == "FAILED" {
            return Err(weft::node_error(format!("the job failed: {}", answer["error"])));
        }
        let result = answer["result"]
            .as_str()
            .ok_or_else(|| weft::node_error(format!("the finished job carries no result: {answer}")))?
            .to_string();
        ctx.pulse_downstream(NodeOutput::new().set("result", result)).await
    }
}
