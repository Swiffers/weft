//! WriteCodeAndRun: a project-local custom node that edits another
//! project's code and runs it, through `ctx.project(id).code()`: the file
//! lands in that project's working copy (what it replaces kept beside it,
//! by its hash, with nothing read or stored again), and the run builds
//! that code first, the way `weft run` does.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::{Node, NodeManifest, RunContext, WeftError, WeftResult};

#[derive(NodeManifest)]
pub struct WriteCodeAndRunNode;

#[async_trait]
impl Node for WriteCodeAndRunNode {
    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let project: String = ctx.inputs.get("project")?;
        let path: String = ctx.inputs.get("path")?;
        let text: String = ctx.inputs.get("text")?;
        let kept_as: String = ctx.inputs.get("keptAs")?;
        let id = uuid::Uuid::parse_str(&project)
            .map_err(|e| WeftError::Input(format!("'{project}' is not a project id: {e}")))?;
        let code = ctx.project(id).code();
        let state = code.state().await?;
        let before = state.working.get(&path).ok_or_else(|| WeftError::Input(format!("the project holds no {path}")))?;
        match code.edit().text(path, text).blob(kept_as, before)?.apply().await? {
            weft::working_copy::Applied::Applied { .. } => {}
            weft::working_copy::Applied::Stale { paths } => return Err(WeftError::Input(format!("changed underneath: {paths:?}"))),
        }
        let started = ctx.project(id).code().run().target("out").start().await?;
        ctx.pulse_downstream(NodeOutput::new().set("runId", started.run_id.to_string())).await
    }
}
