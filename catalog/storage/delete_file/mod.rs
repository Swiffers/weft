//! DeleteFile: delete a stored file now. The key carries its own scope,
//! so the handle's scope here is only the verb's home.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Node, NodeManifest, RunContext, WeftResult};

#[derive(NodeManifest)]
pub struct DeleteFileNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for DeleteFileNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let file: FileHandle = ctx.inputs.get("file")?;
        ctx.storage(StorageScope::Run).delete(&file).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
