//! GoogleDriveListFiles: one authenticated GET; the store refreshed
//! the token lazily if it was near expiry (rotating any single-use
//! refresh token) before the client was handed over.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct GoogleDriveListFilesNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for GoogleDriveListFilesNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let access: Access = ctx.inputs.get("account")?;
        let query: String = ctx.inputs.get_or("query", String::new())?;
        let page_size = super::api::page_size(&ctx.inputs, "pageSize")?;
        let page_token: String = ctx.inputs.get_or("pageToken", String::new())?;

        let drive = ctx.client(&access).await?;
        // One Drive page per call. Drive may answer fewer than
        // `pageSize` even when more match; `hasMore` says so either way.
        let mut base = format!(
            "{}/files?pageSize={page_size}&fields=nextPageToken,files(id,name,mimeType)",
            super::drive::API,
        );
        if !query.trim().is_empty() {
            base.push_str(&format!("&q={}", urlencoding::encode(query.trim())));
        }
        let page =
            super::api::list_page(&drive, &base, "files", &page_token, Value::clone, "list the files", "pageSize")
                .await?;
        ctx.pulse_downstream(
            NodeOutput::new()
                .set("files", Value::Array(page.items))
                .set("hasMore", page.next_page_token.is_some())
                .set("nextPageToken", page.next_page_token.unwrap_or_default()),
        )
        .await
    }
}
