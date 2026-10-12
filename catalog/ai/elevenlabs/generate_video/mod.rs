//! ElevenLabsGenerateVideo: a prompt, and optionally the images it
//! opens and ends on, to one video through the Image & Video API.

use async_trait::async_trait;

use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::{Access, ExecutionContext, Node, NodeManifest, WeftResult};

use super::flows::{generate, video_request, Media, References};

#[derive(NodeManifest)]
pub struct ElevenLabsGenerateVideoNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsGenerateVideoNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let start: Option<FileHandle> = ctx.inputs.opt("startFrame")?;
        let end: Option<FileHandle> = ctx.inputs.opt("endFrame")?;
        let mut body = video_request(&ctx)?;

        let http = ctx.client(&account).await?;
        let refs = References::new(&ctx, &http);
        let video = generate(&ctx, &account, &http, &refs, Media::Video, || async {
            if let Some(start) = &start {
                body["start_frame"] = refs.of(start).await?;
            }
            if let Some(end) = &end {
                body["end_frame"] = refs.of(end).await?;
            }
            Ok(body)
        })
        .await?;
        ctx.pulse_downstream(NodeOutput::new().set("video", video)).await
    }
}
