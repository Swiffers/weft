//! ElevenLabsCloneVoice: mint a durable voice on the account from
//! sample recordings (instant voice clone). The minted voice_id is an
//! ACCOUNT ASSET: it outlives this execution and works in every speak
//! node from then on.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::{json_call, Multipart};
use weft::node::NodeOutput;
use weft::storage::{FileHandle, StorageScope};
use weft::{Access, ExecutionContext, Node, NodeErrExt, NodeManifest, WeftResult};

use super::elevenlabs::API;

#[derive(NodeManifest)]
pub struct ElevenLabsCloneVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsCloneVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let name: String = ctx.inputs.get("name")?;
        let samples: Vec<FileHandle> = ctx.inputs.list("samples")?;
        let description: Option<String> = ctx.inputs.opt("description")?;
        let remove_noise: bool = ctx.inputs.get("removeBackgroundNoise")?;
        let labels: Option<BTreeMap<String, String>> = ctx.inputs.opt("labels")?;

        if samples.is_empty() {
            return Err(weft::WeftError::Input(
                "cloning a voice needs at least one sample recording".into(),
            ));
        }

        let storage = ctx.storage(StorageScope::Execution);
        let mut form = Multipart::form_data().text("name", &name);
        for sample in &samples {
            let (meta, bytes) = storage.get_bytes(sample).await?;
            form = form.file("files", &meta.filename, &meta.mime_type, bytes);
        }
        if let Some(d) = description.as_deref().filter(|d| !d.trim().is_empty()) {
            form = form.text("description", d);
        }
        if remove_noise {
            form = form.text("remove_background_noise", "true");
        }
        // A form field carries text, so the labels go as their JSON.
        if let Some(labels) = labels.filter(|l| !l.is_empty()) {
            form = form.text("labels", &json!(labels).to_string());
        }
        let (content_type, body) = form.build();

        let http = ctx.client(&account).await?;
        let answer = json_call(
            http.post(format!("{API}/voices/add"))
                .header("content-type", content_type)
                .body(body),
            "clone the voice on ElevenLabs",
        )
        .await?;
        let voice_id = answer["voice_id"]
            .as_str()
            .node_err("ElevenLabs cloned the voice but its answer has no voice_id")?;
        // The voice exists on the account once ElevenLabs answers, so a
        // flag it left out closes its port rather than failing a run
        // whose rerun would mint a second voice.
        let mut out = NodeOutput::new().set("voiceId", voice_id);
        if let Some(verify) = answer["requires_verification"].as_bool() {
            out = out.set("requiresVerification", verify);
        }
        ctx.pulse_downstream(out).await
    }
}
