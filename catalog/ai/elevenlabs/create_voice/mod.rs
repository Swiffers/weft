//! ElevenLabsCreateVoice: mint a designed preview (from Design Voice)
//! into a durable voice on the account. The minted voice_id is an
//! account asset usable in every speak node from then on.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde_json::json;

use weft::access::client::post_json;
use weft::node::NodeOutput;
use weft::{Access, RunContext, Node, NodeErrExt, NodeManifest, WeftError, WeftResult};

use super::elevenlabs::API;
use super::flows::set_some;

#[derive(NodeManifest)]
pub struct ElevenLabsCreateVoiceNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ElevenLabsCreateVoiceNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let name: String = ctx.inputs.get("name")?;
        let description: String = ctx.inputs.get("description")?;
        let preview_id: String = ctx.inputs.get("previewId")?;
        let labels: Option<BTreeMap<String, String>> = ctx.inputs.opt("labels")?;

        if let Some(refusal) = description_refusal(&description) {
            return Err(WeftError::Input(refusal));
        }

        let mut body = json!({
            "voice_name": name,
            "voice_description": description,
            "generated_voice_id": preview_id,
        });
        set_some(&mut body, "labels", labels.filter(|l| !l.is_empty()));

        let http = ctx.client(&account).await?;
        let answer = post_json(&http, &format!("{API}/text-to-voice"), &body, "create the voice on ElevenLabs").await?;
        let voice_id = answer["voice_id"]
            .as_str()
            .node_err("ElevenLabs created the voice but its answer has no voice_id")?;

        // The voice exists on the account once ElevenLabs answers, so a
        // part of the answer it left out closes its port rather than
        // failing a run whose rerun would mint a second voice.
        let mut out = NodeOutput::new().set("voiceId", voice_id);
        if let Some(url) = answer["preview_url"].as_str() {
            out = out.set("previewUrl", url);
        }
        if answer["labels"].is_object() {
            out = out.set("labels", answer["labels"].clone());
        }
        ctx.pulse_downstream(out).await
    }
}

/// ElevenLabs keeps a voice description of 20 to 1000 characters; the
/// refusal for one outside that, said before anything is sent (the
/// compiler refuses a written one; this is the floor for a wired one).
fn description_refusal(description: &str) -> Option<String> {
    let length = description.chars().count();
    (!(20..=1000).contains(&length)).then(|| {
        format!(
            "`description` is {length} characters, and ElevenLabs takes 20 to 1000: {}",
            if length < 20 { "describe the voice in more detail" } else { "shorten it" }
        )
    })
}
