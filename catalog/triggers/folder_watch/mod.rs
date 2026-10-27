//! FolderWatch: fires a run when a file in a folder on this machine is
//! created, changed or removed.
//!
//!   - `setup_trigger`: register a folder_watch signal for the folder,
//!     with the name pattern and the changes that fire. The listener
//!     refuses it on an install whose holders are not on this machine,
//!     and when the folder is not there.
//!
//!   - `run`: the listener delivers `{path, event, sizeBytes?}` as the
//!     wake payload. Forward it to the outputs.

use async_trait::async_trait;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::signal::{FolderEvent, FolderWatch};
use weft::{RunContext, Node, NodeManifest, WeftResult};

#[cfg(feature = "node-tests")]
mod tests;

#[derive(NodeManifest)]
pub struct FolderWatchNode;

#[async_trait]
impl Node for FolderWatchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()> {
        let folder: String = ctx.inputs.get("folder")?;
        let recursive: bool = ctx.inputs.get("recursive")?;
        let pattern: Option<String> = ctx.inputs.opt("pattern")?;
        let events: Option<Vec<String>> = ctx.inputs.opt("events")?;
        // The `multiselect` widget binds every entry to one of its
        // options, so a word the kind does not know is a broken input, and
        // fails loud naming it. Left unset is the kind's default; a list
        // emptied by hand goes through as it is, and the kind refuses it.
        let events = match events {
            Some(words) => words
                .iter()
                .map(|w| FolderEvent::parse(w))
                .collect::<Result<Vec<_>, _>>()
                .map_err(weft::error::node_error)?,
            None => FolderWatch::every_event(),
        };
        // Registers the signal; setup emits nothing downstream.
        ctx.register_signal(FolderWatch {
            folder,
            recursive,
            pattern: pattern.filter(|p| !p.is_empty()),
            events,
        })
        .await
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        // `path` and `event` are on every fire; a fire missing one is a
        // broken delivery and fails loud. `sizeBytes` is absent for a
        // removed file, so its port closes then.
        let path: Value = ctx.wake.get("path")?;
        let event: Value = ctx.wake.get("event")?;
        let mut out = NodeOutput::new().set("path", path).set("event", event);
        if let Some(size) = ctx.wake.opt::<Value>("sizeBytes")? {
            out = out.set("sizeBytes", size);
        }
        ctx.pulse_downstream(out).await
    }
}
