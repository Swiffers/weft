//! What holders hear, on Google Cloud: a Pub/Sub topic of their own
//! (`GcpPlatform::holders_topic`), so the brokers listening to the
//! install's run news never see a holder's note, and a holder never pulls
//! the run news.
//!
//! The dispatcher publishes; each holder makes its own subscription as it
//! starts and is told it may have missed some once it is up, which is its
//! first look (`crate::news::Subscribed`). A note is one message of
//! nothing but an attribute.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::{HolderHeard, HolderNews, HolderNote, Listening};

use crate::api::Google;
use crate::news::{GooglePubSub, PubSubCalls, Subscribed};

pub struct PubSubHolderNews {
    topic: Subscribed<HolderHeard>,
}

impl PubSubHolderNews {
    pub fn new(google: Google, gcp: &GcpPlatform) -> Self {
        Self::over(Arc::new(GooglePubSub::new(google)), &gcp.project, &gcp.holders_topic)
    }

    /// Notes on topic `topic` of `project`, through `calls`.
    pub fn over(calls: Arc<dyn PubSubCalls>, project: &str, topic: &str) -> Self {
        Self { topic: Subscribed::over(calls, project, topic, |message| note_in(message).map(HolderHeard::Note)) }
    }
}

/// The message carrying `note`: its word (`HolderNote`'s serde name) as
/// the `note` attribute. Written and read through the same derive, so
/// every note reads back as itself.
fn publish_body(note: HolderNote) -> Value {
    json!({ "messages": [{ "attributes": { "note": note } }] })
}

/// The note `message` carries, or `None` for a message that is no note
/// (run news, or a word no note has).
fn note_in(message: &Value) -> Option<HolderNote> {
    serde_json::from_value(message.get("attributes")?.get("note")?.clone()).ok()
}

#[async_trait]
impl HolderNews for PubSubHolderNews {
    async fn tell(&self, note: HolderNote) -> anyhow::Result<()> {
        self.topic.publish(&publish_body(note)).await
    }

    fn listen(&self) -> Listening<HolderHeard> {
        self.topic.listen()
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.topic.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_note_reads_back_as_the_note_it_was_and_nothing_else_is_one() {
        for (note, word) in [(HolderNote::HeldSetChanged, "held_set_changed"), (HolderNote::AccessChanged, "access_changed")] {
            let message = &publish_body(note)["messages"][0];
            assert_eq!(message["attributes"]["note"], word, "the word a message carries");
            assert_eq!(note_in(message), Some(note));
        }
        assert_eq!(note_in(&json!({ "attributes": { "kind": "answer" } })), None, "run news is not a note");
        assert_eq!(note_in(&json!({ "attributes": { "note": "other" } })), None);
    }
}
