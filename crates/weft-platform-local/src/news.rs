//! News on a local install: its database never sleeps, and its one
//! process listens to it for good, hearing every write that hands a run an
//! answer or asks it to stop (`weft_task_store::parked_fires`,
//! `weft_task_store::runs`). News carries nothing on top.

use async_trait::async_trait;
use tokio::sync::broadcast;
use weft_platform_traits::{Listening, News, NewsHeard, RunNews};

pub struct DatabaseNews {
    /// Kept so every listener stays open, hearing nothing.
    heard: broadcast::Sender<NewsHeard>,
}

impl Default for DatabaseNews {
    fn default() -> Self {
        Self { heard: broadcast::channel(1).0 }
    }
}

#[async_trait]
impl News for DatabaseNews {
    async fn announce(&self, _news: &[RunNews]) -> anyhow::Result<()> {
        Ok(())
    }

    fn listen(&self) -> Listening {
        self.heard.subscribe().into()
    }

    async fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
