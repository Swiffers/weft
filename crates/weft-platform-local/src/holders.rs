//! A local install's one process holds every held connection itself
//! ([`OneProcessHolds`]): no pool to size, and no dead man's switch.
//!
//! What holders hear is its own database's notifications, which its one
//! process listens to for good: the writes that change which signals need
//! a holder announce themselves there, and so do the writes that make a
//! connection again or remove it.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;
use weft_platform_traits::holders::{HolderHeard, HolderNews, HolderNote};
use weft_platform_traits::news::LISTEN_CAPACITY;
use weft_platform_traits::Listening;
use weft_task_store::pg_signal::{Heard, PgSignalWatch};

pub use weft_platform_traits::OneProcessHolds;

/// What holders hear, read off the process's own database watch: a
/// notification on `held_channel` is the held set changing, one on
/// `access_channel` a connection made again or removed, and a watch that may have
/// missed some says so.
pub struct DatabaseHolderNews {
    watch: Arc<PgSignalWatch>,
    held_channel: &'static str,
    access_channel: &'static str,
    heard: broadcast::Sender<HolderHeard>,
    /// The relay from the watch, once something listens.
    relaying: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl DatabaseHolderNews {
    pub fn new(watch: Arc<PgSignalWatch>, held_channel: &'static str, access_channel: &'static str) -> Self {
        Self { watch, held_channel, access_channel, heard: broadcast::channel(LISTEN_CAPACITY).0, relaying: parking_lot::Mutex::new(None) }
    }
}

/// What a holder hears for what the watch heard, if anything.
fn note_of(heard: &Heard, held_channel: &str, access_channel: &str) -> Option<HolderHeard> {
    match heard {
        Heard::Signal { channel, .. } if *channel == held_channel => Some(HolderHeard::Note(HolderNote::HeldSetChanged)),
        Heard::Signal { channel, .. } if *channel == access_channel => Some(HolderHeard::Note(HolderNote::AccessChanged)),
        Heard::Signal { .. } | Heard::Lost => None,
        Heard::Recheck | Heard::Resumed => Some(HolderHeard::MissedSome),
    }
}

#[async_trait]
impl HolderNews for DatabaseHolderNews {
    /// The write that changed things already told the database, which
    /// is what the holder hears.
    async fn tell(&self, _note: HolderNote) -> anyhow::Result<()> {
        Ok(())
    }

    fn listen(&self) -> Listening<HolderHeard> {
        let listening = self.heard.subscribe();
        let mut relaying = self.relaying.lock();
        if relaying.is_none() {
            let mut watched = self.watch.subscribe();
            let (heard, held, access) = (self.heard.clone(), self.held_channel, self.access_channel);
            *relaying = Some(tokio::spawn(async move {
                loop {
                    match watched.next().await {
                        Ok(seen) => {
                            if let Some(note) = note_of(&seen, held, access) {
                                // Nobody listening is fine: a holder that
                                // starts later looks first anyway.
                                let _ = heard.send(note);
                            }
                        }
                        Err(e) => {
                            tracing::error!(target: "weft_platform_local::holders", error = %format!("{e:#}"), "the database watch stopped; the holder hears nothing more");
                            return;
                        }
                    }
                }
            }));
        }
        listening.into()
    }

    async fn close(&self) -> anyhow::Result<()> {
        if let Some(relaying) = self.relaying.lock().take() {
            relaying.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_watch_is_heard_as_holder_notes() {
        let signal = |channel: &'static str| Heard::Signal { channel, payload: Arc::from("") };
        assert_eq!(note_of(&signal("held"), "held", "access"), Some(HolderHeard::Note(HolderNote::HeldSetChanged)));
        assert_eq!(note_of(&signal("access"), "held", "access"), Some(HolderHeard::Note(HolderNote::AccessChanged)));
        assert_eq!(note_of(&signal("other"), "held", "access"), None);
        assert_eq!(note_of(&Heard::Recheck, "held", "access"), Some(HolderHeard::MissedSome));
        assert_eq!(note_of(&Heard::Resumed, "held", "access"), Some(HolderHeard::MissedSome));
        assert_eq!(note_of(&Heard::Lost, "held", "access"), None, "the recheck after it is what says so");
    }
}
