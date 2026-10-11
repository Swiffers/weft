//! Telling the install's other copies that something happened to a run,
//! without the database.
//!
//! A run's worker keeps its line open to one broker copy while it drives
//! the run, and that copy has to pass on what happened to the run
//! elsewhere: a person answered what it waits on, or somebody cancelled it.
//! The copy that wrote it is a dispatcher copy, and a serverless database
//! that sleeps once nothing queries it cannot carry the word: listening to
//! it means holding a connection open, which keeps it awake. News carries
//! it instead, and only the word: which run, and what kind of thing
//! happened. Whoever hears it reads what happened through weft, as on any
//! announcement.
//!
//! The contract is a doorbell: news may be lost, heard twice or out of
//! order. A listener that may have missed some is told so
//! ([`NewsHeard::MissedSome`]) and looks at everything it waits on again.

use async_trait::async_trait;
use tokio::sync::broadcast;

/// What happened to a run, as news says it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NewsKind {
    /// An answer to one of its waits was handed to its worker.
    Answer,
    /// It was asked to stop.
    Cancel,
}

/// One piece of news: run `execution` of `project` had something happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunNews {
    pub project: uuid::Uuid,
    pub execution: weft_core::ExecutionId,
    pub kind: NewsKind,
}

/// What a listener hears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewsHeard {
    News(RunNews),
    /// The listening started, or came back after a gap: news may have been
    /// missed, so whatever waits on some looks again.
    MissedSome,
}

#[async_trait]
pub trait News: Send + Sync {
    /// Tell every copy listening each piece of `news`, in one call. Called
    /// once the writes it announces committed; an error means none of it
    /// went out for sure, and the caller announces it again.
    async fn announce(&self, news: &[RunNews]) -> anyhow::Result<()>;

    /// Everything heard from now on. The first call starts the process's
    /// listening, in the background (never on its caller's path), and
    /// [`NewsHeard::MissedSome`] says when it is up.
    fn listen(&self) -> Listening;

    /// The process is stopping: what its listening holds goes.
    async fn close(&self) -> anyhow::Result<()>;
}

/// How many pieces of news a slow listener may fall behind by before it is
/// told it missed some.
pub const LISTEN_CAPACITY: usize = 1024;

/// One listener's view of the news.
pub struct Listening {
    heard: broadcast::Receiver<NewsHeard>,
}

impl From<broadcast::Receiver<NewsHeard>> for Listening {
    fn from(heard: broadcast::Receiver<NewsHeard>) -> Self {
        Self { heard }
    }
}

impl Listening {
    /// The next thing heard. Falling behind is missing some; the news
    /// stopping is an error, since nothing would be heard again.
    pub async fn next(&mut self) -> anyhow::Result<NewsHeard> {
        match self.heard.recv().await {
            Ok(heard) => Ok(heard),
            Err(broadcast::error::RecvError::Lagged(_)) => Ok(NewsHeard::MissedSome),
            Err(broadcast::error::RecvError::Closed) => anyhow::bail!("the install's news stopped"),
        }
    }
}

/// Records every announcement, and hands every listener what a test sends.
#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    pub struct FakeNews {
        /// Every call, each the batch it announced.
        pub announced: parking_lot::Mutex<Vec<Vec<RunNews>>>,
        pub heard: broadcast::Sender<NewsHeard>,
    }

    impl Default for FakeNews {
        fn default() -> Self {
            Self { announced: parking_lot::Mutex::default(), heard: broadcast::channel(LISTEN_CAPACITY).0 }
        }
    }

    #[async_trait]
    impl News for FakeNews {
        async fn announce(&self, news: &[RunNews]) -> anyhow::Result<()> {
            self.announced.lock().push(news.to_vec());
            Ok(())
        }

        fn listen(&self) -> Listening {
            self.heard.subscribe().into()
        }

        async fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_listener_that_fell_behind_is_told_it_missed_some() {
        let (tx, rx) = broadcast::channel(1);
        let mut listening = Listening::from(rx);
        let news = RunNews { project: uuid::Uuid::from_u128(1), execution: uuid::Uuid::from_u128(2), kind: NewsKind::Answer };
        tx.send(NewsHeard::News(news)).unwrap();
        tx.send(NewsHeard::News(news)).unwrap();
        assert_eq!(listening.next().await.unwrap(), NewsHeard::MissedSome);
        assert_eq!(listening.next().await.unwrap(), NewsHeard::News(news));
        drop(tx);
        assert!(listening.next().await.is_err(), "news that stopped is loud");
    }
}
