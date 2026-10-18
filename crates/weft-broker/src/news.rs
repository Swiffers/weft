//! The install's news (`weft_platform_traits::News`), heard into the
//! broker's signal watch.
//!
//! A worker whose run only waits keeps its line to a broker copy open and
//! stops everything else (`weft_engine::run_holds`), and that copy pushes
//! the run's answer and its cancel down the line (`crate::line`) holding no
//! listening of the database, so the database can sleep. The answer and the
//! cancel are written by a dispatcher copy,
//! which also announces them as news. Each one heard here is told to the
//! copy's waiters exactly as the database would have told it, on the same
//! channel with the same payload, so nothing downstream knows the
//! difference; the database telling it too only costs a look.

use std::sync::Arc;

use weft_platform_traits::{Listening, NewsHeard, NewsKind, RunNews};
use weft_task_store::pg_signal::AlsoTold;

use crate::line::LineFanout;

/// The channels news comes in on, for the watch that hears it.
pub const NEWS_CHANNELS: &[&str] = &[weft_task_store::parked_fires::PARKED_FIRE_CHANNEL, weft_task_store::runs::CANCEL_CHANNEL];

/// What the database announces for `news`: an answer handed to a run as a
/// parked fire naming the run, a cancel as a cancel naming it.
// SYNC: the payloads <-> crates/weft-task-store/src/parked_fires.rs (parked_fire_notify), crates/weft-dispatcher/src/journal/postgres.rs (cancel_run)
fn as_announced(news: RunNews) -> (&'static str, String) {
    let channel = match news.kind {
        NewsKind::Answer => weft_task_store::parked_fires::PARKED_FIRE_CHANNEL,
        NewsKind::Cancel => weft_task_store::runs::CANCEL_CHANNEL,
    };
    (channel, weft_task_store::runs::run_payload(news.project, news.run))
}

/// Tell `told` everything `listening` hears, for the life of the process:
/// news as its announcement. News missed makes the open `lines` look
/// again, and only them: the lines are the only ones here waiting on news,
/// and a loop or a role rung for it would wake the database for nothing
/// (a subscription reports some missed each time it is made, at every
/// start of a broker copy). Returns only when either side stopped, which
/// leaves the broker's waiters deaf to news, so it is run supervised.
pub async fn pass_on(mut listening: Listening, told: AlsoTold, lines: Arc<LineFanout>) {
    loop {
        let heard = match listening.next().await {
            Ok(heard) => heard,
            Err(e) => {
                tracing::error!(target: "weft_broker::news", error = %format!("{e:#}"), "the install's news stopped: runs held on this copy would hear their answers and cancels late");
                return;
            }
        };
        let passed = match heard {
            NewsHeard::News(news) => {
                let (channel, payload) = as_announced(news);
                match told.signal(channel, &payload) {
                    Ok(passed) => passed,
                    Err(e) => {
                        tracing::error!(target: "weft_broker::news", error = %format!("{e:#}"), "news the broker's watch cannot pass on");
                        return;
                    }
                }
            }
            NewsHeard::MissedSome => {
                lines.recheck_lines();
                true
            }
        };
        if !passed {
            tracing::error!(target: "weft_broker::news", "the broker's signal watch stopped; news has nowhere to go");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use weft_broker_client::line::Notice;
    use weft_platform_traits::{FakeNews, News};
    use weft_task_store::pg_signal::{Heard, Subscription};

    use super::*;

    fn news(kind: NewsKind) -> RunNews {
        RunNews { project: uuid::Uuid::from_u128(1), run: uuid::Uuid::from_u128(2), kind }
    }

    #[test]
    fn news_is_told_as_the_database_announces_it() {
        let payload = weft_task_store::runs::run_payload(uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        assert_eq!(as_announced(news(NewsKind::Answer)), (weft_task_store::parked_fires::PARKED_FIRE_CHANNEL, payload.clone()));
        assert_eq!(as_announced(news(NewsKind::Cancel)), (weft_task_store::runs::CANCEL_CHANNEL, payload));
    }

    async fn next(heard: &mut Subscription) -> Heard {
        tokio::time::timeout(Duration::from_secs(10), heard.next()).await.expect("heard in time").unwrap()
    }

    // News reaches the broker's waiters as the database's announcement
    // would, and an answer and a cancel reach the line of the run's
    // project; news missed makes the open lines look again, and rings
    // nothing else.
    weft_core::stress_test! {
        name: news_reaches_the_waiters_and_the_runs_line,
        runs: 10,
        worker_threads: 4,
        async fn body() {
            let fake = FakeNews::default();
            let (tx, rx) = tokio::sync::broadcast::channel(64);
            let mut waiter = Subscription::from(rx);
            let fanout = LineFanout::start(Subscription::with_listening(tx.subscribe(), Arc::new(AtomicBool::new(false))), None);
            let (line, mut on_line) = mpsc::channel(8);
            let _following = fanout.follow("t", &uuid::Uuid::from_u128(1).to_string(), line);
            tokio::spawn(pass_on(fake.listen(), AlsoTold::over(&tx, NEWS_CHANNELS), fanout.clone()));
            // Told once the passing-on listens.
            while fake.heard.receiver_count() < 1 {
                tokio::task::yield_now().await;
            }

            fake.heard.send(NewsHeard::News(news(NewsKind::Answer))).unwrap();
            let (channel, payload) = as_announced(news(NewsKind::Answer));
            assert_eq!(next(&mut waiter).await, Heard::Signal { channel, payload: payload.clone().into() }, "a waiter hears the answer handed");
            let told = tokio::time::timeout(Duration::from_secs(10), on_line.recv()).await.unwrap().unwrap();
            assert_eq!(told, Notice::Signal { channel: channel.into(), payload }, "the run's line hears its answer");

            fake.heard.send(NewsHeard::News(news(NewsKind::Cancel))).unwrap();
            let (channel, payload) = as_announced(news(NewsKind::Cancel));
            let told = tokio::time::timeout(Duration::from_secs(10), on_line.recv()).await.unwrap().unwrap();
            assert_eq!(told, Notice::Signal { channel: channel.into(), payload }, "the run's line hears its cancel");

            assert_eq!(next(&mut waiter).await, Heard::Signal { channel, payload: as_announced(news(NewsKind::Cancel)).1.into() });

            fake.heard.send(NewsHeard::MissedSome).unwrap();
            let told = tokio::time::timeout(Duration::from_secs(10), on_line.recv()).await.unwrap().unwrap();
            assert_eq!(told, Notice::Recheck { listening: false }, "news missed: every line looks again, though the database's listener is quiet");
            // The waiters (the role waker among them) hear nothing of it:
            // the next thing they hear is the next news.
            fake.heard.send(NewsHeard::News(news(NewsKind::Answer))).unwrap();
            let (channel, payload) = as_announced(news(NewsKind::Answer));
            assert_eq!(next(&mut waiter).await, Heard::Signal { channel, payload: payload.into() }, "no role is rung for news missed");
        }
    }
}
