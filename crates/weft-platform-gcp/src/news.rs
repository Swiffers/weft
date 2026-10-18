//! News on Google Cloud: one Pub/Sub topic per install, and a
//! subscription of its own for every process that listens.
//!
//! A piece of news is one message, its run and kind as attributes. A
//! process that listens makes its own subscription to the topic the first
//! time it is asked to (in the background, never on a request's path) and
//! pulls from it for as long as it lives, each pull held by Pub/Sub until a
//! message comes. Nobody can address one Cloud Run instance from outside,
//! so the instance reaches out instead.
//!
//! A subscription Pub/Sub dropped (a day without a pull, Pub/Sub's
//! shortest expiry, which only an instance frozen that long reaches) is
//! made again under the same name; news announced meanwhile was missed, so
//! [`NewsHeard::MissedSome`] goes out each time a subscription is up. The
//! process deletes its subscription as it stops; one that died before it
//! could is dropped by its expiry.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::broadcast;
use weft_platform_traits::config::GcpPlatform;
use weft_platform_traits::news::LISTEN_CAPACITY;
use weft_platform_traits::{Listening, MayMiss, News, NewsHeard, NewsKind, RunNews};

use crate::api::{is_status, Google};

/// How long Pub/Sub holds one pull for a message before it answers an
/// empty one: the deadline this side sets on it (`X-Server-Timeout`, which
/// Google's REST APIs take as the time the server has to finish), since
/// Pub/Sub states no bound of its own.
const SERVER_HOLD: Duration = Duration::from_secs(60);

/// How long this side waits for a pull's answer: well past
/// [`SERVER_HOLD`], so a pull is never given up while Pub/Sub may still
/// answer it with messages it took for it (which would then wait out their
/// acknowledgement deadline before coming again).
const PULL_WAIT: Duration = Duration::from_secs(90);

/// How long a message waits to be acknowledged before Pub/Sub sends it
/// again; it is acknowledged as soon as it is handed on.
const ACK_DEADLINE_SECS: u32 = 10;

/// How long a message nobody pulled is kept: Pub/Sub's shortest. A
/// listener that missed one is told so once it is live again.
const RETENTION: &str = "600s";

/// How long a subscription nobody pulls from lives: Pub/Sub's shortest.
const EXPIRY: &str = "86400s";

/// How many messages one pull takes at most.
const PULL_AT_MOST: u32 = 1000;

/// The pause after a call that failed, at first and at most.
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_LONGEST: Duration = Duration::from_secs(60);

/// The calls news makes to Pub/Sub, a trait so its loop is tested against
/// a fake.
#[async_trait]
pub trait PubSubCalls: Send + Sync {
    async fn publish(&self, topic: &str, body: &Value) -> anyhow::Result<()>;
    async fn create_subscription(&self, subscription: &str, body: &Value) -> anyhow::Result<()>;
    /// Held until a message comes or [`SERVER_HOLD`] passed, answering an
    /// empty pull then.
    async fn pull(&self, subscription: &str, body: &Value) -> anyhow::Result<Value>;
    async fn acknowledge(&self, subscription: &str, body: &Value) -> anyhow::Result<()>;
    async fn delete_subscription(&self, subscription: &str) -> anyhow::Result<()>;
}

const PUBSUB: &str = "https://pubsub.googleapis.com/v1";

/// Pub/Sub's REST API, as the runtime's own account.
pub struct GooglePubSub {
    google: Google,
}

#[async_trait]
impl PubSubCalls for GooglePubSub {
    async fn publish(&self, topic: &str, body: &Value) -> anyhow::Result<()> {
        self.google.post(&format!("{PUBSUB}/{topic}:publish"), body).await.map(|_| ())
    }

    async fn create_subscription(&self, subscription: &str, body: &Value) -> anyhow::Result<()> {
        self.google.put(&format!("{PUBSUB}/{subscription}"), body).await.map(|_| ())
    }

    async fn pull(&self, subscription: &str, body: &Value) -> anyhow::Result<Value> {
        let url = format!("{PUBSUB}/{subscription}:pull");
        let pull = self.google.http().post(&url).json(body).header("X-Server-Timeout", SERVER_HOLD.as_secs().to_string()).timeout(PULL_WAIT);
        let resp = self.google.send_raw(pull).await.map_err(|e| e.context(format!("POST {url}")))?;
        let resp = Google::success(resp).await.map_err(|e| e.context(format!("POST {url}")))?;
        Ok(resp.json().await?)
    }

    async fn acknowledge(&self, subscription: &str, body: &Value) -> anyhow::Result<()> {
        self.google.post(&format!("{PUBSUB}/{subscription}:acknowledge"), body).await.map(|_| ())
    }

    async fn delete_subscription(&self, subscription: &str) -> anyhow::Result<()> {
        self.google.delete(&format!("{PUBSUB}/{subscription}")).await.map(|_| ())
    }
}

impl GooglePubSub {
    pub fn new(google: Google) -> Self {
        Self { google }
    }
}

/// One topic, and this process's own subscription to it once something
/// listens: what every kind of news on Pub/Sub shares. `H` is what a
/// listener hears; `read` says which message is one (`None` for a
/// message this process does not read).
pub struct Subscribed<H> {
    calls: Arc<dyn PubSubCalls>,
    /// `projects/<p>/topics/<topic>`.
    topic: String,
    /// `projects/<p>/subscriptions/<name>`: this process's own.
    subscription: String,
    heard: broadcast::Sender<H>,
    read: fn(&Value) -> Option<H>,
    /// The loop pulling from the subscription, once something listens.
    pulling: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Whether the subscription was made: only one that was is deleted.
    made: Arc<AtomicBool>,
}

impl<H: MayMiss + Send + 'static> Subscribed<H> {
    /// Topic `topic` of `project`, through `calls`, this process's
    /// subscription named after the topic and a name of its own.
    pub fn over(calls: Arc<dyn PubSubCalls>, project: &str, topic: &str, read: fn(&Value) -> Option<H>) -> Self {
        let own = uuid::Uuid::new_v4().simple().to_string();
        Self {
            calls,
            topic: format!("projects/{project}/topics/{topic}"),
            subscription: format!("projects/{project}/subscriptions/{topic}-{own}"),
            heard: broadcast::channel(LISTEN_CAPACITY).0,
            read,
            pulling: parking_lot::Mutex::new(None),
            made: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Publish `body` (`{"messages": [...]}`) on the topic.
    pub async fn publish(&self, body: &Value) -> anyhow::Result<()> {
        self.calls.publish(&self.topic, body).await
    }

    /// Everything heard from now on; the first call starts the pulling.
    pub fn listen(&self) -> Listening<H> {
        let listening = self.heard.subscribe();
        let mut pulling = self.pulling.lock();
        if pulling.is_none() {
            *pulling = Some(tokio::spawn(pull_forever(
                self.calls.clone(),
                self.topic.clone(),
                self.subscription.clone(),
                self.heard.clone(),
                self.read,
                self.made.clone(),
            )));
        }
        listening.into()
    }

    /// Stop pulling and delete the subscription, when one was made.
    pub async fn close(&self) -> anyhow::Result<()> {
        let Some(pulling) = self.pulling.lock().take() else { return Ok(()) };
        pulling.abort();
        if !self.made.load(Ordering::Acquire) {
            return Ok(());
        }
        self.calls.delete_subscription(&self.subscription).await
    }
}

pub struct PubSubNews {
    topic: Subscribed<NewsHeard>,
}

impl PubSubNews {
    pub fn new(google: Google, gcp: &GcpPlatform) -> Self {
        Self::over(Arc::new(GooglePubSub { google }), &gcp.project, &gcp.news_topic)
    }

    /// News on topic `topic` of `project`, through `calls`.
    pub fn over(calls: Arc<dyn PubSubCalls>, project: &str, topic: &str) -> Self {
        Self { topic: Subscribed::over(calls, project, topic, |message| news_in(message).map(NewsHeard::News)) }
    }
}

/// The attribute values a message carries its kind in.
// SYNC: kind_name <-> kind_named
fn kind_name(kind: NewsKind) -> &'static str {
    match kind {
        NewsKind::Answer => "answer",
        NewsKind::Cancel => "cancel",
    }
}

// SYNC: kind_named <-> kind_name
fn kind_named(name: &str) -> Option<NewsKind> {
    match name {
        "answer" => Some(NewsKind::Answer),
        "cancel" => Some(NewsKind::Cancel),
        _ => None,
    }
}

/// One publish announcing every piece of `news`, each a message of
/// nothing but attributes.
fn publish_body(news: &[RunNews]) -> Value {
    let messages: Vec<Value> = news
        .iter()
        .map(|news| {
            json!({
                "attributes": {
                    "project": news.project.to_string(),
                    "run": news.run.to_string(),
                    "kind": kind_name(news.kind),
                },
            })
        })
        .collect();
    json!({ "messages": messages })
}

/// The news a message's attributes say, if they are news.
fn news_in(message: &Value) -> Option<RunNews> {
    let attribute = |name: &str| message.get("attributes")?.get(name)?.as_str();
    Some(RunNews {
        project: attribute("project")?.parse().ok()?,
        run: attribute("run")?.parse().ok()?,
        kind: kind_named(attribute("kind")?)?,
    })
}

/// A subscription of `topic` that keeps nothing past what news needs.
fn subscription_body(topic: &str) -> Value {
    json!({
        "topic": topic,
        "ackDeadlineSeconds": ACK_DEADLINE_SECS,
        "messageRetentionDuration": RETENTION,
        "expirationPolicy": { "ttl": EXPIRY },
    })
}

/// What a pull answered: each message's ack id, and what `read` makes of
/// it (`None` for a message it does not read, acknowledged and dropped
/// loudly: nothing but weft publishes to the topic).
fn pulled<H>(answer: &Value, read: fn(&Value) -> Option<H>) -> Vec<(String, Option<H>)> {
    let Some(received) = answer.get("receivedMessages").and_then(Value::as_array) else { return Vec::new() };
    received
        .iter()
        .filter_map(|r| Some((r.get("ackId").and_then(Value::as_str)?.to_string(), r.get("message").and_then(read))))
        .collect()
}

/// Make the subscription and pull from it, handing on what it hears, for
/// as long as the process lives (the task is stopped by
/// [`Subscribed::close`]). "Missed some" goes out as soon as a
/// subscription is made:
/// Pub/Sub keeps for it every message published from then on, so whatever
/// came before is all that was missed. A call that fails, a subscription
/// that keeps going away included, waits a growing pause before the next,
/// so listeners are told to look again at that pace at most.
async fn pull_forever<H: MayMiss>(
    calls: Arc<dyn PubSubCalls>,
    topic: String,
    subscription: String,
    heard: broadcast::Sender<H>,
    read: fn(&Value) -> Option<H>,
    made: Arc<AtomicBool>,
) {
    let mut retry = RETRY_FIRST;
    let pull = json!({ "maxMessages": PULL_AT_MOST });
    let pause = |retry: &mut Duration| {
        let now = *retry;
        *retry = (*retry * 2).min(RETRY_LONGEST);
        tokio::time::sleep(now)
    };
    loop {
        match calls.create_subscription(&subscription, &subscription_body(&topic)).await {
            // Already there: an earlier make whose answer was lost.
            Ok(()) => {}
            Err(e) if is_status(&e, 409) => {}
            Err(e) => {
                tracing::warn!(target: "weft_platform_gcp::news", %topic, %subscription, error = %format!("{e:#}"), "could not subscribe to the topic; trying again");
                pause(&mut retry).await;
                continue;
            }
        }
        made.store(true, Ordering::Release);
        tracing::info!(target: "weft_platform_gcp::news", %topic, %subscription, "listening to the topic");
        // Nobody listening yet is fine: a listener comes later.
        let _ = heard.send(H::missed_some());
        loop {
            let answer = match calls.pull(&subscription, &pull).await {
                Ok(answer) => answer,
                Err(e) if is_status(&e, 404) => {
                    tracing::warn!(target: "weft_platform_gcp::news", %topic, %subscription, "the subscription to the topic is gone; making it again");
                    pause(&mut retry).await;
                    break;
                }
                Err(e) => {
                    tracing::warn!(target: "weft_platform_gcp::news", %topic, %subscription, error = %format!("{e:#}"), "could not pull from the topic; trying again");
                    pause(&mut retry).await;
                    continue;
                }
            };
            retry = RETRY_FIRST;
            let messages = pulled(&answer, read);
            if messages.is_empty() {
                continue;
            }
            let mut acks = Vec::with_capacity(messages.len());
            for (ack, news) in messages {
                match news {
                    Some(news) => {
                        let _ = heard.send(news);
                    }
                    None => tracing::error!(target: "weft_platform_gcp::news", %subscription, "a message on the topic that this process does not read; dropped"),
                }
                acks.push(ack);
            }
            // A message not acknowledged comes again, which costs a look
            // and nothing else.
            if let Err(e) = calls.acknowledge(&subscription, &json!({ "ackIds": acks })).await {
                tracing::warn!(target: "weft_platform_gcp::news", %topic, %subscription, error = %format!("{e:#}"), "could not acknowledge the messages pulled; they come again");
            }
        }
    }
}

#[async_trait]
impl News for PubSubNews {
    async fn announce(&self, news: &[RunNews]) -> anyhow::Result<()> {
        self.topic.publish(&publish_body(news)).await
    }

    fn listen(&self) -> Listening {
        self.topic.listen()
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.topic.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ApiError;

    fn news(kind: NewsKind) -> RunNews {
        RunNews { project: uuid::Uuid::from_u128(1), run: uuid::Uuid::from_u128(2), kind }
    }

    #[test]
    fn an_announcement_reads_back_as_the_news_it_was() {
        for kind in [NewsKind::Answer, NewsKind::Cancel] {
            let body = publish_body(&[news(kind)]);
            let answer = json!({ "receivedMessages": [{ "ackId": "a1", "message": body["messages"][0] }] });
            assert_eq!(pulled(&answer, news_in), vec![("a1".to_string(), Some(news(kind)))]);
        }
    }

    #[test]
    fn a_message_that_is_not_news_is_still_acknowledged() {
        let answer = json!({ "receivedMessages": [{ "ackId": "a1", "message": { "data": "eA==" } }] });
        assert_eq!(pulled(&answer, news_in), vec![("a1".to_string(), None)]);
        assert!(pulled(&json!({}), news_in).is_empty(), "an empty pull hears nothing");
    }

    #[test]
    fn a_subscription_keeps_nothing_past_what_news_needs() {
        let body = subscription_body("projects/p/topics/t");
        assert_eq!(body["topic"], "projects/p/topics/t");
        assert_eq!(body["expirationPolicy"]["ttl"], "86400s");
        assert_eq!(body["messageRetentionDuration"], "600s");
    }

    /// Every call made, in order, what each pull answers (none left: a
    /// pull that never answers, or the last answer again when `repeat`),
    /// and whether making a subscription fails.
    #[derive(Default)]
    struct FakePubSub {
        calls: parking_lot::Mutex<Vec<(&'static str, String, Value)>>,
        pulls: parking_lot::Mutex<std::collections::VecDeque<anyhow::Result<Value>>>,
        repeat_gone: bool,
        create_fails: bool,
    }

    impl FakePubSub {
        fn log(&self, call: &'static str, on: &str, body: Value) {
            self.calls.lock().push((call, on.to_string(), body));
        }

        fn made(&self) -> Vec<&'static str> {
            self.calls.lock().iter().map(|(call, _, _)| *call).collect()
        }
    }

    #[async_trait]
    impl PubSubCalls for FakePubSub {
        async fn publish(&self, topic: &str, body: &Value) -> anyhow::Result<()> {
            self.log("publish", topic, body.clone());
            Ok(())
        }
        async fn create_subscription(&self, subscription: &str, body: &Value) -> anyhow::Result<()> {
            self.log("create", subscription, body.clone());
            match self.create_fails {
                true => Err(anyhow::Error::new(ApiError { status: 403, body: "denied".into() })),
                false => Ok(()),
            }
        }
        async fn pull(&self, subscription: &str, body: &Value) -> anyhow::Result<Value> {
            self.log("pull", subscription, body.clone());
            if self.repeat_gone {
                return Err(anyhow::Error::new(ApiError { status: 404, body: "gone".into() }));
            }
            let next = self.pulls.lock().pop_front();
            match next {
                Some(answer) => answer,
                None => std::future::pending().await,
            }
        }
        async fn acknowledge(&self, subscription: &str, body: &Value) -> anyhow::Result<()> {
            self.log("ack", subscription, body.clone());
            Ok(())
        }
        async fn delete_subscription(&self, subscription: &str) -> anyhow::Result<()> {
            self.log("delete", subscription, Value::Null);
            Ok(())
        }
    }

    async fn next(listening: &mut Listening) -> NewsHeard {
        tokio::time::timeout(Duration::from_secs(10), listening.next()).await.expect("heard in time").expect("still listening")
    }

    /// The first listener makes the subscription (with its expiry), is
    /// told as soon as it is made, hears each message pulled with each one
    /// acknowledged; a subscription Pub/Sub dropped is made again after a
    /// pause and said live again, and closing deletes it.
    #[tokio::test(start_paused = true)]
    async fn news_is_pulled_acknowledged_and_its_subscription_kept() {
        let fake = Arc::new(FakePubSub::default());
        let message = publish_body(&[news(NewsKind::Answer)])["messages"][0].clone();
        fake.pulls.lock().extend([
            Ok(json!({ "receivedMessages": [{ "ackId": "a1", "message": message }] })),
            Err(anyhow::Error::new(ApiError { status: 404, body: "gone".into() })),
            Ok(json!({})),
        ]);
        let pubsub = PubSubNews::over(fake.clone(), "proj", "weft-news");
        pubsub.announce(&[news(NewsKind::Cancel), news(NewsKind::Answer)]).await.unwrap();
        assert_eq!(fake.made(), vec!["publish"], "announcing listens to nothing, and a batch is one publish");
        assert_eq!(fake.calls.lock()[0].2["messages"].as_array().unwrap().len(), 2);
        assert_eq!(fake.calls.lock()[0].1, "projects/proj/topics/weft-news");

        let mut listening = pubsub.listen();
        assert_eq!(next(&mut listening).await, NewsHeard::MissedSome);
        assert_eq!(next(&mut listening).await, NewsHeard::News(news(NewsKind::Answer)));
        assert_eq!(next(&mut listening).await, NewsHeard::MissedSome, "the dropped subscription was made again");
        let calls = fake.calls.lock().clone();
        assert_eq!(calls.iter().map(|(call, _, _)| *call).take(6).collect::<Vec<_>>(), ["publish", "create", "pull", "ack", "pull", "create"]);
        assert!(calls[1].1.starts_with("projects/proj/subscriptions/weft-news-"), "{}", calls[1].1);
        assert_eq!(calls[1].2["expirationPolicy"]["ttl"], EXPIRY);
        assert_eq!(calls[3].2, json!({ "ackIds": ["a1"] }));
        assert_eq!(calls[5].1, calls[1].1, "made again under its own name");

        let _second = pubsub.listen();
        pubsub.close().await.unwrap();
        let made = fake.made();
        assert_eq!(made.iter().filter(|c| **c == "create").count(), 2, "a second listener shares the one subscription");
        assert_eq!(made.last(), Some(&"delete"));
    }

    /// A subscription that is gone again at every pull is made again only
    /// after a growing pause, so listeners are told to look again at that
    /// pace, not in a loop.
    #[tokio::test(start_paused = true)]
    async fn a_subscription_that_keeps_going_away_is_made_again_slowly() {
        let fake = Arc::new(FakePubSub { repeat_gone: true, ..FakePubSub::default() });
        let pubsub = PubSubNews::over(fake.clone(), "proj", "weft-news");
        let mut listening = pubsub.listen();
        tokio::time::sleep(Duration::from_secs(60)).await;
        let creates = fake.made().iter().filter(|c| **c == "create").count();
        assert!((2..=8).contains(&creates), "made again after a growing pause, not in a loop: {creates}");
        let mut told = 0;
        while let Ok(Ok(heard)) = tokio::time::timeout(Duration::from_millis(1), listening.next()).await {
            assert_eq!(heard, NewsHeard::MissedSome);
            told += 1;
        }
        assert_eq!(told, creates, "told once per subscription made");
    }

    /// A subscription that was never made is not deleted as the process
    /// stops.
    #[tokio::test(start_paused = true)]
    async fn only_a_subscription_that_was_made_is_deleted() {
        let fake = Arc::new(FakePubSub { create_fails: true, ..FakePubSub::default() });
        let pubsub = PubSubNews::over(fake.clone(), "proj", "weft-news");
        let _listening = pubsub.listen();
        tokio::time::sleep(Duration::from_secs(5)).await;
        pubsub.close().await.unwrap();
        assert!(!fake.made().contains(&"delete"), "{:?}", fake.made());
    }
}
