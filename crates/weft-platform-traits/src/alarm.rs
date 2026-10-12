//! Wake a role at a time.
//!
//! Nothing in weft sleeps for long in memory any more: a role may be a
//! service scaled to zero, and a sleeping task there is a task that never
//! wakes. Whatever must happen later (a timer's next fire, a poll's next
//! cycle, a loop's next look) is handed to an [`Alarm`], which calls the
//! role back at that time.
//!
//! The contract is deliberately small, because the receiver is the one
//! that knows the truth:
//!
//! - **One shot.** A wake is delivered once; whatever wants the next one
//!   sets it when it handles this one (so a cron is "each wake sets the
//!   next").
//! - **At least once, maybe late, maybe stale.** A platform may deliver a
//!   wake twice, a little late, or after the thing it was set for has
//!   changed. The receiver recomputes from its own durable state whether
//!   anything is due, and does nothing when not.
//! - **A wake no longer wanted is cancelled.** Whoever moves or drops what
//!   a wake was set for cancels the old one ([`Alarm::cancel`]), so a
//!   quiet install is not woken for nothing. A cancel can come too late
//!   (the wake is already being delivered), which costs the receiver one
//!   look.
//! - **Names never repeat.** A platform may keep a delivered or cancelled
//!   wake's name for a while and refuse a new wake under it, so the name
//!   is made from the key, the time and a `generation`, and a setter gives
//!   every booking a key, time and generation no other booking of it ever
//!   had (a booking's counter, a holder's push counter, a registration's
//!   version with moments that only move forward). A name is then only
//!   ever set again by the same booking, so setting a wake whose name is
//!   taken is that wake already set: an earlier try whose answer was lost,
//!   or the same booking armed twice. A receiver that sets the moment it
//!   is handling again (a wake that arrived early) makes that a booking of
//!   its own, since the task it is answering still holds the name
//!   ([`set_again_if_early`], [`again_key`]).
//! - **The answer's status says whether to try again.** A receiver that
//!   fails for now (5xx, an auth or routing 4xx such as 401, 403 or 404,
//!   or no answer at all) gets the wake again later, for as long as its
//!   queue's retry window allows ([`WakeQueue`]). Only an answer that says
//!   the request body itself is wrong (400, 413, 415, 422: a body from an
//!   older build the receiver no longer reads) means the same call can
//!   never succeed: an alarm that can tell drops it, loudly
//!   ([`WakeRefusal::of_status`]).
//! - **Each delivery says which try it is.** The alarm counts the
//!   deliveries of one wake and puts that count on each call
//!   ([`Alarm::delivery`]), with how many tries the platform gives a
//!   receiver that keeps failing ([`Delivery`]). A receiver whose failure
//!   repeats on the last of them stops what the wake was for, records
//!   why, and answers it as handled, so no wake is retried for as long as
//!   a queue keeps a task.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::roles::CoreRole;

/// One wake: call `role` at `path` with `body`, at `at_unix_ms` (posting
/// [`WakeCall`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Wake {
    /// What this wake is for, unique per thing that wakes (a signal's
    /// token, a loop of a role). With `at_unix_ms` and `generation` it
    /// names the wake.
    pub key: String,
    pub at_unix_ms: i64,
    /// With `key` and `at_unix_ms`, what tells this booking from every
    /// other the setter ever made (a booking's counter, a registration's
    /// version), so a wake set after one was delivered or cancelled never
    /// reuses its name.
    pub generation: i64,
    pub queue: WakeQueue,
    pub role: CoreRole,
    /// The role's internal path to call (`/wake`, or
    /// [`crate::roles::TICK_PATH`] for a role's own tick).
    pub path: String,
    pub body: serde_json::Value,
}

/// Which of the platform's queues a wake goes through: each has its own
/// window for retrying a receiver that keeps failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeQueue {
    /// What keeps a user's triggers going, which must never be dropped
    /// unseen: their timers, crons, polls and provider renewals (each
    /// wake sets the next, so a dropped one ends the chain for good), and
    /// a holder's liveness check (the one thing that frees a dead holder's
    /// claims, so another holder serves its triggers). Retried for as long
    /// as the receiver fails, as the platform allows: a stale wake is a
    /// no-op, and a receiver that fails the last of its tries
    /// ([`Delivery::is_last`]) gives up on what the wake was for, says
    /// why where its user sees it, and answers it as handled.
    Triggers,
    /// weft's own looks: a role's tick for one of its loops, a ring that
    /// failed. Retried briefly; what is left after that waits for the next
    /// real event.
    Internal,
}

/// What an alarm posts to the role's `path` when the wake comes due: the
/// moment it was set for and the body it was set with. Every platform's
/// alarm delivers exactly this, so a role reads one shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WakeCall<B> {
    pub at_unix_ms: i64,
    pub body: B,
}

/// Whether a wake the receiver did not take can ever be taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeRefusal {
    /// The receiver failed for now: try again later.
    Temporary,
    /// The request itself is wrong: the same call will be refused forever.
    Permanent,
}

impl WakeRefusal {
    /// Read the status of an answer that was not a success. Only the
    /// statuses that judge the request body (400 bad request, 413 too
    /// large, 415 unsupported type, 422 unprocessable) are permanent.
    /// Everything else is temporary: a 5xx, a 408 or 429, and also a 401,
    /// 403 or 404, which say the caller's credentials or the receiver's
    /// routes are not right YET (a rotating key, a role mid-deploy), and
    /// would delete a good wake if read as final.
    pub fn of_status(status: u16) -> Self {
        match status {
            400 | 413 | 415 | 422 => WakeRefusal::Permanent,
            _ => WakeRefusal::Temporary,
        }
    }
}

/// Which try of one wake a delivery is, counting only the deliveries its
/// receiver answered ([`answered_by_receiver`]), how many such tries
/// the platform gives a receiver that keeps failing it, and whether any
/// delivery of the wake came before this one at all. A delivery that
/// never reached the receiver (it was down, or its front door answered
/// for it) is not one of its tries, so an outage of the receiver spends
/// none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// 1 when no earlier delivery of the wake was answered, 2 after one
    /// answered failure, and so on.
    pub attempt: u32,
    /// How many answered deliveries of one wake a failing receiver gets:
    /// the platform's config ([`Alarm::attempts`]).
    pub attempts: u32,
    /// Whether the wake was delivered before, answered or not
    /// ([`Alarm::tried_header`]). A receiver that died mid-wake, or ran
    /// past the platform's deadline, answered nothing, so `attempt` can
    /// still be 1 on a delivery that follows one which acted.
    pub redelivered: bool,
}

/// The status a receiver answers a wake it handled and failed, to have it
/// delivered again ("424 Failed Dependency": what it acts through, the
/// broker or a provider, failed). Never a 5xx, which no alarm counts
/// ([`answered_by_receiver`]): a failure answered with one would be
/// retried with no bound.
pub const WAKE_FAILED_STATUS: u16 = 424;

/// Whether a delivery its receiver answered with `status` counts toward
/// [`Delivery::attempt`]. A 5xx does not: it may come from in front of
/// the receiver (a load balancer, a server that never started), so it
/// says nothing about the receiver having run. Every alarm counts as
/// Cloud Tasks' `X-CloudTasks-TaskExecutionCount` does, which leaves 5xx
/// answers out.
pub fn answered_by_receiver(status: u16) -> bool {
    !(500..600).contains(&status)
}

impl Delivery {
    /// Whether a failure on this delivery is the receiver's last: it then
    /// gives up on what the wake was for, records why, and answers the
    /// wake as handled.
    pub fn is_last(&self) -> bool {
        self.attempt >= self.attempts
    }
}

/// How early a wake may arrive and still count as its moment: the
/// alarm's clock and the receiver's never agree to the millisecond. One
/// that arrives earlier is a hop of a wake set past the platform's
/// horizon, or one that went off early: its receiver sets it again for its
/// moment and acts on nothing ([`set_again_if_early`]).
pub const EARLY_WAKE_TOLERANCE: std::time::Duration = std::time::Duration::from_secs(5);

/// Whether a wake aimed at `aimed_at_ms` that arrives at `now_ms` came
/// early ([`EARLY_WAKE_TOLERANCE`]).
pub fn arrived_early(aimed_at_ms: i64, now_ms: i64) -> bool {
    aimed_at_ms - now_ms > EARLY_WAKE_TOLERANCE.as_millis() as i64
}

/// The key of a wake its receiver set again for its moment the
/// `set_again`th time, from the booking's own `key` (`set_again` 0): each
/// is a booking of its own, named apart, since the task being answered
/// still holds the name of the one before. The receiver counts in the
/// wake's body.
pub fn again_key(key: &str, set_again: u32) -> String {
    match set_again {
        0 => key.to_string(),
        n => format!("{key}:again-{n}"),
    }
}

/// A wake aimed at `aimed_at_ms` arrived. If it came early
/// ([`arrived_early`]), set `again()` (the same moment, keyed with
/// [`again_key`] and the body's counter plus one) and answer how that
/// went: the receiver acts on nothing, and answers a failure as one so the
/// platform delivers this wake again. `None`: the wake is due; act on it.
pub async fn set_again_if_early(
    alarm: &dyn Alarm,
    aimed_at_ms: i64,
    again: impl FnOnce() -> anyhow::Result<Wake>,
) -> Option<anyhow::Result<()>> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_millis() as i64;
    if !arrived_early(aimed_at_ms, now_ms) {
        return None;
    }
    Some(match again() {
        Ok(wake) => alarm.set(wake).await,
        Err(e) => Err(e),
    })
}

impl Wake {
    /// The request body an alarm posts for this wake.
    pub fn call(&self) -> WakeCall<serde_json::Value> {
        WakeCall { at_unix_ms: self.at_unix_ms, body: self.body.clone() }
    }

    /// A name for this wake, the same for the same key, time and
    /// generation, and safe as an identifier on every platform (letters,
    /// digits, `-`, `_`; at most 100 characters with any suffix an alarm
    /// adds). The hash leads: a platform that keeps names sorted spreads
    /// them evenly that way, where names led by a time pile up on one end.
    pub fn name(&self) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(format!("{}|{}|{}", self.key, self.at_unix_ms, self.generation).as_bytes());
        let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
        format!("w-{hex}")
    }
}

#[async_trait]
pub trait Alarm: Send + Sync {
    /// The largest [`Wake::body`] this alarm takes, in bytes of JSON, with
    /// room left for the rest of the call. A setter whose body could be
    /// bigger sets a smaller one (a wake that reads its state instead of
    /// carrying it).
    fn largest_body(&self) -> usize;

    async fn set(&self, wake: Wake) -> anyhow::Result<()>;

    /// Drop a wake set earlier (the same key, time and generation), so it
    /// does not go off. A wake already delivered, or never set, is no
    /// failure.
    async fn cancel(&self, wake: &Wake) -> anyhow::Result<()>;

    /// The header every delivery of this alarm's wakes carries how many
    /// earlier deliveries of the same wake its receiver answered in
    /// ([`answered_by_receiver`]; 0 on the first that reaches it).
    fn answered_header(&self) -> &'static str;

    /// The header every delivery of this alarm's wakes carries how many
    /// earlier deliveries of the same wake were tried in, whether the
    /// receiver answered them or not (0 on the first).
    fn tried_header(&self) -> &'static str;

    /// How many answered deliveries of one wake a receiver that keeps
    /// failing gets ([`Delivery::attempts`]), as the platform's config
    /// says.
    fn attempts(&self) -> u32;

    /// Which delivery the call carrying `headers` is. A call without a
    /// readable count in [`Self::answered_header`] and
    /// [`Self::tried_header`] did not come from this alarm, and is
    /// refused.
    fn delivery(&self, headers: &reqwest::header::HeaderMap) -> anyhow::Result<Delivery> {
        let answered = earlier_deliveries(headers, self.answered_header())?;
        let tried = earlier_deliveries(headers, self.tried_header())?;
        Ok(Delivery { attempt: answered.saturating_add(1), attempts: self.attempts(), redelivered: tried > 0 })
    }
}

/// The count of earlier deliveries a wake's call carries in `header`.
fn earlier_deliveries(headers: &reqwest::header::HeaderMap, header: &str) -> anyhow::Result<u32> {
    let value = headers.get(header).ok_or_else(|| anyhow::anyhow!("the wake's call carries no {header} header, so it was not delivered by this install's alarm"))?;
    value
        .to_str()
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .ok_or_else(|| anyhow::anyhow!("the wake's {header} header is {value:?}, not a count of earlier deliveries"))
}

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;
    use parking_lot::Mutex;

    /// Records every wake set and cancelled; delivers nothing (a test calls
    /// the receiver itself with what it finds here). Setting fails while
    /// [`Self::fail_sets`] says so. Takes bodies up to
    /// [`Self::LARGEST_BODY`], or what [`Self::taking_at_most`] says. Like
    /// a platform's alarm, it keeps the first wake set under a name until
    /// that name is cancelled ([`Self::under`]). A delivery's counts are
    /// read from [`Self::ANSWERED_HEADER`], out of [`Self::ATTEMPTS`]
    /// tries, and [`Self::TRIED_HEADER`].
    pub struct FakeAlarm {
        set: Mutex<Vec<Wake>>,
        cancelled: Mutex<Vec<Wake>>,
        taken: Mutex<std::collections::HashMap<String, Wake>>,
        failing: std::sync::atomic::AtomicBool,
        largest_body: usize,
    }

    impl Default for FakeAlarm {
        fn default() -> Self {
            Self {
                set: Mutex::default(),
                cancelled: Mutex::default(),
                taken: Mutex::default(),
                failing: Default::default(),
                largest_body: Self::LARGEST_BODY,
            }
        }
    }

    impl FakeAlarm {
        pub const LARGEST_BODY: usize = 64 * 1024;
        pub const ANSWERED_HEADER: &'static str = "x-fake-wake-answered";
        pub const TRIED_HEADER: &'static str = "x-fake-wake-tried";
        pub const ATTEMPTS: u32 = 3;

        /// The `n`th answered delivery of a wake (1 the first), as this
        /// alarm counts, after only answered ones.
        pub fn delivered(n: u32) -> Delivery {
            Delivery { attempt: n, attempts: Self::ATTEMPTS, redelivered: n > 1 }
        }

        /// A delivery of a wake after `unanswered` deliveries that never
        /// got an answer (the receiver died mid-wake, or ran past the
        /// deadline), and none that did.
        pub fn after_unanswered(unanswered: u32) -> Delivery {
            Delivery { attempt: 1, attempts: Self::ATTEMPTS, redelivered: unanswered > 0 }
        }

        pub fn new() -> Self {
            Self::default()
        }

        /// A fake that takes bodies of at most `bytes`.
        pub fn taking_at_most(bytes: usize) -> Self {
            Self { largest_body: bytes, ..Self::default() }
        }

        pub fn wakes(&self) -> Vec<Wake> {
            self.set.lock().clone()
        }

        /// Every wake set for `key`, oldest first.
        pub fn wakes_for(&self, key: &str) -> Vec<Wake> {
            self.set.lock().iter().filter(|w| w.key == key).cloned().collect()
        }

        /// Every wake cancelled, oldest first.
        pub fn cancelled(&self) -> Vec<Wake> {
            self.cancelled.lock().clone()
        }

        /// The wakes set and not cancelled since.
        pub fn pending(&self) -> Vec<Wake> {
            let cancelled = self.cancelled.lock();
            self.set.lock().iter().filter(|w| !cancelled.iter().any(|c| c.name() == w.name())).cloned().collect()
        }

        /// The wake that goes off under `name`: the first set under it since
        /// it was last cancelled, as a platform keeps it.
        pub fn under(&self, name: &str) -> Option<Wake> {
            self.taken.lock().get(name).cloned()
        }

        /// Make every [`Alarm::set`] fail from now on (`true`) or succeed again.
        pub fn fail_sets(&self, failing: bool) {
            self.failing.store(failing, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl Alarm for FakeAlarm {
        fn largest_body(&self) -> usize {
            self.largest_body
        }

        async fn set(&self, wake: Wake) -> anyhow::Result<()> {
            anyhow::ensure!(!self.failing.load(std::sync::atomic::Ordering::SeqCst), "the fake alarm refuses to set {}", wake.key);
            self.taken.lock().entry(wake.name()).or_insert_with(|| wake.clone());
            self.set.lock().push(wake);
            Ok(())
        }

        async fn cancel(&self, wake: &Wake) -> anyhow::Result<()> {
            self.taken.lock().remove(&wake.name());
            self.cancelled.lock().push(wake.clone());
            Ok(())
        }

        fn answered_header(&self) -> &'static str {
            Self::ANSWERED_HEADER
        }

        fn tried_header(&self) -> &'static str {
            Self::TRIED_HEADER
        }

        fn attempts(&self) -> u32 {
            Self::ATTEMPTS
        }
    }
}


#[cfg(test)]
mod refusal_tests {
    use super::{answered_by_receiver, WakeRefusal, WAKE_FAILED_STATUS};

    /// A receiver's own failure is counted, and still tried again; a 5xx,
    /// which may never have reached the receiver, is not counted.
    #[test]
    fn a_wake_failure_is_counted_and_tried_again_and_a_5xx_is_not_counted() {
        assert!(answered_by_receiver(WAKE_FAILED_STATUS));
        assert_eq!(WakeRefusal::of_status(WAKE_FAILED_STATUS), WakeRefusal::Temporary);
        for status in [200, 204, 400, 404, 429] {
            assert!(answered_by_receiver(status), "{status}");
        }
        for status in [500, 502, 503, 504, 599] {
            assert!(!answered_by_receiver(status), "{status}");
        }
    }

    #[test]
    fn a_server_error_throttle_or_auth_failure_is_tried_again() {
        for status in [500, 502, 503, 599, 408, 429, 401, 403, 404, 405, 409, 499, 302] {
            assert_eq!(WakeRefusal::of_status(status), WakeRefusal::Temporary, "{status}");
        }
    }

    #[test]
    fn only_a_rejected_body_is_permanent() {
        for status in [400, 413, 415, 422] {
            assert_eq!(WakeRefusal::of_status(status), WakeRefusal::Permanent, "{status}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wake(key: &str, at: i64, generation: i64) -> Wake {
        Wake {
            key: key.into(),
            at_unix_ms: at,
            generation,
            queue: WakeQueue::Triggers,
            role: CoreRole::Listener,
            path: "/wake".into(),
            body: serde_json::json!({}),
        }
    }

    /// A wake more than the tolerance before its moment came early; one
    /// inside it, or late, is due. Each time it is set again it is keyed
    /// apart, the booking's own wake keeping its key.
    #[test]
    fn an_early_wake_is_set_again_under_a_key_of_its_own() {
        assert!(arrived_early(10_000, 4_999));
        assert!(!arrived_early(10_000, 5_000));
        assert!(!arrived_early(10_000, 20_000));
        assert_eq!(again_key("loop:x", 0), "loop:x");
        assert_eq!(again_key("loop:x", 2), "loop:x:again-2");
    }

    /// An early wake is set again for its moment and the receiver acts on
    /// nothing; a due one is left to the receiver.
    #[tokio::test]
    async fn only_an_early_wake_is_set_again() {
        let alarm = fake::FakeAlarm::default();
        let far = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64 + 60_000;
        let set = set_again_if_early(&alarm, far, || Ok(wake("k:again-1", far, 1))).await;
        assert!(matches!(set, Some(Ok(()))));
        assert_eq!(alarm.wakes().iter().map(|w| w.key.clone()).collect::<Vec<_>>(), vec!["k:again-1".to_string()]);
        assert!(set_again_if_early(&alarm, 0, || unreachable!("a due wake is not set again")).await.is_none());
    }

    /// The same key, time and generation is the same name; a new
    /// generation of the same key and time is a new one, so a wake set
    /// again after one went off is never refused as already set. The hash
    /// leads, and the name stays well inside the platform's limit.
    #[test]
    fn a_wake_name_differs_per_generation_leads_with_its_hash_and_is_safe_everywhere() {
        assert_eq!(wake("a", 1, 0).name(), wake("a", 1, 0).name());
        assert_ne!(wake("a", 1, 0).name(), wake("a", 2, 0).name());
        assert_ne!(wake("a", 1, 0).name(), wake("b", 1, 0).name());
        assert_ne!(wake("a", 1, 0).name(), wake("a", 1, 1).name());
        let n = wake("sig:@src:x/y z", 1_700_000_000_000, 7).name();
        assert!(n.len() <= 40, "room for the suffixes an alarm adds: {n}");
        assert!(n.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'), "{n}");
        assert!(!n.contains("1700000000000"), "no time in the name: {n}");
    }

    /// A delivery's try is read off the alarm's answered count (earlier
    /// answered deliveries, plus one) and is the last at the platform's
    /// bound; whether it is a redelivery at all is read off the tried
    /// count, so one after a try nobody answered is still a redelivery. A
    /// call without both counts readable is refused.
    #[test]
    fn a_delivery_says_which_try_it_is_whether_it_is_the_last_and_whether_it_came_before() {
        let alarm = fake::FakeAlarm::default();
        let with = |answered: &str, tried: &str| {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(fake::FakeAlarm::ANSWERED_HEADER, answered.parse().unwrap());
            headers.insert(fake::FakeAlarm::TRIED_HEADER, tried.parse().unwrap());
            alarm.delivery(&headers)
        };
        let first = with("0", "0").unwrap();
        assert_eq!(first, Delivery { attempt: 1, attempts: fake::FakeAlarm::ATTEMPTS, redelivered: false });
        assert!(!first.is_last());
        let after_unanswered = with("0", "1").unwrap();
        assert_eq!((after_unanswered.attempt, after_unanswered.redelivered), (1, true), "a try nobody answered spends none, and still came before");
        assert!(!with("1", "1").unwrap().is_last());
        assert!(with("2", "2").unwrap().is_last(), "the third of three tries");
        assert!(with("9", "9").unwrap().is_last());
        assert!(with("many", "0").is_err());
        assert!(with("0", "many").is_err());
        let mut answered_only = reqwest::header::HeaderMap::new();
        answered_only.insert(fake::FakeAlarm::ANSWERED_HEADER, "0".parse().unwrap());
        assert!(alarm.delivery(&answered_only).is_err(), "no tried count");
        assert!(alarm.delivery(&reqwest::header::HeaderMap::new()).is_err());
    }

    #[test]
    fn a_wake_round_trips() {
        let w = wake("k", 5, 3);
        let v = serde_json::to_value(&w).unwrap();
        assert_eq!(serde_json::from_value::<Wake>(v).unwrap(), w);
    }
}
