//! Waking the roles that scale to zero when work is written for them.
//!
//! A role in a local install's one process hears the database announce
//! every write it waits on (`weft_task_store::pg_signal`) and drains the
//! loops it concerns. A role placed serverless hears nothing while it is
//! at zero: it runs only when its tick is called
//! (`weft_platform_traits::roles::TICK_PATH`). Every write is made by a
//! dispatcher or a broker (every other role writes through the broker),
//! and such a process is up while it writes, listening on the database's
//! announcements for its own waiters anyway. So each one also hears the
//! writes, its own and its siblings', and rings the tick of every role
//! that sleeps, naming the loops a write concerns, by the same rules the
//! role's loops wake by in one process. The announcement comes from a
//! trigger on the table, so no writer has to remember to wake anyone; and
//! no process stays up to listen when nothing is being written, so an
//! idle install and its database sleep.
//!
//! It rings every such role, naming every loop a write wakes, whenever
//! notifications may have been lost (its listening connection dropped
//! while it had work). When its watch only went quiet and listens again
//! ([`Heard::Resumed`]), it rings nobody: it listened while it wrote, and
//! what another process wrote meanwhile, that process rang.
//!
//! A ring that fails is handed to the alarm, which calls the role's tick
//! in a moment with the same loops and tries again for as long as its
//! queue's window allows (`weft_task_store::drain::ring_later`); the
//! failure is written down (`weft_task_store::unanswered`), so `weft
//! status` can say why the work waiting on that role does not move, until
//! a ring goes through. A role that answers its ring with a pass that
//! could not write down how it went (its failure carries
//! `crate::server::PASS_FAILED_HEADER`) was reached: nothing says it
//! cannot be, this process writes the pass's failure down for the loops
//! the answer names (`weft_task_store::drain::pass_failed`), and the alarm
//! calls the role again for those loops the same way. When the database is
//! what failed the pass, that write fails too and is only logged: the
//! alarm's call is what carries those loops, and runs them once the
//! database answers. Nothing here tries again on a timer: past the
//! queue's window, the next write that concerns the role, or the next
//! writer to start, rings it again. A process that is stopping waits a few
//! seconds for the rings still out, and logs the ones it gives up on.
//!
//! A ring is coalesced per role: one tick runs at a time, and rings
//! arriving during it are sent as one more once it answers, naming every
//! loop they woke. Two writers that hear the same write each ring once;
//! the tick a write causes twice drains what is there, then nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use weft_broker_client::lifecycle_command::{ISSUED_WAKE, LOOK_WAKE};
use weft_core::net::EmptyBody;
use weft_platform_traits::config::InstallConfig;
use weft_platform_traits::roles::TICK_PATH;
use weft_platform_traits::{Alarm, CoreRole, IdentityTokens, Placement, RoleAddresses};
use weft_task_store::drain::WakeOn;
use weft_task_store::pg_signal::{Heard, Subscription};
use weft_task_store::unanswered::{self, Callee};

use crate::server::{PASS_FAILED_HEADER, TICK_LOOP_PARAM};

/// The supervisor's one pass, woken by a lifecycle command being issued, or
/// by a machine running a project's infra saying how it stands changed.
const SUPERVISOR_WAKES: &[(&str, &[WakeOn])] = &[("supervisor", &[ISSUED_WAKE, LOOK_WAKE])];

/// What wakes each of `role`'s loops, by loop name; the listener and the
/// holder have no loops at all.
fn loop_wakes(role: CoreRole) -> Vec<(&'static str, &'static [WakeOn])> {
    match role {
        CoreRole::Dispatcher => weft_dispatcher::app::loop_wakes(),
        CoreRole::Broker => weft_broker::loop_wakes(),
        CoreRole::Supervisor => SUPERVISOR_WAKES.to_vec(),
        CoreRole::Listener | CoreRole::Holder => Vec::new(),
    }
}

/// The loops of `wakes` a notification on `channel` with `payload` is
/// work for.
fn woken_by(wakes: &[(&'static str, &[WakeOn])], channel: &str, payload: &str) -> BTreeSet<String> {
    wakes
        .iter()
        .filter(|(_, wake_on)| wake_on.iter().any(|w| w.hears(channel, payload)))
        .map(|(name, _)| name.to_string())
        .collect()
}

/// Every loop of `role` a write wakes, by name.
fn every_loop(role: CoreRole) -> BTreeSet<String> {
    loop_wakes(role).iter().map(|(name, _)| name.to_string()).collect()
}

#[derive(Default)]
struct Ring {
    ringing: bool,
    /// Loops woken while a ring was out, sent as one more once it answers.
    pending: Option<BTreeSet<String>>,
    /// A failure of this role's ring is written down and not answered yet,
    /// as far as this process knows: the next ring that goes through
    /// clears it.
    owed: bool,
}

struct Bell {
    role: CoreRole,
    url: String,
    audience: String,
    ring: Mutex<Ring>,
}

pub struct RoleWaker {
    bells: BTreeMap<CoreRole, Arc<Bell>>,
    tokens: Arc<dyn IdentityTokens>,
    /// Where a ring that did not go through is handed, to call the role later.
    alarm: Arc<dyn Alarm>,
    /// Where a ring that keeps failing is written down.
    pool: weft_task_store::db::Pool,
    http: reqwest::Client,
    /// How long a stopping process waits for its rings still out
    /// ([`Self::settle`]): the platform's
    /// `crate::platform::ServerlessLimits::ring_grace`.
    grace: Duration,
    /// Told whenever a ring is answered and none is left for its role, so
    /// a stopping process can wait for the rings still out ([`Self::settle`]).
    answered: Arc<tokio::sync::Notify>,
}

impl RoleWaker {
    /// A bell for every serverless role with loops to wake, at its
    /// address as this process reaches it. `None` when none sleeps.
    pub fn new(
        config: &InstallConfig,
        addresses: &RoleAddresses,
        tokens: Arc<dyn IdentityTokens>,
        alarm: Arc<dyn Alarm>,
        pool: weft_task_store::db::Pool,
    ) -> anyhow::Result<Option<Self>> {
        let mut bells = BTreeMap::new();
        // The listener has no loops to drain, so no tick to ring; the
        // holder is never serverless.
        for role in CoreRole::ALL
            .into_iter()
            .filter(|r| *r != CoreRole::Listener && config.roles.of(*r) == Placement::Serverless)
        {
            let base = addresses.of(role)?.to_string();
            let url = format!("{}{TICK_PATH}", base.trim_end_matches('/'));
            bells.insert(role, Arc::new(Bell { role, url, audience: base, ring: Mutex::new(Ring::default()) }));
        }
        if bells.is_empty() {
            return Ok(None);
        }
        let limits = crate::platform::serverless_limits(config)
            .ok_or_else(|| anyhow::anyhow!("a role is placed serverless, which this install's platform does not do"))?;
        Ok(Some(Self { bells, tokens, alarm, pool, http: weft_core::net::http_client().build()?, answered: Arc::default(), grace: limits.ring_grace }))
    }

    /// Every channel a sleeping role's loops wait on: what this process's
    /// watch has to listen on for the waker.
    pub fn channels(&self) -> Vec<&'static str> {
        self.bells.keys().flat_map(|r| loop_wakes(*r).into_iter().flat_map(|(_, w)| w.iter().map(|w| w.channel))).collect()
    }

    /// Ring on what the watch hears, for the process's whole life.
    pub async fn run(self: Arc<Self>, mut heard: Subscription) -> anyhow::Result<()> {
        // A ring written down as failing is still owed.
        match unanswered::unanswered_roles(&self.pool).await {
            Ok(roles) => {
                for role in roles.iter().filter_map(|r| self.bells.keys().find(|b| b.as_str() == r.as_str()).copied()) {
                    if let Some(bell) = self.bells.get(&role) {
                        bell.ring.lock().owed = true;
                    }
                    self.ring(role, every_loop(role));
                }
            }
            Err(e) => tracing::warn!(target: "weft_runtime::role_waker", error = %format!("{e:#}"), "could not read the rings left unanswered; their roles run at their own next wake"),
        }
        loop {
            match heard.next().await? {
                Heard::Signal { channel, payload } => {
                    for role in self.bells.keys() {
                        let woken = woken_by(&loop_wakes(*role), channel, &payload);
                        if !woken.is_empty() {
                            self.ring(*role, woken);
                        }
                    }
                }
                // Notifications may have been lost.
                Heard::Recheck => self.ring_every_loop(),
                // Quiet, then listening again: this process heard its own
                // writes, and another's were rung by that process.
                Heard::Resumed => {}
                // The recheck after the reconnect rings every loop.
                Heard::Lost => {}
            }
        }
    }

    /// Ring every sleeping role, naming each of its loops that a write
    /// wakes, so each looks whether or not its next look is due: whenever
    /// notifications may have been lost.
    fn ring_every_loop(&self) {
        for role in self.bells.keys() {
            self.ring(*role, every_loop(*role));
        }
    }

    /// Wait, its grace at most, for the rings still out to be answered,
    /// for a process that is stopping; log the roles whose ring it gives up on
    /// (each runs when the next write rings it, or at its own booked look).
    pub async fn settle(&self) {
        let deadline = tokio::time::Instant::now() + self.grace;
        loop {
            // Listening before looking, so an answer between the two is
            // not missed.
            let answered = self.answered.notified();
            tokio::pin!(answered);
            answered.as_mut().enable();
            let out: Vec<CoreRole> = self.bells.values().filter(|b| b.ring.lock().ringing).map(|b| b.role).collect();
            if out.is_empty() {
                return;
            }
            if tokio::time::timeout_at(deadline, answered).await.is_err() {
                tracing::warn!(
                    target: "weft_runtime::role_waker", dropped = out.len(), roles = ?out,
                    "stopping with rings not answered; those roles run when the next write rings them"
                );
                return;
            }
        }
    }

    fn ring(&self, role: CoreRole, woken: BTreeSet<String>) {
        let Some(bell) = self.bells.get(&role).cloned() else { return };
        {
            let mut ring = bell.ring.lock();
            if ring.ringing {
                ring.pending.get_or_insert_with(BTreeSet::new).extend(woken);
                return;
            }
            ring.ringing = true;
        }
        let (tokens, http, answered, pool, alarm) =
            (self.tokens.clone(), self.http.clone(), self.answered.clone(), self.pool.clone(), self.alarm.clone());
        tokio::spawn(async move {
            let mut woken = woken;
            let callee = Callee::Role(bell.role.as_str());
            loop {
                let sent = async {
                    let token = tokens.token_for(&bell.audience).await?;
                    let query: Vec<(&str, &str)> = woken.iter().map(|name| (TICK_LOOP_PARAM, name.as_str())).collect();
                    let answer = http.post(&bell.url).query(&query).bearer_auth(token).empty_body().send().await?;
                    if !answer.status().is_success() && answer.headers().contains_key(PASS_FAILED_HEADER) {
                        let status = answer.status();
                        let body = answer.bytes().await.map_err(|e| anyhow::anyhow!("the role answered {status} for a failed pass, its body unread: {e}"))?;
                        let failed: weft_task_store::drain::PassFailed = serde_json::from_slice(&body).map_err(|e| {
                            anyhow::anyhow!("the role answered {status} for a failed pass without saying which loops ({e}): {}", String::from_utf8_lossy(&body))
                        })?;
                        return anyhow::Ok(Some(failed));
                    }
                    answer.error_for_status()?;
                    anyhow::Ok(None)
                };
                let sent = sent.await;
                if let Ok(Some(failed)) = &sent {
                    // The role answered, so it is reached; its pass could
                    // not write down how it went for the loops it names,
                    // so this process does, and the alarm's queue tries
                    // those loops again shortly.
                    tracing::warn!(target: "weft_runtime::role_waker", role = %bell.role, error = %failed, "a role's pass failed; the alarm calls it again shortly");
                    if let Err(e) = weft_task_store::drain::pass_failed(&pool, bell.role, failed).await {
                        tracing::error!(target: "weft_runtime::role_waker", role = %bell.role, error = %format!("{e:#}"), "could not write down a role's failed pass");
                    }
                    if let Err(e) = weft_task_store::drain::ring_later(alarm.as_ref(), bell.role, failed.loops.clone()).await {
                        tracing::error!(
                            target: "weft_runtime::role_waker", role = %bell.role, error = %format!("{e:#}"),
                            "could not hand a failed pass to the alarm; its loops run when the next write rings them"
                        );
                    }
                }
                if let Err(e) = sent {
                    // Whatever was rung meanwhile goes with it.
                    if let Some(more) = bell.ring.lock().pending.take() {
                        woken.extend(more);
                    }
                    let error = format!("{e:#}");
                    tracing::warn!(target: "weft_runtime::role_waker", role = %bell.role, %error, "could not wake a role; the alarm calls it again shortly");
                    bell.ring.lock().owed = true;
                    match unanswered::failed(&pool, callee, None, &error).await {
                        Ok(alerts) => alerts.log(),
                        Err(e) => {
                            tracing::warn!(target: "weft_runtime::role_waker", role = %bell.role, error = %format!("{e:#}"), "could not write down a ring that failed");
                        }
                    }
                    if let Err(e) = weft_task_store::drain::ring_later(alarm.as_ref(), bell.role, woken.into_iter().collect()).await {
                        tracing::error!(
                            target: "weft_runtime::role_waker", role = %bell.role, error = %format!("{e:#}"),
                            "could not hand a failed ring to the alarm; the role runs when the next write rings it"
                        );
                    }
                    let mut ring = bell.ring.lock();
                    ring.ringing = false;
                    answered.notify_waiters();
                    break;
                }
                if std::mem::take(&mut bell.ring.lock().owed) {
                    if let Err(e) = unanswered::answered(&pool, callee).await {
                        bell.ring.lock().owed = true;
                        tracing::warn!(target: "weft_runtime::role_waker", role = %bell.role, error = %format!("{e:#}"), "could not clear a ring that failed before");
                    }
                }
                let mut ring = bell.ring.lock();
                match ring.pending.take() {
                    Some(more) => woken = more,
                    None => {
                        ring.ringing = false;
                        answered.notify_waiters();
                        break;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_broker_client::lifecycle_command::INFRA_COMMAND_CHANNEL;

    #[test]
    fn a_write_wakes_only_the_loops_it_concerns() {
        let dispatcher = loop_wakes(CoreRole::Dispatcher);
        assert_eq!(
            woken_by(&dispatcher, weft_task_store::tasks::TASK_READY_CHANNEL, "dispatcher"),
            BTreeSet::from(["dispatcher_picker".to_string()])
        );
        assert_eq!(
            woken_by(&dispatcher, weft_task_store::runs::RUN_QUEUED_CHANNEL, &uuid::Uuid::nil().to_string()),
            BTreeSet::from(["delivery".to_string()]),
            "a run queued is handed out"
        );

        let issued = format!("issued:{}", uuid::Uuid::nil());
        assert_eq!(woken_by(&loop_wakes(CoreRole::Supervisor), INFRA_COMMAND_CHANNEL, &issued), BTreeSet::from(["supervisor".to_string()]));
        assert!(woken_by(&loop_wakes(CoreRole::Supervisor), INFRA_COMMAND_CHANNEL, "done:7").is_empty(), "a command's end wakes nobody");
        assert!(woken_by(&loop_wakes(CoreRole::Broker), weft_task_store::tasks::TASK_READY_CHANNEL, "dispatcher").is_empty());
        assert_eq!(
            woken_by(&loop_wakes(CoreRole::Broker), weft_broker::runtime_store::FILE_EXPIRY_CHANNEL, ""),
            BTreeSet::from(["runtime_file_expiry".to_string()])
        );
    }

    /// Ring the dispatcher's two loops at a role that answers every ring
    /// with `answer`, with a database nobody serves (writing anything down
    /// fails, which is only logged), until the ring settles: the alarm's
    /// wakes, and whether a failure of the ring is owed.
    async fn ring_answered_with(answer: String) -> (Vec<weft_platform_traits::Wake>, bool) {
        let alarm = Arc::new(weft_platform_traits::FakeAlarm::new());
        let role = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = role.local_addr().unwrap();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let (mut socket, _) = role.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(answer.as_bytes()).await;
            }
        });
        let nobody = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        let pool = weft_task_store::db::Pool::open(
            weft_task_store::db::config(&format!("postgres://weft@{nobody}/none")).unwrap(),
            1,
            Duration::from_millis(500),
            weft_task_store::db::Waits::Expected,
        );
        let bell = Arc::new(Bell {
            role: CoreRole::Dispatcher,
            url: format!("http://{address}/_weft/tick"),
            audience: format!("http://{address}"),
            ring: Mutex::new(Ring::default()),
        });
        let waker = RoleWaker {
            bells: BTreeMap::from([(CoreRole::Dispatcher, bell.clone())]),
            tokens: Arc::new(weft_platform_traits::FixedToken("t".into())),
            alarm: alarm.clone(),
            pool,
            http: weft_core::net::http_client().build().expect("reqwest client"),
            answered: Arc::default(),
            grace: Duration::from_secs(10),
        };
        waker.ring(CoreRole::Dispatcher, BTreeSet::from(["delivery".to_string(), "run_ends".to_string()]));
        waker.settle().await;
        let owed = bell.ring.lock().owed;
        (alarm.wakes(), owed)
    }

    /// The one wake a failed ring or pass hands the alarm: for the
    /// dispatcher's tick, with `loops`, marked so the tick that takes it
    /// answers the ring.
    fn assert_carried(wakes: &[weft_platform_traits::Wake], loops: &[&str]) {
        assert_eq!(wakes.len(), 1, "{wakes:?}");
        assert_eq!(wakes[0].role, CoreRole::Dispatcher);
        assert_eq!(wakes[0].queue, weft_platform_traits::WakeQueue::Internal);
        let body: weft_task_store::drain::TickBody = serde_json::from_value(wakes[0].body.clone()).unwrap();
        assert_eq!(body.loops, loops);
        assert!(body.after_failed_ring);
    }

    /// A ring that does not get through is handed to the alarm once, with
    /// every loop it named, marked so the tick that takes it answers the
    /// ring; nothing here tries again on a timer.
    #[tokio::test]
    async fn a_ring_that_fails_is_handed_to_the_alarm_once_with_its_loops() {
        let (wakes, owed) = ring_answered_with("HTTP/1.1 503 X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".into()).await;
        assert_carried(&wakes, &["delivery", "run_ends"]);
        assert!(owed, "the next ring that goes through clears what was written down");
    }

    /// A role that answers with a pass that could not write down how it
    /// went for some of its loops was reached: nothing is owed for its
    /// ring, and the alarm calls it again for the loops the answer names
    /// only, not those that drained.
    #[tokio::test]
    async fn a_role_whose_pass_failed_was_reached_and_is_called_again_for_the_loops_it_names() {
        let failed = serde_json::to_string(&weft_task_store::drain::PassFailed { loops: vec!["run_ends".into()], error: "down".into() }).unwrap();
        let (wakes, owed) = ring_answered_with(format!(
            "HTTP/1.1 500 X\r\n{PASS_FAILED_HEADER}: 1\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{failed}",
            failed.len()
        ))
        .await;
        assert_carried(&wakes, &["run_ends"]);
        assert!(!owed, "a role that answered is not one that cannot be reached");
    }

    /// A failed pass whose answer does not say which loops failed is a
    /// ring that did not go through: written down for the role, and handed
    /// to the alarm with every loop the ring named.
    #[tokio::test]
    async fn a_failed_pass_that_names_no_loops_is_a_failed_ring() {
        let (wakes, owed) =
            ring_answered_with(format!("HTTP/1.1 500 X\r\n{PASS_FAILED_HEADER}: 1\r\ncontent-length: 4\r\nconnection: close\r\n\r\ndown")).await;
        assert_carried(&wakes, &["delivery", "run_ends"]);
        assert!(owed);
    }
}
