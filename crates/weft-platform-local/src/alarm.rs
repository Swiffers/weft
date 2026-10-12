//! Wakes on a local install: kept in Postgres (`weft_task_store::alarm`),
//! delivered by a loop in the runtime process.
//!
//! The loop sleeps until the next wake is due (or until a new one is set),
//! claims what is due, and posts each to its role the way a cloud's queue
//! would: `WakeCall` as the body, the install's own identity as the
//! bearer, how many earlier deliveries of the wake the receiver
//! answered in [`ANSWERED_HEADER`] (the row counts them; no answer, or a
//! 5xx, is not counted: `weft_platform_traits::alarm::answered_by_receiver`),
//! so its receiver knows when it is on its last of [`ATTEMPTS`], and how
//! many were tried at all in [`TRIED_HEADER`], so it knows a redelivery
//! after a try it never answered. A
//! receiver that fails for now (a 5xx, 408, 424, 429, an auth or routing
//! 4xx such as 401, 403 or 404, or no answer) gets the wake again later,
//! backing off. A receiver that rejects the request body (400, 413,
//! 415, 422) would reject it forever, so that wake
//! is dropped with one error naming its key, the status and the answer's
//! body ([`WakeRefusal::of_status`]). Every failure logs the answer's body.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;
use weft_platform_traits::{Alarm, CoreRole, IdentityTokens, RoleAddresses, Wake, WakeCall, WakeRefusal};

/// How long a claimed wake is this loop's before it comes due again (a
/// delivery that never finished, the process gone mid-call).
const LEASE_MS: i64 = 5 * 60 * 1000;

/// How many due wakes one pass claims.
const BATCH: i64 = 64;

/// The longest the loop sleeps without looking again, so a wake another
/// process stored is not missed for long.
const LOOK_AGAIN: Duration = Duration::from_secs(60);

/// The most a failing wake waits before its next try.
const MAX_BACKOFF_MS: i64 = 5 * 60 * 1000;

/// How many answered deliveries of one wake a receiver that keeps failing
/// gets (`weft_platform_traits::Delivery`). The backoff above grows with
/// every try, answered or not, so how long the 20 take depends on what
/// came before them: about an hour when the receiver answered every try
/// (waits of 1s doubling to 256s, 511s in all, then ten of 5 minutes),
/// and up to about 95 minutes when an outage already took the backoff to
/// its cap (19 waits of 5 minutes).
const ATTEMPTS: u32 = 20;

/// The header each delivery carries how many earlier deliveries of the
/// same wake its receiver answered in, from the row's own count.
pub const ANSWERED_HEADER: &str = "x-weft-wake-answered";

/// The header each delivery carries how many earlier deliveries of the
/// same wake were tried in, answered or not: the row's claims before this
/// one. A delivery whose process died holding the claim counts here.
pub const TRIED_HEADER: &str = "x-weft-wake-tried";

pub struct LocalAlarm {
    pool: PgPool,
    changed: Arc<tokio::sync::Notify>,
}

impl LocalAlarm {
    pub fn new(pool: PgPool) -> Self {
        Self { pool, changed: Arc::new(tokio::sync::Notify::new()) }
    }

    /// Deliver wakes for as long as the process lives.
    pub async fn run(&self, deliver: Arc<dyn Deliver>) -> anyhow::Result<()> {
        loop {
            let delivered = self.deliver_due(&deliver).await?;
            if delivered as i64 >= BATCH {
                continue;
            }
            let wait = match weft_task_store::alarm::next_due(&self.pool).await? {
                Some(at) => Duration::from_millis((at - now_ms()).max(0) as u64).min(LOOK_AGAIN),
                None => LOOK_AGAIN,
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.changed.notified() => {}
            }
        }
    }

    /// Claim what is due now and deliver it. Returns how many were due.
    pub async fn deliver_due(&self, deliver: &Arc<dyn Deliver>) -> anyhow::Result<usize> {
        let now = now_ms();
        let due = weft_task_store::alarm::claim_due(&self.pool, now, LEASE_MS, BATCH).await?;
        let count = due.len();
        let mut running = tokio::task::JoinSet::new();
        for row in due {
            let deliver = deliver.clone();
            let pool = self.pool.clone();
            running.spawn(async move {
                let role = match CoreRole::parse(&row.role) {
                    Ok(role) => role,
                    Err(e) => {
                        // A row no role can take is dropped loudly: no
                        // retry would ever deliver it.
                        tracing::error!(target: "weft_platform_local::alarm", wake = %row.name, error = %e, "dropping a wake for no known role");
                        return weft_task_store::alarm::delete(&pool, &row.name).await;
                    }
                };
                let call = WakeCall { at_unix_ms: row.set_for_unix_ms, body: row.body.clone() };
                match deliver.deliver(role, &row.path, call, EarlierDeliveries::of(&row)).await {
                    Ok(()) => weft_task_store::alarm::delete(&pool, &row.name).await,
                    Err(NotTaken { refusal: WakeRefusal::Permanent, detail, .. }) => {
                        tracing::error!(
                            target: "weft_platform_local::alarm",
                            wake = %row.name, key = %row.key, attempt = row.attempts, body = %row.body, error = %detail,
                            "dropping a wake its receiver refused as malformed; the same call can never be taken"
                        );
                        weft_task_store::alarm::delete(&pool, &row.name).await
                    }
                    Err(NotTaken { refusal: WakeRefusal::Temporary, detail, answered: by_receiver }) => {
                        let next = now_ms() + backoff_ms(row.attempts);
                        tracing::warn!(
                            target: "weft_platform_local::alarm",
                            wake = %row.name, key = %row.key, attempt = row.attempts, answered = row.answered, error = %detail,
                            "a wake was not taken; trying again later"
                        );
                        weft_task_store::alarm::retry_at(&pool, &row.name, next, by_receiver).await
                    }
                }
            });
        }
        while let Some(done) = running.join_next().await {
            done??;
        }
        Ok(count)
    }
}

/// The largest wake body kept: a row of the install's own `alarm` table,
/// which takes any size; this bound keeps the table, which is read on
/// every delivery, small.
const LARGEST_BODY: usize = 64 * 1024;

#[async_trait]
impl Alarm for LocalAlarm {
    fn largest_body(&self) -> usize {
        LARGEST_BODY
    }

    async fn set(&self, wake: Wake) -> anyhow::Result<()> {
        weft_task_store::alarm::set(
            &self.pool,
            &wake.name(),
            &wake.key,
            wake.at_unix_ms,
            wake.role.as_str(),
            &wake.path,
            &wake.body,
        )
        .await?;
        self.changed.notify_one();
        Ok(())
    }

    async fn cancel(&self, wake: &Wake) -> anyhow::Result<()> {
        weft_task_store::alarm::delete(&self.pool, &wake.name()).await
    }

    fn answered_header(&self) -> &'static str {
        ANSWERED_HEADER
    }

    fn tried_header(&self) -> &'static str {
        TRIED_HEADER
    }

    fn attempts(&self) -> u32 {
        ATTEMPTS
    }
}

/// Why a due wake was not taken, and whether it ever can be.
#[derive(Debug)]
pub struct NotTaken {
    pub refusal: WakeRefusal,
    /// What went wrong, with the receiver's answer body when it gave one.
    pub detail: String,
    /// Whether the receiver answered it
    /// (`weft_platform_traits::alarm::answered_by_receiver`), which counts
    /// it as one of the wake's tries.
    pub answered: bool,
}

impl NotTaken {
    /// A failure with no answer to read (unreachable, no token, the
    /// connection cut): always worth another try, and none of the
    /// receiver's.
    pub fn unanswered(e: impl std::fmt::Display) -> Self {
        Self { refusal: WakeRefusal::Temporary, detail: e.to_string(), answered: false }
    }

    /// The receiver's answer `status`, which was not a success, with
    /// `detail`.
    pub fn answered_with(status: u16, detail: String) -> Self {
        Self {
            refusal: WakeRefusal::of_status(status),
            detail,
            answered: weft_platform_traits::alarm::answered_by_receiver(status),
        }
    }
}

/// How many deliveries of a wake came before the one being made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EarlierDeliveries {
    /// The ones its receiver answered ([`ANSWERED_HEADER`]).
    pub answered: u32,
    /// All of them, answered or not ([`TRIED_HEADER`]).
    pub tried: u32,
}

impl EarlierDeliveries {
    /// From a row just claimed: its claim count already includes the
    /// delivery being made.
    fn of(row: &weft_task_store::alarm::AlarmRow) -> Self {
        let count = |n: i32| u32::try_from(n).expect("an alarm row's counts only ever grow from 0");
        Self { answered: count(row.answered), tried: count(row.attempts) - 1 }
    }
}

/// Hands a due wake to its role, saying how many deliveries of it came
/// before ([`EarlierDeliveries`]).
#[async_trait]
pub trait Deliver: Send + Sync {
    async fn deliver(&self, role: CoreRole, path: &str, call: WakeCall<serde_json::Value>, earlier: EarlierDeliveries) -> Result<(), NotTaken>;
}

/// Posts the wake to the role's internal address with the install's own
/// identity.
pub struct HttpDeliver {
    roles: RoleAddresses,
    tokens: Arc<dyn IdentityTokens>,
    http: reqwest::Client,
}

impl HttpDeliver {
    pub fn new(roles: RoleAddresses, tokens: Arc<dyn IdentityTokens>) -> Self {
        Self { roles, tokens, http: reqwest::Client::new() }
    }
}

#[async_trait]
impl Deliver for HttpDeliver {
    async fn deliver(&self, role: CoreRole, path: &str, call: WakeCall<serde_json::Value>, earlier: EarlierDeliveries) -> Result<(), NotTaken> {
        let base = self.roles.of(role).map_err(|e| NotTaken::unanswered(format!("{e:#}")))?;
        let token = self.tokens.token_for(base).await.map_err(|e| NotTaken::unanswered(format!("{e:#}")))?;
        let url = format!("{base}{path}");
        let answer = self
            .http
            .post(&url)
            .bearer_auth(token)
            .header(ANSWERED_HEADER, earlier.answered)
            .header(TRIED_HEADER, earlier.tried)
            .json(&call)
            .send()
            .await
            .map_err(NotTaken::unanswered)?;
        let status = answer.status();
        if status.is_success() {
            return Ok(());
        }
        let body = answer.text().await.unwrap_or_else(|e| format!("<the answer's body could not be read: {e}>"));
        Err(NotTaken::answered_with(status.as_u16(), format!("{url} answered {status}: {body}")))
    }
}

/// How long a wake that failed `attempts` times waits before the next try:
/// doubling from a second, at most [`MAX_BACKOFF_MS`].
fn backoff_ms(attempts: i32) -> i64 {
    let exp = attempts.clamp(1, 20) as u32 - 1;
    (1000i64 << exp).min(MAX_BACKOFF_MS)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock past UNIX_EPOCH")
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failing_wake_backs_off_doubling_up_to_the_cap() {
        assert_eq!(backoff_ms(1), 1000);
        assert_eq!(backoff_ms(2), 2000);
        assert_eq!(backoff_ms(4), 8000);
        assert_eq!(backoff_ms(40), MAX_BACKOFF_MS);
    }

    /// The span [`ATTEMPTS`] says its tries take: the waits between them,
    /// when every try was answered (the backoff counts from 1), and after
    /// an outage already took the backoff to its cap.
    #[test]
    fn the_answered_tries_take_about_an_hour_or_up_to_95_minutes_after_an_outage() {
        let waits = ATTEMPTS as i32 - 1;
        let answered_every_try: i64 = (1..=waits).map(backoff_ms).sum();
        assert_eq!(answered_every_try, 511_000 + 10 * MAX_BACKOFF_MS);
        assert_eq!(answered_every_try / 60_000, 58, "about an hour");
        let after_an_outage: i64 = (100..100 + waits).map(backoff_ms).sum();
        assert_eq!(after_an_outage, 95 * 60_000);
    }
}
