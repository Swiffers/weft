//! The domains an install answers at (`weft domain add|list|rm`).
//!
//! Kept in the install's database rather than its config file: the CLI
//! adds one over HTTP, wherever the dispatcher runs; the platform's door
//! (`weft_platform_traits::DomainHosting`) holds a certificate for each,
//! and the dispatcher's own door (`crate::door`) routes by them. A
//! project's domains go with it.
//!
//! The platform's door follows the stored domains on its own: every row
//! that comes or goes announces itself ([`DOMAINS_CHANNEL`]), and
//! [`drain_loop`] puts the door on what is stored then, retrying while
//! the platform refuses. So a change whose request was cut off, failed
//! halfway, or never touched the door itself (a project removed with
//! its domains) still ends with the door matching the rows, and nothing
//! billed stands for a domain nobody keeps. `weft domain add` and `rm`
//! also change the door themselves, to answer with the outcome.
//!
//! A loop of its own ([`certificates_loop`]) records whether the door
//! holds each name's certificate yet, asking the platform only while a
//! recent one is still pending. An
//! install domain becomes the address the install hands out in its links
//! the moment its certificate is recorded issued
//! (`weft_task_store::install_address`).

use std::net::IpAddr;
use std::time::Duration;

use anyhow::Result;
use weft_core::install::{Certificate, Domain, DomainServes};
use weft_platform_traits::DomainHosting;
use weft_task_store::db::{self, Client as _, Pool};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn, DB_NOW_MS};

use crate::state::DispatcherState;

/// Announced when a domain row comes or goes (payload empty), or its
/// certificate changes (payload [`CERTIFICATE_CHANGED`]).
pub use weft_task_store::install_address::DOMAINS_CHANNEL;

/// The payload of a notice that only a certificate changed: the door
/// serves the same names, so its loop is not woken by it.
// SYNC: CERTIFICATE_CHANGED <-> GROUP's install_domain_notify (below)
const CERTIFICATE_CHANGED: &str = "certificate";

pub(crate) static ON_DOMAINS: &[WakeOn] = &[WakeOn { channel: DOMAINS_CHANNEL, concerns: |payload| payload != CERTIFICATE_CHANGED }];

// SYNC: install_domain <-> crates/weft-core/src/install.rs (Domain, Certificate), crates/weft-task-store/src/install_address.rs (live_install_domain)
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "install_domain",
    tables: &["install_domain", "install_domain_door"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS install_domain (
            -- Lower case, as `weft_core::install::normalize_domain_name`
            -- leaves it.
            name TEXT PRIMARY KEY,
            -- What it serves: 'install', 'frontend' or 'api'.
            serves TEXT NOT NULL CHECK (serves IN ('install', 'frontend', 'api')),
            -- The project a frontend or API domain belongs to.
            project_id UUID REFERENCES project(id) ON DELETE CASCADE,
            -- Where a frontend runs (its service's https address).
            upstream TEXT,
            added_unix BIGINT NOT NULL,
            -- Where the door's certificate for it stands: 'pending' until
            -- it is issued, 'issued', or 'failed' once the authority gave
            -- up (removing it and adding it again asks anew).
            certificate TEXT NOT NULL DEFAULT 'pending' CHECK (certificate IN ('pending', 'issued', 'failed')),
            -- Why it is not issued, as the platform says it.
            certificate_why TEXT,
            CHECK ((serves = 'install') = (project_id IS NULL)),
            CHECK ((serves = 'frontend') = (upstream IS NOT NULL))
        )"#,
        // Every row that comes or goes, a project's removal included,
        // wakes the loop that puts the platform's door on the rows; a
        // certificate that changes says so, so every copy of the install's
        // address reads it again and the door's loop sleeps on.
        // SYNC: 'weft_install_domains' <-> DOMAINS_CHANNEL (above), 'certificate' <-> CERTIFICATE_CHANGED (above)
        r#"CREATE OR REPLACE FUNCTION install_domain_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_install_domains', CASE TG_OP WHEN 'UPDATE' THEN 'certificate' ELSE '' END);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql"#,
        // What the platform's door last did: the names it last accepted
        // and when (so the pass a change wakes right after it served them
        // asks the platform nothing again), and, while it keeps refusing,
        // why and how many times in a row (so the retries back off, and
        // `weft domain list` says so). One row, and only ever a hint: a
        // pass that finds it stale, or cannot read it, asks the platform.
        r#"CREATE TABLE IF NOT EXISTS install_domain_door (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            served TEXT[] NOT NULL,
            -- When the door last accepted `served` (unix ms, the
            -- database's clock).
            served_at_ms BIGINT NOT NULL,
            refused TEXT,
            refusals INT NOT NULL
        )"#,
        r#"DROP TRIGGER IF EXISTS install_domain_changed ON install_domain"#,
        r#"CREATE TRIGGER install_domain_changed
            AFTER INSERT OR DELETE OR UPDATE OF certificate, certificate_why ON install_domain
            FOR EACH ROW
            EXECUTE FUNCTION install_domain_notify()"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// A stored domain, with where the door's certificate for it stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub domain: Domain,
    pub certificate: Certificate,
    /// When it was stored (unix seconds).
    pub added_unix: i64,
}

struct Row {
    name: String,
    serves: String,
    project_id: Option<uuid::Uuid>,
    upstream: Option<String>,
    added_unix: i64,
    certificate: String,
    certificate_why: Option<String>,
}

impl db::FromRow for Row {
    fn from_row(row: db::Row) -> db::Result<Self> {
        Ok(Self {
            name: row.try_get("name")?,
            serves: row.try_get("serves")?,
            project_id: row.try_get("project_id")?,
            upstream: row.try_get("upstream")?,
            added_unix: row.try_get("added_unix")?,
            certificate: row.try_get("certificate")?,
            certificate_why: row.try_get("certificate_why")?,
        })
    }
}

impl Row {
    fn into_stored(self) -> Result<Stored> {
        let serves = match (self.serves.as_str(), self.project_id, self.upstream) {
            ("install", None, None) => DomainServes::Install,
            ("frontend", Some(project), Some(upstream)) => DomainServes::Frontend { project, upstream },
            ("api", Some(project), None) => DomainServes::Api { project },
            (other, _, _) => anyhow::bail!("the stored domain '{}' has an unreadable shape ('{other}')", self.name),
        };
        let certificate = match (self.certificate.as_str(), self.certificate_why) {
            ("issued", _) => Certificate::Issued,
            ("pending", why) => Certificate::Pending { why },
            ("failed", Some(why)) => Certificate::Failed { why },
            ("failed", None) => Certificate::Failed { why: "the platform gave no reason".into() },
            (other, _) => anyhow::bail!("the stored domain '{}' has an unreadable certificate state ('{other}')", self.name),
        };
        Ok(Stored { domain: Domain { name: self.name, serves }, certificate, added_unix: self.added_unix })
    }
}

/// How `certificate` is stored: its state and why.
fn certificate_columns(certificate: &Certificate) -> (&'static str, Option<&str>) {
    match certificate {
        Certificate::Issued => ("issued", None),
        Certificate::Pending { why } => ("pending", why.as_deref()),
        Certificate::Failed { why } => ("failed", Some(why.as_str())),
    }
}

/// What storing a domain did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Added {
    /// It is stored now.
    New,
    /// Exactly this domain was stored already.
    AlreadyStored,
    /// The name is stored for something else, which it never quietly
    /// moves from.
    TakenForSomethingElse,
}

/// Store `domain`.
pub async fn add(pool: &Pool, domain: &Domain, now_unix: i64) -> Result<Added> {
    domain.validate().map_err(anyhow::Error::msg)?;
    let (serves, project, upstream) = match &domain.serves {
        DomainServes::Install => ("install", None, None),
        DomainServes::Frontend { project, upstream } => ("frontend", Some(*project), Some(upstream.as_str())),
        DomainServes::Api { project } => ("api", Some(*project), None),
    };
    let added = pool
        .execute(
            "INSERT INTO install_domain (name, serves, project_id, upstream, added_unix) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (name) DO NOTHING",
            &[&domain.name, &serves, &project, &upstream, &now_unix],
        )
        .await?;
    if added == 1 {
        return Ok(Added::New);
    }
    Ok(match list(pool).await?.iter().any(|stored| stored == domain) {
        true => Added::AlreadyStored,
        false => Added::TakenForSomethingElse,
    })
}

/// Every stored domain, by name.
pub async fn list(pool: &Pool) -> Result<Vec<Domain>> {
    Ok(list_stored(pool).await?.into_iter().map(|stored| stored.domain).collect())
}

/// Every stored domain with its certificate, by name.
pub async fn list_stored(pool: &Pool) -> Result<Vec<Stored>> {
    pool.all::<Row>(
        "SELECT name, serves, project_id, upstream, added_unix, certificate, certificate_why FROM install_domain ORDER BY name",
        &[],
    )
    .await?
    .into_iter()
    .map(Row::into_stored)
    .collect()
}

/// Record where each name's certificate stands. A row whose record says
/// that already is left alone, so nothing is announced for it.
async fn record_certificates(pool: &Pool, certificates: &std::collections::BTreeMap<String, Certificate>) -> Result<()> {
    let (mut names, mut states, mut whys) = (Vec::new(), Vec::new(), Vec::new());
    for (name, certificate) in certificates {
        let (state, why) = certificate_columns(certificate);
        names.push(name.as_str());
        states.push(state);
        whys.push(why);
    }
    pool.execute(
        "UPDATE install_domain d SET certificate = c.state, certificate_why = c.why \
         FROM UNNEST($1::text[], $2::text[], $3::text[]) AS c(name, state, why) \
         WHERE d.name = c.name AND (d.certificate, d.certificate_why) IS DISTINCT FROM (c.state, c.why)",
        &[&names, &states, &whys],
    )
    .await?;
    Ok(())
}

/// Ask the platform about every certificate in `stored` still pending
/// that `ask` picks, record what it says, and answer `stored` as it
/// stands now.
pub async fn check_certificates(
    pool: &Pool,
    hosting: &dyn DomainHosting,
    mut stored: Vec<Stored>,
    ask: impl Fn(&Stored) -> bool,
) -> Result<Vec<Stored>> {
    let pending: Vec<String> = stored
        .iter()
        .filter(|s| matches!(s.certificate, Certificate::Pending { .. }) && ask(s))
        .map(|s| s.domain.name.clone())
        .collect();
    if pending.is_empty() {
        return Ok(stored);
    }
    let certificates = hosting.certified(&pending).await?;
    record_certificates(pool, &certificates).await?;
    for s in &mut stored {
        if let Some(certificate) = certificates.get(&s.domain.name) {
            s.certificate = certificate.clone();
        }
    }
    Ok(stored)
}

/// Remove `name`. `false` when it was not stored.
pub async fn remove(pool: &Pool, name: &str) -> Result<bool> {
    Ok(pool.execute("DELETE FROM install_domain WHERE name = $1", &[&name]).await? > 0)
}

/// Why the platform's door could not be put on the stored domains.
#[derive(Debug)]
pub enum ServeError {
    /// Reading or writing the stored domains.
    Store(anyhow::Error),
    /// The platform's door.
    Door(anyhow::Error),
}

/// Put the platform's door on every stored domain, and answer its address
/// (`None` once no domain is left). Callers hold the domains' lock
/// (`crate::lease::DOMAINS_DOMAIN`), so the door serves what is stored.
pub async fn serve_stored(pool: &Pool, hosting: &dyn DomainHosting) -> Result<Option<IpAddr>, ServeError> {
    let domains = list(pool).await.map_err(ServeError::Store)?;
    serve_domains(pool, hosting, &domains).await
}

/// What the door's record says it serves for `domain`: its name, and the
/// project an API domain is sent to (the door routes it there itself).
fn served_as(domain: &Domain) -> String {
    match &domain.serves {
        DomainServes::Api { project } => format!("{} api {project}", domain.name),
        DomainServes::Install | DomainServes::Frontend { .. } => domain.name.clone(),
    }
}

/// Put the door on exactly `domains` (sorted by name), for a change a
/// person made, and record what it did. The record is a hint the next
/// pass reads, so failing to write it is logged and changes nothing about
/// the answer the person gets; [`settle`] writes it strictly.
async fn serve_domains(pool: &Pool, hosting: &dyn DomainHosting, domains: &[Domain]) -> Result<Option<IpAddr>, ServeError> {
    let served_as: Vec<String> = domains.iter().map(served_as).collect();
    let (served, recorded) = match hosting.serve(domains).await {
        Ok(address) => (Ok(address), record_served(pool, &served_as).await),
        Err(refused) => {
            let recorded = record_refused(pool, &format!("{refused:#}")).await;
            (Err(ServeError::Door(refused)), recorded)
        }
    };
    if let Err(e) = recorded {
        tracing::warn!(target: "weft_dispatcher::domains", error = %format!("{e:#}"), "could not record what the door did; the next pass asks the platform again");
    }
    served
}

/// What the platform's door last did (`install_domain_door`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DoorState {
    /// The names it last accepted, sorted, and how long ago (ms, the
    /// database's clock); `None` before it was ever asked.
    pub served: Option<(Vec<String>, i64)>,
    /// Why it refused, while it keeps refusing.
    pub refused: Option<String>,
    /// How many times in a row it refused.
    pub refusals: i32,
}

pub async fn door_state(pool: &Pool) -> Result<DoorState> {
    let row: Option<(Vec<String>, i64, Option<String>, i32)> =
        pool.opt(&format!("SELECT served, {DB_NOW_MS} - served_at_ms, refused, refusals FROM install_domain_door"), &[]).await?;
    Ok(row.map_or_else(DoorState::default, |(served, ago, refused, refusals)| DoorState {
        served: Some((served, ago)),
        refused,
        refusals,
    }))
}


async fn record_served(pool: &Pool, names: &[String]) -> Result<()> {
    pool.execute(
        &format!(
            "INSERT INTO install_domain_door (singleton, served, served_at_ms, refused, refusals) VALUES (TRUE, $1, {DB_NOW_MS}, NULL, 0) \
             ON CONFLICT (singleton) DO UPDATE SET served = EXCLUDED.served, served_at_ms = EXCLUDED.served_at_ms, \
                 refused = NULL, refusals = 0"
        ),
        &[&names],
    )
    .await?;
    Ok(())
}

/// Record a refusal. What the door last accepted stays: it may still
/// stand.
async fn record_refused(pool: &Pool, why: &str) -> Result<()> {
    pool.execute(
        "INSERT INTO install_domain_door (singleton, served, served_at_ms, refused, refusals) VALUES (TRUE, '{}', 0, $1, 1) \
         ON CONFLICT (singleton) DO UPDATE SET refused = EXCLUDED.refused, refusals = install_domain_door.refusals + 1",
        &[&why],
    )
    .await?;
    Ok(())
}

/// How long after the door accepted the stored domains a pass trusts the
/// record instead of asking the platform: the pass the same change woke,
/// which runs right after it. Past that, a pass asks, so a door changed
/// outside the install is put back at its next look.
const SERVED_JUST_NOW: Duration = Duration::from_secs(60);

/// Put the door on the stored domains now that `added` is one of them
/// (`new` when this add stored it). When the door refuses a domain this
/// add stored, it is forgotten again, and [`drain_loop`] puts the door
/// back on the domains that remain (taking it down when none does).
pub async fn serve_added(pool: &Pool, hosting: &dyn DomainHosting, added: &str, new: bool) -> Result<IpAddr, ServeError> {
    let refused = match serve_stored(pool, hosting).await {
        Ok(Some(address)) => return Ok(address),
        Ok(None) => anyhow::anyhow!("the platform answered no door for a stored domain"),
        Err(ServeError::Door(e)) => e,
        Err(store) => return Err(store),
    };
    if !new {
        // Stored before this add: it stays, and adding it again retries.
        return Err(ServeError::Door(refused));
    }
    if let Err(e) = remove(pool, added).await {
        return Err(ServeError::Door(anyhow::anyhow!(
            "{refused:#}; forgetting '{added}' again failed too ({e:#}), so it stays stored: \
             `weft domain rm {added}` forgets it"
        )));
    }
    Err(ServeError::Door(refused.context(format!(
        "'{added}' is forgotten again; the door goes back to the domains that remain on its own"
    ))))
}

/// Stop answering at `name`, one of `stored`, and forget it. The door
/// goes first: if it refuses, `name` stays stored and removing it again
/// retries the whole step.
pub async fn unserve(pool: &Pool, hosting: &dyn DomainHosting, stored: &[Domain], name: &str) -> Result<(), ServeError> {
    let rest: Vec<Domain> = stored.iter().filter(|d| d.name != name).cloned().collect();
    serve_domains(pool, hosting, &rest).await?;
    remove(pool, name).await.map_err(ServeError::Store)?;
    Ok(())
}

/// How many refusals in a row the platform's door gets before the loop
/// stops trying: a refusal that keeps coming needs a person, and
/// `weft domain add` of a stored domain tries again.
const DOOR_TRIES: i32 = 3;

/// How soon the door is put on the stored domains again after one
/// refusal, in real time: also the loop's safety interval.
const DOOR_RETRY_FIRST: Duration = Duration::from_secs(300);

/// How soon the door is put on the stored domains again after the
/// platform refused `refusals` times in a row, at this install's pace:
/// [`DOOR_RETRY_FIRST`], doubling.
fn door_retry(refusals: i32) -> Duration {
    let first = weft_core::time_scale::scaled(DOOR_RETRY_FIRST);
    let doublings = u32::try_from(refusals.saturating_sub(1).clamp(0, DOOR_TRIES)).unwrap_or(0);
    first.saturating_mul(1 << doublings)
}

/// Put the platform's door on what is stored, when the rows change, one
/// change to the domains at a time install-wide (the lock `weft domain
/// add` and `rm` take). Asks the platform nothing when the door already
/// serves exactly what is stored. A refusal is logged, recorded for `weft
/// domain list`, and retried a little later, until [`DOOR_TRIES`] refusals
/// in a row.
pub fn drain_loop(state: &DispatcherState) -> DrainLoop {
    let state = state.clone();
    DrainLoop::new("domains_door", "set up the door in front of the install's domains", ON_DOMAINS, DOOR_RETRY_FIRST, move || {
        let state = state.clone();
        async move {
            let key = crate::lease::advisory_key(crate::lease::DOMAINS_DOMAIN, "install");
            let settled = crate::lease::with_advisory_lock(&state.lock_pool, key, || settle(&state.pg_pool, state.domains.as_ref())).await?;
            // A change to the domains holding the lock may have read before
            // the write this wake is for.
            Ok(settled.unwrap_or(DrainStep::RetryIn(weft_task_store::drain::LOCK_HELD_RETRY)))
        }
    })
}

/// One pass of [`drain_loop`], under the domains' lock: put the door on
/// what is stored unless it serves exactly that already, and answer when
/// to look again.
pub async fn settle(pool: &Pool, hosting: &dyn DomainHosting) -> Result<DrainStep> {
    let domains = list(pool).await?;
    let names: Vec<String> = domains.iter().map(served_as).collect();
    let door = door_state(pool).await?;
    let just_served = door.refused.is_none()
        && door.served.as_ref().is_some_and(|(served, ago_ms)| {
            *served == names && u64::try_from(*ago_ms).is_ok_and(|ago| ago < SERVED_JUST_NOW.as_millis() as u64)
        });
    // Nothing stored and no door: nothing to ask the platform to change.
    if just_served || (names.is_empty() && hosting.address().await?.is_none()) {
        return Ok(DrainStep::Done);
    }
    // The record decides the next pass here (the backoff, what `weft
    // domain list` says), so failing to write it fails the pass, which
    // the loop retries.
    match hosting.serve(&domains).await {
        Ok(_) => {
            record_served(pool, &names).await?;
            Ok(DrainStep::Done)
        }
        Err(refused) => {
            record_refused(pool, &format!("{refused:#}")).await?;
            let refusals = door.refusals + 1;
            if refusals >= DOOR_TRIES {
                tracing::error!(
                    target: "weft_dispatcher::domains",
                    error = %format!("{refused:#}"), refusals,
                    "the platform's door keeps refusing to follow the stored domains; `weft domain list` shows why, and \
                     `weft domain add` of a stored domain tries again"
                );
                return Ok(DrainStep::Done);
            }
            let retry = door_retry(refusals);
            tracing::warn!(
                target: "weft_dispatcher::domains",
                error = %format!("{refused:#}"), retry_in_secs = retry.as_secs(),
                "the platform's door refused to follow the stored domains; `weft domain list` shows why, and it is tried again"
            );
            Ok(DrainStep::RetryIn(retry))
        }
    }
}

/// Look after the certificates the door is waiting on, apart from the
/// door itself: asking whether a certificate is issued is a read, and
/// putting the door on the domains again for it would be dozens of calls
/// to the platform each time (and a refusal there would count against
/// the door). Woken by a domain that comes or goes; between, it books
/// its own looks while a recent certificate is pending
/// ([`next_certificate_look`]).
pub fn certificates_loop(state: &DispatcherState) -> DrainLoop {
    let state = state.clone();
    DrainLoop::new(
        "domains_certificates",
        "ask after the certificates of the install's domains",
        ON_DOMAINS,
        CERTIFICATE_LOOK_MOST,
        move || {
            let state = state.clone();
            async move { look_after_certificates(&state.pg_pool, state.domains.as_ref(), crate::lease::now_unix()).await }
        },
    )
}

/// One pass of [`certificates_loop`] at `now_unix`: ask after each pending
/// certificate still looked after, record what the platform says, and
/// answer when to look again. Asks nothing once no certificate is looked
/// after, so an idle install is never woken for one.
pub async fn look_after_certificates(pool: &Pool, hosting: &dyn DomainHosting, now_unix: i64) -> Result<DrainStep> {
    let stored = list_stored(pool).await?;
    // Past that time `weft domain list` is what asks, and its waiting line
    // says so: nothing is logged here, since this pass runs again at every
    // wake and would say it every time.
    if next_certificate_look(&stored, now_unix).is_none() {
        return Ok(DrainStep::Done);
    }
    let looked_after = |s: &Stored| certificate_look(pending_for(s, now_unix)).is_some();
    let stored = match check_certificates(pool, hosting, stored.clone(), looked_after).await {
        Ok(checked) => checked,
        // The look is booked as if it had found nothing new: the cadence
        // and its end hold even while the platform cannot be read.
        Err(e) => {
            tracing::warn!(target: "weft_dispatcher::domains", error = %format!("{e:#}"), "could not read the door's certificates; asking again later");
            stored
        }
    };
    Ok(next_certificate_look(&stored, now_unix).map_or(DrainStep::Done, DrainStep::RetryIn))
}

/// How long `stored`'s certificate has been pending at `now_unix`: since
/// its domain was added.
fn pending_for(stored: &Stored, now_unix: i64) -> Duration {
    Duration::from_secs(u64::try_from(now_unix - stored.added_unix).unwrap_or(0))
}

/// How soon a certificate is asked after again, at first: Google issues
/// one within minutes of the DNS record pointing at the door.
const CERTIFICATE_LOOK_FIRST: Duration = Duration::from_secs(60);

/// The longest wait between two looks at a pending certificate.
const CERTIFICATE_LOOK_MOST: Duration = Duration::from_secs(30 * 60);

// SYNC: CERTIFICATE_LOOKS_FOR <-> crates/weft-cli/src/commands/domain.rs CHECKED_HOW
/// How long after its domain was added a pending certificate stops being
/// asked after on its own. A record nobody sets keeps it pending, and an
/// install that scales to zero must not be woken for it for good; `weft
/// domain list` still asks.
const CERTIFICATE_LOOKS_FOR: Duration = Duration::from_secs(3 * 24 * 3600);

/// When to look at the pending certificates of `stored` again, at this
/// install's pace: soon after a domain is added, less often the longer it
/// has been pending, and not at all once none is pending, or each has
/// been for longer than [`CERTIFICATE_LOOKS_FOR`].
fn next_certificate_look(stored: &[Stored], now_unix: i64) -> Option<Duration> {
    stored
        .iter()
        .filter(|s| matches!(s.certificate, Certificate::Pending { .. }))
        .filter_map(|s| certificate_look(pending_for(s, now_unix)))
        .min()
        .map(weft_core::time_scale::scaled)
}

/// How soon to ask about a certificate pending for `pending_for`, in real
/// time: a quarter of that, between [`CERTIFICATE_LOOK_FIRST`] and
/// [`CERTIFICATE_LOOK_MOST`]; `None` past [`CERTIFICATE_LOOKS_FOR`].
fn certificate_look(pending_for: Duration) -> Option<Duration> {
    (pending_for <= CERTIFICATE_LOOKS_FOR).then(|| (pending_for / 4).clamp(CERTIFICATE_LOOK_FIRST, CERTIFICATE_LOOK_MOST))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(name: &str, certificate: Certificate, added_unix: i64) -> Stored {
        Stored { domain: Domain { name: name.into(), serves: DomainServes::Install }, certificate, added_unix }
    }

    #[test]
    fn a_pending_certificate_is_asked_after_less_often_and_then_not_at_all() {
        assert_eq!(certificate_look(Duration::ZERO), Some(CERTIFICATE_LOOK_FIRST));
        assert_eq!(certificate_look(Duration::from_secs(3600)), Some(Duration::from_secs(900)));
        assert_eq!(certificate_look(Duration::from_secs(24 * 3600)), Some(CERTIFICATE_LOOK_MOST));
        assert_eq!(certificate_look(CERTIFICATE_LOOKS_FOR + Duration::from_secs(1)), None);
    }

    #[test]
    fn only_a_pending_certificate_books_a_look() {
        let now = 1_000_000;
        let pending = Certificate::Pending { why: None };
        assert_eq!(next_certificate_look(&[], now), None);
        assert_eq!(next_certificate_look(&[stored("a.example", Certificate::Issued, now)], now), None);
        assert_eq!(next_certificate_look(&[stored("a.example", Certificate::Failed { why: "x".into() }, now)], now), None);
        let soonest = next_certificate_look(&[stored("a.example", pending.clone(), now - 7200), stored("b.example", pending, now - 60)], now);
        assert_eq!(soonest, Some(weft_core::time_scale::scaled(CERTIFICATE_LOOK_FIRST)), "the newest pending one decides");
    }

    #[test]
    fn a_certificate_is_stored_and_read_back_the_same() {
        for certificate in [Certificate::Issued, Certificate::Pending { why: None }, Certificate::Pending { why: Some("dns".into()) }, Certificate::Failed { why: "caa".into() }] {
            let (state, why) = certificate_columns(&certificate);
            let row = Row {
                name: "a.example".into(),
                serves: "install".into(),
                project_id: None,
                upstream: None,
                added_unix: 1,
                certificate: state.into(),
                certificate_why: why.map(str::to_string),
            };
            assert_eq!(row.into_stored().unwrap().certificate, certificate);
        }
    }
}
