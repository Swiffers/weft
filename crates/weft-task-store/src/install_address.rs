//! The address the install hands out in every link it builds: an OAuth
//! callback, a webhook's activation URL, a file link, a resume link,
//! what `GET /install` tells a project's CI.
//!
//! The platform gives the install an address of its own (a Cloud Run
//! service's `run.app` name, a local install's port), set in its config
//! at boot. A person can also give it a domain (`weft domain add`), which
//! the platform's door holds a certificate for. Once that certificate is
//! issued, the domain is the address: providers show it on their consent
//! screens, and it survives the install being rebuilt. Before that, a link
//! on the domain would fail for whoever opens it, so the platform's own
//! address stays until the door records the certificate (the dispatcher's
//! `crate::domains::settle`). The platform's address keeps answering
//! either way, so a link handed out before the switch still works.
//!
//! The dispatcher and the broker both build links, in separate processes
//! on a cloud, so both hold one of these, each following the domain rows
//! through its process's `LISTEN` connection.

use std::sync::Arc;

use sqlx::PgPool;
use weft_platform_traits::config::InternetAddress;

use crate::held_copy::{Changed, HeldCopy};
use crate::pg_signal::PgSignalWatch;

/// Announced when a domain row comes, goes, or its certificate changes
/// (the `install_domain` group's trigger, in the dispatcher).
// SYNC: DOMAINS_CHANNEL <-> crates/weft-dispatcher/src/domains.rs (GROUP's install_domain_notify)
pub const DOMAINS_CHANNEL: &str = "weft_install_domains";

/// See the module doc.
pub struct InstallAddress {
    /// The platform's own address, from the config.
    service: String,
    /// Where the open internet reaches the install when no domain is the
    /// address (a tunnel's, a fixed one).
    internet: InternetAddress,
    /// The install domain whose certificate is issued, as `https://<name>`.
    domain: Arc<HeldCopy<(), Option<String>>>,
    pool: PgPool,
    http: reqwest::Client,
}

impl InstallAddress {
    /// One following the domain rows on `signals`, which must listen on
    /// [`DOMAINS_CHANNEL`].
    pub fn follow(pool: PgPool, signals: &PgSignalWatch, service: &str, internet: InternetAddress) -> anyhow::Result<Self> {
        let domain = HeldCopy::follow(signals, DOMAINS_CHANNEL, 1, |_| Changed::Everything, |_| true)?;
        Ok(Self::with_copy(pool, domain, service, internet))
    }

    /// One that reads the rows every time: a process with nothing to tell
    /// it about changes (a test, a tool).
    pub fn unfollowed(pool: PgPool, service: &str, internet: InternetAddress) -> Self {
        Self::with_copy(pool, HeldCopy::unfollowed(1, |_| true), service, internet)
    }

    fn with_copy(pool: PgPool, domain: Arc<HeldCopy<(), Option<String>>>, service: &str, internet: InternetAddress) -> Self {
        Self { service: service.trim_end_matches('/').to_string(), internet, domain, pool, http: reqwest::Client::new() }
    }

    /// The platform's own address, which always answers: what the config
    /// says, whatever domain the install has.
    pub fn service_url(&self) -> &str {
        &self.service
    }

    /// The install domain that is the address now, `https://<name>`.
    async fn live_domain(&self) -> anyhow::Result<Option<String>> {
        let pool = self.pool.clone();
        let held = self.domain.get_or_load((), || async move { live_install_domain(&pool).await }).await?;
        Ok((*held).clone())
    }

    /// The address people and providers are given for the install: its
    /// domain once the domain's certificate is issued, else the
    /// platform's own address.
    pub async fn stable(&self) -> anyhow::Result<String> {
        Ok(choose(&self.service, self.live_domain().await?.as_deref()).to_string())
    }

    /// [`Self::stable`] read from the rows themselves, for an answer
    /// that has just changed them (a certificate recorded issued is heard
    /// a few milliseconds after it commits).
    pub async fn stable_fresh(&self) -> anyhow::Result<String> {
        let pool = self.pool.clone();
        let held = self.domain.load_fresh((), || async move { live_install_domain(&pool).await }).await?;
        Ok(choose(&self.service, held.as_deref()).to_string())
    }

    /// The address the open internet reaches the install at now: its
    /// domain once issued (the door is on the internet), else the
    /// internet address the config gives (a quick tunnel's is asked each
    /// time). `None` when the internet reaches none.
    pub async fn internet(&self) -> anyhow::Result<Option<String>> {
        match self.live_domain().await? {
            Some(domain) => Ok(Some(domain)),
            None => self.internet.current(&self.http).await,
        }
    }

    /// The base an address handed to an OUTSIDE caller is built on (an
    /// activation URL, a public endpoint, an address a provider posts
    /// to): the internet address when there is one, else the stable one.
    pub async fn outside(&self) -> anyhow::Result<String> {
        match self.internet().await? {
            Some(internet) => Ok(internet),
            None => self.stable().await,
        }
    }

    /// The address a request that came in naming `host` was sent to,
    /// when the install answers there: a provider told the platform's own
    /// address before the domain became the address keeps posting there
    /// until it is told again, and a push signed over the address it was
    /// sent to verifies against that one. Any other host is the internet
    /// address as it is now.
    pub async fn arrived_at(&self, host: Option<&str>) -> anyhow::Result<Option<String>> {
        if host.is_some_and(|host| host_of(&self.service).is_some_and(|ours| ours.eq_ignore_ascii_case(host))) {
            return Ok(Some(self.service.clone()));
        }
        self.internet().await
    }
}

/// The address handed out: the live domain when there is one.
fn choose<'a>(service: &'a str, domain: Option<&'a str>) -> &'a str {
    domain.unwrap_or(service)
}

/// The host (and port, when it names one) of `url`.
fn host_of(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    Some(match parsed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

/// The install domain whose certificate is issued, the first added when
/// there are several.
// SYNC: the columns <-> crates/weft-dispatcher/src/domains.rs (GROUP's install_domain)
async fn live_install_domain(pool: &PgPool) -> anyhow::Result<Option<String>> {
    let name: Option<String> = sqlx::query_scalar(
        "SELECT name FROM install_domain WHERE serves = 'install' AND certificate = 'issued' ORDER BY added_unix, name LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| anyhow::Error::from(e).context("read the install's domain"))?;
    Ok(name.map(|name| format!("https://{name}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_live_domain_is_the_address_and_nothing_else_moves_it() {
        assert_eq!(choose("https://weft-x.run.app", None), "https://weft-x.run.app");
        assert_eq!(choose("https://weft-x.run.app", Some("https://w.example.com")), "https://w.example.com");
    }

    #[test]
    fn a_host_is_read_with_its_port() {
        assert_eq!(host_of("https://weft-x.run.app").as_deref(), Some("weft-x.run.app"));
        assert_eq!(host_of("http://127.0.0.1:14111").as_deref(), Some("127.0.0.1:14111"));
        assert_eq!(host_of("not a url"), None);
    }
}
