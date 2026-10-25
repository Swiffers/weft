//! A local install has no door of its own in front of a domain: the open
//! internet reaches it through its tunnel, at the tunnel's address.

use async_trait::async_trait;
use weft_platform_traits::DomainHosting;

pub struct NoDomains;

#[async_trait]
impl DomainHosting for NoDomains {
    async fn serve(&self, domains: &[weft_core::install::Domain]) -> anyhow::Result<Option<std::net::IpAddr>> {
        anyhow::ensure!(
            domains.is_empty(),
            "a local install has no door to point a domain at: the internet reaches it through its tunnel \
             (`weft daemon start --public-url`), at the tunnel's address; domains are for a cloud install"
        );
        Ok(None)
    }

    async fn address(&self) -> anyhow::Result<Option<std::net::IpAddr>> {
        Ok(None)
    }

    fn cost(&self) -> Option<&'static str> {
        None
    }

    /// It serves no name, so it holds no certificate.
    async fn certified(&self, names: &[String]) -> anyhow::Result<std::collections::BTreeMap<String, weft_core::install::Certificate>> {
        Ok(names
            .iter()
            .map(|n| (n.clone(), weft_core::install::Certificate::Failed { why: "a local install has no door to hold a certificate".into() }))
            .collect())
    }
}
