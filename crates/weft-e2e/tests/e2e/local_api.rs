//! A local install's management API asks every caller for the install's
//! key and answers no web page, while the doors outside callers use stay
//! open to every origin, each checking its own token.
#![cfg(feature = "e2e")]

use weft_e2e::ensure;

/// Without the key the management API refuses; with it, it answers but
/// never tells a browser that a page of another origin may read the
/// answer. A trigger door still answers a page's preflight.
#[tokio::test]
async fn the_management_api_wants_the_key_and_answers_no_web_page() -> anyhow::Result<()> {
    let disp = ensure::up().await?;
    let http = reqwest::Client::new();
    let base = disp.base();
    let key = disp.install().secret(weft_core::signal_token::INSTALL_KEY_SECRET).map_err(anyhow::Error::msg)?;

    let unkeyed = http.get(format!("{base}/projects")).header("origin", "https://evil.example").send().await?;
    anyhow::ensure!(unkeyed.status() == reqwest::StatusCode::UNAUTHORIZED, "an unkeyed call is refused, got {}", unkeyed.status());

    let keyed = http.get(format!("{base}/projects")).bearer_auth(&key).header("origin", "https://evil.example").send().await?;
    anyhow::ensure!(keyed.status().is_success(), "the key is accepted, got {}", keyed.status());
    anyhow::ensure!(
        keyed.headers().get("access-control-allow-origin").is_none(),
        "no web origin may read the management API: {:?}",
        keyed.headers().get("access-control-allow-origin")
    );

    let preflight = http
        .request(reqwest::Method::OPTIONS, format!("{base}/projects"))
        .header("origin", "https://evil.example")
        .header("access-control-request-method", "DELETE")
        .send()
        .await?;
    anyhow::ensure!(preflight.headers().get("access-control-allow-origin").is_none(), "no preflight lets a page through");

    let door = http
        .request(reqwest::Method::OPTIONS, format!("{base}/signal/not-a-token"))
        .header("origin", "https://site.example")
        .header("access-control-request-method", "POST")
        .send()
        .await?;
    anyhow::ensure!(
        door.headers().get("access-control-allow-origin").is_some(),
        "a trigger door answers every origin, its token being its check"
    );
    Ok(())
}
