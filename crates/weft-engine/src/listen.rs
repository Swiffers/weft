//! Where a worker (and the node-test server) listens: `0.0.0.0:<port>` in
//! a container, whose network is its own, and the address
//! [`LISTEN_ENV`] names when it runs as a plain process on a person's
//! machine, where any port it takes is the machine's (the local runner
//! passes the install's public address for the project's front, which
//! listens on the project's own port, and `127.0.0.1` for every other
//! worker).
//!
//! A port already taken is the one failure the runner that started the
//! worker has to tell apart from a broken worker, so it ends the process
//! with its own exit code and one line on stderr, [`BIND_FAILED_EXIT`]
//! and [`BIND_FAILED_LINE`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// The variable naming the IP address to listen on.
// SYNC: LISTEN_ENV <-> crates/weft-platform-local/src/runner.rs (worker_env)
pub const LISTEN_ENV: &str = "WEFT_LISTEN";

/// The exit code of a worker whose port is already taken: the platform's
/// own, which a runner that started it on that port reads.
pub const BIND_FAILED_EXIT: i32 = weft_platform_traits::runner::WORKER_PORT_TAKEN_EXIT;

/// The start of the one line a worker whose port is taken prints to
/// stderr, followed by the address it tried.
pub const BIND_FAILED_LINE: &str = weft_platform_traits::runner::WORKER_PORT_TAKEN_LINE;

/// The address to listen on for `port`, from [`LISTEN_ENV`] when it is set.
fn address(listen: Option<&str>, port: u16) -> anyhow::Result<SocketAddr> {
    let ip = match listen {
        None => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        Some(listen) => listen
            .trim()
            .parse::<IpAddr>()
            .map_err(|_| anyhow::anyhow!("{LISTEN_ENV} is `{listen}`, which is not an IP address"))?,
    };
    Ok(SocketAddr::new(ip, port))
}

/// Take `port` on the address [`LISTEN_ENV`] names. A port already taken
/// ends the process (see the module doc); any other failure is returned.
pub async fn bind(port: u16) -> anyhow::Result<tokio::net::TcpListener> {
    let addr = address(std::env::var(LISTEN_ENV).ok().as_deref(), port)?;
    match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => Ok(listener),
        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
            eprintln!("{BIND_FAILED_LINE} {addr}");
            std::process::exit(BIND_FAILED_EXIT);
        }
        Err(error) => Err(anyhow::anyhow!("could not listen on {addr}: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_address_is_every_interface_unless_one_is_named() {
        assert_eq!(address(None, 8080).unwrap(), "0.0.0.0:8080".parse().unwrap());
        assert_eq!(address(Some("127.0.0.1"), 8080).unwrap(), "127.0.0.1:8080".parse().unwrap());
        assert_eq!(address(Some("::1"), 9).unwrap(), "[::1]:9".parse().unwrap());
        assert!(address(Some("localhost"), 8080).is_err(), "a name is not an address");
    }
}
