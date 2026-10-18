//! Shared plumbing for the outbound event-source kinds (`SseSubscribe`,
//! `PollEndpoint`, `SocketListen`). All three hold or poll an external
//! source and fire a fresh run per event; what differs is HOW they
//! read (held SSE stream vs periodic poll vs bidirectional socket). The
//! payload coercion, the reconnect-backoff ladder and what a reconnecting
//! connection says are identical, so they live here once and the
//! handlers call in.

use serde_json::Value;
use tokio::time::{sleep, Duration};

/// Coerce an event's raw text into the JSON fire payload: parse it as JSON,
/// or wrap the raw text as a JSON string. The fire pipeline is JSON-typed
/// end to end, so every event source funnels text through here.
pub fn coerce_text_payload(text: String) -> Value {
    serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text))
}

/// Exponential reconnect backoff, shared by every event source. Starts at
/// 1s, doubles per failure, caps at 60s. A connection that stayed healthy
/// long enough to matter resets the ladder so a flapping endpoint is not
/// hammered while a genuinely-recovered one retries promptly.
pub struct Backoff {
    current_secs: u64,
}

/// A connection must stay up at least this long to count as "healthy" and
/// reset the backoff ladder. Below this we treat reconnect as part of a
/// flap and keep climbing.
const HEALTHY_THRESHOLD_SECS: u64 = 30;
const MAX_BACKOFF_SECS: u64 = 60;

impl Backoff {
    pub fn new() -> Self {
        Self { current_secs: 1 }
    }

    /// Sleep for the current backoff, then climb. Call after a failed or
    /// dropped connection before retrying.
    pub async fn wait_then_climb(&mut self) {
        sleep(Duration::from_secs(self.current_secs)).await;
        self.current_secs = (self.current_secs * 2).min(MAX_BACKOFF_SECS);
    }

    /// Reset to the floor if the connection stayed up long enough to be
    /// considered healthy. `uptime` is how long the just-dropped connection
    /// lasted.
    pub fn reset_if_healthy(&mut self, uptime: Duration) {
        if uptime >= Duration::from_secs(HEALTHY_THRESHOLD_SECS) {
            self.current_secs = 1;
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

/// What a connection that drops and comes back says about itself, said
/// only when it changes: `connecting` as it first comes up, `connected`,
/// and once when it drops, `reconnecting since <time>: <first error>`,
/// however many attempts follow. A holder writes each change to the row
/// another process shows it from, so an attempt says nothing new.
pub struct Said {
    report: super::ServingReport,
    down: bool,
}

impl Said {
    pub fn new(report: super::ServingReport) -> Self {
        report("connecting".to_string());
        Self { report, down: false }
    }

    /// Up again, saying how (`connected`).
    pub fn up(&mut self, how: &str) {
        self.down = false;
        (self.report)(how.to_string());
    }

    pub fn down(&mut self, why: impl std::fmt::Display) {
        if !std::mem::replace(&mut self.down, true) {
            (self.report)(format!("reconnecting since {}: {why}", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));
        }
    }

    /// For good: nothing follows.
    pub fn stopped(&self, why: impl std::fmt::Display) {
        (self.report)(format!("stopped: {why}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_climbs_and_caps() {
        let mut b = Backoff::new();
        assert_eq!(b.current_secs, 1);
        // Simulate the climb without sleeping by replicating the doubling.
        for expected in [2u64, 4, 8, 16, 32, 60, 60] {
            b.current_secs = (b.current_secs * 2).min(MAX_BACKOFF_SECS);
            assert_eq!(b.current_secs, expected);
        }
    }

    #[test]
    fn healthy_uptime_resets_ladder() {
        let mut b = Backoff { current_secs: 32 };
        b.reset_if_healthy(Duration::from_secs(5));
        assert_eq!(b.current_secs, 32, "short uptime keeps climbing");
        b.reset_if_healthy(Duration::from_secs(31));
        assert_eq!(b.current_secs, 1, "healthy uptime resets to floor");
    }
}
