//! When a wait ends, for a wait its caller may make as long as it likes.

use std::time::Duration;

use tokio::time::Instant;

/// `wait` from `from`, or, for a wait past what the clock holds, so far
/// ahead that it never comes.
pub fn after(from: Instant, wait: Duration) -> Instant {
    from.checked_add(wait).unwrap_or_else(|| from + FOREVER)
}

/// What [`after`] reads as never: thirty years, as tokio's own timers do.
const FOREVER: Duration = Duration::from_secs(30 * 365 * 24 * 3600);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_past_the_clock_is_forever() {
        let now = Instant::now();
        assert_eq!(after(now, Duration::from_secs(5)), now + Duration::from_secs(5));
        assert_eq!(after(now, Duration::MAX), now + FOREVER);
    }
}
