//! What a listener process keeps of the broker's answers about where it
//! connects and as whom, so a connection that drops and comes back, or a
//! poll that runs again, asks nothing of the broker (and so wakes no
//! database).
//!
//! - **Addresses** (`crate::infra_address`), per signal and address: an
//!   address that is no infra endpoint is the one the trigger was given,
//!   for good. An infra endpoint's can move (a local unit made again on
//!   another port), so it is asked again after a connect to it failed. A
//!   poll's wake carries them to the next (`crate::kinds::WakeBody`), so a
//!   copy of the listener that just started asks nothing either.
//! - **Credentials** (`crate::listener_access`), per signal, as one
//!   registration of it reads its connection: fetched again when the
//!   outside refused them, before the moment the broker said they stop
//!   being good, for a new registration of the signal, and, in a holder,
//!   when a connection was made again or removed (`HolderNote::AccessChanged`, or news it
//!   may have missed), which drops every one. A copy of the listener that
//!   is not a holder hears no such news, so a connection that changes
//!   reaches the signals it serves as a new registration of each. A
//!   resolve made as a signal is registered, before it has a registration,
//!   keeps nothing.
//!
//! Both are dropped for a signal this process stops serving, that is
//! registered again here, that gave up, or whose wake found its row gone
//! or registered again ([`Reach::forget`]). A copy of the listener that
//! serves wakes hears of none of that for a signal whose other wakes went
//! to other copies, so credentials past the moment they stop being good
//! are dropped whenever credentials are read or kept, and each map keeps
//! at most [`KEPT_AT_MOST`] entries, the longest kept going first: what
//! is dropped is fetched again if it is ever used.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use weft_broker_client::protocol::{ListenerInfraAddress, ListenerResolveRequest, ListenerResolvedSource};

/// The most entries each map keeps: well past what one copy serves at
/// once (a holder takes a few hundred signals), so only what no signal
/// served here uses any more is dropped for room.
pub const KEPT_AT_MOST: usize = 4096;

#[derive(Default)]
pub struct Reach {
    /// By `(signal token, host:port as given)`, with when it was kept.
    addresses: DashMap<(String, String), (u64, ListenerInfraAddress)>,
    /// By signal token: what one of its registrations fetched.
    credentials: DashMap<String, Kept>,
    /// Counts what is kept, so the longest kept is the lowest.
    kept: AtomicU64,
}

/// A signal's connection as registration `registration` of it fetched it,
/// for the resolve request `request` (as JSON).
struct Kept {
    registration: i64,
    request: String,
    source: ListenerResolvedSource,
    /// When it was kept ([`Reach::kept`]).
    order: u64,
}

impl Kept {
    /// Whether the broker said these stop being good by `now` (unix seconds).
    fn expired(&self, now: i64) -> bool {
        self.source.expires_at_unix.is_some_and(|at| at <= now)
    }
}

/// Drop the longest-kept entries of `map` (by the order `order_of` reads)
/// until it holds at most [`KEPT_AT_MOST`].
fn make_room<K: Eq + std::hash::Hash + Clone, V>(map: &DashMap<K, V>, order_of: impl Fn(&V) -> u64) {
    while map.len() > KEPT_AT_MOST {
        let Some(oldest) = map.iter().min_by_key(|e| order_of(e.value())).map(|e| e.key().clone()) else { return };
        map.remove(&oldest);
    }
}

/// The registration of a signal that has none yet: it is being registered.
pub const UNREGISTERED: i64 = 0;

impl Reach {
    fn next_order(&self) -> u64 {
        self.kept.fetch_add(1, Ordering::Relaxed)
    }

    pub fn address(&self, token: &str, authority: &str) -> Option<ListenerInfraAddress> {
        self.addresses.get(&(token.to_string(), authority.to_string())).map(|a| a.1.clone())
    }

    pub fn keep_address(&self, token: &str, authority: &str, answer: ListenerInfraAddress) {
        self.addresses.insert((token.to_string(), authority.to_string()), (self.next_order(), answer));
        make_room(&self.addresses, |(order, _)| *order);
    }

    /// Every address kept for signal `token`, by the address given, for a
    /// wake to carry to whichever copy of the listener handles the next.
    pub fn addresses_of(&self, token: &str) -> std::collections::BTreeMap<String, ListenerInfraAddress> {
        self.addresses.iter().filter(|e| e.key().0 == token).map(|e| (e.key().1.clone(), e.value().1.clone())).collect()
    }

    /// A connect to the address given as `authority` failed: an infra
    /// endpoint's is asked again next time.
    pub fn unreached(&self, token: &str, authority: &str) {
        self.addresses.remove_if(&(token.to_string(), authority.to_string()), |_, (_, a)| a.is_infra);
    }

    /// The credentials registration `registration` of signal `token` keeps
    /// for `req`, while they are still good. What is kept for the signal
    /// and no longer good (past its moment, another registration's) is
    /// dropped.
    pub fn credentials(&self, token: &str, registration: i64, req: &ListenerResolveRequest) -> Option<ListenerResolvedSource> {
        let now = chrono::Utc::now().timestamp();
        let usable = |kept: &Kept| !kept.expired(now) && kept.registration >= registration;
        self.credentials.remove_if(token, |_, kept| !usable(kept));
        let kept = self.credentials.get(token)?;
        (kept.registration == registration && kept.request == request_key(req)).then(|| kept.source.clone())
    }

    /// Keep what registration `registration` of signal `token` fetched for
    /// `req`, in place of anything kept for it before, dropping every
    /// signal's credentials that are no longer good. Nothing is kept for a
    /// signal being registered ([`UNREGISTERED`]).
    pub fn keep_credentials(&self, token: &str, registration: i64, req: &ListenerResolveRequest, source: ListenerResolvedSource) {
        if registration == UNREGISTERED {
            return;
        }
        let now = chrono::Utc::now().timestamp();
        self.credentials.retain(|_, kept| !kept.expired(now));
        self.credentials.insert(token.to_string(), Kept { registration, request: request_key(req), source, order: self.next_order() });
        make_room(&self.credentials, |kept| kept.order);
    }

    /// The outside refused the credentials kept for signal `token`.
    pub fn refused(&self, token: &str) {
        self.credentials.remove(token);
    }

    /// Signal `token` is no longer served here, or is registered again:
    /// nothing is kept for it.
    pub fn forget(&self, token: &str) {
        self.credentials.remove(token);
        self.addresses.retain(|(kept_for, _), _| kept_for != token);
    }

    /// A connection was made again or removed somewhere: no credential
    /// kept is trusted.
    pub fn access_changed(&self) {
        self.credentials.clear();
    }
}

fn request_key(req: &ListenerResolveRequest) -> String {
    serde_json::to_string(req).expect("a resolve request always serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ListenerResolveRequest {
        ListenerResolveRequest { tenant: "t".into(), for_instance: None, access_id: "a".into(), service: "s".into(), required_values: vec![] }
    }

    fn source(expires_at_unix: Option<i64>) -> ListenerResolvedSource {
        ListenerResolvedSource {
            values: Default::default(),
            auth: vec![],
            events: Default::default(),
            recipe_values: Default::default(),
            provider_account: None,
            expires_at_unix,
        }
    }

    fn now() -> i64 {
        chrono::Utc::now().timestamp()
    }

    /// Credentials past their moment, or kept by an older registration of
    /// the signal, are dropped as soon as they are read; and keeping any
    /// signal's drops every other signal's that stopped being good.
    #[test]
    fn credentials_no_longer_good_are_dropped_not_kept() {
        let reach = Reach::default();
        reach.keep_credentials("old", 1, &request(), source(Some(now() + 3600)));
        assert!(reach.credentials("old", 2, &request()).is_none(), "a newer registration asks again");
        assert!(!reach.credentials.contains_key("old"), "and the older one's is gone");

        reach.keep_credentials("newer", 3, &request(), source(Some(now() + 3600)));
        assert!(reach.credentials("newer", 2, &request()).is_none(), "an older registration's wake fetches its own");
        assert!(reach.credentials.contains_key("newer"), "without dropping the newer one's");

        reach.keep_credentials("spent", 1, &request(), source(Some(now() - 1)));
        assert!(reach.credentials.contains_key("spent"));
        reach.keep_credentials("other", 1, &request(), source(None));
        assert!(!reach.credentials.contains_key("spent"), "keeping another drops what stopped being good");
        assert!(reach.credentials("other", 1, &request()).is_some(), "one that never expires stays");
    }

    /// Past the bound, the longest kept go first, so a copy that served
    /// many signals once keeps no more than the bound.
    #[test]
    fn each_map_keeps_at_most_the_bound_the_longest_kept_going_first() {
        let reach = Reach::default();
        for n in 0..=KEPT_AT_MOST {
            reach.keep_credentials(&format!("sig-{n}"), 1, &request(), source(None));
            reach.keep_address(&format!("sig-{n}"), "h:1", ListenerInfraAddress { authority: "h:1".into(), is_infra: false });
        }
        assert_eq!(reach.credentials.len(), KEPT_AT_MOST);
        assert_eq!(reach.addresses.len(), KEPT_AT_MOST);
        assert!(reach.credentials("sig-0", 1, &request()).is_none(), "the first kept went");
        assert!(reach.address("sig-0", "h:1").is_none());
        let last = format!("sig-{KEPT_AT_MOST}");
        assert!(reach.credentials(&last, 1, &request()).is_some());
        assert!(reach.address(&last, "h:1").is_some());
    }
}
