//! What a listener is started with.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// This process's replica id: what its broker calls are made as, the
    /// name its claims on held signals are taken in, and what keeps its
    /// shared provider sockets apart from another in-process listener's
    /// (tests).
    pub replica: String,
    /// Whether this process holds the signals that keep a connection open
    /// (a local install's one process, or a holder): it runs the hold loop
    /// (`crate::hold`) and brings such a signal up itself. A listener that
    /// scales to zero does not; the holders do.
    pub holds_here: bool,
    /// Where this install's holders run.
    pub holders: Holders,
}

/// Where an install's holders run, which decides what a held signal can
/// reach and what holding it costs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Holders {
    /// On the machine the install runs on, the person's own (a local
    /// install's one process): a signal can watch a folder there, and a
    /// subscription a provider could push to keeps dialing out, since the
    /// process is up anyway and its tunnel's address can change.
    OnThisMachine,
    /// On machines of their own that run only while something needs one (a
    /// cloud's pool), where the person's folders are not: a subscription a
    /// provider can push takes the push, since holding a connection costs
    /// money and the install's address is the internet's for good.
    Pool,
}
