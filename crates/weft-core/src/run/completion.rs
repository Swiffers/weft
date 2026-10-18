//! Completion detection. A run is complete when:
//! - every node that has a run record has reached a terminal
//!   status, AND
//! - no pending pulses remain in the table.
//!
//! A run is failed when any run record is in Failed
//! status.

use crate::run::firing::{NodeFiringStatus, NodeFiringTable};
use crate::pulse::PulseTable;

/// Return `Some(failed_flag)` when the run is complete:
/// - `Some(false)`: all terminal, no pending, no failures.
/// - `Some(true)`: all terminal, but at least one failed.
/// - `None`: still work to do (pending pulses or non-terminal
///   firings).
pub fn check_completion(pulses: &PulseTable, firings: &NodeFiringTable) -> Option<bool> {
    // `in_flight` (not `is_pending`): a ROUTED pulse sits in a live
    // sink awaiting its take, which is still work; declaring the
    // run complete over it would silently drop a delivered-but-
    // untaken stream item.
    for bucket in pulses.values() {
        if bucket.iter().any(|p| p.status.in_flight()) {
            return None;
        }
    }

    let mut any_failed = false;
    for firings in firings.values() {
        for e in firings {
            if !e.status.is_terminal() {
                return None;
            }
            if e.status == NodeFiringStatus::Failed {
                any_failed = true;
            }
        }
    }

    Some(any_failed)
}
