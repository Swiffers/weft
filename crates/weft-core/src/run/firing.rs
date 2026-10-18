//! Per-dispatch run record. A `NodeFiring` is created every
//! time a node fires; multiple fires (parallel loop iterations) produce
//! multiple entries keyed by node id.

use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::frames::LoopFrames;
use crate::RunId;

// SYNC: NodeFiringStatus <-> packages/weft-graph/src/protocol.ts NodeFiringStatus
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeFiringStatus {
    Running,
    Completed,
    Failed,
    WaitingForInput,
    Skipped,
    Cancelled,
}

impl NodeFiringStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::WaitingForInput => "waiting_for_input",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Record of one node-firing run. Pulses stay pure data
/// carriers; every bit of run metadata (status, cost, logs,
/// timing) lives here. The received input records what the firing used,
/// even after stream pulses are consumed. Outputs are reconstructed from
/// emission rows; the record retains which ports were mentioned and closed.
///
/// Suspend-then-resume keeps the same record. The `status`
/// transitions through Running and WaitingForInput on the same
/// record without churning new entries. One run fires a node
/// once per frames, so a location holds at most one record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeFiring {
    pub id: uuid::Uuid,
    /// Effective inputs at this firing, derived once under its own birth
    /// context. Inherited display never reinterprets them through a child cut.
    pub received: super::ready::FiringInput,
    pub node_id: String,
    pub status: NodeFiringStatus,
    pub skip_reason: Option<super::skip::SkipReason>,
    /// Input pulses consumed by this dispatch.
    pub pulses_absorbed: Vec<uuid::Uuid>,
    pub error: Option<String>,
    /// The firing failed on a wait it gave up at a time limit
    /// (`WeftError::WaitGaveUp`): a run this failure ends reached a time
    /// limit, and its caller hears it as one. Carried by the `NodeFailed`
    /// row, so whichever worker ends the run reads it here.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub gave_up_wait: bool,
    /// Suspension token (set while `status == WaitingForInput`).
    pub callback_id: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
    pub cost_usd: f64,
    pub logs: Vec<Value>,
    /// The output ports this firing has put on a wire or closed (an
    /// emission, a close): what its termination sweep leaves alone,
    /// closing every other output port, and what a body resumed after
    /// a pause starts from, so its replayed emissions on these ports
    /// send nothing. Kept on the record so the worker and the fold
    /// read one thing.
    #[serde(default)]
    pub mentioned_ports: HashSet<String>,
    /// Explicit output closures, including generator ends already delivered.
    #[serde(default)]
    pub closed_output_ports: HashSet<String>,
    pub run_id: RunId,
    pub frames: LoopFrames,
    /// The run this record was inherited from, when a seeded run reused
    /// it instead of firing the node again (`RunStarted.seed`).
    /// `None` for a firing of this run's own. The record still carries
    /// THIS run's `run_id` (the table is this run's); the origin is here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited_from: Option<RunId>,
}

/// One entry per node, growing as each dispatch records its lifecycle.
pub type NodeFiringTable = BTreeMap<String, Vec<NodeFiring>>;

/// The record at `(node, run_id, frames)`: the one firing every
/// row and every sweep about that location is about.
pub fn firing_at<'a>(
    firings: &'a NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &LoopFrames,
) -> Option<&'a NodeFiring> {
    firings.get(node_id)?.iter().find(|e| e.run_id == run_id && &e.frames == frames)
}

/// `firing_at`, mutably: the record a lifecycle row about the location
/// updates.
pub fn firing_at_mut<'a>(
    firings: &'a mut NodeFiringTable,
    node_id: &str,
    run_id: RunId,
    frames: &LoopFrames,
) -> Option<&'a mut NodeFiring> {
    firings.get_mut(node_id)?.iter_mut().find(|e| e.run_id == run_id && &e.frames == frames)
}
