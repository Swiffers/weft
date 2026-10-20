//! What the dispatcher tells a client happened: one [`DispatcherEvent`]
//! per thing, with its delivery identity ([`IdentifiedEvent`]). The live
//! stream (`/events/project/{id}`) and a run's replay
//! (`/runs/{id}/replay`) answer the same rows, and every Rust
//! reader (the CLI's `weft events`, `weft logs`, `weft follow`) decodes
//! them through these types.

use serde::{Deserialize, Serialize};

use crate::frames::LoopFrames;
use crate::RunId;

/// An event and its delivery identity. A run's recorded events take theirs
/// from their place in its record (the row's `seq`, the event's index in
/// it), so replay and live delivery identify the same event without
/// comparing timestamps or payload contents.
// SYNC: IdentifiedEvent.event_id <-> extension-vscode/src/runFollower.ts DispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentifiedEvent<T> {
    pub event_id: String,
    #[serde(flatten)]
    pub event: T,
}

impl<T> IdentifiedEvent<T> {
    /// The event at `index` of its run's record row `seq`.
    pub fn recorded(seq: i32, index: u32, event: T) -> Self {
        Self { event_id: format!("journal:{seq}:{index}"), event }
    }

    pub fn transient(event: T) -> Self {
        Self { event_id: format!("live:{}", uuid::Uuid::new_v4()), event }
    }

    pub fn project<U>(self, project: impl FnOnce(T) -> Vec<U>) -> Vec<IdentifiedEvent<U>> {
        project(self.event).into_iter().enumerate().map(|(index, event)| IdentifiedEvent {
            event_id: format!("{}:{index}", self.event_id), event,
        }).collect()
    }
}

pub type LiveEvent = IdentifiedEvent<DispatcherEvent>;


/// An event the dispatcher publishes about some piece of runtime
/// state changing. Tagged enum so SSE serialization matches the
/// spec in the design doc.
// Every event projected from a journal row carries that row's `at_unix`
// (the journal's own stamp), so a replay renders when each thing happened
// rather than when it was read.
// SYNC: DispatcherEvent <-> extension-vscode/src/runFollower.ts DispatcherEvent, weavemind/website/src/lib/graph/dispatcher-host.ts translateDispatcherEvent
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase")]
pub enum DispatcherEvent {
    /// `subgraph` is every place the run's program runs (its cut of the
    /// project, `crate::project::cut`; a place that stands in for itself
    /// is not among them): the graph paints every other node as not in
    /// this run. `None` when the run's program could not be read. `seed` names the run this one inherits from and what it
    /// re-ran, for the run's banner. `phase` says what the run
    /// is for: a run of the graph (`fire`), or the setup an `infra
    /// start` or an activation runs, which the editor shows as that verb
    /// working rather than as a run to stop.
    RunStarted {
        run_id: RunId,
        entry_node: String,
        phase: crate::context::Phase,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subgraph: Option<Vec<crate::frames::Located>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seed: Option<crate::run_spec::Seed>,
        project_id: uuid::Uuid,
        /// Who the run is for, when it is one instance's run. Carried for
        /// any client following the run; no client reads it today.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        instance: Option<crate::instance::InstanceId>,
        at_unix: u64,
    },
    RunCompleted { run_id: RunId, project_id: uuid::Uuid, outputs: serde_json::Value, at_unix: u64 },
    RunFailed { run_id: RunId, project_id: uuid::Uuid, error: String, at_unix: u64 },
    /// `cause` is the structured who-or-what behind the cancel (`reason`
    /// is its text). `None` only for a journal row written before the
    /// cause existed; skipped on the wire when absent so the TS peers'
    /// optional (`cause?`) types match reality instead of decoding null.
    RunCancelled {
        run_id: RunId,
        project_id: uuid::Uuid,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<crate::run::CancelCause>,
        at_unix: u64,
    },
    /// The run tagged itself (`ctx.tag_run`); the inspector shows
    /// the tags on the run. `tags` is this call's list, not the run's
    /// cumulative set.
    RunTagged { run_id: RunId, project_id: uuid::Uuid, tags: Vec<String>, at_unix: u64 },
    /// The run was erased (`weft clean`, a prune, the editor's delete):
    /// its journal, its storage and the wake signals it was parked on
    /// are gone. Rides NOTIFY rather than the journal, since the
    /// journal is what just went; every client drops the run from its
    /// lists and re-reads the project's verbs, because a run parked on
    /// a question counted as preserved state until now.
    RunDeleted { run_id: RunId, project_id: uuid::Uuid },
    /// The run parked: every firing still open waits on a signal (a
    /// person, an outside job, a timer), and no worker drives it until an
    /// answer comes. Its listing reads `waiting_for_input` from here on.
    /// Rides NOTIFY like `RunDeleted`: parking writes no record row
    /// (`weft_broker::door::let_go` publishes it in the transaction that
    /// parks the run). A run's replay ends with it when the run is parked
    /// at the time of the read, which is where it stands rather than a
    /// moment of its history.
    RunParked { run_id: RunId, project_id: uuid::Uuid },
    /// Every node event carries `inherited_from` when the firing was
    /// not this run's own but taken from the run it was seeded from
    /// (`weft run --seed`): the row is the seed's, painted here so the
    /// graph shows the reused value and marks it inherited. Absent on
    /// the wire for the run's own firings. `provided_ports` on a start
    /// names input ports receiving supplied values; absent when none.
    NodeStarted {
        run_id: RunId,
        node: String,
        frames: LoopFrames,
        input: serde_json::Value,
        closed_ports: Vec<String>,
        // SYNC: input origins <-> extension-vscode/src/runFollower.ts DispatcherEvent, packages/weft-graph/src/protocol.ts NodeFiringEvent, packages/weft-graph/src/webview/lib/types/index.ts NodeFiring
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        provided_ports: Vec<String>,
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        inherited_ports: std::collections::BTreeMap<String, RunId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        project_id: uuid::Uuid,
        at_unix: u64,
    },
    NodeSuspended { run_id: RunId, node: String, frames: LoopFrames, token: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// `token`: the wait the firing resumed from, `None` for a resume no
    /// wait drove (a boundary fired again). How that wait ended is its
    /// [`Self::WaitEnded`].
    NodeResumed { run_id: RunId, node: String, frames: LoopFrames, token: Option<String>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// A wait of the firing at `node` and `frames` ended, the first ending
    /// on record: its answer, a person skipping it, or the run giving it
    /// up. Its own event because a wait the run holds in its worker never
    /// suspends or resumes the firing, so `NodeSuspended` and `NodeResumed`
    /// see only the waits that paused the run.
    WaitEnded { run_id: RunId, node: String, frames: LoopFrames, token: String, ended: crate::primitive::AwaitEnd, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeCancelled { run_id: RunId, node: String, frames: LoopFrames, reason: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeCompleted { run_id: RunId, node: String, frames: LoopFrames, output: serde_json::Value, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    NodeFailed { run_id: RunId, node: String, frames: LoopFrames, error: String, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// `reason` says WHY: the author's `_should_flow` said no, or an
    /// input the node needed never arrived. A decision and a consequence
    /// look identical on the graph without it.
    /// `None` only for a journal row written before the field existed
    /// (the UI renders "reason not recorded"); every live writer sends
    /// `Some`.
    NodeSkipped { run_id: RunId, node: String, frames: LoopFrames, closed_ports: Vec<String>, reason: Option<crate::run::skip::SkipReason>, #[serde(default, skip_serializing_if = "Option::is_none")] inherited_from: Option<RunId>, project_id: uuid::Uuid, at_unix: u64 },
    /// A copy of a scope (a group, a call site, an included body, at
    /// `frames`) was decided: it opened, or it did not run and why
    /// (`crate::run::scope`). The graph paints a group from these, per
    /// copy. A copy inside one that did not run is decided with it.
    // SYNC: ScopeDecided <-> extension-vscode/src/runFollower.ts DispatcherEvent, packages/weft-graph/src/protocol.ts ScopeDecidedEvent
    ScopeDecided {
        run_id: RunId,
        project_id: uuid::Uuid,
        scope: String,
        frames: LoopFrames,
        #[serde(flatten)]
        state: crate::run::scope::ScopeState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        at_unix: u64,
    },
    /// A loop instance was created at `parent_frames`. The inspector
    /// uses this to render a "Loop opened" marker at the loop's box.
    // SYNC: LoopInstantiated <-> extension-vscode/src/runFollower.ts loop_instantiated, packages/weft-graph/src/protocol.ts LoopInspectorEvent 'instantiated'
    LoopInstantiated {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        /// Effective iteration CAP; `None` for an uncapped loop (a
        /// done-driven or stream-driven loop with no `maxIters`),
        /// whose iteration count is unknowable up front.
        iter_cap: Option<u32>,
        parallel: bool,
        at_unix: u64,
    },
    /// An iteration of the loop launched. Inspector renders an
    /// iteration marker at body_frames.
    LoopIterationLaunched {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        at_unix: u64,
    },
    /// LoopOut fired for iteration `index`. The per-port gather /
    /// carry writes ride on the journal but are NOT mirrored to the
    /// inspector stream: the renderer reads the loop's outward emit
    /// (a normal pulse) and the per-iteration body activity, not the
    /// LoopOut firing's raw write map.
    LoopOutFired {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        index: u32,
        done_vote: Option<bool>,
        at_unix: u64,
    },
    /// The loop terminated outward and emitted its outer outputs.
    LoopTerminated {
        run_id: RunId,
        project_id: uuid::Uuid,
        group_id: String,
        parent_frames: LoopFrames,
        reason: crate::primitive::LoopTerminationReason,
        at_unix: u64,
    },
    /// One metered call's cost record landed on the journal, attributed to
    /// the exact firing (`node_id` + `frames`). `amount_usd` `None` = the
    /// meter could not resolve the figure (an honest unknown).
    CostReported {
        run_id: RunId,
        // SYNC: CostReported <-> extension-vscode/src/runFollower.ts DispatcherEvent cost_reported, packages/weft-graph/src/protocol.ts runCost
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        project_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        /// Stable per-record identity; the webview dedups on it (the same
        /// journal row can arrive via both the replay and the live stream).
        cost_id: String,
        service: String,
        amount_usd: Option<f64>,
        /// Whose credential the call spent (`their-own` or `ours`), so a
        /// client can say whose account a figure landed on.
        origin: crate::CredentialOwner,
        at_unix: u64,
    },
    /// A firing changed a stored file's content in place (the journal's
    /// `FileEdited`): the inspector lists it on that firing's "Files
    /// edited" card. `inherited_from` as on [`Self::CostReported`].
    FileEdited {
        run_id: RunId,
        // SYNC: FileEdited <-> extension-vscode/src/runFollower.ts DispatcherEvent file_edited, packages/weft-graph/src/protocol.ts execFileEdit
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_from: Option<RunId>,
        project_id: uuid::Uuid,
        node_id: String,
        frames: LoopFrames,
        edit: crate::storage::FileEdit,
        at_unix: u64,
    },
    TriggerUrlChanged { project_id: uuid::Uuid, node_id: String, url: String },
    ProjectRegistered { project_id: uuid::Uuid, name: String },
    ProjectActivated { project_id: uuid::Uuid },
    ProjectDeactivated { project_id: uuid::Uuid },
    /// A project lifecycle axis flipped: entering/leaving a
    /// transitional state (activating, deactivating, building,
    /// cancelling_build, resyncing) or landing at rest. Carries both axes so a
    /// client can render the new state without a round-trip; clients
    /// that prefer one code path just refetch `/status` on receipt.
    /// This is what makes backend-owned transitional state observable
    /// in near-real-time (the backend-owns-state rule has no teeth
    /// without it).
    ProjectTransitionChanged { project_id: uuid::Uuid, status: String, transition: String },
    /// Infra node transitioned between status values. Catch-all for
    /// supervisor-driven state changes the extension renders as a
    /// per-node badge.
    InfraStatusChanged { project_id: uuid::Uuid, node_id: String, status: String },
    /// Supervisor declared an infra node flaky; the extension shows
    /// the orange banner with `reason`.
    InfraFlaky { project_id: uuid::Uuid, node_id: String, reason: String },
    /// Inverse of InfraFlaky.
    InfraRecovered { project_id: uuid::Uuid, node_id: String },
    /// Supervisor finished terminating an infra node; the
    /// `infra_node` row has been deleted.
    InfraTerminated { project_id: uuid::Uuid, node_id: String },
    /// A bus participant came online. `bus_id` is the channel's uuid
    /// (same one embedded in the bus marker), so the inspector groups
    /// multiple buses cleanly. `offset` is the bus-local position used
    /// to tiebreak same-second entries. `at_unix` is the journal's
    /// stamp so replay renders honest timestamps, not "now".
    BusJoined {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// A bus participant dropped. Pairs with `BusJoined` for the same
    /// `(bus_id, name)`.
    BusLeft {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        name: String,
        at_unix: u64,
    },
    /// One journal-aggregation window of a bus's messages (one row per
    /// bus per window; default 1s). A journaled bus's `messages` carry
    /// every message in the window (senders, kinds, payloads); an
    /// ephemeral bus's `messages` are empty and `totals` (count + bytes
    /// per sender/kind) are the whole story. The inspector unpacks
    /// `messages` into its per-message log and renders a summary line
    /// for a window that carries only totals.
    // SYNC: BusWindow <-> crates/weft-journal/src/events.rs BusWindow, packages/weft-graph/src/protocol.ts BusInspectorEvent 'window', extension-vscode/src/runFollower.ts DispatcherEvent 'bus_window'
    BusWindow {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<crate::bus::WindowedBusMessage>,
        totals: Vec<crate::bus::BusWindowTotal>,
        at_unix: u64,
    },
    /// The bus was closed, and how it ended. Inspector renders an
    /// explicit `* the bus closed here` marker, or where it failed and
    /// why; replay cursors stop here.
    // SYNC: BusClosed <-> crates/weft-journal/src/events.rs BusClosed, packages/weft-graph/src/protocol.ts BusInspectorEvent 'closed', extension-vscode/src/runFollower.ts DispatcherEvent 'bus_closed'
    BusClosed {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        offset: u64,
        end: crate::bus::StreamEnd,
        at_unix: u64,
    },
    /// A live caller attached to this run. First event in the
    /// caller stream; the inspector opens a "caller" panel on the run.
    CallerConnected {
        run_id: RunId,
        project_id: uuid::Uuid,
        offset: u64,
        protocol: String,
        at_unix: u64,
    },
    /// What the conversation said during one window, both directions in
    /// one row, the same shape and clock as a bus window. A message
    /// whose content was not kept still appears carrying its size.
    /// `dropped` names the program's messages the caller never received
    /// (it read too slowly), possibly ones of an earlier window
    /// ([`crate::stream_journal::aggregate_caller_window`]).
    // SYNC: CallerWindow <-> crates/weft-journal/src/events.rs CallerWindow, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'window', extension-vscode/src/runFollower.ts DispatcherEvent 'caller_window'
    CallerWindow {
        run_id: RunId,
        project_id: uuid::Uuid,
        first_offset: u64,
        last_offset: u64,
        messages: Vec<crate::stream_journal::WindowedCallerMessage>,
        totals: Vec<crate::stream_journal::CallerWindowTotal>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        dropped: Vec<u64>,
        at_unix: u64,
    },
    /// A node error surfaced to the caller.
    CallerErrored {
        run_id: RunId,
        project_id: uuid::Uuid,
        offset: u64,
        message: String,
        at_unix: u64,
    },
    /// The caller is gone (response complete OR disconnected). Last
    /// event in the caller stream; replay cursors stop here. `dropped`
    /// names the program's messages still waiting to go out when the
    /// exchange ended, which the caller never received.
    // SYNC: CallerDisconnected <-> crates/weft-journal/src/events.rs CallerDisconnected, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'disconnected', extension-vscode/src/runFollower.ts DispatcherEvent 'caller_disconnected'
    CallerDisconnected {
        run_id: RunId,
        project_id: uuid::Uuid,
        offset: u64,
        reason: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        dropped: Vec<u64>,
        at_unix: u64,
    },
    /// Graph-level participation: a node was wired to a bus. Derived
    /// from the pulses the fold puts on the wires whose value carries a
    /// bus marker on a `Bus` port: both source and target nodes are
    /// participants.
    /// `ephemeral` is sniffed from the marker JSON itself (which
    /// encodes the bus's mode) so the inspector can render a mode
    /// badge in the panel header without a separate journal event.
    BusParticipant {
        run_id: RunId,
        project_id: uuid::Uuid,
        bus_id: String,
        node_id: String,
        ephemeral: bool,
    },
    /// A journal row could not be applied during fold (corruption).
    /// Surfaced one-shot at replay time per affected row so the
    /// inspector can render a muted "N journal rows corrupted"
    /// line. Not alarming by design: corrupt rows are a real but
    /// rare event the user only investigates if they look.
    JournalCorruption {
        run_id: RunId,
        project_id: uuid::Uuid,
        site: crate::primitive::CorruptionSite,
        reason: String,
    },
}

impl DispatcherEvent {
    pub fn project_id(&self) -> uuid::Uuid {
        match self {
            Self::RunStarted { project_id, .. }
            | Self::RunCompleted { project_id, .. }
            | Self::RunFailed { project_id, .. }
            | Self::RunCancelled { project_id, .. }
            | Self::RunTagged { project_id, .. }
            | Self::RunDeleted { project_id, .. }
            | Self::RunParked { project_id, .. }
            | Self::NodeStarted { project_id, .. }
            | Self::NodeSuspended { project_id, .. }
            | Self::NodeResumed { project_id, .. }
            | Self::WaitEnded { project_id, .. }
            | Self::NodeCancelled { project_id, .. }
            | Self::NodeCompleted { project_id, .. }
            | Self::NodeFailed { project_id, .. }
            | Self::NodeSkipped { project_id, .. }
            | Self::ScopeDecided { project_id, .. }
            | Self::LoopInstantiated { project_id, .. }
            | Self::LoopIterationLaunched { project_id, .. }
            | Self::LoopOutFired { project_id, .. }
            | Self::LoopTerminated { project_id, .. }
            | Self::CostReported { project_id, .. }
            | Self::FileEdited { project_id, .. }
            | Self::TriggerUrlChanged { project_id, .. }
            | Self::ProjectRegistered { project_id, .. }
            | Self::ProjectActivated { project_id }
            | Self::ProjectDeactivated { project_id }
            | Self::ProjectTransitionChanged { project_id, .. }
            | Self::InfraStatusChanged { project_id, .. }
            | Self::InfraFlaky { project_id, .. }
            | Self::InfraRecovered { project_id, .. }
            | Self::InfraTerminated { project_id, .. }
            | Self::BusJoined { project_id, .. }
            | Self::BusLeft { project_id, .. }
            | Self::BusWindow { project_id, .. }
            | Self::BusClosed { project_id, .. }
            | Self::BusParticipant { project_id, .. }
            | Self::CallerConnected { project_id, .. }
            | Self::CallerWindow { project_id, .. }
            | Self::CallerErrored { project_id, .. }
            | Self::CallerDisconnected { project_id, .. }
            | Self::JournalCorruption { project_id, .. } => *project_id,
        }
    }

    /// The loop and call frames of the one place this event is about: a
    /// firing (a node's lifecycle, a cost or a file edit at that firing)
    /// or a scope's copy being decided; `None` for an event about the
    /// run, the project, a loop as a whole, or anything else that is not
    /// one place.
    pub fn place_frames(&self) -> Option<&LoopFrames> {
        match self {
            Self::NodeStarted { frames, .. }
            | Self::NodeSuspended { frames, .. }
            | Self::NodeResumed { frames, .. }
            | Self::WaitEnded { frames, .. }
            | Self::NodeCancelled { frames, .. }
            | Self::NodeCompleted { frames, .. }
            | Self::NodeFailed { frames, .. }
            | Self::NodeSkipped { frames, .. }
            | Self::CostReported { frames, .. }
            | Self::FileEdited { frames, .. }
            | Self::ScopeDecided { frames, .. } => Some(frames),
            Self::RunStarted { .. }
            | Self::RunCompleted { .. }
            | Self::RunFailed { .. }
            | Self::RunCancelled { .. }
            | Self::RunTagged { .. }
            | Self::RunDeleted { .. }
            | Self::RunParked { .. }
            | Self::LoopInstantiated { .. }
            | Self::LoopIterationLaunched { .. }
            | Self::LoopOutFired { .. }
            | Self::LoopTerminated { .. }
            | Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::ProjectDeactivated { .. }
            | Self::ProjectTransitionChanged { .. }
            | Self::InfraStatusChanged { .. }
            | Self::InfraFlaky { .. }
            | Self::InfraRecovered { .. }
            | Self::InfraTerminated { .. }
            | Self::BusJoined { .. }
            | Self::BusLeft { .. }
            | Self::BusWindow { .. }
            | Self::BusClosed { .. }
            | Self::BusParticipant { .. }
            | Self::CallerConnected { .. }
            | Self::CallerWindow { .. }
            | Self::CallerErrored { .. }
            | Self::CallerDisconnected { .. }
            | Self::JournalCorruption { .. } => None,
        }
    }

    /// Whether this event ends its run (a cancel is an end too).
    // SYNC: DispatcherEvent::is_run_terminal <-> crates/weft-journal/src/events.rs RunEvent::is_run_terminal, crates/weft-journal/src/events.rs RUN_TERMINAL_KINDS_SQL
    pub fn is_run_terminal(&self) -> bool {
        matches!(
            self,
            Self::RunCompleted { .. } | Self::RunFailed { .. } | Self::RunCancelled { .. }
        )
    }

    pub fn run_id(&self) -> Option<RunId> {
        match self {
            Self::RunStarted { run_id, .. }
            | Self::RunCompleted { run_id, .. }
            | Self::RunFailed { run_id, .. }
            | Self::RunCancelled { run_id, .. }
            | Self::RunTagged { run_id, .. }
            | Self::RunDeleted { run_id, .. }
            | Self::RunParked { run_id, .. }
            | Self::NodeStarted { run_id, .. }
            | Self::NodeSuspended { run_id, .. }
            | Self::NodeResumed { run_id, .. }
            | Self::WaitEnded { run_id, .. }
            | Self::NodeCancelled { run_id, .. }
            | Self::NodeCompleted { run_id, .. }
            | Self::NodeFailed { run_id, .. }
            | Self::NodeSkipped { run_id, .. }
            | Self::ScopeDecided { run_id, .. }
            | Self::LoopInstantiated { run_id, .. }
            | Self::LoopIterationLaunched { run_id, .. }
            | Self::LoopOutFired { run_id, .. }
            | Self::LoopTerminated { run_id, .. }
            | Self::CostReported { run_id, .. }
            | Self::FileEdited { run_id, .. }
            | Self::BusJoined { run_id, .. }
            | Self::BusLeft { run_id, .. }
            | Self::BusWindow { run_id, .. }
            | Self::BusClosed { run_id, .. }
            | Self::BusParticipant { run_id, .. }
            | Self::CallerConnected { run_id, .. }
            | Self::CallerWindow { run_id, .. }
            | Self::CallerErrored { run_id, .. }
            | Self::CallerDisconnected { run_id, .. }
            | Self::JournalCorruption { run_id, .. } => Some(*run_id),
            Self::TriggerUrlChanged { .. }
            | Self::ProjectRegistered { .. }
            | Self::ProjectActivated { .. }
            | Self::ProjectDeactivated { .. }
            | Self::ProjectTransitionChanged { .. }
            | Self::InfraStatusChanged { .. }
            | Self::InfraFlaky { .. }
            | Self::InfraRecovered { .. }
            | Self::InfraTerminated { .. } => None,
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    #[test]
    fn a_started_run_names_its_fields_in_camel_case_under_a_snake_case_kind() {
        let event = DispatcherEvent::RunStarted {
            run_id: uuid::Uuid::nil(),
            entry_node: "hook".into(),
            phase: crate::context::Phase::Fire,
            subgraph: None,
            seed: None,
            project_id: uuid::Uuid::from_u128(1),
            instance: None,
            at_unix: 5,
        };
        let wire = serde_json::to_value(&event).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "kind": "run_started",
                "runId": "00000000-0000-0000-0000-000000000000",
                "entryNode": "hook",
                "phase": "fire",
                "projectId": "00000000-0000-0000-0000-000000000001",
                "atUnix": 5,
            })
        );
        let back = serde_json::to_value(serde_json::from_value::<DispatcherEvent>(wire.clone()).unwrap()).unwrap();
        assert_eq!(back, wire);
    }

    #[test]
    fn identities_survive_projection_and_wire_round_trips() {
        let event = DispatcherEvent::RunCompleted {
            run_id: uuid::Uuid::nil(), project_id: uuid::Uuid::from_u128(0x100),
            outputs: serde_json::json!({}), at_unix: 1,
        };
        let record = IdentifiedEvent::recorded(42, 3, event);
        let projected = record.clone().project(|event| vec![event.clone(), event]);
        assert_ne!(projected[0].event_id, projected[1].event_id);
        let replay = record.project(|event| vec![event.clone(), event]);
        assert_eq!(serde_json::to_value(&projected).unwrap(), serde_json::to_value(replay).unwrap());
        let json = serde_json::to_value(&projected[0]).unwrap();
        assert_eq!(json["eventId"], "journal:42:3:0");
        assert_eq!(json["kind"], "run_completed");
        let decoded: LiveEvent = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.event_id, projected[0].event_id);
        let distinct = IdentifiedEvent::recorded(42, 4, decoded.event).project(|event| vec![event]);
        assert_ne!(distinct[0].event_id, projected[0].event_id);
    }
}
