//! `ctx.costs()` and `ctx.runs()`: a project's runs and what they cost.
//! `ctx.run_by_id(id)`: one run, named by its id, wherever it ran among
//! the owner's projects.

use crate::program::runs::{EventsRequest, LogsRequest, OutputsRequest, PageMark, RunCall, RunList, RunStopped, WakeRequest};
use crate::program::{
    CleanOutcome, CostFilter, CostRecord, DeletedRun, NodeState, PaidBy, RunDetail, RunEvents, RunFilter, RunLogs, RunPage, RunsCounted,
    StillGoing, MAX_RUNS_PAGE,
};
use crate::run_spec::Expected;
use crate::RunId;

use crate::error::{WeftError, WeftResult};
use crate::program::ProgramCall;
use crate::tag::StopSelf;

use super::{instance_id, ProjectCalls};

impl<'a> ProjectCalls<'a> {
    /// What the project's calls cost, narrowed by the filters chained on
    /// it, read with `.list()`. Weft reports the price it measured; what
    /// anybody is billed is the program's to decide.
    pub fn costs(self) -> CostQuery<'a> {
        CostQuery { at: self, instance: None, filter: CostFilter::default() }
    }

    /// The project's runs, narrowed by the filters chained on it: listed
    /// with `.list(..)`, counted with `.count()`, deleted with `.clean(..)`.
    pub fn runs(self) -> RunQuery<'a> {
        RunQuery { at: self, instance: None, list: RunList::default() }
    }
}

/// See [`ProjectCalls::costs`].
pub struct CostQuery<'a> {
    at: ProjectCalls<'a>,
    instance: Option<String>,
    filter: CostFilter,
}

impl CostQuery<'_> {
    /// Costs of that instance's runs.
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instance = Some(id.into());
        self
    }

    /// Costs one node booked, named the way the program writes it.
    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.filter.node = Some(node.into());
        self
    }

    /// Costs of one service (`openrouter`, `elevenlabs`).
    pub fn service(mut self, service: impl Into<String>) -> Self {
        self.filter.service = Some(service.into());
        self
    }

    /// Costs paid from one kind of credential: the runtime's, the
    /// author's, or an instance's own.
    pub fn paid_by(mut self, paid_by: PaidBy) -> Self {
        self.filter.paid_by = Some(paid_by);
        self
    }

    /// Costs of one run (`ctx.run_id` for this one).
    pub fn run(mut self, run_id: RunId) -> Self {
        self.filter.run = Some(run_id);
        self
    }

    /// Costs booked at or after this unix second.
    pub fn since(mut self, unix: u64) -> Self {
        self.filter.since_unix = Some(unix);
        self
    }

    pub async fn list(mut self) -> WeftResult<Vec<CostRecord>> {
        self.filter.instance = instance_id(&self.instance)?;
        self.at.call(ProgramCall::Runs(RunCall::Costs { filter: self.filter }), StopSelf::Keep).await
    }
}

/// See [`ProjectCalls::runs`]. The filters up to [`Self::older_than`] narrow
/// a listing, a count and a clean alike; the ones after it (`weft runs`'s
/// own: phase, a start window, a node fired, words recorded, paging) only
/// a listing, and a count or a clean given one is refused naming it.
pub struct RunQuery<'a> {
    at: ProjectCalls<'a>,
    instance: Option<String>,
    /// The filters, and the listing's own, with no limit yet.
    list: RunList,
}

impl<'a> RunQuery<'a> {
    /// Runs for that instance.
    pub fn instance(mut self, id: impl Into<String>) -> Self {
        self.instance = Some(id.into());
        self
    }

    /// Runs standing this way: `running` (not ended, a run parked on a
    /// wait included), `waiting_for_input` (parked on a wait),
    /// `completed`, `failed` or `cancelled`. Any other word is refused
    /// naming these, rather than matching nothing.
    pub fn status(mut self, status: &str) -> WeftResult<Self> {
        let parsed = crate::program::RunStatus::parse(status).ok_or_else(|| WeftError::Input(format!(
            "runs: '{status}' is not a run status; use one of: {}",
            crate::program::RunStatus::accepted()
        )))?;
        self.list.filter.status = Some(parsed);
        Ok(self)
    }

    /// Runs started by this node (the trigger that fired, or the node a
    /// run was started from).
    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.list.filter.node = Some(node.into());
        self
    }

    /// Runs carrying this tag (`ctx.tag_run`), normalized the
    /// same way, so the value a run tagged itself with finds it. An
    /// empty tag is refused.
    pub fn tag(mut self, tag: &str) -> WeftResult<Self> {
        let tag = crate::tag::normalize_tag(tag).map_err(|e| WeftError::Input(format!("runs: {e}")))?;
        self.list.filter.tag = Some(tag);
        Ok(self)
    }

    /// Runs started at least this long ago.
    pub fn older_than(mut self, age: std::time::Duration) -> Self {
        self.list.filter.older_than_secs = Some(age.as_secs());
        self
    }

    /// Listing only: runs of this phase (`weft runs --phase`).
    pub fn phase(mut self, phase: crate::primitive::Phase) -> Self {
        self.list.phase = Some(phase);
        self
    }

    /// Listing only: runs started at or after this unix second (`weft runs
    /// --since`).
    pub fn since(mut self, unix: u64) -> Self {
        self.list.started_after = Some(unix);
        self
    }

    /// Listing only: finished runs in which this node fired, wherever it
    /// sits (`weft runs --through`), once their search entry is built.
    pub fn through(mut self, node: impl Into<String>) -> Self {
        self.list.through = Some(node.into());
        self
    }

    /// Listing only: finished runs that recorded every one of these words
    /// (`weft runs --search`), once their search entry is built.
    pub fn search(mut self, words: impl Into<String>) -> Self {
        self.list.search = Some(words.into());
        self
    }

    /// Listing only: skip this many runs before the page (`weft runs
    /// --offset`).
    pub fn offset(mut self, offset: u32) -> Self {
        self.list.offset = offset;
        self
    }

    /// Listing only: the page after this run, the last of the page before
    /// (`RunSummary::started_at` and `run_id`): a walk that hands each
    /// page's last run back reaches every run once, whatever starts or ends
    /// meanwhile.
    pub fn after(mut self, started_at: u64, run: RunId) -> Self {
        self.list.after = Some(PageMark { started_at, run });
        self
    }

    /// Delete them. A run still going is never deleted from under itself:
    /// `StillGoing::Cancel` stops it (its rows go with the next clean),
    /// `Leave` leaves it running. This run is never deleted; with
    /// `StopSelf::Include` and matching the filters, it is stopped.
    pub async fn clean(self, running: StillGoing, stop_self: StopSelf) -> WeftResult<CleanOutcome> {
        let (at, filter) = self.shared_filter("clean")?;
        at.call(ProgramCall::Runs(RunCall::Clean { filter, running }), stop_self).await
    }

    /// The newest `limit` of them (1 to [`MAX_RUNS_PAGE`]), newest first,
    /// with how many match in all (`total`), so a listing that stopped
    /// short says so. A run parked on a wait reads `waiting_for_input`.
    pub async fn list(mut self, limit: u32) -> WeftResult<RunPage> {
        if !(1..=MAX_RUNS_PAGE).contains(&limit) {
            return Err(WeftError::Input(format!(
                "runs list: a limit of {limit} is out of range; give 1 to {MAX_RUNS_PAGE}, and narrow the filters to reach older runs"
            )));
        }
        self.list.filter.instance = instance_id(&self.instance)?;
        let list = RunList { limit, ..self.list };
        self.at.call(ProgramCall::Runs(RunCall::List(list)), StopSelf::Keep).await
    }

    /// How many of them there are.
    pub async fn count(self) -> WeftResult<u64> {
        let (at, filter) = self.shared_filter("count")?;
        let answer: RunsCounted = at.call(ProgramCall::Runs(RunCall::Count { filter }), StopSelf::Keep).await?;
        Ok(answer.total)
    }

    /// The filters a count and a clean take, refusing a listing's own.
    fn shared_filter(self, verb: &str) -> WeftResult<(ProjectCalls<'a>, RunFilter)> {
        let RunList { mut filter, limit: _, offset, phase, started_after, started_before, through, search, after } = self.list;
        let listing_only = [
            ("phase", phase.is_some()),
            ("since", started_after.is_some() || started_before.is_some()),
            ("through", through.is_some()),
            ("search", search.is_some()),
            ("offset", offset != 0),
            ("after", after.is_some()),
        ];
        if let Some((name, _)) = listing_only.into_iter().find(|(_, set)| *set) {
            return Err(WeftError::Input(format!("runs {verb}: `.{name}(..)` narrows a listing only; drop it, or `.list(..)` instead")));
        }
        filter.instance = instance_id(&self.instance)?;
        Ok((self.at, filter))
    }
}

/// See [`RunContext::run_by_id`](super::super::RunContext::run_by_id).
#[derive(Clone, Copy)]
pub struct RunByIdCalls<'a> {
    /// The calling run's own project: the call travels as it, and the run
    /// named is held to the owner, not to a project.
    pub(super) at: ProjectCalls<'a>,
    pub(super) run: RunId,
}

impl<'a> RunByIdCalls<'a> {
    /// Stop the run, as `weft stop` does. The answer says whether it had
    /// already ended (and how), so a stop asked again answers the same.
    /// Naming this run stops it: the call does not return, and the run
    /// ends cancelled with nothing after it running.
    pub async fn stop(self) -> WeftResult<RunStopped> {
        self.at.call(ProgramCall::Runs(RunCall::Stop { run: self.run }), StopSelf::Keep).await
    }

    /// Resolve the wait `node` holds in the run now, instead of waiting
    /// for what it waits for, as `weft wake` does. Only a wait that can be
    /// woken by hand (a timer) is; one that expects a value (a form) is
    /// refused naming its kind. A node waiting in several loop iterations
    /// is refused listing them: name one with [`Self::wake_in`].
    pub async fn wake(self, node: impl Into<String>) -> WeftResult<()> {
        self.wake_with(node.into(), WakeRequest::default()).await
    }

    /// [`Self::wake`] for the wait in loop iterations `iteration` (`[3]`,
    /// or `[3, 0]` inside an inner loop).
    pub async fn wake_in(self, node: impl Into<String>, iteration: &[u32]) -> WeftResult<()> {
        self.wake_with(node.into(), WakeRequest { iteration: Some(iteration_text(iteration)) }).await
    }

    async fn wake_with(self, node: String, request: WakeRequest) -> WeftResult<()> {
        self.at.call::<serde_json::Value>(ProgramCall::Runs(RunCall::Wake { run: self.run, node, request }), StopSelf::Keep).await.map(drop)
    }

    /// The last `lines` lines of the run's log (1 to the ceiling a refusal
    /// names), as `weft logs --limit` reads them.
    pub async fn logs(self, lines: u32) -> WeftResult<RunLogs> {
        let request = LogsRequest { limit: Some(lines) };
        self.at.call(ProgramCall::Runs(RunCall::Logs { run: self.run, request }), StopSelf::Keep).await
    }

    /// The run's events, narrowed by the filters chained on it, read with
    /// `.read()`, as `weft events` reads them.
    pub fn events(self) -> RunEventsQuery<'a> {
        RunEventsQuery { of: self, request: EventsRequest::default() }
    }

    /// Everything the run produced, as `weft freeze` reads it: every
    /// output wire, and every place it covered. `focus` names places (nodes
    /// or loops, spelled the way the run's program writes them) the answer
    /// puts first; each must have run.
    pub async fn outputs(self, focus: &[&str]) -> WeftResult<Expected> {
        let request = OutputsRequest { expect: (!focus.is_empty()).then(|| focus.join(",")) };
        self.at.call(ProgramCall::Runs(RunCall::Outputs { run: self.run, request }), StopSelf::Keep).await
    }

    /// The run as the listing shows it, and the waits it is parked on.
    pub async fn detail(self) -> WeftResult<RunDetail> {
        self.at.call(ProgramCall::Runs(RunCall::Detail { run: self.run }), StopSelf::Keep).await
    }

    /// Where one node of the run stands: pending, running, waiting (with
    /// the raw last answer its wait saw), completed, failed (with the
    /// error), skipped or cancelled, read from the run's record. `node` is
    /// spelled the way `--from` takes it (`render`, `one.render` inside the
    /// file the site `one` includes); a node inside a loop fires once per
    /// iteration and is refused, while the loop itself is read.
    pub async fn node_state(self, node: impl Into<String>) -> WeftResult<NodeState> {
        self.at.call(ProgramCall::Runs(RunCall::NodeState { run: self.run, node: node.into() }), StopSelf::Keep).await
    }

    /// Delete the run, as `weft clean <run>` does: its record, its stored
    /// files, and the version it leaves bare. A run still going is refused
    /// (stop it first), and so is this run.
    pub async fn delete(self) -> WeftResult<DeletedRun> {
        self.at.call(ProgramCall::Runs(RunCall::Delete { run: self.run }), StopSelf::Keep).await
    }
}

/// See [`RunByIdCalls::events`].
pub struct RunEventsQuery<'a> {
    of: RunByIdCalls<'a>,
    request: EventsRequest,
}

impl RunEventsQuery<'_> {
    /// Only the rows about this node or group, spelled the way the
    /// program the run ran is written.
    pub fn node(mut self, node: impl Into<String>) -> Self {
        self.request.node = Some(node.into());
        self
    }

    /// Only rows of this kind (`node_failed`, `run_completed`).
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.request.kind = Some(kind.into());
        self
    }

    /// Only rows inside these loop iterations of the node named (`[3]`, or
    /// `[3, 0]` inside an inner loop).
    pub fn iteration(mut self, iteration: &[u32]) -> Self {
        self.request.iteration = Some(iteration_text(iteration));
        self
    }

    pub async fn read(self) -> WeftResult<RunEvents> {
        let call = RunCall::Events { run: self.of.run, request: self.request };
        self.of.at.call(ProgramCall::Runs(call), StopSelf::Keep).await
    }
}

/// Loop iterations the way the wire takes them (`3.0`).
fn iteration_text(iteration: &[u32]) -> String {
    iteration.iter().map(u32::to_string).collect::<Vec<_>>().join(".")
}
