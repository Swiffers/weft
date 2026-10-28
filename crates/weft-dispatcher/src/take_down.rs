//! One engine for every take-down.
//!
//! `weft deactivate` (the triggers `--trigger` / `--instance` /
//! `--all-instances` name), `weft resync`'s first half, the infra verbs
//! that take down the triggers whose cut contains their copies, the health
//! loop taking down what reaches a broken copy, `weft maintenance on`
//! (`crate::maintenance`), project removal, and a
//! program's own `ctx.trigger(..).deactivate(..)` calls all send the same
//! request: a TARGET, a [`TakeDown`] (the door, what is held, what is
//! running: `weft_core::take_down`), and the run that asked, if one did.
//!
//! The three answers are carried out in their order, so nothing arriving
//! slips between them: the door first (the lifecycle write, which also
//! drops what is held when asked to, in its own transaction), then the
//! paused runs a `held: drop` cancels, then the running runs a `running:
//! cancel` stops. Under `wait` this leaves them going: the wait is the
//! command's own, after this (`crate::drain`: an infra command's before its
//! copies go, a resync's before its new version takes over, a
//! deactivate's before it answers). The trigger is down at once either
//! way.
//!
//! [`affected_runs`] decides, in one place, which runs a target reaches:
//! the runs its triggers fired (for their instance), or every run for the
//! whole project. The run that asked is never among them: a program that
//! takes its own trigger down keeps running unless it asked to be stopped
//! too (`StopSelf::Include`), which the asking side carries out once this
//! has run, the same way `ctx.stop_tagged` does.

use axum::http::StatusCode;

use weft_core::activation::ActivationKey;
use weft_core::deactivation::TakeDownReport;
use weft_core::instance::InstanceId;
use weft_core::projects::HeldWork;
use weft_core::take_down::{Door, Held, Running, TakeDown};
use weft_core::RunId;
use weft_task_store::db::Client as _;

pub use crate::activation_store::DownCause;
use crate::activation_store::{ActivationLifecycle, Forget, LifecycleWrite, ProjectStatus, SignalsGoing};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// What a take-down aims at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakeDownTarget {
    /// These activations, and the runs their triggers fired.
    Activations(Vec<ActivationKey>),
    /// Everything the project has: every activation of every owner, and
    /// every run whatever started it. Project removal.
    WholeProject,
}

/// Who asks for a take-down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeDownAsker {
    /// A person, a lifecycle command or one of the dispatcher's loops.
    Outside,
    /// A program's run: never among the runs the take-down reaches.
    Run(RunId),
    /// A resync taking down the triggers it brings back, under its drive
    /// (`crate::task_kinds::resync::ResyncPlan::driver`): it goes through
    /// only while that drive holds the project.
    Resync(uuid::Uuid),
}

impl TakeDownAsker {
    /// The run that asked, if one did.
    pub fn run(self) -> Option<RunId> {
        match self {
            TakeDownAsker::Run(run) => Some(run),
            TakeDownAsker::Outside | TakeDownAsker::Resync(_) => None,
        }
    }

    /// The resync drive that asked, if one did.
    fn resync(self) -> Option<uuid::Uuid> {
        match self {
            TakeDownAsker::Resync(driver) => Some(driver),
            TakeDownAsker::Outside | TakeDownAsker::Run(_) => None,
        }
    }
}

/// How a take-down ended, when nothing refused it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TakenDown {
    Done(Taken),
    /// The project does not exist.
    ProjectGone,
    /// It was a resync's ([`TakeDownAsker::Resync`]), and that drive no
    /// longer holds the project: nothing was taken down.
    ResyncGone,
}

/// What a take-down did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Taken {
    /// The activations it took down (the ones among its target that had a
    /// row).
    pub keys: Vec<ActivationKey>,
    /// What they held as they went down: kept, or dropped.
    pub held: HeldWork,
    /// Runs going right now that it reached: cancelled, or left going.
    pub runs: u64,
    /// The infra commands it issued with the triggers
    /// ([`DownCause::Infra`]), first issued first.
    pub commands: Vec<i64>,
    /// The entry tokens of the activations it took down, as they stood
    /// before: what a wait for their runs counts at the door
    /// (`crate::drain::Reaching`).
    pub tokens: Vec<String>,
}

impl Taken {
    /// Add what another take-down did (another owner's, of one verb).
    pub fn add(&mut self, other: Taken) {
        self.keys.extend(other.keys);
        self.held += other.held;
        self.runs += other.runs;
        self.commands.extend(other.commands);
        self.tokens.extend(other.tokens);
    }

    /// What a person reads of it, for `take_down`.
    pub fn report(&self, take_down: TakeDown) -> TakeDownReport {
        TakeDownReport { take_down, triggers: weft_core::activation::by_owner(&self.keys), held: self.held, runs: self.runs }
    }
}

/// What a take-down needs to know about one live run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFacts {
    pub run_id: RunId,
    /// Who the run is for.
    pub instance: Option<InstanceId>,
    /// The trigger that fired it (`None` for a run started by hand).
    pub fired_by: Option<String>,
    /// Paused on a wait (`state = 'parked'`), rather than working now.
    pub suspended: bool,
}

impl RunFacts {
    /// Whether this run belongs to the activation `key`: its trigger
    /// fired it, for its owner.
    pub fn fired_by(&self, key: &ActivationKey) -> bool {
        self.fired_by.as_deref() == Some(key.trigger.as_str()) && self.instance.as_ref() == key.instance()
    }
}

/// The live runs `target` reaches, never the run that asked (`asked_by`):
/// that one's fate is its own `StopSelf`, carried out by the asker.
pub fn affected_runs<'a>(target: &TakeDownTarget, runs: &'a [RunFacts], asked_by: Option<RunId>) -> Vec<&'a RunFacts> {
    runs.iter()
        .filter(|run| Some(run.run_id) != asked_by)
        .filter(|run| match target {
            TakeDownTarget::WholeProject => true,
            TakeDownTarget::Activations(keys) => keys.iter().any(|key| run.fired_by(key)),
        })
        .collect()
}

/// Which of `affected` the take-down cancels: the paused ones when it
/// drops what is held, the ones going when it cancels what runs.
pub fn runs_to_cancel<'a>(take_down: &TakeDown, affected: &[&'a RunFacts]) -> Vec<&'a RunFacts> {
    affected
        .iter()
        .copied()
        .filter(|run| match run.suspended {
            true => take_down.held == Held::Drop,
            false => take_down.running == Running::Cancel,
        })
        .collect()
}

/// What the lifecycle write deletes in its own transaction: every signal
/// of the project when it is removed, every signal the activations govern
/// when their door refuses with nothing kept, only what they hold when the
/// door goes on taking but what is held is dropped, nothing otherwise.
/// A door that refuses with what is held kept takes `places` too, the
/// fresh state of each entry signal that keeps a place in what it reads
/// (asked of the listener when [`places_go`] says so, empty otherwise).
pub fn signals_going<'a>(target: &TakeDownTarget, take_down: &TakeDown, asked_by: Option<RunId>, places: &'a [crate::store::FreshPlace]) -> SignalsGoing<'a> {
    match (target, take_down.door, take_down.held) {
        (TakeDownTarget::WholeProject, _, _) => SignalsGoing::Project { except: asked_by },
        (TakeDownTarget::Activations(_), Door::Refuse, Held::Drop) => SignalsGoing::Activations,
        (TakeDownTarget::Activations(_), _, Held::Drop) => SignalsGoing::HeldArrivals,
        (TakeDownTarget::Activations(_), Door::Refuse, Held::Keep) if !places.is_empty() => SignalsGoing::Places(places),
        (TakeDownTarget::Activations(_), _, Held::Keep) => SignalsGoing::Kept,
    }
}

/// Whether a take-down takes the places its triggers' entry signals keep
/// in what they read (a poll's cursor, what a folder held), so nothing that
/// arrives behind its door is ever caught up: a door that refuses with
/// what is held kept (one that drops it deletes the signals). Not weft's
/// own maintenance, whose door refuses while nobody refused anything: what
/// came meanwhile is caught up once it is lifted.
pub fn places_go(target: &TakeDownTarget, take_down: &TakeDown, cause: &DownCause<'_>) -> bool {
    matches!(target, TakeDownTarget::Activations(_))
        && take_down.door == Door::Refuse
        && take_down.held == Held::Keep
        && !matches!(cause, DownCause::Maintenance)
}

/// The fresh places of the entry signals of `keys` whose kind keeps one
/// ([`places_go`]), each read at the version its row stands at.
async fn fresh_places(state: &DispatcherState, project_id: uuid::Uuid, keys: &[ActivationKey]) -> anyhow::Result<Vec<crate::store::FreshPlace>> {
    let entries: Vec<crate::store::SignalRegistration> = crate::store::postgres::activation_signals(&state.pg_pool, project_id, keys)
        .await?
        .into_iter()
        .filter(|signal| !signal.is_resume)
        .collect();
    state.listener.fresh_places(&entries).await
}

/// Whether a take-down's door makes triggers listen again that did not
/// (`before`, how they stood): one that refused, or whose window ended,
/// was let go of by the listener, so a door that takes or hides from now
/// on has their signals brought up again. Pure.
fn listens_again(before: &[&ActivationLifecycle], landing: &ActivationLifecycle, now_unix: i64) -> bool {
    use weft_core::arrival::Arrival;
    landing.standing().arrival(now_unix) != Arrival::Refused
        && before.iter().any(|lifecycle| lifecycle.standing().arrival(now_unix) == Arrival::Refused)
}

/// Every run of the project that has not ended, with what a take-down
/// needs to know.
pub(crate) async fn live_runs(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<RunFacts>> {
    let rows: Vec<(RunId, Option<String>, Option<String>, bool)> = state
        .pg_pool
        .all(
            "SELECT run_id, instance_id, fired_by, state = 'parked' FROM run \
             WHERE project_id = $1 AND state <> 'ended' ORDER BY started_at, run_id",
            &[&project_id],
        )
        .await?;
    rows.into_iter()
        .map(|(run_id, instance, fired_by, suspended)| {
            let instance = instance.map(InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("run {run_id} holds a broken instance: {e}"))?;
            Ok(RunFacts { run_id, instance, fired_by, suspended })
        })
        .collect()
}

/// Take `target` down as `take_down` says, as `asker` asked, marked by
/// `cause` (an infra cause's commands are issued with it).
///
/// Refused for a target that is mid-activation (cancel that first) or a
/// project mid-build. The lifecycle write comes first, guarded, so a
/// refused write leaves nothing done: it sets the door, deletes what goes
/// with it and takes the places a refusing door takes ([`signals_going`]),
/// counts what was held and issues an infra cause's commands, in one
/// transaction. Then the listeners let go of the signals that went (or
/// stopped listening), or bring up again those a door that takes now
/// listens for ([`listens_again`]), and the runs the answers reach are
/// cancelled ([`runs_to_cancel`]). Activations of which none has a row take nothing
/// down: only an infra cause's commands are issued.
pub async fn take_down(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    target: &TakeDownTarget,
    take_down: &TakeDown,
    cause: DownCause<'_>,
    asker: TakeDownAsker,
) -> Result<TakenDown, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let asked_by = asker.run();
    let Some(transition) = state.projects.transition(project_id).await.map_err(|e| internal("transition", e))? else {
        return Ok(TakenDown::ProjectGone);
    };
    let existing = state.activations.list(project_id).await.map_err(|e| internal("activations", e))?;
    let keys: Vec<ActivationKey> = match target {
        TakeDownTarget::WholeProject => existing.iter().map(|a| a.key.clone()).collect(),
        TakeDownTarget::Activations(keys) => keys.iter().filter(|k| existing.iter().any(|a| &a.key == *k)).cloned().collect(),
    };
    if keys.is_empty() && matches!(target, TakeDownTarget::Activations(_)) {
        let commands = match cause {
            DownCause::Person | DownCause::Health(_) | DownCause::Maintenance => Vec::new(),
            DownCause::Infra(commands) => match commands.issue(&state.pg_pool, project_id).await.map_err(|e| internal("issue the infra commands", e))? {
                Some(crate::infra_lifecycle_command::Issued::Commands(ids)) => ids,
                Some(crate::infra_lifecycle_command::Issued::UpgradeInFlight(command)) => {
                    return Err(crate::infra_lifecycle_command::upgrade_in_flight_refusal(command))
                }
                None => return Ok(TakenDown::ProjectGone),
            },
        };
        return Ok(TakenDown::Done(Taken { commands, ..Taken::default() }));
    }
    if transition.is_building() {
        return Err((
            StatusCode::CONFLICT,
            format!("cannot take triggers down: project is {}; cancel the build first", transition.as_str()),
        ));
    }
    if let Some(busy) = existing.iter().find(|a| keys.contains(&a.key) && a.lifecycle.status == ProjectStatus::Activating) {
        return Err((
            StatusCode::CONFLICT,
            format!("cannot take trigger {} down: it is activating; cancel the activation first", busy.key),
        ));
    }
    // 1. The door, and what goes with it, in one guarded write: when it is
    // refused (a trigger began activating, a build started) or the project
    // is gone, nothing has been cancelled, deleted or issued yet.
    let now = crate::lease::now_unix();
    let landing = ActivationLifecycle::down(take_down, now);
    let before: Vec<&ActivationLifecycle> = existing.iter().filter(|a| keys.contains(&a.key)).map(|a| &a.lifecycle).collect();
    let reopens = listens_again(&before, &landing, now);
    // A door that refuses takes the places its entry signals keep, in the
    // same write: each fresh place is computed from its row as read, and
    // written only while the row is still there, so a wake claiming in
    // between has this ask again. Each round lost is a claim some wake
    // made, so running out of rounds means its wakes never stop claiming.
    const ROUNDS: usize = 5;
    let takes_places = places_go(target, take_down, &cause);
    let mut round = 0;
    let (keys, tokens, unlisten, held, commands, rows_stay) = loop {
        round += 1;
        let places = match takes_places {
            true => fresh_places(state, project_id, &keys).await.map_err(|e| internal("the places the door takes", e))?,
            false => Vec::new(),
        };
        let signals = signals_going(target, take_down, asked_by, &places);
        let rows_stay = matches!(signals, SignalsGoing::Kept | SignalsGoing::Places(_));
        match state
            .activations
            .set_lifecycle_guarded(project_id, &keys, &landing, signals, Forget::of(take_down, &cause), cause, asker.resync())
            .await
            .map_err(|e| internal("set lifecycle", e))?
        {
            LifecycleWrite::Applied { keys: taken, tokens, unlisten, held, commands } => break (taken, tokens, unlisten, held, commands, rows_stay),
            LifecycleWrite::StateMoved if round < ROUNDS => continue,
            LifecycleWrite::StateMoved => {
                return Err(internal(
                    "set lifecycle",
                    anyhow::anyhow!("the signals' state moved under each of {ROUNDS} take-downs in a row: their wakes keep claiming them"),
                ))
            }
            LifecycleWrite::NotFound => return Ok(TakenDown::ProjectGone),
            LifecycleWrite::ResyncGone => return Ok(TakenDown::ResyncGone),
            LifecycleWrite::UpgradeInFlight(command) => return Err(crate::infra_lifecycle_command::upgrade_in_flight_refusal(command)),
            LifecycleWrite::Rejected { blocker } => {
                return Err((StatusCode::CONFLICT, format!("cannot take triggers down: {blocker}; cancel it first")));
            }
        }
    };
    // Every one it was aimed at was down already (taken down since it was
    // listed): nothing else of the project changed.
    if keys.is_empty() && matches!(target, TakeDownTarget::Activations(_)) {
        return Ok(TakenDown::Done(Taken { commands, ..Taken::default() }));
    }
    // The listeners' copies of the signals that went. A retry finds the
    // rows already gone and repeats nothing.
    if !unlisten.is_empty() {
        state.listener.unregister_many(&unlisten).await;
    }
    // A door that refuses with what it holds kept stops listening at once,
    // its rows kept for when it is back: the listener lets go of them the
    // way it does when a door's window ends (`crate::reaper`), and what it
    // could not let go of is marked for the door-windows loop to tell it
    // again.
    if take_down.door == Door::Refuse && rows_stay && !keys.is_empty() {
        let kept = crate::store::postgres::activation_signals(&state.pg_pool, project_id, &keys)
            .await
            .map_err(|e| internal("the signals to let go of", e))?;
        let refused = crate::listener::let_go_of_stopped(&state.pg_pool, &state.listener, &kept)
            .await
            .map_err(|e| internal("let go of the signals", e))?;
        crate::listener::note_let_go(&state.pg_pool, &kept, &refused).await.map_err(|e| internal("note the let-go", e))?;
    }
    // A door that takes or hides from now on, over triggers the listener
    // let go of behind a door that refused: their signals come up again (a
    // wake set, a held connection), from the state their rows hold (the
    // refusing door took their places, `places_go`). A signal that cannot
    // come up is retried by the listener meanwhile.
    if reopens && !keys.is_empty() {
        state.listener.rehydrate(project_id, &[]).await.map_err(|e| {
            internal("the door takes again, but not every signal of the project came up (`weft activate` brings them up)", e)
        })?;
    }

    // 2 and 3. What is held that runs (the paused runs), and what is
    // running now, as the answers say. Cancelling strips a run's own waits.
    let runs = live_runs(state, project_id).await.map_err(|e| internal("live runs", e))?;
    let affected = affected_runs(target, &runs, asked_by);
    let why = cancel_cause(&cause, asker);
    let cancelled: Vec<(RunId, &weft_core::run::CancelCause)> =
        runs_to_cancel(take_down, &affected).into_iter().map(|r| (r.run_id, &why)).collect();
    crate::api::run::cancel_run_ids(state, &cancelled).await.map_err(|e| internal("cancel", e))?;

    // Nothing of the project takes work any more: its front goes.
    crate::front::let_go_if_idle(state, project_id).await.map_err(|e| internal("let the front go", e))?;
    state.events.publish(DispatcherEvent::ProjectDeactivated { project_id }).await;
    let going = affected.iter().filter(|run| !run.suspended).count() as u64;
    Ok(TakenDown::Done(Taken { keys, held, runs: going, commands, tokens }))
}

/// Why the runs a take-down cancels were cancelled, as their record says
/// it: the health check names the copies it found broken, the maintenance
/// says so, a program's run names itself, and anything else was a
/// person's command.
fn cancel_cause(cause: &DownCause<'_>, asker: TakeDownAsker) -> weft_core::run::CancelCause {
    use weft_core::run::CancelCause;
    match (cause, asker.run()) {
        (DownCause::Health(broken), _) => {
            let copies: Vec<String> = broken
                .iter()
                .map(|copy| match &copy.instance {
                    Some(instance) => format!("'{}' of instance '{instance}'", copy.node_id),
                    None => format!("'{}'", copy.node_id),
                })
                .collect();
            CancelCause::Runtime {
                detail: format!(
                    "the infra health check found {} broken and took down the triggers that read it, stopping their runs",
                    copies.join(", ")
                ),
            }
        }
        (DownCause::Maintenance, _) => maintenance_cancel(),
        (_, Some(by)) => CancelCause::Run { by, tag: None },
        (_, None) => CancelCause::User,
    }
}

/// Why a run `weft maintenance on --running cancel` cancelled was
/// cancelled, as its record says it.
pub(crate) fn maintenance_cancel() -> weft_core::run::CancelCause {
    weft_core::run::CancelCause::Runtime {
        detail: "the install went into maintenance (`weft maintenance on --running cancel`), which cancelled the runs going".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::instance::Owner;

    fn run(run_id: u128, instance: Option<&str>, fired_by: Option<&str>) -> RunFacts {
        RunFacts {
            run_id: RunId::from_u128(run_id),
            instance: instance.map(|m| InstanceId::new(m).unwrap()),
            fired_by: fired_by.map(str::to_string),
            suspended: false,
        }
    }

    fn run_ids(runs: Vec<&RunFacts>) -> Vec<u128> {
        runs.into_iter().map(|r| r.run_id.as_u128()).collect()
    }

    fn instance(id: &str) -> Owner {
        Owner::Instance(InstanceId::new(id).unwrap())
    }

    /// An instance's trigger reaches that instance's runs of it, and nothing
    /// else: not another instance's, not the shared copy's, not a run
    /// started by hand.
    #[test]
    fn an_instances_activation_reaches_only_its_runs() {
        let runs = [
            run(1, Some("a"), Some("receive")),
            run(2, Some("b"), Some("receive")),
            run(3, None, Some("receive")),
            run(4, Some("a"), None),
            run(5, Some("a"), Some("cron")),
        ];
        let target = TakeDownTarget::Activations(vec![ActivationKey::new("receive", instance("a"))]);
        assert_eq!(run_ids(affected_runs(&target, &runs, None)), vec![1]);
        let target = TakeDownTarget::Activations(vec![ActivationKey::new("receive", Owner::Shared)]);
        assert_eq!(run_ids(affected_runs(&target, &runs, None)), vec![3]);
    }

    /// The whole project reaches every run, and the asker is never among
    /// the affected, whatever the target.
    #[test]
    fn the_asker_is_never_affected() {
        let runs = [run(1, Some("a"), Some("receive")), run(2, None, None)];
        assert_eq!(run_ids(affected_runs(&TakeDownTarget::WholeProject, &runs, None)), vec![1, 2]);
        assert_eq!(run_ids(affected_runs(&TakeDownTarget::WholeProject, &runs, Some(RunId::from_u128(1)))), vec![2]);
        let target = TakeDownTarget::Activations(vec![ActivationKey::new("receive", instance("a"))]);
        assert!(affected_runs(&target, &runs, Some(RunId::from_u128(1))).is_empty());
    }

    /// `held` decides the paused runs, `running` the ones going: dropping
    /// what is held cancels the paused ones and nothing else, cancelling
    /// what runs the going ones and nothing else, and the defaults touch
    /// neither.
    #[test]
    fn held_decides_the_paused_runs_and_running_the_ones_going() {
        let going = run(1, None, Some("door"));
        let paused = RunFacts { suspended: true, ..run(2, None, Some("door")) };
        let affected = [&going, &paused];
        let cancelled = |take_down: TakeDown| run_ids(runs_to_cancel(&take_down, &affected));
        let defaults = TakeDown::DEFAULT;
        assert!(cancelled(defaults).is_empty(), "nothing destructive unless typed: the wait is the command's own");
        assert_eq!(cancelled(TakeDown { held: Held::Drop, ..defaults }), vec![2]);
        assert_eq!(cancelled(TakeDown { running: Running::Cancel, ..defaults }), vec![1]);
        assert_eq!(cancelled(TakeDown::REMOVE), vec![1, 2]);
    }

    /// The door that refuses with nothing kept takes the signals with it;
    /// one that goes on taking keeps them and drops only what they hold.
    #[test]
    fn what_goes_with_the_door_follows_the_door_and_held() {
        let keys = TakeDownTarget::Activations(vec![ActivationKey::new("door", Owner::Shared)]);
        let defaults = TakeDown::DEFAULT;
        let places = [crate::store::FreshPlace { token: "poll".into(), kind_state_seq: 3, kind_state: serde_json::json!({}) }];
        assert!(matches!(signals_going(&keys, &defaults, None, &[]), SignalsGoing::Kept));
        assert!(matches!(signals_going(&keys, &TakeDown { held: Held::Drop, ..defaults }, None, &[]), SignalsGoing::HeldArrivals));
        assert!(matches!(signals_going(&keys, &TakeDown::REMOVE, None, &[]), SignalsGoing::Activations));
        let refused_kept = TakeDown { door: Door::Refuse, ..defaults };
        assert!(matches!(signals_going(&keys, &refused_kept, None, &[]), SignalsGoing::Kept), "a refusing door keeps what it held for later");
        assert!(
            matches!(signals_going(&keys, &refused_kept, None, &places), SignalsGoing::Places(p) if p == places),
            "and takes the places its signals keep in what they read"
        );
        let asker = Some(RunId::from_u128(7));
        assert!(matches!(
            signals_going(&TakeDownTarget::WholeProject, &TakeDown::REMOVE, asker, &[]),
            SignalsGoing::Project { except: Some(e) } if e == RunId::from_u128(7)
        ));
    }

    /// Only a door that refuses with what is held kept takes the places in
    /// what its triggers read: a door that holds goes on listening, one
    /// that drops what is held deletes the signals, a removal takes
    /// everything, and weft's own maintenance refuses while nobody refused
    /// anything.
    #[test]
    fn only_a_refusing_door_that_keeps_what_is_held_takes_the_places() {
        let keys = TakeDownTarget::Activations(vec![ActivationKey::new("feed", Owner::Shared)]);
        let refused_kept = TakeDown { door: Door::Refuse, ..TakeDown::DEFAULT };
        assert!(places_go(&keys, &refused_kept, &DownCause::Person));
        assert!(!places_go(&keys, &refused_kept, &DownCause::Maintenance), "the maintenance catches up once lifted");
        assert!(!places_go(&keys, &TakeDown::DEFAULT, &DownCause::Person), "a door that takes listens on");
        assert!(!places_go(&keys, &TakeDown { door: Door::Hide, ..TakeDown::DEFAULT }, &DownCause::Person), "a door that hides listens on");
        assert!(!places_go(&keys, &TakeDown::REMOVE, &DownCause::Person), "the signals go");
        assert!(!places_go(&TakeDownTarget::WholeProject, &refused_kept, &DownCause::Person), "the project goes");
    }

    /// A door that takes or hides over a trigger that refused (or whose
    /// window ended) brings its signals up again; one that was listening
    /// already, or a door that still refuses, does not.
    #[test]
    fn a_door_that_takes_again_brings_the_refused_signals_back_up() {
        let refused = ActivationLifecycle::down(&TakeDown { door: Door::Refuse, ..TakeDown::DEFAULT }, 10);
        let taking = ActivationLifecycle::down(&TakeDown::DEFAULT, 10);
        let hiding = ActivationLifecycle::down(&TakeDown { door: Door::Hide, ..TakeDown::DEFAULT }, 10);
        let window_over = ActivationLifecycle::down(&TakeDown { door_for_minutes: Some(1), ..TakeDown::DEFAULT }, 10);
        assert!(listens_again(&[&refused], &taking, 20));
        assert!(listens_again(&[&refused], &hiding, 20));
        assert!(listens_again(&[&taking, &window_over], &taking, 200), "a window that ended let go of its signals");
        assert!(!listens_again(&[&taking], &taking, 20), "it listened already");
        assert!(!listens_again(&[&ActivationLifecycle::active()], &taking, 20), "it was on");
        assert!(!listens_again(&[&refused], &refused, 20), "the door still refuses");
    }

    #[test]
    fn a_take_down_reports_its_triggers_by_owner() {
        let taken = Taken {
            keys: vec![ActivationKey::new("door", Owner::Shared), ActivationKey::new("inbox", instance("ada"))],
            held: HeldWork { arrivals: 2, paused: 0 },
            runs: 1,
            commands: Vec::new(),
            tokens: Vec::new(),
        };
        let report = taken.report(TakeDown::DEFAULT);
        assert_eq!(report.triggers.len(), 2);
        assert_eq!(report.held, HeldWork { arrivals: 2, paused: 0 });
        assert_eq!(report.runs, 1);
    }

    /// A run the health check stops says the health check did, naming the
    /// broken copies; one a program's run stops names that run; the rest
    /// were a person's.
    #[test]
    fn the_cancelled_runs_say_what_took_their_triggers_down() {
        use weft_broker_client::protocol::InfraCopy;
        use weft_core::run::CancelCause;
        let broken = [
            InfraCopy { node_id: "db".into(), instance: None },
            InfraCopy { node_id: "cache".into(), instance: Some(InstanceId::new("ada").unwrap()) },
        ];
        let CancelCause::Runtime { detail } = cancel_cause(&DownCause::Health(&broken), TakeDownAsker::Outside) else {
            panic!("the health check is no person");
        };
        assert!(detail.contains("health check") && detail.contains("'db', 'cache' of instance 'ada'"), "{detail}");
        let by = RunId::from_u128(7);
        assert_eq!(cancel_cause(&DownCause::Person, TakeDownAsker::Run(by)), CancelCause::Run { by, tag: None });
        assert_eq!(cancel_cause(&DownCause::Person, TakeDownAsker::Outside), CancelCause::User);
        assert_eq!(cancel_cause(&DownCause::Person, TakeDownAsker::Resync(uuid::Uuid::nil())), CancelCause::User);
        let CancelCause::Runtime { detail } = cancel_cause(&DownCause::Maintenance, TakeDownAsker::Outside) else {
            panic!("the maintenance is no person");
        };
        assert!(detail.contains("maintenance"), "{detail}");
    }
}
