//! One trigger activation's lifecycle, and several as one.
//!
//! The dispatcher owns the rows (`trigger_activation`); the broker reads
//! them for the supervisor's health loop. Both answer "what state is
//! this project's listening in" with [`aggregate`], so the two can
//! never disagree.

use crate::protocol::ProjectStatus;

/// One activation's lifecycle: status plus the three axes the fire gate,
/// the consumer enumeration and the door's window read.
///
///   accepting_fires:            the gate holds a fire while the trigger
///                               is down when true, refuses it when false.
///   fires_visible_to_consumers: a token's enumeration shows the
///                               signals when true, hides them when false.
///   fires_deadline_unix:        accepting only until then.
///
/// A trigger taken down sets them from its door
/// (`weft_core::take_down::Door`, [`Self::down`]): take = accepting and
/// visible, hide = accepting and hidden, refuse = neither; a door's
/// minutes are the deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationLifecycle {
    pub status: ProjectStatus,
    pub accepting_fires: bool,
    pub fires_visible_to_consumers: bool,
    pub fires_deadline_unix: Option<i64>,
    /// Why the CURRENT take-down happened, when it was not a person: the
    /// health loop taking it down because a copy broke, an infra verb
    /// (stop, terminate, upgrade) taking it down with its copies, or the
    /// install going into maintenance. What took it down coming back
    /// brings it back (the health loop's auto-recover, an infra start, the
    /// maintenance lift); one a person took down stays down until they
    /// switch it on.
    pub went_down: Option<WentDown>,
    /// The trigger-setup run of the activation in flight, reserved before
    /// setup is queued; every write the activation makes is guarded by it.
    /// `None` outside Activating.
    pub activating_run_id: Option<uuid::Uuid>,
}

impl ActivationLifecycle {
    /// How it stands for arriving work (`weft_core::arrival`).
    pub fn standing(&self) -> weft_core::arrival::Standing {
        weft_core::arrival::Standing {
            status: self.status,
            accepting_fires: self.accepting_fires,
            fires_deadline_unix: self.fires_deadline_unix,
        }
    }

    /// Never activated: what an absent row means.
    pub fn registered() -> Self {
        Self { status: ProjectStatus::Registered, ..Self::active() }
    }

    /// Live: fires execute immediately.
    pub fn active() -> Self {
        Self {
            status: ProjectStatus::Active,
            accepting_fires: true,
            fires_visible_to_consumers: true,
            fires_deadline_unix: None,
            went_down: None,
            activating_run_id: None,
        }
    }

    /// While setup runs: fires are held (the listener may not have every
    /// signal yet) and consumers see nothing; the activation's end hands
    /// over what was held.
    pub fn activating(run_id: uuid::Uuid) -> Self {
        Self {
            status: ProjectStatus::Activating,
            fires_visible_to_consumers: false,
            activating_run_id: Some(run_id),
            ..Self::active()
        }
    }

    /// Taken down with `take_down`'s door at `now_unix`. What took it down,
    /// when not a person, is the write's to mark (`went_down`): an infra
    /// take-down's mark names the command it issues in that write.
    pub fn down(take_down: &weft_core::take_down::TakeDown, now_unix: i64) -> Self {
        use weft_core::take_down::Door;
        let (accepting_fires, fires_visible_to_consumers) = match take_down.door {
            Door::Take => (true, true),
            Door::Hide => (true, false),
            Door::Refuse => (false, false),
        };
        Self {
            status: ProjectStatus::Inactive,
            accepting_fires,
            fires_visible_to_consumers,
            fires_deadline_unix: take_down.door_for_minutes.map(|minutes| now_unix + i64::from(minutes) * 60),
            went_down: None,
            activating_run_id: None,
        }
    }

    /// Down, its door refusing, with nothing kept: what an activation
    /// nobody finished and that kept no work lands at.
    pub fn refused() -> Self {
        Self::down(&weft_core::take_down::TakeDown::REMOVE, 0)
    }

    /// Down, its door taking what arrives: what an activation nobody
    /// finished lands at when its triggers kept work, so that work waits
    /// for their next activation.
    pub fn holding() -> Self {
        Self::down(&weft_core::take_down::TakeDown::DEFAULT, 0)
    }

    /// Whether the health loop took this down and it is still down: what
    /// its auto-recover brings back.
    pub fn down_by_health(&self) -> bool {
        self.went_down == Some(WentDown::Health) && self.status == ProjectStatus::Inactive
    }

    /// The door of an activation that is down, as its axes read it.
    pub fn door(&self) -> weft_core::take_down::Door {
        use weft_core::take_down::Door;
        match (self.accepting_fires, self.fires_visible_to_consumers) {
            (false, _) => Door::Refuse,
            (true, true) => Door::Take,
            (true, false) => Door::Hide,
        }
    }

    /// Where the activation stands, as a person reads it: `registered`,
    /// `activating`, `active`, or for one down what its door does: `take`,
    /// `hide`, `refuse`.
    pub fn mode(&self) -> weft_core::activation::ActivationMode {
        use weft_core::activation::ActivationMode;
        match self.status {
            ProjectStatus::Registered => ActivationMode::Registered,
            ProjectStatus::Activating => ActivationMode::Activating,
            ProjectStatus::Active => ActivationMode::Active,
            ProjectStatus::Inactive => ActivationMode::Down(self.door()),
        }
    }
}

/// Several activations as one lifecycle: what a project shows for its
/// shared triggers. Activating wins (a person sees the verb working and
/// is offered its cancel); then anything listening makes it active.
/// All-down keeps the most that survived (accepting if any accepts,
/// visible if any is, the latest deadline). None at all is registered.
pub fn aggregate<'a>(lifecycles: impl IntoIterator<Item = &'a ActivationLifecycle>) -> ActivationLifecycle {
    let all: Vec<&ActivationLifecycle> = lifecycles.into_iter().collect();
    let with = |status: ProjectStatus| all.iter().copied().filter(move |l| l.status == status);
    if let Some(first) = with(ProjectStatus::Activating).next() {
        return first.clone();
    }
    if with(ProjectStatus::Active).next().is_some() {
        return ActivationLifecycle::active();
    }
    let inactive: Vec<&ActivationLifecycle> = with(ProjectStatus::Inactive).collect();
    if inactive.is_empty() {
        return ActivationLifecycle::registered();
    }
    ActivationLifecycle {
        status: ProjectStatus::Inactive,
        accepting_fires: inactive.iter().any(|l| l.accepting_fires),
        fires_visible_to_consumers: inactive.iter().any(|l| l.fires_visible_to_consumers),
        fires_deadline_unix: inactive.iter().filter_map(|l| l.fires_deadline_unix).max(),
        went_down: same_cause(&inactive),
        activating_run_id: None,
    }
}

/// The cause every one of `lifecycles` went down with, when they all went
/// down the same way; of infra commands, the earliest.
fn same_cause(lifecycles: &[&ActivationLifecycle]) -> Option<WentDown> {
    let mut went = lifecycles.first()?.went_down?;
    for lifecycle in lifecycles {
        went = match (went, lifecycle.went_down?) {
            (WentDown::Health, WentDown::Health) => WentDown::Health,
            (WentDown::Infra { first_command: a }, WentDown::Infra { first_command: b }) => WentDown::Infra { first_command: a.min(b) },
            (WentDown::Maintenance, WentDown::Maintenance) => WentDown::Maintenance,
            _ => return None,
        };
    }
    Some(went)
}

/// What took a trigger down when it was not a person (see
/// [`ActivationLifecycle::went_down`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WentDown {
    /// The health loop parked it: a copy it reads broke.
    Health,
    /// An infra verb took it down with the copies it reads, in the same
    /// transaction that issued its commands, the first of which is
    /// `first_command`: what ended or came back after that command is
    /// what brings it back, so nothing older can.
    Infra { first_command: i64 },
    /// `weft maintenance on` took it down with everything else on the
    /// install: the maintenance lift brings it back.
    Maintenance,
}

impl WentDown {
    pub fn with(self) -> DownWith {
        match self {
            WentDown::Health => DownWith::Health,
            WentDown::Infra { .. } => DownWith::Infra,
            WentDown::Maintenance => DownWith::Maintenance,
        }
    }

    /// The command its mark names, for an infra take-down.
    pub fn first_command(self) -> Option<i64> {
        match self {
            WentDown::Health | WentDown::Maintenance => None,
            WentDown::Infra { first_command } => Some(first_command),
        }
    }

    /// From its row's `went_down_with` and `went_down_by_command`.
    pub fn of_columns(with: Option<&str>, first_command: Option<i64>) -> Result<Option<Self>, String> {
        match (with.map(|w| (w, DownWith::parse(w))), first_command) {
            (None, None) => Ok(None),
            (Some((_, Some(DownWith::Health))), None) => Ok(Some(WentDown::Health)),
            (Some((_, Some(DownWith::Infra))), Some(first_command)) => Ok(Some(WentDown::Infra { first_command })),
            (Some((_, Some(DownWith::Maintenance))), None) => Ok(Some(WentDown::Maintenance)),
            (Some((spelling, None)), _) => Err(format!("unknown trigger_activation.went_down_with '{spelling}'")),
            (with, command) => Err(format!(
                "trigger_activation.went_down_with={:?} and went_down_by_command={command:?} disagree: an infra take-down names its command, nothing else does",
                with.map(|w| w.0)
            )),
        }
    }
}

/// The kind of [`WentDown`], as the row spells it and `weft status` says it.
pub use weft_core::projects::DownWith;

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::take_down::{Door, Held, Running, TakeDown};

    #[test]
    fn nothing_activated_is_registered() {
        assert_eq!(aggregate([]).status, ProjectStatus::Registered);
    }

    #[test]
    fn activating_wins_the_aggregate() {
        let run_id = uuid::Uuid::new_v4();
        let all = [ActivationLifecycle::active(), ActivationLifecycle::activating(run_id), ActivationLifecycle::holding()];
        assert_eq!(aggregate(&all).activating_run_id, Some(run_id));
    }

    #[test]
    fn anything_listening_is_active() {
        let all = [ActivationLifecycle::refused(), ActivationLifecycle::active()];
        assert_eq!(aggregate(&all).mode().as_str(), "active");
    }

    /// The door sets the three axes, and reads back as itself.
    #[test]
    fn a_door_sets_the_axes_and_reads_back() {
        let at = |door, minutes| TakeDown { door, door_for_minutes: minutes, held: Held::Keep, running: Running::Wait };
        let take = ActivationLifecycle::down(&at(Door::Take, None), 100);
        assert!(take.accepting_fires && take.fires_visible_to_consumers && take.fires_deadline_unix.is_none());
        let hide = ActivationLifecycle::down(&at(Door::Hide, Some(10)), 100);
        assert!(hide.accepting_fires && !hide.fires_visible_to_consumers);
        assert_eq!(hide.fires_deadline_unix, Some(100 + 600));
        let refuse = ActivationLifecycle::down(&at(Door::Refuse, None), 100);
        assert!(!refuse.accepting_fires && !refuse.fires_visible_to_consumers);
        for (lifecycle, door) in [(take, Door::Take), (hide, Door::Hide), (refuse, Door::Refuse)] {
            assert_eq!(lifecycle.mode(), weft_core::activation::ActivationMode::Down(door));
        }
        assert!(ActivationLifecycle { went_down: Some(WentDown::Health), ..ActivationLifecycle::down(&TakeDown::HEALTH, 100) }.down_by_health());
        assert_eq!(ActivationLifecycle::holding().door(), TakeDown::DEFAULT.door);
    }

    #[test]
    fn all_down_keeps_the_most_that_survived() {
        let hidden = ActivationLifecycle::down(
            &TakeDown { door: Door::Hide, door_for_minutes: Some(1), held: Held::Keep, running: Running::Wait },
            0,
        );
        let all = [ActivationLifecycle::refused(), hidden.clone(), ActivationLifecycle::holding()];
        assert_eq!(aggregate(&all).mode().as_str(), "take");
        let agg = aggregate(&[ActivationLifecycle::refused(), hidden]);
        assert_eq!(agg.mode().as_str(), "hide");
        assert_eq!(agg.fires_deadline_unix, Some(60));
        assert_eq!(aggregate(&[ActivationLifecycle::refused()]).mode().as_str(), "refuse");
    }

    /// A row's mark reads back as what took it down, and a mark whose two
    /// columns disagree is refused rather than guessed at.
    #[test]
    fn a_mark_reads_back_from_its_columns() {
        assert_eq!(WentDown::of_columns(None, None), Ok(None));
        assert_eq!(WentDown::of_columns(Some("health"), None), Ok(Some(WentDown::Health)));
        assert_eq!(WentDown::of_columns(Some("infra"), Some(7)), Ok(Some(WentDown::Infra { first_command: 7 })));
        assert_eq!(WentDown::of_columns(Some("maintenance"), None), Ok(Some(WentDown::Maintenance)));
        for (with, command) in [(Some("infra"), None), (Some("health"), Some(7)), (Some("maintenance"), Some(7)), (None, Some(7)), (Some("flood"), None)] {
            assert!(WentDown::of_columns(with, command).is_err(), "{with:?} {command:?}");
        }
        for went in [WentDown::Health, WentDown::Infra { first_command: 3 }, WentDown::Maintenance] {
            assert_eq!(DownWith::parse(went.with().as_str()), Some(went.with()));
        }
    }

    /// Down the same way, several read as one, an infra mark as its
    /// earliest command; down different ways, or one by a person, they
    /// carry no mark.
    #[test]
    fn the_aggregate_marks_only_a_shared_cause() {
        let marked = |went_down| ActivationLifecycle { went_down, ..ActivationLifecycle::holding() };
        let infra = |first_command| marked(Some(WentDown::Infra { first_command }));
        assert_eq!(aggregate(&[infra(9), infra(4)]).went_down, Some(WentDown::Infra { first_command: 4 }));
        assert_eq!(aggregate(&[marked(Some(WentDown::Health)), marked(Some(WentDown::Health))]).went_down, Some(WentDown::Health));
        assert_eq!(aggregate(&[infra(4), marked(Some(WentDown::Health))]).went_down, None);
        assert_eq!(aggregate(&[infra(4), marked(None)]).went_down, None);
        let maintenance = || marked(Some(WentDown::Maintenance));
        assert_eq!(aggregate(&[maintenance(), maintenance()]).went_down, Some(WentDown::Maintenance));
        assert_eq!(aggregate(&[maintenance(), infra(4)]).went_down, None);
    }
}
