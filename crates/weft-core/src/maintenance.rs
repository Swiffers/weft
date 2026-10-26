//! The install's maintenance: everything switched off while weft itself is
//! updated, so an older and a newer weft never run side by side.
//!
//! `weft maintenance on` takes down every trigger that is on, its door
//! refusing what arrives meanwhile, and stops every
//! infra copy that runs, marking each (the trigger's `went_down_with`, a row
//! per copy), and from then on nothing is turned on: no activation, no infra
//! start, no resync, no new run. Running it again adds what is on now and
//! clears nothing. `weft maintenance off` builds each project it took
//! something down in again on this weft, starts the copies it stopped on
//! that build, then brings back the triggers it took down, and ends the
//! maintenance. A project that cannot come back stays down without holding
//! the others, and its next `weft resync` brings it back. The update itself (`./setup.sh`, the GCP install
//! workflow) refuses to replace an install that has something on
//! ([`update_refusal`]).
//!
//! These are the wire shapes of the dispatcher's `/install/maintenance`
//! routes and of the lift's report.

use serde::{Deserialize, Serialize};

use crate::activation::OwnerTriggers;
use crate::infra::wire::CopyRef;
use crate::take_down::{Door, Held, Running, TakeDown};

/// What a verb that turns something on answers while the install is in
/// maintenance.
pub const IN_MAINTENANCE: &str =
    "the install is in maintenance (`weft maintenance on`), so nothing is turned on until `weft maintenance off` lifts it";

/// `POST /install/maintenance`: what happens to the runs going.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceOnRequest {
    /// They finish (`wait`, the default) or are cancelled (`cancel`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<Running>,
}

impl MaintenanceOnRequest {
    /// The answer for the runs going, `wait` when none was given.
    pub fn running(&self) -> Running {
        self.running.unwrap_or(Running::Wait)
    }

    /// How `on` takes each trigger down: its door refuses, so it takes no
    /// work and nothing of it starts or keeps a worker (one of a build from
    /// before the update) until the lift switches it back on; what it held
    /// already is kept, and runs after the lift; the runs going as asked.
    pub fn take_down(&self) -> TakeDown {
        TakeDown { door: Door::Refuse, door_for_minutes: None, held: Held::Keep, running: self.running() }
    }
}

/// What `weft maintenance on` did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceOn {
    /// Since when the install is in maintenance: the first `on`'s time,
    /// which a second one keeps.
    pub since_unix: i64,
    /// Each project something was taken down in, or could not be.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projects: Vec<ProjectWentDown>,
}

impl MaintenanceOn {
    /// Whether something was left on, which another `on` takes down once
    /// what held it is over.
    pub fn left_something_on(&self) -> bool {
        self.projects.iter().any(|p| !p.refused.is_empty())
    }
}

/// What one `on` did in one project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectWentDown {
    pub id: uuid::Uuid,
    pub name: String,
    /// The triggers it took down.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<OwnerTriggers>,
    /// The infra copies it stopped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copies: Vec<CopyRef>,
    /// The runs it cancelled (`--running cancel`).
    #[serde(default)]
    pub runs_cancelled: u64,
    /// What it could not take down, each with the way through.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refused: Vec<String>,
}

/// `GET /install/maintenance`: whether the install is in maintenance, what
/// `weft maintenance off` brings back, and what is on right now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceState {
    /// Since when it is in maintenance; `None` when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_unix: Option<i64>,
    /// What the lift brings back, project by project.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub left: Vec<ProjectLeft>,
    /// What is on right now.
    pub going: Going,
}

/// What the lift brings back in one project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectLeft {
    pub id: uuid::Uuid,
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub copies: Vec<CopyRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub triggers: Vec<OwnerTriggers>,
}

/// What is on across the install right now.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Going {
    /// Triggers on, or being switched on.
    pub triggers_on: u64,
    /// Infra copies up (running, below their readiness, or starting).
    pub copies_up: u64,
    /// Infra copies being stopped or terminated.
    pub copies_changing: u64,
    /// Runs queued or running (a run paused on a wait is not one: what it
    /// waits on is kept, and it carries on after the update).
    pub runs: u64,
}

impl Going {
    /// Nothing on at all: what an update needs.
    pub fn is_quiet(&self) -> bool {
        *self == Going::default()
    }

    /// Something is still going down on its own: a run finishing, a copy
    /// being stopped. What `weft maintenance on` waits for.
    pub fn still_going_down(&self) -> bool {
        self.runs > 0 || self.copies_changing > 0
    }

    /// What is on, as a person reads it; empty when nothing is.
    pub fn words(&self) -> String {
        let mut parts = Vec::new();
        let mut say = |n: u64, one: &str, many: &str| {
            if n > 0 {
                parts.push(format!("{n} {}", if n == 1 { one } else { many }));
            }
        };
        say(self.triggers_on, "trigger on", "triggers on");
        say(self.copies_up, "infra copy up", "infra copies up");
        say(self.copies_changing, "infra copy being stopped", "infra copies being stopped");
        say(self.runs, "run going", "runs going");
        parts.join(", ")
    }
}

/// Why an update of an install in `state` must not go ahead, or `None`
/// when it may: nothing is on. `weft` is how the person spells the
/// command for this install (with its `--on <target>` when it has one),
/// and `update` the step that updates it (`./setup.sh`, the workflow): the
/// answer is the steps to take, one per line, in the order to take them.
pub fn update_refusal(state: &MaintenanceState, weft: impl Fn(&str) -> String, update: &str) -> Option<String> {
    if state.going.is_quiet() {
        return None;
    }
    let on = state.going.words();
    let steps = |first: Vec<String>| {
        let all: Vec<String> = first.into_iter().chain([update.to_string(), weft("maintenance off")]).collect();
        all.iter().enumerate().map(|(i, step)| format!("\n    {}. {step}", i + 1)).collect::<String>()
    };
    Some(match state.since_unix {
        None => format!("weft cannot be updated while something is on, and this install has {on}. To update:{}", steps(vec![weft("maintenance on")])),
        Some(_) if state.going.triggers_on == 0 && state.going.copies_up == 0 => {
            let wait = match (state.going.runs > 0, state.going.copies_changing > 0) {
                (true, true) => format!("wait for the runs and the infra stopping (`{}` waits; add `--running cancel` to cancel the runs)", weft("maintenance on")),
                (true, false) => format!("wait for the runs (`{}` waits; add `--running cancel` to cancel them)", weft("maintenance on")),
                (false, _) => format!("wait for the infra to finish stopping (`{}` waits)", weft("maintenance on")),
            };
            format!("the install is in maintenance but still has {on}. To update:{}", steps(vec![wait]))
        }
        Some(_) => format!("the install is in maintenance but still has {on}. To update:{}", steps(vec![weft("maintenance on")])),
    })
}

/// `POST /install/maintenance/off`: the lift, under way, or nothing to do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LiftAnswer {
    /// The install is not in maintenance.
    NotOn,
    /// The lift runs as a task of its own; follow it at
    /// `/install/maintenance/follow/{task}`.
    UnderWay { task: uuid::Uuid },
}

/// What a lift did: its task's result. The maintenance is over once it
/// has one; a project with something [`ProjectLifted::failed`] stays down
/// until its next `weft resync`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiftReport {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projects: Vec<ProjectLifted>,
}

/// What a lift did in one project.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectLifted {
    pub id: uuid::Uuid,
    pub name: String,
    /// It was built again on this weft, from the request its running build
    /// came from, and what it brought back runs on that build.
    #[serde(default)]
    pub rebuilt: bool,
    /// The infra copies it started.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub started: Vec<CopyRef>,
    /// The triggers it turned back on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub turned_on: Vec<OwnerTriggers>,
    /// What it had nothing to do for, and why (a copy terminated during
    /// the maintenance, one the program no longer has).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<String>,
    /// What did not come back, and why: still recorded, and brought back by
    /// the project's next `weft resync` (or `weft maintenance off`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<String>,
}

impl ProjectLifted {
    /// Take in what one part of the project's lift did (one owner's
    /// copies, one owner's triggers), lifted beside the others.
    pub fn absorb(&mut self, part: ProjectLifted) {
        self.started.extend(part.started);
        self.turned_on.extend(part.turned_on);
        self.skipped.extend(part.skipped);
        self.failed.extend(part.failed);
    }
}

/// Where a lift is, as its task says while it runs (the `going` answer of
/// `/install/maintenance/follow/{task}`): the projects it is done with, and
/// the step each project it lifts now is on (they go side by side).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiftProgress {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub done: Vec<ProjectLifted>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub going: Vec<LiftAt>,
}

/// The step a lift is on, in one project, since `since_unix`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiftAt {
    pub id: uuid::Uuid,
    pub name: String,
    pub since_unix: i64,
    pub doing: LiftDoing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum LiftDoing {
    /// Building the project again on this weft; `build` once its images
    /// build, read at `/projects/{id}/builds/{build}`.
    Building {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        build: Option<uuid::Uuid>,
    },
    /// Starting these copies, every owner's at once.
    StartingInfra { copies: Vec<CopyRef> },
    /// Turning these back on, every owner's at once.
    TurningOn { triggers: Vec<OwnerTriggers> },
}

impl LiftDoing {
    /// The step in words, as the follower says it.
    pub fn words(&self) -> String {
        match self {
            LiftDoing::Building { .. } => "building it again on this weft".into(),
            LiftDoing::StartingInfra { copies } => {
                format!("starting infra {}", copies.iter().map(|copy| copy.node.as_str()).collect::<Vec<_>>().join(", "))
            }
            // Counted: a program can have dozens.
            LiftDoing::TurningOn { triggers } => {
                let n: usize = triggers.iter().map(|owned| owned.triggers.len()).sum();
                format!("turning on {n} {}", if n == 1 { "trigger" } else { "triggers" })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn weft(verb: &str) -> String {
        format!("weft {verb} --on cloud")
    }

    #[test]
    fn where_a_lift_is_reads_back_and_says_its_step() {
        let progress = LiftProgress {
            done: vec![ProjectLifted { id: uuid::Uuid::nil(), name: "feeds".into(), rebuilt: true, ..Default::default() }],
            going: vec![LiftAt {
                id: uuid::Uuid::nil(),
                name: "media".into(),
                since_unix: 7,
                doing: LiftDoing::Building { build: Some(uuid::Uuid::nil()) },
            }],
        };
        let wire = serde_json::to_value(&progress).unwrap();
        assert_eq!(wire["going"][0]["doing"]["step"], "building");
        assert_eq!(wire["going"][0]["sinceUnix"], 7);
        assert_eq!(serde_json::from_value::<LiftProgress>(wire).unwrap(), progress);
        let copies = vec![CopyRef { node: "studio".into(), instance: None }, CopyRef { node: "db".into(), instance: None }];
        assert_eq!(LiftDoing::StartingInfra { copies }.words(), "starting infra studio, db");
        let triggers = |owner, n: usize| OwnerTriggers { owner, triggers: (0..n).map(|i| format!("t{i}")).collect() };
        let ada = crate::instance::Owner::Instance(crate::instance::InstanceId::new("ada").unwrap());
        let every_owner = vec![triggers(crate::instance::Owner::Shared, 64), triggers(ada.clone(), 1)];
        assert_eq!(LiftDoing::TurningOn { triggers: every_owner }.words(), "turning on 65 triggers", "every owner's, counted");
        assert_eq!(LiftDoing::TurningOn { triggers: vec![triggers(ada, 1)] }.words(), "turning on 1 trigger");
    }

    #[test]
    fn nothing_on_lets_the_update_go_in_or_out_of_maintenance() {
        assert_eq!(update_refusal(&MaintenanceState::default(), weft, "./setup.sh"), None);
        let quiet = MaintenanceState { since_unix: Some(5), ..Default::default() };
        assert_eq!(update_refusal(&quiet, weft, "./setup.sh"), None);
    }

    #[test]
    fn something_on_outside_maintenance_names_the_three_steps() {
        let state = MaintenanceState { going: Going { triggers_on: 2, runs: 1, ..Default::default() }, ..Default::default() };
        let why = update_refusal(&state, weft, "./setup.sh").expect("refused");
        assert_eq!(
            why,
            "weft cannot be updated while something is on, and this install has 2 triggers on, 1 run going. To update:\n    \
             1. weft maintenance on --on cloud\n    2. ./setup.sh\n    3. weft maintenance off --on cloud"
        );
    }

    #[test]
    fn in_maintenance_what_is_still_going_down_is_waited_for_and_what_is_on_is_taken_down_again() {
        // Only what is still going is named, with only the way through it.
        let stopping = MaintenanceState { since_unix: Some(1), going: Going { copies_changing: 1, ..Default::default() }, ..Default::default() };
        let why = update_refusal(&stopping, weft, "./setup.sh").expect("refused");
        assert!(why.contains("1 infra copy being stopped") && why.contains("1. wait for the infra to finish stopping"), "{why}");
        assert!(!why.contains("run") && !why.contains("--running cancel"), "no run is going: {why}");
        let running = MaintenanceState { since_unix: Some(1), going: Going { runs: 2, ..Default::default() }, ..Default::default() };
        let why = update_refusal(&running, weft, "./setup.sh").expect("refused");
        assert!(why.contains("1. wait for the runs") && why.contains("--running cancel"), "{why}");
        assert!(!why.contains("infra"), "{why}");
        let on = MaintenanceState { since_unix: Some(1), going: Going { triggers_on: 1, ..Default::default() }, ..Default::default() };
        let why = update_refusal(&on, weft, "./setup.sh").expect("refused");
        assert!(why.contains("1. weft maintenance on --on cloud\n    2. ./setup.sh\n    3. weft maintenance off --on cloud"), "{why}");
    }

    #[test]
    fn what_goes_down_on_its_own_is_runs_and_copies_stopping() {
        assert!(Going { runs: 1, ..Default::default() }.still_going_down());
        assert!(Going { copies_changing: 1, ..Default::default() }.still_going_down());
        assert!(!Going { triggers_on: 1, copies_up: 1, ..Default::default() }.still_going_down());
    }

    /// The wire shapes keep camelCase fields and snake_case tags.
    #[test]
    fn the_shapes_read_back_under_their_wire_names() {
        let state = MaintenanceState { since_unix: Some(3), left: Vec::new(), going: Going { copies_up: 1, ..Default::default() } };
        let wire = serde_json::to_value(&state).unwrap();
        assert_eq!(wire["sinceUnix"], 3);
        assert_eq!(wire["going"]["copiesUp"], 1);
        assert_eq!(serde_json::from_value::<MaintenanceState>(wire).unwrap(), state);
        let task = uuid::Uuid::from_u128(9);
        let under_way = serde_json::to_value(LiftAnswer::UnderWay { task }).unwrap();
        assert_eq!(under_way["state"], "under_way");
        assert_eq!(serde_json::from_value::<LiftAnswer>(under_way).unwrap(), LiftAnswer::UnderWay { task });
        assert_eq!(serde_json::to_value(LiftAnswer::NotOn).unwrap()["state"], "not_on");
        let request: MaintenanceOnRequest = serde_json::from_value(serde_json::json!({ "running": "cancel" })).unwrap();
        assert_eq!(request.running(), Running::Cancel);
        assert_eq!(MaintenanceOnRequest::default().running(), Running::Wait);
    }

    /// The maintenance's doors refuse, keep what they held, and leave the
    /// runs going as asked; a valid take-down either way.
    #[test]
    fn maintenance_takes_triggers_down_with_a_refusing_door() {
        for running in [Running::Wait, Running::Cancel] {
            let take_down = MaintenanceOnRequest { running: Some(running) }.take_down();
            assert_eq!((take_down.door, take_down.held, take_down.running), (Door::Refuse, Held::Keep, running));
            take_down.check().expect("a take-down the verbs accept");
        }
    }
}
