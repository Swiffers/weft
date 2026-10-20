//! How triggers go down, and how they come back: one spec for every
//! command that takes them down.
//!
//! Taking a trigger down answers three questions, applied in this order
//! so nothing arriving slips between them:
//!
//! 1. **the door** ([`Door`]): what happens to work arriving from now on.
//!    `take` holds it (a question stays listed for the people who answer
//!    it), `hide` holds it out of sight, `refuse` takes nothing. With
//!    `take` or `hide`, `doorForMinutes` refuses once that long has passed.
//! 2. **what is held** ([`Held`]): the work already waiting on the
//!    trigger (arrivals held while it was down, runs it fired that are
//!    paused on a wait). `drop` deletes the arrivals and cancels the
//!    paused runs.
//! 3. **what is running** ([`Running`]): the runs going right now.
//!    `wait` lets them finish on the version they started on, and the
//!    command goes on only once they have (an infra copy goes, a resync's
//!    new version takes over); `cancel` stops them at once.
//!
//! `weft deactivate`, `weft resync`, `weft infra stop | terminate |
//! upgrade`, the editor, a program's `ctx.trigger(..).deactivate(..)` and
//! `ctx.infra(..).stop(..)`, and the catalog nodes wrapping them all send a
//! [`TakeDownChoice`] (each answer optional), resolved against the
//! defaults every command shares ([`TakeDown::DEFAULT`]). `weft rm` and an infra copy
//! found broken take triggers down with fixed answers
//! ([`TakeDown::REMOVE`], [`TakeDown::HEALTH`]). Nothing is ever asked
//! interactively: what a command did is printed after it did it.
//!
//! Coming back is `weft activate`, which releases what was held
//! ([`ReleaseHeld`]), paced so new arrivals still get through.

use serde::{Deserialize, Serialize};

crate::wire_enum! {
    /// What happens to work arriving while a trigger is down.
    // SYNC: Door <-> packages/weft-graph/src/protocol.ts Door
    pub enum Door {
        /// Hold it until the trigger is back; a question stays listed
        /// for the people who answer it.
        Take = "take",
        /// Hold it, hidden from the people who answer questions.
        Hide = "hide",
        /// Take nothing.
        Refuse = "refuse",
    }
}

crate::wire_enum! {
    /// What happens to the work already waiting on a trigger as it goes
    /// down: the arrivals it held while it was down before, and the runs
    /// it fired that are paused on a wait.
    // SYNC: Held <-> packages/weft-graph/src/protocol.ts Held
    pub enum Held {
        /// Keep it for when the trigger is back.
        Keep = "keep",
        /// Delete the held arrivals and cancel the paused runs.
        Drop = "drop",
    }
}

crate::wire_enum! {
    /// What happens to the runs going right now.
    // SYNC: Running <-> packages/weft-graph/src/protocol.ts Running
    pub enum Running {
        /// They finish on the version they started on, and the command
        /// waits for them, however long that takes, before what it does
        /// next: an infra copy goes, a resync's new version takes over
        /// (locally, takes the project's port from the old worker once it
        /// has nothing left running). Ctrl+C or `weft stop` ends the wait.
        Wait = "wait",
        /// They are cancelled at once.
        Cancel = "cancel",
    }
}

crate::wire_enum! {
    /// What `weft activate` does with the work its triggers held while
    /// they were down.
    // SYNC: ReleaseHeld <-> packages/weft-graph/src/protocol.ts ReleaseHeld
    #[derive(Default)]
    pub enum ReleaseHeld {
        /// Run it, paced at half the trigger's own limits so new
        /// arrivals still get through.
        #[default]
        Run = "run",
        /// Delete the held arrivals and cancel the paused runs.
        Drop = "drop",
    }
}

/// The three words parse from a flag the way they read on the wire, and
/// a word nobody spells is refused naming the ones that exist.
macro_rules! parse_flag {
    ($($name:ident: $what:literal),+ $(,)?) => {$(
        impl std::str::FromStr for $name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, String> {
                Self::parse(s).ok_or_else(|| format!("unknown {} '{s}'; must be one of: {}", $what, Self::accepted()))
            }
        }
    )+};
}

parse_flag!(Door: "door", Held: "held answer", Running: "running answer", ReleaseHeld: "held answer");

/// A take-down, every answer given.
// SYNC: TakeDown <-> packages/weft-graph/src/protocol.ts TakeDown
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TakeDown {
    pub door: Door,
    /// With `take` or `hide`: the door refuses once this many minutes have
    /// passed. `None`: it stays as it is until the trigger is back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub door_for_minutes: Option<u32>,
    pub held: Held,
    pub running: Running,
}

impl TakeDown {
    /// What an infra copy found broken does to the triggers whose cut
    /// contains it: their work waits for it, and the runs reaching it are
    /// stopped, since they cannot go on on infra that is down. The
    /// triggers come back once it is healthy.
    pub const HEALTH: Self = Self { door: Door::Take, door_for_minutes: None, held: Held::Keep, running: Running::Cancel };

    /// What `weft rm` does: everything off and dropped.
    pub const REMOVE: Self = Self { door: Door::Refuse, door_for_minutes: None, held: Held::Drop, running: Running::Cancel };

    /// What every command does when a request says nothing: nothing
    /// destructive. The door takes and keeps what it took, and the runs
    /// going finish on their own version before the command goes on.
    // SYNC: TakeDown::DEFAULT <-> packages/weft-graph/src/protocol.ts TAKE_DOWN_DEFAULT
    pub const DEFAULT: Self = Self { door: Door::Take, door_for_minutes: None, held: Held::Keep, running: Running::Wait };

    /// Whether the door's answers fit together; the refusal names what to
    /// pass instead.
    pub fn check(&self) -> Result<(), String> {
        match (self.door, self.door_for_minutes) {
            (Door::Refuse, Some(_)) => {
                Err("`doorForMinutes` (`--door-for`) goes with a door that takes or hides: a refusing door takes nothing already".into())
            }
            (_, Some(0)) => {
                Err("`doorForMinutes` (`--door-for`) is a number of minutes from 1 up; to refuse at once, pass `refuse` for the door".into())
            }
            _ => Ok(()),
        }
    }

    /// Every answer, as a request would carry it.
    pub fn choice(&self) -> TakeDownChoice {
        TakeDownChoice {
            door: Some(self.door),
            door_for_minutes: self.door_for_minutes,
            held: Some(self.held),
            running: Some(self.running),
        }
    }

    /// The door, as a person reads it in what a command did: "the door
    /// holds what arrives", "..., for 30 minutes, then refuses".
    pub fn door_words(&self) -> String {
        let what = match self.door {
            Door::Take => "the door holds what arrives",
            Door::Hide => "the door holds what arrives, out of sight",
            Door::Refuse => return "the door refuses what arrives".into(),
        };
        match self.door_for_minutes {
            Some(1) => format!("{what}, for 1 minute, then refuses"),
            Some(minutes) => format!("{what}, for {minutes} minutes, then refuses"),
            None => what.into(),
        }
    }
}

/// A take-down as a request says it: each answer it gives, the rest left
/// to the defaults ([`TakeDown::DEFAULT`]). What every command
/// that takes triggers down carries on the wire, flattened into its body;
/// the CLI's `--door`, `--door-for`, `--held` and `--running`.
// SYNC: TakeDownChoice <-> packages/weft-graph/src/protocol.ts TakeDownChoice
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TakeDownChoice {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub door: Option<Door>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub door_for_minutes: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held: Option<Held>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<Running>,
}

impl TakeDownChoice {
    /// The take-down it asks for: each answer given, the default for the
    /// rest, refused when its door's answers do not fit together.
    pub fn resolve(&self) -> Result<TakeDown, String> {
        let defaults = TakeDown::DEFAULT;
        let take_down = TakeDown {
            door: self.door.unwrap_or(defaults.door),
            door_for_minutes: self.door_for_minutes,
            held: self.held.unwrap_or(defaults.held),
            running: self.running.unwrap_or(defaults.running),
        };
        take_down.check()?;
        Ok(take_down)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::wire_enum_roundtrip_tests!(Door, Held, Running, ReleaseHeld);

    /// Nothing destructive unless typed: the door holds, what is held is
    /// kept, and the runs going finish before the command goes on.
    #[test]
    fn a_request_saying_nothing_is_nothing_destructive() {
        assert_eq!(
            TakeDownChoice::default().resolve().unwrap(),
            TakeDown { door: Door::Take, door_for_minutes: None, held: Held::Keep, running: Running::Wait },
        );
    }

    /// An answer given wins; the rest are the defaults.
    #[test]
    fn a_given_answer_wins_and_the_rest_are_the_defaults() {
        let choice = TakeDownChoice { door: Some(Door::Hide), door_for_minutes: Some(30), running: Some(Running::Cancel), ..Default::default() };
        let resolved = choice.resolve().unwrap();
        assert_eq!(resolved, TakeDown { door: Door::Hide, door_for_minutes: Some(30), held: Held::Keep, running: Running::Cancel });
        assert_eq!(resolved.choice().resolve().unwrap(), resolved, "a full choice resolves to itself");
    }

    /// A refusing door has no minutes to count, and zero minutes is a
    /// refusing door spelled wrong.
    #[test]
    fn a_door_that_does_not_fit_is_refused_naming_what_to_pass() {
        let refused = TakeDownChoice { door: Some(Door::Refuse), door_for_minutes: Some(5), ..Default::default() };
        assert!(refused.resolve().unwrap_err().contains("--door-for"));
        let zero = TakeDownChoice { door_for_minutes: Some(0), ..Default::default() };
        assert!(zero.resolve().unwrap_err().contains("`refuse`"));
        assert!("keep".parse::<Running>().unwrap_err().contains("wait, cancel"), "`keep` is gone");
    }

    /// The fixed take-downs resolve to themselves.
    #[test]
    fn the_fixed_take_downs_resolve_to_themselves() {
        for fixed in [TakeDown::HEALTH, TakeDown::REMOVE, TakeDown::DEFAULT] {
            assert_eq!(fixed.choice().resolve(), Ok(fixed));
        }
    }

    #[test]
    fn the_wire_spells_every_answer_in_camel_case() {
        let take_down = TakeDown { door: Door::Take, door_for_minutes: Some(10), held: Held::Drop, running: Running::Cancel };
        let wire = serde_json::to_value(take_down).unwrap();
        assert_eq!(wire, serde_json::json!({ "door": "take", "doorForMinutes": 10, "held": "drop", "running": "cancel" }));
        assert_eq!(serde_json::from_value::<TakeDown>(wire).unwrap(), take_down);
        let partial: TakeDownChoice = serde_json::from_value(serde_json::json!({ "running": "cancel" })).unwrap();
        assert_eq!(partial, TakeDownChoice { running: Some(Running::Cancel), ..Default::default() });
        assert_eq!(serde_json::to_value(TakeDownChoice::default()).unwrap(), serde_json::json!({}));
        assert!(serde_json::from_value::<TakeDownChoice>(serde_json::json!({ "door": "park" })).is_err());
    }

    #[test]
    fn a_flag_word_parses_and_an_unknown_one_names_the_real_ones() {
        assert_eq!("hide".parse::<Door>(), Ok(Door::Hide));
        assert!("park".parse::<Door>().unwrap_err().contains("take, hide, refuse"));
        assert!("wipe".parse::<Held>().unwrap_err().contains("keep, drop"));
        assert_eq!("drop".parse::<ReleaseHeld>(), Ok(ReleaseHeld::Drop));
    }

    #[test]
    fn the_door_reads_as_a_person_reads_it() {
        let take_down = TakeDown::DEFAULT;
        assert_eq!(take_down.door_words(), "the door holds what arrives");
        let timed = TakeDown { door: Door::Hide, door_for_minutes: Some(30), ..take_down };
        assert_eq!(timed.door_words(), "the door holds what arrives, out of sight, for 30 minutes, then refuses");
        assert_eq!(TakeDown::REMOVE.door_words(), "the door refuses what arrives");
    }
}
