//! The health rules of an infra node: what the supervisor does about a
//! copy of the node whose units stop being ready, and when it undoes it.
//!
//! The node's author writes them in [`InfraSpec::health_rules`], next to
//! each unit's windows (`Unit::health`). A rule reads ONE copy of the node
//! at a time: that copy's own units, whether the project's triggers are
//! listening, and whether the health check took something down. So one
//! instance's broken copy says nothing about another's, and two nodes'
//! rules never read each other's units. A node that writes none runs
//! [`built_in_rules`].
//!
//! On the wire (camelCase fields, snake_case `kind`s):
//!
//! ```json
//! "healthRules": [
//!   { "name": "take-down-while-flaky",
//!     "when": { "kind": "all", "conditions": [
//!       { "kind": "unit_flaky", "unit": "db" },
//!       { "kind": "listening" } ] },
//!     "action": { "kind": "take_down_triggers" } },
//!   { "name": "bring-back-once-healthy",
//!     "when": { "kind": "all", "conditions": [
//!       { "kind": "not", "condition": { "kind": "unit_flaky" } },
//!       { "kind": "taken_down_by_health" } ] },
//!     "action": { "kind": "bring_back" },
//!     "timeoutSeconds": 600 } ]
//! ```

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// How long a rule's action may take when the rule does not say: half an
/// hour.
pub const DEFAULT_RULE_TIMEOUT_SECONDS: NonZeroU32 = NonZeroU32::new(1800).expect("not zero");

fn default_timeout() -> NonZeroU32 {
    DEFAULT_RULE_TIMEOUT_SECONDS
}

/// The health rules an infra node wrote, as its copy's row stores them
/// (`infra_node.health_rules_json`).
pub const STORED_RULES: crate::stored::Kind<Vec<HealthRule>> = crate::stored::Kind::new("infra node's health rules", &[]);

/// One rule: when its condition holds for a copy, its action runs for
/// that copy, once, until the condition stops holding for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HealthRule {
    /// Unique within the node: what `weft status` names when its action
    /// gave up, and what keeps the rule's state for each copy.
    pub name: String,
    pub when: HealthCondition,
    pub action: HealthAction,
    /// How long the action may take, from the supervisor queueing it to
    /// the dispatcher finishing the take-down or bring-back it queued:
    /// past it, or once that command failed, the attempt counts as failed
    /// and is tried again later (after 5 s, then 10 s; the third failure
    /// in a row gives up). The rule counts as fired while its command
    /// runs, and stays fired once it succeeded.
    #[serde(default = "default_timeout")]
    pub timeout_seconds: NonZeroU32,
}

/// What a rule reads, on one copy of its node. A condition on units reads
/// only the units the host has reported: one it has not reported yet is
/// neither ready nor broken, so no condition on units is true of it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HealthCondition {
    /// A unit of the copy is flaky: not ready for its whole
    /// `flakyAfterSeconds` after having been ready, and not yet ready
    /// again for its whole `recoveryAfterSeconds`. The latch that sets the
    /// unit's `flaky` status, so one bad reading never makes it true.
    /// `unit` names one of the node's units; left out, any unit.
    UnitFlaky {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unit: Option<String>,
    },
    /// Some trigger of the project is on, and none is being switched on.
    Listening,
    /// Something the health check took down is still down. A person's
    /// stop or deactivate clears that mark on what it takes down, so a
    /// rule gated on this never overrides a person.
    TakenDownByHealth,
    /// Every one of `conditions` holds (true when there is none).
    All { conditions: Vec<HealthCondition> },
    /// One of `conditions` holds (false when there is none).
    Any { conditions: Vec<HealthCondition> },
    /// `condition` does not hold.
    Not { condition: Box<HealthCondition> },
}

/// What a rule does for the copy its condition holds for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HealthAction {
    /// Take down the triggers that are on whose cut contains the copy,
    /// the way `weft infra stop` of that copy would: their door holds what
    /// arrives, what they hold is kept, and the runs going on it are
    /// stopped (`crate::take_down::TakeDown::HEALTH`). A trigger whose cut
    /// does not contain it keeps running.
    TakeDownTriggers,
    /// Bring back what the health check took down, except the triggers
    /// whose cut contains a copy still broken: one whose node runs a
    /// `bring_back` rule and none holds for it, or one whose node runs
    /// none and whose own take-down took something down (that stays down
    /// until a person brings it back). A copy that took nothing down and
    /// runs no `bring_back` rule holds nothing back. Never what a person
    /// or an infra command took down.
    BringBack,
}

/// One unit of one copy, as the supervisor's look saw it. Only units the
/// host has reported, and only those expected to run now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnitReading {
    pub unit: String,
    /// The host reports it running and every readiness check passing.
    pub ready: bool,
    /// Its latched health: what makes `unit_flaky` true.
    pub flaky: bool,
}

/// Everything a condition reads about one copy.
#[derive(Debug, Clone, Copy)]
pub struct CopyReading<'a> {
    /// The copy's units the host reported.
    pub units: &'a [UnitReading],
    /// The project's listening reads `active` (`HealthCondition::Listening`).
    pub listening: bool,
    /// Something the health check took down is still down
    /// (`HealthCondition::TakenDownByHealth`).
    pub taken_down_by_health: bool,
}

impl HealthCondition {
    /// Whether the condition holds for the copy `copy` reads.
    pub fn holds(&self, copy: &CopyReading<'_>) -> bool {
        match self {
            HealthCondition::UnitFlaky { unit } => {
                copy.units.iter().any(|u| u.flaky && unit.as_ref().is_none_or(|name| *name == u.unit))
            }
            HealthCondition::Listening => copy.listening,
            HealthCondition::TakenDownByHealth => copy.taken_down_by_health,
            HealthCondition::All { conditions } => conditions.iter().all(|c| c.holds(copy)),
            HealthCondition::Any { conditions } => conditions.iter().any(|c| c.holds(copy)),
            HealthCondition::Not { condition } => !condition.holds(copy),
        }
    }

    /// Every unit the condition names, wherever it sits in it.
    pub fn units_named(&self) -> Vec<&str> {
        match self {
            HealthCondition::UnitFlaky { unit } => unit.as_deref().into_iter().collect(),
            HealthCondition::Listening | HealthCondition::TakenDownByHealth => Vec::new(),
            HealthCondition::All { conditions } | HealthCondition::Any { conditions } => {
                conditions.iter().flat_map(HealthCondition::units_named).collect()
            }
            HealthCondition::Not { condition } => condition.units_named(),
        }
    }
}

/// The rules of a node that writes none:
///
/// 1. **take-down-while-flaky**: while the project listens and a unit of
///    the copy is flaky, take down the triggers whose cut contains the
///    copy.
/// 2. **bring-back-once-healthy**: while something the health check took
///    down is still down and no unit of the copy is flaky, bring it back
///    (except what another copy still broken holds down).
pub fn built_in_rules() -> Vec<HealthRule> {
    let a_unit_flaky = HealthCondition::UnitFlaky { unit: None };
    vec![
        HealthRule {
            name: "take-down-while-flaky".into(),
            when: HealthCondition::All { conditions: vec![a_unit_flaky.clone(), HealthCondition::Listening] },
            action: HealthAction::TakeDownTriggers,
            timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
        },
        HealthRule {
            name: "bring-back-once-healthy".into(),
            when: HealthCondition::All {
                conditions: vec![
                    HealthCondition::Not { condition: Box::new(a_unit_flaky) },
                    HealthCondition::TakenDownByHealth,
                ],
            },
            action: HealthAction::BringBack,
            timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
        },
    ]
}

/// The rules a copy runs: the ones its node wrote (`None`: it wrote none,
/// so the built-in ones; an empty list: none at all).
pub fn rules_of(written: Option<&[HealthRule]>) -> Vec<HealthRule> {
    match written {
        Some(rules) => rules.to_vec(),
        None => built_in_rules(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(unit: &str, ready: bool, flaky: bool) -> UnitReading {
        UnitReading { unit: unit.into(), ready, flaky }
    }

    fn copy(units: &[UnitReading]) -> CopyReading<'_> {
        CopyReading { units, listening: true, taken_down_by_health: false }
    }

    /// The example the module documents reads back as written, and writes
    /// back as the same JSON: camelCase fields, snake_case kinds, a unit
    /// left out meaning any, a timeout left out meaning the default.
    #[test]
    fn rules_round_trip_through_their_wire_form() {
        let json = serde_json::json!([
            { "name": "take-down-while-flaky",
              "when": { "kind": "all", "conditions": [
                { "kind": "unit_flaky", "unit": "db" },
                { "kind": "listening" } ] },
              "action": { "kind": "take_down_triggers" } },
            { "name": "bring-back-once-healthy",
              "when": { "kind": "all", "conditions": [
                { "kind": "not", "condition": { "kind": "unit_flaky" } },
                { "kind": "any", "conditions": [{ "kind": "taken_down_by_health" }] } ] },
              "action": { "kind": "bring_back" },
              "timeoutSeconds": 600 }
        ]);
        let rules: Vec<HealthRule> = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(rules[0].timeout_seconds, DEFAULT_RULE_TIMEOUT_SECONDS);
        assert_eq!(rules[0].when.units_named(), vec!["db"]);
        assert_eq!(rules[1].action, HealthAction::BringBack);
        let mut expected = json;
        expected[0]["timeoutSeconds"] = 1800.into();
        assert_eq!(serde_json::to_value(&rules).unwrap(), expected);
        assert_eq!(serde_json::from_value::<Vec<HealthRule>>(serde_json::to_value(built_in_rules()).unwrap()).unwrap(), built_in_rules());
    }

    /// A misspelt field, an unknown kind, a field a kind does not take and
    /// a zero timeout are refused, never read as something else.
    #[test]
    fn a_rule_that_says_something_unknown_is_refused() {
        let rule = |when: serde_json::Value, extra: serde_json::Value| {
            let mut v = serde_json::json!({ "name": "r", "when": when, "action": { "kind": "bring_back" } });
            v.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            serde_json::from_value::<HealthRule>(v)
        };
        let flaky = serde_json::json!({ "kind": "unit_flaky" });
        assert!(rule(flaky.clone(), serde_json::json!({})).is_ok());
        assert!(rule(flaky.clone(), serde_json::json!({ "timeout": 5 })).is_err());
        assert!(rule(flaky.clone(), serde_json::json!({ "timeoutSeconds": 0 })).is_err());
        assert!(rule(serde_json::json!({ "kind": "node_flaky" }), serde_json::json!({})).is_err());
        assert!(rule(serde_json::json!({ "kind": "unit_flaky", "nodeId": "db" }), serde_json::json!({})).is_err());
    }

    /// `unit_flaky` reads the latch, never one reading, and only the unit
    /// it names (or any, left out); the combinators compose.
    #[test]
    fn conditions_read_the_copys_own_units() {
        let units = [reading("db", false, true), reading("cache", false, false)];
        let flaky = |unit: Option<&str>| HealthCondition::UnitFlaky { unit: unit.map(str::to_string) };
        assert!(flaky(None).holds(&copy(&units)));
        assert!(flaky(Some("db")).holds(&copy(&units)));
        assert!(!flaky(Some("cache")).holds(&copy(&units)), "not ready, not yet flaky");
        assert!(!flaky(None).holds(&copy(&[])), "a unit the host has not reported is not broken");
        let either = HealthCondition::Any { conditions: vec![flaky(Some("cache")), HealthCondition::TakenDownByHealth] };
        assert!(!either.holds(&copy(&units)));
        assert!(HealthCondition::All { conditions: vec![] }.holds(&copy(&[])));
        assert!(!HealthCondition::Any { conditions: vec![] }.holds(&copy(&[])));
        let not_listening = CopyReading { listening: false, ..copy(&units) };
        assert!(!HealthCondition::Listening.holds(&not_listening));
        assert!(HealthCondition::Not { condition: Box::new(HealthCondition::Listening) }.holds(&not_listening));
    }

    /// The built-in pair: take down while a unit is flaky and the project
    /// listens; bring back once none is and the health check took
    /// something down.
    #[test]
    fn the_built_in_rules_take_down_and_bring_back() {
        let rules = built_in_rules();
        let (take_down, bring_back) = (&rules[0], &rules[1]);
        assert_eq!((take_down.action, bring_back.action), (HealthAction::TakeDownTriggers, HealthAction::BringBack));
        let broken = [reading("db", false, true)];
        let healthy = [reading("db", true, false)];
        assert!(take_down.when.holds(&copy(&broken)));
        assert!(!take_down.when.holds(&CopyReading { listening: false, ..copy(&broken) }));
        assert!(!take_down.when.holds(&copy(&[reading("db", false, false)])), "one bad reading takes nothing down");
        assert!(!bring_back.when.holds(&copy(&healthy)), "nothing the health check took is down");
        assert!(bring_back.when.holds(&CopyReading { taken_down_by_health: true, ..copy(&healthy) }));
        assert!(!bring_back.when.holds(&CopyReading { taken_down_by_health: true, ..copy(&broken) }));
        assert_eq!(rules_of(None), rules);
        assert!(rules_of(Some(&[])).is_empty(), "a node may run none");
    }
}
