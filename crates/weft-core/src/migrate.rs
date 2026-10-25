//! Copying what one install keeps to another (`weft migrate connections`,
//! `weft migrate files`): which items a person picked, and the wire
//! shapes the copy travels in. The source keeps everything it had.

use serde::{Deserialize, Serialize};

/// Which of an install's connections or files to copy. Nothing is picked
/// unless the person says what ([`Selection::validate`]): `all`, or any
/// mix of the narrower picks, which all have to hold for an item (and,
/// within one kind, any of its values: two ids pick two items).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Selection {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub all: bool,
    /// Connection ids, or file keys.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
    /// Made at or after this moment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    /// Made before this moment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<chrono::DateTime<chrono::Utc>>,
    /// Connections of these services.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<String>,
    /// Connections of these projects (a connection shared across the
    /// tenant's projects belongs to none).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projects: Vec<uuid::Uuid>,
    /// Files whose key, past the tenant, starts with one of these
    /// (`project/<id>/`, `shared/<name>/`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prefixes: Vec<String>,
}

/// One item as a [`Selection`] looks at it.
#[derive(Debug, Clone, Copy)]
pub struct Candidate<'a> {
    pub id: &'a str,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub service: Option<&'a str>,
    pub project: Option<uuid::Uuid>,
    /// A file's key past the tenant.
    pub key: Option<&'a str>,
}

impl Selection {
    /// Refuse a selection that picks nothing, or `all` beside a narrower
    /// pick (which it would quietly override).
    pub fn validate(&self) -> Result<(), String> {
        let narrows = !self.ids.is_empty()
            || self.since.is_some()
            || self.until.is_some()
            || !self.services.is_empty()
            || !self.projects.is_empty()
            || !self.prefixes.is_empty();
        match (self.all, narrows) {
            (false, false) => Err("say what to copy: --all for everything, or --id, --since/--until, --service, --project or --prefix".into()),
            (true, true) => Err("--all copies everything, so it goes alone; drop it to copy only what the other flags pick".into()),
            _ => Ok(()),
        }
    }

    /// Whether `item` is picked.
    pub fn picks(&self, item: &Candidate<'_>) -> bool {
        if self.all {
            return true;
        }
        let any_of = |wanted: &[String], have: Option<&str>| wanted.is_empty() || have.is_some_and(|have| wanted.iter().any(|w| w == have));
        any_of(&self.ids, Some(item.id))
            && self.since.is_none_or(|since| item.created_at >= since)
            && self.until.is_none_or(|until| item.created_at < until)
            && any_of(&self.services, item.service)
            && (self.projects.is_empty() || item.project.is_some_and(|p| self.projects.contains(&p)))
            && (self.prefixes.is_empty() || item.key.is_some_and(|key| self.prefixes.iter().any(|p| key.starts_with(p.as_str()))))
    }
}

/// What a copy did with one item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum Outcome {
    Copied,
    /// The target holds exactly this already.
    AlreadyThere,
    /// Not copied, and why, with what to do about it when there is something.
    Skipped { why: String },
}

/// `POST /access/migrate/key` on the install connections move TO: a key
/// pair made for one move, whose private half that install keeps (sealed,
/// used once, gone after a few minutes).
// SYNC: TransferKey <-> crates/weft-broker/src/access_admin.rs migrate_key, crates/weft-dispatcher/src/api/access.rs migrate_key, crates/weft-cli/src/commands/migrate.rs connections
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransferKey {
    pub key_id: uuid::Uuid,
    /// The public half, base64 of an uncompressed P-256 point.
    pub public_key: String,
}

/// `POST /access/migrate/export` on the install connections move FROM.
// SYNC: ExportConnections <-> crates/weft-broker/src/access_admin.rs migrate_export, crates/weft-dispatcher/src/api/access.rs migrate_export, crates/weft-cli/src/commands/migrate.rs connections
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportConnections {
    pub selection: Selection,
    /// The target's [`TransferKey::public_key`]; `None` only lists what
    /// would be copied (`--dry-run`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key: Option<String>,
}

/// One connection an export picked, in plain words: nothing secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExportedConnection {
    pub id: uuid::Uuid,
    pub service: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<uuid::Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Whether it is a browser sign-in, which renews itself with a refresh
    /// token: a copy shares that token with the source, and a provider
    /// that replaces it each time it is used breaks whichever copy renews
    /// second.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sign_in: bool,
    /// Why it is not in the sealed batch, when it is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
}

/// Which connection one of the source install's picks points at: what to
/// pick again on the target (picks belong to one install).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConnectionPick {
    pub project_id: uuid::Uuid,
    /// The node's address in the program.
    pub node: String,
    pub field: String,
    pub grant_id: uuid::Uuid,
}

/// What an export answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExportedConnections {
    pub connections: Vec<ExportedConnection>,
    /// The picked connections not skipped, sealed to the target's key;
    /// `None` for a dry run, or when none is left to copy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<String>,
    /// The source's picks of the connections in the batch.
    #[serde(default)]
    pub picks: Vec<ConnectionPick>,
}

/// `POST /access/migrate/import` on the target: the sealed batch, and the
/// key it was sealed to.
// SYNC: ImportConnections <-> crates/weft-broker/src/access_admin.rs migrate_import, crates/weft-dispatcher/src/api/access.rs migrate_import, crates/weft-cli/src/commands/migrate.rs connections
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportConnections {
    pub key_id: uuid::Uuid,
    pub sealed: String,
}

/// What became of one connection of an import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedConnection {
    pub id: uuid::Uuid,
    pub service: String,
    #[serde(flatten)]
    pub outcome: Outcome,
}

/// What an import answers, one entry per connection of the batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImportedConnections {
    pub connections: Vec<ImportedConnection>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(day: u32) -> chrono::DateTime<chrono::Utc> {
        chrono::NaiveDate::from_ymd_opt(2026, 10, day).unwrap().and_hms_opt(12, 0, 0).unwrap().and_utc()
    }

    fn item<'a>(id: &'a str, day: u32, service: Option<&'a str>, project: Option<uuid::Uuid>, key: Option<&'a str>) -> Candidate<'a> {
        Candidate { id, created_at: at(day), service, project, key }
    }

    #[test]
    fn nothing_is_picked_unless_asked_and_all_goes_alone() {
        assert!(Selection::default().validate().unwrap_err().contains("--all"));
        assert!(Selection { all: true, ..Default::default() }.validate().is_ok());
        assert!(Selection { all: true, services: vec!["slack".into()], ..Default::default() }.validate().unwrap_err().contains("goes alone"));
        assert!(Selection { since: Some(at(1)), ..Default::default() }.validate().is_ok());
    }

    #[test]
    fn every_given_pick_has_to_hold() {
        let p = uuid::Uuid::from_u128(7);
        let slack = item("a", 10, Some("slack"), Some(p), None);
        let google = item("b", 20, Some("google"), None, None);
        let all = Selection { all: true, ..Default::default() };
        assert!(all.picks(&slack) && all.picks(&google));
        let ids = Selection { ids: vec!["a".into(), "c".into()], ..Default::default() };
        assert!(ids.picks(&slack) && !ids.picks(&google));
        let window = Selection { since: Some(at(10)), until: Some(at(20)), ..Default::default() };
        assert!(window.picks(&slack), "since is inclusive");
        assert!(!window.picks(&google), "until is not");
        let service_and_project = Selection { services: vec!["slack".into(), "google".into()], projects: vec![p], ..Default::default() };
        assert!(service_and_project.picks(&slack));
        assert!(!service_and_project.picks(&google), "a connection of no project is not one of the project's");
        let prefix = Selection { prefixes: vec!["project/7/".into()], ..Default::default() };
        assert!(prefix.picks(&item("k", 1, None, None, Some("project/7/x"))));
        assert!(!prefix.picks(&item("k", 1, None, None, Some("project/70/x"))), "a prefix ends where its slash says");
        assert!(!prefix.picks(&slack), "an item with no key is no file");
    }

    #[test]
    fn the_wire_shapes_read_back() {
        let selection = Selection { ids: vec!["a".into()], since: Some(at(1)), ..Default::default() };
        let v = serde_json::to_value(&selection).unwrap();
        assert_eq!(v, serde_json::json!({ "ids": ["a"], "since": "2026-10-01T12:00:00Z" }));
        assert_eq!(serde_json::from_value::<Selection>(v).unwrap(), selection);
        let imported = ImportedConnections {
            connections: vec![
                ImportedConnection { id: uuid::Uuid::nil(), service: "slack".into(), outcome: Outcome::Copied },
                ImportedConnection { id: uuid::Uuid::nil(), service: "exa".into(), outcome: Outcome::Skipped { why: "no key".into() } },
            ],
        };
        let v = serde_json::to_value(&imported).unwrap();
        assert_eq!(v["connections"][0]["outcome"], "copied");
        assert_eq!(v["connections"][1]["why"], "no key");
        assert_eq!(serde_json::from_value::<ImportedConnections>(v).unwrap(), imported);
        let key = TransferKey { key_id: uuid::Uuid::nil(), public_key: "pk".into() };
        assert_eq!(serde_json::to_value(&key).unwrap()["publicKey"], "pk");
    }
}
