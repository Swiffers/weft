//! Every name a node chooses is spelled the way weft spells it
//! (`weft_core::casing`): the node type in PascalCase; its ports, its
//! settings, the fields of its record types, the values its connection
//! stores and every key of its `metadata.json` in camelCase. Discovery
//! runs these checks on every node it loads, the standard library's and
//! a project's own alike, and turns each name that is off into a catalog
//! warning naming the node, the file and the spelling that fits. A
//! warning never leaves a node out: the node loads as usual.
//!
//! Only names weft or the node author choose are held to this. What is
//! someone else's (a value the node passes to a provider, a field a
//! third-party library serializes) is DATA, and the walk skips it; each
//! skip below is listed with the reason it is not ours to name.
//!
//! Discovery calls this on the `serde_json::Value` and the
//! `NodeMetadata` it already holds, so the check reads no file and
//! parses nothing a second time.

use serde_json::{Map, Value};
use weft_core::casing::Casing;
use weft_core::node::NodeMetadata;
use weft_core::weft_type::WeftType;

/// Port names the LANGUAGE reserves. Every one starts with `_`, which
/// keeps it out of every node's own namespace, so it is the language's
/// spelling and never a node's.
const RESERVED_PORTS: &[(&str, &str)] = &[
    (weft_core::run::skip::SHOULD_FLOW_PORT, "the gate every node takes, owned by the language"),
    (weft_core::run::skip::SHOULD_NOT_FLOW_PORT, "the inverted gate, owned by the language"),
    (weft_core::tag::TAGS_CONFIG_KEY, "the tags key the language reads off every node"),
];

/// Declared types whose record fields copy a third-party wire format
/// verbatim. A rename here breaks the round trip the type exists for.
const THIRD_PARTY_TYPES: &[(&str, &str)] = &[
    (
        "ChatMessage",
        "mirrors minillmlib's message serde field for field (`tool_call_id`, `tool_calls`, \
         `cache_breakpoint`); `tests/chat_types.rs` holds the two together",
    ),
    (
        "ChatContentPart",
        "mirrors minillmlib's content-part serde, itself the OpenAI wire shape (`image_url`, \
         `input_audio`, `duration_secs`); `tests/chat_types.rs` holds the two together",
    ),
];

/// How the walk treats the value at one place in a `metadata.json`.
#[derive(Clone, Copy)]
enum Place {
    /// Free data: neither its keys nor anything below them are
    /// metadata keys, so nothing there is checked.
    Data,
    /// A map whose keys are NAMES of something (a type, a field, a
    /// topic) rather than metadata keys: each key is checked against
    /// `Casing` as a `what`, with a trailing `?` (an optional field)
    /// taken off first, and each entry's value is walked as `*`.
    NamesAre(Casing, &'static str),
}

/// Where a `metadata.json` holds something other than metadata keys.
/// A pattern is the path from the file's root, one segment per item:
/// map keys, `[]` for any list item, `*` for any one key, `**` for any
/// run of segments. Each entry says why. The patterns are written
/// pre-split so the walk, which runs on every node discovery loads,
/// never allocates to match one.
const PLACES: &[(&[&str], Place, &str)] = &[
    (
        &["inputs", "[]", "default"],
        Place::Data,
        "the value an input gets when nothing drives it: the node's data",
    ),
    (
        &["validate", "**", "equals"],
        Place::Data,
        "the JSON value a `config_equals` rule compares a setting against: the setting's data",
    ),
    (
        &["inputs", "[]", "widget", "sources", "[]", "where"],
        Place::Data,
        "dotted paths into the PROVIDER's list items (`can_do_text_to_speech`): the provider's names",
    ),
    (&["service", "**", "body"], Place::Data, "the request body sent to the provider as written: the provider's names"),
    (
        &["service", "ownPage", "mint", "payload"],
        Place::Data,
        "the app-creation payload sent to the provider as written",
    ),
    (
        &["service", "acquisition", "authParams"],
        Place::Data,
        "extra consent-URL parameters (`access_type`): the OAuth provider's parameter names",
    ),
    (
        &["service", "acquisition", "extraParams"],
        Place::Data,
        "extra token-request parameters: the provider's names",
    ),
    (&["service", "acquisition", "claims"], Place::Data, "JWT claims (`iss`, `sub`): the JWT standard's names"),
    (
        &["accessApps", "*"],
        Place::Data,
        "an OAuth app's registration as the token exchange sends it: `client_id` and `client_secret` are \
         OAuth's parameter names, and the extras are named by the service's `registrationFields`, \
         which are checked where the service declares them",
    ),
    // A service name is a value of a fixed set (the services weft knows):
    // a node names it as a value (`"service": "google"`), so it takes the
    // snake_case every fixed-set value takes.
    (&["accessApps"], Place::NamesAre(Casing::Snake, "service name"), ""),
    (&["types"], Place::NamesAre(Casing::Pascal, "declared type"), ""),
    (&["firesWith"], Place::NamesAre(Casing::Camel, "fire payload field"), ""),
    (&["service", "events", "*", "fields"], Place::NamesAre(Casing::Camel, "event field"), ""),
    // A topic is a value of a fixed set (the service's event kinds): a
    // trigger names it as a value (`"drive_changes"`), so it takes the
    // snake_case every fixed-set value takes.
    (&["service", "events"], Place::NamesAre(Casing::Snake, "event topic"), ""),
];

/// The stored values a connection keeps are named by the recipe and
/// read by name (`{smtpHost}` in a template, `requiresValues`, node
/// code), so each is a name weft chooses even though it sits in a JSON
/// string. These are the lists whose items' `name` is one.
const VALUE_NAME_LISTS: &[&str] = &["fields", "captures", "registrationFields"];

/// Every name in one `metadata.json` document (a node's own, or a
/// package root's defaults) that is off: every key that is a metadata key
/// or a name, at every depth, every stored-value name, and the record
/// fields of every type the document declares. One message per name, as
/// `Casing::check` words it. Declared types are parsed through the
/// registry that is active, so call this inside discovery's
/// `TypeRegistry::scoped`.
pub(crate) fn check_document(document: &Map<String, Value>, out: &mut Vec<String>) {
    walk_map(out, &mut Vec::new(), document, place_at(&[]));
    if let Some(types) = document.get("types").and_then(Value::as_object) {
        for name in types.keys() {
            if let Some(ty) = WeftType::parse(name) {
                check_record_fields(out, &ty, true);
            }
        }
    }
}

/// Every name a loaded node chooses that is off, read from its typed
/// metadata: the node type, its ports (and the record fields of their
/// types), the stored values its inputs require, and the ports its
/// `portsFromConfig` specs make.
pub(crate) fn check_node(meta: &NodeMetadata, out: &mut Vec<String>) {
    let reserved = |name: &str| RESERVED_PORTS.iter().any(|(r, _)| *r == name);
    check(out, Casing::Pascal, "node type", &meta.node_type);
    for input in &meta.inputs {
        if !reserved(&input.name) {
            check(out, Casing::Camel, "input", &input.name);
        }
        check_record_fields(out, &input.input_type, false);
        for value in input.requires_values.iter().flatten() {
            check(out, Casing::Camel, "stored value name", value);
        }
    }
    for output in &meta.outputs {
        if !reserved(&output.name) {
            check(out, Casing::Camel, "output", &output.name);
        }
        check_record_fields(out, &output.port_type, false);
    }
    if let Some(pfc) = &meta.ports_from_config {
        for spec in &pfc.specs {
            check(out, Casing::Camel, "entry key field", &spec.key_field);
            for field in &spec.fields {
                check(out, Casing::Camel, "entry field", &field.key);
            }
            // A template makes a port per entry; fill it with a
            // well-spelled key and the port it makes must be too.
            for template in spec.adds_inputs.iter().chain(&spec.adds_outputs) {
                let port = template.name_template.replace("{key}", "answer");
                check(out, Casing::Camel, "port made by a template", &port);
            }
        }
    }
}

fn check(out: &mut Vec<String>, casing: Casing, what: &str, name: &str) {
    if let Err(message) = casing.check(what, name) {
        out.push(message);
    }
}

fn matches(pattern: &[&str], path: &[&str]) -> bool {
    match (pattern.split_first(), path.split_first()) {
        (None, None) => true,
        (Some((&"**", rest)), _) => {
            matches(rest, path) || (!path.is_empty() && matches(pattern, &path[1..]))
        }
        (Some((p, rest)), Some((s, tail))) => (*p == "*" || p == s) && matches(rest, tail),
        _ => false,
    }
}

fn place_at(path: &[&str]) -> Option<Place> {
    PLACES.iter().find_map(|(pattern, place, _)| matches(pattern, path).then_some(*place))
}

/// Walk one value of a `metadata.json`, checking every key that is a
/// metadata key or a name, and every stored-value name. `path` is where
/// the value sits, as `PLACES` patterns spell it.
fn walk<'a>(out: &mut Vec<String>, path: &mut Vec<&'a str>, value: &'a Value) {
    let place = place_at(path);
    if matches!(place, Some(Place::Data)) {
        return;
    }
    match value {
        Value::Object(map) => walk_map(out, path, map, place),
        Value::Array(items) => {
            path.push("[]");
            for item in items {
                walk(out, path, item);
            }
            path.pop();
        }
        _ => {}
    }
}

/// The keys of one map at `path`, which sits at `place`, and every value
/// below them.
fn walk_map<'a>(out: &mut Vec<String>, path: &mut Vec<&'a str>, map: &'a Map<String, Value>, place: Option<Place>) {
    let in_value_list = path.len() >= 2
        && path[path.len() - 1] == "[]"
        && path.first() == Some(&"service")
        && VALUE_NAME_LISTS.contains(&path[path.len() - 2]);
    for (key, child) in map {
        let segment = match place {
            Some(Place::NamesAre(casing, what)) => {
                check(out, casing, what, key.strip_suffix('?').unwrap_or(key));
                "*"
            }
            _ => {
                check(out, Casing::Camel, "metadata key", key);
                key.as_str()
            }
        };
        if in_value_list && key == "name" {
            if let Some(name) = child.as_str() {
                check(out, Casing::Camel, "stored value name", name);
            }
        }
        path.push(segment);
        walk(out, path, child);
        path.pop();
    }
}

/// Every record field name inside `ty`, checked camelCase, except
/// inside a declared type that copies a third party.
fn check_record_fields(out: &mut Vec<String>, ty: &WeftType, declaration: bool) {
    match ty {
        WeftType::Record(fields) => {
            for field in fields {
                check(out, Casing::Camel, "record field", &field.name);
                check_record_fields(out, &field.ty, false);
            }
        }
        // A named type's fields are checked once, where it is declared
        // (`declaration`); everywhere it is used, it is only its name, so
        // a package root's type warns on the root and never again on each
        // member whose ports use it.
        WeftType::Named { name, body } => {
            if declaration && !THIRD_PARTY_TYPES.iter().any(|(t, _)| t == name) {
                check_record_fields(out, body, false);
            }
        }
        WeftType::List(inner) | WeftType::Generator(inner) => check_record_fields(out, inner, false),
        WeftType::Dict(k, v) => {
            check_record_fields(out, k, false);
            check_record_fields(out, v, false);
        }
        WeftType::Union(members) => {
            for m in members {
                check_record_fields(out, m, false);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(document: Value) -> Vec<String> {
        let mut out = Vec::new();
        check_document(document.as_object().expect("an object"), &mut out);
        out
    }

    #[test]
    fn a_key_that_is_data_or_a_third_partys_is_not_held_to_weft_spelling() {
        let found = messages(serde_json::json!({
            "inputs": [{ "name": "x", "default": { "user_id": 1 } }],
            "accessApps": { "google": { "label": "Acme", "client_id": "id" } },
            "service": { "acquisition": { "authParams": { "access_type": "offline" } } },
        }));
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_snake_case_metadata_key_and_a_wrong_name_are_each_one_message() {
        let found = messages(serde_json::json!({
            "some_key": 1,
            "service": { "events": { "driveChanges": { "fields": { "file_id": "String" } } } },
        }));
        let mut found = found;
        found.sort();
        assert_eq!(
            found,
            [
                "event field 'file_id' is not camelCase: write 'fileId'",
                "event topic 'driveChanges' is not snake_case: write 'drive_changes'",
                "metadata key 'some_key' is not camelCase: write 'someKey'",
            ]
        );
    }
}
