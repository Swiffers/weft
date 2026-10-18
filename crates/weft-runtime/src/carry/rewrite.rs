//! What the carry-over changes in a row on its way from the `serverless`
//! generation to the `run` one: names inside the data weft stored,
//! which no SQL migration could reach (compressed run records, sealed
//! connection values, file keys). Pure: a row in, the row the new
//! database holds out, so every rule is tested without a database.
//!
//! The rules only ever rename what weft named. A user's own data (a
//! node's output, a port value, a payload, a node's settings) rides in
//! the subtrees [`USER_DATA`] lists and is never walked.

use serde_json::{Map, Value};

/// The keys under which a value is the user's own data, whose inside is
/// never renamed: node inputs and outputs, payloads, answers, settings,
/// captured trigger values, a cost's provider metadata.
const USER_DATA: &[&str] = &[
    "value", "output", "outputs", "input", "inputs", "payload", "answer", "config", "captured", "port_snapshot",
    "portLiterals", "scratch", "metadata", "kind_state", "from", "emit", "caller", "expected", "question", "body",
    "values", "instance_values", "data", "fire_payload", "consumer_payload", "selection",
];

/// Which way a key is renamed.
#[derive(Clone, Copy)]
enum Spelling {
    /// A Rust type's own snake_case field: `execution_id` -> `run_id`.
    Snake,
    /// A type whose wire names became camelCase: `execution_id` -> `runId`,
    /// `at_unix` -> `atUnix`.
    Camel,
}

/// `key`, renamed for `spelling`.
fn renamed(key: &str, spelling: Spelling) -> String {
    let snake = key.replace("execution", "run");
    match spelling {
        Spelling::Snake => snake,
        Spelling::Camel => {
            let mut out = String::with_capacity(snake.len());
            let mut upper = false;
            for c in snake.chars() {
                if c == '_' {
                    upper = !out.is_empty();
                } else if upper {
                    out.push(c.to_ascii_uppercase());
                    upper = false;
                } else {
                    out.push(c);
                }
            }
            out
        }
    }
}

/// Rename the keys of `object` itself (not its children).
fn rename_keys(object: &mut Map<String, Value>, spelling: Spelling) {
    let keys: Vec<String> = object.keys().cloned().collect();
    for key in keys {
        let new = renamed(&key, spelling);
        if new != key {
            let value = object.remove(&key).expect("a key just listed");
            object.insert(new, value);
        }
    }
}

/// Rename every key under `value` that names an execution
/// (`execution_id`, `setup_execution_id`, ...) to the run, in snake_case,
/// skipping the user's data.
pub fn run_keys(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let keys: Vec<String> = object.keys().cloned().collect();
            for key in keys {
                if key.contains("execution") {
                    let child = object.remove(&key).expect("a key just listed");
                    object.insert(renamed(&key, Spelling::Snake), child);
                }
            }
            for (key, child) in object.iter_mut() {
                if !USER_DATA.contains(&key.as_str()) {
                    run_keys(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(run_keys),
        _ => {}
    }
}

/// Rename every key of `value` and of everything under it to camelCase,
/// skipping the user's data: a value of a type that became camelCase
/// whole.
pub fn camel_keys(value: &mut Value) {
    match value {
        Value::Object(object) => {
            rename_keys(object, Spelling::Camel);
            for (key, child) in object.iter_mut() {
                if !USER_DATA.contains(&key.as_str()) {
                    camel_keys(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(camel_keys),
        _ => {}
    }
}

/// A run-scoped file's key under its new scope tag: `<tenant>/exec/<run>/<id>`
/// (or the tenant-relative `exec/<run>/<id>`) becomes `.../run/...`.
/// `None` for anything else. The run segment must be a run's id, so a
/// user's string that merely looks like a path is left alone.
pub fn file_key(s: &str) -> Option<String> {
    let parts: Vec<&str> = s.split('/').collect();
    let (at, len) = match parts.as_slice() {
        [_, "exec", run, id] if is_run_id(run) && !id.is_empty() => (1, 4),
        ["exec", run, id] if is_run_id(run) && !id.is_empty() => (0, 3),
        _ => return None,
    };
    debug_assert_eq!(parts.len(), len);
    let mut parts = parts;
    parts[at] = "run";
    Some(parts.join("/"))
}

fn is_run_id(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok()
}

/// Every string under `value` that is a run-scoped file's key, moved to
/// the new scope tag, wherever it is: a file value inside a node's output
/// is weft's reference to weft's file.
pub fn file_keys(value: &mut Value) {
    match value {
        Value::String(s) => {
            if let Some(new) = file_key(s) {
                *s = new;
            }
        }
        Value::Object(object) => object.values_mut().for_each(file_keys),
        Value::Array(items) => items.iter_mut().for_each(file_keys),
        _ => {}
    }
}

/// Every cancel cause and storage scope under `value` that named an
/// execution, renamed: a cause `{"kind": "execution", "by", "tag"}` (one
/// run stopping another) and a scope `{"kind": "execution"}`.
pub fn execution_kinds(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if object.get("kind").and_then(Value::as_str) == Some("execution") {
                let a_cause = object.contains_key("by");
                let a_scope = object.len() == 1;
                if a_cause || a_scope {
                    object.insert("kind".into(), Value::String("run".into()));
                }
            }
            for (key, child) in object.iter_mut() {
                if !USER_DATA.contains(&key.as_str()) {
                    execution_kinds(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(execution_kinds),
        _ => {}
    }
}

/// What every JSON value weft stored gets: execution keys renamed,
/// execution kinds renamed, file keys moved.
pub fn everywhere(value: &mut Value) {
    run_keys(value);
    execution_kinds(value);
    file_keys(value);
}

/// The `metadata.json` keys whose spelling weft's own types chose, which
/// became camelCase (a node's metadata, a connection's spec, its events
/// recipe): stored copies of them sit in connection snapshots and in
/// stored programs.
const METADATA_KEYS: &[&str] = &[
    "requires_infra", "per_instance", "instance_filled", "free_text", "depends_on", "mime_types", "cursor_param",
    "cursor_path", "connection_optional", "own_page", "all_permissions_url", "own_only", "token_url", "scope_delimiter",
    "auth_params", "extra_params", "registration_fields", "token_auth", "jwt_ttl_secs", "auth_url", "token_path",
    "expires_path", "expect_status", "access_key_id", "secret_access_key", "consumer_key", "consumer_secret",
    "token_secret", "when_field", "url_from", "event_path", "multi_account", "id_path", "route_by", "signature_header",
    "timestamp_header", "jwks_url", "renew_margin_secs",
];

/// Under these keys of a spec or a stored program sit names somebody
/// else chose, never renamed: a provider's (OAuth parameters, JWT claims,
/// request bodies, a source's `where` paths) and the user's (a node's
/// settings and literals, a default, a value a rule compares).
const METADATA_SKIP: &[&str] = &[
    "authParams", "extraParams", "claims", "auth_params", "extra_params", "body", "where", "config", "portLiterals",
    "default", "equals", "example", "value", "values", "payload", "captured",
];

/// The names connection values are stored under that weft chose, which
/// became camelCase. OAuth's own (`access_token`, `client_id`, ...) keep
/// their spelling.
const VALUE_NAMES: &[&str] = &[
    "account_name", "key_label", "user_email", "bot_username", "imap_host", "imap_port", "smtp_host", "smtp_port",
    "send_as", "resource_id", "team_id", "app_token", "jwks_url", "access_key_id", "secret_access_key", "signing_secret",
    "public_key", "subscription_id", "subscription_token", "receiver_url", "expires_at",
];

/// Every key of [`METADATA_KEYS`] under `value`, camelCase, skipping
/// [`METADATA_SKIP`].
pub fn metadata_keys(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let keys: Vec<String> = object.keys().filter(|k| METADATA_KEYS.contains(&k.as_str())).cloned().collect();
            for key in keys {
                let child = object.remove(&key).expect("a key just listed");
                object.insert(renamed(&key, Spelling::Camel), child);
            }
            for (key, child) in object.iter_mut() {
                if !METADATA_SKIP.contains(&key.as_str()) {
                    metadata_keys(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(metadata_keys),
        _ => {}
    }
}

/// A loop's settings that became camelCase. The others (`parallel`,
/// `over`, `carry`) are single words.
const LOOP_SETTINGS: &[(&str, &str)] = &[("max_iters", "maxIters"), ("trim_on_mismatch", "trimOnMismatch")];

/// A stored program (`weft_core::ProjectDefinition`) with its loops'
/// settings under their new names, in the two places a loop's settings
/// sit: the `config` of each `LoopIn` boundary node (what the runtime
/// reads, so a key left behind would run the loop uncapped) and a loop
/// group's `loopConfig`. Nothing else is touched: the same key in any
/// other node's settings is the user's.
pub fn loop_settings(program: &mut Value) {
    let rename = |settings: Option<&mut Value>| {
        if let Some(Value::Object(settings)) = settings {
            for (old, new) in LOOP_SETTINGS {
                if let Some(value) = settings.remove(*old) {
                    settings.insert((*new).into(), value);
                }
            }
        }
    };
    if let Some(Value::Array(nodes)) = program.get_mut("nodes") {
        for node in nodes.iter_mut() {
            if node.get("nodeType").and_then(Value::as_str) == Some(weft_core::project::boundary_types::LOOP_IN) {
                rename(node.get_mut("config"));
                // Where each setting was written, keyed like the config.
                rename(node.get_mut("configSpans"));
            }
        }
    }
    if let Some(Value::Array(groups)) = program.get_mut("groups") {
        for group in groups.iter_mut() {
            if group.get("kind").and_then(Value::as_str) == Some("loop") {
                rename(group.get_mut("loopConfig"));
            }
        }
    }
}

/// A stored connection value's name, as this generation stores it.
pub fn value_name(name: &str) -> String {
    match VALUE_NAMES.contains(&name) {
        true => renamed(name, Spelling::Camel),
        false => name.to_string(),
    }
}

/// A connection's stored values (`{name: value}`) under their new names.
pub fn connection_values(values: &mut Value) {
    if let Value::Object(object) = values {
        let keys: Vec<String> = object.keys().cloned().collect();
        for key in keys {
            let new = value_name(&key);
            if new != key {
                let value = object.remove(&key).expect("a key just listed");
                object.insert(new, value);
            }
        }
    }
}

/// A list of connection value names under their new names.
pub fn connection_value_names(names: &mut Value) {
    if let Value::Array(items) = names {
        for item in items {
            if let Value::String(name) = item {
                *name = value_name(name);
            }
        }
    }
}

/// The event kinds that named the execution.
fn event_kind(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "execution_started" => "run_started",
        "execution_tagged" => "run_tagged",
        "execution_completed" => "run_completed",
        "execution_failed" => "run_failed",
        "execution_cancelled" => "run_cancelled",
        _ => return None,
    })
}

/// One run record's events (the JSON array a `run_log` row holds), as
/// this generation writes them.
pub fn events(events: &mut Value) -> anyhow::Result<()> {
    let items = events.as_array_mut().ok_or_else(|| anyhow::anyhow!("a run record row is not an array of events"))?;
    for event in items {
        let object = event.as_object_mut().ok_or_else(|| anyhow::anyhow!("a run record event is not an object"))?;
        let kind = object.get("kind").and_then(Value::as_str).unwrap_or_default().to_string();
        if let Some(new) = event_kind(&kind) {
            object.insert("kind".into(), Value::String(new.into()));
        }
        if (kind == "execution_started" || kind == "run_started") && object.get("run_kind").and_then(Value::as_str) == Some("execution") {
            object.insert("run_kind".into(), Value::String("project".into()));
        }
        // The windows of a bus and of a caller's stream carry types whose
        // names became camelCase.
        for list in ["messages", "totals"] {
            if let Some(Value::Array(entries)) = object.get_mut(list) {
                for entry in entries.iter_mut() {
                    if let Value::Object(entry) = entry {
                        rename_keys(entry, Spelling::Camel);
                    }
                }
            }
        }
        everywhere(event);
    }
    Ok(())
}

/// A run spec (`run.spec`, a saved example's `spec`) as this generation
/// writes it: `frozen_from` and its fields, an answer's `gave_up`.
pub fn run_spec(spec: &mut Value) {
    let Some(object) = spec.as_object_mut() else { return };
    if let Some(mut frozen) = object.remove("frozen_from") {
        if let Value::Object(from) = &mut frozen {
            rename_keys(from, Spelling::Camel);
        }
        object.insert("frozenFrom".into(), frozen);
    }
    if let Some(Value::Object(settings)) = object.get_mut("settings") {
        for (old, new) in [("keep_for", "keepFor"), ("hold_secs", "holdSecs")] {
            if let Some(value) = settings.remove(old) {
                settings.insert(new.into(), value);
            }
        }
    }
    if let Some(Value::Array(answers)) = object.get_mut("answers") {
        for answer in answers.iter_mut() {
            if let Value::Object(answer) = answer {
                if let Some(gave_up) = answer.remove("gave_up") {
                    answer.insert("gaveUp".into(), gave_up);
                }
            }
        }
    }
    everywhere(spec);
}

/// A trigger's bake (`trigger_bake.bake_json`) as this generation writes
/// it: its own fields camelCase, the program it names and the values it
/// captured as they were.
pub fn trigger_bake(bake: &mut Value) {
    if let Value::Object(object) = bake {
        rename_keys(object, Spelling::Camel);
    }
    everywhere(bake);
}

/// The program calls whose answers became camelCase, with how deep: a
/// list of runs, a clean's counts, a list of costs, a list of
/// connections, a minted token.
fn camel_answer(call: &str) -> bool {
    matches!(call, "runs_list" | "runs_clean" | "costs_list" | "connections_list" | "tokens_mint")
}

/// A change of an instance's values (`values_change`) as this generation
/// writes it: each value given and each field cleared names its node
/// under `node`, where it said `step`.
fn values_change(call: &mut Map<String, Value>) {
    for list in ["set", "clear"] {
        let Some(Value::Array(items)) = call.get_mut(list) else { continue };
        for item in items {
            if let Value::Object(item) = item {
                if let Some(node) = item.remove("step") {
                    item.insert("node".into(), node);
                }
            }
        }
    }
}

/// A queued task's payload and result as this generation writes them.
pub fn task(kind: &str, payload: &mut Value, result: Option<&mut Value>) {
    if kind == "program_call" {
        if let Some(Value::Object(call)) = payload.get_mut("call") {
            for filter in ["filter"] {
                if let Some(Value::Object(f)) = call.get_mut(filter) {
                    rename_keys(f, Spelling::Camel);
                }
            }
            if call.get("call").and_then(Value::as_str) == Some("values_change") {
                values_change(call);
            }
        }
        let call = payload.pointer("/call/call").and_then(Value::as_str).unwrap_or_default().to_string();
        if let Some(result) = result {
            if camel_answer(&call) {
                if let Some(answer) = result.get_mut("value") {
                    camel_keys(answer);
                }
            }
            everywhere(result);
        }
    } else if let Some(result) = result {
        everywhere(result);
    }
    everywhere(payload);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_spec_takes_its_camel_case_keys_and_keeps_the_providers_names() {
        let mut v = json!({
            "service": "google",
            "connection_optional": true,
            "acquisition": { "kind": "oauth2", "token_url": "t", "auth_params": { "access_type": "offline" },
                             "grant": { "kind": "authorization_code", "auth_url": "a" } },
            "config": { "token_url": "a user's own port" },
        });
        metadata_keys(&mut v);
        assert_eq!(
            v,
            json!({
                "service": "google",
                "connectionOptional": true,
                "acquisition": { "kind": "oauth2", "tokenUrl": "t", "authParams": { "access_type": "offline" },
                                 "grant": { "kind": "authorization_code", "authUrl": "a" } },
                "config": { "token_url": "a user's own port" },
            })
        );
    }

    /// A stored program with one loop `l` (settings `parallel`,
    /// `maxIters`, `trimOnMismatch` under `max`/`trim`'s spelling) and a
    /// `Text` node whose own setting is called `max`: the shape a
    /// `project_json` column holds.
    fn program_with_a_loop(max: &str, trim: &str) -> Value {
        json!({
            "id": "9dfba28c-45c3-4ccc-aa77-985947728672",
            "nodes": [
                { "id": "l__in", "nodeType": "LoopIn", "label": null, "position": { "x": 0.0, "y": 0.0 },
                  "scope": ["l"], "groupBoundary": { "groupId": "l", "role": "In" },
                  "config": { "parentId": "l", "parallel": false, max: 5, trim: false } },
                { "id": "l__out", "nodeType": "LoopOut", "label": null, "position": { "x": 0.0, "y": 0.0 },
                  "scope": ["l"], "groupBoundary": { "groupId": "l", "role": "Out" }, "config": { "parentId": "l" } },
                { "id": "t", "nodeType": "Text", "label": null, "position": { "x": 0.0, "y": 0.0 },
                  "config": { "max_iters": "a user's own setting" } },
            ],
            "edges": [],
            "groups": [
                { "id": "l", "kind": "loop", "label": null, "loopConfig": { "parallel": false, max: 5, trim: false } },
                { "id": "g", "kind": "group", "label": null, "portLiterals": { "max_iters": 1 } },
            ],
        })
    }

    #[test]
    fn a_stored_programs_loop_settings_take_their_new_names_and_nothing_else_does() {
        let mut v = program_with_a_loop("max_iters", "trim_on_mismatch");
        loop_settings(&mut v);
        assert_eq!(v, program_with_a_loop("maxIters", "trimOnMismatch"));
        let program: weft_core::ProjectDefinition = serde_json::from_value(v).expect("still a program");
        let config = weft_core::run::loop_runtime::LoopConfig::from_node_config(&program.nodes[0].config).unwrap();
        assert_eq!(config.max_iters, Some(5), "the runtime reads the cap");
        assert!(!config.trim_on_mismatch);
    }

    #[test]
    fn a_connections_values_take_their_new_names_and_oauths_keep_theirs() {
        let mut values = json!({ "smtp_host": "h", "access_token": "t", "signing_secret": "s" });
        connection_values(&mut values);
        assert_eq!(values, json!({ "smtpHost": "h", "access_token": "t", "signingSecret": "s" }));
        let mut names = json!(["smtp_host", "refresh_token"]);
        connection_value_names(&mut names);
        assert_eq!(names, json!(["smtpHost", "refresh_token"]));
    }

    const RUN: &str = "e4366595-1cfd-446c-b70a-5f1af995f80a";

    #[test]
    fn a_key_naming_the_execution_names_the_run_in_its_types_own_spelling() {
        assert_eq!(renamed("execution_id", Spelling::Snake), "run_id");
        assert_eq!(renamed("setup_execution_id", Spelling::Snake), "setup_run_id");
        assert_eq!(renamed("execution_id", Spelling::Camel), "runId");
        assert_eq!(renamed("at_unix", Spelling::Camel), "atUnix");
        assert_eq!(renamed("payload_byte_size", Spelling::Camel), "payloadByteSize");
        assert_eq!(renamed("tag", Spelling::Camel), "tag");
    }

    #[test]
    fn execution_keys_are_renamed_everywhere_but_inside_the_users_data() {
        let mut v = json!({
            "execution_id": RUN,
            "spec": { "setup_execution_id": RUN },
            "output": { "execution_id": "mine" },
            "list": [{ "seed_execution_id": RUN }],
        });
        run_keys(&mut v);
        assert_eq!(
            v,
            json!({
                "run_id": RUN,
                "spec": { "setup_run_id": RUN },
                "output": { "execution_id": "mine" },
                "list": [{ "seed_run_id": RUN }],
            })
        );
    }

    #[test]
    fn a_run_scoped_file_key_moves_to_the_run_tag_and_nothing_else_does() {
        assert_eq!(file_key(&format!("t1/exec/{RUN}/f0")), Some(format!("t1/run/{RUN}/f0")));
        assert_eq!(file_key(&format!("exec/{RUN}/f0")), Some(format!("run/{RUN}/f0")));
        assert_eq!(file_key("team/exec/q1/report"), None, "not a run's id");
        assert_eq!(file_key("t1/project/p/f"), None);
        assert_eq!(file_key(&format!("t1/exec/{RUN}")), None);
        let mut v = json!({ "output": { "file": { "key": format!("t1/exec/{RUN}/f0") } } });
        file_keys(&mut v);
        assert_eq!(v["output"]["file"]["key"], format!("t1/run/{RUN}/f0"), "a file value in an output is weft's own reference");
    }

    #[test]
    fn a_cause_and_a_scope_that_named_the_execution_name_the_run() {
        let mut v = json!({
            "cause": { "kind": "execution", "by": RUN, "tag": "t" },
            "scope": { "kind": "execution" },
            "other": { "kind": "execution", "note": "a user's object" },
            "output": { "kind": "execution", "by": "x" },
        });
        execution_kinds(&mut v);
        assert_eq!(v["cause"]["kind"], "run");
        assert_eq!(v["scope"]["kind"], "run");
        assert_eq!(v["other"]["kind"], "execution");
        assert_eq!(v["output"]["kind"], "execution");
    }

    #[test]
    fn a_record_row_takes_the_new_event_names_and_window_fields() {
        let mut v = json!([
            { "kind": "execution_started", "entry_node": "n", "run_kind": "node_test" },
            { "kind": "execution_cancelled", "reason": "r", "cause": { "kind": "execution", "by": RUN, "tag": "t" }, "at_unix": 2 },
            { "kind": "bus_window", "bus_id": "b", "messages": [{ "at_unix": 1, "msg_kind": "data", "payload_byte_size": 3, "payload": { "at_unix": "mine" } }], "totals": [{ "msg_kind": "data", "count": 1 }] },
            { "kind": "node_completed", "node_id": "n", "output": { "f": { "key": format!("t/exec/{RUN}/x") } } },
        ]);
        events(&mut v).unwrap();
        assert_eq!(v[0]["kind"], "run_started");
        assert_eq!(v[0]["run_kind"], "node_test");
        assert_eq!(v[1]["kind"], "run_cancelled");
        assert_eq!(v[1]["cause"]["kind"], "run");
        assert_eq!(v[1]["at_unix"], 2, "a record's own fields keep their snake_case");
        assert_eq!(v[2]["messages"][0], json!({ "atUnix": 1, "msgKind": "data", "payloadByteSize": 3, "payload": { "at_unix": "mine" } }));
        assert_eq!(v[2]["totals"][0], json!({ "msgKind": "data", "count": 1 }));
        assert_eq!(v[3]["output"]["f"]["key"], format!("t/run/{RUN}/x"));
    }

    #[test]
    fn a_run_spec_takes_its_camel_case_names_and_keeps_the_values_it_was_given() {
        let mut v = json!({
            "name": "n",
            "from": { "a": { "frozen_from": 1 } },
            "frozen_from": { "version": "v", "execution_id": RUN, "definition_hash": "d" },
            "answers": [{ "node": "ask", "gave_up": "quiet" }],
            "settings": { "keeping": "durable", "keep_for": "7d", "hold_secs": 0 },
        });
        run_spec(&mut v);
        assert_eq!(
            v,
            json!({
                "name": "n",
                "from": { "a": { "frozen_from": 1 } },
                "frozenFrom": { "version": "v", "runId": RUN, "definitionHash": "d" },
                "answers": [{ "node": "ask", "gaveUp": "quiet" }],
                "settings": { "keeping": "durable", "keepFor": "7d", "holdSecs": 0 },
            })
        );
    }

    #[test]
    fn a_bake_camel_cases_its_fields_and_keeps_its_program_and_captured_values() {
        let mut v = json!({
            "project_id": "p", "source_version": "v1", "execution_id": RUN, "at_unix": 1,
            "program": { "definition_hash": "d", "binary_hash": "b" }, "captured": { "a_b": 1 },
        });
        trigger_bake(&mut v);
        assert_eq!(v["projectId"], "p");
        assert_eq!(v["sourceVersion"], "v1");
        assert_eq!(v["runId"], RUN);
        assert_eq!(v["atUnix"], 1);
        assert_eq!(v["program"], json!({ "definition_hash": "d", "binary_hash": "b" }), "the program identity keeps its names");
        assert_eq!(v["captured"], json!({ "a_b": 1 }));
    }

    #[test]
    fn a_program_call_takes_its_camel_case_filter_and_answer() {
        let mut payload = json!({ "by": RUN, "stop_self": "keep", "call": { "call": "runs_clean", "filter": { "older_than_secs": 5 }, "running": "wait" } });
        let mut result = json!({ "value": { "deleted": 1, "cancelled": 0, "left_running": 2 }, "stops_asker": false });
        task("program_call", &mut payload, Some(&mut result));
        assert_eq!(payload["call"]["filter"], json!({ "olderThanSecs": 5 }));
        assert_eq!(payload["stop_self"], "keep", "the task's own envelope keeps its names");
        assert_eq!(result["value"]["leftRunning"], 2);
        assert_eq!(result["stops_asker"], false);

        let mut payload = json!({ "execution_id": RUN, "node_id": "n" });
        task("fire_signal", &mut payload, None);
        assert_eq!(payload, json!({ "run_id": RUN, "node_id": "n" }));
    }

    #[test]
    fn a_change_of_an_instances_values_names_its_nodes_and_keeps_the_values() {
        let mut payload = json!({ "by": RUN, "stop_self": "keep", "call": {
            "call": "values_change", "instance": "ada",
            "set": [{ "step": "one.read", "field": "account", "value": { "step": "mine" } }],
            "clear": [{ "step": "tick", "field": "cron" }],
        } });
        task("program_call", &mut payload, None);
        assert_eq!(
            payload["call"],
            json!({
                "call": "values_change", "instance": "ada",
                "set": [{ "node": "one.read", "field": "account", "value": { "step": "mine" } }],
                "clear": [{ "node": "tick", "field": "cron" }],
            })
        );
        let _: weft_core::program::ProgramCallPayload = serde_json::from_value(payload).expect("reads with today's types");

        let mut other = json!({ "by": RUN, "stop_self": "keep", "call": { "call": "values_get", "instance": "ada", "step": "x" } });
        task("program_call", &mut other, None);
        assert_eq!(other["call"]["step"], "x", "only a change of values is renamed");
    }
}
