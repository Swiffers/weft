//! How a JSON document weft keeps (a row's column, a saved file) is carried
//! from the shape an older weft wrote to the shape the code reads today.
//!
//! A stored document's type has a version: 1 is the shape it had when it
//! was first versioned, and every change to the shape since is one step,
//! version N to N+1, kept next to the type. A step works on the raw JSON,
//! because the Rust type of the older shape no longer exists. Most changes
//! are a few [`Op`]s (a key renamed, a key added with a value, a key
//! dropped, a tag value renamed); one that those cannot say is a
//! [`Op::Code`] function over the JSON at a path.
//!
//! Every document carries the version it was written at: a row in the
//! integer column beside its own (`<column>_version`), a saved spec file
//! under its own key. Whoever reads one hands both to its [`Kind`]'s
//! [`Kind::decode`], the one read of a stored document: at today's
//! version it is a plain parse, an older one is carried through the steps
//! it has not taken first, and a newer one is refused. Every way it fails
//! is one [`DecodeError`], which the reader tells where the document sat
//! ([`DecodeError::at`]). The boot also rewrites every older row in place
//! (`weft_task_store::documents`), so the readers almost never carry one.
//!
//! An update runs with every part of weft switched off, so no older weft
//! writes beside this one: what an older weft wrote is only ever what it
//! left behind.

use std::marker::PhantomData;

use serde_json::Value;

/// One change to a stored document. Every op acts at a path, `at`: dotted
/// keys from the document's root, where `*` stands for every item of an
/// array or every value of an object, and `""` is the root itself. A path
/// that leads nowhere (an optional field the document leaves out) does
/// nothing: a step changes what is there and leaves the rest alone.
#[derive(Debug, Clone, Copy)]
pub enum Op {
    /// In every object at `at`, the key `from` is now spelled `to`.
    RenameKey { at: &'static str, from: &'static str, to: &'static str },
    /// Every object at `at` gains the key `key`, holding `value` (JSON
    /// text). For a new field with no serde default.
    AddKey { at: &'static str, key: &'static str, value: &'static str },
    /// Every object at `at` loses the key `key`.
    DropKey { at: &'static str, key: &'static str },
    /// In every object at `at` whose `tag` key holds the string `from`, it
    /// holds `to` instead: a renamed variant of a tagged enum.
    RenameTag { at: &'static str, tag: &'static str, from: &'static str, to: &'static str },
    /// `ops`, with each value at `at` as their root: a type stored inside
    /// another applies its own steps where it sits.
    Within { at: &'static str, ops: &'static [Op] },
    /// Rust code, for a change the ops above cannot say: called with each
    /// value at `at`, it rewrites the value in place or refuses it.
    Code { at: &'static str, run: fn(&mut Value) -> Result<(), String> },
}

/// Apply `ops` to `doc`, in order. An error names the op's path and why.
pub fn apply(ops: &[Op], doc: &mut Value) -> Result<(), String> {
    for op in ops {
        match *op {
            Op::RenameKey { at, from, to } => each_object(doc, at, &mut |obj| {
                if let Some(value) = obj.remove(from) {
                    if obj.contains_key(to) {
                        return Err(format!("'{at}' holds both '{from}' and '{to}', so '{from}' cannot become '{to}'"));
                    }
                    obj.insert(to.to_string(), value);
                }
                Ok(())
            })?,
            Op::AddKey { at, key, value } => {
                let value: Value = serde_json::from_str(value)
                    .map_err(|e| format!("the value added as '{at}.{key}' is not JSON ({e})"))?;
                each_object(doc, at, &mut |obj| {
                    if obj.contains_key(key) {
                        return Err(format!("'{at}' already holds '{key}'"));
                    }
                    obj.insert(key.to_string(), value.clone());
                    Ok(())
                })?
            }
            Op::DropKey { at, key } => each_object(doc, at, &mut |obj| {
                obj.remove(key);
                Ok(())
            })?,
            Op::RenameTag { at, tag, from, to } => each_object(doc, at, &mut |obj| {
                if obj.get(tag).and_then(Value::as_str) == Some(from) {
                    obj.insert(tag.to_string(), Value::String(to.to_string()));
                }
                Ok(())
            })?,
            Op::Within { at, ops } => each_value(doc, &segments(at), &mut |value| apply(ops, value))
                .map_err(|e| format!("in '{at}': {e}"))?,
            Op::Code { at, run } => {
                each_value(doc, &segments(at), &mut |value| run(value)).map_err(|e| format!("at '{at}': {e}"))?
            }
        }
    }
    Ok(())
}

/// The version of a stored document's shape, as a row's version column
/// (an `INTEGER`) and a saved file hold it.
pub type Version = i32;

/// The version of a document written before documents carried one: what
/// the rows already in a table hold the day its version column is added
/// (`weft_task_store::documents::Document::backfill`), and what a message
/// from a weft of that release, which names none, means.
pub const fn unversioned() -> Version {
    1
}

/// One kind of document weft stores: the type it reads as, what messages
/// call it, and the steps that carry an older weft's JSON of it to today's
/// shape. Declared once next to its type, and shared by every column (and
/// file) that holds it.
pub struct Kind<T> {
    /// What a message calls one ("this {name} was written by a newer
    /// weft"): the noun a reader knows it by, where the column it sits in
    /// (`table.column`) says where.
    pub name: &'static str,
    /// `steps[0]` takes version 1 to 2, so today's version is
    /// `steps.len() + 1`.
    pub steps: &'static [&'static [Op]],
    of: PhantomData<fn() -> T>,
}

impl<T> Kind<T> {
    pub const fn new(name: &'static str, steps: &'static [&'static [Op]]) -> Self {
        Kind { name, steps, of: PhantomData }
    }

    /// The version this weft writes, and reads without a step.
    pub const fn version(&self) -> Version {
        current_version(self.steps)
    }

    fn error(&self, version: Version, why: Why) -> DecodeError {
        DecodeError { kind: self.name, version, current: self.version(), at: None, why }
    }
}

impl<T: serde::de::DeserializeOwned> Kind<T> {
    /// A stored document, written at `version`, read as today's `T`. At
    /// today's version it is parsed and nothing else; an older one is
    /// carried through the steps it has not taken first; a newer one, or
    /// one at no version a weft writes, is refused naming both versions.
    pub fn decode(&self, json: impl Raw, version: Version) -> Result<T, DecodeError> {
        let current = self.version();
        if version == current {
            return json.parse().map_err(|e| self.error(version, Why::DoesNotRead(e)));
        }
        if version > current {
            return Err(self.error(version, Why::Newer));
        }
        let mut value = json.value().map_err(|e| self.error(version, Why::NotJson(e)))?;
        upgrade(self.steps, version, &mut value).map_err(|e| self.error(version, Why::DoesNotCarry(e)))?;
        serde_json::from_value(value).map_err(|e| self.error(version, Why::DoesNotRead(e)))
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> Kind<T> {
    /// A stored document's text in today's shape, for a reader that hands
    /// the text on rather than reading it: at today's version as it is (not
    /// even parsed), else read through [`Kind::decode`] and written again.
    pub fn in_todays_shape(&self, text: String, version: Version) -> Result<String, DecodeError> {
        if version == self.version() {
            return Ok(text);
        }
        let doc = self.decode(text.as_str(), version)?;
        serde_json::to_string(&doc).map_err(|e| self.error(version, Why::DoesNotWrite(e)))
    }
}

/// Why a stored document did not read ([`Kind::decode`]): which kind it
/// is, the version it was written at and the one this weft reads, where it
/// sat once the reader says ([`DecodeError::at`]), and what went wrong.
#[derive(Debug)]
pub struct DecodeError {
    pub kind: &'static str,
    pub version: Version,
    pub current: Version,
    /// Where the document sat ("run 1234's spec"), set by its reader.
    pub at: Option<String>,
    pub why: Why,
}

/// What went wrong reading a stored document.
#[derive(Debug)]
pub enum Why {
    /// A newer weft wrote it.
    Newer,
    /// Its text is not JSON.
    NotJson(serde_json::Error),
    /// The steps from its version refused it.
    DoesNotCarry(String),
    /// At today's shape (as written, or once carried), today's type does
    /// not read it.
    DoesNotRead(serde_json::Error),
    /// Read, today's type did not write it back.
    DoesNotWrite(serde_json::Error),
}

impl DecodeError {
    /// The same error, saying where the document sat: what a reader adds
    /// once, so every message names its row.
    pub fn at(mut self, at: impl std::fmt::Display) -> Self {
        self.at = Some(at.to_string());
        self
    }
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let DecodeError { kind, version, current, at, why } = self;
        if let Some(at) = at {
            write!(f, "{at}: ")?;
        }
        match why {
            Why::Newer => write!(
                f,
                "this {kind} was written by a newer weft: it is at version {version}, and this weft reads up to \
                 version {current}. Update weft to read it (on a local install, ./setup.sh from the checkout \
                 that has it)."
            ),
            Why::NotJson(e) => write!(f, "this {kind} is not JSON: {e}"),
            Why::DoesNotCarry(e) => {
                write!(f, "this {kind} was written by an older weft, and does not carry to today's shape: {e}")
            }
            Why::DoesNotRead(e) if version == current => write!(
                f,
                "this {kind} does not read at version {version}, today's: {e}. If an older weft is still \
                 running beside this one (a replica not yet replaced during an update), it may have rewritten \
                 it in place in its own shape; that write is refused here rather than read wrong, and stops \
                 once every replica runs this weft."
            ),
            Why::DoesNotRead(e) => write!(f, "this {kind}, carried from version {version}, does not read: {e}"),
            Why::DoesNotWrite(e) => write!(f, "this {kind} does not write: {e}"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.why {
            Why::NotJson(e) | Why::DoesNotRead(e) | Why::DoesNotWrite(e) => Some(e),
            Why::Newer | Why::DoesNotCarry(_) => None,
        }
    }
}

/// JSON as a column or a file hands it over: text, bytes, or a value
/// already parsed (a `JSONB` column read as one).
pub trait Raw {
    fn parse<T: serde::de::DeserializeOwned>(self) -> serde_json::Result<T>;
    fn value(self) -> serde_json::Result<Value>;
}

impl Raw for &str {
    fn parse<T: serde::de::DeserializeOwned>(self) -> serde_json::Result<T> {
        serde_json::from_str(self)
    }
    fn value(self) -> serde_json::Result<Value> {
        serde_json::from_str(self)
    }
}

impl Raw for &[u8] {
    fn parse<T: serde::de::DeserializeOwned>(self) -> serde_json::Result<T> {
        serde_json::from_slice(self)
    }
    fn value(self) -> serde_json::Result<Value> {
        serde_json::from_slice(self)
    }
}

impl Raw for Value {
    fn parse<T: serde::de::DeserializeOwned>(self) -> serde_json::Result<T> {
        serde_json::from_value(self)
    }
    fn value(self) -> serde_json::Result<Value> {
        Ok(self)
    }
}

/// Carry `doc`, written at version `from`, through `steps` to the current
/// version. `steps[0]` takes version 1 to 2, so the current version is
/// `steps.len() + 1`.
pub fn upgrade(steps: &[&[Op]], from: Version, doc: &mut Value) -> Result<(), String> {
    let current = current_version(steps);
    if from < 1 || from > current {
        return Err(format!("version {from} is not one this weft knows (it knows 1 to {current})"));
    }
    for (at, ops) in steps.iter().enumerate().skip(from as usize - 1) {
        apply(ops, doc).map_err(|e| format!("version {} to {}: {e}", at + 1, at + 2))?;
    }
    Ok(())
}

/// The version a document is at once every step in `steps` has run.
pub const fn current_version(steps: &[&[Op]]) -> Version {
    steps.len() as Version + 1
}

/// `value` read as today's `T` and written back: what the code would store
/// for it. A document that does not read is the error.
pub fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned>(value: Value) -> Result<Value, String> {
    let typed: T = serde_json::from_value(value).map_err(|e| e.to_string())?;
    serde_json::to_value(&typed).map_err(|e| e.to_string())
}

fn segments(at: &str) -> Vec<&str> {
    if at.is_empty() {
        Vec::new()
    } else {
        at.split('.').collect()
    }
}

fn each_value(value: &mut Value, path: &[&str], f: &mut dyn FnMut(&mut Value) -> Result<(), String>) -> Result<(), String> {
    let Some((head, rest)) = path.split_first() else {
        return f(value);
    };
    match (*head, value) {
        ("*", Value::Array(items)) => items.iter_mut().try_for_each(|item| each_value(item, rest, f)),
        ("*", Value::Object(obj)) => obj.values_mut().try_for_each(|item| each_value(item, rest, f)),
        (key, Value::Object(obj)) => match obj.get_mut(key) {
            Some(child) => each_value(child, rest, f),
            None => Ok(()),
        },
        _ => Ok(()),
    }
}

fn each_object(
    doc: &mut Value,
    at: &str,
    f: &mut impl FnMut(&mut serde_json::Map<String, Value>) -> Result<(), String>,
) -> Result<(), String> {
    each_value(doc, &segments(at), &mut |value| match value {
        Value::Object(obj) => f(obj),
        Value::Null => Ok(()),
        other => Err(format!("'{at}' should be an object, and is {other}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ops_act_at_their_path_through_lists_and_maps() {
        let mut doc = json!({
            "a": [{ "old": 1 }, { "old": 2, "kind": "x" }],
            "m": { "t1": { "kind": "x" }, "t2": { "kind": "y" } },
        });
        apply(
            &[
                Op::RenameKey { at: "a.*", from: "old", to: "new" },
                Op::RenameTag { at: "m.*", tag: "kind", from: "x", to: "z" },
                Op::AddKey { at: "m.t2", key: "flag", value: "true" },
                Op::DropKey { at: "a.*", key: "kind" },
                Op::Within { at: "m", ops: &[Op::DropKey { at: "t1", key: "kind" }] },
            ],
            &mut doc,
        )
        .unwrap();
        assert_eq!(
            doc,
            json!({
                "a": [{ "new": 1 }, { "new": 2 }],
                "m": { "t1": {}, "t2": { "kind": "y", "flag": true } },
            })
        );
    }

    /// A path that leads nowhere is a document without that part: nothing
    /// to change.
    #[test]
    fn a_missing_path_changes_nothing() {
        let mut doc = json!({ "a": 1 });
        apply(&[Op::RenameKey { at: "b.*.c", from: "x", to: "y" }], &mut doc).unwrap();
        assert_eq!(doc, json!({ "a": 1 }));
    }

    #[test]
    fn a_rename_onto_a_key_already_there_is_refused() {
        let mut doc = json!({ "x": 1, "y": 2 });
        let err = apply(&[Op::RenameKey { at: "", from: "x", to: "y" }], &mut doc).unwrap_err();
        assert!(err.contains("both 'x' and 'y'"), "{err}");
    }

    #[test]
    fn upgrade_runs_the_steps_after_the_version_given() {
        const STEPS: &[&[Op]] = &[
            &[Op::RenameKey { at: "", from: "a", to: "b" }],
            &[Op::RenameKey { at: "", from: "b", to: "c" }],
        ];
        assert_eq!(current_version(STEPS), 3);
        let mut from_one = json!({ "a": 1 });
        upgrade(STEPS, 1, &mut from_one).unwrap();
        assert_eq!(from_one, json!({ "c": 1 }));
        let mut from_two = json!({ "b": 1 });
        upgrade(STEPS, 2, &mut from_two).unwrap();
        assert_eq!(from_two, json!({ "c": 1 }));
        let mut current = json!({ "c": 1 });
        upgrade(STEPS, 3, &mut current).unwrap();
        assert_eq!(current, json!({ "c": 1 }));
        assert!(upgrade(STEPS, 4, &mut current).unwrap_err().contains("knows 1 to 3"));
    }

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Today {
        c: u32,
    }

    const TODAY: Kind<Today> = Kind::new("probe", &[
        &[Op::RenameKey { at: "", from: "a", to: "b" }],
        &[Op::RenameKey { at: "", from: "b", to: "c" }],
    ]);

    /// A document reads at the version it was written at, from text, bytes
    /// or a value: today's as it is, an older one carried first, a newer
    /// one refused naming both versions.
    #[test]
    fn decode_reads_a_document_at_the_version_it_was_written_at() {
        assert_eq!(TODAY.version(), 3);
        assert_eq!(TODAY.decode(r#"{"c": 1}"#, 3).unwrap(), Today { c: 1 });
        assert_eq!(TODAY.decode(&br#"{"a": 2}"#[..], 1).unwrap(), Today { c: 2 });
        assert_eq!(TODAY.decode(json!({ "b": 3 }), 2).unwrap(), Today { c: 3 });
        let newer = TODAY.decode(r#"{"d": 1}"#, 4).unwrap_err().to_string();
        assert!(newer.contains("probe was written by a newer weft") && newer.contains("version 4") && newer.contains("up to version 3"), "{newer}");
        assert!(TODAY.decode(r#"{"c": 1}"#, 0).unwrap_err().to_string().contains("knows 1 to 3"));
        // Today's version is a plain parse: no step runs on it, and a
        // document that does not read there names where it sat and that
        // an older weft may have written it in place.
        let today = TODAY.decode(r#"{"a": 1}"#, 3).unwrap_err().at("probe row 7").to_string();
        assert!(today.starts_with("probe row 7: this probe does not read at version 3") && today.contains("older weft"), "{today}");
    }
}
