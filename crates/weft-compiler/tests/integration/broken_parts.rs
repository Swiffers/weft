//! Layer-3: a program with parts that do not compile, compiled the way
//! every command compiles it (`weft_compiler::compile_program`), then cut
//! the way a trigger's fire and a setup cut it. The compiler and the
//! standard catalog are real: a broken part stops only the cuts that need
//! it, and a mistake in the entry file stops everything.
use weft_compiler::CompileFs;
use weft_core::frames::Located;
use weft_core::run_spec::{compute_trigger_fire, setup_cut};

/// `tick` feeds a branch that compiles; `other` feeds the site of a file
/// whose group never closes.
const MAIN: &str = "tick = Cron { cron: \"0 0 * * * *\" }
a = Format(p: String) { template: \"{{p}}\", p: tick.scheduledTime }
d = Debug { data: a.text }
other = Cron { cron: \"0 0 * * * *\" }
draft = @include(\"draft.weft\")
draft.text = other.scheduledTime
e = Debug { data: draft.out }
";

const DRAFT: &str = "Group(text: String) -> (out: String) {
  f = Format(t: String) { template: \"{{t}}\", t: self.text
";

fn catalog() -> weft_catalog::FsCatalog {
    weft_catalog::FsCatalog::discover(&weft_catalog::stdlib_root().unwrap()).unwrap()
}

fn compile_in(files: &[(&str, &str)], main: &str) -> Result<weft_core::ProjectDefinition, Vec<weft_compiler::Diagnostic>> {
    let dir = tempfile::tempdir().unwrap();
    for (rel, body) in files {
        let path = dir.path().join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }
    weft_compiler::compile_program(main, uuid::Uuid::new_v4(), CompileFs::disk(dir.path()), &catalog())
}

/// An included file that does not parse is a placeholder at its site, its
/// errors pinned to it: the program compiles, a fire that never reaches the
/// site runs, and one that does is refused naming the file and the line.
#[test]
fn a_broken_include_stops_only_the_fires_that_reach_it() {
    let program = compile_in(&[("draft.weft", DRAFT)], MAIN).expect("the program around the file compiles");
    let placeholder = program.nodes.iter().find(|node| node.id == "draft").expect("a placeholder at the site");
    assert_eq!(placeholder.include_path.as_deref(), Some("draft.weft"));
    let own = program.broken.iter().find(|part| part.parts == ["draft"]).expect("the file's errors, pinned to its site");
    assert!(own.why.starts_with("draft.weft:"), "the error names the file from the project root, and its line: {}", own.why);
    assert!(program.broken.iter().all(|part| part.parts.iter().any(|id| id == "draft")), "every broken part involves the site: {:?}", program.broken);
    assert!(own.hides && program.hides_nodes(), "the file's nodes are behind the placeholder");

    let fine = compute_trigger_fire(&program, "tick", &serde_json::Value::Null, None, &Default::default()).expect("tick never reaches the file");
    assert!(fine.program.broken.is_empty());
    setup_cut(&program, &[Located::top("tick")]).expect("its setup neither");

    let refused = compute_trigger_fire(&program, "other", &serde_json::Value::Null, None, &Default::default()).unwrap_err();
    assert!(refused.contains("a fire of 'other' cannot run") && refused.contains("draft.weft:"), "{refused}");
}

/// A file included from another included file is a placeholder inside that
/// file's body: the body still compiles, and its own nodes still run. The
/// first site hands the body only `text`, which never reaches the broken
/// file; the second hands it `extra` too, which does.
#[test]
fn a_broken_include_inside_an_included_file_leaves_the_rest_of_that_file() {
    const OUTER: &str = "Group(text: String, extra?: String) -> (out: String, raw: String) {
  keep = Format(t: String) { template: \"{{t}}\", t: self.text }
  inner = @include(\"draft.weft\")
  inner.text = self.extra
  self.out = inner.out
  self.raw = keep.text
}
";
    let main = "tick = Cron { cron: \"0 0 * * * *\" }
wrap = @include(\"lib/outer.weft\")
wrap.text = tick.scheduledTime
d = Debug { data: wrap.raw }
other = Cron { cron: \"0 0 * * * *\" }
wrap2 = @include(\"lib/outer.weft\")
wrap2.text = other.scheduledTime
wrap2.extra = other.scheduledTime
e = Debug { data: wrap2.out }
";
    let program = compile_in(&[("lib/outer.weft", OUTER), ("lib/draft.weft", DRAFT)], main).expect("compiles");
    let placeholder = program.nodes.iter().find(|node| node.include_path.is_some()).expect("a placeholder in the body");
    assert!(placeholder.id.ends_with(".inner"), "{}", placeholder.id);
    assert!(program.broken.iter().any(|part| part.parts == [placeholder.id.clone()] && part.why.contains("draft.weft:")), "{:?}", program.broken);
    compute_trigger_fire(&program, "tick", &serde_json::Value::Null, None, &Default::default()).expect("wrap.keep runs");
    let refused = compute_trigger_fire(&program, "other", &serde_json::Value::Null, None, &Default::default()).unwrap_err();
    assert!(refused.contains("draft.weft:"), "{refused}");
}

/// A node whose type the catalog does not serve is broken where it stands.
#[test]
fn a_node_of_an_unknown_type_is_a_broken_part() {
    let main = "tick = Cron { cron: \"0 0 * * * *\" }
d = Debug { data: tick.scheduledTime }
z = NoSuchNode
";
    let program = compile_in(&[], main).expect("compiles");
    let part = program.broken.iter().find(|part| part.parts == ["z"]).expect("z is broken");
    assert!(part.why.contains("NoSuchNode"), "{}", part.why);
    assert!(part.hides, "nothing says whether z is a trigger or infra");
    compute_trigger_fire(&program, "tick", &serde_json::Value::Null, None, &Default::default()).expect("z is no business of tick's");
    assert!(setup_cut(&program, &[Located::top("z")]).unwrap_err().contains("NoSuchNode"));
}

/// A wire between two ports that cannot meet breaks its two ends and hides
/// nothing: the program still says every node it has.
#[test]
fn a_wire_that_cannot_carry_its_value_hides_no_node() {
    let main = "tick = Cron { cron: \"0 0 * * * *\" }
n = ExecPython() -> (out: Number) { code: \"x\" }
f = Format(p: String) { template: \"{{p}}\", p: n.out }
";
    let program = compile_in(&[], main).expect("compiles");
    assert!(!program.broken.is_empty(), "the Number into a String is broken");
    assert!(!program.hides_nodes(), "{:?}", program.broken);
}

/// A wire whose value cannot enter a plain group's port breaks a cut that
/// runs it even when that cut keeps nothing of the group as a scope (its
/// member is fed, nothing waits on the group): the member and the wire
/// through the port still run.
#[test]
fn a_broken_wire_through_a_dissolved_group_is_still_refused() {
    let main = "n = ExecPython() -> (out: String) { code: \"x\" }
g = Group(x: Number) -> (y: String) {
  f = Format(p: Number) { template: \"{{p}}\", p: self.x }
  self.y = f.text
}
g.x = n.out
d = Debug { data: g.y }
";
    let program = compile_in(&[], main).expect("compiles");
    assert!(program.broken.iter().any(|part| part.parts.iter().any(|id| id == "g")), "{:?}", program.broken);
    let refused = setup_cut(&program, &[Located::top("d")]).unwrap_err();
    assert!(refused.contains("do not compile"), "{refused}");
}

/// The entry file not parsing is the program as a whole: nothing compiles.
#[test]
fn an_entry_file_that_does_not_parse_stops_everything() {
    let errors = compile_in(&[], "tick = Cron { cron: \"0 0 * * * *\"\nd = = Debug\n").unwrap_err();
    assert!(errors.iter().any(|d| matches!(d.severity, weft_compiler::Severity::Error) && d.subjects.is_empty()), "{errors:?}");
}

/// A trigger inside a loop is an error about that trigger alone, and a cut
/// that names it as an end is refused with the compiler's own reason (it
/// cannot live in a loop body), never told to select the whole loop: the
/// node's own errors come before any rule about where an end may sit.
#[test]
fn an_end_that_does_not_compile_is_refused_with_its_own_reason() {
    let main = "scope = Loop(values: List[Number]) -> (out: List[String | Null]) {
  over: [\"values\"]
  tick = Cron { cron: \"0 0 * * * *\" }
  self.out = tick.scheduledTime
}
scope.values = [1]
";
    let program = compile_in(&[], main).expect("the program around the trigger compiles");
    assert!(program.broken.iter().any(|part| part.parts == ["scope.tick"] && part.why.contains("inside a Loop")), "{:?}", program.broken);
    for refused in [
        setup_cut(&program, &[Located::top("scope.tick")]).unwrap_err(),
        compute_trigger_fire(&program, "scope.tick", &serde_json::Value::Null, None, &Default::default()).unwrap_err(),
    ] {
        assert!(refused.contains("do not compile") && refused.contains("inside a Loop"), "{refused}");
        assert!(!refused.contains("select the whole loop"), "{refused}");
    }
}
