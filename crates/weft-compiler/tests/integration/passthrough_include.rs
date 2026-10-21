//! Layer-3: an included file whose output only passes its input on
//! (`self.y = self.x`, no node inside), used once and used twice in a
//! chain. The compiler is real; the program index is what a run reads.
use crate::rig::compile;
use weft_compiler::CompileFs;
use weft_core::frames::Located;
use weft_core::project::graph::ProjectGraph;
use weft_core::project::places;
use weft_core::project::ProgramIndex;

const PASS: &str = r#"
Group(x: String) -> (y: String) {
  self.y = self.x
}
"#;

fn compiled(main: &str) -> weft_core::ProjectDefinition {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("lib")).unwrap();
    std::fs::write(dir.path().join("lib/pass.weft"), PASS).unwrap();
    compile(main, uuid::Uuid::new_v4(), CompileFs::disk(dir.path())).expect("compile")
}

/// The wire from `src` to `out` is taken at the top, by the run and by
/// the walks over places alike: it enters each site and leaves it again.
fn reaches_out(project: &weft_core::ProjectDefinition) {
    let wire = project.edges.iter().find(|edge| edge.source == "src" && edge.target == "out")
        .unwrap_or_else(|| panic!("one wire from src to out, through the file: {:#?}", project.edges));
    assert_eq!(ProgramIndex::build(project).frames_across(wire, &Vec::new()), Some(Vec::new()));
    let reached: Vec<Located> = places::outgoing(&ProjectGraph::new(project), &Located::new("src", Vec::new()))
        .map(|(_, target)| target).collect();
    assert_eq!(reached, vec![Located::new("out", Vec::new())]);
}

#[test]
fn a_file_that_passes_its_input_on_carries_it_through_one_call() {
    reaches_out(&compiled(r#"
src = Text { value: "hi" }
one = @include("lib/pass.weft")
one.x = src.value
out = Debug
out.data = one.y
"#));
}

/// The same file twice in a chain passes the same port of it under two
/// calls: two places, no loop.
#[test]
fn a_file_that_passes_its_input_on_chains_through_two_calls() {
    reaches_out(&compiled(r#"
src = Text { value: "hi" }
one = @include("lib/pass.weft")
one.x = src.value
two = @include("lib/pass.weft")
two.x = one.y
out = Debug
out.data = two.y
"#));
}
