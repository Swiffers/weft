//! Every name a catalog node chooses is spelled the way weft spells it
//! (`weft_core::casing`): the node type in PascalCase; its ports, its
//! settings, the fields of its record types, the values its connection
//! stores and every key of its `metadata.json` in camelCase.
//!
//! The checks themselves live in the library (`src/spelling.rs`, with
//! every name it skips and why): discovery runs them on every node it
//! loads, a project's own included, and reports each name that is off as
//! a catalog warning. The standard library is held to having none. One
//! failure lists every name that is off, each with the node, the file
//! and the spelling that fits, so a rename is one pass instead of one per
//! run.

use weft_catalog::{stdlib_root, FsCatalog, WarningKind};

#[test]
fn every_name_a_catalog_node_chooses_is_spelled_the_weft_way() {
    let root = stdlib_root().expect("stdlib root");
    let catalog = FsCatalog::discover(&root).expect("stdlib catalog");
    let mut lines: Vec<&str> = catalog
        .warnings()
        .iter()
        .filter(|w| w.kind == WarningKind::Spelling)
        .map(|w| w.message.as_str())
        .collect();
    lines.sort();
    assert!(
        lines.is_empty(),
        "{} name(s) in the catalog are not spelled the weft way:\n  {}",
        lines.len(),
        lines.join("\n  ")
    );
}
