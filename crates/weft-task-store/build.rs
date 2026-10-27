//! Turn this crate's `migrations/` directory into a table it can read.
//!
//! A migration is a file. Dropping one in `migrations/<group>/` is the whole
//! act of writing it: nothing lists it, nothing declares it, and no Rust
//! changes. This script walks the directory at build time and embeds what it
//! finds, so a forgotten registration cannot happen. The walk itself is
//! `src/migration_files.rs`, the one reader of the layout, which the
//! migration generator also reads the disk through.
use std::fmt::Write;

// Every item is used by the crate's generator; this script needs the walk
// and the fields it embeds.
#[allow(dead_code)]
#[path = "src/migration_files.rs"]
mod migration_files;

fn main() {
    // Only the `migrate` feature reads the history. A worker builds without
    // it and from a copy of this crate that has no `migrations/` at all
    // (`[package.metadata.weft]` in Cargo.toml), so a migration never names
    // a new worker. Without this line cargo would rerun the script on any
    // file of the crate changing.
    if std::env::var_os("CARGO_FEATURE_MIGRATE").is_none() {
        println!("cargo:rerun-if-changed=build.rs");
        return;
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    println!("cargo:rerun-if-changed={}", root.display());

    let groups = migration_files::read_groups(&root).unwrap_or_else(|e| panic!("{e}"));
    let mut origins = String::from("static ORIGINS: &[(&str, &str)] = &[\n");
    let mut out = String::from("pub static MIGRATIONS: &[Migration<'static>] = &[\n");
    for g in groups {
        let group = &g.group;
        println!("cargo:rerun-if-changed={}", g.dir.display());
        // Drafts are gitignored and exist only on the machine that wrote
        // them: they keep a work-in-progress database in step with a shape
        // still being decided, and get collapsed into one released file
        // when it is.
        if let Some(drafts) = &g.drafts {
            println!("cargo:rerun-if-changed={}", drafts.display());
        }
        if let Some(origin) = &g.origin {
            writeln!(origins, "    ({group:?}, include_str!({:?})),", origin.display().to_string())
                .expect("write to a String");
        }
        for file in g.files {
            let part = if file.contract { "Part::Contract" } else { "Part::Expand" };
            writeln!(
                out,
                "    Migration {{ group: {group:?}, id: {:?}, part: {part}, draft: {}, \
                 sql: include_str!({:?}) }},",
                file.id,
                file.draft,
                file.path.display().to_string()
            )
            .expect("write to a String");
        }
    }
    out.push_str("];\n");
    origins.push_str("];\n");
    out.push_str(&origins);

    let dest = std::path::Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR")).join("migrations.rs");
    std::fs::write(&dest, out).unwrap_or_else(|e| panic!("write {}: {e}", dest.display()));
}
