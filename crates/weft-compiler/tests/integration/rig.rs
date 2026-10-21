//! The asserting shapes these tests read the production pipeline through:
//! a source that must lower with no error, and a program that must enrich
//! with no failure. Each is the production entry point
//! (`weft_compiler::compile_lenient`, `enrich::enrich_collecting`) with
//! its findings turned into an `Err`, so a test states what it expects
//! without restating the collection.

use uuid::Uuid;
use weft_compiler::enrich::{enrich_collecting, EnrichPolicy};
use weft_compiler::weft_compiler::{compile_lenient, IncludeMode, SourceError};
use weft_compiler::CompileFs;
use weft_core::node::MetadataCatalog;
use weft_core::ProjectDefinition;

/// The entry file `source`, every `@include` inlined, or every error it
/// lowered with.
pub fn compile(source: &str, project_id: Uuid, fs: CompileFs) -> Result<ProjectDefinition, Vec<SourceError>> {
    compile_with_mode(source, project_id, fs, IncludeMode::Full, None)
}

/// [`compile`] with the include mode and the file identity the test
/// names (`source_name`, see `compile_lenient`).
pub fn compile_with_mode(
    source: &str,
    project_id: Uuid,
    fs: CompileFs,
    include_mode: IncludeMode,
    source_name: Option<&str>,
) -> Result<ProjectDefinition, Vec<SourceError>> {
    let (project, errors) = compile_lenient(source, project_id, fs, include_mode, source_name);
    if errors.is_empty() { Ok(project) } else { Err(errors) }
}

/// Strict enrichment of `project`, or its failures as one line each
/// joined by `; `.
pub fn enrich(project: &mut ProjectDefinition, catalog: &dyn MetadataCatalog) -> Result<(), String> {
    let errors = enrich_collecting(project, catalog, EnrichPolicy::Strict);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.into_iter().map(|e| e.message).collect::<Vec<_>>().join("; "))
    }
}
