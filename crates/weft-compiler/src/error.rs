use thiserror::Error;

/// A step of building a project that failed as a whole: the manifest does
/// not load, the program does not validate, the catalog or an image
/// context cannot be staged. One rendered message, for a caller that only
/// needs to say why the build stopped. An error at a place in the source is
/// a [`crate::weft_compiler::SourceError`] instead, which lowering collects
/// and keeps going.
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("project load error: {0}")]
    Project(String),
    #[error("enrichment error: {0}")]
    Enrich(String),
    #[error("validation error: {0}")]
    Validate(String),
    #[error("build error: {0}")]
    Build(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("other: {0}")]
    Other(#[from] anyhow::Error),
}

pub type BuildResult<T> = Result<T, BuildError>;
