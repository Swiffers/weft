//! Where a worker writes the files it keeps for itself (its CPU profile,
//! `crate::profile`): the folder [`SCRATCH_DIR_ENV`] names, or the system's
//! temporary folder when it is unset. A container has that folder to
//! itself; workers running as plain processes on one machine would share
//! it, so the local runner gives each its own.

use std::path::PathBuf;

/// The variable naming the worker's own scratch folder.
// SYNC: SCRATCH_DIR_ENV <-> crates/weft-platform-local/src/runner.rs (worker_env)
pub const SCRATCH_DIR_ENV: &str = "WEFT_SCRATCH_DIR";

/// The worker's scratch folder. A [`SCRATCH_DIR_ENV`] set to nothing is
/// refused, rather than read as the folder the worker runs in.
pub fn dir() -> anyhow::Result<PathBuf> {
    from(std::env::var_os(SCRATCH_DIR_ENV))
}

fn from(set: Option<std::ffi::OsString>) -> anyhow::Result<PathBuf> {
    match set {
        None => Ok(std::env::temp_dir()),
        Some(set) if set.is_empty() => anyhow::bail!("{SCRATCH_DIR_ENV} is set to nothing; name a folder or unset it"),
        Some(set) => Ok(PathBuf::from(set)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_scratch_folder_is_the_one_named_else_the_system_s() {
        assert_eq!(from(Some("/work/scratch".into())).unwrap(), PathBuf::from("/work/scratch"));
        assert_eq!(from(None).unwrap(), std::env::temp_dir());
        assert!(from(Some("".into())).is_err());
    }
}
