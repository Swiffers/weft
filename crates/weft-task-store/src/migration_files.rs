//! How the schema's history sits on disk: one directory per group under
//! `migrations/`, holding the group's frozen `origin.sql`, its released
//! migration files, and (gitignored, this machine's only) its drafts under
//! `drafts/`. A file's name is its id, and says which part of a change it
//! holds and whether it is a draft.
//!
//! The ONE reader of that layout. The build script includes this file
//! (`#[path]`) to embed what it finds into the runtime, and the migration
//! generator reads the same walk straight from disk
//! (`schema_guard::HistoryOnDisk`), so the two can never disagree on what
//! counts as a migration. Plain `std` only, since a build script has none
//! of the crate's dependencies.

use std::path::{Path, PathBuf};

/// What ends a contract part's file name before `.sql`, after its
/// change's id: `20261023T120000_project_resync_drive.contract.sql` beside
/// the change's `20261023T120000_project_resync_drive.sql`.
pub const CONTRACT_SUFFIX: &str = ".contract";

/// What a draft's id starts with. Drafts sort after every released file,
/// so they run last.
pub const DRAFT_PREFIX: &str = "draft_";

/// The directory under a group's that holds its drafts.
pub const DRAFTS_DIR: &str = "drafts";

/// The file stem of a group's frozen origin, never a migration's id.
pub const ORIGIN_STEM: &str = "origin";

/// One group's directory, as the layout reads it.
pub struct GroupFiles {
    pub group: String,
    pub dir: PathBuf,
    /// `drafts/` under the group's directory, when there is one.
    pub drafts: Option<PathBuf>,
    /// `origin.sql`, once the group has frozen one.
    pub origin: Option<PathBuf>,
    /// Every migration file, released and draft, in id order.
    pub files: Vec<MigrationFile>,
}

/// One migration file.
pub struct MigrationFile {
    /// The file name without `.sql`.
    pub id: String,
    pub contract: bool,
    pub draft: bool,
    pub path: PathBuf,
}

/// Every group directory under `root`, in name order. Refuses two files
/// in one group sharing an id (a released file and a same-stem draft):
/// the applied-ids table keys on (group, id), so they would race each
/// other at boot.
pub fn read_groups(root: &Path) -> Result<Vec<GroupFiles>, String> {
    let mut dirs = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|e| format!("read {}: {e}", root.display()))? {
        let path = entry.map_err(|e| format!("read an entry of {}: {e}", root.display()))?.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }
    dirs.sort();
    dirs.into_iter().map(read_group).collect()
}

fn read_group(dir: PathBuf) -> Result<GroupFiles, String> {
    let group = dir
        .file_name()
        .ok_or_else(|| format!("{} has no name", dir.display()))?
        .to_string_lossy()
        .to_string();
    let drafts = Some(dir.join(DRAFTS_DIR)).filter(|d| d.is_dir());
    let mut files = Vec::new();
    for d in std::iter::once(&dir).chain(drafts.as_ref()) {
        for entry in std::fs::read_dir(d).map_err(|e| format!("read {}: {e}", d.display()))? {
            let path = entry.map_err(|e| format!("read a migration file in {}: {e}", d.display()))?.path();
            let Some(id) = path.file_stem().map(|s| s.to_string_lossy().to_string()) else { continue };
            if path.extension().is_none_or(|x| x != "sql") || id == ORIGIN_STEM {
                continue;
            }
            files.push(MigrationFile {
                contract: id.ends_with(CONTRACT_SUFFIX),
                draft: id.starts_with(DRAFT_PREFIX),
                id,
                path,
            });
        }
    }
    files.sort_by(|a, b| a.id.cmp(&b.id));
    if let Some(pair) = files.windows(2).find(|w| w[0].id == w[1].id) {
        return Err(format!(
            "migrations/{group}: two files share the id '{}': {} and {}",
            pair[0].id,
            pair[0].path.display(),
            pair[1].path.display()
        ));
    }
    let origin = Some(dir.join(format!("{ORIGIN_STEM}.sql"))).filter(|p| p.is_file());
    Ok(GroupFiles { group, dir, drafts, origin, files })
}
