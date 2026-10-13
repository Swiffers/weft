//! The ledger of what tests made, so the next run removes what a failed one
//! kept, and only that.
//!
//! A project is found by its `e2e_` name, but nothing else a test makes can
//! be told from yours by looking at it: a connection (an access grant) is
//! named by the service it connects to, and a cell's `e2e` name says only
//! that SOME run made it, maybe another branch's, from another checkout.
//! So each one is written here while it lives, and struck off when the test
//! removes it; whatever a failed test left in the ledger is what the next
//! run of `scripts/run-e2e.sh` removes before it starts. A cell's own grants
//! go with the cell.
//!
//! The runner says where the ledger lives (`WEFT_E2E_KEPT_DIR`, under the
//! install under test's own directory), outside the per-run log directory,
//! so a new run does not forget what the last one kept, and a run on one
//! install never sees another install's.
//!
//! One empty file per entry, named after it, in a folder per kind. Every
//! test is a process of its own and many run at once, so the ledger is
//! written concurrently; distinct names never collide, and making or
//! removing one file is atomic, so no write can tear another.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::client::Dispatcher;

/// The variable the runner names the ledger's directory in.
// SYNC: WEFT_E2E_KEPT_DIR <-> scripts/run-e2e.sh
pub const KEPT_DIR_ENV: &str = "WEFT_E2E_KEPT_DIR";

/// What the ledger keeps, one folder each.
#[derive(Clone, Copy)]
enum Kind {
    Grant,
    Cell,
}

impl Kind {
    fn folder(self) -> &'static str {
        match self {
            Kind::Grant => "grants",
            Kind::Cell => "cells",
        }
    }
}

/// A grant `disp`'s install now holds for this test. A cell's grants go
/// with the cell, so only the install under test's are written down.
pub(crate) fn grant_made(disp: &Dispatcher, grant_id: &str) -> Result<()> {
    if disp.install() != &crate::ensure::install_under_test()? {
        return Ok(());
    }
    grant_seeded(grant_id)
}

/// A grant the install under test now holds for this test (one seeded
/// straight into its database).
pub(crate) fn grant_seeded(grant_id: &str) -> Result<()> {
    record(&ledger()?, Kind::Grant, grant_id)
}

/// The test removed the grant.
pub(crate) fn grant_gone(grant_id: &str) -> Result<()> {
    strike(&ledger()?, Kind::Grant, grant_id)
}

/// A cell this test is starting.
pub(crate) fn cell_made(name: &str) -> Result<()> {
    record(&ledger()?, Kind::Cell, name)
}

/// The test removed the cell.
pub(crate) fn cell_gone(name: &str) -> Result<()> {
    strike(&ledger()?, Kind::Cell, name)
}

/// Every grant the ledger still holds: what failed tests kept. Read
/// only by the sweep, which reaches the install, so `e2e` only.
#[cfg(feature = "e2e")]
pub(crate) fn kept_grants() -> Result<Vec<String>> {
    listed(&ledger()?, Kind::Grant)
}

/// Every cell the ledger still holds: what failed tests kept.
#[cfg(feature = "e2e")]
pub(crate) fn kept_cells() -> Result<Vec<String>> {
    listed(&ledger()?, Kind::Cell)
}

fn ledger() -> Result<PathBuf> {
    let root = std::env::var(KEPT_DIR_ENV).map_err(|_| {
        anyhow::anyhow!(
            "{KEPT_DIR_ENV} is not set: e2e tests run through scripts/run-e2e.sh, which \
             says where the ledger of what they keep lives"
        )
    })?;
    Ok(PathBuf::from(root))
}

fn record(ledger: &Path, kind: Kind, name: &str) -> Result<()> {
    let dir = ledger.join(kind.folder());
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    std::fs::write(dir.join(name), b"").with_context(|| format!("record {name} in {}", dir.display()))
}

fn strike(ledger: &Path, kind: Kind, name: &str) -> Result<()> {
    let path = ledger.join(kind.folder()).join(name);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("strike {} off the ledger", path.display())),
    }
}

#[cfg(any(test, feature = "e2e"))]
fn listed(ledger: &Path, kind: Kind) -> Result<Vec<String>> {
    let dir = ledger.join(kind.folder());
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("read {}", dir.display())),
    };
    let mut names = entries
        .map(|e| Ok(e?.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::{listed, record, strike, Kind};

    fn ledger(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("weft-kept-ledger-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn a_ledger_lists_what_was_made_and_not_struck_off() {
        let dir = ledger("lists");
        assert!(listed(&dir, Kind::Cell).unwrap().is_empty(), "a ledger nobody wrote lists nothing");
        record(&dir, Kind::Cell, "e2ekept1").unwrap();
        record(&dir, Kind::Cell, "e2egone1").unwrap();
        record(&dir, Kind::Grant, "grant-1").unwrap();
        strike(&dir, Kind::Cell, "e2egone1").unwrap();
        // Striking twice, or what was never made, is as good as struck.
        strike(&dir, Kind::Cell, "e2egone1").unwrap();
        strike(&dir, Kind::Grant, "never-made").unwrap();
        assert_eq!(listed(&dir, Kind::Cell).unwrap(), vec!["e2ekept1"]);
        assert_eq!(listed(&dir, Kind::Grant).unwrap(), vec!["grant-1"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn tests_writing_the_ledger_at_once_lose_nothing() {
        let dir = ledger("concurrent");
        let names: Vec<String> = (0..32).map(|i| format!("e2ecell{i:02}")).collect();
        std::thread::scope(|s| {
            for name in &names {
                let dir = &dir;
                s.spawn(move || record(dir, Kind::Cell, name).unwrap());
            }
        });
        assert_eq!(listed(&dir, Kind::Cell).unwrap(), names);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
