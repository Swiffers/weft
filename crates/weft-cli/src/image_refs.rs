//! Which shared images and compile caches each install on this machine
//! uses, so a machine-wide cleanup keeps every install's and only drops
//! what none of them uses.
//!
//! Each install keeps its own record ([`ImageRefs`], in its folder): its
//! start writes it, its builds add to it, and a sweep keeps the union of
//! every install's. An install with no record (one last started by an
//! older weft, or one whose first start has not written it yet) could be
//! using anything, so while one exists the sweep removes nothing of that
//! kind and says which install to start.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::Result;
use weft_core::images::ImageRefs;
use weft_core::infra::Install;

/// An install found on this machine, and what it recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    pub install: Install,
    pub refs: Option<ImageRefs>,
}

/// Every install's record, held: no install's build adds to its record
/// and no start replaces one while this lives
/// ([`ImageRefs::load_held`]). A sweep holds it from its last look at the
/// records through the removal that look decided, and no longer: a build
/// waits at most one removal.
pub struct Records {
    pub installs: Vec<Recorded>,
    _held: Vec<std::fs::File>,
}

/// Every install on the machine whose weft files are under `data_dir`,
/// with its record ([`crate::commands::daemon::Install::every_on`]), each
/// read under its lock, taken in the order of the installs' folders (the
/// default one first, then by name), so two sweeps never wait on each
/// other in a circle.
///
/// An install whose folder went while it was being read (a `weft daemon
/// remove` at that moment) is left out: it uses nothing any more.
/// Waits on the blocking pool, saying whose record it waits for.
pub async fn every_install(data_dir: &Path) -> Result<Records> {
    let mut records = Records { installs: Vec::new(), _held: Vec::new() };
    for install in crate::commands::daemon::Install::every_on(data_dir)? {
        let (dir, label) = (install.dir.clone(), install.id.label_value().to_string());
        let (refs, held) = tokio::task::spawn_blocking(move || {
            ImageRefs::load_held(&dir, || eprintln!("waiting for the image record of install '{label}': a build or another cleanup holds it"))
        })
        .await??;
        if refs.is_none() && !install.dir.exists() {
            continue;
        }
        records.installs.push(Recorded { refs, install: install.id });
        records._held.extend(held);
    }
    Ok(records)
}

/// What a sweep of one kind may keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kept {
    /// Every install recorded its own: these, and nothing else of the
    /// kind, are in use.
    Only(BTreeSet<String>),
    /// At least one install has not: nothing of the kind may go, and the
    /// message says which install and what to do.
    Unknown(String),
}

/// What of one kind (`field` of each record, `what` in the message) the
/// installs in `records` use: the union of their records, or why nothing
/// of it may go.
pub fn kept(records: &[Recorded], field: fn(&ImageRefs) -> Vec<&str>, what: &str) -> Kept {
    let unrecorded: Vec<&Install> = records.iter().filter(|r| r.refs.is_none()).map(|r| &r.install).collect();
    if unrecorded.is_empty() {
        return Kept::Only(records.iter().filter_map(|r| r.refs.as_ref()).flat_map(field).map(str::to_string).collect());
    }
    let names: Vec<String> = unrecorded.iter().map(|i| format!("'{}'", i.label_value())).collect();
    let starts: Vec<String> = unrecorded.iter().map(|i| format!("`WEFT_INSTALL={} weft daemon start`", i.env_value())).collect();
    let removes: Vec<String> =
        unrecorded.iter().filter(|i| i.name().is_some()).map(|i| format!("`WEFT_INSTALL={} weft daemon remove`", i.env_value())).collect();
    let or_remove = match removes.is_empty() {
        true => String::new(),
        false => format!(", or take it off this machine with {}", removes.join(", ")),
    };
    Kept::Unknown(format!(
        "no {what} removed: install {} has not recorded which images it uses (an older weft started it, or its \
         first start has not written it yet); run {} (or ./setup.sh in its checkout), then clean again{or_remove}",
        names.join(", "),
        starts.join(", "),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refs(n: u8) -> ImageRefs {
        ImageRefs {
            runtime: format!("ghcr.io/weavemindai/weft-runtime:r{n}"),
            standard_worker: format!("ghcr.io/weavemindai/weft-worker:w{n}"),
            builder_bases: [format!("ghcr.io/weavemindai/weft-builder-base:b{n}")].into(),
            compile_cache_keys: [format!("k{n}")].into(),
        }
    }

    fn keys(r: &ImageRefs) -> Vec<&str> {
        r.compile_cache_keys.iter().map(String::as_str).collect()
    }

    #[test]
    fn the_union_of_every_record_is_kept() {
        let mut grown = refs(2);
        grown.compile_cache_keys.insert("k9".into());
        let records = [
            Recorded { install: Install::default_install(), refs: Some(refs(1)) },
            Recorded { install: Install::named("deploy").unwrap(), refs: Some(grown) },
            Recorded { install: Install::named("cell1").unwrap(), refs: Some(refs(2)) },
        ];
        assert_eq!(
            kept(&records, |r| r.builder_bases.iter().map(String::as_str).collect(), "builder-base image"),
            Kept::Only(["ghcr.io/weavemindai/weft-builder-base:b1".to_string(), "ghcr.io/weavemindai/weft-builder-base:b2".to_string()].into())
        );
        assert_eq!(
            kept(&records, keys, "compile cache"),
            Kept::Only(["k1".to_string(), "k2".to_string(), "k9".to_string()].into()),
            "every key an install's builds added is kept"
        );
        assert_eq!(kept(&[], |r| vec![r.runtime.as_str()], "runtime image"), Kept::Only(BTreeSet::new()), "no install on the machine uses anything");
    }

    #[test]
    fn an_install_without_a_record_keeps_the_whole_kind() {
        let records = [
            Recorded { install: Install::default_install(), refs: None },
            Recorded { install: Install::named("deploy").unwrap(), refs: Some(refs(2)) },
        ];
        let Kept::Unknown(why) = kept(&records, |r| vec![r.runtime.as_str()], "runtime image") else { panic!("nothing may go") };
        assert!(why.starts_with("no runtime image removed"), "{why}");
        assert!(why.contains("'default'") && why.contains("`WEFT_INSTALL=default weft daemon start`"), "{why}");
        assert!(why.contains("./setup.sh in its checkout") && !why.contains("deploy") && !why.contains("weft daemon remove"), "{why}");

        let named = [Recorded { install: Install::named("half").unwrap(), refs: None }];
        let Kept::Unknown(why) = kept(&named, keys, "worker compile cache") else { panic!("nothing may go") };
        assert!(why.contains("`WEFT_INSTALL=half weft daemon remove`"), "a named install can also be taken off: {why}");
    }

    /// A named install's folder is an install from the moment it exists,
    /// before its first start writes anything into it: a sweep from
    /// another checkout in that window keeps everything.
    #[test]
    fn every_install_on_the_machine_is_read_with_its_record() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path();
        refs(1).save(data).unwrap();
        std::fs::create_dir_all(data.join("installs/deploy")).unwrap();
        let found = tokio::runtime::Runtime::new().unwrap().block_on(every_install(data)).unwrap().installs;
        assert_eq!(
            found,
            vec![
                Recorded { install: Install::default_install(), refs: Some(refs(1)) },
                Recorded { install: Install::named("deploy").unwrap(), refs: None },
            ]
        );
        assert!(matches!(kept(&found, keys, "worker compile cache"), Kept::Unknown(_)), "the starting install keeps every key");
    }
}
