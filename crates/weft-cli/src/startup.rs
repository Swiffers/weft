//! What runs before the flags are read: which `weft` binary serves this
//! command (the front door), and which `.env` files feed it.

use std::path::{Path, PathBuf};

use weft_core::infra::{Install, Wanted, WantedFrom};

/// What the front door does with a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Door {
    /// This binary runs it.
    Here,
    /// The install's own binary at this path runs it, with the same
    /// arguments, in place of this process.
    Exec(PathBuf),
    /// Nothing runs it: the message says why and what to do.
    Refuse(String),
}

/// The `weft` in the bin folder of the install a command acts on, as the
/// front door finds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WantedBin {
    /// There is none: that install was never set up.
    Missing,
    /// There is one at `path`, and once every link is resolved its file
    /// lives in the bin folder of `belongs_to` (`None`: in no install's,
    /// like a link to a `target/` build).
    At { path: PathBuf, belongs_to: Option<Install> },
}

/// Which binary serves a command acting on `wanted`, run as the binary
/// whose own install is `mine` (`None` for a build outside every
/// install's bin, like `target/debug/weft`, which serves any install: it
/// is how setup.sh asks before anything is installed, and how a person
/// tries a build by hand). Every install runs its own CLI, so a binary of
/// one install hands a command for another to that one's `weft`, and
/// refuses when there is none, rather than act on an install it was not
/// built for. `answers_only` is `weft daemon which`, which reports on the
/// install without acting on it, so this binary always serves it.
///
/// It never sends a command round in a circle: the binary it hands over
/// to lives in `wanted`'s bin, so that binary's `mine` is `wanted` and it
/// runs the command itself; a `weft` there that resolves into ANOTHER
/// install's bin would hand it straight back, and is refused instead.
pub fn front_door(mine: Option<&Install>, wanted: &Wanted, bin: &WantedBin, answers_only: bool) -> Door {
    let Some(mine) = mine else { return Door::Here };
    if mine == &wanted.install || answers_only {
        return Door::Here;
    }
    match bin {
        WantedBin::At { path, belongs_to: Some(other) } if other != &wanted.install => Door::Refuse(format!(
            "{} should be the CLI of install '{}', and it resolves into the bin folder of install '{}' instead; \
             run ./setup.sh in the checkout of '{}' to install its own",
            path.display(),
            wanted.install.label_value(),
            other.label_value(),
            wanted.install.label_value()
        )),
        WantedBin::At { path, .. } => Door::Exec(path.clone()),
        WantedBin::Missing => match &wanted.from {
            // A test cell, or a person naming an install by hand: a named
            // install whose files this binary makes itself.
            WantedFrom::Env => Door::Here,
            WantedFrom::Checkout { root, branch } => Door::Refuse(format!(
                "this checkout ({}) is on branch {branch}, whose install ({}) is not set up; run ./setup.sh in {}. \
                 To take that install off this machine instead, run `{}={} weft daemon remove`",
                root.display(),
                wanted.install.label_value(),
                root.display(),
                weft_core::infra::INSTALL_ENV,
                wanted.install.env_value()
            )),
            WantedFrom::Outside => Door::Refuse(format!(
                "outside a weft checkout, weft acts on the default install, which is not set up on this machine \
                 (no {}); run ./setup.sh in the checkout of weft's main branch",
                cli_of(&wanted.install).display()
            )),
        },
    }
}

/// The install's own `weft`, in its bin folder.
// SYNC: weft <-> setup.sh (the install's bin), Cargo.toml ([[bin]] name)
pub fn cli_of(install: &Install) -> PathBuf {
    install.bin_dir().join("weft")
}

/// The install a command run from `cwd` with `env_value` in
/// `WEFT_INSTALL` acts on, and its `weft` as [`front_door`] reads it.
pub fn wanted_here(env_value: Option<&str>, cwd: &Path) -> anyhow::Result<(Wanted, WantedBin)> {
    let wanted = Install::wanted(env_value, cwd).map_err(anyhow::Error::msg)?;
    let path = cli_of(&wanted.install);
    if !path.is_file() {
        return Ok((wanted, WantedBin::Missing));
    }
    // Resolved the way `Install::of_current_exe` resolves the binary that
    // runs, so the two agree on where a file lives.
    let resolved = path.canonicalize().map_err(|e| anyhow::anyhow!("cannot resolve {}: {e}", path.display()))?;
    let data = weft_core::infra::data_dir();
    let data = data.canonicalize().map_err(|e| anyhow::anyhow!("cannot resolve {}: {e}", data.display()))?;
    let belongs_to = Install::of_bin_dir(resolved.parent().unwrap_or(&resolved), &data);
    Ok((wanted, WantedBin::At { path, belongs_to }))
}

/// Replace this process with `path`, given the same arguments. Returns
/// only when that could not happen. The CLI runs on Unix only (it hands
/// its output streams around through `std::os::fd`), where `exec` keeps
/// the process id, the terminal and the exit status the caller waits on.
pub fn exec(path: &Path) -> anyhow::Error {
    use std::os::unix::process::CommandExt as _;
    let error = std::process::Command::new(path).args(std::env::args_os().skip(1)).exec();
    anyhow::anyhow!("could not run {}: {error}", path.display())
}

/// The `.env` files a command started in `cwd` loads, in the order it
/// loads them: the nearest folder at or above `cwd` holding a `.env` or a
/// `.env.local`, its `.env.local` first, then its `.env`. A value is
/// never overwritten once set, so `.env.local` (this checkout's own,
/// never linked into another worktree) wins over `.env` (the shared one),
/// and the real environment wins over both. The folder is the one where
/// the file was found, never where a linked `.env` points.
pub fn env_files(cwd: &Path) -> Vec<PathBuf> {
    let Some(dir) = cwd.ancestors().find(|d| ENV_FILES.iter().any(|f| d.join(f).is_file())) else {
        return Vec::new();
    };
    ENV_FILES.iter().map(|f| dir.join(f)).filter(|p| p.is_file()).collect()
}

/// The `.env` files of one folder, in load order.
// SYNC: ENV_FILES <-> crates/weft-e2e/src/ensure.rs (load_repo_env), .gitignore (.env.local),
//       scripts/add-worktree.sh (links .env, never .env.local)
const ENV_FILES: [&str; 2] = [".env.local", ".env"];

/// Load `files` into this process's environment, each value only where
/// none is set yet. A file that does not parse fails loud, naming it.
pub fn load_env_files(files: &[PathBuf]) -> anyhow::Result<()> {
    for file in files {
        dotenvy::from_path(file).map_err(|e| anyhow::anyhow!("failed to load {}: {e}", file.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(name: &str) -> Install {
        Install::named(name).unwrap()
    }

    fn checkout_wanted(install: Install) -> Wanted {
        Wanted { install, from: WantedFrom::Checkout { root: "/w/deploy".into(), branch: "deploy".into() } }
    }

    fn at(path: &str, belongs_to: Option<Install>) -> WantedBin {
        WantedBin::At { path: path.into(), belongs_to }
    }

    #[test]
    fn a_build_outside_every_install_and_the_install_own_binary_run_the_command() {
        let wanted = checkout_wanted(named("deploy"));
        for bin in [WantedBin::Missing, at("/d/installs/deploy/bin/weft", Some(named("deploy")))] {
            assert_eq!(front_door(None, &wanted, &bin, false), Door::Here, "a target/ build serves any install");
            assert_eq!(front_door(Some(&named("deploy")), &wanted, &bin, false), Door::Here, "its own binary");
        }
    }

    #[test]
    fn another_install_binary_hands_the_command_to_the_wanted_one() {
        let wanted = checkout_wanted(named("deploy"));
        let bin = at("/d/installs/deploy/bin/weft", Some(named("deploy")));
        let mine = Install::default_install();
        assert_eq!(front_door(Some(&mine), &wanted, &bin, false), Door::Exec("/d/installs/deploy/bin/weft".into()));
        // A link into no install's bin (a target/ build) is handed over
        // too: that binary serves any install.
        let linked = at("/d/installs/deploy/bin/weft", None);
        assert_eq!(front_door(Some(&mine), &wanted, &linked, false), Door::Exec("/d/installs/deploy/bin/weft".into()));
        // The default install's own CLI, from a named one's.
        let default = Wanted { install: Install::default_install(), from: WantedFrom::Outside };
        let default_bin = at("/d/bin/weft", Some(Install::default_install()));
        assert_eq!(front_door(Some(&named("deploy")), &default, &default_bin, false), Door::Exec("/d/bin/weft".into()));
    }

    #[test]
    fn an_install_that_is_not_set_up_is_refused_unless_the_env_named_it() {
        let mine = Install::default_install();
        let from_checkout = checkout_wanted(named("deploy"));
        let Door::Refuse(why) = front_door(Some(&mine), &from_checkout, &WantedBin::Missing, false) else { panic!("refused") };
        assert!(why.contains("/w/deploy") && why.contains("branch deploy") && why.contains("./setup.sh"), "{why}");
        assert!(why.contains("`WEFT_INSTALL=deploy weft daemon remove`"), "says how to remove it instead: {why}");
        // That command names the install in the environment, which this
        // binary serves even with no bin of the install's own.
        let removing = Wanted { install: named("deploy"), from: WantedFrom::Env };
        assert_eq!(front_door(Some(&mine), &removing, &WantedBin::Missing, false), Door::Here);

        let outside = Wanted { install: Install::default_install(), from: WantedFrom::Outside };
        let Door::Refuse(why) = front_door(Some(&named("deploy")), &outside, &WantedBin::Missing, false) else { panic!("refused") };
        assert!(why.contains("default install") && why.contains("./setup.sh"), "{why}");

        let cell = Wanted { install: named("e2eab"), from: WantedFrom::Env };
        assert_eq!(front_door(Some(&mine), &cell, &WantedBin::Missing, false), Door::Here, "a test cell runs on this binary");
    }

    #[test]
    fn which_is_answered_by_any_binary() {
        let mine = Install::default_install();
        let from_checkout = checkout_wanted(named("deploy"));
        assert_eq!(front_door(Some(&mine), &from_checkout, &WantedBin::Missing, true), Door::Here);
        let bin = at("/d/installs/deploy/bin/weft", Some(named("deploy")));
        assert_eq!(front_door(Some(&mine), &from_checkout, &bin, true), Door::Here);
    }

    /// Whatever the front door hands a command to, that binary runs it
    /// itself: its own install is the wanted one, or it belongs to none.
    #[test]
    fn a_command_is_never_handed_round_in_a_circle() {
        let installs = [None, Some(Install::default_install()), Some(named("deploy")), Some(named("other"))];
        let froms = [WantedFrom::Env, WantedFrom::Outside, WantedFrom::Checkout { root: "/w".into(), branch: "b".into() }];
        for wanted_install in installs.iter().flatten() {
            for from in &froms {
                let wanted = Wanted { install: wanted_install.clone(), from: from.clone() };
                for mine in &installs {
                    for belongs_to in &installs {
                        let bin = at("/x/weft", belongs_to.clone());
                        if let Door::Exec(_) = front_door(mine.as_ref(), &wanted, &bin, false) {
                            // The next process: the same wanted, its own
                            // install the one the file lives in.
                            let next = front_door(belongs_to.as_ref(), &wanted, &bin, false);
                            assert_eq!(next, Door::Here, "{mine:?} -> {belongs_to:?} for {wanted:?}");
                        }
                    }
                }
            }
        }
        let wrong = at("/d/installs/deploy/bin/weft", Some(named("other")));
        let Door::Refuse(why) = front_door(Some(&Install::default_install()), &checkout_wanted(named("deploy")), &wrong, false) else {
            panic!("a weft that resolves into another install would hand the command back")
        };
        assert!(why.contains("'other'") && why.contains("'deploy'"), "{why}");
    }

    #[test]
    fn the_env_files_come_from_the_nearest_folder_holding_one_local_first() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("weft");
        let inner = root.join("crates/x");
        std::fs::create_dir_all(&inner).unwrap();
        assert!(env_files(&inner).is_empty(), "no file is normal");
        std::fs::write(root.join(".env"), "A=1\n").unwrap();
        assert_eq!(env_files(&inner), vec![root.join(".env")]);
        std::fs::write(root.join(".env.local"), "A=2\n").unwrap();
        assert_eq!(env_files(&inner), vec![root.join(".env.local"), root.join(".env")]);
        // A nearer folder with only a `.env.local` is the one.
        std::fs::write(inner.join(".env.local"), "A=3\n").unwrap();
        assert_eq!(env_files(&inner), vec![inner.join(".env.local")]);
    }

    /// A linked `.env` is read where it was found: the `.env.local` beside
    /// the link, never the one beside the file it points at.
    #[test]
    fn a_linked_env_takes_the_local_file_beside_the_link() {
        let dir = tempfile::tempdir().unwrap();
        let main = dir.path().join("main");
        let worktree = dir.path().join("wt");
        std::fs::create_dir_all(&main).unwrap();
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::write(main.join(".env"), "A=1\n").unwrap();
        std::fs::write(main.join(".env.local"), "A=main\n").unwrap();
        std::os::unix::fs::symlink(main.join(".env"), worktree.join(".env")).unwrap();
        assert_eq!(env_files(&worktree), vec![worktree.join(".env")]);
        std::fs::write(worktree.join(".env.local"), "A=wt\n").unwrap();
        assert_eq!(env_files(&worktree), vec![worktree.join(".env.local"), worktree.join(".env")]);
    }

    /// `.env.local` wins over `.env`, and the real environment over both.
    /// Sets process variables of its own: nextest runs each test in its
    /// own process.
    #[test]
    fn the_local_file_wins_over_the_shared_one_and_the_environment_over_both() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, c) = ("WEFT_STARTUP_TEST_A", "WEFT_STARTUP_TEST_B", "WEFT_STARTUP_TEST_C");
        std::fs::write(dir.path().join(".env"), format!("{a}=shared\n{b}=shared\n{c}=shared\n")).unwrap();
        std::fs::write(dir.path().join(".env.local"), format!("{a}=local\n{c}=local\n")).unwrap();
        std::env::set_var(c, "real");
        load_env_files(&env_files(dir.path())).unwrap();
        assert_eq!(std::env::var(a).unwrap(), "local");
        assert_eq!(std::env::var(b).unwrap(), "shared");
        assert_eq!(std::env::var(c).unwrap(), "real");

        std::fs::write(dir.path().join(".env.local"), "not a line\n").unwrap();
        let error = load_env_files(&env_files(dir.path())).unwrap_err().to_string();
        assert!(error.contains(".env.local"), "a malformed file is named: {error}");
    }
}
