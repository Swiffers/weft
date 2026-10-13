//! Which install a process belongs to, when one machine holds several.
//!
//! A machine normally holds one weft install: the one `weft daemon start`
//! brings up. A NAMED install (a test cell, say) sits beside it with names
//! of its own for everything it creates on the machine or in a cloud
//! project (its database, its worker and infra containers, its services),
//! so the two never touch each other's. Every such name starts with
//! [`Install::resource_prefix`], answered here and nowhere else.

/// The longest install name: it goes into container and service names
/// next to a unit or project suffix.
pub const MAX_INSTALL_NAME: usize = 12;

/// One install's identity. The default install has no name.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Install {
    name: Option<String>,
}

impl Install {
    /// The install a machine holds when nobody named one.
    pub fn default_install() -> Self {
        Self { name: None }
    }

    /// A named install. The name becomes part of resource names, so it is
    /// refused unless it is 1 to [`MAX_INSTALL_NAME`] lowercase letters
    /// and digits starting with a letter.
    pub fn named(name: &str) -> Result<Self, String> {
        let ok = !name.is_empty()
            && name.len() <= MAX_INSTALL_NAME
            && name.starts_with(|c: char| c.is_ascii_lowercase())
            && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            // The default install's label value.
            && name != "default";
        if !ok {
            return Err(format!(
                "'{name}' cannot name an install: it goes into resource names, so it must be \
                 1 to {MAX_INSTALL_NAME} lowercase letters and digits, starting with a letter, \
                 and not `default`"
            ));
        }
        Ok(Self { name: Some(name.to_string()) })
    }

    /// The install this process acts on: the one [`INSTALL_ENV`] names
    /// (`default` names the default install), the default install when it
    /// is unset or blank. It never looks at the working directory: that is
    /// [`Install::wanted`], which the CLI's front door asks once before
    /// setting the variable, so every later read in the process, and every
    /// process it starts, agrees.
    ///
    /// When the front door could not tell which install a command acts on
    /// (a checkout on no branch, [`mark_unresolved`]) and the variable is
    /// unset, this is that reason: a command that needs an install fails
    /// with it, and one that needs none (validating a project) never asks.
    pub fn from_env() -> Result<Self, String> {
        let value = std::env::var(INSTALL_ENV).ok();
        if let (None, Some(why)) = (value.as_deref().filter(|v| !v.trim().is_empty()), UNRESOLVED.get()) {
            return Err(why.clone());
        }
        Self::from_env_value(value.as_deref())
    }

    /// The install a value of [`INSTALL_ENV`] names: `None` or blank is
    /// the default install, and so is `default`.
    pub fn from_env_value(value: Option<&str>) -> Result<Self, String> {
        match value.map(str::trim).filter(|v| !v.is_empty()) {
            None | Some(DEFAULT_LABEL) => Ok(Self::default_install()),
            Some(name) => Self::named(name),
        }
    }

    /// What [`INSTALL_ENV`] holds for this install: `default` for the
    /// default install, so a process that inherits it never re-derives the
    /// install from its own working directory.
    pub fn env_value(&self) -> &str {
        self.label_value()
    }

    /// The install a branch of weft gets: the default install for `main`,
    /// an install of its own for every other branch. A branch name that is
    /// already a valid install name (`deploy`) is used as is; any other
    /// (`fix/journal-holes`) becomes up to 7 of its letters and digits and
    /// 5 hex digits of its hash (`fixjour3be1c`). Two branches share an
    /// install only when they cut to the same letters and their hashes
    /// agree on those 5 digits (about one chance in a million), or when
    /// one is named exactly like the other's hashed name. A name starting
    /// `e2e` is never produced: those are the e2e runner's cells.
    pub fn for_branch(branch: &str) -> Self {
        if branch == "main" {
            return Self::default_install();
        }
        let kept: String = branch.chars().filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit()).collect();
        if kept == branch && !kept.starts_with(E2E_CELL_PREFIX) {
            if let Ok(install) = Self::named(&kept) {
                return install;
            }
        }
        let kept: String = branch.to_ascii_lowercase().chars().filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit()).collect();
        let hash = format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(branch.as_bytes()));
        let hash = &hash[..BRANCH_HASH_LEN];
        let mut prefix = kept;
        prefix.truncate(MAX_INSTALL_NAME - BRANCH_HASH_LEN);
        // The hash can finish an `e2e` the letters started (`e2` + `e...`).
        if !prefix.starts_with(|c: char| c.is_ascii_lowercase()) || format!("{prefix}{hash}").starts_with(E2E_CELL_PREFIX) {
            prefix.insert(0, 'b');
            prefix.truncate(MAX_INSTALL_NAME - BRANCH_HASH_LEN);
        }
        Self::named(&format!("{prefix}{hash}")).expect("a letter, then letters and digits, within the length")
    }

    /// The install the weft checkout at `root` acts on: its branch's
    /// ([`Install::for_branch`]). A checkout on no branch picks none.
    pub fn of_checkout(root: &std::path::Path) -> Result<Self, String> {
        match crate::checkout::branch(root)? {
            Some(branch) => Ok(Self::for_branch(&branch)),
            None => Err(on_no_branch(root)),
        }
    }

    /// The install a command run with `env_value` in [`INSTALL_ENV`],
    /// from the folder `cwd`, acts on: the one the variable names when it
    /// is set; otherwise the one of the weft checkout `cwd` is inside
    /// ([`Install::of_checkout`]); otherwise, outside any checkout, the
    /// default install.
    pub fn wanted(env_value: Option<&str>, cwd: &std::path::Path) -> Result<Wanted, String> {
        if env_value.is_some_and(|v| !v.trim().is_empty()) {
            return Ok(Wanted { install: Self::from_env_value(env_value)?, from: WantedFrom::Env });
        }
        match crate::checkout::enclosing(cwd) {
            None => Ok(Wanted { install: Self::default_install(), from: WantedFrom::Outside }),
            Some(root) => match crate::checkout::branch(&root)? {
                Some(branch) => Ok(Wanted { install: Self::for_branch(&branch), from: WantedFrom::Checkout { root, branch } }),
                None => Err(on_no_branch(&root)),
            },
        }
    }

    /// The install whose binaries live in `bin_dir`, under the root of
    /// weft's files `data_dir`: `<data_dir>/bin` is the default install's,
    /// `<data_dir>/installs/<name>/bin` a named one's. `None` for any other
    /// folder (a `target/debug` build, run by hand or by cargo): such a
    /// binary belongs to no install.
    pub fn of_bin_dir(bin_dir: &std::path::Path, data_dir: &std::path::Path) -> Option<Self> {
        if bin_dir == data_dir.join(BIN_DIR) {
            return Some(Self::default_install());
        }
        let rest = bin_dir.strip_prefix(data_dir.join("installs")).ok()?;
        let mut parts = rest.components();
        let name = parts.next()?.as_os_str().to_str()?;
        let is_bin = parts.next().is_some_and(|c| c.as_os_str() == BIN_DIR) && parts.next().is_none();
        is_bin.then(|| Self::named(name).ok()).flatten()
    }

    /// The install the running binary belongs to ([`Install::of_bin_dir`]
    /// on the folder it runs from, both paths resolved through links, so
    /// `~/.local/bin/weft` is the default install's). `None` for a binary
    /// outside every install's `bin`.
    pub fn of_current_exe() -> Option<Self> {
        let exe = std::env::current_exe().ok()?.canonicalize().ok()?;
        let data = data_dir().canonicalize().ok()?;
        Self::of_bin_dir(exe.parent()?, &data)
    }

    /// The file holding this install's secrets, one `NAME=value` per line:
    /// written by `weft daemon start`, sourced by its runtime's unit.
    pub fn secrets_path(&self) -> std::path::PathBuf {
        secrets_file(&self.dir())
    }

    /// The value `name` has in this install's [`Install::secrets_path`].
    /// An install never started, or a file naming no such line, is an
    /// error saying how to get one.
    pub fn secret(&self, name: &str) -> Result<String, String> {
        secret_in(&self.secrets_path(), self.env_value(), name)
    }

    /// Where this install's own `weft` and `weft-runtime` live: `bin` in
    /// its [`Install::dir`].
    pub fn bin_dir(&self) -> std::path::PathBuf {
        self.dir().join(BIN_DIR)
    }

    /// Where this install keeps its files on this machine: the root of
    /// weft's files for the default install, `installs/<name>` under it
    /// for a named one.
    pub fn dir(&self) -> std::path::PathBuf {
        match &self.name {
            None => data_dir(),
            Some(n) => data_dir().join("installs").join(n),
        }
    }

    /// The install's name, `None` for the default install.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// What every resource this install creates is named after: `weft`
    /// for the default install, `weft-<name>` for a named one. A named
    /// install's prefix never starts another install's, because the
    /// default one stops at `weft` and a name holds no `-`.
    pub fn resource_prefix(&self) -> String {
        match &self.name {
            None => "weft".to_string(),
            Some(n) => format!("weft-{n}"),
        }
    }

    /// The value of the `weft-install` label every resource this install
    /// creates carries (a container label, a cloud resource label), which
    /// is how a cleanup finds exactly this install's resources.
    pub fn label_value(&self) -> &str {
        self.name.as_deref().unwrap_or(DEFAULT_LABEL)
    }

    /// The name this install gives an image it keeps on the local Docker
    /// daemon, from its content-addressed `tag` (`weft-worker:<hash>`):
    /// the tag itself for the default install,
    /// `localhost/weft-<name>/<tag>` for a named one.
    ///
    /// Each install holds its images under names of its own, so two
    /// installs that built the same content each hold a name on one
    /// image, and an install removing its name never takes the image from
    /// another (Docker deletes an image with its last name). `localhost/`
    /// keeps Docker from ever pulling such a name from Docker Hub.
    pub fn local_image_ref(&self, tag: &str) -> String {
        match &self.name {
            None => tag.to_string(),
            Some(n) => format!("{LOCAL_IMAGE_REGISTRY}/weft-{n}/{tag}"),
        }
    }

    /// The tag an install's local image name carries (`weft-worker:<hash>`),
    /// when `image_ref` is one of any install's local image names
    /// ([`Self::local_image_ref`]); `None` for any other image.
    pub fn tag_of_local_image(image_ref: &str) -> Option<&str> {
        let tag = match image_ref.strip_prefix(LOCAL_IMAGE_REGISTRY).and_then(|r| r.strip_prefix("/weft-")) {
            Some(rest) => {
                let (name, tag) = rest.split_once('/')?;
                Self::named(name).ok()?;
                tag
            }
            None => image_ref,
        };
        (!tag.contains('/') && tag.contains(':')).then_some(tag)
    }
}

/// Why the install this process acts on could not be told, when it could
/// not ([`mark_unresolved`]).
static UNRESOLVED: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Record that this process could not tell which install it acts on, and
/// why: from then on [`Install::from_env`] answers that reason, unless
/// [`INSTALL_ENV`] names one. The CLI's front door calls it, so a command
/// that needs no install still runs where none can be picked.
pub fn mark_unresolved(why: String) {
    let _ = UNRESOLVED.set(why);
}

/// Why the checkout at `root` picks no install: it is on no branch.
fn on_no_branch(root: &std::path::Path) -> String {
    format!(
        "{} is on no branch (a detached HEAD), and the branch is what picks the install a command acts on; \
         check out a branch, or name the install with {INSTALL_ENV}=<name> (`default` for the main one)",
        root.display()
    )
}

/// The install a command acts on, and what picked it
/// ([`Install::wanted`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wanted {
    pub install: Install,
    pub from: WantedFrom,
}

/// What picked the install a command acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WantedFrom {
    /// [`INSTALL_ENV`] named it.
    Env,
    /// The branch of the weft checkout the command was run in.
    Checkout { root: std::path::PathBuf, branch: String },
    /// The command was run outside every weft checkout: the default install.
    Outside,
}

/// The default install's label value, and what [`INSTALL_ENV`] holds
/// for it. Never a valid install name.
// SYNC: DEFAULT_LABEL <-> extension-vscode/src/localInstall.ts (DEFAULT_INSTALL_ENV)
const DEFAULT_LABEL: &str = "default";

/// The start no branch install's name has: the e2e runner names its
/// cells that way (`weft_e2e::cell`).
pub const E2E_CELL_PREFIX: &str = "e2e";

/// How many hex digits of a branch's hash end its install's name, when
/// the branch's own name cannot be the install's.
const BRANCH_HASH_LEN: usize = 5;

/// The folder in an install's [`Install::dir`] holding its binaries.
// SYNC: BIN_DIR <-> setup.sh (the install's bin), scripts/lib (bin of `weft daemon which`)
const BIN_DIR: &str = "bin";

/// The registry part of a named install's local image names: one Docker
/// resolves to this machine, so a missing image is never pulled from
/// anywhere else.
const LOCAL_IMAGE_REGISTRY: &str = "localhost";

/// The file in an install's dir holding its secrets
/// ([`Install::secrets_path`]).
// SYNC: SECRETS_FILE <-> scripts/run-e2e.sh (WEFT_E2E_DATABASE_URL, read the way env_file_values reads it),
//       crates/weft-cli/src/commands/daemon.rs (start: the writer)
pub const SECRETS_FILE: &str = "secrets.env";

/// The secrets file of the install whose files are in `dir`.
pub fn secrets_file(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join(SECRETS_FILE)
}

/// The value `name` has in the secrets file at `path`, of the install
/// `env_value` names (for the messages).
fn secret_in(path: &std::path::Path, env_value: &str, name: &str) -> Result<String, String> {
    let body = match std::fs::read_to_string(path) {
        Ok(body) => body,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "install '{env_value}' was never started ({} does not exist); start it with ./setup.sh --daemon from its checkout",
                path.display()
            ))
        }
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };
    env_file_values(&body).remove(name).filter(|v| !v.is_empty()).ok_or_else(|| {
        format!("{} names no {name}; start the install again with ./setup.sh --daemon from its checkout, which rewrites it", path.display())
    })
}

/// The values of an env file (`secrets.env`, `tunnel.env`): one
/// `NAME=value` per line, both sides trimmed, later lines winning.
pub fn env_file_values(body: &str) -> std::collections::BTreeMap<String, String> {
    body.lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

/// Write `bytes` to the file `name` in an install's `dir`, whole: to a
/// file beside it first, then renamed over it. The editor, the CLI and
/// setup.sh read an install's files while it starts, and a rename on one
/// filesystem means they see the old content or the new, never half.
pub fn write_whole(dir: &std::path::Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let partial = dir.join(format!("{name}.{}.tmp", std::process::id()));
    let written = std::fs::write(&partial, bytes).and_then(|()| std::fs::rename(&partial, dir.join(name)));
    if written.is_err() {
        // The save failed either way; this only keeps a stray file out of
        // the install's folder.
        let _ = std::fs::remove_file(&partial);
    }
    written
}

/// The root of weft's files on this machine.
/// Panics when `HOME` is unset: every install's files are under it, and
/// a relative path in its place would put them in whatever folder the
/// process happens to run in.
// SYNC: data_dir <-> scripts/lib/data-dir.sh (weft_state_dir),
//       extension-vscode/src/localInstall.ts (dataDir)
pub fn data_dir() -> std::path::PathBuf {
    let home = std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .expect("HOME is not set: weft keeps every install's files under $HOME/.local/share/weft");
    std::path::PathBuf::from(home).join(".local/share/weft")
}

/// The environment variable naming the install a command acts on (unset:
/// the default install).
// Rust reads and sets it through this constant; the sites outside Rust
// spell it out:
// SYNC: INSTALL_ENV <-> extension-vscode/src/localInstall.ts (installName), setup.sh,
//       scripts/run-e2e.sh, scripts/run-node-tests.sh, scripts/lib/checkout-install.sh
//       (weft_which), scripts/remove-worktree.sh
pub const INSTALL_ENV: &str = "WEFT_INSTALL";

/// The label key naming the install a resource belongs to.
// SYNC: INSTALL_LABEL <-> crates/weft-platform-local (docker labels),
//       crates/weft-platform-gcp (resource labels),
//       scripts/lib/weft-cleanup.sh (weft_install_label; setup.sh --purge
//       and scripts/scrub-old-install.sh read it from there)
pub const INSTALL_LABEL: &str = "weft-install";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_and_a_named_install_name_their_resources_apart() {
        let d = Install::default_install();
        let c = Install::named("cell7").unwrap();
        assert_eq!(d.resource_prefix(), "weft");
        assert_eq!(c.resource_prefix(), "weft-cell7");
        assert_eq!(d.label_value(), "default");
        assert_eq!(c.label_value(), "cell7");
    }

    #[test]
    fn each_install_names_its_local_images_apart() {
        let d = Install::default_install();
        let c = Install::named("cell7").unwrap();
        assert_eq!(d.local_image_ref("weft-worker:ab"), "weft-worker:ab");
        assert_eq!(c.local_image_ref("weft-worker:ab"), "localhost/weft-cell7/weft-worker:ab");
        assert_eq!(Install::tag_of_local_image("weft-worker:ab"), Some("weft-worker:ab"));
        assert_eq!(Install::tag_of_local_image("localhost/weft-cell7/weft-worker:ab"), Some("weft-worker:ab"));
        for other in ["localhost/other/weft-worker:ab", "ghcr.io/x/weft-worker:ab", "evil/weft-worker:ab", "weft-worker", "localhost/weft-Bad/weft-worker:ab"] {
            assert_eq!(Install::tag_of_local_image(other), None, "{other}");
        }
    }

    #[test]
    fn a_name_that_cannot_live_in_a_resource_name_is_refused() {
        for bad in ["Cell", "7cell", "cell-a", "abcdefghijklm", "cell_a", "", "default"] {
            assert!(Install::named(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn main_is_the_default_install_and_a_plain_branch_keeps_its_name() {
        assert_eq!(Install::for_branch("main"), Install::default_install());
        assert_eq!(Install::for_branch("deploy"), Install::named("deploy").unwrap());
        assert_eq!(Install::for_branch("fix2"), Install::named("fix2").unwrap());
    }

    #[test]
    fn any_other_branch_gets_a_valid_stable_name_of_its_own() {
        let journal = Install::for_branch("fix/journal-holes");
        let name = journal.name().unwrap();
        assert_eq!(name.len(), MAX_INSTALL_NAME);
        assert!(name.starts_with("fixjour"), "{name}");
        assert_eq!(Install::for_branch("fix/journal-holes"), journal, "the same branch, the same install");
        // Two branches that cut to the same letters still differ.
        assert_ne!(Install::for_branch("fix/journal-other"), journal);
        // Upper case is part of the name: `Deploy` is not `deploy`.
        assert_ne!(Install::for_branch("Deploy"), Install::for_branch("deploy"));
        for branch in ["e2e", "e2e-flaky", "e2ex", "default", "7fixes", "-", "Été", "a-very-long-branch-name-indeed", "main2", "MAIN"] {
            let install = Install::for_branch(branch);
            let name = install.name().unwrap_or_else(|| panic!("{branch} is not main"));
            assert!(Install::named(name).is_ok(), "{branch} -> {name}");
            assert!(!name.starts_with(E2E_CELL_PREFIX), "{branch} -> {name} looks like a test cell");
        }
    }

    #[test]
    fn a_binary_belongs_to_the_install_whose_bin_it_lives_in() {
        let data = std::path::Path::new("/home/q/.local/share/weft");
        assert_eq!(Install::of_bin_dir(&data.join("bin"), data), Some(Install::default_install()));
        assert_eq!(Install::of_bin_dir(&data.join("installs/deploy/bin"), data), Some(Install::named("deploy").unwrap()));
        for elsewhere in ["/home/q/weft/target/debug", "/home/q/.local/share/weft/installs/deploy", "/home/q/.local/share/weft/installs/deploy/bin/x", "/home/q/.local/share/weft/installs/Bad/bin", "/usr/bin"] {
            assert_eq!(Install::of_bin_dir(std::path::Path::new(elsewhere), data), None, "{elsewhere}");
        }
    }

    #[test]
    fn the_env_value_names_the_default_install_too() {
        assert_eq!(Install::from_env_value(None).unwrap(), Install::default_install());
        assert_eq!(Install::from_env_value(Some(" ")).unwrap(), Install::default_install());
        assert_eq!(Install::from_env_value(Some("default")).unwrap(), Install::default_install());
        assert_eq!(Install::from_env_value(Some("cell1")).unwrap(), Install::named("cell1").unwrap());
        for install in [Install::default_install(), Install::named("cell1").unwrap()] {
            assert_eq!(Install::from_env_value(Some(install.env_value())).unwrap(), install);
        }
    }

    #[test]
    fn where_a_command_runs_picks_its_install_unless_the_env_names_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("weft");
        for d in ["crates/weft-catalog/src", "catalog", ".git"] {
            std::fs::create_dir_all(root.join(d)).unwrap();
        }
        std::fs::write(root.join("Cargo.toml"), "").unwrap();
        std::fs::write(root.join("Cargo.lock"), "").unwrap();
        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/deploy\n").unwrap();
        let inside = root.join("crates/weft-catalog/src");

        let got = Install::wanted(None, &inside).unwrap();
        assert_eq!(got.install, Install::named("deploy").unwrap());
        assert_eq!(got.from, WantedFrom::Checkout { root: root.clone(), branch: "deploy".into() });

        let got = Install::wanted(Some("e2eab"), &inside).unwrap();
        assert_eq!((got.install, got.from), (Install::named("e2eab").unwrap(), WantedFrom::Env));
        let got = Install::wanted(Some("default"), &inside).unwrap();
        assert_eq!((got.install, got.from), (Install::default_install(), WantedFrom::Env));

        let got = Install::wanted(None, dir.path()).unwrap();
        assert_eq!((got.install, got.from), (Install::default_install(), WantedFrom::Outside));

        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
        assert_eq!(Install::wanted(None, &inside).unwrap().install, Install::default_install());

        std::fs::write(root.join(".git/HEAD"), "60bdaf09a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6\n").unwrap();
        let error = Install::wanted(None, &inside).unwrap_err();
        assert!(error.contains("detached HEAD") && error.contains(INSTALL_ENV), "{error}");
    }

    #[test]
    fn a_secret_is_read_from_its_own_line_and_a_missing_one_says_how_to_get_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = secrets_file(dir.path());
        assert!(secret_in(&path, "cell1", "WEFT_DATABASE_URL").unwrap_err().contains("never started"));
        std::fs::write(&path, "WEFT_DATABASE_LISTEN_URL=postgres://elsewhere\nWEFT_DATABASE_URL = postgres://weft@127.0.0.1:5433/weft\nEMPTY=\nTWICE=first\nTWICE=last\n").unwrap();
        assert_eq!(secret_in(&path, "cell1", "WEFT_DATABASE_URL").unwrap(), "postgres://weft@127.0.0.1:5433/weft");
        assert_eq!(secret_in(&path, "cell1", "TWICE").unwrap(), "last", "the last line naming it wins");
        assert!(secret_in(&path, "cell1", "EMPTY").unwrap_err().contains("names no EMPTY"));
        assert!(secret_in(&path, "cell1", "MISSING").unwrap_err().contains("./setup.sh --daemon"));
    }

    /// Where the front door could tell no install, a command that asks
    /// for one hears why, unless the variable names one. Sets the process
    /// environment: nextest runs each test in its own process.
    #[test]
    fn an_install_that_could_not_be_told_answers_why() {
        std::env::remove_var(INSTALL_ENV);
        assert_eq!(Install::from_env(), Ok(Install::default_install()));
        mark_unresolved("on no branch".into());
        assert_eq!(Install::from_env(), Err("on no branch".to_string()));
        std::env::set_var(INSTALL_ENV, "cell1");
        assert_eq!(Install::from_env(), Ok(Install::named("cell1").unwrap()));
        std::env::remove_var(INSTALL_ENV);
    }

    #[test]
    fn an_install_is_its_name_on_the_wire() {
        assert_eq!(serde_json::to_value(Install::default_install()).unwrap(), serde_json::Value::Null);
        assert_eq!(serde_json::to_value(Install::named("c1").unwrap()).unwrap(), serde_json::json!("c1"));
    }
}
