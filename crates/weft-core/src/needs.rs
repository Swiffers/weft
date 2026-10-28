//! What a node needs from the machine its worker runs on: a program it
//! launches (`python3`, `ffmpeg`), or something its package needs to
//! compile (a C library's headers). Declared in the node package's
//! `deps.toml` as `[[needs]]`, read by the catalog
//! (`weft_catalog::NodeDeps::needs`).
//!
//! One declaration serves every platform: a machine's install checks each
//! run need of the nodes a program uses and refuses before anything starts
//! when one is missing, naming `weft deps-install`; an image build installs
//! the same packages into the image. A node that finds one missing as it
//! runs fails saying so (`crate::error::need_missing`), with the same
//! words ([`missing_at_run`]).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One thing a node needs.
///
/// ```toml
/// [[needs]]
/// name  = "python3"
/// check = ["python3", "--version"]
/// [needs.install.apt]  default = ["python3"]
/// [needs.install.brew] default = ["python"]
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Need {
    /// What the person is told the node needs.
    pub name: String,
    #[serde(default)]
    pub stage: NeedStage,
    /// A command that runs and exits 0 when the need is met. Its first
    /// word is looked up on the PATH.
    pub check: Vec<String>,
    /// The packages that meet it, by package manager.
    #[serde(default)]
    pub install: BTreeMap<PackageManager, DistroPackages>,
}

/// When a need matters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NeedStage {
    /// While the worker runs: checked before a program that uses the node
    /// starts, installed into an image's runtime stage.
    #[default]
    Run,
    /// While the node's package compiles: installed into an image's
    /// builder stage, and by `weft deps-install` on a machine; a package
    /// whose build need is missing does not compile, and is left out.
    Build,
}

/// The package managers a need can name packages for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageManager {
    /// Debian, Ubuntu (and WSL on them).
    Apt,
    /// Fedora, RHEL, Rocky.
    Dnf,
    /// Alpine.
    Apk,
    /// macOS (Homebrew).
    Brew,
}

/// One manager's packages, by distro: `default`, or a `<distro>_<major>`
/// key (`debian_12`, `ubuntu_24_04`) for a distro whose package names
/// differ.
pub type DistroPackages = BTreeMap<String, Vec<String>>;

/// The distro key every manager may fill when the package names do not
/// change between distros.
pub const DEFAULT_DISTRO: &str = "default";

impl Need {
    /// The packages that meet this need on `manager` for `distro` (its
    /// `<distro>_<major>` key, when known): that distro's entry, else the
    /// manager's `default`, else none.
    pub fn packages_for(&self, manager: PackageManager, distro: Option<&str>) -> Option<&[String]> {
        let table = self.install.get(&manager)?;
        distro.and_then(|d| table.get(d)).or_else(|| table.get(DEFAULT_DISTRO)).map(Vec::as_slice)
    }
}

/// The longest a need's `check` may take before it counts as unmet: a
/// check asks a program its version, it never works.
pub const CHECK_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

impl Need {
    /// Whether this need is met on this machine: its `check` starts (its
    /// first word found on the PATH), and exits 0 within [`CHECK_LIMIT`].
    /// Blocks while the check runs.
    pub fn is_met(&self) -> bool {
        let Some((program, args)) = self.check.split_first() else { return false };
        let child = std::process::Command::new(program)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
        let Ok(mut child) = child else { return false };
        let started = std::time::Instant::now();
        loop {
            match child.try_wait() {
                Ok(Some(status)) => return status.success(),
                Ok(None) if started.elapsed() < CHECK_LIMIT => std::thread::sleep(std::time::Duration::from_millis(20)),
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return false;
                }
            }
        }
    }
}

/// The system a machine runs, as far as installing a need goes: its
/// package manager and its distro key (`debian_12`, `ubuntu_24_04`; `None`
/// on macOS, where Homebrew's names do not change).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct System {
    pub manager: PackageManager,
    pub distro: Option<String>,
}

impl System {
    /// This machine's: macOS is Homebrew's, a Linux reads `/etc/os-release`.
    /// `Err` names what was found when no supported manager fits.
    pub fn here() -> Result<Self, String> {
        if cfg!(target_os = "macos") {
            return Ok(Self { manager: PackageManager::Brew, distro: None });
        }
        let release = std::fs::read_to_string("/etc/os-release").map_err(|e| format!("cannot read /etc/os-release to tell which system this is: {e}"))?;
        Self::from_os_release(&release)
    }

    /// The system an `/etc/os-release` describes: its `ID`, else the first
    /// of its `ID_LIKE` that a manager serves, with `<ID>_<VERSION_ID>` (dots
    /// made underscores) as the distro key.
    pub fn from_os_release(release: &str) -> Result<Self, String> {
        let field = |name: &str| {
            release.lines().find_map(|line| line.strip_prefix(&format!("{name}="))).map(|v| v.trim().trim_matches('"').to_string())
        };
        let id = field("ID").unwrap_or_default();
        let like = field("ID_LIKE").unwrap_or_default();
        let manager_of = |name: &str| match name {
            "debian" | "ubuntu" => Some(PackageManager::Apt),
            "fedora" | "rhel" | "centos" | "rocky" | "almalinux" => Some(PackageManager::Dnf),
            "alpine" => Some(PackageManager::Apk),
            _ => None,
        };
        let manager = std::iter::once(id.as_str()).chain(like.split_whitespace()).find_map(manager_of).ok_or_else(|| {
            format!("this system ({}) has no package manager weft installs with (apt, dnf, apk, brew)", if id.is_empty() { "unknown" } else { &id })
        })?;
        let distro = field("VERSION_ID").filter(|_| !id.is_empty()).map(|version| format!("{id}_{}", version.replace('.', "_")));
        Ok(Self { manager, distro })
    }

    /// The command that installs `packages` here, run as the person (who
    /// is asked for their password by `sudo` when the manager needs it).
    pub fn install_command(&self, packages: &[String]) -> Vec<String> {
        let mut command: Vec<String> = match self.manager {
            PackageManager::Apt => ["sudo", "apt-get", "install", "-y"].map(String::from).to_vec(),
            PackageManager::Dnf => ["sudo", "dnf", "install", "-y"].map(String::from).to_vec(),
            PackageManager::Apk => ["sudo", "apk", "add"].map(String::from).to_vec(),
            PackageManager::Brew => ["brew", "install"].map(String::from).to_vec(),
        };
        command.extend(packages.iter().cloned());
        command
    }
}

/// One need of a node type, with the program's nodes of that type, met
/// or not: what a machine is asked about ([`program_needs`]), and what the
/// refusal and `weft deps-install` say of the ones it lacks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeNeed {
    /// The node type whose package declares it.
    pub node_type: String,
    /// The program's nodes of that type, as the program spells them.
    pub nodes: Vec<String>,
    pub need: Need,
}

/// What each node type needs, by node type: a worker's fact (its
/// packages' `deps.toml`), kept with it, which the program it runs is read
/// against ([`program_needs`]).
pub type NodeNeeds = BTreeMap<String, Vec<Need>>;

/// The needs of the nodes `definition` uses, from `node_needs` (a node
/// type it does not use is never asked): one row per need, by node type
/// then need, with the program's nodes of that type, sorted.
pub fn program_needs(definition: &crate::ProjectDefinition, node_needs: &NodeNeeds) -> Vec<NodeNeed> {
    let mut nodes_by_type: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for node in &definition.nodes {
        if node_needs.contains_key(&node.node_type) {
            nodes_by_type.entry(&node.node_type).or_default().push(node.id.clone());
        }
    }
    let mut rows = Vec::new();
    for (node_type, mut nodes) in nodes_by_type {
        nodes.sort();
        let mut needs = node_needs[node_type].clone();
        needs.sort_by(|a, b| (a.stage, &a.name).cmp(&(b.stage, &b.name)));
        rows.extend(needs.into_iter().map(|need| NodeNeed { node_type: node_type.to_string(), nodes: nodes.clone(), need }));
    }
    rows
}

/// The refusal of a program whose nodes need what the machine its workers
/// run on lacks: each node type with its nodes and what it needs, then
/// the way to meet them where the workers get them (`from`).
pub fn refusal(missing: &[NodeNeed], from: NeedsFrom) -> String {
    let mut lines = vec![MISSING_HEADER.to_string()];
    let mut types: Vec<&str> = Vec::new();
    for row in missing {
        lines.push(need_line(&row.node_type, &row.nodes, &row.need.name));
        if !types.contains(&row.node_type.as_str()) {
            types.push(&row.node_type);
        }
    }
    lines.push(from.remedy(&types));
    lines.join("\n")
}

/// What a node whose need `need` is missing as it runs fails with
/// (`crate::error::WeftError::NeedMissing`): the same words as the
/// [`refusal`] before a program starts, for the one node, with the way to
/// meet it where its worker runs.
pub fn missing_at_run(node_type: &str, node: &str, need: &str, from: NeedsFrom) -> String {
    [MISSING_HEADER.to_string(), need_line(node_type, &[node.to_string()], need), from.remedy(&[node_type])].join("\n")
}

const MISSING_HEADER: &str = "this program uses nodes that need things the machine its workers run on doesn't have:";

fn need_line(node_type: &str, nodes: &[String], need: &str) -> String {
    let nodes = nodes.iter().map(|n| format!("'{}'", crate::project::plain_id(n))).collect::<Vec<_>>().join(", ");
    format!("  {node_type} (node {nodes}) needs {need}")
}

/// Where an install's workers get what their nodes need, which decides how
/// a missing need is met: the machine they run on, or the image they run
/// from. Every platform says which to each worker it starts
/// ([`NEEDS_FROM_ENV`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeedsFrom {
    /// A local install: its workers are processes on the person's machine,
    /// which `weft deps-install` installs a need on.
    Machine,
    /// A cloud install: a worker has only what its image installs, from
    /// the packages each need names for the image's package manager.
    Image,
}

/// The variable a platform sets on every worker it starts, naming
/// [`NeedsFrom::env_value`].
pub const NEEDS_FROM_ENV: &str = "WEFT_NEEDS_FROM";

impl NeedsFrom {
    pub fn env_value(self) -> &'static str {
        match self {
            Self::Machine => "machine",
            Self::Image => "image",
        }
    }

    /// The one [`NeedsFrom::env_value`] spells `value`.
    pub fn from_env_value(value: &str) -> Result<Self, String> {
        match value.trim() {
            "machine" => Ok(Self::Machine),
            "image" => Ok(Self::Image),
            other => Err(format!("{NEEDS_FROM_ENV} is '{other}'; it is `machine` or `image`")),
        }
    }

    /// The way to meet what the node types `node_types` need and lack
    /// here: the last line of a [`refusal`] and of [`missing_at_run`].
    pub fn remedy(self, node_types: &[&str]) -> String {
        match self {
            Self::Machine => format!("install them with:  weft deps-install {}", node_types.join(" ")),
            Self::Image => format!(
                "a cloud worker has only what its image installs, and the image installs a need from the packages it names \
                 for the project's base image: give each one an entry for that image's package manager \
                 (`[needs.install.apt]` for the default Debian image) in the deps.toml of {}; a project with its own \
                 `[build.worker] dockerfile_template` also needs the template to keep the `{{{{install_runtime_system_packages}}}}` \
                 line, where those packages are installed. Then build again",
                node_types.join(", ")
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_need_reads_from_toml_and_picks_its_distro_then_the_default() {
        let need: Need = toml::from_str(
            r#"
            name = "python3"
            check = ["python3", "--version"]
            [install.apt]
            default = ["python3"]
            debian_11 = ["python3.9"]
            [install.brew]
            default = ["python"]
            "#,
        )
        .unwrap();
        assert_eq!(need.stage, NeedStage::Run);
        assert_eq!(need.packages_for(PackageManager::Apt, Some("debian_11")), Some(&["python3.9".to_string()][..]));
        assert_eq!(need.packages_for(PackageManager::Apt, Some("ubuntu_24_04")), Some(&["python3".to_string()][..]));
        assert_eq!(need.packages_for(PackageManager::Brew, None), Some(&["python".to_string()][..]));
        assert_eq!(need.packages_for(PackageManager::Dnf, None), None);
    }

    #[test]
    fn a_need_is_met_when_its_check_exits_zero() {
        let need = |check: &[&str]| Need { name: "x".into(), stage: NeedStage::Run, check: check.iter().map(|s| s.to_string()).collect(), install: BTreeMap::new() };
        assert!(need(&["true"]).is_met());
        assert!(!need(&["false"]).is_met());
        assert!(!need(&["weft-no-such-program-anywhere"]).is_met());
        assert!(!need(&[]).is_met());
    }

    #[test]
    fn a_system_is_read_from_its_os_release() {
        let ubuntu = "NAME=\"Ubuntu\"\nID=ubuntu\nID_LIKE=debian\nVERSION_ID=\"24.04\"\n";
        assert_eq!(System::from_os_release(ubuntu), Ok(System { manager: PackageManager::Apt, distro: Some("ubuntu_24_04".into()) }));
        let mint = "ID=linuxmint\nID_LIKE=\"ubuntu debian\"\nVERSION_ID=\"22\"\n";
        assert_eq!(System::from_os_release(mint).unwrap().manager, PackageManager::Apt, "known through ID_LIKE");
        assert_eq!(System::from_os_release("ID=fedora\nVERSION_ID=41\n").unwrap().manager, PackageManager::Dnf);
        assert!(System::from_os_release("ID=arch\n").unwrap_err().contains("arch"));
    }

    #[test]
    fn the_install_command_is_the_managers_own() {
        let apt = System { manager: PackageManager::Apt, distro: None };
        assert_eq!(apt.install_command(&["python3".into(), "ffmpeg".into()]), ["sudo", "apt-get", "install", "-y", "python3", "ffmpeg"]);
        let brew = System { manager: PackageManager::Brew, distro: None };
        assert_eq!(brew.install_command(&["python".into()]), ["brew", "install", "python"]);
    }

    #[test]
    fn a_program_is_asked_only_about_the_node_types_it_uses() {
        let definition: crate::ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000001",
            "nodes": [
                { "id": "greet", "nodeType": "ExecPython", "inputs": [], "outputs": [], "position": { "x": 0, "y": 0 } },
                { "id": "again", "nodeType": "ExecPython", "inputs": [], "outputs": [], "position": { "x": 0, "y": 0 } },
                { "id": "say", "nodeType": "Text", "inputs": [], "outputs": [], "position": { "x": 0, "y": 0 } }
            ],
            "edges": []
        }))
        .unwrap();
        let python = Need { name: "python3".into(), stage: NeedStage::Run, check: vec!["python3".into()], install: BTreeMap::new() };
        let ffmpeg = Need { name: "ffmpeg".into(), ..python.clone() };
        let node_needs: NodeNeeds = [("ExecPython".to_string(), vec![python.clone()]), ("Studio".to_string(), vec![ffmpeg])].into();
        let rows = program_needs(&definition, &node_needs);
        assert_eq!(rows, [NodeNeed { node_type: "ExecPython".into(), nodes: vec!["again".into(), "greet".into()], need: python }], "Studio is not used");
        assert_eq!(
            refusal(&rows, NeedsFrom::Machine),
            "this program uses nodes that need things the machine its workers run on doesn't have:\n  ExecPython (node 'again', 'greet') needs python3\ninstall them with:  weft deps-install ExecPython"
        );
    }

    #[test]
    fn a_need_missing_as_its_node_runs_is_said_with_the_platforms_remedy() {
        assert_eq!(
            missing_at_run("ExecPython", "greet", "python3", NeedsFrom::Machine),
            "this program uses nodes that need things the machine its workers run on doesn't have:\n  ExecPython (node 'greet') needs python3\ninstall them with:  weft deps-install ExecPython"
        );
        assert!(missing_at_run("ExecPython", "greet", "python3", NeedsFrom::Image).contains("in the deps.toml of ExecPython;"));
        for from in [NeedsFrom::Machine, NeedsFrom::Image] {
            assert_eq!(NeedsFrom::from_env_value(from.env_value()), Ok(from));
        }
        assert!(NeedsFrom::from_env_value("cloud").unwrap_err().contains(NEEDS_FROM_ENV));
    }

    #[test]
    fn an_unknown_key_is_refused() {
        assert!(toml::from_str::<Need>("name = \"x\"\ncheck = [\"x\"]\nchek = 1").is_err());
    }
}
