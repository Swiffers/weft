//! `weft deps-install [NodeType...] [--all]`: install on this machine what
//! nodes need from it (`weft_core::needs`): the programs they launch, what
//! their packages need to compile. With no node type, what the program in
//! this folder uses; with `--all`, every node of the project's catalog.
//! Only the needs this machine does not meet yet are installed, with the
//! system's own package manager, after the person agreed to the exact
//! command (`--yes` for a script, `--print` to only show it).

use anyhow::{Context, Result};

use weft_core::needs::{NodeNeed, NeedStage, System};
use weft_platform_local::LocalNeeds;
use weft_platform_traits::MachineNeeds;

use super::Ctx;

pub struct DepsInstall {
    pub node_types: Vec<String>,
    pub all: bool,
    pub stage: Option<NeedStage>,
    pub yes: bool,
    pub print: bool,
}

pub async fn run(ctx: Ctx, args: DepsInstall) -> Result<()> {
    let project = ctx.project()?;
    let wanted = needs_wanted(project, &args)?;
    let wanted: Vec<NodeNeed> = wanted.into_iter().filter(|row| args.stage.is_none_or(|stage| row.need.stage == stage)).collect();
    if wanted.is_empty() {
        println!("{}", nothing_needed(&args));
        return Ok(());
    }
    // Checked the way the install checks them before a program starts.
    let missing = LocalNeeds.missing(&wanted).await?;
    if missing.is_empty() {
        println!("everything these nodes need is on this machine already");
        return Ok(());
    }
    let system = System::here().map_err(anyhow::Error::msg)?;
    let plan = install_plan(&system, &missing);
    for row in &missing {
        println!("{} needs {}", row.node_type, row.need.name);
    }
    for row in &plan.unknown {
        eprintln!("{} needs {}, and names no package for this system: install it by hand", row.node_type, row.need.name);
    }
    if plan.packages.is_empty() {
        anyhow::bail!("nothing here can be installed for this system; install what is said above by hand, then run this again");
    }
    let command = system.install_command(&plan.packages);
    println!("to install them: {}", command.join(" "));
    if args.print {
        return Ok(());
    }
    if !args.yes && !crate::prompt::ask_yes_no("run it now? [Y/n] ", true, "--yes (or --print to only show it)")? {
        println!("nothing was installed");
        return Ok(());
    }
    let status = tokio::process::Command::new(&command[0])
        .args(&command[1..])
        .status()
        .await
        .with_context(|| format!("run `{}`", command.join(" ")))?;
    anyhow::ensure!(status.success(), "`{}` failed ({status}); nothing more was done", command.join(" "));
    // Only what the command installed is checked again: a need it had no
    // package for is still missing, and is said apart.
    let still = LocalNeeds.missing(&plan.covered).await?;
    if !still.is_empty() {
        anyhow::bail!(
            "installed, but this machine still does not meet: {}; check that what was installed is on your PATH, \
             then run ./setup.sh again from that shell: the install's workers get the PATH of the shell that last \
             started it",
            named(&still)
        );
    }
    if !plan.unknown.is_empty() {
        anyhow::bail!(
            "installed what this system has packages for; still to install by hand: {}; then run this again",
            named(&plan.unknown)
        );
    }
    println!("installed; every need these nodes have is met");
    Ok(())
}

/// The needs the command was asked about: the named node types', every
/// catalog type's with `--all`, else the program's (only the nodes it uses).
fn needs_wanted(project: &weft_compiler::project::Project, args: &DepsInstall) -> Result<Vec<NodeNeed>> {
    if args.all && !args.node_types.is_empty() {
        anyhow::bail!("--all installs what every node needs, so it takes no node type; drop one of them");
    }
    if args.node_types.is_empty() && !args.all {
        let (definition, catalog) = weft_compiler::hash::load_program(project).map_err(|e| anyhow::anyhow!("compile the program in this folder: {e}"))?;
        return weft_compiler::codegen::needs_of(&definition, &catalog).map_err(|e| anyhow::anyhow!("{e}"));
    }
    let roots = weft_compiler::project::node_roots(&project.root);
    let catalog = weft_catalog::FsCatalog::discover_roots(&roots.iter().map(|r| r.as_path()).collect::<Vec<_>>())
        .map_err(|e| anyhow::anyhow!("read the project's catalog: {e}"))?;
    let node_types: std::collections::BTreeSet<String> = if args.all {
        catalog.iter().map(|entry| entry.node_type.clone()).collect()
    } else {
        for node_type in &args.node_types {
            anyhow::ensure!(
                catalog.entry(node_type).is_some(),
                "no node type '{node_type}' in this project's catalog (`weft describe-nodes --list` lists them)"
            );
        }
        args.node_types.iter().cloned().collect()
    };
    let needs = weft_compiler::codegen::node_needs(&node_types, &catalog).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(needs
        .into_iter()
        .flat_map(|(node_type, needs)| needs.into_iter().map(move |need| NodeNeed { node_type: node_type.clone(), nodes: Vec::new(), need }))
        .collect())
}

fn nothing_needed(args: &DepsInstall) -> &'static str {
    match (args.all, args.node_types.is_empty()) {
        (true, _) => "no node of this project's catalog needs anything from the machine",
        (false, true) => "no node this program uses needs anything from the machine",
        (false, false) => "these nodes need nothing from the machine",
    }
}

/// What to install for `missing` on `system`: every package, once, the
/// needs those packages meet, and the needs that name none for it.
#[derive(Debug, PartialEq, Eq)]
struct InstallPlan {
    packages: Vec<String>,
    /// The needs `packages` are installed for, the ones checked again after.
    covered: Vec<NodeNeed>,
    /// The needs with no package for this system, left to install by hand.
    unknown: Vec<NodeNeed>,
}

fn install_plan(system: &System, missing: &[NodeNeed]) -> InstallPlan {
    let mut plan = InstallPlan { packages: Vec::new(), covered: Vec::new(), unknown: Vec::new() };
    for row in missing {
        match row.need.packages_for(system.manager, system.distro.as_deref()) {
            Some(packages) => {
                for package in packages {
                    if !plan.packages.contains(package) {
                        plan.packages.push(package.clone());
                    }
                }
                plan.covered.push(row.clone());
            }
            None => plan.unknown.push(row.clone()),
        }
    }
    plan
}

/// `need (node type)` for each of `rows`, comma-separated.
fn named(rows: &[NodeNeed]) -> String {
    rows.iter().map(|row| format!("{} ({})", row.need.name, row.node_type)).collect::<Vec<_>>().join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::needs::{Need, PackageManager};

    fn row(node_type: &str, name: &str, apt: &[&str]) -> NodeNeed {
        let mut install = std::collections::BTreeMap::new();
        if !apt.is_empty() {
            install.insert(PackageManager::Apt, [("default".to_string(), apt.iter().map(|p| p.to_string()).collect())].into());
        }
        NodeNeed { node_type: node_type.into(), nodes: Vec::new(), need: Need { name: name.into(), stage: NeedStage::Run, check: vec![name.into()], install } }
    }

    #[test]
    fn each_package_is_installed_once_and_a_need_with_none_is_named() {
        let apt = System { manager: PackageManager::Apt, distro: Some("debian_12".into()) };
        let (python, ffmpeg, odd) = (row("ExecPython", "python3", &["python3"]), row("Studio", "ffmpeg", &["ffmpeg", "python3"]), row("Odd", "thing", &[]));
        let plan = install_plan(&apt, &[python.clone(), ffmpeg.clone(), odd.clone()]);
        assert_eq!(plan.packages, ["python3", "ffmpeg"]);
        // Only the needs a package meets are checked again after the
        // install; the one with none is left to say apart.
        assert_eq!(plan.covered, [python, ffmpeg]);
        assert_eq!(plan.unknown, [odd]);
        assert_eq!(named(&plan.unknown), "thing (Odd)");
    }
}
