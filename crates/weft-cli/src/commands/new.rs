//! `weft new <name>`: scaffold a new project directory with
//! weft.toml, src/main.weft, nodes/, .weft/, and an initialized git
//! repo, plus the Tangle assistant persona for the AI coding assistant
//! it will be opened in, copied out of the weft checkout (see
//! `AssistantSpec`).
//!
//! Which assistant comes from `--assistant`, else the remembered default
//! (`--remember`), else the user is asked to pick from a list, the ones
//! found on this machine first. With nobody to ask (no terminal, or
//! `--json`) the command refuses, naming the values: a project without
//! Tangle is only ever made by asking for it (`--assistant none`).
//! Remembering is never a side effect of a flag, so a scratch project
//! made by an agent never changes what the user picked.
//! `--assistant agents` is the fallback for an assistant weft has no
//! template for. With `--ci <cloud>`, also the deploy workflow
//! `weft ci add` writes.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Context;

use super::Ctx;

/// One supported AI coding assistant: which template directory under
/// `tangle/` in the weft checkout holds its persona, the shorthand the
/// CLI accepts for it, which entries land in the project under which
/// name, how to tell it is on this machine, and how to open a project in
/// it. Adding an assistant is one more row here (plus the template
/// directory itself); nothing else in `weft new` is assistant-specific.
#[derive(Debug)]
pub struct AssistantSpec {
    /// Directory under `tangle/` in the weft checkout.
    pub dir: &'static str,
    /// Shorthand accepted as the `--assistant` value (`cc` for
    /// `claude-code`, `kc` for `kilo-code`).
    pub shorthand: &'static str,
    /// The assistant's own name, as the picker shows it.
    pub name: &'static str,
    /// `(name in the project, path inside the template dir)` pairs, each
    /// copied into the project.
    pub links: &'static [(&'static str, &'static str)],
    /// Signs that the assistant is installed here; any one is enough.
    /// Empty when weft knows no honest sign, so the row is never shown as
    /// found (it stays pickable).
    pub found_by: &'static [Sign],
    /// How the user opens the new project in it.
    pub open: Opener,
}

/// One cheap check that an assistant is installed on this machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sign {
    /// An executable of this name on `PATH`.
    Program(&'static str),
    /// A VS Code-style extension, by its lowercase `publisher.name` id,
    /// in one of the editors' extension folders ([`EXTENSION_DIRS`]).
    Extension(&'static str),
}

/// How a project is opened in an assistant, which is what `weft new`
/// prints last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opener {
    /// An editor command that takes the folder and a file to show: the
    /// project opens with `src/main.weft`, so the weft extension draws
    /// the graph.
    Editor(&'static str),
    /// A program run from inside the project folder.
    Terminal(&'static str),
    /// An app with no command weft knows; the user opens the folder in it.
    App(&'static str),
}

/// Every assistant weft ships a Tangle template for. Each row's `links` are
/// the paths that assistant actually reads, which is why no two rows look
/// alike: the persona is a rules file in one and a plain `AGENTS.md` in the
/// next, and the specialists are TOML here and markdown there. The template
/// directories are independent copies on purpose (see `tangle/README.md`):
/// a wording that works better on one assistant belongs only in that
/// assistant's copy.
pub const ASSISTANTS: &[AssistantSpec] = &[
    AssistantSpec {
        dir: "claude-code",
        shorthand: "cc",
        name: "Claude Code",
        links: &[("CLAUDE.md", "CLAUDE.md"), (".claude", ".claude")],
        found_by: &[Sign::Program("claude"), Sign::Extension("anthropic.claude-code")],
        open: Opener::Terminal("claude"),
    },
    AssistantSpec {
        dir: "kilo-code",
        shorthand: "kc",
        name: "Kilo Code",
        links: &[("kilo.json", "kilo.json"), (".kilo", ".kilo")],
        found_by: &[Sign::Extension("kilocode.kilo-code"), Sign::Program("kilo")],
        open: Opener::Editor("code"),
    },
    AssistantSpec {
        dir: "cursor",
        shorthand: "cu",
        name: "Cursor",
        links: &[(".cursor", ".cursor")],
        found_by: &[Sign::Program("cursor"), Sign::Program("cursor-agent")],
        open: Opener::Editor("cursor"),
    },
    AssistantSpec {
        dir: "codex",
        shorthand: "cx",
        name: "OpenAI Codex",
        links: &[
            ("AGENTS.md", "AGENTS.md"),
            (".agents", ".agents"),
            (".codex", ".codex"),
        ],
        found_by: &[Sign::Program("codex"), Sign::Extension("openai.chatgpt")],
        open: Opener::Terminal("codex"),
    },
    AssistantSpec {
        dir: "github-copilot",
        shorthand: "gh",
        name: "GitHub Copilot",
        links: &[(".github", ".github")],
        found_by: &[
            Sign::Extension("github.copilot-chat"),
            Sign::Extension("github.copilot"),
            Sign::Program("copilot"),
        ],
        open: Opener::Editor("code"),
    },
    AssistantSpec {
        dir: "gemini-cli",
        shorthand: "gc",
        name: "Gemini CLI",
        links: &[("GEMINI.md", "GEMINI.md"), (".gemini", ".gemini")],
        found_by: &[Sign::Program("gemini")],
        open: Opener::Terminal("gemini"),
    },
    AssistantSpec {
        dir: "cline",
        shorthand: "cl",
        name: "Cline",
        links: &[(".clinerules", ".clinerules"), (".cline", ".cline")],
        found_by: &[Sign::Extension("saoudrizwan.claude-dev"), Sign::Program("cline")],
        open: Opener::Editor("code"),
    },
    AssistantSpec {
        dir: "opencode",
        shorthand: "oc",
        name: "OpenCode",
        links: &[
            ("AGENTS.md", "AGENTS.md"),
            ("opencode.json", "opencode.json"),
            (".opencode", ".opencode"),
        ],
        found_by: &[Sign::Program("opencode")],
        open: Opener::Terminal("opencode"),
    },
    AssistantSpec {
        dir: "devin-desktop",
        shorthand: "dd",
        name: "Devin Desktop",
        links: &[(".devin", ".devin"), (".windsurf", ".windsurf")],
        // Renamed from Windsurf in June 2026; weft knows neither the
        // command it installs now nor where it keeps extensions.
        found_by: &[],
        open: Opener::App("Devin Desktop"),
    },
    AssistantSpec {
        dir: "junie",
        shorthand: "ju",
        name: "JetBrains Junie",
        links: &[(".junie", ".junie")],
        found_by: &[Sign::Program("junie")],
        open: Opener::Terminal("junie"),
    },
];

/// The template for an assistant weft has no row for. It is a plain
/// `AGENTS.md` plus the skills beside it, the two shapes almost every
/// assistant reads, so an unknown one still gets Tangle.
///
/// It installs ONLY when nothing else did, and that restraint is the whole
/// design. Cursor, Cline and Gemini each merge a root `AGENTS.md` with their
/// own persona file rather than treating it as an alternative, so shipping
/// both would put Tangle in the context twice, at double the tokens, with
/// two copies free to disagree.
pub const FALLBACK: AssistantSpec = AssistantSpec {
    dir: "fallback",
    shorthand: "agents",
    name: "another assistant (a plain AGENTS.md)",
    links: &[("AGENTS.md", "AGENTS.md"), (".agents", ".agents")],
    found_by: &[],
    open: Opener::App("your assistant"),
};

/// The extension folders of the VS Code family, under `HOME`: the desktop
/// editor, its server on a remote or WSL machine, and Cursor's two.
const EXTENSION_DIRS: &[&str] = &[
    ".vscode/extensions",
    ".vscode-server/extensions",
    ".vscode-insiders/extensions",
    ".cursor/extensions",
    ".cursor-server/extensions",
];

/// The `--assistant` values, as every message that names them lists them.
pub fn known_assistants() -> String {
    ASSISTANTS
        .iter()
        .map(|a| format!("{} ({})", a.dir, a.shorthand))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether `sign` holds on this machine. Unsure is no: a folder that
/// cannot be read, or `HOME` unset, finds nothing.
fn holds_here(sign: &Sign) -> bool {
    match sign {
        Sign::Program(program) => std::env::var_os("PATH").is_some_and(|path| {
            std::env::split_paths(&path).any(|dir| is_executable(&dir.join(program)))
        }),
        Sign::Extension(id) => {
            let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
                return false;
            };
            // Extension folders are named `<publisher.name>-<version>`.
            let prefix = format!("{id}-");
            EXTENSION_DIRS.iter().any(|dir| {
                std::fs::read_dir(home.join(dir)).is_ok_and(|entries| {
                    entries.flatten().any(|entry| {
                        entry.file_name().to_string_lossy().to_lowercase().starts_with(&prefix)
                    })
                })
            })
        }
    }
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

/// Every assistant the picker offers, each with whether it was found
/// here: the found ones first, the rest after, both in table order, and
/// the fallback last (it is never found, it is for what is not listed).
fn picker_rows(holds: impl Fn(&Sign) -> bool) -> Vec<(&'static AssistantSpec, bool)> {
    let found = |spec: &AssistantSpec| spec.found_by.iter().any(&holds);
    let mut rows: Vec<(&'static AssistantSpec, bool)> =
        ASSISTANTS.iter().map(|spec| (spec, found(spec))).collect();
    rows.sort_by_key(|(_, found)| !found);
    rows.push((&FALLBACK, false));
    rows
}

/// The picker's numbered list, one line per row.
fn picker_lines(rows: &[(&AssistantSpec, bool)]) -> Vec<String> {
    let width = rows.len().to_string().len();
    rows.iter()
        .enumerate()
        .map(|(i, (spec, found))| {
            let mark = if *found { ", found on this machine" } else { "" };
            format!("  {:>width$}. {} ({}{mark})", i + 1, spec.name, spec.shorthand)
        })
        .collect()
}

/// Ask which assistants to set Tangle up for, then whether to remember
/// the answer (yes unless told otherwise). Only called with a human at
/// the terminal.
fn ask_for_assistants() -> anyhow::Result<(Vec<&'static AssistantSpec>, bool)> {
    let rows = picker_rows(holds_here);
    println!("Which AI assistant will you open this project in? weft sets up Tangle, its builder persona, for it.");
    for line in picker_lines(&rows) {
        println!("{line}");
    }
    let chosen = loop {
        let picked = crate::prompt::pick_numbers(
            "Pick a number, or several separated by spaces (--assistant none makes a project without Tangle): ",
            rows.len(),
            "--assistant <name>",
        )?;
        let dirs: Vec<String> = picked.iter().map(|&i| rows[i].0.dir.to_string()).collect();
        match resolve_assistants(&dirs) {
            Ok(chosen) => break chosen,
            Err(why) => eprintln!("{why}"),
        }
    };
    let remember =
        crate::prompt::ask_yes_no("Use this for your future projects too? [Y/n] ", true, "--remember")?;
    Ok((chosen, remember))
}

/// Why `weft new` refuses when nothing chose an assistant and nobody can
/// be asked: the values that choose one, and the explicit way to have none.
fn no_assistant_error() -> anyhow::Error {
    anyhow::anyhow!(
        "no assistant chosen, and nobody to ask (no terminal, or --json). \
         Pass --assistant <name> (repeatable) for the AI assistant you will open the project in: \
         {}, or {} for one not on that list. \
         Pass --assistant none to create the project without Tangle.",
        known_assistants(),
        FALLBACK.shorthand
    )
}

/// `word` as one shell argument: as it is when it holds nothing a shell
/// reads specially, else single-quoted.
fn shell_word(word: &str) -> String {
    let plain = !word.is_empty()
        && word.chars().all(|c| c.is_ascii_alphanumeric() || "-_./+@%:=,".contains(c));
    if plain {
        word.to_string()
    } else {
        format!("'{}'", word.replace('\'', r"'\''"))
    }
}

/// What `weft new` prints last: how to open the project at `folder` in
/// each assistant, and, when none of them is an editor, how to see the
/// graph.
fn open_lines(installed: &[&AssistantSpec], folder: &str) -> Vec<String> {
    let main = shell_word(&format!("{folder}/src/main.weft"));
    let folder = shell_word(folder);
    let in_editor = |editor: &str| format!("{editor} {folder} {main}");
    let mut lines: Vec<String> = installed
        .iter()
        .map(|spec| match spec.open {
            Opener::Editor(editor) => format!("open it in {}: {}", spec.name, in_editor(editor)),
            Opener::Terminal(program) => format!("open it in {}: cd {folder} && {program}", spec.name),
            Opener::App(app) => format!("open it: open the folder {folder} in {app}, then src/main.weft"),
        })
        .collect();
    if !installed.iter().any(|spec| matches!(spec.open, Opener::Editor(_))) {
        lines.push(format!(
            "see the graph: {} (VS Code, with the weft extension)",
            in_editor("code")
        ));
    }
    lines
}

/// Where the remembered default assistants live: one canonical assistant
/// dir name per line. Absent or empty means "no default", and `weft new`
/// asks. Sits beside the recorded `repo-root` in `~/.local/share/weft/`,
/// same pattern.
// SYNC: default-assistants <-> scripts/scrub-old-install.sh (keep)
fn assistants_record_path() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|h| {
        PathBuf::from(h)
            .join(".local/share/weft")
            .join("default-assistants")
    })
}

/// Find the row `value` names, by dir or shorthand.
fn assistant_named(value: &str) -> Option<&'static AssistantSpec> {
    std::iter::once(&FALLBACK)
        .chain(ASSISTANTS.iter())
        .find(|a| a.dir == value || a.shorthand == value)
}

/// Resolve `--assistant` values the user passed right now. Strict: an
/// unknown name fails the whole command, because a typo'd
/// `--assistant cluade-code` that silently produced a Tangle-less project
/// is the bad outcome.
pub fn resolve_assistants(values: &[String]) -> anyhow::Result<Vec<&'static AssistantSpec>> {
    let mut resolved: Vec<&'static AssistantSpec> = Vec::new();
    for value in values {
        let Some(spec) = assistant_named(value) else {
            anyhow::bail!(
                "unknown assistant '{}'; known assistants: {}, \
                 or '{}' for an assistant not on that list",
                value,
                known_assistants(),
                FALLBACK.shorthand
            );
        };
        if !resolved.iter().any(|r| r.dir == spec.dir) {
            resolved.push(spec);
        }
    }
    // The fallback is a fallback: it ships the persona as a root `AGENTS.md`,
    // and the assistants that have their own row read that file IN ADDITION
    // to their own persona rather than instead of it. Installing both would
    // load Tangle twice.
    if resolved.len() > 1 && resolved.iter().any(|a| a.dir == FALLBACK.dir) {
        let named: Vec<&str> = resolved
            .iter()
            .filter(|a| a.dir != FALLBACK.dir)
            .map(|a| a.dir)
            .collect();
        anyhow::bail!(
            "'{}' is for an assistant weft has no template for, so it cannot be \
             installed alongside {}: those read a root AGENTS.md on top of their \
             own persona, and Tangle would load twice. Pick one.",
            FALLBACK.shorthand,
            named.join(" and ")
        );
    }
    Ok(resolved)
}

/// Resolve the remembered defaults. Lenient: a name this weft does not
/// know (a record from another machine or a future binary) is warned
/// about and skipped, not fatal, and the rest still installs. A record
/// left with nothing known is no default at all, so `weft new` asks.
fn recorded_assistants() -> Vec<&'static AssistantSpec> {
    let Some(path) = assistants_record_path() else {
        return Vec::new();
    };
    let Ok(record) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let mut resolved: Vec<&'static AssistantSpec> = Vec::new();
    for value in record.split_whitespace() {
        match assistant_named(value) {
            Some(spec) => {
                if !resolved.iter().any(|r| r.dir == spec.dir) {
                    resolved.push(spec);
                }
            }
            None => {
                eprintln!(
                    "warning: remembered assistant '{}' is unknown to this weft; \
                     pass --assistant <name> to choose ({} has the record)",
                    value,
                    path.display()
                );
            }
        }
    }
    resolved
}

/// Record the assistants to install by default in future `weft new` runs,
/// one canonical dir name per line. An empty list removes the record:
/// with no default, the next `weft new` asks.
fn write_recorded_assistants(path: &Path, installed: &[&AssistantSpec]) -> anyhow::Result<()> {
    if installed.is_empty() {
        return match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
        };
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("create weft data dir")?;
    }
    let body: String = installed.iter().map(|spec| format!("{}\n", spec.dir)).collect();
    std::fs::write(path, body).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

/// Where this run's assistants came from, which decides what `weft new`
/// says about them.
enum Choice {
    /// `--assistant none`.
    NoTangle,
    /// `--assistant <name>`.
    Flag,
    /// The remembered default.
    Remembered,
    /// Picked in the terminal just now.
    Picked,
}

pub async fn run(
    ctx: Ctx,
    name: String,
    assistants: Vec<String>,
    remember: bool,
    ci: Option<super::ci::Cloud>,
) -> anyhow::Result<()> {
    if name.is_empty() {
        anyhow::bail!("project name cannot be empty");
    }
    let root = PathBuf::from(&name);
    if root.exists() {
        anyhow::bail!("{} already exists", root.display());
    }

    // `--assistant` picks for this project alone; `--remember` also makes
    // that pick the default (`none` clears it). No flag uses the default,
    // and with no default a person is asked; a script or an agent, which
    // cannot be, is refused, so no project ends up without Tangle unless
    // somebody said `none`. Remembering is never a side effect of a flag,
    // because agents make scratch projects with whatever flag suits the
    // moment. (clap refuses `--remember` without an `--assistant`.)
    let (installed, choice, remember) = if assistants.iter().any(|a| a == "none") {
        if assistants.len() > 1 {
            anyhow::bail!("--assistant none means no Tangle, so it cannot be given with another --assistant");
        }
        (Vec::new(), Choice::NoTangle, remember)
    } else if !assistants.is_empty() {
        (resolve_assistants(&assistants)?, Choice::Flag, remember)
    } else {
        let recorded = recorded_assistants();
        if !recorded.is_empty() {
            (recorded, Choice::Remembered, false)
        } else if ctx.json() || !crate::prompt::is_interactive() {
            return Err(no_assistant_error());
        } else {
            let (picked, remember) = ask_for_assistants()?;
            (picked, Choice::Picked, remember)
        }
    };

    // Known before anything is written, so a `--remember` with nowhere to
    // keep it fails with no project made.
    let record = if remember {
        Some(assistants_record_path().context(
            "HOME is unset, so there is nowhere to remember the assistant; drop --remember",
        )?)
    } else {
        None
    };

    // Every file of the new project is written into a staging directory
    // beside the target, and the target appears only once all of it
    // worked: a failure halfway (the stdlib seed, a Tangle template)
    // used to leave a folder with a `weft.toml` and no catalog, which
    // later failed as "unknown node type" on the first run.
    let project = assemble_atomically(&root, |staging| {
        let project = weft_compiler::project::Project::init(staging, &name)
            .map_err(|e| anyhow::anyhow!("init: {e}"))?;

        // Tangle is copied in, so its files are ordinary project files and
        // belong in the repo: clone the project on another machine and the
        // persona is there, with no weft checkout to point at.
        // `.env` auto-loads next to a project and is where a key the
        // project MINTS lands (`weft connect --set-env`), so it holds
        // secrets by the time anybody would commit it. `.scratch/` is
        // where Tangle's specialists keep their throwaway files (a probe, a
        // screenshot, a prompt trial's inputs): inside the project, so
        // writing there never needs a permission the specialist cannot
        // ask for, and hidden, so no version ever records it.
        let gitignore = String::from("target/\n.weft/\nnode_modules/\n.env\n.scratch/\n");

        // Initialize git. Best-effort: skip quietly if git is missing.
        let git_init = Command::new("git")
            .arg("init")
            .arg("--quiet")
            .current_dir(staging)
            .status();
        match git_init {
            Ok(status) if status.success() => {
                std::fs::write(staging.join(".gitignore"), gitignore).context("write .gitignore")?;
            }
            Ok(_) | Err(_) => {
                // git missing or `git init` failed. Not fatal: user can
                // opt into git later.
            }
        }

        install_tangle(staging, &installed)?;
        if let Some(cloud) = ci {
            super::ci::write_workflow(staging, &name, cloud)?;
        }
        Ok(project)
    })?;

    if let Some(record) = &record {
        write_recorded_assistants(record, &installed)?;
    }

    println!(
        "created project {} (id {}) at {}",
        project.manifest.package.name,
        project.id(),
        root.display()
    );
    let shown = installed.iter().map(|s| s.dir).collect::<Vec<_>>().join(", ");
    match (choice, remember) {
        (Choice::NoTangle, true) => println!(
            "no tangle in this project, and no default any more: the next weft new asks which assistant"
        ),
        (Choice::NoTangle, false) => println!("no tangle in this project; your default is unchanged"),
        (Choice::Remembered, _) => println!(
            "tangle ({shown}) installed, your default (--assistant <name> --remember changes it, \
             --assistant none --remember clears it)"
        ),
        (Choice::Flag | Choice::Picked, true) => {
            println!("tangle ({shown}) installed, and remembered as the default for future projects")
        }
        (Choice::Flag, false) => println!(
            "tangle ({shown}) installed (add --remember to make it the default for future projects)"
        ),
        (Choice::Picked, false) => println!("tangle ({shown}) installed for this project only"),
    }
    if installed.is_empty() {
        println!("next: cd {} && weft run", shell_word(&name));
    } else {
        for line in open_lines(&installed, &name) {
            println!("{line}");
        }
    }
    Ok(())
}

/// Build a new directory at `root` all at once: `build` fills a staging
/// directory created beside `root` (same parent, so the final move is a
/// rename on one filesystem, never a copy), and the staging directory
/// becomes `root` only when `build` succeeded. On failure the staging
/// directory is removed and `root` is left exactly as it was.
fn assemble_atomically<T>(
    root: &Path,
    build: impl FnOnce(&Path) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let parent = match root.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    std::fs::create_dir_all(&parent).with_context(|| format!("create {}", parent.display()))?;
    // Dropped on any early return, which deletes it with everything
    // `build` wrote.
    let staging = tempfile::Builder::new()
        .prefix(".weft-new-")
        .tempdir_in(&parent)
        .with_context(|| format!("create a staging directory in {}", parent.display()))?;
    let value = build(staging.path())?;
    // Checked again right before the move: on Linux a rename onto an
    // EMPTY directory replaces it silently, so a folder somebody made
    // while this ran would be swallowed rather than refused.
    if root.exists() {
        anyhow::bail!("{} appeared while the project was being created; nothing was written to it", root.display());
    }
    let staged = staging.keep();
    std::fs::rename(&staged, root).with_context(|| {
        // The rename failing leaves the staging directory as the only
        // copy of the work; it is removed so nothing half-named stays.
        let _ = std::fs::remove_dir_all(&staged);
        format!("move the new project into {}", root.display())
    })?;
    Ok(value)
}

/// The one file every Tangle template puts inside the assistant's own
/// directory. A project HAS Tangle for an assistant when this file is
/// there under one of that assistant's names, which is how
/// `weft tangle update` knows what to refresh: nothing but an install
/// writes it, so a `.github/` an assistant made for itself is not
/// mistaken for a persona.
const WITNESS: &str = "skills/weft-running/SKILL.md";

/// Names an assistant writes into its own directory that are not ours
/// to ship: dependency trees, lockfiles and scratch state that the
/// checkout picks up from being used as a project itself, and the
/// person's own settings (Claude Code's `settings.local.json`, which
/// Tangle tells the user survives an update). The templates track none
/// of these.
const TEMPLATE_EXCLUDE: &[&str] = &[
    "settings.local.json",
    ".git",
    ".gitignore",
    "target",
    "node_modules",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "bun.lock",
    "yarn.lock",
    "agent-manager.json",
    "worktrees",
];

/// Whether an entry of a template stays behind when it is copied: one of
/// [`TEMPLATE_EXCLUDE`], or what Python writes beside a hook the checkout
/// ran (`weft_catalog::is_python_cache`).
fn left_out_of_template(name: &str) -> bool {
    TEMPLATE_EXCLUDE.contains(&name) || weft_catalog::is_python_cache(name)
}

/// True when `root` already holds Tangle for `spec`.
pub fn tangle_installed(root: &Path, spec: &AssistantSpec) -> bool {
    spec.links
        .iter()
        .any(|(name, _)| root.join(name).join(WITNESS).is_file())
}

/// Copy every selected Tangle persona out of the weft checkout into
/// `root`, replacing what the template owns and leaving alone anything
/// the assistant itself put there. Every template is checked before a
/// byte is written, so a bad later assistant cannot leave an earlier one
/// half installed.
///
/// Copies, not symlinks: an assistant reads a project's instructions as
/// untrusted input, and Kilo refuses outright to read an agent file that
/// resolves outside the project, so a link into the checkout loads
/// nothing and says nothing. A copy is also what makes Tangle survive a
/// clone, which is why the generated `.gitignore` no longer hides it.
/// The cost is that a project's Tangle is the version that installed it;
/// `weft tangle update` is how it catches up, the same way
/// `weft catalog update` re-seeds the stdlib.
pub fn install_tangle(root: &Path, assistants: &[&AssistantSpec]) -> anyhow::Result<()> {
    let repo = weft_catalog::weft_repo_root()
        .map_err(|e| anyhow::anyhow!("locating the weft checkout for tangle: {e}"))?;
    let mut pairs = Vec::new();

    for spec in assistants {
        let template = repo.join("tangle").join(spec.dir);
        if !template.is_dir() {
            anyhow::bail!(
                "the tangle template for {} is missing in this weft checkout (expected {}); \
                 git pull the checkout, or create the project without --assistant {}",
                spec.dir,
                template.display(),
                spec.dir
            );
        }

        for (link_name, rel) in spec.links {
            let source = template.join(rel);
            // metadata follows links: a dangling source in the template
            // must not become a half-written entry in the project.
            if let Err(error) = std::fs::metadata(&source) {
                if error.kind() == std::io::ErrorKind::NotFound {
                    anyhow::bail!(
                        "the tangle template for {} is incomplete in this weft checkout ({} is missing); \
                         git pull the checkout, or create the project without --assistant {}",
                        spec.dir,
                        source.display(),
                        spec.dir
                    );
                }
                return Err(error).with_context(|| format!("inspect {}", source.display()));
            }
            pairs.push((root.join(link_name), source));
        }
    }

    for (dest, source) in pairs {
        merge_template(&source, &dest)?;
    }
    Ok(())
}

/// Put the template at `source` into `dest`, entry by entry. A file
/// replaces whatever is at `dest`; a directory keeps everything the
/// template does not name (an assistant's own `worktrees/`, its
/// installed `node_modules/`) and replaces everything it does.
///
/// An earlier weft installed these as symlinks into the checkout, so a
/// `dest` that IS a link is removed rather than written through: that
/// would edit the checkout itself and leave the project still pointing
/// at it.
fn merge_template(source: &Path, dest: &Path) -> anyhow::Result<()> {
    let linked = std::fs::symlink_metadata(dest)
        .map(|meta| meta.file_type().is_symlink())
        .unwrap_or(false);
    if linked {
        std::fs::remove_file(dest)
            .with_context(|| format!("remove the old tangle link at {}", dest.display()))?;
    }
    if !source.is_dir() {
        return replace_with_template(source, dest);
    }
    std::fs::create_dir_all(dest).with_context(|| format!("create {}", dest.display()))?;
    for entry in std::fs::read_dir(source)
        .with_context(|| format!("read the tangle template at {}", source.display()))?
    {
        let entry = entry.with_context(|| format!("read the tangle template at {}", source.display()))?;
        let name = entry.file_name();
        if left_out_of_template(&name.to_string_lossy()) {
            continue;
        }
        replace_with_template(&entry.path(), &dest.join(&name))?;
    }
    Ok(())
}

/// Throw away whatever is at `dest` and write the template's copy of it,
/// so a file the template dropped since the last install does not linger
/// in the project claiming to be part of Tangle.
fn replace_with_template(source: &Path, dest: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(dest) {
        Ok(meta) if meta.is_dir() => std::fs::remove_dir_all(dest)
            .with_context(|| format!("replace {}", dest.display()))?,
        Ok(_) => {
            std::fs::remove_file(dest).with_context(|| format!("replace {}", dest.display()))?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| format!("inspect {}", dest.display())),
    }
    if source.is_dir() {
        weft_compiler::build::copy_dir_filtered(source, dest, &left_out_of_template)
            .map_err(|e| anyhow::anyhow!("copy {} into {}: {e}", source.display(), dest.display()))
    } else {
        std::fs::copy(source, dest)
            .with_context(|| format!("copy {} into {}", source.display(), dest.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        assemble_atomically, merge_template, no_assistant_error, open_lines, picker_lines, picker_rows,
        resolve_assistants, shell_word, tangle_installed, AssistantSpec, Opener, Sign, ASSISTANTS, FALLBACK,
        WITNESS,
    };

    fn spec(dir: &str) -> &'static AssistantSpec {
        std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()).find(|a| a.dir == dir).unwrap()
    }

    #[test]
    fn the_picker_lists_what_was_found_first_and_the_fallback_last() {
        // Only Cline's extension and the codex program are "installed".
        let rows = picker_rows(|sign| {
            matches!(sign, Sign::Extension("saoudrizwan.claude-dev") | Sign::Program("codex"))
        });
        let order: Vec<(&str, bool)> = rows.iter().map(|(spec, found)| (spec.dir, *found)).collect();
        assert_eq!(&order[..2], &[("codex", true), ("cline", true)], "found ones first, in table order");
        assert!(order[2..].iter().all(|(_, found)| !found));
        assert_eq!(order.last(), Some(&("fallback", false)));
        assert_eq!(rows.len(), ASSISTANTS.len() + 1, "every assistant is offered");
        let lines = picker_lines(&rows);
        assert_eq!(lines[0], "   1. OpenAI Codex (cx, found on this machine)");
        assert_eq!(lines[2], "   3. Claude Code (cc)");
    }

    #[test]
    fn nothing_found_keeps_the_table_order() {
        let rows = picker_rows(|_| false);
        let dirs: Vec<&str> = rows.iter().map(|(spec, _)| spec.dir).collect();
        let table: Vec<&str> = ASSISTANTS.iter().map(|a| a.dir).chain(["fallback"]).collect();
        assert_eq!(dirs, table);
    }

    #[test]
    fn a_row_with_no_sign_is_never_found() {
        let rows = picker_rows(|_| true);
        let devin = rows.iter().find(|(spec, _)| spec.dir == "devin-desktop").unwrap();
        assert!(!devin.1);
    }

    #[test]
    fn refusing_without_a_terminal_names_every_value_and_the_way_to_have_none() {
        let message = no_assistant_error().to_string();
        for spec in ASSISTANTS {
            assert!(message.contains(&format!("{} ({})", spec.dir, spec.shorthand)), "{message}");
        }
        assert!(message.contains("agents for one not on that list"), "{message}");
        assert!(message.contains("--assistant none to create the project without Tangle"), "{message}");
    }

    #[test]
    fn an_editor_opens_the_folder_and_its_main_file() {
        assert_eq!(
            open_lines(&[spec("cursor")], "hello"),
            vec!["open it in Cursor: cursor hello hello/src/main.weft"]
        );
        assert_eq!(
            open_lines(&[spec("kilo-code")], "my app"),
            vec!["open it in Kilo Code: code 'my app' 'my app/src/main.weft'"]
        );
    }

    #[test]
    fn a_terminal_assistant_also_says_how_to_see_the_graph() {
        assert_eq!(
            open_lines(&[spec("claude-code")], "hello"),
            vec![
                "open it in Claude Code: cd hello && claude",
                "see the graph: code hello hello/src/main.weft (VS Code, with the weft extension)",
            ]
        );
    }

    #[test]
    fn the_graph_line_is_left_out_when_an_editor_was_chosen_too() {
        let lines = open_lines(&[spec("codex"), spec("github-copilot")], "hello");
        assert_eq!(
            lines,
            vec![
                "open it in OpenAI Codex: cd hello && codex",
                "open it in GitHub Copilot: code hello hello/src/main.weft",
            ]
        );
    }

    #[test]
    fn an_app_with_no_command_is_named() {
        assert_eq!(
            open_lines(&[spec("fallback")], "hello"),
            vec![
                "open it: open the folder hello in your assistant, then src/main.weft",
                "see the graph: code hello hello/src/main.weft (VS Code, with the weft extension)",
            ]
        );
    }

    #[test]
    fn every_row_says_how_to_open_a_project() {
        for spec in std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()) {
            let named = match spec.open {
                Opener::Editor(command) | Opener::Terminal(command) | Opener::App(command) => command,
            };
            assert!(!named.is_empty(), "{} has no opener", spec.dir);
            assert!(!spec.name.is_empty(), "{} has no name", spec.dir);
        }
    }

    #[test]
    fn a_folder_name_a_shell_would_split_is_quoted() {
        assert_eq!(shell_word("plain-name_1.0"), "plain-name_1.0");
        assert_eq!(shell_word("it's"), r"'it'\''s'");
    }

    #[test]
    fn resolves_kilo_code_and_its_shorthand() {
        let assistants = resolve_assistants(&["kilo-code".into(), "kc".into()]).unwrap();
        assert_eq!(assistants.len(), 1);
        assert_eq!(assistants[0].dir, "kilo-code");
        assert_eq!(assistants[0].links, &[("kilo.json", "kilo.json"), (".kilo", ".kilo")]);
    }

    #[test]
    fn every_assistant_has_a_unique_name_shorthand_and_link_set() {
        // A duplicate shorthand would silently resolve to whichever row came
        // first, and two rows claiming the same link name would collide in
        // the project when a user installs both.
        let mut names = std::collections::BTreeSet::new();
        for spec in std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()) {
            assert!(names.insert(spec.dir), "duplicate dir {}", spec.dir);
            assert!(names.insert(spec.shorthand), "duplicate shorthand {}", spec.shorthand);
            assert!(!spec.links.is_empty(), "{} installs nothing", spec.dir);
        }
    }

    #[test]
    fn the_fallback_refuses_to_install_beside_a_real_assistant() {
        // Cursor, Cline and Gemini read a root AGENTS.md ON TOP OF their own
        // persona file, so shipping both would load Tangle twice.
        let error = resolve_assistants(&["cursor".into(), "agents".into()]).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("cannot be installed alongside"), "{message}");
        assert!(message.contains("cursor"), "{message}");
    }

    #[test]
    fn the_fallback_installs_on_its_own() {
        let assistants = resolve_assistants(&["agents".into()]).unwrap();
        assert_eq!(assistants.len(), 1);
        assert_eq!(assistants[0].dir, "fallback");
    }

    #[test]
    fn an_unknown_assistant_names_the_fallback_in_its_error() {
        // The typo case has to point somewhere useful: a user on an
        // assistant weft has no row for still has a way in.
        let error = resolve_assistants(&["cluade-code".into()]).unwrap_err();
        assert!(error.to_string().contains("agents"), "{error}");
    }

    #[test]
    fn every_row_points_at_a_template_that_exists() {
        // A row naming a directory nobody wrote fails only at `weft new`,
        // in a user's terminal. Check the shipped tree instead: the crate
        // sits two levels under the checkout root.
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("checkout root")
            .to_path_buf();
        for spec in std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()) {
            let template = repo.join("tangle").join(spec.dir);
            assert!(template.is_dir(), "missing template {}", template.display());
            for (_, rel) in spec.links {
                let target = template.join(rel);
                assert!(
                    target.exists(),
                    "{} promises {} but it is not in the template",
                    spec.dir,
                    target.display()
                );
            }
        }
    }

    #[test]
    fn every_template_carries_the_witness_under_one_of_its_own_names() {
        // `weft tangle update` finds what a project has by looking for
        // WITNESS under each of an assistant's names. A template that
        // stopped shipping it would go silently un-refreshable.
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("checkout root")
            .to_path_buf();
        for spec in std::iter::once(&FALLBACK).chain(ASSISTANTS.iter()) {
            let template = repo.join("tangle").join(spec.dir);
            let found = spec
                .links
                .iter()
                .any(|(_, rel)| template.join(rel).join(WITNESS).is_file());
            assert!(found, "{} ships no {WITNESS} under any of its names", spec.dir);
        }
    }

    #[test]
    fn every_claude_code_hook_names_a_script_the_template_ships() {
        // A hook whose script was renamed or dropped fails in the user's
        // session on every tool call, long after `weft new` said it was
        // fine. The install copies `.claude/` whole, so the script only has
        // to be in the template, next to the settings that name it.
        let template = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("checkout root")
            .join("tangle/claude-code");
        let settings: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(template.join(".claude/settings.json")).unwrap())
                .unwrap();
        let mut scripts = Vec::new();
        for (event, groups) in settings["hooks"].as_object().expect("a hooks block") {
            for group in groups.as_array().unwrap_or_else(|| panic!("{event} is not a list")) {
                for hook in group["hooks"].as_array().unwrap_or_else(|| panic!("{event} has no hooks")) {
                    let command = hook["command"].as_str().unwrap_or_else(|| panic!("{event} has no command"));
                    let (_, rest) = command
                        .split_once("$CLAUDE_PROJECT_DIR/")
                        .unwrap_or_else(|| panic!("{event} runs `{command}`, outside the project"));
                    scripts.push((event.clone(), rest.trim_end_matches('"').to_string()));
                }
            }
        }
        assert!(!scripts.is_empty(), "the template registers no hook at all");
        for (event, script) in scripts {
            assert!(template.join(&script).is_file(), "{event} runs {script}, which the template does not ship");
        }
    }

    #[test]
    fn every_claude_code_specialist_has_its_own_command_list() {
        // The PreToolUse hook leaves an agent the list does not name to the
        // shared allow list, which is Tangle's own: a specialist added
        // without a list would quietly get Tangle's commands, and a list
        // whose agent was renamed would guard nobody.
        let template = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("checkout root")
            .join("tangle/claude-code/.claude");
        let lists: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(template.join("hooks/specialist_commands.json")).unwrap(),
        )
        .unwrap();
        let lists = lists.as_object().expect("one object, keyed by agent name");
        let mut agents = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(template.join("agents")).unwrap() {
            let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
            let name = text
                .lines()
                .find_map(|line| line.strip_prefix("name: "))
                .expect("an agent file names itself")
                .trim()
                .to_string();
            let commands = lists
                .get(&name)
                .and_then(|list| list["commands"].as_array())
                .unwrap_or_else(|| panic!("{name} has no command list"));
            assert!(!commands.is_empty(), "{name}'s command list is empty");
            agents.insert(name);
        }
        for name in lists.keys() {
            assert!(agents.contains(name), "a command list names {name}, which is no agent");
        }
    }

    #[test]
    fn a_refresh_replaces_what_tangle_owns_and_keeps_what_the_assistant_made() {
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        std::fs::create_dir_all(template.join("agent")).unwrap();
        std::fs::write(template.join("agent/tangle.md"), "new").unwrap();
        std::fs::write(template.join("node_modules"), "never copied").unwrap();
        std::fs::create_dir_all(template.join("hooks/__pycache__")).unwrap();
        std::fs::write(template.join("hooks/check.py"), "hook").unwrap();
        std::fs::write(template.join("hooks/__pycache__/check.pyc"), "never copied").unwrap();
        std::fs::write(template.join("settings.local.json"), "the checkout's own").unwrap();

        // A project whose last install left a stale agent behind, and
        // whose assistant keeps its own scratch state in the same tree.
        let dest = temp.path().join("project/.kilo");
        std::fs::create_dir_all(dest.join("agent")).unwrap();
        std::fs::create_dir_all(dest.join("worktrees")).unwrap();
        std::fs::write(dest.join("agent/tangle.md"), "old").unwrap();
        std::fs::write(dest.join("agent/dropped.md"), "gone after this").unwrap();
        std::fs::write(dest.join("worktrees/mine"), "the assistant's").unwrap();
        std::fs::write(dest.join("settings.local.json"), "the user's").unwrap();

        merge_template(&template, &dest).unwrap();

        assert_eq!(std::fs::read_to_string(dest.join("agent/tangle.md")).unwrap(), "new");
        assert!(!dest.join("agent/dropped.md").exists(), "a dropped template file goes");
        assert!(!dest.join("node_modules").exists(), "an assistant's own tree never travels");
        assert_eq!(std::fs::read_to_string(dest.join("hooks/check.py")).unwrap(), "hook");
        assert!(
            !dest.join("hooks/__pycache__").exists(),
            "the bytecode a hook left in the checkout never travels"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("settings.local.json")).unwrap(),
            "the user's",
            "the person's own settings survive a refresh"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("worktrees/mine")).unwrap(),
            "the assistant's",
            "what the assistant put there survives a refresh"
        );
    }

    #[test]
    fn a_refresh_turns_an_older_install_s_symlink_into_real_files() {
        // Before copies, these were symlinks into the weft checkout.
        // Writing through one would edit the checkout and leave the
        // project still pointing at it.
        let temp = tempfile::tempdir().unwrap();
        let template = temp.path().join("template");
        std::fs::create_dir_all(template.join("agent")).unwrap();
        std::fs::write(template.join("agent/tangle.md"), "new").unwrap();

        let dest = temp.path().join(".kilo");
        std::os::unix::fs::symlink(&template, &dest).unwrap();
        merge_template(&template, &dest).unwrap();

        assert!(!std::fs::symlink_metadata(&dest).unwrap().file_type().is_symlink());
        assert_eq!(std::fs::read_to_string(dest.join("agent/tangle.md")).unwrap(), "new");
    }

    #[test]
    fn a_project_is_only_seen_as_installed_when_the_witness_is_there() {
        // An assistant that made its own `.kilo/` (Kilo does) must not
        // read as a project that has Tangle.
        let temp = tempfile::tempdir().unwrap();
        let kilo = &ASSISTANTS[1];
        std::fs::create_dir_all(temp.path().join(".kilo/worktrees")).unwrap();
        assert!(!tangle_installed(temp.path(), kilo));

        std::fs::create_dir_all(temp.path().join(".kilo").join(WITNESS).parent().unwrap()).unwrap();
        std::fs::write(temp.path().join(".kilo").join(WITNESS), "skill").unwrap();
        assert!(tangle_installed(temp.path(), kilo));
    }

    #[test]
    fn a_failed_step_leaves_no_project_and_no_staging_behind() {
        // The seed failing after the scaffold was written is the case
        // that used to strand a project with a weft.toml and no catalog.
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("probe");
        let error = assemble_atomically::<()>(&root, |staging| {
            std::fs::write(staging.join("weft.toml"), "[package]").unwrap();
            std::fs::create_dir_all(staging.join("nodes")).unwrap();
            anyhow::bail!("cannot locate the weft checkout")
        })
        .unwrap_err();
        assert!(error.to_string().contains("weft checkout"), "{error}");
        assert!(!root.exists(), "the target stays absent");
        let left: Vec<_> = std::fs::read_dir(temp.path()).unwrap().collect();
        assert!(left.is_empty(), "the staging directory is removed: {left:?}");
    }

    #[test]
    fn a_successful_build_appears_whole_at_the_target() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("nested/probe");
        let answer = assemble_atomically(&root, |staging| {
            std::fs::create_dir_all(staging.join("src")).unwrap();
            std::fs::write(staging.join("src/main.weft"), "x").unwrap();
            Ok(7)
        })
        .unwrap();
        assert_eq!(answer, 7);
        assert_eq!(std::fs::read_to_string(root.join("src/main.weft")).unwrap(), "x");
        let left: Vec<_> = std::fs::read_dir(temp.path().join("nested"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(left, vec![std::ffi::OsString::from("probe")], "no staging directory left");
    }

    #[test]
    fn a_target_that_appears_meanwhile_is_left_untouched() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("probe");
        let error = assemble_atomically(&root, |_| {
            std::fs::create_dir_all(&root).unwrap();
            Ok(())
        })
        .unwrap_err();
        assert!(error.to_string().contains("appeared"), "{error}");
        assert!(std::fs::read_dir(&root).unwrap().next().is_none(), "the target is not written");
        assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1, "staging removed");
    }
}
