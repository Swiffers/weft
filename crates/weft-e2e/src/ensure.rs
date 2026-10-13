//! Reaching the install a test runs against.
//!
//! The install under test is the one this checkout acts on
//! ([`install_under_test`]): the one `WEFT_INSTALL` names (the runner sets
//! it), else the install of the checkout's branch. The runner
//! (`scripts/run-e2e.sh`) brings it to current code ONCE, before any test
//! starts, so a test only has to confirm the dispatcher answers. Nothing here wipes anything: every test cleans up
//! exactly what it created when it passes, and keeps it when it fails, so
//! tests running side by side never touch each other's state. What a failed
//! run left behind is removed when the next run starts (or by
//! `scripts/run-e2e.sh --clean`).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::client::{poll_until, Dispatcher};

/// The dispatcher of the install under test, once it answers `/health`.
/// Called at the START of every test that runs on that install; a test
/// that needs an install of its own starts a [`crate::cell::Cell`]
/// instead.
pub async fn up() -> Result<Dispatcher> {
    load_repo_env();
    let disp = Dispatcher::under_test()?;
    wait_healthy(&disp).await?;
    Ok(disp)
}

/// Load the repo root's `.env.local`, then its `.env`, into this test
/// process's environment (the same two files the CLI reads), so a test that
/// spends on a real key finds it without the operator re-exporting it into
/// the test shell. Nothing overrides a variable that is already set, so the
/// real environment wins over both files and `.env.local` (this checkout
/// only) wins over `.env` (shared by every worktree). A missing file is
/// fine (a test needing a key skips on its own); a malformed one fails the
/// test, as it fails the CLI. Only under the `e2e`
/// feature: the files mean nothing without the live system.
fn load_repo_env() {
    #[cfg(feature = "e2e")]
    if let Ok(root) = repo_root() {
        load_env_files(&root);
    }
}

/// The files [`load_repo_env`] reads at `root`, first one winning.
// SYNC: the two env files <-> crates/weft-cli/src/startup.rs (ENV_FILES)
#[cfg(feature = "e2e")]
fn load_env_files(root: &Path) {
    for file in [".env.local", ".env"] {
        let path = root.join(file);
        match dotenvy::from_path(&path) {
            Ok(()) => {}
            Err(e) if e.not_found() => {}
            Err(e) => panic!("{} is malformed: {e}", path.display()),
        }
    }
}

/// Read a GROUP of external-service variables a test needs together,
/// or announce ONE skip naming the whole shortfall and return `None`.
/// Loads the repo root's `.env.local` and `.env` first, so the check is
/// order-independent: a test may (and should) gate on its variables
/// BEFORE the expensive [`up`], and a single-test binary must not depend
/// on a sibling test having loaded them for it. `what` names the service the group
/// belongs to; the returned values are in `vars` order (destructure
/// with `<[String; N]>::try_from`).
pub fn env_group_or_skip(what: &str, vars: &[&str]) -> Option<Vec<String>> {
    load_repo_env();
    let mut present = Vec::new();
    let mut missing = Vec::new();
    let mut values = Vec::new();
    for &var in vars {
        match std::env::var(var) {
            Ok(v) if !v.trim().is_empty() => {
                present.push(var);
                values.push(v);
            }
            _ => missing.push(var),
        }
    }
    if missing.is_empty() {
        Some(values)
    } else {
        eprintln!("SKIPPED {what}: missing {missing:?} (have {present:?})");
        None
    }
}

/// Single-variable form of [`env_group_or_skip`] (one implementation).
pub fn env_or_skip(var: &str) -> Option<String> {
    env_group_or_skip(var, &[var]).map(|mut values| values.pop().expect("one var, one value"))
}

/// Absolute path to the repo root (where `setup.sh` lives). Resolved from this
/// crate's manifest dir (`<root>/crates/weft-e2e`) so it is correct regardless
/// of the cwd the test binary runs in. Fails loud if `setup.sh` is not found,
/// which means the layout changed and the rig must be updated.
pub(crate) fn repo_root() -> Result<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest
        .parent()
        .and_then(Path::parent)
        .context("locate repo root from CARGO_MANIFEST_DIR")?
        .to_path_buf();
    let setup = root.join("setup.sh");
    if !setup.is_file() {
        bail!(
            "setup.sh not found at {}; repo layout changed, update weft-e2e::ensure::repo_root",
            setup.display()
        );
    }
    Ok(root)
}

/// Poll the dispatcher's `/health` until it answers `ok`. A dispatcher that
/// was just rolled, or a cell's that just started, can take a beat to accept
/// connections, so every test confirms reachability before it proceeds.
pub async fn wait_healthy(disp: &Dispatcher) -> Result<()> {
    poll_until(
        &format!("dispatcher /health at {} to answer ok", disp.base()),
        Duration::from_secs(60),
        Duration::from_millis(500),
        || {
            let disp = disp.clone();
            async move {
                match disp.get_raw("/health").await {
                    Ok((status, body)) if status.is_success() && body.trim() == "ok" => {
                        Ok(Some(()))
                    }
                    // Reachable-but-not-ok, or not-yet-reachable: keep waiting.
                    Ok(_) => Ok(None),
                    Err(_) => Ok(None),
                }
            }
        },
    )
    .await
}

/// The install this run tests: the one `WEFT_INSTALL` names (the runner
/// exports it for everything it starts), else the install of this
/// checkout's branch, exactly what `weft` run in the checkout acts on
/// ([`weft_core::infra::Install::wanted`]). Every test that does not start
/// a cell runs on it, and every `weft` the rig spawns is its binary
/// ([`crate::client::weft_command`]).
pub fn install_under_test() -> Result<weft_core::infra::Install> {
    let env = std::env::var(weft_core::infra::INSTALL_ENV).ok();
    weft_core::infra::Install::wanted(env.as_deref(), &repo_root()?)
        .map(|wanted| wanted.install)
        .map_err(anyhow::Error::msg)
}

#[cfg(all(test, feature = "e2e"))]
mod tests {
    /// `.env.local` wins over `.env`, and a variable already set wins over
    /// both. Sets process variables, which is safe here: nextest runs every
    /// test in a process of its own.
    #[test]
    fn env_local_wins_over_env_and_the_real_environment_wins_over_both() {
        let root = std::env::temp_dir().join(format!("weft-env-files-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let var = |name: &str| format!("WEFT_E2E_PRECEDENCE_{name}_{}", std::process::id());
        let (both, shared_only, set) = (var("BOTH"), var("SHARED"), var("SET"));
        std::fs::write(root.join(".env.local"), format!("{both}=local\n{set}=local\n")).unwrap();
        std::fs::write(root.join(".env"), format!("{both}=shared\n{shared_only}=shared\n{set}=shared\n")).unwrap();
        std::env::set_var(&set, "real");

        super::load_env_files(&root);

        assert_eq!(std::env::var(&both).unwrap(), "local");
        assert_eq!(std::env::var(&shared_only).unwrap(), "shared");
        assert_eq!(std::env::var(&set).unwrap(), "real");
        std::fs::remove_dir_all(&root).unwrap();
    }
}

#[cfg(test)]
mod env_reads {
    use std::path::{Path, PathBuf};

    /// `src` with every comment, string literal and char literal blanked
    /// to spaces (newlines kept, so line numbers still hold): what is left
    /// is the code. Covers `//` and nested `/* */` comments, `"..."` and
    /// `b"..."` with escapes, raw strings (`r"..."`, `r#"..."#`, `br#"..."#`)
    /// and char literals, told apart from lifetimes.
    fn code_only(src: &str) -> String {
        let c: Vec<char> = src.chars().collect();
        let mut out = String::with_capacity(src.len());
        let blank = |out: &mut String, from: &[char]| {
            out.extend(from.iter().map(|&ch| if ch == '\n' { '\n' } else { ' ' }));
        };
        let ident = |ch: char| ch.is_alphanumeric() || ch == '_';
        let mut i = 0;
        while i < c.len() {
            let at = |k: usize| c.get(i + k).copied();
            let end = if at(0) == Some('/') && at(1) == Some('/') {
                let mut j = i;
                while j < c.len() && c[j] != '\n' {
                    j += 1;
                }
                j
            } else if at(0) == Some('/') && at(1) == Some('*') {
                let (mut j, mut depth) = (i + 2, 1);
                while j < c.len() && depth > 0 {
                    if c[j] == '/' && c.get(j + 1) == Some(&'*') {
                        depth += 1;
                        j += 2;
                    } else if c[j] == '*' && c.get(j + 1) == Some(&'/') {
                        depth -= 1;
                        j += 2;
                    } else {
                        j += 1;
                    }
                }
                j
            } else if let Some(open) = raw_string_open(&c, i) {
                // `open` is the index of the opening quote; the closing
                // quote must be followed by as many `#` as preceded it.
                let hashes = open - i - c[i..open].iter().take_while(|&&ch| ch != '#').count();
                let mut j = open + 1;
                while j < c.len() && !(c[j] == '"' && c[j + 1..].iter().take(hashes).filter(|&&ch| ch == '#').count() == hashes) {
                    j += 1;
                }
                (j + 1 + hashes).min(c.len())
            } else if at(0) == Some('"') || (at(0) == Some('b') && at(1) == Some('"') && (i == 0 || !ident(c[i - 1]))) {
                let mut j = if at(0) == Some('"') { i + 1 } else { i + 2 };
                while j < c.len() && c[j] != '"' {
                    j += if c[j] == '\\' { 2 } else { 1 };
                }
                (j + 1).min(c.len())
            } else if at(0) == Some('\'') && at(1) == Some('\\') {
                // Past the escaped char, which may itself be a quote: `'\''`.
                let mut j = i + 3;
                while j < c.len() && c[j] != '\'' {
                    j += 1;
                }
                (j + 1).min(c.len())
            } else if at(0) == Some('\'') && at(2) == Some('\'') {
                i + 3
            } else {
                out.push(c[i]);
                i += 1;
                continue;
            };
            blank(&mut out, &c[i..end]);
            i = end;
        }
        out
    }

    /// Where a raw string starting at `i` (`r"`, `r#"`, `br##"`, ...) has
    /// its opening quote, or `None` when none starts there.
    fn raw_string_open(c: &[char], i: usize) -> Option<usize> {
        if i > 0 && (c[i - 1].is_alphanumeric() || c[i - 1] == '_') {
            return None;
        }
        let mut j = i;
        if c.get(j) == Some(&'b') {
            j += 1;
        }
        if c.get(j) != Some(&'r') {
            return None;
        }
        j += 1;
        while c.get(j) == Some(&'#') {
            j += 1;
        }
        (c.get(j) == Some(&'"')).then_some(j)
    }

    /// Every place `src` reaches the process environment from Rust, as
    /// `(line, what)`. The rule: in code (comments and strings removed),
    /// `env` is refused wherever it can only be the `std::env` module or
    /// the `env!` macro: next to a `::` (`std::env`, `env::var`, a path
    /// through any alias of `std`), in a `use` (which also catches it
    /// brought in under another name, `use std::{env as e}`), or followed
    /// by `!`. `option_env` and `dotenvy` are refused anywhere. A local
    /// called `env` and a method `.env(` (setting a child's variable) pass.
    fn env_reads_in(src: &str) -> Vec<(usize, String)> {
        let code = code_only(src);
        // (text, line): identifiers whole, every other non-space char alone.
        let mut tokens: Vec<(String, usize)> = Vec::new();
        let (mut line, mut chars) = (1, code.chars().peekable());
        while let Some(ch) = chars.next() {
            if ch == '\n' {
                line += 1;
            } else if ch.is_alphabetic() || ch == '_' {
                let mut word = String::from(ch);
                while let Some(&next) = chars.peek().filter(|n| n.is_alphanumeric() || **n == '_') {
                    word.push(next);
                    chars.next();
                }
                // A raw identifier: `r#env` is `env`.
                if word == "r" && chars.peek() == Some(&'#') {
                    chars.next();
                    word.clear();
                    while let Some(&next) = chars.peek().filter(|n| n.is_alphanumeric() || **n == '_') {
                        word.push(next);
                        chars.next();
                    }
                }
                tokens.push((word, line));
            } else if !ch.is_whitespace() {
                tokens.push((ch.to_string(), line));
            }
        }
        let is = |k: Option<usize>, text: &str| k.and_then(|k| tokens.get(k)).is_some_and(|(t, _)| t == text);
        let mut found = Vec::new();
        let mut in_use = false;
        for (k, (text, line)) in tokens.iter().enumerate() {
            let before = |n: usize| k.checked_sub(n);
            match text.as_str() {
                "use" if !is(before(1), ".") => in_use = true,
                ";" => in_use = false,
                "env" => {
                    let path = (is(before(1), ":") && is(before(2), ":")) || (is(Some(k + 1), ":") && is(Some(k + 2), ":"));
                    let macro_call = is(Some(k + 1), "!") && ["(", "[", "{"].iter().any(|open| is(Some(k + 2), open));
                    if path || in_use || macro_call {
                        found.push((*line, text.clone()));
                    }
                }
                "option_env" | "dotenvy" => found.push((*line, text.clone())),
                _ => {}
            }
        }
        found
    }

    fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rust_files(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push(path);
            }
        }
    }

    /// No test reads the environment itself: a credential goes through
    /// `env_or_skip` / `env_group_or_skip`, which load the repo's env
    /// files first and skip, rather than fail, when it is missing; and
    /// what is not a credential (the dispatcher's address, the fixtures)
    /// comes from the toolkit, which knows the install under test.
    #[test]
    fn tests_read_the_environment_only_through_the_toolkit() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
        let mut files = Vec::new();
        rust_files(&root, &mut files);
        let mut found = Vec::new();
        for file in &files {
            let text = std::fs::read_to_string(file).unwrap();
            let lines: Vec<&str> = text.lines().collect();
            for (line, what) in env_reads_in(&text) {
                found.push(format!("{}:{line}: `{what}` in {}", file.display(), lines[line - 1].trim()));
            }
        }
        assert!(
            found.is_empty(),
            "these tests read the environment directly; read a credential with \
             `ensure::env_or_skip` / `ensure::env_group_or_skip` (it loads `.env.local` and `.env` \
             and skips the test when the variable is missing), and anything else through the \
             toolkit (`disp.base()`, `project::fixtures_root()`, `ensure::install_under_test()`):\n{}",
            found.join("\n")
        );
    }

    #[test]
    fn the_guard_finds_every_way_to_name_the_environment() {
        let lines = |src: &str| env_reads_in(src).into_iter().map(|(line, _)| line).collect::<Vec<_>>();
        for (src, want) in [
            ("let k = std::env::var(\"K\");", vec![1]),
            ("use std::{env, fs};\nlet v = env::vars_os();", vec![1, 2]),
            ("use std::{env as e};\nlet v = e::var(\"K\");", vec![1]),
            ("use std::env::var as get;\nget(\"K\");", vec![1]),
            ("use std as s;\nlet v = s::env::var(\"K\");", vec![2]),
            ("let x = \"http://a\"; let k = std::env::var(\"K\");", vec![1]),
            ("let x = r#\"a \" // b\"#; let k = std::r#env::var(\"K\");", vec![1]),
            ("let p = env!(\"CARGO_MANIFEST_DIR\");", vec![1]),
            ("let p = option_env!(\"K\");", vec![1]),
            ("dotenvy::dotenv().ok();", vec![1]),
        ] {
            assert_eq!(lines(src), want, "in {src:?}");
        }
    }

    #[test]
    fn the_guard_passes_what_only_looks_like_the_environment() {
        for src in [
            "// std::env::var(\"K\") in a comment",
            "/* outer /* std::env */ still a comment */ let a = 1;",
            "let s = \"std::env::var and dotenvy and env!(X)\";",
            "let s = r#\"std::env::var(\"K\")\"#;",
            "let s = b\"env::var\";",
            "let c = '\"'; let s = \"env::var\";",
            "fn f<'a>(x: &'a str) -> &'a str { x }",
            "let env = ensure::env_group_or_skip(\"x\", &[\"A\"]);\nlet n = env.len();",
            "cmd.env(\"K\", \"v\");",
            "let q = '\\''; let s = \"env::var\";",
            "if env != other { }",
        ] {
            assert!(env_reads_in(src).is_empty(), "flagged {src:?}: {:?}", env_reads_in(src));
        }
    }
}
