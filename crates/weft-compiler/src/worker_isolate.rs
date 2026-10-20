//! `weft-isolate`: builds a worker crate so that one node package that does
//! not compile costs only its own node types, never the whole image.
//!
//! Emitted by codegen as a standalone package beside the worker crate
//! (`codegen::write_isolation`) and run by the image build in the crate's
//! directory: `weft-isolate <isolate.json> <left-out.json> <cargo messages>`. Every package
//! crate is an optional dependency of the worker behind a feature of its
//! own, all of them on by default, and carries its package's build script
//! and build-dependencies itself, so:
//!
//! 1. It runs `cargo build --release --keep-going --message-format=json`,
//!    echoing everything cargo prints to stderr. `--keep-going` makes cargo
//!    build every crate it can, so the healthy package crates are compiled
//!    (and cached) even when a sibling fails. On success the record is
//!    empty.
//! 2. Otherwise it decides from what cargo produced: a candidate
//!    (`isolate.json`: one per package crate) is kept when cargo reported
//!    its library artifact. One cargo built nothing for is left out only
//!    when an error cargo printed traces to it: a compile error in it or in
//!    a crate it depends on (`cargo metadata` says which), or a build
//!    script of one of those failing when it ran. It carries the first
//!    such error, and the record of them is written to `left-out.json`,
//!    which the worker's registry embeds.
//!
//!    A candidate cargo built nothing for, with no error tracing to it,
//!    fails the whole build instead: cargo stopped for a reason outside
//!    the package (the registry or the network unreachable, the machine
//!    out of disk or memory), and leaving it out would bake that outage
//!    into an image every later build with the same sources reuses. A
//!    failed build produces no image, so the next one tries again.
//! 3. It builds the worker again with only the kept packages' features.
//!
//! Every JSON message cargo writes in either build is appended to the
//! messages file, which is how the compile-cache sweep after the build
//! (`worker_image::CACHE_GC_SCRIPT`) knows every unit the build used.
//!
//! A failure in the worker crate itself or in a crate it depends on other
//! than through a package (the engine) fails the build: leaving packages
//! out cannot fix it. Depends only on serde and serde_json, so it compiles
//! in seconds and is one fixed package every build shares in the compile
//! cache. weft-compiler compiles this same file as a module, for the
//! candidate type codegen writes and for its tests.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, ExitCode, Stdio};

use serde::{Deserialize, Serialize};

/// The most bytes the record may take (see weft-core's constant).
// SYNC: RECORD_LIMIT <-> crates/weft-core/src/builds.rs LEFT_OUT_RECORD_LIMIT
const RECORD_LIMIT: usize = 4000;

/// How short an error may be cut to make the record fit. Below this it
/// says too little to be worth a line, and the build fails instead.
const SHORTEST_ERROR: usize = 80;

/// What codegen writes to `isolate.json`: the package crates the build may
/// leave out.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Candidates {
    pub(crate) packages: Vec<Candidate>,
}

/// One package crate of the worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct Candidate {
    /// The worker's cargo feature that compiles it in.
    pub(crate) feature: String,
    /// Its directory under the worker crate (`pkg_<name>-<slot>`), which is
    /// how cargo's package ids name it.
    pub(crate) crate_dir: String,
    /// The package's name, as the record names it.
    pub(crate) package: String,
    pub(crate) node_types: Vec<String>,
}

/// The record of what the build left out.
// SYNC: Record <-> crates/weft-core/src/builds.rs LeftOut
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Record {
    packages: Vec<LeftOutPackage>,
}

// SYNC: LeftOutPackage <-> crates/weft-core/src/builds.rs LeftOutPackage
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LeftOutPackage {
    package: String,
    node_types: Vec<String>,
    error: String,
}

/// What one `--message-format=json` build said, in the order cargo said
/// it.
#[derive(Debug, Default)]
struct BuildReport {
    /// Package ids cargo produced a library for (fresh or compiled).
    libraries: Vec<String>,
    /// Every compile error, from the JSON stream on stdout.
    errors: Vec<BuildError>,
    /// Every build script that compiled but failed when it ran, from
    /// cargo's own text on stderr (which has no JSON message for it).
    script_failures: Vec<ScriptFailure>,
}

/// One compile error of a build.
#[derive(Debug, Clone)]
struct BuildError {
    /// The package id of the crate it is in.
    package_id: String,
    /// One line for a person ([`error_line`]).
    line: String,
}

/// A build script that failed when it ran, as cargo's stderr names it:
/// `` error: failed to run custom build command for `name v1.2.3 (path)` ``.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScriptFailure {
    name: String,
    version: String,
    /// One line for a person: the script's panic message or first line of
    /// its own stderr.
    line: String,
}

/// The crates of the build and what each depends on (normal, build and
/// dev edges alike), from `cargo metadata`.
#[derive(Debug, Default)]
struct CrateGraph {
    /// The worker crate's package id.
    root: String,
    deps: BTreeMap<String, Vec<String>>,
    /// Package id to (name, version).
    packages: BTreeMap<String, (String, String)>,
}

/// What to do after a failed first build.
#[derive(Debug, PartialEq, Eq)]
struct Decision {
    left_out: Vec<LeftOutPackage>,
    /// The features of the candidates that built, in candidate order.
    healthy: Vec<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("weft-isolate: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [candidates_path, record_path, messages_path] = args.as_slice() else {
        return Err("usage: weft-isolate <isolate.json> <left-out.json> <cargo messages>".into());
    };
    let mut messages = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(messages_path)
        .map_err(|e| format!("open {messages_path}: {e}"))?;
    let candidates: Candidates = serde_json::from_slice(
        &std::fs::read(candidates_path).map_err(|e| format!("read {candidates_path}: {e}"))?,
    )
    .map_err(|e| format!("{candidates_path} does not read: {e}"))?;
    let write_record = |json: &str| std::fs::write(record_path, json).map_err(|e| format!("write {record_path}: {e}"));

    let mut first = Command::new("cargo")
        .args(["build", "--release", "--keep-going", "--message-format=json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("start cargo: {e}"))?;
    let stdout = first.stdout.take().ok_or("cargo's output is not piped")?;
    let stderr = first.stderr.take().ok_or("cargo's errors are not piped")?;
    // Read side by side: cargo blocks on whichever pipe nobody drains.
    let script_failures = std::thread::spawn(move || {
        read_script_failures(BufReader::new(stderr).lines().map_while(Result::ok), &mut |line| eprintln!("{line}"))
    });
    let lines = BufReader::new(stdout).lines().map_while(Result::ok).inspect(|line| {
        // The sweep only bounds the cache: a message it misses costs a
        // recompile later, never this build.
        let _ = writeln!(messages, "{line}");
    });
    let mut report = read_messages(lines, &mut |text| eprint!("{text}"));
    report.script_failures = script_failures.join().map_err(|_| "reading cargo's errors failed")?;
    let status = first.wait().map_err(|e| format!("wait for cargo: {e}"))?;
    if status.success() {
        write_record(&fit_record(&[], RECORD_LIMIT)?)?;
        return Ok(ExitCode::SUCCESS);
    }
    if candidates.packages.is_empty() {
        eprintln!("weft: the crate did not compile (the errors are above)");
        return Ok(ExitCode::FAILURE);
    }

    let graph = crate_graph()?;
    let decision = match decide(&candidates.packages, &report, &graph) {
        Ok(decision) => decision,
        Err(why) => {
            eprintln!("weft: {why}");
            return Ok(ExitCode::FAILURE);
        }
    };
    for package in &decision.left_out {
        eprintln!("weft: left package {} out of the worker: {}", package.package, package.error);
    }
    // Written before the second build: the worker's registry embeds it.
    write_record(&fit_record(&decision.left_out, RECORD_LIMIT)?)?;

    let mut second = Command::new("cargo");
    second.args(["build", "--release", "--no-default-features", "--message-format=json-render-diagnostics"]);
    second.stdout(messages);
    if !decision.healthy.is_empty() {
        second.arg("--features").arg(decision.healthy.join(","));
    }
    let status = second.status().map_err(|e| format!("start cargo: {e}"))?;
    Ok(if status.success() { ExitCode::SUCCESS } else { ExitCode::FAILURE })
}

/// Read a cargo JSON message stream, handing every diagnostic's rendered
/// text (and any line that is not a message) to `echo`.
fn read_messages(lines: impl Iterator<Item = String>, echo: &mut dyn FnMut(&str)) -> BuildReport {
    let mut report = BuildReport::default();
    for line in lines {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(&line) else {
            echo(&format!("{line}\n"));
            continue;
        };
        let package_id = message["package_id"].as_str().unwrap_or_default().to_string();
        match message["reason"].as_str() {
            Some("compiler-message") => {
                let diagnostic = &message["message"];
                if let Some(rendered) = diagnostic["rendered"].as_str() {
                    echo(rendered);
                }
                if diagnostic["level"].as_str() == Some("error") {
                    report.errors.push(BuildError { package_id, line: error_line(diagnostic) });
                }
            }
            Some("compiler-artifact") => {
                let kinds = message["target"]["kind"].as_array().cloned().unwrap_or_default();
                if kinds.iter().any(|kind| matches!(kind.as_str(), Some("lib" | "rlib"))) {
                    report.libraries.push(package_id);
                }
            }
            _ => {}
        }
    }
    report
}

/// One line for a person: the diagnostic's headline (`error[E0308]:
/// mismatched types`), and where it points when it points somewhere.
fn error_line(diagnostic: &serde_json::Value) -> String {
    let first_line = |text: Option<&str>| text.and_then(|t| t.lines().next()).map(str::trim).filter(|l| !l.is_empty()).map(str::to_string);
    let head = first_line(diagnostic["rendered"].as_str())
        .or_else(|| first_line(diagnostic["message"].as_str()))
        .unwrap_or_else(|| "a compile error with no message".to_string());
    let spans = diagnostic["spans"].as_array().cloned().unwrap_or_default();
    match spans.iter().find(|span| span["is_primary"].as_bool() == Some(true)) {
        Some(span) => format!("{head} at {}:{}", span["file_name"].as_str().unwrap_or("?"), span["line_start"]),
        None => head,
    }
}

/// Whether a cargo package id names the crate at `crate_dir`, in either
/// spelling cargo uses (`path+file:///work/<dir>#name@0.1.0`, or
/// `name 0.1.0 (path+file:///work/<dir>)`).
fn names_crate(package_id: &str, crate_dir: &str) -> bool {
    package_id.contains(&format!("/{crate_dir}#")) || package_id.contains(&format!("/{crate_dir})"))
}

/// Read cargo's stderr, handing every line to `echo`, and answer the build
/// scripts it says failed when they ran. Cargo prints each as one block:
///
/// ```text
/// error: failed to run custom build command for `pkg_a v0.1.0 (/work/pkg_a-1)`
///
/// Caused by:
///   process didn't exit successfully: `...build-script-build` (exit status: 101)
///   --- stderr
///
///   thread 'main' panicked at /weft/project-nodes/nodes/a/build.rs:3:5:
///   libfoo was not found
/// ```
///
/// The line kept is the panic's message when the script panicked, else the
/// first line the script wrote to stderr, else cargo's own headline.
fn read_script_failures(lines: impl Iterator<Item = String>, echo: &mut dyn FnMut(&str)) -> Vec<ScriptFailure> {
    const HEAD: &str = "error: failed to run custom build command for `";
    struct Open {
        name: String,
        version: String,
        in_stderr: bool,
        after_panic: bool,
        panic_message: Option<String>,
        first_stderr: Option<String>,
    }
    let close = |open: Open| ScriptFailure {
        line: match open.panic_message.or(open.first_stderr) {
            Some(said) => format!("the build script of `{} v{}` failed: {said}", open.name, open.version),
            None => format!("the build script of `{} v{}` failed", open.name, open.version),
        },
        name: open.name,
        version: open.version,
    };
    let mut failures = Vec::new();
    let mut open: Option<Open> = None;
    for line in lines {
        echo(&line);
        if let Some(rest) = line.strip_prefix(HEAD) {
            if let Some(done) = open.take() {
                failures.push(close(done));
            }
            let site = rest.split('`').next().unwrap_or_default();
            let mut words = site.split_whitespace();
            let (Some(name), Some(version)) = (words.next(), words.next()) else { continue };
            open = Some(Open {
                name: name.to_string(),
                version: version.trim_start_matches('v').to_string(),
                in_stderr: false,
                after_panic: false,
                panic_message: None,
                first_stderr: None,
            });
            continue;
        }
        let Some(current) = open.as_mut() else { continue };
        // A line at the left margin is cargo speaking again: the block ended.
        if !line.is_empty() && !line.starts_with(' ') && line != "Caused by:" {
            failures.push(close(open.take().expect("open")));
            continue;
        }
        let said = line.trim();
        if said == "--- stderr" {
            current.in_stderr = true;
        } else if current.in_stderr && !said.is_empty() {
            if current.after_panic && current.panic_message.is_none() {
                current.panic_message = Some(said.to_string());
            } else if said.starts_with("thread '") && said.contains("panicked at") {
                current.after_panic = true;
            } else if current.first_stderr.is_none() {
                current.first_stderr = Some(said.to_string());
            }
        }
    }
    failures.extend(open.map(close));
    failures
}

/// Ask cargo how the worker's crates depend on each other, on the platform
/// it was just built for. Offline: the build fetched exactly that
/// platform's crates, and a crate only another platform needs (an Android
/// time zone reader under chrono) is in the registry cache only when some
/// other build happened to fetch it, so an unfiltered graph fails asking
/// for it.
fn crate_graph() -> Result<CrateGraph, String> {
    let host = host_platform()?;
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--offline", "--filter-platform", &host])
        .stderr(Stdio::inherit())
        .output()
        .map_err(|e| format!("start cargo metadata: {e}"))?;
    if !output.status.success() {
        return Err("cargo metadata failed (its error is above)".into());
    }
    let metadata = serde_json::from_slice(&output.stdout).map_err(|e| format!("cargo metadata does not read: {e}"))?;
    read_graph(&metadata)
}

/// The platform `rustc` builds for here, as `rustc -vV` names it.
fn host_platform() -> Result<String, String> {
    let output = Command::new("rustc").arg("-vV").output().map_err(|e| format!("start rustc -vV: {e}"))?;
    if !output.status.success() {
        return Err(format!("rustc -vV failed: {}", String::from_utf8_lossy(&output.stderr)));
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(|host| host.trim().to_string())
        .ok_or_else(|| "rustc -vV names no host".into())
}

/// A [`CrateGraph`] from `cargo metadata --format-version 1`.
fn read_graph(metadata: &serde_json::Value) -> Result<CrateGraph, String> {
    let resolve = &metadata["resolve"];
    let root = resolve["root"].as_str().ok_or("cargo metadata names no root crate")?.to_string();
    let mut graph = CrateGraph { root, ..Default::default() };
    for node in resolve["nodes"].as_array().into_iter().flatten() {
        let Some(id) = node["id"].as_str() else { continue };
        let deps = node["deps"].as_array().into_iter().flatten().filter_map(|dep| dep["pkg"].as_str().map(str::to_string)).collect();
        graph.deps.insert(id.to_string(), deps);
    }
    for package in metadata["packages"].as_array().into_iter().flatten() {
        if let (Some(id), Some(name), Some(version)) = (package["id"].as_str(), package["name"].as_str(), package["version"].as_str()) {
            graph.packages.insert(id.to_string(), (name.to_string(), version.to_string()));
        }
    }
    Ok(graph)
}

impl CrateGraph {
    /// `from` and every crate it reaches, never walking into `stop`.
    fn reach(&self, from: &str, stop: &BTreeSet<&str>) -> BTreeSet<String> {
        let mut seen = BTreeSet::new();
        let mut todo = vec![from.to_string()];
        while let Some(id) = todo.pop() {
            if stop.contains(id.as_str()) || !seen.insert(id.clone()) {
                continue;
            }
            todo.extend(self.deps.get(&id).into_iter().flatten().cloned());
        }
        seen
    }
}

/// Every failure of the build, as (package id, line), compile errors first
/// and then the build scripts that failed when they ran, each in the order
/// cargo printed it.
fn failures(report: &BuildReport, graph: &CrateGraph) -> Vec<(String, String)> {
    let mut all: Vec<(String, String)> = report.errors.iter().map(|e| (e.package_id.clone(), e.line.clone())).collect();
    for failure in &report.script_failures {
        for (id, (name, version)) in &graph.packages {
            if *name == failure.name && *version == failure.version {
                all.push((id.clone(), failure.line.clone()));
            }
        }
    }
    all
}

/// Kept: every candidate cargo produced a library for. Left out: every
/// other one a failure traces to, carrying the first failure in itself or
/// in a crate it depends on. The answer is an error, and the build fails,
/// when leaving packages out cannot fix the build: a failure in the worker
/// crate or in a crate it reaches other than through a candidate (the
/// engine), a candidate cargo built nothing for with no failure tracing to
/// it ([`nothing_traced`]), or nothing to leave out at all.
fn decide(candidates: &[Candidate], report: &BuildReport, graph: &CrateGraph) -> Result<Decision, String> {
    let failures = failures(report, graph);
    let ids: Vec<Option<&str>> = candidates
        .iter()
        .map(|c| graph.deps.keys().map(String::as_str).find(|id| names_crate(id, &c.crate_dir)))
        .collect();
    let candidate_ids: BTreeSet<&str> = ids.iter().flatten().copied().collect();
    let worker_own = graph.reach(&graph.root, &candidate_ids);
    if let Some((_, line)) = failures.iter().find(|(id, _)| worker_own.contains(id)) {
        return Err(format!("the worker did not compile, and none of its node packages is the cause: {line}"));
    }
    let mut decision = Decision { left_out: Vec::new(), healthy: Vec::new() };
    let mut untraced = Vec::new();
    for (candidate, id) in candidates.iter().zip(&ids) {
        if report.libraries.iter().any(|built| names_crate(built, &candidate.crate_dir)) {
            decision.healthy.push(candidate.feature.clone());
            continue;
        }
        let reached = id.map(|id| graph.reach(id, &BTreeSet::new())).unwrap_or_default();
        match failures.iter().find(|(failed, _)| reached.contains(failed)) {
            Some((_, line)) => decision.left_out.push(LeftOutPackage {
                package: candidate.package.clone(),
                node_types: candidate.node_types.clone(),
                error: line.clone(),
            }),
            None => untraced.push(candidate.package.as_str()),
        }
    }
    if !untraced.is_empty() || decision.left_out.is_empty() {
        return Err(nothing_traced(&untraced));
    }
    Ok(decision)
}

/// Why a build fails when cargo stopped and no error it printed traces to
/// a package: `unbuilt` are the packages it built nothing for.
fn nothing_traced(unbuilt: &[&str]) -> String {
    let which = match unbuilt {
        [] => String::new(),
        [one] => format!(" (it built nothing for the package {one})"),
        many => format!(" (it built nothing for the packages {})", many.join(", ")),
    };
    format!(
        "cargo stopped before printing an error that traces to any node package{which}, so no package is left out and the \
         build fails. This is usually something outside the project's code, like crates.io or the network being unreachable, \
         or the build machine running out of disk or memory: cargo's own output above, in the build log, says which. Once \
         that is over, build again: a failed build leaves no image behind, so the next one tries every package again."
    )
}

/// The record as JSON within `limit` bytes: every error is cut shorter
/// until it fits, never below [`SHORTEST_ERROR`] characters, and never a
/// package or a node type dropped. Too many packages to fit is an error
/// naming them.
fn fit_record(packages: &[LeftOutPackage], limit: usize) -> Result<String, String> {
    let mut cap = packages.iter().map(|p| p.error.chars().count()).max().unwrap_or(0);
    loop {
        let record = Record {
            packages: packages.iter().map(|p| LeftOutPackage { error: shorten(&p.error, cap), ..p.clone() }).collect(),
        };
        let json = serde_json::to_string(&record).map_err(|e| format!("write the record: {e}"))?;
        if json.len() <= limit {
            return Ok(json);
        }
        if cap <= SHORTEST_ERROR {
            let names: Vec<&str> = packages.iter().map(|p| p.package.as_str()).collect();
            return Err(format!(
                "the record of the packages left out of the worker does not fit in {limit} bytes, even with every error cut to \
                 {SHORTEST_ERROR} characters; these packages did not compile: {}",
                names.join(", ")
            ));
        }
        let step = (json.len() - limit).div_ceil(packages.len()).max(1);
        cap = cap.saturating_sub(step).max(SHORTEST_ERROR);
    }
}

/// `text` cut to `cap` characters, the cut marked with "...".
fn shorten(text: &str, cap: usize) -> String {
    if text.chars().count() <= cap {
        return text.to_string();
    }
    let kept: String = text.chars().take(cap.saturating_sub(3)).collect();
    format!("{kept}...")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The platform the graph is filtered to is rustc's own, a full target
    /// triple.
    #[test]
    fn the_host_platform_is_rustcs_target_triple() {
        let host = host_platform().unwrap();
        assert!(host.split('-').count() >= 3 && !host.contains(char::is_whitespace), "{host}");
    }

    /// A trimmed `cargo build --keep-going --message-format=json` stream
    /// from a worker with two package crates: `pkg_basic` compiled,
    /// `pkg_media` failed with one error (plus cargo's closing "aborting"
    /// error), in the new package id spelling; a non-JSON line; a warning.
    const ONE_FAILS: &str = r#"{"reason":"compiler-artifact","package_id":"registry+https://github.com/rust-lang/crates.io-index#serde@1.0.219","target":{"kind":["lib"],"name":"serde"},"fresh":true}
{"reason":"compiler-message","package_id":"path+file:///work/pkg_basic-1111aaaa#pkg_basic@0.1.0","message":{"level":"warning","message":"unused variable: `x`","rendered":"warning: unused variable: `x`\n","spans":[]}}
{"reason":"compiler-artifact","package_id":"path+file:///work/pkg_basic-1111aaaa#pkg_basic@0.1.0","target":{"kind":["lib"],"name":"pkg_basic"},"fresh":false}
{"reason":"compiler-message","package_id":"path+file:///work/pkg_media-2222bbbb#pkg_media@0.1.0","message":{"level":"error","message":"mismatched types\nexpected u32","rendered":"error[E0308]: mismatched types\n --> /weft/project-nodes/nodes/media/resize/mod.rs:12:5\n","spans":[{"file_name":"/weft/project-nodes/nodes/media/resize/mod.rs","line_start":12,"is_primary":true}]}}
{"reason":"compiler-message","package_id":"path+file:///work/pkg_media-2222bbbb#pkg_media@0.1.0","message":{"level":"error","message":"aborting due to 1 previous error","rendered":"error: aborting due to 1 previous error\n","spans":[]}}
not a message
{"reason":"build-finished","success":false}
"#;

    fn candidate(name: &str, dir: &str, types: &[&str]) -> Candidate {
        Candidate {
            feature: format!("pkg_{name}"),
            crate_dir: dir.into(),
            package: name.into(),
            node_types: types.iter().map(|t| t.to_string()).collect(),
        }
    }

    fn two_packages() -> Vec<Candidate> {
        vec![candidate("basic", "pkg_basic-1111aaaa", &["Text"]), candidate("media", "pkg_media-2222bbbb", &["Resize", "Crop"])]
    }

    const WORKER: &str = "path+file:///work#weft-worker@0.1.0";
    const BASIC: &str = "path+file:///work/pkg_basic-1111aaaa#pkg_basic@0.1.0";
    const MEDIA: &str = "path+file:///work/pkg_media-2222bbbb#pkg_media@0.1.0";
    const CORE: &str = "path+file:///weft/crates/weft-core#0.1.0";
    const SERDE: &str = "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.219";
    const IMAGE: &str = "registry+https://github.com/rust-lang/crates.io-index#image@0.25.0";
    const PKG_CONFIG: &str = "registry+https://github.com/rust-lang/crates.io-index#pkg-config@0.3.30";

    /// The worker's crates as `cargo metadata` answers them: the worker on
    /// weft-core and both packages; both packages on weft-core; media on
    /// `image`, and on `pkg-config` as a build-dependency.
    fn graph() -> CrateGraph {
        let metadata = serde_json::json!({
            "packages": [
                { "id": WORKER, "name": "weft-worker", "version": "0.1.0" },
                { "id": BASIC, "name": "pkg_basic", "version": "0.1.0" },
                { "id": MEDIA, "name": "pkg_media", "version": "0.1.0" },
                { "id": CORE, "name": "weft-core", "version": "0.1.0" },
                { "id": SERDE, "name": "serde", "version": "1.0.219" },
                { "id": IMAGE, "name": "image", "version": "0.25.0" },
                { "id": PKG_CONFIG, "name": "pkg-config", "version": "0.3.30" },
            ],
            "resolve": {
                "root": WORKER,
                "nodes": [
                    { "id": WORKER, "deps": [{ "pkg": CORE }, { "pkg": BASIC }, { "pkg": MEDIA }] },
                    { "id": BASIC, "deps": [{ "pkg": CORE }] },
                    { "id": MEDIA, "deps": [{ "pkg": CORE }, { "pkg": IMAGE }, { "pkg": PKG_CONFIG, "dep_kinds": [{ "kind": "build" }] }] },
                    { "id": CORE, "deps": [{ "pkg": SERDE }] },
                    { "id": SERDE, "deps": [] },
                    { "id": IMAGE, "deps": [] },
                    { "id": PKG_CONFIG, "deps": [] },
                ],
            },
        });
        read_graph(&metadata).expect("the metadata reads")
    }

    fn read(stream: &str) -> (BuildReport, String) {
        let mut echoed = String::new();
        let report = read_messages(stream.lines().map(str::to_string), &mut |text| echoed.push_str(text));
        (report, echoed)
    }

    fn library(id: &str) -> String {
        format!(r#"{{"reason":"compiler-artifact","package_id":"{id}","target":{{"kind":["lib"]}}}}"#)
    }

    fn error(id: &str, rendered: &str) -> String {
        format!(r#"{{"reason":"compiler-message","package_id":"{id}","message":{{"level":"error","message":"x","rendered":"{rendered}\n","spans":[]}}}}"#)
    }

    /// The package that failed is left out with its first error and where
    /// it points; the one that built stays; every diagnostic and stray
    /// line reaches the log.
    #[test]
    fn a_failing_package_is_left_out_and_a_built_one_kept() {
        let (report, echoed) = read(ONE_FAILS);
        assert!(echoed.contains("warning: unused variable") && echoed.contains("error[E0308]") && echoed.contains("not a message\n"), "{echoed}");
        let decision = decide(&two_packages(), &report, &graph()).unwrap();
        assert_eq!(decision.healthy, vec!["pkg_basic".to_string()]);
        assert_eq!(
            decision.left_out,
            vec![LeftOutPackage {
                package: "media".into(),
                node_types: vec!["Resize".into(), "Crop".into()],
                error: "error[E0308]: mismatched types at /weft/project-nodes/nodes/media/resize/mod.rs:12".into(),
            }]
        );
    }

    /// A package whose dependency failed built nothing and has no error of
    /// its own: it is left out naming its dependency's error.
    #[test]
    fn a_failing_dependency_leaves_its_package_out() {
        let stream = [library(BASIC), error(IMAGE, "error: cannot find macro `x`")].join("\n");
        let decision = decide(&two_packages(), &read(&stream).0, &graph()).unwrap();
        assert_eq!(decision.healthy, vec!["pkg_basic".to_string()]);
        assert_eq!(decision.left_out.len(), 1);
        assert_eq!(decision.left_out[0].error, "error: cannot find macro `x`");
    }

    /// A build-dependency that does not compile stops the package's build
    /// script, so cargo never builds its library: the package is left out
    /// naming the build-dependency's error, the other one kept.
    #[test]
    fn a_failing_build_dependency_leaves_its_package_out() {
        let stream = [library(BASIC), error(PKG_CONFIG, "error[E0425]: cannot find value `y`")].join("\n");
        let decision = decide(&two_packages(), &read(&stream).0, &graph()).unwrap();
        assert_eq!(decision.healthy, vec!["pkg_basic".to_string()]);
        assert_eq!(decision.left_out.len(), 1);
        assert_eq!(decision.left_out[0].package, "media");
        assert_eq!(decision.left_out[0].error, "error[E0425]: cannot find value `y`");
    }

    /// A build script that compiles but panics when it runs gets no JSON
    /// message; cargo's stderr names it, and the package is left out with
    /// the panic's message. Every line still reaches the log.
    #[test]
    fn a_crashing_build_script_leaves_its_package_out() {
        let stderr = [
            "   Compiling pkg_media v0.1.0 (/work/pkg_media-2222bbbb)",
            "error: failed to run custom build command for `pkg_media v0.1.0 (/work/pkg_media-2222bbbb)`",
            "",
            "Caused by:",
            "  process didn't exit successfully: `/cache/target/release/build/pkg_media-9a74/build-script-build` (exit status: 101)",
            "  --- stderr",
            "",
            "  thread 'main' (2749348) panicked at /weft/project-nodes/nodes/media/build.rs:3:5:",
            "  libvips was not found",
            "  note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
            "warning: build failed, waiting for other jobs to finish...",
        ]
        .join("\n");
        let mut echoed = Vec::new();
        let failures = read_script_failures(stderr.lines().map(str::to_string), &mut |line| echoed.push(line.to_string()));
        assert_eq!(echoed.len(), stderr.lines().count());
        assert_eq!(
            failures,
            vec![ScriptFailure {
                name: "pkg_media".into(),
                version: "0.1.0".into(),
                line: "the build script of `pkg_media v0.1.0` failed: libvips was not found".into(),
            }]
        );
        let mut report = read(&library(BASIC)).0;
        report.script_failures = failures;
        let decision = decide(&two_packages(), &report, &graph()).unwrap();
        assert_eq!(decision.healthy, vec!["pkg_basic".to_string()]);
        assert_eq!(decision.left_out.len(), 1);
        assert_eq!(decision.left_out[0].error, "the build script of `pkg_media v0.1.0` failed: libvips was not found");
    }

    /// A build script that fails without panicking is named by the first
    /// line it wrote.
    #[test]
    fn a_build_script_that_exits_is_named_by_what_it_wrote() {
        let stderr = "error: failed to run custom build command for `image v0.25.0`\n\nCaused by:\n  --- stderr\n  nasm not found\n";
        let failures = read_script_failures(stderr.lines().map(str::to_string), &mut |_| {});
        assert_eq!(failures[0].line, "the build script of `image v0.25.0` failed: nasm not found");
    }

    /// A package cargo built no library for, with no error anywhere it
    /// reaches (crates.io down, the network gone mid-build), is never left
    /// out: the build fails naming it and saying to build again, so no
    /// image keeps the outage. The same holds when cargo built nothing at
    /// all, and when another package beside it is left out for a real
    /// error.
    #[test]
    fn a_package_that_did_not_build_for_no_printed_reason_fails_the_build() {
        let err = decide(&two_packages(), &read(&library(BASIC)).0, &graph()).unwrap_err();
        assert!(err.contains("before printing an error that traces to any node package (it built nothing for the package media)"), "{err}");
        assert!(err.contains("build again"), "{err}");

        let err = decide(&two_packages(), &BuildReport::default(), &graph()).unwrap_err();
        assert!(err.contains("the packages basic, media"), "{err}");

        let mut graph = graph();
        graph.deps.get_mut(MEDIA).unwrap().retain(|dep| dep != IMAGE);
        graph.deps.get_mut(BASIC).unwrap().push(IMAGE.into());
        let err = decide(&two_packages(), &read(&error(IMAGE, "error: boom")).0, &graph).unwrap_err();
        assert!(err.contains("the package media)"), "{err}");
    }

    /// Cargo failed but built every package and printed no error: there is
    /// nothing to leave out, and the build fails the same way.
    #[test]
    fn a_failed_build_that_built_every_package_fails() {
        let stream = [library(BASIC), library(MEDIA)].join("\n");
        let err = decide(&two_packages(), &read(&stream).0, &graph()).unwrap_err();
        assert!(err.contains("traces to any node package, so no package is left out"), "{err}");
    }

    /// A library only packages share failing takes all of them out.
    #[test]
    fn a_failing_library_the_packages_share_takes_them_all_out() {
        let mut graph = graph();
        graph.deps.get_mut(BASIC).unwrap().push(IMAGE.into());
        let decision = decide(&two_packages(), &read(&error(IMAGE, "error: boom")).0, &graph).unwrap();
        assert!(decision.healthy.is_empty());
        assert_eq!(decision.left_out.iter().map(|p| p.error.as_str()).collect::<Vec<_>>(), vec!["error: boom", "error: boom"]);
    }

    /// Every package built but the worker crate itself failed: leaving
    /// packages out cannot fix it, and the build fails naming it.
    #[test]
    fn a_failure_in_the_worker_crate_fails_the_build() {
        let stream = [library(BASIC), library(MEDIA), error(WORKER, "error: oops")].join("\n");
        let err = decide(&two_packages(), &read(&stream).0, &graph()).unwrap_err();
        assert!(err.contains("none of its node packages is the cause: error: oops"), "{err}");
    }

    /// The engine (a crate the worker reaches other than through a
    /// package) not compiling builds no package, but blames none: the build
    /// fails naming it.
    #[test]
    fn a_failure_every_package_shares_fails_the_build() {
        let err = decide(&two_packages(), &read(&error(CORE, "error: oops")).0, &graph()).unwrap_err();
        assert!(err.contains("error: oops"), "{err}");
    }

    /// A crate directory names exactly its own crate, not one whose name
    /// it begins.
    #[test]
    fn a_package_id_names_only_its_own_crate_directory() {
        assert!(names_crate("path+file:///work/pkg_a-1#pkg_a@0.1.0", "pkg_a-1"));
        assert!(names_crate("pkg_a 0.1.0 (path+file:///work/pkg_a-1)", "pkg_a-1"));
        assert!(!names_crate("path+file:///work/pkg_a-12#pkg_a@0.1.0", "pkg_a-1"));
    }

    fn left(name: &str, error_len: usize) -> LeftOutPackage {
        LeftOutPackage { package: name.into(), node_types: vec![format!("{name}Type")], error: "e".repeat(error_len) }
    }

    /// A record that fits is written whole, and it reads as weft-core's.
    #[test]
    fn a_record_that_fits_is_kept_whole() {
        let json = fit_record(&[left("a", 300)], RECORD_LIMIT).unwrap();
        let record: weft_core::builds::LeftOut = weft_core::builds::LeftOut::parse(json.as_bytes()).unwrap();
        assert_eq!(record.packages[0].error.len(), 300);
        assert_eq!(fit_record(&[], RECORD_LIMIT).unwrap(), r#"{"packages":[]}"#);
    }

    /// A record too large has every error cut (marked "...") until it fits,
    /// every package and node type kept; past the shortest an error may
    /// get, the build fails naming the packages.
    #[test]
    fn a_record_too_large_is_cut_to_fit_or_refused() {
        let packages: Vec<LeftOutPackage> = (0..10).map(|i| left(&format!("p{i}"), 2000)).collect();
        let json = fit_record(&packages, RECORD_LIMIT).unwrap();
        assert!(json.len() <= RECORD_LIMIT, "{}", json.len());
        let record: Record = serde_json::from_str(&json).unwrap();
        assert_eq!(record.packages.len(), 10);
        assert!(record.packages.iter().all(|p| p.error.ends_with("...") && p.error.chars().count() >= SHORTEST_ERROR && p.node_types.len() == 1));

        let too_many: Vec<LeftOutPackage> = (0..60).map(|i| left(&format!("p{i}"), 2000)).collect();
        let err = fit_record(&too_many, RECORD_LIMIT).unwrap_err();
        assert!(err.contains("p0") && err.contains("p59"), "{err}");
    }
}
