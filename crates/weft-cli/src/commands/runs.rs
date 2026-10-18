//! `weft runs`, `weft events`, `weft clean`. Journal inspection
//! and cleanup. Graph view replay is an extension command; these are
//! the scripting surface.

use anyhow::Context;
use weft_core::program::RunPage;

use super::{utc_time, Ctx};

/// A value put into a query string. A node id is the author's own
/// spelling, so it can hold anything they typed; only the handful of
/// characters that would end the value or start another parameter have
/// to move, and everything else stays readable in a log line.
fn query_escaped(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '&' => "%26".to_string(),
            '=' => "%3D".to_string(),
            '#' => "%23".to_string(),
            '+' => "%2B".to_string(),
            '%' => "%25".to_string(),
            ' ' => "%20".to_string(),
            other => other.to_string(),
        })
        .collect()
}

/// One page of the dispatcher's run listing. Anything that does
/// not read as one is a broken contract and fails loudly rather than
/// reading as "no runs".
async fn runs_page(
    client: &crate::client::DispatcherClient,
    filter: &ListFilter,
) -> anyhow::Result<RunPage> {
    let mut path =
        format!("/runs?limit={}&offset={}", filter.limit, filter.offset);
    if let Some(p) = &filter.project {
        path.push_str(&format!("&project_id={p}"));
    }
    if let Some(p) = filter.phase {
        path.push_str(&format!("&phase={}", p.as_str()));
    }
    if let Some(node) = &filter.node {
        path.push_str(&format!("&entry_node={}", query_escaped(node)));
    }
    if let Some(since) = filter.since {
        path.push_str(&format!("&started_after={since}"));
    }
    if let Some(status) = &filter.status {
        path.push_str(&format!("&status={status}"));
    }
    if let Some(instance) = &filter.instance {
        path.push_str(&format!("&instance={instance}"));
    }
    if let Some(tag) = &filter.tag {
        path.push_str(&format!("&tag={}", query_escaped(tag)));
    }
    if let Some(through) = &filter.through {
        path.push_str(&format!("&node={}", query_escaped(through)));
    }
    if let Some(search) = &filter.search {
        path.push_str(&format!("&search={}", query_escaped(search)));
    }
    serde_json::from_value(client.get_json(&path).await?).context("read the runs listing")
}

/// What `weft runs` narrows the listing to. One struct rather
/// than six positional arguments, so adding the next filter does not
/// re-thread every call site.
#[derive(Debug, Clone)]
pub struct ListFilter {
    pub limit: u32,
    pub offset: u32,
    pub project: Option<String>,
    pub phase: Option<weft_core::context::Phase>,
    /// The node whose firing started the run.
    pub node: Option<String>,
    /// Unix second: only runs that started at or after it.
    pub since: Option<u64>,
    /// Where the run stands.
    pub status: Option<weft_core::program::RunStatus>,
    /// Which instance the run is in.
    pub instance: Option<weft_core::instance::InstanceId>,
    /// A tag the run carries.
    pub tag: Option<String>,
    /// A node that fired in the run.
    pub through: Option<String>,
    /// Words the finished run carried among what it recorded.
    pub search: Option<String>,
}

pub async fn list(ctx: Ctx, filter: ListFilter) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let page = runs_page(&client, &filter).await?;
    if ctx.json_out(&serde_json::to_value(&page)?)? {
        return Ok(());
    }
    if page.runs.is_empty() {
        println!("(no runs)");
        return Ok(());
    }
    println!(
        "{:<36}  {:<9}  {:<13}  {:<23}  {:<36}  entryNode  tags",
        "runId", "status", "phase", "started", "projectId"
    );
    for row in &page.runs {
        let run_id = row.run_id.to_string();
        let project = row.project_id.to_string();
        let (status, phase, entry) = (&row.status, row.phase.as_str(), &row.entry_node);
        // The tags the run put on itself (`ctx.tag_run`), the
        // handle a sibling's `ctx.stop_tagged` selects on.
        let tags = if row.tags.is_empty() { String::new() } else { format!("  {}", row.tags.join(",")) };
        // Which instance the run is in, when it is in one.
        let instance = row.instance.as_ref().map(|m| format!("  (instance {m})")).unwrap_or_default();
        println!(
            "{run_id:<36}  {status:<9}  {phase:<13}  {:<23}  {project:<36}  {entry}{tags}{instance}",
            utc_time(row.started_at)
        );
    }
    // The server clamps the page size, so a big --limit can come back
    // short; say so rather than letting the page read as the total.
    // It does NOT say "raise --limit": past the server's cap that is
    // advice the CLI knows will not work.
    let shown = page.runs.len() as u64;
    if shown < page.total {
        println!(
            "showing {shown} of {} (one page; the dispatcher caps how many a page can hold, \
             so walk the rest with --offset {}, or narrow with --node / --since)",
            page.total,
            filter.offset as u64 + shown
        );
    }
    Ok(())
}

/// What `weft events` keeps and how much of each row it shows. The
/// default is the compact read a person or an agent can hold for a
/// long run: every row, values cut short. The filters narrow to one
/// node or one kind of event, and `full` opens the values.
#[derive(Debug, Default, Clone)]
pub struct EventsFilter {
    pub node: Option<String>,
    /// The call sites the named node's rows must run under, outermost
    /// first: what `--node auth.check` resolves to beside the node's
    /// id (`Auth.check` under `["auth"]`). Empty = any, or none.
    pub call_path: Vec<String>,
    pub kind: Option<String>,
    /// Loop iterations, outermost first (`--iteration 3` or `3.0` for the
    /// first inner iteration inside the fourth outer one): only the rows
    /// fired inside them pass. Empty = any.
    pub iteration: Vec<u32>,
    pub full: bool,
}

/// Read `--iteration`: iteration numbers from 0, outermost loop first,
/// joined by `.` (`3`, `3.0`).
pub fn parse_iteration(text: &str) -> anyhow::Result<Vec<u32>> {
    text.split('.')
        .map(|part| {
            part.trim().parse::<u32>().map_err(|_| {
                anyhow::anyhow!(
                    "--iteration takes loop iteration numbers from 0, outermost loop first, joined by '.' \
                     (`3`, or `3.0` for an inner loop's first iteration); got '{text}'"
                )
            })
        })
        .collect()
}

impl EventsFilter {
    /// Read `--node` the way the program is written: through the call
    /// sites (`auth.check`), which the compiled definition turns into
    /// the node's own id and the call path its rows carry. Without a
    /// definition the spelling is taken as the id itself. A node of an
    /// included file has no spelling of its own: it is always named
    /// through a site.
    pub fn resolve_node(
        &mut self,
        project: &weft_compiler::project::Project,
        definition: &weft_core::ProjectDefinition,
    ) -> anyhow::Result<()> {
        if let Some(spelled) = &self.node {
            // A node or a group: `--node gate` names a group, whose own
            // two boundaries are its rows too (see `keeps`), so a group
            // is taken here where the daemon-facing commands refuse it.
            let (id, call_path) = match super::resolve_spelling(project, definition, spelled)? {
                super::Spelled::Node { id, call_path } | super::Spelled::Group { id, call_path } => (id, call_path),
            };
            self.node = Some(id);
            self.call_path = call_path;
        }
        Ok(())
    }

    /// Whether one replay row survives the filters. A node filter is
    /// exact (a node's id), so a run-level row (a start, a completion,
    /// the run failing) never passes one: it names no node; and when
    /// the node was named through call sites, only the rows under
    /// that call path pass. A kind filter matches the kind exactly or
    /// as a substring, so `failed` finds both `node_failed` and
    /// `run_failed`, and `loop` finds the loop lifecycle.
    /// `row` is `event` as it reads on the wire: the kind and node filters
    /// read the wire spelling, the frame filters the typed firing.
    pub fn keeps(&self, event: &weft_core::live_event::DispatcherEvent, row: &serde_json::Value) -> bool {
        let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let kind_ok = self.kind.as_deref().is_none_or(|k| kind == k || kind.contains(k));
        // `--node gate` names the group: its own two boundaries are
        // its rows too.
        let node_ok = self.node.as_deref().is_none_or(|n| row_node(row).is_some_and(|id|
            id == n || id == weft_core::project::boundary_in_id(n) || id == weft_core::project::boundary_out_id(n)));
        let frames = event.firing_frames();
        let call_ok = self.call_path.is_empty()
            || frames.is_some_and(|frames| {
                weft_core::frames::call_path(frames) == self.call_path.iter().map(String::as_str).collect::<Vec<_>>()
            });
        // A row inside the named iterations: its own iterations, outermost
        // first, start with them (an inner loop's rows belong to the outer
        // iteration they ran in). A run-level row is inside none.
        let iteration_ok = self.iteration.is_empty()
            || frames.is_some_and(|frames| weft_core::frames::loop_indices(frames).starts_with(&self.iteration));
        kind_ok && node_ok && call_ok && iteration_ok
    }
}

/// How much of a value the compact line shows before `...`. Long
/// enough to recognise a value, short enough that a row of forty
/// nodes fits a screen.
const SUMMARY_CHARS: usize = 120;

/// The columns every line prints in its own place, plus the two every
/// row of one run repeats (they name the run, which you already have:
/// you asked for it by run). Nothing here reaches the generic tail.
const COLUMNS: &[&str] = &["kind", "node", "nodeId", "atUnix", "runId", "projectId"];

/// The node a replay row is about. Most rows name it `node`; the two
/// that come off a pulse rather than a journal row (`cost_reported`,
/// `bus_participant`) name it `nodeId`. One reader, so the filter and
/// the printed column can never disagree about which rows have a node.
fn row_node(row: &serde_json::Value) -> Option<&str> {
    row.get("node")
        .or_else(|| row.get("nodeId"))
        .and_then(|v| v.as_str())
}

/// One replay row as the line `weft events` prints: local time, the
/// kind, the node, then everything else the row carries, `key=value`,
/// in the row's own field order. The tail is generic on purpose: a
/// hand-picked key list silently swallows whatever a new event kind
/// carries (a cost's amount, a suspension's token, a loop's index),
/// and the one thing a reader wants from a row is exactly the field
/// that kind was added for. `full` prints values whole; otherwise
/// each is cut at `SUMMARY_CHARS`, on a character boundary.
pub fn event_line(row: &serde_json::Value, full: bool) -> String {
    let kind = row.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
    // Every row projected from a journal row carries the journal's
    // `atUnix`; the derived rows (a bus participant sniffed off a
    // pulse, a corruption the replay found) have none and get a blank
    // of the same width, so the columns still line up.
    let at = match row.get("atUnix").and_then(|v| v.as_u64()) {
        Some(at) => format!("[{:<23}]", utc_time(at)),
        None => " ".repeat(25),
    };
    let node = row_node(row).unwrap_or("");
    let mut line = format!("{at} {kind:<23} {node}");
    if silent_completion(row) {
        // An empty output is dropped by the tail below, exactly like
        // every other empty field, which left this line identical to a
        // node that emitted plenty. It is not: every port closed, so
        // everything downstream of it skipped, and whoever is reading
        // this is usually reading it because a value never arrived.
        line.push_str("  output=(nothing emitted)");
    }
    for (key, value) in printed_fields(row) {
        line.push_str(&format!("  {key}={}", field_text(value, full)));
    }
    line
}

/// A firing that completed without emitting on any port. The run is not
/// wrong (a node may decide it has nothing to say) but the consequence
/// is total: every output closes and the whole branch behind it skips.
/// The row carries `output` as null or an empty object depending on the
/// path that built it, and both mean the same nothing here.
fn silent_completion(row: &serde_json::Value) -> bool {
    if row.get("kind").and_then(|v| v.as_str()) != Some("node_completed") {
        return false;
    }
    match row.get("output") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Object(map)) => map.is_empty(),
        _ => false,
    }
}

/// The fields one row shows in its generic tail: everything it carries
/// except the columns printed in their own place, and except the ones
/// saying nothing (an absent value and an empty one say the same
/// nothing, and a `frames=[]` on every root-level firing is noise in
/// the column the reader is scanning).
///
/// One reader, so what a line prints and what counts as cut can never
/// disagree.
fn printed_fields(row: &serde_json::Value) -> impl Iterator<Item = (&String, &serde_json::Value)> {
    row.as_object().into_iter().flatten().filter(|(key, value)| {
        let empty = match value {
            serde_json::Value::Null => true,
            serde_json::Value::Array(items) => items.is_empty(),
            serde_json::Value::Object(map) => map.is_empty(),
            serde_json::Value::String(text) => text.is_empty(),
            _ => false,
        };
        !COLUMNS.contains(&key.as_str()) && !empty
    })
}

/// Whether the compact line for this row had to cut a value short.
///
/// The cut is silent, and the value it cuts is often the exact thing
/// being chased (a reply body, a model's answer). So the reader is told
/// `--full` exists at the one moment it would help, instead of
/// abandoning the journal for `curl`.
pub fn any_value_cut(row: &serde_json::Value) -> bool {
    printed_fields(row).any(|(_, value)| field_text(value, true).chars().count() > SUMMARY_CHARS)
}

/// One field of a replay row as the line shows it: a string bare (an
/// error message reads as itself, not as a quoted JSON string), a list
/// of strings joined, anything else as its JSON, and all of it cut to
/// `SUMMARY_CHARS` unless `full`.
fn field_text(value: &serde_json::Value, full: bool) -> String {
    let text = match value {
        serde_json::Value::String(text) => text.clone(),
        // A skip's reason is structured (`{"kind": "did_not_flow"}`);
        // its kind is the readable part, and the rest is machinery.
        serde_json::Value::Object(map) if map.len() == 1 => match map.get("kind") {
            Some(serde_json::Value::String(kind)) => kind.clone(),
            _ => serde_json::to_string(value).unwrap_or_default(),
        },
        serde_json::Value::Array(items) if items.iter().all(|i| i.is_string()) => items
            .iter()
            .filter_map(|i| i.as_str())
            .collect::<Vec<_>>()
            .join(","),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    if full || text.chars().count() <= SUMMARY_CHARS {
        return text;
    }
    let cut: String = text.chars().take(SUMMARY_CHARS.saturating_sub(3)).collect();
    format!("{cut}...")
}

pub async fn events(ctx: Ctx, run_id: String, mut filter: EventsFilter) -> anyhow::Result<()> {
    let run_id = super::resolve_run_id(&ctx, &run_id).await?;
    let client = ctx.client()?;
    // `--node` is spelled through the call sites, the way the program
    // reads; the project's compiled definition says which id and which
    // call path that is, and the rows print their node the same way.
    // That definition is this folder's project, so it only reads a run
    // of that same project: another project's run shows ids, and a
    // `--node` for it is refused, since this folder's spellings name
    // other nodes. Outside a project the spelling is the id and the
    // rows show ids. A project here that does not load (a compile
    // error, a bad `weft.toml`) only stops a `--node` filter, which
    // cannot be read without it; the journal itself still prints, by
    // id, saying why.
    let loaded = ctx.project_here().and_then(|here| match here {
        Some(project) => {
            let (definition, _) = weft_compiler::hash::load_enriched_project(project)?;
            Ok(Some((project, definition)))
        }
        None => Ok(None),
    });
    let definition = match loaded {
        Ok(Some((project, definition))) => {
            let detail: weft_core::program::RunDetail =
                serde_json::from_value(client.get_json(&format!("/runs/{run_id}")).await?)
                    .context("read the run")?;
            let run_project = detail.summary.project_id;
            if run_project == definition.id {
                filter.resolve_node(project, &definition)?;
                Some(definition)
            } else if filter.node.is_some() {
                anyhow::bail!(
                    "run {run_id} belongs to project {run_project}, not to this folder's project {}; `--node` is read \
                     through this folder's program, so run `weft events` from that project's folder",
                    definition.id
                );
            } else {
                eprintln!(
                    "warning: nodes show as ids, because run {run_id} belongs to project {run_project}, not to this folder's project {}",
                    definition.id
                );
                None
            }
        }
        Ok(None) => None,
        Err(e) if filter.node.is_some() => {
            return Err(e.context("`--node` is read the way the program is written, so this folder's project must load"));
        }
        Err(e) => {
            eprintln!("warning: nodes show as ids, because this folder's project did not load: {e:#}");
            None
        }
    };
    // Typed first, so a row the dispatcher and this CLI disagree on
    // fails here by name; the printing below then reads each row as the
    // JSON object it is on the wire, since its generic tail prints
    // whatever fields a kind carries.
    let typed = super::versions::replay_rows(&client, &run_id).await?;
    let arr = typed.iter().map(serde_json::to_value).collect::<Result<Vec<_>, _>>()?;
    let kept: Vec<serde_json::Value> = typed.iter().zip(&arr).filter(|(event, row)| filter.keeps(&event.event, row))
        .map(|(_, row)| row)
        .filter_map(|row| match &definition { Some(definition) => spell_node(row.clone(), definition), None => Some(row.clone()) })
        .collect();
    if ctx.json_out(&kept)? {
        return Ok(());
    }
    if kept.is_empty() {
        println!(
            "(no events{})",
            if arr.is_empty() { String::new() } else { format!(" match; the run has {}", arr.len()) }
        );
        return Ok(());
    }
    for row in &kept {
        println!("{}", event_line(row, filter.full));
    }
    if !filter.full && kept.iter().any(any_value_cut) {
        println!("(some values were cut to fit; `--full` prints them whole, `--json` for a tool)");
    }
    Ok(())
}

/// The row with everything it names spelled the way the program
/// reads: a node of an included file through the site its call frames
/// say (`one.strip`), so two calls of one file read apart; a group's
/// boundary as the group (`gate`), with `boundary=in|out` saying which
/// end; the call frames, a loop's id, a skip's scope, the run's
/// subgraph, the seed's origins and the completion's outputs the same
/// way. The loop frames stay, for the iteration.
///
/// `None` for a row of an included file's own boundary: the site's
/// boundary carries the same values under the same name (`one`), and
/// the hop between the two is the compiler's, not a person's.
pub fn spell_node(mut row: serde_json::Value, project: &weft_core::ProjectDefinition) -> Option<serde_json::Value> {
    use serde_json::Value;
    use weft_core::frames::{call_path, Frame, Located, LoopFrames};
    use weft_core::project::{address_of, group_address, selection::is_body};
    let boundary_group = |id: &str| project.nodes.iter().find(|n| n.id == id).and_then(|n| n.group_boundary.as_ref());
    let boundary_of = |id: &str| boundary_group(id)
        .map(|b| match b.role { weft_core::project::GroupBoundaryRole::In => "in", weft_core::project::GroupBoundaryRole::Out => "out" });
    let spell_place = |place: &Located| address_of(project, &place.id, &place.path);
    let spell_frames = |frames: &LoopFrames| -> Vec<Value> {
        let mut above: Vec<String> = Vec::new();
        frames.iter().map(|frame| match frame {
            Frame::Loop { index } => serde_json::json!({ "index": index }),
            Frame::Call { site } => {
                let spelled = address_of(project, site, &above);
                above.push(site.clone());
                serde_json::json!({ "site": spelled })
            }
        }).collect()
    };
    let frames: LoopFrames = row.get("frames").and_then(|f| serde_json::from_value(f.clone()).ok()).unwrap_or_default();
    let path: Vec<String> = call_path(&frames).into_iter().map(str::to_string).collect();
    let key = ["node", "nodeId"].into_iter().find(|key| row.get(key).and_then(|v| v.as_str()).is_some());
    if let Some(key) = key {
        let id = row[key].as_str().unwrap_or_default().to_string();
        if boundary_group(&id).is_some_and(|b| is_body(project, &b.group_id)) { return None; }
        row[key] = Value::String(address_of(project, &id, &path));
        if let Some(role) = boundary_of(&id) { row["boundary"] = Value::String(role.into()); }
    }
    if !frames.is_empty() {
        row["frames"] = Value::Array(spell_frames(&frames));
    }
    // A loop's lifecycle rows name the loop and the frames around it.
    let parent_frames: LoopFrames = row.get("parentFrames").and_then(|f| serde_json::from_value(f.clone()).ok()).unwrap_or_default();
    if let Some(group) = row.get("groupId").and_then(|g| g.as_str()).map(str::to_string) {
        let parent_path: Vec<String> = call_path(&parent_frames).into_iter().map(str::to_string).collect();
        row["groupId"] = Value::String(group_address(project, &group, &parent_path));
    }
    if !parent_frames.is_empty() {
        row["parentFrames"] = Value::Array(spell_frames(&parent_frames));
    }
    if let Some(scope) = row.get("reason").and_then(|r| r.get("scope")).and_then(|s| s.as_str()).map(str::to_string) {
        // A site is refused in its caller's frames, one above the
        // call frame its body's rows carry.
        let scope_path = if path.last() == Some(&scope) { &path[..path.len() - 1] } else { &path[..] };
        row["reason"]["scope"] = Value::String(group_address(project, &scope, scope_path));
    }
    // A place list or map (`subgraph`, `seed.origins`, `outputs`): each
    // key spelled, boundaries folded into their group.
    let places = |value: &Value| -> Vec<(Located, Value)> {
        match value {
            Value::Array(items) => items.iter().filter_map(|i| serde_json::from_value::<Located>(i.clone()).ok().map(|p| (p, Value::Null))).collect(),
            Value::Object(map) => map.iter().filter_map(|(k, v)| serde_json::from_str::<Located>(&format!("\"{k}\"")).ok().map(|p| (p, v.clone()))).collect(),
            _ => Vec::new(),
        }
    };
    if let Some(subgraph) = row.get("subgraph").cloned() {
        let mut seen = std::collections::BTreeSet::new();
        row["subgraph"] = Value::Array(places(&subgraph).into_iter()
            .filter(|(place, _)| boundary_of(&place.id).is_none())
            .map(|(place, _)| spell_place(&place)).filter(|s| seen.insert(s.clone())).map(Value::String).collect());
    }
    for key in ["outputs", "origins"] {
        let holder = if key == "origins" { row.get_mut("seed") } else { Some(&mut row) };
        let Some(holder) = holder else { continue };
        if let Some(map) = holder.get(key).cloned().filter(|v| v.is_object()) {
            let spelled: serde_json::Map<String, Value> = places(&map).into_iter()
                .filter(|(place, _)| boundary_of(&place.id).is_none())
                .map(|(place, v)| (spell_place(&place), v)).collect();
            holder[key] = Value::Object(spelled);
        }
    }
    Some(row)
}

/// What narrows a bulk clean beyond project and age: the same filters a
/// program's `ctx.runs()` takes (`weft_core::program::RunFilter`).
#[derive(Debug, Default)]
pub struct CleanNarrowing {
    pub instance: Option<weft_core::instance::InstanceId>,
    pub status: Option<weft_core::program::RunStatus>,
    pub node: Option<String>,
    pub tag: Option<String>,
    /// Cancel matching runs still going, instead of leaving them.
    pub cancel_running: bool,
}

pub async fn clean(
    ctx: Ctx,
    run_id: Option<String>,
    keep_days: Option<u32>,
    all: bool,
    images: bool,
    build_cache: bool,
    project: Option<String>,
    narrow: CleanNarrowing,
    yes: bool,
) -> anyhow::Result<()> {
    if images || build_cache {
        if images {
            clean_build_images(&ctx, all).await?;
        }
        if build_cache {
            clean_build_cache().await?;
        }
        return Ok(());
    }

    // A journal row deleted is gone for good, so every run
    // deletion is confirmed: a terminal is asked, a script says
    // `--yes`. The sweep says how many rows it is about to take.
    let confirm = |what: String| -> anyhow::Result<bool> {
        if yes {
            return Ok(true);
        }
        println!("About to delete {what}.");
        let ok = crate::prompt::confirm("Type 'yes' to confirm: ", "--yes")?;
        if !ok {
            println!("aborted");
        }
        Ok(ok)
    };

    let client = ctx.client()?;
    let project = project
        .map(|p| p.parse::<uuid::Uuid>().map_err(|_| anyhow::anyhow!("--project takes a project id, and '{p}' is not one")))
        .transpose()?;
    if let Some(c) = run_id {
        anyhow::ensure!(
            project.is_none(),
            "a run names ONE run, so --project cannot narrow it further: \
             drop one of them"
        );
        let c = super::resolve_run_id(&ctx, &c).await?;
        if !confirm(format!("run {c}"))? {
            return Ok(());
        }
        let deleted: weft_core::program::DeletedRun =
            serde_json::from_value(client.delete_json(&format!("/runs/{c}")).await?)
                .context("read what the delete removed")?;
        println!("deleted {c}");
        // The sweep of whatever version this run left bare happens on the
        // dispatcher, inside the delete: the run's own project is the one
        // to sweep, and a run can be cleaned from anywhere, so no
        // client is in a position to know it. This just reports it.
        let swept = deleted.swept.len();
        if swept > 0 {
            println!("dropped {swept} bare versions");
        }
        return Ok(());
    }

    // Bulk clean. Naming a SUBJECT means you mean all of it (a run
    // deletes outright; a project, an instance or a tag takes every run it
    // names), so the 30-day default guards only the sweep that names
    // nothing. `--keep-days` still narrows any of them when asked for.
    let named = project.is_some() || narrow.instance.is_some() || narrow.tag.is_some();
    let days = match (keep_days, all, named) {
        (Some(d), _, _) => Some(d),
        (None, true, _) => None,  // --all: no cutoff
        (None, false, true) => None,  // a named subject: all of its runs
        (None, false, false) => Some(30),  // the unnamed sweep's guard
    };
    let mut scope = match &project {
        Some(p) => format!(" of project {p}"),
        None => String::new(),
    };
    if let Some(instance) = &narrow.instance {
        scope.push_str(&format!(" in instance {instance}"));
    }
    if let Some(tag) = &narrow.tag {
        scope.push_str(&format!(" tagged {tag}"));
    }
    if let Some(status) = &narrow.status {
        scope.push_str(&format!(" {status}"));
    }
    if let Some(node) = &narrow.node {
        scope.push_str(&format!(" started by {node}"));
    }
    let subject = match days {
        Some(d) => format!("every run{scope} older than {d} days"),
        None => format!("every run{scope}"),
    };
    if !confirm(subject)? {
        return Ok(());
    }
    let filter = weft_core::program::RunFilter {
        instance: narrow.instance,
        status: narrow.status,
        node: narrow.node,
        tag: narrow.tag,
        older_than_secs: days.map(|d| d as u64 * 24 * 3600),
    };
    // The dispatcher deletes what the filter reaches and sweeps the
    // versions the deletes left bare, project by project; a run still
    // going follows `--cancel-running` (stopped now, its rows gone with
    // the next clean) or is left to finish.
    let body = weft_core::program::CleanRequest {
        project,
        filter,
        running: if narrow.cancel_running { weft_core::RunningPolicy::Cancel } else { weft_core::RunningPolicy::Wait },
    };
    let answer: weft_core::program::CleanOutcome = serde_json::from_value(client.post_json("/runs/clean", &serde_json::to_value(&body)?).await?)
        .map_err(|e| anyhow::anyhow!("unexpected /runs/clean answer: {e}"))?;
    if ctx.json_out(&serde_json::to_value(&answer)?)? {
        return Ok(());
    }
    match days {
        Some(d) => println!("deleted {} runs{scope} older than {d}d", answer.deleted),
        None => println!("deleted {} runs{scope}", answer.deleted),
    }
    if answer.cancelled > 0 {
        println!("cancelled {} still running (their rows go with the next clean)", answer.cancelled);
    }
    if answer.left_running > 0 {
        println!("left {} still running (--cancel-running stops them)", answer.left_running);
    }
    Ok(())
}


/// Reclaim the images builds produce that nothing runs any more (worker
/// and infra images, node-test leftovers, and under `--all` old builder
/// bases, runtimes and compile caches). Each layer is its own sweep, none
/// gating another (a layer that fails is reported at the end, after every
/// other layer ran):
///
///   1. The images the install built: it deletes each one its keep-set
///      does not cover (`POST /images/prune`), the cwd project's or, with
///      `--all`, every project's. The install is the only one that
///      deletes them: every build claims the images it relies on until
///      they are registered, so a prune never takes an image a build just
///      found or made (it reports it in use), and it forgets what it
///      deleted from its ledger.
///   2. Dangling (untagged) leftovers of the node-test images this host
///      built (`weft test-node`), which the install never saw: those
///      carrying the cwd project's `weft.dev/project` label, or any value
///      with `--all`.
///   3. With `--all` only: the shared images no install on this machine
///      uses (builder bases, node-test images, runtimes, registry-named
///      standard workers): each engine change mints fresh ones, and every
///      install on the machine may run a different version
///      ([`crate::image_refs`]).
///   4. With `--all` only: the worker compile caches no install uses.
async fn clean_build_images(ctx: &Ctx, all: bool) -> anyhow::Result<()> {
    // The scope, settled BEFORE any layer runs: a scoped clean that cannot
    // name its project must delete nothing, not run the global layers and
    // report the error afterwards.
    let scope = if all {
        println!("reclaiming the images no live project references");
        Scope::All
    } else {
        let project = ctx.project().map_err(|e| {
            anyhow::anyhow!("{e}; pass --all to clean every project's images")
        })?;
        println!(
            "reclaiming the images of project {} ({}) nothing references",
            project.manifest.package.name,
            project.id()
        );
        Scope::Project(project.id())
    };

    let mut failures: Vec<anyhow::Error> = Vec::new();
    let mut layer = |name: &str, result: anyhow::Result<()>| {
        if let Err(e) = result {
            failures.push(e.context(name.to_string()));
        }
    };
    layer("built images", registry_prune(ctx, &scope).await);
    layer("dangling node-test leftovers", dangling_prune(&scope).await);
    if all {
        let data = super::daemon::data_dir();
        for kind in SharedImages::ALL {
            layer(kind.layer(), unused_image_sweep(&data, kind).await);
        }
        layer("unused compile caches", unused_compile_cache_sweep(&data).await);
    }
    // The compile cache every worker build shares is not an image, but it
    // is disk, so it is never invisible. Reading its size is not a reclaim:
    // a failure to read it is said, never counted against the sweep.
    if let Err(error) = report_compile_cache_size().await {
        eprintln!("could not read the worker compile cache size: {error:#}");
    }
    if failures.is_empty() {
        return Ok(());
    }
    let mut msg = format!("{} reclaim layer(s) failed:", failures.len());
    for e in &failures {
        msg.push_str(&format!("\n  {e:#}"));
    }
    anyhow::bail!("{msg}");
}

/// Layer 1: the install deletes the images it built that nothing references.
async fn registry_prune(ctx: &Ctx, scope: &Scope) -> anyhow::Result<()> {
    let request = weft_core::images::PruneRequest {
        project: match scope {
            Scope::All => None,
            Scope::Project(id) => Some(*id),
        },
    };
    let body = serde_json::to_value(&request).context("encode the prune request")?;
    let report: weft_core::images::PruneReport = serde_json::from_value(ctx.client()?.post_json("/images/prune", &body).await?)
        .context("read the install's prune report")?;
    for image in &report.removed {
        println!("  removed {image}");
    }
    for image in &report.in_use {
        println!("  kept {image}: a container still runs from it, or a build is about to register it; a later clean takes it");
    }
    if report.removed.is_empty() && report.in_use.is_empty() && report.failed.is_empty() {
        println!("no unreferenced built images");
    }
    anyhow::ensure!(
        report.failed.is_empty(),
        "{} image(s) could not be deleted:\n  {}",
        report.failed.len(),
        report.failed.iter().map(|(image, why)| format!("{image}: {why}")).collect::<Vec<_>>().join("\n  ")
    );
    Ok(())
}

/// Every project's images, or one project's.
enum Scope {
    All,
    Project(uuid::Uuid),
}

/// Layer 2: dangling (untagged) leftovers of this host's node-test builds,
/// through the `weft.dev/project` label `weft test-node` stamps on them.
async fn dangling_prune(scope: &Scope) -> anyhow::Result<()> {
    // SYNC: the label <-> crates/weft-cli/src/commands/test_node/mod.rs (the node-test image labels)
    let filter = match scope {
        Scope::All => "label=weft.dev/project".to_string(),
        Scope::Project(id) => format!("label=weft.dev/project={id}"),
    };
    let status = crate::images::docker()
        .args(["image", "prune", "--force", "--filter", "dangling=true", "--filter"])
        .arg(filter)
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker image prune exited {status}");
    }
    Ok(())
}

/// `weft clean --images --build-plane`: the builder bases and node-test
/// images no install on this machine uses, the part of layer 3 that
/// needs no running install (setup.sh runs it after every start, so an
/// engine change leaves nothing of the version it replaced). Fails only
/// when docker cannot be asked.
pub async fn clean_build_plane() -> anyhow::Result<()> {
    let data = super::daemon::data_dir();
    for kind in SharedImages::BUILD_PLANE {
        unused_image_sweep(&data, kind).await?;
    }
    Ok(())
}

/// The kinds of shared image a machine-wide sweep reclaims: the refs each
/// install records ([`crate::image_refs`]: its start writes one of each,
/// and its builds add the builder bases they pick), and
/// nothing evicts the previous one implicitly (an implicit sweep would
/// race a build FROMing it or a unit agent running it), so an explicit
/// clean is where they go. Whatever no install's record names is dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedImages {
    /// `weft-builder-base:<hash>`, about 1.4GB each.
    BuilderBase,
    /// `weft-node-tests:<hash>`, kept by the builder base they were
    /// built on.
    NodeTests,
    Runtime,
    /// The registry-named standard workers; an install's own name for
    /// one is in its ledger, and layer 1 reclaims it.
    StandardWorker,
}

impl SharedImages {
    /// Every kind, the order `--all` sweeps them in.
    const ALL: [Self; 4] = [Self::BuilderBase, Self::NodeTests, Self::Runtime, Self::StandardWorker];
    /// The kinds `--build-plane` sweeps.
    const BUILD_PLANE: [Self; 2] = [Self::BuilderBase, Self::NodeTests];

    /// What a person calls one of them.
    fn what(self) -> &'static str {
        match self {
            Self::BuilderBase => "builder-base image",
            Self::NodeTests => "node-test image",
            Self::Runtime => "runtime image",
            Self::StandardWorker => "standard worker image",
        }
    }

    /// The layer's name in a failure report.
    fn layer(self) -> &'static str {
        match self {
            Self::BuilderBase => "builder-base images",
            Self::NodeTests => "node-test images",
            Self::Runtime => "runtime images",
            Self::StandardWorker => "standard worker images",
        }
    }

    /// The refs of an install's record that keep images of this kind.
    fn recorded(self) -> fn(&weft_core::images::ImageRefs) -> Vec<&str> {
        match self {
            Self::BuilderBase | Self::NodeTests => |r| r.builder_bases.iter().map(String::as_str).collect(),
            Self::Runtime => |r| vec![r.runtime.as_str()],
            Self::StandardWorker => |r| vec![r.standard_worker.as_str()],
        }
    }
}

/// Remove the images of `kind` no install in `records` uses, or nothing
/// at all while one has not recorded what it uses (saying which).
async fn unused_image_sweep(data_dir: &std::path::Path, kind: SharedImages) -> anyhow::Result<()> {
    // Held from the look at the records through the removal it decides
    // (`crate::image_refs::Records`).
    let records = crate::image_refs::every_install(data_dir).await?;
    let kept = match crate::image_refs::kept(&records.installs, kind.recorded(), kind.what()) {
        crate::image_refs::Kept::Unknown(why) => {
            println!("{why}");
            return Ok(());
        }
        crate::image_refs::Kept::Only(kept) => kept,
    };
    let listing = host_image_listing(&[]).await?;
    let kept_refs: Vec<&str> = kept.iter().map(String::as_str).collect();
    let stale = match kind {
        SharedImages::BuilderBase => crate::images::host_images_matching(
            &listing,
            crate::images::outside_kept(weft_compiler::worker_image::BUILDER_BASE_REPO, &kept_refs)?,
        ),
        SharedImages::Runtime => crate::images::host_images_matching(&listing, crate::images::outside_kept(crate::images::RUNTIME_REPO, &kept_refs)?),
        SharedImages::StandardWorker => crate::images::stale_standard_workers(&listing, kept_refs)?,
        SharedImages::NodeTests => {
            let mut built_on_kept = String::new();
            for base in kept_refs {
                let (_, tag) = crate::images::ref_repo_tag(base)?;
                built_on_kept.push_str(&host_image_listing(&[format!("label={}={tag}", crate::images::NODE_TEST_BASE_LABEL)]).await?);
            }
            crate::images::stale_node_test_images(&listing, &built_on_kept)
        }
    };
    if stale.is_empty() {
        println!("no unused {}s", kind.what());
        return Ok(());
    }
    println!("{}", reclaim_host_images(&stale).await?.report(&format!("unused {}(s)", kind.what())));
    Ok(())
}

/// Every host image (narrowed by docker's `filters`, `label=k=v` and
/// the like) as one `repo:tag` line, the listing the host-side matcher
/// reads.
async fn host_image_listing(filters: &[String]) -> anyhow::Result<String> {
    let mut images = crate::images::docker();
    images.arg("images");
    for filter in filters {
        images.args(["--filter", filter]);
    }
    let listing = images
        .args(["--format", "{{.Repository}}:{{.Tag}}"])
        .output()
        .await?;
    anyhow::ensure!(
        listing.status.success(),
        "docker images exited {}: {}",
        listing.status,
        String::from_utf8_lossy(&listing.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&listing.stdout).into_owned())
}

/// What one removal pass did with its condemned refs. `already_gone`
/// is a ref a concurrent clean reclaimed between our listing and the
/// delete; `in_use` is a ref the runtime refused to drop because a
/// container still runs it (kept; the next clean gets it).
#[derive(Debug, Default, PartialEq)]
struct Reclaimed {
    removed: usize,
    already_gone: usize,
    in_use: usize,
}

impl Reclaimed {
    /// The one report line every host sweep prints, so the report sites
    /// cannot drift in wording: "removed N <what>", then only the
    /// tails that happened.
    fn report(&self, what: &str) -> String {
        let mut line = format!("removed {} {what}", self.removed);
        if self.already_gone > 0 {
            line.push_str(&format!(", {} already reclaimed", self.already_gone));
        }
        if self.in_use > 0 {
            line.push_str(&format!(", {} still in use (kept)", self.in_use));
        }
        line
    }
}

/// Remove host docker images one by one: a tag a concurrent clean already reclaimed is a
/// success, and a tag docker refuses to drop because a container still
/// uses it is kept, both detected by observing presence rather than
/// parsing error prose. No `-f`: force would untag a live container's
/// image anyway and strand its restart. "Absent" is only a verdict
/// while the daemon still answers.
async fn reclaim_host_images(images: &[String]) -> anyhow::Result<Reclaimed> {
    let mut done = Reclaimed::default();
    for image in images {
        let out = crate::images::docker().args(["rmi", image]).output().await?;
        if out.status.success() {
            done.removed += 1;
            continue;
        }
        let present = crate::images::docker()
            .args(["image", "inspect", image])
            .output()
            .await?
            .status
            .success();
        if present {
            println!("kept {image}: {}", String::from_utf8_lossy(&out.stderr).trim());
            done.in_use += 1;
            continue;
        }
        let daemon_up = crate::images::docker()
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .await?
            .status
            .success();
        if daemon_up {
            done.already_gone += 1;
            continue;
        }
        anyhow::bail!(
            "docker stopped answering while removing {image}; check the daemon \
             and rerun `weft clean --images`"
        );
    }
    Ok(done)
}

/// What the two compile caches on this host take on disk and how to
/// drop each: the node-test cache `weft test-node` builds into, and the
/// worker compile caches image builds share, per cache key and lane.
async fn report_compile_cache_size() -> anyhow::Result<()> {
    match super::test_node::cache_size_bytes()? {
        None => println!("node-test cache: nothing built on this host"),
        Some(bytes) => println!(
            "node-test cache: {:.1}GB; it wipes itself past WEFT_TEST_CACHE_CAP_GB (6 by default), \
             `weft rm --local` drops one project's slice, `weft clean --build-cache` drops it whole",
            bytes as f64 / 1024.0 / 1024.0 / 1024.0
        ),
    }
    let caches = crate::images::worker_compile_caches().await?;
    if caches.is_empty() {
        println!("worker compile cache: no BuildKit record (nothing built on this host, or already pruned)");
        return Ok(());
    }
    let sizes: Vec<String> = caches.iter().map(|cache| {
        let mut line = if cache.idle_days == 0 { cache.size.clone() } else { format!("{} (unused for {} days)", cache.size, cache.idle_days) };
        if cache.lane.is_none() {
            line.push_str(&format!(" ({})", compile_cache_lane(cache)));
        }
        line
    }).collect();
    println!(
        "worker compile cache: {} across {} cache(s) (one per key and compile lane); a build drops the per-project crates no build on this host \
         has linked for {} days, `weft clean --images --all` (the last step of `setup.sh`) drops every key no install on this machine \
         uses, `weft clean --build-cache` drops everything now",
        sizes.join(" + "),
        caches.len(),
        weft_compiler::worker_image::WORKER_CACHE_RETENTION_DAYS
    );
    Ok(())
}

/// Layer 4 (`--all` only): the worker compile caches no install uses.
/// The key changes with the build environment or the builder, and
/// nothing mounts an old one again unless an install goes back to it, so
/// its own sweep never runs; this is the only thing that reclaims it.
/// Every lane of a key an install records stays (its start recorded it,
/// or one of its builds picked it); every lane of any other key goes, at
/// once. While an install has not recorded its keys, nothing goes.
///
/// A lane a build on this machine holds right now (its lock file under
/// the data dir) is skipped whatever its key, and the sweep holds each
/// lane it drops from, so no CLI build (a node test, the standard worker)
/// starts on it mid-drop. An install's own builds pick their lanes in its
/// database, out of this sweep's sight; what keeps their caches is the
/// key each build adds to its install's record before it starts.
async fn unused_compile_cache_sweep(data_dir: &std::path::Path) -> anyhow::Result<()> {
    fn keys(r: &weft_core::images::ImageRefs) -> Vec<&str> {
        r.compile_cache_keys.iter().map(String::as_str).collect()
    }
    let mut lanes: std::collections::BTreeMap<u32, Option<crate::images::CompileLane>> = std::collections::BTreeMap::new();
    let mut failures = Vec::new();
    for cache in crate::images::worker_compile_caches().await?.iter() {
        // The records are looked at again for each cache, and held through
        // its prune: a build that records its key meanwhile either got in
        // first (the cache stays) or waits for this one prune.
        let records = crate::image_refs::every_install(data_dir).await?;
        let kept = match crate::image_refs::kept(&records.installs, keys, "worker compile cache") {
            crate::image_refs::Kept::Unknown(why) => {
                println!("{why}");
                return Ok(());
            }
            crate::image_refs::Kept::Only(kept) => kept,
        };
        if kept.contains(&cache.key) {
            continue;
        }
        let lane = compile_cache_lane(cache);
        if let Some(number) = cache.lane {
            let taken = match lanes.entry(number) {
                std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::btree_map::Entry::Vacant(e) => e.insert(crate::images::CompileLane::try_take(number)?),
            };
            if taken.is_none() {
                println!("keeping a worker compile cache no install uses (key {}, {lane}, {}): a build holds that lane right now", cache.key, cache.size);
                continue;
            }
        }
        println!("dropping a worker compile cache no install uses (key {}, {lane}, {})", cache.key, cache.size);
        if let Err(error) = crate::images::prune_build_record(&cache.id).await {
            failures.push(format!("{error:#}"));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

/// Which lane a compile cache's record names, in words: its number, or
/// that the record has none or names something else.
fn compile_cache_lane(cache: &crate::images::CompileCacheRecord) -> String {
    match (cache.lane, cache.unreadable_lane.as_deref()) {
        (Some(lane), _) => format!("lane {lane}"),
        (None, Some("") | None) => format!("record {} has no lane number", cache.id),
        (None, Some(suffix)) => format!("record {} has lane '{suffix}', not a number", cache.id),
    }
}

/// `docker buildx prune` reclaims BuildKit's intermediate layers.
/// This is the heavy reclaim: cargo deps, intermediate Rust compile
/// state, etc. The next build will re-download deps and re-link. The
/// host's node-test cache goes with it: it is the same kind of thing
/// (compiled engine plus dependencies, re-made by the next build), and
/// "drop every build cache" should mean every one.
async fn clean_build_cache() -> anyhow::Result<()> {
    println!("dropping the node-test cache (the next `weft test-node` builds cold)…");
    super::test_node::wipe_cache()?;
    println!("pruning docker BuildKit cache (next build will be slower)…");
    let status = crate::images::docker()
        .args(["buildx", "prune", "--force"])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("docker buildx prune exited {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{event_line, EventsFilter, Reclaimed};
    use serde_json::json;

    /// One replay row, typed and as it reads on the wire, from the fields
    /// that matter to a filter (the rest filled with throwaway values).
    fn replay_row(mut fields: serde_json::Value) -> (weft_core::live_event::DispatcherEvent, serde_json::Value) {
        let row = fields.as_object_mut().unwrap();
        let filler = json!({
            "runId": "00000000-0000-0000-0000-000000000001",
            "projectId": "00000000-0000-0000-0000-000000000002",
            "atUnix": 1u64, "frames": [], "error": "e", "output": null, "outputs": null,
        });
        let kind = row["kind"].as_str().unwrap().to_string();
        for (key, value) in filler.as_object().unwrap() {
            let wanted = match key.as_str() {
                "frames" | "output" => kind.starts_with("node_") && (key != "output" || kind == "node_completed"),
                "error" => kind.ends_with("_failed"),
                "outputs" => kind == "run_completed",
                _ => true,
            };
            if wanted {
                row.entry(key.clone()).or_insert(value.clone());
            }
        }
        let event: weft_core::live_event::DispatcherEvent = serde_json::from_value(fields.clone()).unwrap();
        let wire = serde_json::to_value(&event).unwrap();
        (event, wire)
    }

    fn keeps(filter: &EventsFilter, (event, row): &(weft_core::live_event::DispatcherEvent, serde_json::Value)) -> bool {
        filter.keeps(event, row)
    }

    /// The kind filter matches exactly or by substring, the node filter
    /// exactly, and a run-level row (no node) never passes a node filter.
    #[test]
    fn events_filter_narrows_by_node_and_kind() {
        let failed = replay_row(json!({"kind": "node_failed", "node": "llm"}));
        let done = replay_row(json!({"kind": "node_completed", "node": "reply"}));
        let run_failed = replay_row(json!({"kind": "run_failed"}));
        let all = EventsFilter::default();
        assert!(keeps(&all, &failed) && keeps(&all, &done) && keeps(&all, &run_failed));
        let by_kind = EventsFilter { kind: Some("failed".into()), ..Default::default() };
        assert!(keeps(&by_kind, &failed) && keeps(&by_kind, &run_failed) && !keeps(&by_kind, &done));
        let exact = EventsFilter { kind: Some("node_completed".into()), ..Default::default() };
        assert!(keeps(&exact, &done) && !keeps(&exact, &failed));
        let by_node = EventsFilter { node: Some("llm".into()), ..Default::default() };
        assert!(keeps(&by_node, &failed) && !keeps(&by_node, &done) && !keeps(&by_node, &run_failed));
        // Named through a call site, only the rows under that call pass.
        let in_call = replay_row(json!({"kind": "node_completed", "node": "Auth.check", "frames": [{"site": "auth"}]}));
        let other_call = replay_row(json!({"kind": "node_completed", "node": "Auth.check", "frames": [{"index": 1}, {"site": "again"}]}));
        let by_call = EventsFilter { node: Some("Auth.check".into()), call_path: vec!["auth".into()], ..Default::default() };
        assert!(keeps(&by_call, &in_call) && !keeps(&by_call, &other_call));
        let any_call = EventsFilter { node: Some("Auth.check".into()), ..Default::default() };
        assert!(keeps(&any_call, &in_call) && keeps(&any_call, &other_call));
    }

    /// `--iteration` keeps the rows fired inside those loop iterations,
    /// an inner loop's rows under the outer iteration they ran in.
    #[test]
    fn events_filter_narrows_by_loop_iteration() {
        let outer_three = replay_row(json!({"kind": "node_completed", "node": "step", "frames": [{"index": 3}]}));
        let inner = replay_row(json!({"kind": "node_completed", "node": "step", "frames": [{"index": 3}, {"site": "auth"}, {"index": 0}]}));
        let outer_one = replay_row(json!({"kind": "node_completed", "node": "step", "frames": [{"index": 1}]}));
        let run_level = replay_row(json!({"kind": "run_completed"}));
        let third = EventsFilter { iteration: super::parse_iteration("3").unwrap(), ..Default::default() };
        assert!(keeps(&third, &outer_three) && keeps(&third, &inner) && !keeps(&third, &outer_one) && !keeps(&third, &run_level));
        let nested = EventsFilter { iteration: super::parse_iteration("3.0").unwrap(), ..Default::default() };
        assert!(keeps(&nested, &inner) && !keeps(&nested, &outer_three));
        assert!(super::parse_iteration("x").is_err() && super::parse_iteration("3.").is_err());
    }

    /// The compact line cuts a long value at the summary width on a
    /// character boundary; `full` prints it whole; and everything the
    /// row carries reaches the line, including the fields only one
    /// A node that completed without emitting says so on its own line.
    ///
    /// The generic tail drops empty fields, which is right for almost
    /// everything and wrong for exactly this: a firing that emitted
    /// nothing closed every one of its ports, so the whole branch behind
    /// it skipped. Without the words, the line is indistinguishable from
    /// a node that emitted plenty, and the person reading is usually
    /// reading BECAUSE a value never arrived.
    #[test]
    fn a_completion_that_emitted_nothing_says_so() {
        for output in [json!({}), json!(null)] {
            let row = json!({
                "kind": "node_completed", "node": "pick",
                "atUnix": 1_756_838_207u64, "output": output,
            });
            let line = event_line(&row, false);
            assert!(line.contains("(nothing emitted)"), "{line}");
        }
        // Absent entirely is the same nothing.
        let bare = json!({ "kind": "node_completed", "node": "pick", "atUnix": 1_756_838_207u64 });
        assert!(event_line(&bare, false).contains("(nothing emitted)"));

        // A node that emitted is untouched, and says nothing about silence.
        let spoke = json!({
            "kind": "node_completed", "node": "pick",
            "atUnix": 1_756_838_207u64, "output": {"value": 1},
        });
        let line = event_line(&spoke, false);
        assert!(!line.contains("nothing emitted"), "{line}");
        assert!(line.contains("output="), "{line}");

        // Only completions: a skip carries its own reason and says why
        // already, so the words would be noise on top of a better line.
        let skipped = json!({
            "kind": "node_skipped", "node": "pick",
            "atUnix": 1_756_838_207u64, "reason": {"kind": "did_not_flow"},
        });
        assert!(!event_line(&skipped, false).contains("nothing emitted"));
    }

    /// kind has.
    #[test]
    fn event_line_summarises_and_expands() {
        let long: String = "é".repeat(300);
        let row = json!({
            "kind": "node_completed",
            "node": "llm",
            "atUnix": 1_756_838_207u64,
            "output": {"text": long},
        });
        let compact = event_line(&row, false);
        assert!(compact.contains(" node_completed ") && compact.contains(" llm"), "{compact}");
        assert!(compact.ends_with("..."), "{compact}");
        assert!(compact.chars().count() < 200, "{}", compact.chars().count());
        let full = event_line(&row, true);
        assert!(full.contains(&long), "full keeps the whole value");

        let cancelled = json!({
            "kind": "run_cancelled",
            "reason": "stopped by sibling",
            "tags": ["user:1"],
            "atUnix": 1_756_838_207u64,
        });
        let line = event_line(&cancelled, false);
        assert!(line.contains("reason=stopped by sibling") && line.contains("tags=user:1"), "{line}");
        let skipped = json!({
            "kind": "node_skipped",
            "node": "send",
            "reason": {"kind": "did_not_flow"},
            "closedPorts": ["ok"],
            "atUnix": 5u64,
        });
        let line = event_line(&skipped, false);
        assert!(line.contains("reason=did_not_flow") && line.contains("closedPorts=ok"), "{line}");
        // A derived row with no stamp keeps the columns aligned, and
        // the row that names its node `nodeId` still shows it.
        let derived = json!({"kind": "journal_corruption", "reason": "bad row"});
        assert!(event_line(&derived, false).starts_with(&" ".repeat(21)));
        let cost = json!({
            "kind": "cost_reported",
            "nodeId": "llm",
            "projectId": "e8969195-52aa-42e0-8a9b-f9577ddd8bed",
            "frames": [],
            "service": "openai",
            "amountUsd": 0.0123,
            "atUnix": 5u64,
        });
        let line = event_line(&cost, false);
        assert!(line.contains(" llm") && line.contains("service=openai") && line.contains("amountUsd=0.0123"), "{line}");
        assert!(!line.contains("projectId") && !line.contains("frames"), "the run's own name and an empty frame stack are not news: {line}");
        // The fields the old hand-picked key list dropped.
        let suspended = json!({"kind": "node_suspended", "node": "ask", "token": "a44e5cad", "atUnix": 5u64});
        assert!(event_line(&suspended, false).contains("token=a44e5cad"));
        let iteration = json!({"kind": "loop_iteration_launched", "node": "run__in", "index": 7, "atUnix": 5u64});
        assert!(event_line(&iteration, false).contains("index=7"));
    }

    /// One report line per sweep: the count always, each tail only
    /// when it happened.
    #[test]
    fn reclaim_report_names_only_what_happened() {
        let plain = Reclaimed { removed: 2, already_gone: 0, in_use: 0 };
        assert_eq!(plain.report("stale infra image(s)"), "removed 2 stale infra image(s)");
        let busy = Reclaimed { removed: 1, already_gone: 1, in_use: 3 };
        assert_eq!(
            busy.report("stale worker image(s)"),
            "removed 1 stale worker image(s), 1 already reclaimed, 3 still in use (kept)"
        );
    }
}

#[cfg(test)]
mod cut_notice_tests {
    use serde_json::json;

    use super::any_value_cut;

    /// The cut is silent, and the value it cuts is usually the one
    /// being chased. The line telling the reader `--full` exists has to
    /// appear exactly when something was cut, and not otherwise: a
    /// notice on every run is noise nobody reads.
    #[test]
    fn a_row_says_whether_it_lost_anything() {
        let long = json!({ "kind": "node_completed", "output": { "text": "x".repeat(400) } });
        assert!(any_value_cut(&long));

        let short = json!({ "kind": "node_completed", "output": { "text": "ok" } });
        assert!(!any_value_cut(&short));

        // A long value in a column printed in its own place is not part
        // of the tail, so it cannot be what was cut.
        let column = json!({ "kind": "node_completed", "node": "n".repeat(400) });
        assert!(!any_value_cut(&column));
    }
}
