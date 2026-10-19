//! A program's PLACES, and the walks over them.
//!
//! Every node here is a place ([`Located`]): its id and the call path it
//! runs under. An included file is compiled once and reached through
//! every site that includes it, so one id is as many places as there are
//! calls of it, and a wire is taken at a place: the wire from `strip` to
//! `loud` exists under `one` and under `two`, and a wire into a body from
//! one site is never taken under another (it enters through its own
//! site, [`super::Edge::via`]).

use std::collections::BTreeSet;

use crate::frames::Located;
use super::graph::{GraphView, ProjectGraph};
use super::{Edge, GroupKind, NodeDefinition, ProjectDefinition};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Direction {
    Upstream,
    Downstream,
}

/// The place at the other end of `edge` from `from`: the call path
/// carried across the sites the wire crosses ([`Edge::carry_calls`]).
/// `None` when the wire is another call's.
pub fn step(g: &impl GraphView, from: &Located, edge: &Edge, direction: Direction) -> Option<Located> {
    let mut path = from.path.clone();
    let other = match direction {
        Direction::Downstream => &edge.target,
        Direction::Upstream => &edge.source,
    };
    Edge::carry_calls(edge.site_crossings(g), direction, &mut path, |site| Some(site.as_str()), str::to_string)
        .then(|| Located::new(other, path))
}

/// The wires into `at` that are taken there, each with its source's
/// place. A wire that stops at a group port nobody reads carries nothing
/// and is not among them.
pub fn incoming<'a>(g: &'a impl GraphView, at: &Located) -> impl Iterator<Item = (&'a Edge, Located)> + 'a {
    let edges: Vec<&Edge> = g.edges_into(&at.id).filter(|edge| !edge.to_group_port()).collect();
    let at = at.clone();
    edges.into_iter().filter_map(move |edge| step(g, &at, edge, Direction::Upstream).map(|source| (edge, source)))
}

/// The wires out of `at` that are taken there, each with its target's
/// place.
pub fn outgoing<'a>(g: &'a impl GraphView, at: &Located) -> impl Iterator<Item = (&'a Edge, Located)> + 'a {
    let edges: Vec<&Edge> = g.edges_out_of(&at.id).filter(|edge| !edge.to_group_port()).collect();
    let at = at.clone();
    edges.into_iter().filter_map(move |edge| step(g, &at, edge, Direction::Downstream).map(|target| (edge, target)))
}

/// Every place reached from `seeds` by following wires backward, seeds
/// included: the plain data walk. A body entered through a site is walked
/// under that site alone (see [`step`]), so an infra node inside a file
/// included twice is reached once per call. Wires only: nothing
/// structural is pulled in (no gate around a node, no loop taken whole),
/// which is what tells it apart from the walk a run is cut with
/// (`super::cut`): that one is the run's shape, this one is what a value's
/// path runs through.
pub fn upstream_by_wires(g: &ProjectGraph, seeds: &[Located]) -> BTreeSet<Located> {
    let mut reached = BTreeSet::new();
    let mut pending: Vec<Located> = seeds.to_vec();
    while let Some(place) = pending.pop() {
        if !reached.insert(place.clone()) { continue; }
        pending.extend(incoming(g, &place).map(|(_, source)| source));
    }
    reached
}

/// Where a value that arrives at a port ends up needed: the input that
/// would leave its node skipped without it, and the first start of the
/// run the value passes on the way (where a person hands it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Need {
    pub needer: (Located, String),
    pub start: Option<(Located, String)>,
    /// The `@require_one_of` set the needer belongs to, when that set is
    /// why it is needed (every other member gets nothing); `None` when
    /// the input is required on its own.
    pub one_of: Option<Vec<String>>,
}

/// The input a value arriving at `port` of the node at `at` ends up
/// needed by, or `None` when nothing that needs it lies on its path.
///
/// A node answers for itself: its input is needed when it is required
/// (or required once wired, which a port reached along a wire is) with
/// no default, or when it belongs to a `@require_one_of` set whose every
/// other member gets nothing (`fed(place, port)` says whether an input of
/// the node at `place` gets a value some other way). A loop's `LoopIn`
/// requires the ports it iterates or carries (with no list there is no
/// iteration), which the compiler records as that port's `required`, so a
/// loop answers like a node for those and is walked through for the rest,
/// to the members that read the port inside.
///
/// `counts(place, port)` says where a value arriving there still matters:
/// everywhere for the compiler; for a run, only places the run executes,
/// never a trigger (a fired one reads its bake, the others close), and not
/// past a start that was handed a value for that port. `is_start` marks
/// the run's starts, so the answer can name where to hand a value.
pub fn required_consumer(
    project: &ProjectDefinition,
    at: &Located,
    port: &str,
    counts: &dyn Fn(&Located, &str) -> bool,
    is_start: &dyn Fn(&Located) -> bool,
    fed: &dyn Fn(&Located, &str) -> bool,
) -> Option<Need> {
    let mut pending = vec![(at.clone(), port.to_string(), None::<(Located, String)>)];
    let mut visited = BTreeSet::new();
    while let Some((place, port, start)) = pending.pop() {
        if !visited.insert((place.clone(), port.clone())) { continue; }
        if crate::run::skip::is_gate_port(&port) || !counts(&place, &port) { continue; }
        let Some(node) = project.node(&place.id) else { continue };
        let start = start.or_else(|| is_start(&place).then(|| (place.clone(), port.clone())));
        let input = node.inputs.iter().find(|p| p.name == port);
        // The walk only ever reaches a port along a wire, so an input
        // required once wired counts as required here. A default does
        // not help: it fills only an unwired port, and a wired one whose
        // wire closes skips its node (`ready::required_inputs`).
        if input.is_some_and(|input| input.is_required(true)) {
            return Some(Need { needer: (place, port), start, one_of: None });
        }
        let last_of = node.features.one_of_required.iter()
            .find(|set| set.contains(&port) && set.iter().all(|other| *other == port || !fed(&place, other)));
        if let Some(set) = last_of {
            return Some(Need { needer: (place, port), start, one_of: Some(set.clone()) });
        }
        if node.node_type != super::boundary_types::LOOP_IN { continue; }
        for (edge, target) in outgoing(project, &place) {
            if edge.source_port() != port { continue; }
            pending.push((target, edge.target_port().to_string(), start.clone()));
        }
    }
    None
}

/// The output ports of `place` that a wire into one of `runs` reads, each
/// wire followed on its path (so a file included twice reads its own
/// site's node, never the other site's). A wire into a trigger is not a
/// read: a run never delivers it (a trigger read its inputs when it was
/// set up).
pub fn ports_read_by(project: &ProjectDefinition, place: &Located, runs: &BTreeSet<Located>) -> BTreeSet<String> {
    let g = ProjectGraph::new(project);
    outgoing(&g, place)
        .filter(|(_, target)| runs.contains(target) && !g.is_trigger(&target.id))
        .map(|(edge, _)| edge.source_port().to_string())
        .collect()
}

/// Every wire at every place it runs: `(wire, source place, target
/// place)`, a wire inside an included file once per site that reaches
/// it, and a wire into or out of a file's body only for the site it
/// belongs to. What resolves and checks types per place reads.
pub fn located_wires(project: &ProjectDefinition) -> Vec<(&Edge, Located, Located)> {
    let mut wires = Vec::new();
    for at in every_place(project) {
        wires.extend(incoming(project, &at).map(|(edge, source)| (edge, source, at.clone())));
    }
    wires
}

/// Every place a group runs at, each at the call path its members run
/// under: a group of the program once, at the top; a group inside an
/// included file (the file's own body among them) once per site that
/// reaches it, however deep. A site sits in the file around it, at that
/// file's path.
pub fn every_group_place(project: &ProjectDefinition) -> BTreeSet<Located> {
    let g = ProjectGraph::new(project);
    let mut out = BTreeSet::new();
    // Each file reached (its body, `None` for the program) and the path
    // reaching it.
    let mut pending: Vec<(Option<String>, Vec<String>)> = vec![(None, Vec::new())];
    while let Some((file, path)) = pending.pop() {
        for group in &project.groups {
            if body_of_group(&g, &group.id) != file { continue; }
            out.insert(Located::new(group.id.clone(), path.clone()));
            if let GroupKind::Call { body, .. } = &group.kind {
                let mut deeper = path.clone();
                deeper.push(group.id.clone());
                pending.push((Some(body.clone()), deeper));
            }
        }
    }
    out
}

/// The group place each of `edge`'s crossings passes, in order, for the
/// wire taken from `source` (its source's place): the call path carried
/// across the sites the way [`Edge::carry_calls`] carries it, a site's
/// own port read in the file around it and a body's inside the call.
pub fn crossing_places(g: &impl GraphView, edge: &Edge, source: &Located) -> Vec<Located> {
    let mut path = source.path.clone();
    edge.via.iter().map(|crossing| {
        if !g.is_site(&crossing.group) {
            return Located::new(crossing.group.clone(), path.clone());
        }
        match crossing.side {
            super::CrossingSide::In => {
                let at = Located::new(crossing.group.clone(), path.clone());
                path.push(crossing.group.clone());
                at
            }
            super::CrossingSide::Out => {
                path.pop();
                Located::new(crossing.group.clone(), path.clone())
            }
        }
    }).collect()
}

/// Every wire at every place it runs, the way type resolution and type
/// checks read them: [`located_wires`] with their target's place, and a
/// wire that stops at a group port nobody reads (`None` for its target)
/// at every place its source runs from which the wire is taken (one
/// leaving an included file leaves through the site of that place only,
/// [`Edge::carry_calls`]).
pub fn wires_at_places(project: &ProjectDefinition) -> Vec<(&Edge, Located, Option<Located>)> {
    let g = ProjectGraph::new(project);
    let mut wires: Vec<(&Edge, Located, Option<Located>)> =
        located_wires(project).into_iter().map(|(edge, source, target)| (edge, source, Some(target))).collect();
    let sources: BTreeSet<Located> = every_place_in(&g).into_iter().chain(every_group_place(project)).collect();
    for edge in project.edges.iter().filter(|edge| edge.to_group_port()) {
        for at in sources.iter().filter(|at| at.id == edge.source) {
            let mut path = at.path.clone();
            if Edge::carry_calls(edge.site_crossings(&g), Direction::Downstream, &mut path, |site| Some(site.as_str()), str::to_string) {
                wires.push((edge, at.clone(), None));
            }
        }
    }
    wires
}

/// A place whose state in a run is one thing (`GetNodeState`): a node
/// outside every loop, or a loop, which is read as a whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatePlace {
    Node(Located),
    Loop(Located),
}

/// The place `spelled` names (written the way `--from` takes it) whose
/// state a run can tell, or why it has none. A node inside a loop fires
/// once per iteration, so it is refused like a cut inside a loop, and the
/// loop around it is named instead; a plain group or an include's site
/// has no state of its own (it is a scope: whether it ran is the state
/// of the nodes inside it).
pub fn state_place(project: &ProjectDefinition, spelled: &str) -> Result<StatePlace, String> {
    let g = ProjectGraph::new(project);
    let (id, path) = super::resolve_address(project, spelled);
    let place = Located::new(id.clone(), path);
    let (subject, around) = if g.node(&id).is_some() {
        ("node", enclosing_loops(&g, &place))
    } else if g.is_loop(&id) {
        ("loop", scopes_around_group(&g, &place).into_iter().filter(|scope| g.is_loop(&scope.id)).collect())
    } else if g.group(&id).is_some() {
        return Err(format!("'{spelled}' is a group, and a group has no state of its own; name a node inside it, or a loop"));
    } else {
        return Err(format!("'{spelled}' is no node or loop of this program"));
    };
    if let Some(outer) = around.last() {
        let outer = super::group_address(project, &outer.id, &outer.path);
        return Err(format!("the {subject} '{spelled}' is inside loop '{outer}': it runs once per iteration, so it has no one state; read the loop '{outer}' instead"));
    }
    // Outside every loop, what is left to refuse is a file's node or group
    // named without the site that calls it.
    if subject == "node" {
        validate_place(&g, &place, spelled)?;
        Ok(StatePlace::Node(place))
    } else {
        validate_group_place(&g, &place, spelled)?;
        Ok(StatePlace::Loop(place))
    }
}

/// Every place in the program: the top-level nodes, and each included
/// file's nodes once per site that reaches it, however deep.
pub fn every_place(project: &ProjectDefinition) -> BTreeSet<Located> {
    every_place_in(&ProjectGraph::new(project))
}

/// The places a program runs: every place ([`every_place`]) its call
/// sites run (`ProgramIndex::admits`), less those that stand in for
/// themselves (`ProjectDefinition::given`).
pub fn running_places(project: &ProjectDefinition) -> BTreeSet<Located> {
    let index = super::ProgramIndex::build(project);
    every_place(project)
        .into_iter()
        .filter(|place| index.admits(&place.id, &place.frames()) && !index.stands_in(&place.id, &place.frames()))
        .collect()
}

/// [`every_place`] over a program already indexed.
pub fn every_place_in(g: &ProjectGraph) -> BTreeSet<Located> {
    let project = g.project();
    let mut places: BTreeSet<Located> = project.nodes.iter().filter(|node| body_around(g, node).is_none())
        .map(|node| Located::top(node.id.clone())).collect();
    for site in project.groups.iter().filter(|group| matches!(group.kind, GroupKind::Call { .. })) {
        if body_of_group(g, &site.id).is_none() {
            places.extend(members_in(g, &site.id, &[]));
        }
    }
    places
}

/// The groups `group` sits in, outermost first (in its own file).
fn groups_around(g: &impl GraphView, group: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut at = g.group(group).and_then(|here| here.parent_group_id.clone());
    while let Some(id) = at {
        at = g.group(&id).and_then(|here| here.parent_group_id.clone());
        out.push(id);
    }
    out.reverse();
    out
}

/// The scopes `place` runs inside, outermost first, each at the place it
/// runs at (`Located` naming the scope): every site on its path (at the
/// path above it) with the groups around that site, then the groups and
/// loops around the node itself (at its own path). A loop's `LoopIn` and
/// `LoopOut` sit in the scope around their loop.
pub fn scopes_around(g: &impl GraphView, place: &Located) -> Vec<Located> {
    let mut out = path_scopes(g, &place.path);
    if let Some(node) = g.node(&place.id) {
        out.extend(node.scope.iter().map(|group| Located::new(group.clone(), place.path.clone())));
    }
    out
}

/// The scopes the group at `group` runs inside, outermost first, the way
/// [`scopes_around`] lists a node's.
pub fn scopes_around_group(g: &impl GraphView, group: &Located) -> Vec<Located> {
    let mut out = path_scopes(g, &group.path);
    out.extend(groups_around(g, &group.id).into_iter().map(|id| Located::new(id, group.path.clone())));
    out
}

/// The scopes a call path runs inside: each site on it (at the path above
/// it), after the groups around that site.
fn path_scopes(g: &impl GraphView, path: &[String]) -> Vec<Located> {
    let mut out = Vec::new();
    for (depth, site) in path.iter().enumerate() {
        let above = path[..depth].to_vec();
        out.extend(groups_around(g, site).into_iter().map(|group| Located::new(group, above.clone())));
        out.push(Located::new(site.clone(), above));
    }
    out
}

/// The loops around `place`, outermost first, each at the place the loop
/// runs: a loop in the node's own scope, a loop around any site on its
/// path (a call inside a loop runs whole with the loop), and a loop's own
/// boundaries' loop.
pub fn enclosing_loops(g: &impl GraphView, place: &Located) -> Vec<Located> {
    let mut loops: Vec<Located> = scopes_around(g, place).into_iter().filter(|scope| g.is_loop(&scope.id)).collect();
    if let Some(boundary) = g.node(&place.id).and_then(|node| node.group_boundary.as_ref()) {
        loops.push(Located::new(boundary.group_id.clone(), place.path.clone()));
    }
    loops.dedup();
    loops
}

/// A group's members, each at the place it runs when the group is at
/// `path`: its nodes at `path` (a loop's own two boundaries among a
/// loop's), and behind every call site among them (the group itself,
/// when it is one) the whole body one site deeper. Takes the graph so a
/// caller walking many groups indexes the program once.
pub fn members_in(g: &impl GraphView, group: &str, path: &[String]) -> Vec<Located> {
    let mut out: Vec<Located> = g.members(group).map(|n| Located::new(n.id.clone(), path.to_vec())).collect();
    let sites: Vec<(String, String)> = g.sites(group)
        .filter_map(|site| g.body_of(&site.id).map(|body| (site.id.clone(), body.to_string()))).collect();
    for (site, body) in sites {
        let mut deeper = path.to_vec();
        deeper.push(site);
        out.extend(members_in(g, &body, &deeper));
    }
    out
}

/// A bound as a person wrote it, resolved: the node or group id and the
/// call path it names. An unknown spelling is reported as written.
pub fn locate(g: &impl GraphView, spelled: &str) -> Result<Located, String> {
    let (id, path) = super::resolve_address(g.project(), spelled);
    if g.node(&id).is_some() || g.group(&id).is_some() {
        Ok(Located::new(id, path))
    } else {
        Err(format!("unknown node '{spelled}'"))
    }
}

/// A group may be an endpoint (cut at, started, or run as `--group`)
/// under the same rule as a node (`validate_place`): nothing between
/// the top of the program and it runs whole, so no loop around it and
/// none around a site on its path; and a group inside an included file
/// is named through a site, never through the file's own id. A body is
/// never an endpoint: it runs through the site that calls it. A loop is
/// cut whole, so a loop itself is an endpoint like any group.
pub fn validate_group_place(g: &impl GraphView, place: &Located, spelled: &str) -> Result<(), String> {
    let group = g.group(&place.id).ok_or_else(|| format!("unknown group '{spelled}'"))?;
    if matches!(group.kind, GroupKind::Body) {
        return Err(format!("cannot cut at '{spelled}': it is an included file, which runs through the site that includes it; name the site"));
    }
    if let Some(container) = groups_around(g, &group.id).into_iter().find(|id| g.is_loop(id)) {
        let container = super::plain_id(&container);
        return Err(format!("cannot cut at '{spelled}' inside loop '{container}'; select the whole loop"));
    }
    if place.path.is_empty() {
        if let Some(body) = body_of_group(g, &group.id) {
            return Err(inside_a_file(g.project(), spelled, &group.id, &body));
        }
    }
    for site in &place.path {
        if let Some(container) = groups_around(g, site).into_iter().find(|id| g.is_loop(id)) {
            let (site, container) = (super::plain_id(site), super::plain_id(&container));
            return Err(format!("cannot cut at '{spelled}': the site '{site}' sits inside loop '{container}'; select the whole loop"));
        }
    }
    Ok(())
}

/// The included file's body `group` sits in (itself, when it is one).
pub fn body_of_group(g: &impl GraphView, group: &str) -> Option<String> {
    let mut at = g.group(group);
    while let Some(here) = at {
        if matches!(here.kind, GroupKind::Body) { return Some(here.id.clone()); }
        at = here.parent_group_id.as_deref().and_then(|parent| g.group(parent));
    }
    None
}

/// The refusal for a body's node or group named without a site: the
/// spelling that names one call of it.
fn inside_a_file(project: &ProjectDefinition, spelled: &str, id: &str, body: &str) -> String {
    let example = project.groups.iter()
        .find(|g| matches!(&g.kind, GroupKind::Call { body: called, .. } if called == body))
        .map(|g| super::address_of(project, id, std::slice::from_ref(&g.id)))
        .unwrap_or_else(|| format!("<site>.{}", id.rsplit('.').next().unwrap_or(id)));
    format!("cannot cut at '{spelled}': it is inside an included file, which is reached through a site; name it through the site, like `{example}`")
}

/// A node may be an endpoint when nothing between the top of the program
/// and it runs whole: not the node's own loops, and not a loop around any
/// site on its call path (a site inside a loop is cut with the loop). A
/// node inside an included file is reached through its site, spelled
/// `site.node`; naming the body's own id (`Triage.up`) says nothing about
/// which call, so it is refused with the spelling that does.
pub fn validate_place(g: &impl GraphView, place: &Located, spelled: &str) -> Result<(), String> {
    let node = g.node(&place.id).ok_or_else(|| format!("unknown node '{spelled}'"))?;
    if let Some(container) = enclosing_loop(g, node) {
        let container = super::plain_id(&container);
        return Err(format!("cannot cut at '{spelled}' inside loop '{container}'; select the whole loop"));
    }
    if place.path.is_empty() {
        if let Some(body) = body_around(g, node) {
            return Err(inside_a_file(g.project(), spelled, &place.id, &body));
        }
    }
    for site in &place.path {
        if g.group(site).is_none() {
            return Err(format!("call site '{}' is not in the program", super::plain_id(site)));
        }
        if let Some(container) = groups_around(g, site).into_iter().find(|id| g.is_loop(id)) {
            let (site, container) = (super::plain_id(site), super::plain_id(&container));
            return Err(format!("cannot cut at '{spelled}': the site '{site}' sits inside loop '{container}'; select the whole loop"));
        }
    }
    Ok(())
}

/// Whether `group` is an included file's body.
pub fn is_body(project: &ProjectDefinition, group: &str) -> bool {
    project.group(group).is_some_and(|g| matches!(g.kind, GroupKind::Body))
}

/// The included file's body `node` sits in, if any.
pub fn enclosing_body(project: &ProjectDefinition, node: &NodeDefinition) -> Option<String> {
    body_around(project, node)
}

fn body_around(g: &impl GraphView, node: &NodeDefinition) -> Option<String> {
    first_group_around(g, node, |kind| matches!(kind, GroupKind::Body))
}

/// The loop around `node`, if any: a loop's body runs whole, once per
/// iteration, so a run is never cut inside one. The first in the
/// program's group order, when the node is in several. A loop's own
/// boundaries count as inside it.
fn enclosing_loop(g: &impl GraphView, node: &NodeDefinition) -> Option<String> {
    first_group_around(g, node, |kind| matches!(kind, GroupKind::Loop { .. }))
}

/// The first group, in the program's group order, of a kind `wanted`
/// takes, that `node` is a member or a loop boundary of.
fn first_group_around(g: &impl GraphView, node: &NodeDefinition, wanted: impl Fn(&GroupKind) -> bool) -> Option<String> {
    let project = g.project();
    let order = |id: &str| project.groups.iter().position(|group| group.id == id);
    node.scope.iter().chain(node.group_boundary.iter().map(|b| &b.group_id))
        .filter(|id| g.group(id).is_some_and(|group| wanted(&group.kind)))
        .filter_map(|id| order(id).map(|at| (at, id)))
        .min()
        .map(|(_, id)| id.clone())
}
