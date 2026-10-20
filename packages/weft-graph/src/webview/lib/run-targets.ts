// Running the selected nodes.
//
// A run of targets kicks only the roots those nodes need, so any node
// can be a target. The graph never runs the whole program: the
// contextual bar's Run runs what is selected, and the CLI keeps every
// other way to start one.

import type { NodeFiringStatus } from '../../protocol';

/// The infra nodes a run of `targets` reaches, read off the graph: the
/// upstream walk from every target, with triggers as terminators (a
/// trigger in the walk is included but not walked through), closed over
/// scopes (a node inside a group or loop brings the whole container,
/// whose own inputs are then walked). Only these gate the run's
/// readiness: infra outside the walk never runs, so it has no say.
export function runInfraIds(
  targets: Iterable<string>,
  nodes: Array<{ id: string; isTrigger: boolean; isInfra: boolean; parentId?: string }>,
  edges: Array<{ source: string; target: string }>,
): string[] {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const stack = [...targets];
  const seen = new Set<string>();
  const infraIds: string[] = [];
  while (stack.length > 0) {
    const id = stack.pop()!;
    if (seen.has(id)) continue;
    seen.add(id);
    const node = byId.get(id);
    if (node?.isInfra) infraIds.push(id);
    // A node inside a container brings the container (its inputs are
    // what the scope needs), and a container brings everything inside
    // it. Both directions, so the walk reaches all the way down a
    // nesting: the parent pushes its children, and a child that is
    // itself a container pushes its own. Walking only siblings stopped
    // one level in, and an infra node or trigger two groups deep was
    // missed here while the dispatcher counted it.
    if (node?.parentId) stack.push(node.parentId);
    for (const other of nodes) {
      if (other.parentId === id) stack.push(other.id);
    }
    if (node?.isTrigger) continue; // terminator: included, not walked through
    for (const e of edges) {
      if (e.target === id) stack.push(e.source);
    }
  }
  return infraIds;
}

/// What a run check's answer depends on, by value: the selected places
/// and the program's source (`weft run --check` reads the files on
/// disk). The same places give the same key whatever array (or order)
/// they arrive in, so a graph redrawn around an unchanged selection and
/// program never asks the walk again, and any change of the source (the
/// text tab, a canvas edit once saved, an outside writer) asks it anew.
export function runCheckKey(targets: Iterable<string>, source: string): string {
  return JSON.stringify([[...new Set(targets)].sort(), source]);
}

/// How far the followed run got: the places it reached that are done
/// (their latest firing ended, whichever way), out of the places its
/// program runs (`null` when the run's program could not be read).
export function runProgress(
  firings: Record<string, Array<{ status: NodeFiringStatus }>>,
  scope: string[] | null | undefined,
): { done: number; total: number | null } {
  let done = 0;
  for (const rows of Object.values(firings)) {
    const last = rows[rows.length - 1];
    if (last && last.status !== 'running' && last.status !== 'waiting_for_input') done += 1;
  }
  return { done, total: Array.isArray(scope) ? scope.length : null };
}
