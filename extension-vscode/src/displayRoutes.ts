// Which nodes on screen show a display, where each one is served from,
// and the display stream path that asks for all of them. Kept apart
// from the graph view so these decisions are tested without VS Code.

import type { ParseResponse } from '../../packages/weft-graph/src/protocol';
import { addressOf } from '../../packages/weft-graph/src/run-spec';
import type { LiveRun } from './runFollower';

/// Where one node's display is served from, and the node as the
/// display stream names it: its place, spelled the way a person writes
/// it (`one.door`). An infra node's container serves its own (`infra`),
/// the listener kind holding a trigger's signal serves a trigger's
/// (`signal`), and any other node shows the display of its wait in the
/// followed run while it waits (`wait`).
// SYNC: DisplayRoute.source <-> crates/weft-dispatcher/src/display_feeds.rs (DisplaySource)
export interface DisplayRoute {
  source: 'infra' | 'signal' | 'wait';
  node: string;
}

type ParsedNode = ParseResponse['project']['nodes'][number];
type Parsed = Pick<ParseResponse, 'project' | 'catalog'>;

// The parsed node first, the catalog only when it says nothing: the
// same precedence the webview's role predicates use, so the host never
// asks about a node the webview draws no panel for, or the reverse.
// SYNC: role precedence <-> packages/weft-graph/src/webview/lib/utils/node-roles.ts
const isInfraNode = (response: Parsed, n: ParsedNode): boolean =>
  n.requiresInfra ?? response.catalog[n.nodeType]?.requiresInfra ?? false;
const isTriggerNode = (response: Parsed, n: ParsedNode): boolean =>
  n.features?.isTrigger ?? response.catalog[n.nodeType]?.features?.isTrigger ?? false;
const servesLive = (response: Parsed, n: ParsedNode): boolean =>
  (n.features?.liveEndpoint ?? response.catalog[n.nodeType]?.features?.liveEndpoint) != null;

/** Every infra node of a parse: the nodes whose display carries
 *  buttons a press can reach. */
export function infraNodeIdsOf(response: Parsed): Set<string> {
  return new Set(response.project.nodes.filter((n) => isInfraNode(response, n)).map((n) => n.id));
}

/** Where each node of a parse, seen from the place `callPath`, has its
 *  display served from, keyed by node id.
 *
 *  An infra node's container serves one only when the node's metadata
 *  names the endpoint; one that speaks only TCP has nothing to serve it
 *  on, so it shows a wait's display like any node that is not a
 *  trigger. A trigger's listener kind always serves one. A node
 *  that exists once per instance has no one display to show here: each
 *  instance's copy shows its own through the instance door, and the
 *  editor shows the shared program alone. */
export function displayRoutesOf(response: Parsed, callPath: readonly string[]): Map<string, DisplayRoute> {
  const routes = new Map<string, DisplayRoute>();
  for (const n of response.project.nodes) {
    if (n.perInstance) continue;
    // Every door takes the place spelled the way a person writes it
    // (`one.door` for the `door` inside the file the site `one`
    // includes): the node under the calls this view descended through.
    const node = addressOf(callPath, n.id);
    if (isInfraNode(response, n)) routes.set(n.id, { source: servesLive(response, n) ? 'infra' : 'wait', node });
    else if (isTriggerNode(response, n)) routes.set(n.id, { source: 'signal', node });
    else routes.set(n.id, { source: 'wait', node });
  }
  return routes;
}

/** The display stream path for `routes` of `projectId`, with the waits
 *  of `run` when the followed run belongs to that project; undefined
 *  when there is nothing to ask for. A run of another project adds
 *  nothing: the dispatcher refuses the whole stream for a run that is
 *  not its project's. Infra and trigger nodes are named one by one; a
 *  run's waits come with the run, whichever nodes they turn out to be
 *  on. Every list is sorted so the same set always spells the same
 *  path, which is what decides whether the stream reconnects. */
export function displayStreamPath(
  projectId: string,
  routes: Iterable<DisplayRoute>,
  run: LiveRun | undefined,
): string | undefined {
  const all = [...routes];
  const of = (source: DisplayRoute['source']) =>
    all
      .filter((r) => r.source === source)
      .map((r) => encodeURIComponent(r.node))
      .sort()
      .join(',');
  const params: string[] = [];
  if (all.some((r) => r.source !== 'wait')) params.push(`infra=${of('infra')}`, `signals=${of('signal')}`);
  if (run?.projectId === projectId) params.push(`run=${encodeURIComponent(run.runId)}`);
  if (params.length === 0) return undefined;
  return `/events/project/${projectId}/displays?${params.join('&')}`;
}

/** The nodes that showed their own display (infra or trigger) under
 *  `previous` and no longer get it from that same route under `next`:
 *  a node whose type changed, an infra node that lost its endpoint, a
 *  node that moved to another place or left the screen. Nothing on the
 *  stream will say `absent` for them, so their last display stays
 *  unless cleared. */
export function ownDisplaysLost(
  previous: ReadonlyMap<string, DisplayRoute>,
  next: ReadonlyMap<string, DisplayRoute>,
): string[] {
  const lost: string[] = [];
  for (const [nodeId, route] of previous) {
    if (route.source === 'wait') continue;
    const now = next.get(nodeId);
    if (now?.source !== route.source || now.node !== route.node) lost.push(nodeId);
  }
  return lost;
}

/** The id of the node on screen a display message is about: the one
 *  whose display is served from the message's source at the message's
 *  place. Undefined for a node no longer on screen. */
export function displayedNodeOf(
  routes: ReadonlyMap<string, DisplayRoute>,
  msg: DisplayRoute,
): string | undefined {
  for (const [nodeId, route] of routes) {
    if (route.source === msg.source && route.node === msg.node) return nodeId;
  }
  return undefined;
}
