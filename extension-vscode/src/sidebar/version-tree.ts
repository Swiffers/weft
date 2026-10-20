// The version tree as the Runs view shows it in "by version"
// mode: what `weft tree --json` answers, folded into nested version
// nodes with their runs beneath. Pure, so the shape is tested without
// an editor process; the provider only draws it.

import { specScopedTo, type RunSpec } from '../../../packages/weft-graph/src/run-spec';
import type { RunSummary } from './runs';
import { describeOutcome } from './outcome';

// SYNC: TreeJson, VersionSummary, VersionRun <-> crates/weft-core/src/versions.rs VersionTree, Head, VersionSummary, VersionRun (the shape `weft tree --json` prints, with `diskVersion` added by crates/weft-cli/src/commands/tree.rs)
export interface TreeJson {
  head: { headVersion: string | null; headRun: string | null; activatedVersions: string[] };
  versions: VersionSummary[];
  runs: VersionRun[];
  /** The version the files on disk are, when it is one the tree holds. */
  diskVersion?: string | null;
}

export interface VersionSummary {
  id: string;
  parentId: string | null;
  label: string | null;
  createdAt: number;
  diff: { added: string[]; removed: string[]; changed: string[] };
  manifest: Record<string, string>;
  /** How many runs the project's triggers started on this version (too
   *  many to list one by one), and the newest. */
  triggerRuns?: number;
  lastTriggerRun?: string | null;
}

export interface VersionRun {
  runId: string;
  versionId: string;
  definitionHash: string;
  seedRunId: string | null;
  stale: string[];
  spec: RunSpec | null;
  example: string | null;
  /** When it started, as the version tree recorded it. */
  startedAt: number;
  /** The run as the runs listing shows it, or null when its record is
   *  gone. */
  // SYNC: VersionRun.listing <-> crates/weft-core/src/versions.rs VersionRun.listing
  listing: RunSummary | null;
}

/** One version in the drawn tree. */
export interface VersionTreeNode {
  version: VersionSummary;
  runs: VersionRun[];
  children: VersionTreeNode[];
  /** Head's version. */
  isHead: boolean;
  /** The version the files on disk are. */
  isDisk: boolean;
  /** The version the triggers are activated on. */
  isActivated: boolean;
}

/** Nest the flat list: roots first (a version whose parent the tree no
 *  longer holds is a root too), children and runs in the order the
 *  dispatcher recorded them (the order it lists them in). */
export function buildVersionTree(tree: TreeJson): VersionTreeNode[] {
  const known = new Set(tree.versions.map((v) => v.id));
  const runsOf = new Map<string, VersionRun[]>();
  for (const r of tree.runs) {
    const list = runsOf.get(r.versionId) ?? [];
    list.push(r);
    runsOf.set(r.versionId, list);
  }
  // Children indexed once, in order, instead of scanning every version
  // per version: a project records a version per run, so the history is
  // long and mostly linear, and the scan-per-node was quadratic in it.
  const childrenOf = new Map<string, VersionSummary[]>();
  for (const v of tree.versions) {
    if (v.parentId === null || !known.has(v.parentId)) continue;
    const list = childrenOf.get(v.parentId) ?? [];
    list.push(v);
    childrenOf.set(v.parentId, list);
  }
  // `seen` is not decoration. A version's parent link carries no foreign
  // key, so two rows pointing at each other, or one reachable from two
  // parents, is something the database permits; walking it for ever
  // takes the sidebar down, and drawing one version twice is a tree that
  // lies. Each version is placed once, under whichever parent reaches it
  // first.
  const seen = new Set<string>();
  // Built by an explicit walk rather than by recursion: a version per
  // run makes the depth of a long history as large as its length, and
  // recursing that deep overflows the stack for the same reason a cycle
  // did, just later.
  const node = (v: VersionSummary): VersionTreeNode => {
    seen.add(v.id);
    const root: VersionTreeNode = {
      version: v,
      runs: runsOf.get(v.id) ?? [],
      children: [],
      isHead: tree.head.headVersion === v.id,
      isDisk: tree.diskVersion === v.id,
      isActivated: tree.head.activatedVersions.includes(v.id),
    };
    const pending: VersionTreeNode[] = [root];
    while (pending.length > 0) {
      const parent = pending.pop()!;
      for (const c of childrenOf.get(parent.version.id) ?? []) {
        if (seen.has(c.id)) continue;
        seen.add(c.id);
        const child: VersionTreeNode = {
          version: c,
          runs: runsOf.get(c.id) ?? [],
          children: [],
          isHead: tree.head.headVersion === c.id,
          isDisk: tree.diskVersion === c.id,
          isActivated: tree.head.activatedVersions.includes(c.id),
        };
        parent.children.push(child);
        pending.push(child);
      }
    }
    return root;
  };
  const out: VersionTreeNode[] = [];
  for (const v of tree.versions) {
    if (seen.has(v.id)) continue;
    if (v.parentId === null || !known.has(v.parentId)) out.push(node(v));
  }
  // Whatever is left no root reaches, which means a cycle: either a
  // version in one, or one hanging off a member of one. Shown rather than
  // hidden, because a version the person cannot see is one they cannot
  // prune.
  //
  // The cycle's own members go FIRST. Starting anywhere would draw a
  // version at top level whose parent is a leftover too, and the parent
  // then appears further down with the child under it as well: the same
  // version drawn twice, or drawn at a place that is not where it sits.
  const inACycle = (start: VersionSummary): boolean => {
    let at = start.parentId;
    for (let step = 0; step < tree.versions.length && at !== null && at !== undefined; step += 1) {
      if (at === start.id) return true;
      at = tree.versions.find((v) => v.id === at)?.parentId ?? null;
    }
    return false;
  };
  for (const v of tree.versions) {
    if (!seen.has(v.id) && inACycle(v)) out.push(node(v));
  }
  // Anything still left hangs off a cycle that is now drawn, so it has
  // nowhere else to go.
  for (const v of tree.versions) {
    if (!seen.has(v.id)) out.push(node(v));
  }
  return out;
}

export function shortId(id: string): string {
  return id.slice(0, 8);
}

/** The version's line: id, label, and what changed against its parent. */
export function versionLabel(node: VersionTreeNode): string {
  const parts = [shortId(node.version.id)];
  if (node.version.label) parts.push(`(${node.version.label})`);
  const d = node.version.diff;
  const changed = [
    ...d.changed.map((p) => `~${p}`),
    ...d.added.map((p) => `+${p}`),
    ...d.removed.map((p) => `-${p}`),
  ];
  if (node.version.parentId === null) parts.push('root');
  else if (changed.length > 0) parts.push(changed.join(' '));
  return parts.join(' ');
}

/** What is special about a version, for its description. */
export function versionMarks(node: VersionTreeNode): string[] {
  const marks: string[] = [];
  if (node.isHead) marks.push('HEAD');
  if (node.isDisk) marks.push('on disk');
  if (node.isActivated) marks.push('activated');
  const fired = node.version.triggerRuns ?? 0;
  if (fired > 0) marks.push(`${fired} trigger run${fired === 1 ? '' : 's'}`);
  return marks;
}

/** A run's description: status, seed, scope, example. */
export function runDescription(run: VersionRun, headRun: string | null): string {
  const listing = run.listing;
  const parts = [describeOutcome(listing?.status ?? 'unknown', listing?.cancelCause, listing?.skippedNodes)];
  if (run.seedRunId) parts.push(`seed ${shortId(run.seedRunId)} (${run.stale.length} stale)`);
  if (run.spec) parts.push(`spec ${run.spec.name}`);
  if (run.example) parts.push(`example ${run.example}`);
  if (headRun === run.runId) parts.push('HEAD run');
  return parts.join('  ·  ');
}

/** Whether a run is scoped to `group` (or inside it): the runs the
 *  list marks when the editor is focused inside that group. */
export function runScopedTo(run: VersionRun, group: string | null): boolean {
  return !!run.spec && !!group && specScopedTo(run.spec, group);
}
