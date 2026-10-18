// THE one parse of the dispatcher's
// `/projects/{id}/status` payload (identical whether it arrives via
// `weft status --json` on the CLI or a direct HTTP fetch). Every host
// (the VS Code extension, a browser-based editor) builds its
// `ActionAvailability` snapshot AND its `BackendSnapshot` through
// here, so the mapping can never fork between them.

import type {
  ActionAvailability,
  ActionBarState,
  ActionVerb,
  ChangeProgress,
  BackendSnapshot,
  RunPhase,
  ProjectTransition,
} from './protocol';
import { RUN_PHASES } from './protocol';

/// The raw dispatcher status payload (the wire shape). Only the
/// fields the frontends consume; extra fields are ignored.
// SYNC: RawStatusPayload <-> crates/weft-core/src/projects.rs ProjectStatusResponse
export interface RawStatusPayload {
  /** `weft rm` marked the project and has not finished removing it. */
  removing?: boolean;
  status?: string;
  transition?: string;
  mode?: string;
  firesDeadlineUnix?: number;
  runningCount?: number;
  orphanedInfra?: boolean;
  infraRollup?: string;
  infraBusy?: boolean;
  builds?: Array<{ image: string; build: string; startedAtUnix: number; logUrl?: string }>;
  infra?: Array<{
    node?: string;
    nodeType?: string;
    status?: string;
    failureStage?: string;
    failureMessage?: string;
    instanceCopyCount?: number;
    progress?: ChangeProgress;
  }>;
  drift?: {
    binaryDrift?: boolean;
    definitionDrift?: boolean;
    infraDrift?: boolean;
    activationDrift?: boolean;
  };
  availableActions?: string[];
  preservation?: { parked?: number; suspended?: number };
  activations?: Array<{ trigger?: string; instance?: string; status?: string }>;
  runs?: {
    lastStatus?: string;
    lastRunId?: string;
    /** Every run running right now, oldest first, with what it
     *  is for; the editor replaces its running set with this on each
     *  refresh. */
    running?: GoingRun[];
  };
}

/// What a person is told about a project `weft rm` marked and has not
/// finished removing (`RawStatusPayload.removing`).
// SYNC: PROJECT_BEING_REMOVED <-> crates/weft-core/src/projects.rs PROJECT_BEING_REMOVED
export const PROJECT_BEING_REMOVED = 'the project is being removed; run `weft rm` again to finish removing it';

// SYNC: infraRollup values <-> crates/weft-core/src/projects.rs ProjectStatusResponse.infra_rollup
const VALID_ROLLUPS = [
  'none',
  'stopped',
  'partial',
  'running',
  'failed',
  'flaky',
  'stopping',
  'terminating',
  'provisioning',
] as const;

const VALID_STATUSES = [
  'registered',
  'activating',
  'active',
  'deactivating',
  'inactive',
] as const;

// SYNC: VALID_TRANSITIONS <-> crates/weft-core/src/projects.rs ProjectTransition, packages/weft-graph/src/protocol.ts ProjectTransition
const VALID_TRANSITIONS: ProjectTransition[] = ['none', 'building', 'cancelling_build', 'resyncing'];

/// Collapse a raw transition string to the enum, resting on 'none' for
/// an unknown value (a version-skewed dispatcher shouldn't brick the
/// bar). Shared by the status parse and the SSE transition event.
export function parseTransition(raw: unknown): ProjectTransition {
  const s = String(raw ?? 'none');
  return VALID_TRANSITIONS.includes(s as ProjectTransition) ? (s as ProjectTransition) : 'none';
}

/// The honest snapshot for a project the dispatcher doesn't know yet
/// (first graph open, post-wipe): empty verb list (starter verbs light
/// from graph shape), everything at rest. Hosts return this instead of
/// faking a verb list when the status fetch 404s.
export function emptyActionAvailability(): ActionAvailability {
  return {
    removing: false,
    availableActions: [],
    binaryDrift: false,
    definitionDrift: false,
    infraDrift: false,
    projectStatus: 'unknown',
    transition: 'none',
    orphanedInfra: false,
    mode: 'unknown',
    runningCount: 0,
    triggersOff: [],
    infraRollup: 'none',
    infraBusy: false,
    infraNodes: [],
    preservation: { parked: 0, suspended: 0 },
  };
}

/// Parse a raw status payload into the shared `ActionAvailability`.
/// Unknown enum strings collapse to their resting value rather than
/// crashing the bar (a version-skewed dispatcher shouldn't brick the
/// UI); the raw string is still visible via a status refetch once the
/// client updates.
export function parseStatusPayload(raw: RawStatusPayload): ActionAvailability {
  const drift = raw.drift ?? {};
  const statusStr = String(raw.status ?? 'unknown');
  const projectStatus = (VALID_STATUSES as readonly string[]).includes(statusStr)
    ? (statusStr as ActionAvailability['projectStatus'])
    : 'unknown';
  const transition = parseTransition(raw.transition);
  const rollupStr = String(raw.infraRollup ?? 'none');
  const infraRollup = (VALID_ROLLUPS as readonly string[]).includes(rollupStr)
    ? (rollupStr as ActionAvailability['infraRollup'])
    : 'none';
  const firesDeadlineUnix =
    typeof raw.firesDeadlineUnix === 'number' ? raw.firesDeadlineUnix : undefined;
  const infraNodes = (Array.isArray(raw.infra) ? raw.infra : []).map((n) => ({
    node: n.node ?? '',
    nodeType: n.nodeType ?? '',
    status: n.status ?? 'unknown',
    ...(n.failureStage !== undefined ? { failureStage: n.failureStage } : {}),
    ...(n.failureMessage !== undefined ? { failureMessage: n.failureMessage } : {}),
    ...(n.instanceCopyCount !== undefined ? { instanceCopyCount: n.instanceCopyCount } : {}),
    ...(n.progress !== undefined ? { progress: progressOnLocalClock(n.progress, Date.now() / 1000) } : {}),
  }));
  return {
    removing: !!raw.removing,
    availableActions: (Array.isArray(raw.availableActions)
      ? raw.availableActions
      : []) as ActionVerb[],
    binaryDrift: !!drift.binaryDrift,
    definitionDrift: !!drift.definitionDrift,
    infraDrift: !!drift.infraDrift,
    projectStatus,
    transition,
    orphanedInfra: !!raw.orphanedInfra,
    mode: String(raw.mode ?? 'unknown'),
    ...(firesDeadlineUnix !== undefined ? { firesDeadlineUnix } : {}),
    runningCount: Number(raw.runningCount ?? 0),
    triggersOff: triggersOff(raw),
    infraRollup,
    infraBusy: !!raw.infraBusy,
    infraNodes,
    preservation: {
      parked: Number(raw.preservation?.parked ?? 0),
      suspended: Number(raw.preservation?.suspended ?? 0),
    },
  };
}

/// The program's own triggers that are off: registered and never
/// switched on, or taken down. An instance's copies are its own and stay
/// out of it, as the bar acts on the program's.
// SYNC: triggersOff <-> crates/weft-dispatcher/src/api/project.rs is_down
function triggersOff(raw: RawStatusPayload): string[] {
  return (raw.activations ?? [])
    .filter((a) => !a.instance && (a.status === 'registered' || a.status === 'inactive'))
    .map((a) => a.trigger ?? '')
    .filter((trigger) => trigger !== '');
}

/// One run running right now, and what it is for.
// SYNC: GoingRun <-> crates/weft-core/src/projects.rs GoingRun
export interface GoingRun {
  runId: string;
  phase: RunPhase;
}

/// The running runs a status payload names, oldest first. An
/// entry whose phase is not one the editor knows is a wire drift and
/// throws, rather than being shown as a run it may not be.
export function parseRunning(raw: RawStatusPayload): GoingRun[] {
  const running = raw.runs?.running ?? [];
  return running.map((entry) => {
    if (typeof entry?.runId !== 'string' || !RUN_PHASES.includes(entry.phase)) {
      throw new Error(`status names a running run the editor cannot read: ${JSON.stringify(entry)}`);
    }
    return { runId: entry.runId, phase: entry.phase };
  });
}

/// The bar before any status has arrived: nothing offered, nothing
/// running. Every host starts from this one value.
export function idleActionBarState(): ActionBarState {
  return {
    backend: backendFromSnapshot(emptyActionAvailability()),
    overlay: { kind: 'idle' },
    infraSetup: false,
  };
}

/// Project the snapshot down to the action bar's `BackendSnapshot`.
/// One derivation so the two shapes can't drift apart per host.
export function backendFromSnapshot(snapshot: ActionAvailability): BackendSnapshot {
  return {
    removing: snapshot.removing,
    available: snapshot.availableActions,
    status: snapshot.projectStatus,
    transition: snapshot.transition,
    orphanedInfra: snapshot.orphanedInfra,
    mode: snapshot.mode,
    infraRollup: snapshot.infraRollup,
    infraBusy: snapshot.infraBusy,
    runningCount: snapshot.runningCount,
    triggersOff: snapshot.triggersOff,
    ...(snapshot.firesDeadlineUnix !== undefined
      ? { firesDeadlineUnix: snapshot.firesDeadlineUnix }
      : {}),
  };
}

/** The server's progress, moved onto this machine's clock: both stamps
 *  shift by how far this clock reads from the server's when the answer
 *  arrived (`localNowUnix`). Read against `Date.now()` afterwards, the
 *  duration is the server's at the answer plus the time elapsed here
 *  since, so it keeps ticking between answers whatever the two clocks
 *  say. */
export function progressOnLocalClock(progress: ChangeProgress, localNowUnix: number): ChangeProgress {
  const shift = localNowUnix - progress.asOfUnix;
  return { ...progress, sinceUnix: progress.sinceUnix + shift, asOfUnix: localNowUnix };
}

/** How far a change of an infra copy got, as a person reads it: "for
 *  3m12s, waiting on: ...". `nowUnix` is on the clock the progress's
 *  stamps are on (this machine's, once `parseStatusPayload` moved it). */
// SYNC: describeProgress <-> crates/weft-core/src/infra/wire.rs ChangeProgress::describe
export function describeProgress(progress: ChangeProgress, nowUnix: number): string {
  const secs = Math.max(0, Math.floor(nowUnix - progress.sinceUnix));
  const elapsed = secs >= 60 ? `${Math.floor(secs / 60)}m${String(secs % 60).padStart(2, '0')}s` : `${secs}s`;
  return progress.waiting ? `for ${elapsed}, waiting on: ${progress.waiting}` : `for ${elapsed}`;
}
