import { describe, expect, it } from 'vitest';
import { DispatcherClient } from './dispatcher';
import { RunFollower, MAX_REPLAY_BUFFER_BYTES, type DispatcherEvent, type LiveRun } from './runFollower';
import type { HostMessage } from '../../packages/weft-graph/src/protocol';
import type { LivePause } from './presence';

type Handlers = NonNullable<Parameters<DispatcherClient['subscribe']>[2]>;

class FakeDispatcher extends DispatcherClient {
  subscriptions: Array<{ handlers: Handlers; send: (data: string) => void; closed: boolean }> = [];
  reads: Array<{ path: string; signal?: AbortSignal; resolve: (events: DispatcherEvent[]) => void; reject: (err: Error) => void }> = [];

  constructor() { super('unused'); }

  override subscribe(_path: string, onEvent: (ev: { data: string }) => void, handlers: Handlers = {}) {
    const entry = { handlers, send: (data: string) => onEvent({ data }), closed: false };
    this.subscriptions.push(entry);
    return { close: () => { entry.closed = true; } };
  }

  override get<T>(path: string, signal?: AbortSignal): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      signal?.addEventListener('abort', () => reject(signal.reason), { once: true });
      this.reads.push({ path, signal, resolve: (events) => resolve(events as T), reject });
    });
  }
}

function completed(node: string): DispatcherEvent {
  return { eventId: `event:${node}`, kind: 'node_completed', runId: 'a', projectId: 'p', node, frames: [], output: {}, atUnix: 1 };
}

/** A pause the test flips by hand. */
class ManualPause implements LivePause {
  private value = false;
  private readonly listeners: Array<(paused: boolean) => void> = [];
  paused(): boolean { return this.value; }
  onChange(listener: (paused: boolean) => void): void { this.listeners.push(listener); }
  set(value: boolean): void {
    this.value = value;
    for (const listener of this.listeners) listener(value);
  }
}

function rig() {
  const client = new FakeDispatcher();
  const posted: HostMessage[] = [];
  const pause = new ManualPause();
  const follower = new RunFollower(client, (event) => posted.push(event), pause);
  return { client, follower, posted, pause };
}

describe('run history and live follow', () => {
  it('applies overlapping history once and preserves identical but distinct events', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    const event = completed('n');
    client.subscriptions[0].send(JSON.stringify(event));
    client.reads[0].resolve([event]);
    await following;
    client.subscriptions[0].send(JSON.stringify(event));
    client.subscriptions[0].send(JSON.stringify({ ...event, eventId: 'distinct' }));
    expect(posted.filter((e) => e.kind === 'firingEvent')).toHaveLength(2);
    follower.stop();
  });

  it('cancels an abandoned history request without waiting for its server', async () => {
    const { client, follower } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    follower.stop();
    expect(client.reads[0].signal?.aborted).toBe(true);
    await following;
  });

  it('ends a stalled replay when waiting live data exceeds its memory budget', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    client.subscriptions[0].send(JSON.stringify({ ...completed('large'), output: { text: 'x'.repeat(MAX_REPLAY_BUFFER_BYTES) } }));
    await following;
    expect(client.reads[0].signal?.aborted).toBe(true);
    expect(client.subscriptions[0].closed).toBe(true);
    expect(posted.at(-1)).toEqual({ kind: 'followLost', runId: 'a', reason: 'error' });
    expect(posted.some((e) => e.kind === 'firingEvent')).toBe(false);
  });

  it('waits for acceptance before reading history, then drains live events', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    expect(client.reads).toHaveLength(0);
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    expect(client.reads.map((read) => read.path)).toEqual(['/runs/a/replay']);
    client.subscriptions[0].send(JSON.stringify(completed('live')));
    expect(posted).toEqual([{ kind: 'runReset' }]);
    client.reads[0].resolve([completed('history')]);
    await following;
    expect(posted.filter((e) => e.kind === 'firingEvent').map((e) => e.event.nodeId))
      .toEqual(['history', 'live']);
    follower.stop();
  });

  it('settles a failed connection without fetching incomplete history', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onClosed?.();
    await following;
    expect(client.reads).toHaveLength(0);
    expect(client.subscriptions[0].closed).toBe(true);
    expect(posted.at(-1)).toEqual({ kind: 'followLost', runId: 'a', reason: 'closed' });
  });

  it('stopping while connecting settles the pending follow', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    follower.stop();
    await following;
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify(completed('stale')));
    expect(client.reads).toHaveLength(0);
    expect(posted).toEqual([{ kind: 'runReset' }]);
  });

  it('ignores old history and callbacks when reopening the same run', async () => {
    const { client, follower, posted } = rig();
    const first = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    const second = follower.replay('a');
    client.subscriptions[1].handlers.onOpen?.();
    await Promise.resolve();
    client.reads[1].resolve([completed('current')]);
    await second;
    client.reads[0].resolve([completed('stale')]);
    await first;
    client.subscriptions[0].send(JSON.stringify(completed('stale-live')));
    client.subscriptions[0].handlers.onClosed?.();
    expect(posted.filter((e) => e.kind === 'firingEvent').map((e) => e.event.nodeId))
      .toEqual(['current']);
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);
    follower.stop();
  });

  it('does not revive a broken stream when its history finally arrives', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    client.subscriptions[0].handlers.onClosed?.();
    client.reads[0].resolve([completed('stale')]);
    await following;
    expect(posted.some((e) => e.kind === 'firingEvent')).toBe(false);
    expect(posted.at(-1)).toEqual({ kind: 'followLost', runId: 'a', reason: 'closed' });
  });
});

describe('pausing while the person is away', () => {
  it('closes the stream on pause and reopens the same run, history first, on resume', async () => {
    const { client, follower, posted, pause } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    client.reads[0].resolve([completed('before')]);
    await following;

    pause.set(true);
    expect(client.subscriptions[0].closed).toBe(true);
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);

    pause.set(false);
    expect(client.subscriptions).toHaveLength(2);
    client.subscriptions[1].handlers.onOpen?.();
    await Promise.resolve();
    expect(client.reads[1].path).toBe('/runs/a/replay');
    client.reads[1].resolve([completed('before'), completed('meanwhile')]);
    await Promise.resolve();
    await Promise.resolve();
    expect(posted.filter((e) => e.kind === 'runReset')).toHaveLength(2);
    expect(posted.filter((e) => e.kind === 'firingEvent').map((e) => e.event.nodeId))
      .toEqual(['before', 'before', 'meanwhile']);
    follower.stop();
  });

  it('opens nothing for a run picked while paused until the person is back', async () => {
    const { client, follower, pause } = rig();
    pause.set(true);
    await follower.replay('a');
    expect(client.subscriptions).toHaveLength(0);
    pause.set(false);
    expect(client.subscriptions).toHaveLength(1);
    follower.stop();
  });

  it('does not revive a follow that was stopped or lost', async () => {
    const { client, follower, pause } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onClosed?.();
    await following;
    pause.set(true);
    pause.set(false);
    expect(client.subscriptions).toHaveLength(1);
    follower.stop();
    pause.set(true);
    pause.set(false);
    expect(client.subscriptions).toHaveLength(1);
  });
});

describe('which followed run has not ended', () => {
  const live: LiveRun = { runId: 'a', projectId: 'p' };

  function ended(runId: string): DispatcherEvent {
    return { eventId: `event:end:${runId}`, kind: 'run_completed', runId, projectId: 'p', outputs: {}, atUnix: 2 };
  }

  /** Follow `runId` with `history` as its replay, recording what the
   *  live-run listeners hear. */
  async function follow(history: DispatcherEvent[]) {
    const r = rig();
    const heard: Array<LiveRun | undefined> = [];
    r.follower.onLiveRun((run) => heard.push(run));
    const following = r.follower.replay('a');
    r.client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    expect(heard).toEqual([]);
    r.client.reads[0].resolve(history);
    await following;
    return { ...r, heard };
  }

  it('names a run once its history is applied, and drops it when the run ends', async () => {
    const { client, follower, heard } = await follow([completed('n')]);
    expect(heard).toEqual([live]);
    client.subscriptions[0].send(JSON.stringify(ended('a')));
    expect(heard).toEqual([live, undefined]);
    follower.stop();
    expect(heard).toEqual([live, undefined]);
  });

  it('names a run whose history held no event once a live one says its project', async () => {
    const { client, follower, heard } = await follow([]);
    expect(heard).toEqual([]);
    client.subscriptions[0].send(JSON.stringify(completed('n')));
    expect(heard).toEqual([live]);
    follower.stop();
  });

  it('never names a run whose history already holds its end', async () => {
    const { follower, heard } = await follow([completed('n'), ended('a')]);
    expect(heard).toEqual([]);
    follower.stop();
  });

  it('drops the run when the follow is lost or stopped', async () => {
    const lost = await follow([completed('n')]);
    lost.client.subscriptions[0].handlers.onClosed?.();
    expect(lost.heard).toEqual([live, undefined]);
    const stopped = await follow([completed('n')]);
    stopped.follower.stop();
    expect(stopped.heard).toEqual([live, undefined]);
  });

  it('drops the run the moment another one is followed', async () => {
    const { client, follower, heard } = await follow([completed('n')]);
    void follower.replay('b');
    expect(heard).toEqual([live, undefined]);
    client.subscriptions[1].handlers.onOpen?.();
    await Promise.resolve();
    expect(heard).toEqual([live, undefined]);
    follower.stop();
  });

  it('paints nothing for a parked run', async () => {
    const { client, follower, posted, heard } = await follow([completed('n')]);
    const before = posted.length;
    client.subscriptions[0].send(JSON.stringify({ eventId: 'live:1', kind: 'run_parked', runId: 'a', projectId: 'p' }));
    expect(posted).toHaveLength(before);
    expect(heard).toEqual([live]);
    follower.stop();
  });
});
