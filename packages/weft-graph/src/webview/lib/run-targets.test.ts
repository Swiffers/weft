import { describe, it, expect } from 'vitest';
import { runInfraIds, runProgress, runCheckKey } from './run-targets';

describe('runInfraIds', () => {
  // trigger -> mid -> out ; bridge(infra) -> door ; side -> door
  const nodes = [
    { id: 'trigger', isTrigger: true, isInfra: false },
    { id: 'mid', isTrigger: false, isInfra: false },
    { id: 'out', isTrigger: false, isInfra: false },
    { id: 'bridge', isTrigger: false, isInfra: true },
    { id: 'side', isTrigger: false, isInfra: false },
    { id: 'door', isTrigger: false, isInfra: false },
  ];
  const edges = [
    { source: 'trigger', target: 'mid' },
    { source: 'mid', target: 'out' },
    { source: 'bridge', target: 'door' },
    { source: 'side', target: 'door' },
  ];

  it('reports the infra inside the joined subgraph and nothing else', () => {
    expect(runInfraIds(['door'], nodes, edges)).toEqual(['bridge']);
    expect(runInfraIds(['out'], nodes, edges)).toEqual([]);
  });

  it('does not walk through a trigger to infra above it', () => {
    const edges2 = [...edges, { source: 'bridge', target: 'trigger' }];
    expect(runInfraIds(['out'], nodes, edges2)).toEqual([]);
  });

  it('a target inside a group brings the group, its inputs, and its siblings', () => {
    // feed -> grp ; grp contains inner (the target) and db (infra) ; trigger -> feed
    const scoped = [
      { id: 'trigger', isTrigger: true, isInfra: false },
      { id: 'feed', isTrigger: false, isInfra: false },
      { id: 'grp', isTrigger: false, isInfra: false },
      { id: 'inner', isTrigger: false, isInfra: false, parentId: 'grp' },
      { id: 'db', isTrigger: false, isInfra: true, parentId: 'grp' },
    ];
    const scopedEdges = [
      { source: 'trigger', target: 'feed' },
      { source: 'feed', target: 'grp' },
    ];
    expect(runInfraIds(['inner'], scoped, scopedEdges)).toEqual(['db']);
  });

  it('reaches infra nested two containers deep', () => {
    // grp contains inner (the target) and sub ; sub contains db (infra)
    const scoped = [
      { id: 'grp', isTrigger: false, isInfra: false },
      { id: 'inner', isTrigger: false, isInfra: false, parentId: 'grp' },
      { id: 'sub', isTrigger: false, isInfra: false, parentId: 'grp' },
      { id: 'db', isTrigger: false, isInfra: true, parentId: 'sub' },
    ];
    expect(runInfraIds(['inner'], scoped, [])).toEqual(['db']);
  });

  it('a container target brings what is inside it', () => {
    const scoped = [
      { id: 'grp', isTrigger: false, isInfra: false },
      { id: 'db', isTrigger: false, isInfra: true, parentId: 'grp' },
    ];
    expect(runInfraIds(['grp'], scoped, [])).toEqual(['db']);
  });
});

describe('runProgress', () => {
  it('counts the places whose latest firing ended, out of the run\'s places', () => {
    const firings = {
      a: [{ status: 'completed' as const }],
      b: [{ status: 'completed' as const }, { status: 'running' as const }],
      c: [{ status: 'skipped' as const }],
      d: [{ status: 'waiting_for_input' as const }],
    };
    expect(runProgress(firings, ['a', 'b', 'c', 'd', 'e'])).toEqual({ done: 2, total: 5 });
  });

  it('has no total when the run\'s program could not be read', () => {
    expect(runProgress({}, null)).toEqual({ done: 0, total: null });
  });
});

describe('runCheckKey', () => {
  const src = 'a = Text\n';
  it('gives the same key for the same selection in a new array', () => {
    const selection = ['b', 'a.x'];
    expect(runCheckKey([...selection], src)).toBe(runCheckKey(selection, src));
    expect(runCheckKey(['a.x', 'b'], src)).toBe(runCheckKey(['b', 'a.x'], src));
  });

  it('gives another key when the selection changes', () => {
    expect(runCheckKey(['a', 'b'], src)).not.toBe(runCheckKey(['a'], src));
    expect(runCheckKey(['a', 'b'], src)).not.toBe(runCheckKey(['a', 'c'], src));
  });

  it('gives another key when the program changes under the same selection', () => {
    expect(runCheckKey(['a'], src)).not.toBe(runCheckKey(['a'], 'a = Text\nb = Text\n'));
  });

  it('cannot confuse two places with one whose name holds the separator', () => {
    expect(runCheckKey(['a,b'], src)).not.toBe(runCheckKey(['a', 'b'], src));
  });
});
