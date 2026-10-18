import { describe, it, expect } from 'vitest';
import {
  bodyFeedOf,
  nodeIsTrigger,
  nodeRequiresInfra,
  projectHasInfra,
  projectHasTriggers,
} from './node-roles';
import { INCLUDE_NODE_TYPE } from '../types';

/// An `@include` as the editor's parse delivers it: one opaque node, no
/// body, plus a record of what the file behind it holds.
function includeNode(contents: { requiresInfra?: boolean; hasTrigger?: boolean }) {
  return { nodeType: INCLUDE_NODE_TYPE, includePath: 'lib/thing.weft', includeContents: contents };
}

describe('the project-level questions count through includes', () => {
  it('finds a trigger that only exists inside an included file', () => {
    const nodes = [{ nodeType: 'Text' }, includeNode({ hasTrigger: true })];
    expect(projectHasTriggers(nodes)).toBe(true);
    // This is the bug it fixes: the graph shows one plain node and one
    // opaque box, so counting per-node says the project has no trigger
    // and the Activate button never appears.
    expect(nodes.some(nodeIsTrigger)).toBe(false);
  });

  it('finds an infra node that only exists inside an included file', () => {
    const nodes = [{ nodeType: 'Text' }, includeNode({ requiresInfra: true })];
    expect(projectHasInfra(nodes)).toBe(true);
    expect(nodes.some(nodeRequiresInfra)).toBe(false);
  });

  it('says no when the included file holds neither', () => {
    const nodes = [{ nodeType: 'Text' }, includeNode({})];
    expect(projectHasTriggers(nodes)).toBe(false);
    expect(projectHasInfra(nodes)).toBe(false);
  });

  it('still counts a visible node, include or no include', () => {
    const nodes = [{ nodeType: 'Route', features: { isTrigger: true } }];
    expect(projectHasTriggers(nodes)).toBe(true);
    const infra = [{ nodeType: 'PostgresDatabase', requiresInfra: true }];
    expect(projectHasInfra(infra)).toBe(true);
  });

  it('leaves out a per-instance node, whose copies are each instance\'s to run', () => {
    const trigger = { nodeType: 'Route', features: { isTrigger: true }, perInstance: 'marked' as const };
    const infra = { nodeType: 'PostgresDatabase', requiresInfra: true, perInstance: 'marked' as const };
    expect(projectHasTriggers([trigger])).toBe(false);
    expect(projectHasInfra([infra])).toBe(false);
  });

  it('is false on an empty graph', () => {
    expect(projectHasTriggers([])).toBe(false);
    expect(projectHasInfra([])).toBe(false);
  });
});

describe('which display a node shows on its body', () => {
  const trigger = { nodeType: 'Route', features: { isTrigger: true } };
  const servingInfra = { nodeType: 'Box', requiresInfra: true, features: { liveEndpoint: '/live' } };
  const tcpInfra = { nodeType: 'Box', requiresInfra: true };
  const plain = { nodeType: 'Text' };
  const shown = { state: 'ok' as const, items: [{ type: 'text' as const, label: 'Last answer', data: '{}' }] };
  const failed = { state: 'error' as const, error: 'could not read the wait' };
  const absent = { state: 'absent' as const };

  it('a trigger or a serving infra node shows every state, absent included', () => {
    for (const node of [trigger, servingInfra]) {
      expect(bodyFeedOf(node, shown)).toBe(shown);
      expect(bodyFeedOf(node, failed)).toBe(failed);
      // Absent names the button that brings the display back.
      expect(bodyFeedOf(node, absent)).toBe(absent);
      expect(bodyFeedOf(node, undefined)).toBeUndefined();
    }
  });

  it('any other node shows a wait while it has one, and nothing once it is over', () => {
    for (const node of [plain, tcpInfra]) {
      expect(bodyFeedOf(node, shown)).toBe(shown);
      expect(bodyFeedOf(node, failed)).toBe(failed);
      expect(bodyFeedOf(node, absent)).toBeUndefined();
      expect(bodyFeedOf(node, undefined)).toBeUndefined();
    }
  });
});
