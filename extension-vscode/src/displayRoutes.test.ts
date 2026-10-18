import { describe, expect, it } from 'vitest';
import { displayedNodeOf, displayRoutesOf, displayStreamPath, ownDisplaysLost, type DisplayRoute } from './displayRoutes';
import type { ParseResponse } from '../../packages/weft-graph/src/protocol';

/** A parse holding `nodes`, with an empty catalog: every role comes
 *  from the parsed node itself. */
function parsed(nodes: Array<Record<string, unknown>>): Pick<ParseResponse, 'project' | 'catalog'> {
  return { project: { nodes }, catalog: {} } as unknown as Pick<ParseResponse, 'project' | 'catalog'>;
}

describe('where each node on screen has its display from', () => {
  const response = parsed([
    { id: 'door', nodeType: 'Route', features: { isTrigger: true } },
    { id: 'box', nodeType: 'Box', requiresInfra: true, features: { liveEndpoint: '/live' } },
    { id: 'tcp', nodeType: 'Box', requiresInfra: true },
    { id: 'render', nodeType: 'Poll' },
    { id: 'copy', nodeType: 'Poll', perInstance: { source: 'each' } },
  ]);

  it('triggers from their signal, serving infra from its container, the rest from a wait', () => {
    expect(Object.fromEntries(displayRoutesOf(response, ['one']))).toEqual({
      door: { source: 'signal', node: 'one.door' },
      box: { source: 'infra', node: 'one.box' },
      tcp: { source: 'wait', node: 'one.tcp' },
      render: { source: 'wait', node: 'one.render' },
    });
  });

  it('maps a message to the node at its place and source, and drops one for no node on screen', () => {
    const routes = displayRoutesOf(response, ['one']);
    expect(displayedNodeOf(routes, { source: 'wait', node: 'one.render' })).toBe('render');
    expect(displayedNodeOf(routes, { source: 'signal', node: 'one.door' })).toBe('door');
    // A wait message about a trigger is not the trigger's own display.
    expect(displayedNodeOf(routes, { source: 'wait', node: 'one.door' })).toBeUndefined();
    expect(displayedNodeOf(routes, { source: 'wait', node: 'two.render' })).toBeUndefined();
  });
});

describe('the display stream path', () => {
  const routes: DisplayRoute[] = [
    { source: 'signal', node: 'one.door' },
    { source: 'infra', node: 'b' },
    { source: 'infra', node: 'a' },
    { source: 'wait', node: 'render' },
  ];

  const run = { runId: 'r1', projectId: 'p' };

  it('names infra and trigger nodes, sorted, and the followed run when there is one', () => {
    expect(displayStreamPath('p', routes, undefined))
      .toBe('/events/project/p/displays?infra=a,b&signals=one.door');
    expect(displayStreamPath('p', routes, run))
      .toBe('/events/project/p/displays?infra=a,b&signals=one.door&run=r1');
  });

  it('asks for the run alone when no infra or trigger node is on screen', () => {
    const waitsOnly: DisplayRoute[] = [{ source: 'wait', node: 'render' }];
    expect(displayStreamPath('p', waitsOnly, run)).toBe('/events/project/p/displays?run=r1');
    expect(displayStreamPath('p', waitsOnly, undefined)).toBeUndefined();
    expect(displayStreamPath('p', [], undefined)).toBeUndefined();
  });

  it('leaves out a followed run of another project', () => {
    const elsewhere = { runId: 'r1', projectId: 'q' };
    expect(displayStreamPath('p', routes, elsewhere))
      .toBe('/events/project/p/displays?infra=a,b&signals=one.door');
    expect(displayStreamPath('p', [{ source: 'wait', node: 'render' }], elsewhere)).toBeUndefined();
  });
});

describe('the nodes that lose their own display', () => {
  const routes = (entries: Array<[string, DisplayRoute]>) => new Map(entries);

  it('names an infra or trigger node whose route changed or left, never a wait', () => {
    const previous = routes([
      ['box', { source: 'infra', node: 'box' }],
      ['door', { source: 'signal', node: 'door' }],
      ['gone', { source: 'infra', node: 'gone' }],
      ['moved', { source: 'signal', node: 'one.moved' }],
      ['kept', { source: 'infra', node: 'kept' }],
      ['render', { source: 'wait', node: 'render' }],
    ]);
    const next = routes([
      ['box', { source: 'wait', node: 'box' }],
      ['door', { source: 'infra', node: 'door' }],
      ['moved', { source: 'signal', node: 'two.moved' }],
      ['kept', { source: 'infra', node: 'kept' }],
    ]);
    expect(ownDisplaysLost(previous, next)).toEqual(['box', 'door', 'gone', 'moved']);
  });
});
