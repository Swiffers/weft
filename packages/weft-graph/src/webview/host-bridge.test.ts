import { describe, it, expect } from 'vitest';
import { translateProject } from './host-bridge';
import type { ProjectDefinition as HostProject } from '../protocol';

/** The regression this file pins: `translateProject` maps wire nodes
 *  FIELD BY FIELD, so any definition field it forgets is silently
 *  stripped from the editor's truth. `portLiterals` was dropped this
 *  way: a port field's value rendered until the first parse round-trip
 *  replaced the truth, then vanished from the graph while the source
 *  still carried it. */
describe('translateProject', () => {
  it('carries portLiterals and portLiteralSpans through the translation', () => {
    const host = {
      id: 'p1',
      nodes: [
        {
          id: 'orc',
          nodeType: 'OpenRouterConfig',
          label: null,
          config: {},
          position: { x: 0, y: 0 },
          scope: [],
          groupBoundary: null,
          inputs: [
            {
              name: 'systemPrompt', portType: 'String', required: false,
              accepts: ['literal', 'wire'], widget: { kind: 'textarea' }, default: 'be nice',
              label: 'System prompt', placeholder: 'You are...',
              declaredType: 'String',
              fromSpec: true, requiresScopes: ['chat:write'], requiresValues: { team: 'x' },
            },
          ],
          outputs: [
            { name: 'haiku', portType: 'String', required: false, declaredType: 'String' },
            { name: 'response', portType: 'Number', required: false },
          ],
          features: {},
          portLiterals: { systemPrompt: 'test' },
          portLiteralSpans: {
            systemPrompt: {
              span: { startLine: 7, startColumn: 2, endLine: 7, endColumn: 44 },
              origin: 'inline' as const,
            },
          },
        },
      ],
      edges: [],
      groups: [],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const node = v1.nodes.find((n) => n.id === 'orc')!;
    expect(node.portLiterals).toEqual({ systemPrompt: 'test' });
    expect(node.portLiteralSpans?.systemPrompt?.origin).toBe('inline');
    // The input's resolved editor surface survives too (the field
    // renderer + form toggle read all of it off the instance).
    expect(node.inputs[0].accepts).toEqual(['literal', 'wire']);
    expect(node.inputs[0].widget).toEqual({ kind: 'textarea' });
    expect(node.inputs[0].default).toBe('be nice');
    expect(node.inputs[0].label).toBe('System prompt');
    expect(node.inputs[0].placeholder).toBe('You are...');
    // `declaredType` survives on both sides. When the translation
    // dropped it, every parse round-trip un-declared every header
    // port, and the next ports gesture rewrote the header without
    // them: custom ports silently vanished from the source.
    expect(node.inputs[0].declaredType).toBe('String');
    expect(node.outputs[0].declaredType).toBe('String');
    expect(node.outputs[1].declaredType).toBeUndefined();
    // The permission-shortfall fields survive too (the same old copy
    // dropped them, deadening the shortfall check on parsed nodes).
    expect(node.inputs[0].fromSpec).toBe(true);
    expect(node.inputs[0].requiresScopes).toEqual(['chat:write']);
    expect(node.inputs[0].requiresValues).toEqual({ team: 'x' });
    // Every port is a FRESH object: the projection mutates the graph in
    // place and must never alias the parse message.
    expect(node.inputs[0]).not.toBe(host.nodes[0].inputs[0]);
  });

  it('labels a group by its local id segment, never the full dotted path', () => {
    const group = (id: string, parentGroupId: string | null) => ({
      id,
      kind: 'group' as const,
      label: null,
      inPorts: [],
      outPorts: [],
      parentGroupId,
      childGroupIds: [],
      nodeIds: [],
      anonymous: false,
    });
    const host = {
      id: 'p1',
      nodes: [],
      edges: [],
      groups: [group('outer', null), group('outer.inner', 'outer')],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const labels = new Map(v1.nodes.map((n) => [n.id, n.label]));
    expect(labels.get('outer')).toBe('outer');
    expect(labels.get('outer.inner')).toBe('inner');
  });

  it("labels an included file's body by its file name, never its path id", () => {
    // Opening an included file parses it under its body id, which is the
    // file's path and is unspellable on purpose. The box the reader sees
    // is that file, so it reads as the file.
    const group = (id: string, parentGroupId: string | null) => ({
      id,
      kind: 'group' as const,
      label: null,
      inPorts: [],
      outPorts: [],
      parentGroupId,
      childGroupIds: [],
      nodeIds: [],
      anonymous: false,
    });
    const host = {
      id: 'p1',
      nodes: [],
      edges: [],
      groups: [group('@src:cards:read_cards', null), group('@src:cards:read_cards.rows', '@src:cards:read_cards')],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const labels = new Map(v1.nodes.map((n) => [n.id, n.label]));
    expect(labels.get('@src:cards:read_cards')).toBe('read_cards');
    expect(labels.get('@src:cards:read_cards.rows')).toBe('rows');
  });

  it("marks an included file's own group, and no other", () => {
    // The file's top-level group is the one the editor opens by default
    // (`expandedOf`), so the flag has to survive the translation.
    const group = (id: string, parentGroupId: string | null, anonymous: boolean) => ({
      id,
      kind: 'group' as const,
      label: null,
      inPorts: [],
      outPorts: [],
      parentGroupId,
      childGroupIds: [],
      nodeIds: [],
      anonymous,
    });
    const host = {
      id: 'p1',
      nodes: [],
      edges: [],
      groups: [group('@src:cards:read_cards', null, true), group('@src:cards:read_cards.rows', '@src:cards:read_cards', false)],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const anonymous = new Map(v1.nodes.map((n) => [n.id, n.anonymous]));
    expect(anonymous.get('@src:cards:read_cards')).toBe(true);
    expect(anonymous.get('@src:cards:read_cards.rows')).toBeUndefined();
  });

  it("carries a group's `_should_flow` literal across as a port literal", () => {
    const host = {
      id: 'p1',
      nodes: [],
      edges: [],
      groups: [
        {
          id: 'off', kind: 'group' as const, label: null,
          inPorts: [], outPorts: [],
          parentGroupId: null, childGroupIds: [], nodeIds: [], anonymous: false,
          portLiterals: { _should_flow: false },
        },
        {
          id: 'on', kind: 'group' as const, label: null,
          inPorts: [], outPorts: [],
          parentGroupId: null, childGroupIds: [], nodeIds: [], anonymous: false,
        },
      ],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const byId = new Map(v1.nodes.map((n) => [n.id, n]));
    expect(byId.get('off')?.portLiterals).toEqual({ _should_flow: false });
    expect(byId.get('on')?.portLiterals).toBeUndefined();
  });

  it("puts a group's `@file` marker back in place of the resolved content, as for a node", () => {
    const ref = { path: 'prompt.txt', type: 'String', marker: 'file' as const };
    const marker = { __weftFileRef: ref };
    const host = {
      id: 'p1',
      nodes: [
        {
          id: 'n', nodeType: 'Text', label: null, config: {},
          position: { x: 0, y: 0 }, scope: [], groupBoundary: null,
          inputs: [], outputs: [], features: {},
          portLiterals: { text: 'resolved content' },
          fileRefs: { text: ref },
        },
      ],
      edges: [],
      groups: [
        {
          id: 'g', kind: 'group' as const, label: null,
          inPorts: [], outPorts: [],
          parentGroupId: null, childGroupIds: [], nodeIds: [], anonymous: false,
          portLiterals: { prompt: 'resolved content', other: 'plain' },
          fileRefs: { prompt: ref },
        },
      ],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const byId = new Map(v1.nodes.map((n) => [n.id, n]));
    expect(byId.get('g')?.portLiterals).toEqual({ prompt: marker, other: 'plain' });
    expect(byId.get('n')?.portLiterals).toEqual({ text: marker });
  });
});

/** The compiled program has one wire per value, end to end, naming the
 *  group ports it passes; the editor draws the legs a person wrote. */
describe('legsOf: a compiled wire back into the legs a person wrote', () => {
  const group = (id: string, parentGroupId: string | null = null, kind: 'group' | 'loop' = 'group') => ({
    id, kind, label: null, inPorts: [], outPorts: [], parentGroupId,
    childGroupIds: [], nodeIds: [], anonymous: false,
    ...(kind === 'loop' ? { loopConfig: {} } : {}),
  });
  const node = (id: string, scope: string[] = [], groupBoundary: { groupId: string; role: 'In' | 'Out' } | null = null) => ({
    id, nodeType: groupBoundary ? (groupBoundary.role === 'In' ? 'LoopIn' : 'LoopOut') : 'Text',
    label: null, config: {}, position: { x: 0, y: 0 }, scope, groupBoundary,
    inputs: [], outputs: [], features: {},
  });
  const legs = (host: unknown) =>
    translateProject(host as HostProject, 'src', '').edges
      .map((e) => `${e.source}.${e.sourceHandle} -> ${e.target}.${e.targetHandle}${e.path ? ` [${e.path.join('.')}]` : ''}`)
      .sort();

  it('draws the leg into a fanned-out group port once, with a stable id', () => {
    const host = {
      id: 'p', groups: [group('g')],
      nodes: [node('a'), node('g.m1', ['g']), node('g.m2', ['g'])],
      edges: [
        { id: 'e-a-out-in:g:x-g.m1-in', source: 'a', sourceHandle: 'out', target: 'g.m1', targetHandle: 'in', via: [{ group: 'g', port: 'x', side: 'in' }] },
        { id: 'e-a-out-in:g:x-g.m2-in', source: 'a', sourceHandle: 'out', target: 'g.m2', targetHandle: 'in', via: [{ group: 'g', port: 'x', side: 'in' }] },
      ],
    };
    expect(legs(host)).toEqual([
      'a.out -> g.x',
      'g.x__inner -> g.m1.in',
      'g.x__inner -> g.m2.in',
    ]);
    // The id is the leg's four ends, the same one the optimistic
    // projection gives a leg it adds, so a reparse keeps the leg's identity.
    const v1 = translateProject(host as unknown as HostProject, 'src', '');
    expect(v1.edges.map((e) => e.id).sort()).toEqual([
      'e-a-out-g-x',
      'e-g-x__inner-g.m1-in',
      'e-g-x__inner-g.m2-in',
    ]);
  });

  it("draws a send from a group's own input as the inside leg alone", () => {
    // `g.x = 5` outside: nothing leads to the port, the group sends the
    // literal when it opens.
    const host = {
      id: 'p', groups: [group('g')],
      nodes: [node('g.m', ['g'])],
      edges: [
        { id: 'e-g-x-in:g:x-g.m-in', source: 'g', sourceHandle: 'x', target: 'g.m', targetHandle: 'in', via: [{ group: 'g', port: 'x', side: 'in' }] },
      ],
    };
    expect(legs(host)).toEqual(['g.x__inner -> g.m.in']);
  });

  it('draws a wire that stops at a port nobody reads as the leg up to it', () => {
    const host = {
      id: 'p', groups: [group('g')],
      nodes: [node('a'), node('g.m', ['g'])],
      edges: [
        // Into an input no member reads.
        { id: 'e-a-out-in:g:x-g-x', source: 'a', sourceHandle: 'out', target: 'g', targetHandle: 'x', via: [{ group: 'g', port: 'x', side: 'in' }] },
        // Out of a member into an output nobody outside reads.
        { id: 'e-g.m-out-out:g:y-g-y', source: 'g.m', sourceHandle: 'out', target: 'g', targetHandle: 'y', via: [{ group: 'g', port: 'y', side: 'out' }] },
      ],
    };
    expect(legs(host)).toEqual([
      'a.out -> g.x',
      'g.m.out -> g.y__inner',
    ]);
  });

  it("draws a gate wire as legs ending on the group's own gate", () => {
    // `g._should_flow = h.done`, where `h.done` is written inside `h`.
    const host = {
      id: 'p', groups: [group('h'), group('g')],
      nodes: [node('h.k', ['h']), node('g.m', ['g'])],
      edges: [
        { id: 'e-h.k-ok-out:h:done-g-_should_flow', source: 'h.k', sourceHandle: 'ok', target: 'g', targetHandle: '_should_flow', via: [{ group: 'h', port: 'done', side: 'out' }] },
      ],
    };
    expect(legs(host)).toEqual([
      'h.done -> g._should_flow',
      'h.k.ok -> h.done__inner',
    ]);
  });

  it('draws a wire across two nested groups as one leg per line, each with its own keys', () => {
    // `outer.x = a.out.p`, `inner.y = self.x`, `m.in = self.y.q`.
    const host = {
      id: 'p', groups: [group('outer'), group('outer.inner', 'outer')],
      nodes: [node('a'), node('outer.inner.m', ['outer', 'outer.inner'])],
      edges: [
        {
          id: 'e-a-out-in:outer:x-in:outer.inner:y-outer.inner.m-in',
          source: 'a', sourceHandle: 'out', target: 'outer.inner.m', targetHandle: 'in',
          path: ['p', 'q'],
          via: [
            { group: 'outer', port: 'x', side: 'in', path: ['p'] },
            { group: 'outer.inner', port: 'y', side: 'in' },
          ],
        },
      ],
    };
    expect(legs(host)).toEqual([
      'a.out -> outer.x [p]',
      'outer.inner.y__inner -> outer.inner.m.in [q]',
      'outer.x__inner -> outer.inner.y',
    ]);
  });

  it("draws a loop's boundary nodes as the loop box's two faces", () => {
    const host = {
      id: 'p', groups: [group('l', null, 'loop')],
      nodes: [
        node('a'), node('b'), node('l.m', ['l']),
        node('l__in', [], { groupId: 'l', role: 'In' }),
        node('l__out', [], { groupId: 'l', role: 'Out' }),
      ],
      edges: [
        { id: 'e-a-out-l__in-items', source: 'a', sourceHandle: 'out', target: 'l__in', targetHandle: 'items' },
        { id: 'e-l__in-items-l.m-in', source: 'l__in', sourceHandle: 'items', target: 'l.m', targetHandle: 'in' },
        { id: 'e-l.m-out-l__out-all', source: 'l.m', sourceHandle: 'out', target: 'l__out', targetHandle: 'all' },
        { id: 'e-l__out-all-b-in', source: 'l__out', sourceHandle: 'all', target: 'b', targetHandle: 'in' },
      ],
    };
    expect(legs(host)).toEqual([
      'a.out -> l.items',
      'l.all -> b.in',
      'l.items__inner -> l.m.in',
      'l.m.out -> l.all__inner',
    ]);
    const v1 = translateProject(host as unknown as HostProject, 'src', '');
    expect(v1.nodes.map((n) => n.id).sort()).toEqual(['a', 'b', 'l', 'l.m']);
  });
});
