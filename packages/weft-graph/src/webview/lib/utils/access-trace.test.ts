import { describe, it, expect } from 'vitest';
import { accessFeedingEdge } from './access-trace';

const edge = (source: string, sourceHandle: string, target: string, targetHandle: string) =>
	({ source, sourceHandle, target, targetHandle });

describe('accessFeedingEdge', () => {
	it('finds the access node wired straight into a model node', () => {
		const edges = [edge('openrouter', 'access', 'model', 'access')];
		expect(accessFeedingEdge(edges, 'model', 'access')?.source).toBe('openrouter');
	});

	it('follows the wire out through the group ports it was threaded down', () => {
		// openrouter.access -> outer.conn ; outer's inside leg -> inner.conn ; inner's inside leg -> model.access
		const edges = [
			edge('openrouter', 'access', 'outer', 'conn'),
			edge('outer', 'conn__inner', 'inner', 'conn'),
			edge('inner', 'conn__inner', 'model', 'access'),
		];
		expect(accessFeedingEdge(edges, 'model', 'access')?.source).toBe('openrouter');
	});

	it('is null when nothing feeds the input, or the group port is unwired outside', () => {
		expect(accessFeedingEdge([], 'model', 'access')).toBeNull();
		expect(accessFeedingEdge([edge('g', 'conn__inner', 'model', 'access')], 'model', 'access')).toBeNull();
	});
});
