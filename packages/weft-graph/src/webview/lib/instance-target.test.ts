import { describe, it, expect } from 'vitest';
import { InstanceTargetMemory } from './instance-target';

describe('the instance target', () => {
	it('starts on the shared ones', () => {
		expect(new InstanceTargetMemory().get('p')).toBeUndefined();
	});

	it('is remembered for the session, per project', () => {
		const memory = new InstanceTargetMemory();
		memory.set('p', ['ada']);
		// A later selection reads the same memory back.
		expect(memory.get('p')).toEqual(['ada']);
		expect(memory.get('other')).toBeUndefined();
		memory.set('p', 'all_instances');
		expect(memory.get('p')).toBe('all_instances');
		memory.set('p', 'all');
		expect(memory.get('p')).toBe('all');
	});

	it('goes back to the shared ones when they are picked again', () => {
		const memory = new InstanceTargetMemory();
		memory.set('p', ['ada']);
		memory.set('p', undefined);
		expect(memory.get('p')).toBeUndefined();
	});
});
