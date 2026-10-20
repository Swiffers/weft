// What the contextual bar offers for a selection, and what the badges
// on the nodes say. Every state comes from the status answer; these
// check that the bar and the badges read it the same way.

import { describe, it, expect } from 'vitest';
import type { ActionAvailability, InfraPlacementStatus, TriggerEntry, TriggerState } from '../../protocol';
import { emptyActionAvailability } from '../../status';
import { contextActions, describeHeld, infraBadge, runUnavailable, takenDownWith, triggerBadge, type SelectedNode } from './lifecycle';

function status(over: Partial<ActionAvailability> = {}): ActionAvailability {
	return { ...emptyActionAvailability(), ...over };
}

const trigger = (place: string, perInstance = false): SelectedNode => ({ place, isTrigger: true, isInfra: false, perInstance });
const infra = (place: string, perInstance = false): SelectedNode => ({ place, isTrigger: false, isInfra: true, perInstance });
const plain = (place: string): SelectedNode => ({ place, isTrigger: false, isInfra: false, perInstance: false });

const shared = (node: string, state: TriggerState): TriggerEntry => ({
	node,
	perInstance: false,
	shared: { state, mode: state === 'off' ? 'take' : 'active' },
});

const place = (node: string, status: string, over: Partial<InfraPlacementStatus> = {}): InfraPlacementStatus => ({
	node,
	nodeType: 'Postgres',
	status,
	...over,
});

describe('the contextual bar counts', () => {
	it('lists each action once, with how many selected nodes it applies to', () => {
		const s = status({
			triggers: [shared('a', 'on'), shared('b', 'behind'), shared('c', 'off')],
		});
		const actions = contextActions([trigger('a'), trigger('b'), trigger('c'), plain('mid')], s, undefined);
		expect(actions.triggers).toEqual([
			{ verb: 'deactivate', places: ['a', 'b'] },
			{ verb: 'activate', places: ['c'] },
			{ verb: 'resync', places: ['b'] },
		]);
		expect(actions.infra).toEqual([]);
	});

	it('reads a trigger the install never turned on as off', () => {
		const actions = contextActions([trigger('fresh')], status(), undefined);
		expect(actions.triggers).toEqual([{ verb: 'activate', places: ['fresh'] }]);
	});

	it('offers nothing on a trigger whose setup is running', () => {
		const actions = contextActions([trigger('a')], status({ triggers: [shared('a', 'turning_on')] }), undefined);
		expect(actions.triggers).toEqual([]);
	});

	it('groups infra by what each copy takes', () => {
		const s = status({
			infraNodes: [
				place('db', 'running', { behind: true }),
				place('cache', 'stopped'),
				place('queue', 'not_started'),
				place('bridge', 'provisioning'),
			],
		});
		const actions = contextActions([infra('db'), infra('cache'), infra('queue'), infra('bridge')], s, undefined);
		expect(actions.infra).toEqual([
			{ verb: 'infra_start', places: ['cache', 'queue'] },
			{ verb: 'infra_stop', places: ['db'] },
			{ verb: 'infra_terminate', places: ['db', 'cache'] },
			{ verb: 'infra_upgrade', places: ['db'] },
		]);
	});

	it('asks for an instance before acting on a per-instance node', () => {
		const s = status({
			triggers: [{ node: 'inbox', perInstance: true, copies: [{ instance: 'ada', state: 'on', mode: 'active' }] }],
			infraNodes: [place('box', 'per_instance', { instanceCopyCount: 1 })],
		});
		const selection = [trigger('inbox', true), infra('box', true), trigger('door')];
		const onShared = contextActions(selection, s, undefined);
		expect(onShared.needInstance).toEqual(['inbox', 'box']);
		expect(onShared.anyPerInstance).toBe(true);
		expect(onShared.triggers).toEqual([{ verb: 'activate', places: ['door'] }]);
		// One instance picked: the per-instance nodes count, the shared one
		// is not reached.
		const onAda = contextActions(selection, s, ['ada']);
		expect(onAda.needInstance).toEqual([]);
		expect(onAda.sharedLeftOut).toEqual(['door']);
		expect(onAda.triggers).toEqual([{ verb: 'deactivate', places: ['inbox'] }]);
		expect(onAda.infra).toEqual([{ verb: 'infra_start', places: ['box'] }]);
	});

	it('acts on every instance weft holds anything for with all_instances', () => {
		const s = status({
			triggers: [{ node: 'inbox', perInstance: true, copies: [{ instance: 'ada', state: 'on', mode: 'active' }] }],
			instances: [
				{ instance: 'ada', values: 0, connections: 0, tokens: 0, copies: [], triggers: [] },
				{ instance: 'bob', values: 0, connections: 0, tokens: 0, copies: [], triggers: [] },
			],
		});
		const actions = contextActions([trigger('inbox', true), trigger('door')], s, 'all_instances');
		// Ada's is on, Bob's is off: both actions apply to the one node.
		expect(actions.triggers).toEqual([
			{ verb: 'deactivate', places: ['inbox'] },
			{ verb: 'activate', places: ['inbox'] },
		]);
		// The shared trigger is not reached.
		expect(actions.sharedLeftOut).toEqual(['door']);
	});

	it('acts on the shared ones and every instance with all', () => {
		const s = status({
			triggers: [
				{ node: 'inbox', perInstance: true, copies: [{ instance: 'ada', state: 'on', mode: 'active' }] },
				shared('door', 'behind'),
			],
			infraNodes: [place('box', 'per_instance', { instanceCopyCount: 0 })],
			instances: [{ instance: 'ada', values: 0, connections: 0, tokens: 0, copies: [], triggers: [] }],
		});
		const actions = contextActions([trigger('inbox', true), trigger('door'), infra('box', true)], s, 'all');
		expect(actions.needInstance).toEqual([]);
		expect(actions.sharedLeftOut).toEqual([]);
		// Ada's inbox is on, the shared door is behind.
		expect(actions.triggers).toEqual([
			{ verb: 'deactivate', places: ['inbox', 'door'] },
			{ verb: 'resync', places: ['door'] },
		]);
		expect(actions.infra).toEqual([{ verb: 'infra_start', places: ['box'] }]);
	});

	it('names what taking infra down takes down with it', () => {
		const s = status({
			infraNodes: [
				place('db', 'running', {
					takesDown: [
						{ owner: { kind: 'shared' }, triggers: ['door', 'feed'] },
						{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
					],
				}),
				place('cache', 'running', {
					takesDown: [
						{ owner: { kind: 'shared' }, triggers: ['feed', 'clock'] },
						{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
					],
				}),
			],
		});
		// Each owner once, each of its triggers once.
		expect(takenDownWith(['db', 'cache'], s)).toEqual([
			{ owner: { kind: 'shared' }, triggers: ['door', 'feed', 'clock'] },
			{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
		]);
	});
});

describe('the badges', () => {
	it('say what a shared trigger is doing', () => {
		expect(triggerBadge(shared('a', 'on')).label).toBe('on');
		expect(triggerBadge(shared('a', 'behind')).label).toBe('behind');
		expect(triggerBadge(shared('a', 'turning_on')).label).toBe('turning on...');
		expect(triggerBadge(shared('a', 'off')).label).toBe('off');
		const holding = triggerBadge({
			node: 'a',
			perInstance: false,
			shared: { state: 'off', mode: 'take', held: { arrivals: 2, paused: 1 } },
		});
		expect(holding.label).toBe('held 3');
		expect(holding.detail).toContain('2 arrivals and 1 paused run');
		const heldBack = triggerBadge({
			node: 'a',
			perInstance: false,
			shared: { state: 'off', mode: 'take', downWith: 'health', heldBack: 'other triggers are on another build. `weft resync --all` moves them' },
		});
		expect(heldBack.label).toBe('held back');
		expect(heldBack.detail).toContain('Held back: other triggers are on another build. `weft resync --all` moves them');
	});

	it('count a per-instance trigger\'s instances that are on', () => {
		const badge = triggerBadge({
			node: 'inbox',
			perInstance: true,
			copies: [
				{ instance: 'ada', state: 'on', mode: 'active' },
				{ instance: 'bob', state: 'behind', mode: 'active' },
				{ instance: 'cy', state: 'off', mode: 'take' },
			],
		});
		expect(badge.label).toBe('2 of 3 on');
		expect(badge.detail).toBe('ada: on\nbob: behind\ncy: off');
	});

	it('mark infra behind and starting', () => {
		expect(infraBadge(place('db', 'running', { behind: true }), [], 0).label).toBe('behind');
		expect(infraBadge(place('db', 'provisioning'), [], 0).label).toBe('starting...');
		expect(infraBadge(place('db', 'not_started'), [], 0).label).toBe('stopped');
		expect(
			infraBadge(
				place('db', 'running', {
					takesDown: [
						{ owner: { kind: 'shared' }, triggers: ['door'] },
						{ owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
					],
				}),
				[],
				0,
			).detail,
		).toBe("Taking it down takes down: 'door'; 'inbox' of instance 'ada'.");
	});

	it('count a per-instance infra node\'s copies that run', () => {
		const badge = infraBadge(place('box', 'per_instance'), [
			{ node: 'box', instance: 'ada', status: 'running' },
			{ node: 'box', instance: 'bob', status: 'stopped' },
			{ node: 'other', instance: 'ada', status: 'running' },
		], 0);
		expect(badge.label).toBe('1 of 2 running');
		expect(badge.detail).toBe('ada: running\nbob: stopped');
	});

	it('refuse an infra status they cannot read', () => {
		expect(() => infraBadge(place('db', 'mystery'), [], 0)).toThrow(/cannot read/);
	});

	it('word held work the way weft does', () => {
		expect(describeHeld({ arrivals: 1, paused: 0 })).toBe('1 arrival');
		expect(describeHeld({ arrivals: 0, paused: 2 })).toBe('2 paused runs');
	});
});

describe('the contextual bar\'s Run', () => {
	const open = { blocked: false, offered: true, runBlocked: null };

	it('is disabled with the walk\'s refusal as its text', () => {
		const refusal = "needs `door`, which only fires from outside; use the CLI with --fire or --from";
		expect(runUnavailable({ ...open, refusal })).toBe(refusal);
	});

	it('goes once the walk resolves', () => {
		expect(runUnavailable({ ...open, refusal: null })).toBeUndefined();
	});

	it('waits for the walk\'s answer', () => {
		expect(runUnavailable({ ...open, refusal: undefined })).toMatch(/Checking/);
	});

	it('says what stops it before asking the walk', () => {
		expect(runUnavailable({ ...open, runBlocked: 'Start db first: this run reaches it.', refusal: null })).toBe('Start db first: this run reaches it.');
		expect(runUnavailable({ ...open, blocked: true, refusal: null })).toMatch(/under way/);
	});
});
