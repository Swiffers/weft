import { describe, it, expect } from 'vitest';
import {
	isOnScreen,
	springEase,
	zoomAfterCollapse,
	zoomAfterExpand,
	zoomAround,
	zoomFrame,
	MAX_ZOOM,
	EXPAND_NODES_PER_SCREEN,
	FEW_NODES,
} from './group-zoom';

const screen = { width: 1600, height: 900 };
const node = (x: number, y: number) => ({ x, y, width: 250, height: 150 });
/// A column-major grid of `count` nodes starting at (x, y), 300 apart.
const grid = (count: number, x = 0, y = 0) =>
	Array.from({ length: count }, (_, i) => node(x + Math.floor(i / 5) * 300, y + (i % 5) * 200));
/// The flow point under a screen point.
const flowAt = (vp: { x: number; y: number; zoom: number }, p: { x: number; y: number }) =>
	({ x: (p.x - vp.x) / vp.zoom, y: (p.y - vp.y) / vp.zoom });
/// Where a flow rect lands on screen.
const onScreenRect = (vp: { x: number; y: number; zoom: number }, r: { x: number; y: number; width: number; height: number }) =>
	({ left: r.x * vp.zoom + vp.x, top: r.y * vp.zoom + vp.y, right: (r.x + r.width) * vp.zoom + vp.x, bottom: (r.y + r.height) * vp.zoom + vp.y });

describe('zoomAround', () => {
	it('keeps the flow point under the anchor where it is', () => {
		const vp = { x: -120, y: 40, zoom: 1.3 };
		const mouse = { x: 700, y: 300 };
		const next = zoomAround(vp, mouse, 0.6);
		expect(next.zoom).toBe(0.6);
		const before = flowAt(vp, mouse);
		const after = flowAt(next, mouse);
		expect(after.x).toBeCloseTo(before.x, 9);
		expect(after.y).toBeCloseTo(before.y, 9);
	});
});

describe('zoomAfterExpand', () => {
	// Twelve nodes laid out in a 1600x1140 box: 152000 flow units each.
	const group = { x: 0, y: 0, width: 1600, height: 1140 };
	const opened = Array.from({ length: 12 }, (_, i) => node((i % 4) * 400, Math.floor(i / 4) * 380));
	/// How many nodes, at that crowding, the screen holds at `zoom`.
	const nodesPerScreen = (zoom: number) => (screen.width * screen.height) / (zoom * zoom) / ((1600 * 1140) / 12);
	const mouse = { x: 800, y: 450 };

	it('zooms out to about a dozen nodes per screen at the group\'s own crowding', () => {
		const next = zoomAfterExpand({ x: 0, y: 0, zoom: 2 }, screen, group, opened, mouse);
		expect(next.zoom).toBeLessThan(2);
		expect(nodesPerScreen(next.zoom)).toBeCloseTo(EXPAND_NODES_PER_SCREEN, 9);
	});

	it('leaves a small group alone when the view is already out that far', () => {
		// Three nodes in an 825x361 box: a dozen per screen is about 1.1.
		const small = { x: 0, y: 0, width: 825, height: 361 };
		const vp = { x: 0, y: 0, zoom: 1 };
		expect(zoomAfterExpand(vp, screen, small, grid(3), mouse)).toEqual(vp);
	});

	it('counts room for the wires even when the nodes sit back to back', () => {
		// Two 250x150 nodes packed into a 500x150 box: taken as twice their
		// own area, so as spaced as they would be with wires between them.
		const packed = { x: 0, y: 0, width: 500, height: 150 };
		const next = zoomAfterExpand({ x: 0, y: 0, zoom: 2 }, screen, packed, [node(0, 0), node(250, 0)], mouse);
		expect(next.zoom).toBeCloseTo(Math.sqrt((1600 * 900 * 2) / (EXPAND_NODES_PER_SCREEN * 2 * 2 * 250 * 150)), 9);
	});

	it('takes the same zoom wherever the mouse is, so an edge never sends the view far out', () => {
		const vp = { x: 0, y: 0, zoom: 2 };
		const centered = zoomAfterExpand(vp, screen, group, opened, mouse);
		const onEdge = zoomAfterExpand(vp, screen, group, opened, { x: 800, y: 900 });
		expect(onEdge.zoom).toBeCloseTo(centered.zoom, 9);
	});

	it('never fits a big group whole', () => {
		const vp = { x: 0, y: 0, zoom: 2 };
		const big = { x: 0, y: 0, width: 4000, height: 2280 };
		const many = Array.from({ length: 60 }, (_, i) => node((i % 10) * 400, Math.floor(i / 10) * 380));
		const next = zoomAfterExpand(vp, screen, big, many, mouse);
		expect(next.zoom).toBeGreaterThan(Math.min(screen.width / big.width, screen.height / big.height) * 2);
	});

	it('never zooms in', () => {
		const vp = { x: 0, y: 0, zoom: 0.5 };
		expect(zoomAfterExpand(vp, screen, group, opened, mouse)).toEqual(vp);
	});

	it('zooms around the mouse', () => {
		const vp = { x: -300, y: -100, zoom: 2 };
		const next = zoomAfterExpand(vp, screen, group, opened, mouse);
		expect(next.zoom).toBeLessThan(vp.zoom);
		const before = flowAt(vp, mouse);
		const after = flowAt(next, mouse);
		expect(after.x).toBeCloseTo(before.x, 9);
		expect(after.y).toBeCloseTo(before.y, 9);
	});

	it('stays when the mouse is off the canvas', () => {
		const vp = { x: 0, y: 0, zoom: 2 };
		expect(zoomAfterExpand(vp, screen, group, opened, { x: -10, y: 200 })).toEqual(vp);
		expect(zoomAfterExpand(vp, screen, group, opened, { x: 800, y: 901 })).toEqual(vp);
	});

	it('leaves the view alone when nothing opened inside', () => {
		const vp = { x: 0, y: 0, zoom: 2 };
		expect(zoomAfterExpand(vp, screen, group, [], mouse)).toEqual(vp);
	});
});

describe('zoomAfterCollapse', () => {
	const mouse = { x: 400, y: 450 };

	it('zooms in when a few nodes float far out', () => {
		const vp = { x: 0, y: 0, zoom: 0.3 };
		const next = zoomAfterCollapse(vp, screen, grid(3, 200, 200), mouse);
		expect(next.zoom).toBeGreaterThan(vp.zoom);
	});

	it('zooms in around the mouse', () => {
		const vp = { x: 0, y: 0, zoom: 0.3 };
		const next = zoomAfterCollapse(vp, screen, grid(3, 200, 200), mouse);
		const before = flowAt(vp, mouse);
		const after = flowAt(next, mouse);
		expect(after.x).toBeCloseTo(before.x, 9);
		expect(after.y).toBeCloseTo(before.y, 9);
	});

	it('never zooms past the point where all of them stay on screen', () => {
		const vp = { x: 100, y: 300, zoom: 0.1 };
		// Two nodes far apart: reading them at two per screen would push one off.
		const onScreen = [node(0, 0), node(4000, 0)];
		const next = zoomAfterCollapse(vp, screen, onScreen, { x: 300, y: 307 });
		expect(next.zoom).toBeCloseTo(0.15, 9);
		expect(onScreenRect(next, onScreen[0]).left).toBeCloseTo(0, 9);
	});

	it('stays when more than a few nodes are on screen', () => {
		const vp = { x: 0, y: 0, zoom: 0.3 };
		expect(zoomAfterCollapse(vp, screen, grid(FEW_NODES + 1), mouse)).toEqual(vp);
	});

	it('stays when the few nodes already read', () => {
		const small = { width: 800, height: 500 };
		const vp = { x: 0, y: 0, zoom: 1.5 };
		expect(zoomAfterCollapse(vp, small, grid(2, 100, 100), mouse)).toEqual(vp);
	});

	it('stays when the mouse is off the canvas', () => {
		const vp = { x: 0, y: 0, zoom: 0.3 };
		expect(zoomAfterCollapse(vp, screen, grid(3, 200, 200), { x: 1601, y: 450 })).toEqual(vp);
	});

	it('never zooms past the canvas limit', () => {
		const vp = { x: 0, y: 0, zoom: 0.05 };
		// One tiny node right under the mouse: nothing else limits the zoom.
		const next = zoomAfterCollapse(vp, screen, [{ x: 7995, y: 8995, width: 10, height: 10 }], mouse);
		expect(next.zoom).toBe(MAX_ZOOM);
	});
});

describe('isOnScreen', () => {
	const vp = { x: 100, y: 50, zoom: 0.5 };

	it('counts any overlap', () => {
		expect(isOnScreen(node(0, 0), vp, screen)).toBe(true);
		// Its right edge pokes onto the left of the screen.
		expect(isOnScreen(node(-440, 0), vp, screen)).toBe(true);
	});

	it('leaves out what is fully off', () => {
		expect(isOnScreen(node(-500, 0), vp, screen)).toBe(false);
		expect(isOnScreen(node(3100, 0), vp, screen)).toBe(false);
		expect(isOnScreen(node(0, 1700), vp, screen)).toBe(false);
	});
});

describe('the spring', () => {
	it('starts at 0 and lands on 1', () => {
		expect(springEase(0)).toBeCloseTo(0, 9);
		expect(springEase(1)).toBeCloseTo(1, 9);
	});

	it('overshoots slightly on the way', () => {
		const peak = Math.max(...Array.from({ length: 101 }, (_, i) => springEase(i / 100)));
		expect(peak).toBeGreaterThan(1.02);
		expect(peak).toBeLessThan(1.1);
	});

	it('frames run from the start view to the target zoom, the mouse point fixed throughout', () => {
		const from = { x: -200, y: 30, zoom: 1.8 };
		const mouse = { x: 640, y: 380 };
		const first = zoomFrame(from, mouse, 0.7, 0);
		expect(first.x).toBeCloseTo(from.x, 9);
		expect(first.y).toBeCloseTo(from.y, 9);
		expect(first.zoom).toBeCloseTo(from.zoom, 9);
		expect(zoomFrame(from, mouse, 0.7, 1).zoom).toBeCloseTo(0.7, 9);
		const anchor = flowAt(from, mouse);
		for (const t of [0.2, 0.5, 0.8]) {
			const p = flowAt(zoomFrame(from, mouse, 0.7, t), mouse);
			expect(p.x).toBeCloseTo(anchor.x, 9);
			expect(p.y).toBeCloseTo(anchor.y, 9);
		}
	});
});
