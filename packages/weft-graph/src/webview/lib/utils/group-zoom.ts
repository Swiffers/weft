// Where the camera goes when a group opens or closes.
//
// Opening a group while zoomed in fills the screen with its box and shows
// almost none of what is inside, so the view zooms out, around the mouse,
// to a rough estimate: the zoom at which the screen would hold about
// `EXPAND_NODES_PER_SCREEN` nodes if the group's nodes, spread as they are
// in its box, covered the whole screen. It reads how crowded the box is,
// never where the mouse is, so the zoom stays moderate near an edge, and a
// big group is never shrunk to fit whole. Closing a group can leave
// `FEW_NODES` or fewer floating in an empty screen, so the view zooms back
// in, around the mouse, by the same estimate toward
// `COLLAPSE_NODES_PER_SCREEN`. With more than that on screen the person
// chose that view, and it stays. A mouse off the canvas has nothing to
// center on, so the view stays.
//
// Everything here is plain numbers: viewports are xyflow's `{x, y, zoom}`
// (screen = flow * zoom + offset), rects are in flow coordinates, points
// and sizes are in screen pixels relative to the canvas.

import type { Viewport, Rect, XYPosition, Dimensions } from '@xyflow/svelte';

/// The canvas's zoom limits, for every zoom the editor sets.
export const MIN_ZOOM = 0.05;
export const MAX_ZOOM = 2;

/// About how many nodes a screen holds after a group opens.
export const EXPAND_NODES_PER_SCREEN = 12;
/// About how many nodes a screen holds after a group closes, when it zooms in.
export const COLLAPSE_NODES_PER_SCREEN = 2;
/// At most this many nodes left on screen after a close makes it zoom in.
export const FEW_NODES = 5;

/// How long a group toggle's zoom takes, in milliseconds.
export const ZOOM_ANIMATION_MS = 450;

export function clampZoom(zoom: number): number {
	return Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, zoom));
}

/// The viewport at `zoom` that keeps the flow point under `anchor` (a
/// screen point) exactly where it is: the mouse-wheel zoom.
export function zoomAround(viewport: Viewport, anchor: XYPosition, zoom: number): Viewport {
	const ratio = zoom / viewport.zoom;
	return {
		x: anchor.x - (anchor.x - viewport.x) * ratio,
		y: anchor.y - (anchor.y - viewport.y) * ratio,
		zoom,
	};
}

/// Nodes never sit back to back: wires run between them, so a node takes
/// at least about this many times its own box.
const ROOM_PER_NODE = 2;

/// The zoom at which `screen` would hold about `perScreen` nodes, if
/// `nodes`, spread as they are over `box` (the room they sit in), were
/// spread the same way over the whole screen. The room is never taken as
/// less than `ROOM_PER_NODE` times the nodes' own boxes, so one node, or a
/// few packed tight, still counts their wires.
function zoomForNodesPerScreen(perScreen: number, nodes: Rect[], box: Rect, screen: Dimensions): number {
	const own = nodes.reduce((sum, n) => sum + n.width * n.height, 0);
	const room = Math.max(box.width * box.height, ROOM_PER_NODE * own);
	return Math.sqrt((screen.width * screen.height * nodes.length) / (perScreen * room));
}

/// The largest zoom around `anchor` at which all of `box` is on screen.
/// The anchor stays put, so each side of the box can only use the room
/// between the anchor and that side of the screen: an anchor on an edge
/// with the box beyond it gives 0. Infinity when nothing limits it (a box
/// with no size on the anchor). `anchor` must be on the screen.
function fitAroundZoom(viewport: Viewport, screen: Dimensions, anchor: XYPosition, box: Rect): number {
	const a = { x: (anchor.x - viewport.x) / viewport.zoom, y: (anchor.y - viewport.y) / viewport.zoom };
	const limits: number[] = [];
	if (box.x < a.x) limits.push(anchor.x / (a.x - box.x));
	if (box.x + box.width > a.x) limits.push((screen.width - anchor.x) / (box.x + box.width - a.x));
	if (box.y < a.y) limits.push(anchor.y / (a.y - box.y));
	if (box.y + box.height > a.y) limits.push((screen.height - anchor.y) / (box.y + box.height - a.y));
	return Math.min(Infinity, ...limits);
}

function isInside(point: XYPosition, screen: Dimensions): boolean {
	return point.x >= 0 && point.y >= 0 && point.x <= screen.width && point.y <= screen.height;
}

function boundsOf(rects: Rect[]): Rect {
	const minX = Math.min(...rects.map((r) => r.x));
	const minY = Math.min(...rects.map((r) => r.y));
	const maxX = Math.max(...rects.map((r) => r.x + r.width));
	const maxY = Math.max(...rects.map((r) => r.y + r.height));
	return { x: minX, y: minY, width: maxX - minX, height: maxY - minY };
}

/// Where the view goes once a group has opened and the layout has made room
/// for it. `group` is its box, `opened` the nodes now drawn inside it
/// (expanded groups inside it left out: they are boxes, not things to
/// read).
///
/// The view only ever zooms out here, around `mouse`, to about
/// `EXPAND_NODES_PER_SCREEN` of the opened nodes per screen at the group's
/// own crowding (`zoomForNodesPerScreen` over the box). The mouse's place does not
/// count, so an edge never sends the view far out. A view already at or
/// below that zoom stays where it is.
export function zoomAfterExpand(
	viewport: Viewport,
	screen: Dimensions,
	group: Rect,
	opened: Rect[],
	mouse: XYPosition,
): Viewport {
	if (opened.length === 0 || !isInside(mouse, screen)) return viewport;
	const enough = zoomForNodesPerScreen(EXPAND_NODES_PER_SCREEN, opened, group, screen);
	const zoom = clampZoom(Math.min(viewport.zoom, enough));
	return zoom < viewport.zoom ? zoomAround(viewport, mouse, zoom) : viewport;
}

/// Where the view goes once a group has closed. `onScreen` is every node
/// the screen shows (expanded groups left out, the closed group in).
///
/// When at most `FEW_NODES` are on screen, the view zooms in around `mouse`
/// toward about `COLLAPSE_NODES_PER_SCREEN` of them per screen at the
/// crowding of the box they sit in (`zoomForNodesPerScreen`), never past
/// the zoom where all of them stay on screen. Otherwise it stays.
export function zoomAfterCollapse(
	viewport: Viewport,
	screen: Dimensions,
	onScreen: Rect[],
	mouse: XYPosition,
): Viewport {
	if (onScreen.length === 0 || onScreen.length > FEW_NODES || !isInside(mouse, screen)) return viewport;
	const box = boundsOf(onScreen);
	const enough = Math.min(
		zoomForNodesPerScreen(COLLAPSE_NODES_PER_SCREEN, onScreen, box, screen),
		fitAroundZoom(viewport, screen, mouse, box),
	);
	const zoom = clampZoom(Math.max(viewport.zoom, enough));
	return zoom > viewport.zoom ? zoomAround(viewport, mouse, zoom) : viewport;
}

/// Whether a flow rect shows on a screen of `screen` size under `viewport`
/// (any overlap counts).
export function isOnScreen(rect: Rect, viewport: Viewport, screen: Dimensions): boolean {
	const left = rect.x * viewport.zoom + viewport.x;
	const top = rect.y * viewport.zoom + viewport.y;
	const right = left + rect.width * viewport.zoom;
	const bottom = top + rect.height * viewport.zoom;
	return right > 0 && bottom > 0 && left < screen.width && top < screen.height;
}

/// Ease out with a slight overshoot: rises fast, passes 1 by about five
/// percent, settles back on 1 at `t = 1`. Fed to the zoom, it lands with a
/// small spring.
export function springEase(t: number): number {
	const overshoot = 1.2;
	const u = t - 1;
	return 1 + (overshoot + 1) * u * u * u + overshoot * u * u;
}

/// The viewport `t` of the way (0 to 1, before easing) from `from` to
/// `toZoom`, zooming around `anchor`. The zoom moves on a log scale, so each
/// frame scales the view by the same factor, and every frame keeps the flow
/// point under `anchor` in place, so the mouse stays on what it pointed at.
export function zoomFrame(from: Viewport, anchor: XYPosition, toZoom: number, t: number): Viewport {
	const zoom = from.zoom * Math.pow(toZoom / from.zoom, springEase(t));
	return zoomAround(from, anchor, clampZoom(zoom));
}
