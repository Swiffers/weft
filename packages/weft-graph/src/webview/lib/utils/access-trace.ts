/** The wire an Access input is fed by, followed back through group
 *  ports: an access threaded down through a group arrives on the group's
 *  input port (the inside leg leaves `port__inner`), so the connection is
 *  the one wired into that port outside, however many groups deep.
 *  `null` when nothing feeds the input. */
// SYNC: accessFeedingEdge <-> crates/weft-core/src/instance.rs lookup_connection
export function accessFeedingEdge<E extends { source: string; target: string; sourceHandle?: string | null; targetHandle?: string | null }>(
	edges: readonly E[],
	nodeId: string,
	accessInput: string,
): E | null {
	let edge = edges.find((e) => e.target === nodeId && e.targetHandle === accessInput);
	while (edge?.sourceHandle?.endsWith('__inner')) {
		const groupId = edge.source;
		const port = edge.sourceHandle.slice(0, -'__inner'.length);
		edge = edges.find((e) => e.target === groupId && e.targetHandle === port);
	}
	return edge ?? null;
}
