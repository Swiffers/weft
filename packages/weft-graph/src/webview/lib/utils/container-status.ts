import type { NodeFiringStatus } from '../types';

/// The status a group, include or loop card shows for one launch of it,
/// read off the launch's own state (`inStatus`: a loop's In boundary
/// firing, a group's or include's scope decision) and every member run
/// at play.
///
/// A skipped launch is the whole story: the container did not run (its
/// gate said no, nothing reached it, or a loop had nothing to iterate),
/// and its members carry the same skip, so the card says skipped rather
/// than "completed" for a set of rows that are all terminal.
///
/// A member parked on a human (or any suspension) holds the container
/// with it: nothing inside is progressing, so the card says waiting,
/// in the same cyan the parked node wears, rather than running. Only
/// when some other member is actually running does running win.
export function containerStatus(
	inStatus: NodeFiringStatus,
	related: ReadonlyArray<{ status: NodeFiringStatus }>,
): NodeFiringStatus {
	if (related.some((e) => e.status === 'running')) return 'running';
	if (related.some((e) => e.status === 'waiting_for_input')) return 'waiting_for_input';
	if (related.some((e) => e.status === 'failed')) return 'failed';
	if (inStatus === 'skipped') return 'skipped';
	const allTerminal =
		related.length > 0 &&
		related.every(
			(e) =>
				e.status === 'completed' ||
				e.status === 'skipped' ||
				e.status === 'failed' ||
				e.status === 'cancelled',
		);
	return allTerminal ? 'completed' : inStatus;
}
