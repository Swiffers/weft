<script lang="ts">
	/// The error dock: the `_error` output the language gives every
	/// catalog node, which the runtime fills with why the node failed.
	/// It is the gate dock's mirror (`FlowDock`): it sits at the same
	/// height on the right edge, apart from the node's own output rows,
	/// because it answers a different question from them. A circle with
	/// a "!" in it, outline while nothing is wired to it (a failure stops
	/// the run) and filled once a wire leaves it (the failure is handled).
	///
	/// It is a real source handle named `_error`, so a wire is dragged
	/// out of it like out of any output, and an `x._error` edge attaches
	/// to it.
	import { Handle, Position } from '@xyflow/svelte';
	import { ERROR_PORT } from '../../../../protocol';

	let { top, wired }: {
		/// Distance from the top of the node, in pixels.
		top: number;
		/// True when a wire leaves `_error`.
		wired: boolean;
	} = $props();

	/// Tailwind's red-500, the red the graph already uses for errors (a
	/// failed status, a permission shortfall).
	const COLOR = '#ef4444';

	const title = `${ERROR_PORT}: why this node failed. Wire it to handle the failure; unwired, a failure stops the run`;
</script>

<Handle
	type="source"
	position={Position.Right}
	id={ERROR_PORT}
	{title}
	style="top: {top}px; z-index: 6; background: none; border: none; width: 10px; height: 10px;"
>
	<svg
		width="10"
		height="10"
		viewBox="0 0 10 10"
		style="pointer-events: none; position: absolute; left: 0; top: 0;"
	>
		<circle cx="5" cy="5" r="4.25" fill={wired ? COLOR : 'white'} stroke={COLOR} stroke-width="1.5" />
		<line x1="5" y1="2.6" x2="5" y2="5.6" stroke={wired ? 'white' : COLOR} stroke-width="1.4" stroke-linecap="round" />
		<circle cx="5" cy="7.3" r="0.8" fill={wired ? 'white' : COLOR} />
	</svg>
</Handle>
