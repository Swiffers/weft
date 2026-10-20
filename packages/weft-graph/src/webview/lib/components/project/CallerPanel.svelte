<script lang="ts">
	/// The back-and-forth between a run and the caller that started it.
	///
	/// Why this sits under the toolbar rather than inside a node's
	/// inspector, where the bus log lives: a bus is a per-node fact (two
	/// nodes are on different buses), while a run has exactly ONE caller
	/// and the same exchange whichever node you open. A copy in every
	/// node's modal would say the same thing N times, each time about
	/// something other than the node in front of you.
	import { ArrowDown, ArrowUp, X } from '@lucide/svelte';
	import type { CallerInspectorEvent } from '../../../../protocol';
	import { formatClockTime } from '../../utils/stream-log';
	import { callerCopyText, callerLines } from '../../utils/caller-log';
	import CopyButton from '../ui/CopyButton.svelte';

	let {
		events,
		onClose,
	}: {
		events: CallerInspectorEvent[];
		onClose: () => void;
	} = $props();

	const lines = $derived(callerLines(events));

	/// The same conversation as plain text, for pasting into a message
	/// to somebody who cannot open this panel.
	const copyText = $derived(callerCopyText(lines));
</script>

<div class="w-[32rem] max-w-[90vw] border border-zinc-200 rounded-md bg-white shadow-lg overflow-hidden">
	<div class="flex items-center justify-between px-2 py-1 border-b border-zinc-200 bg-zinc-50">
		<span class="text-[10px] font-medium text-zinc-500 uppercase tracking-wider">The caller</span>
		<div class="flex items-center gap-1">
			<span class="text-[9px] text-zinc-400">
				{lines.length}
				{lines.length === 1 ? 'line' : 'lines'}
			</span>
			<CopyButton text={copyText} label="Copy the exchange" />
			<button
				type="button"
				onclick={onClose}
				class="flex items-center justify-center w-5 h-5 rounded text-zinc-400 hover:text-zinc-700 hover:bg-zinc-100 transition"
				title="Close"
				aria-label="Close the caller panel"
			>
				<X class="w-3 h-3" />
			</button>
		</div>
	</div>
	<div class="overflow-auto font-mono text-[11px] leading-tight px-2 py-1 max-h-64 min-h-16">
		{#if lines.length === 0}
			<div class="text-zinc-400 italic">(nothing said yet)</div>
		{:else}
			{#each lines as line, idx (idx)}
				<div class="flex items-baseline gap-1.5">
					<span class="text-[9px] text-zinc-300 tabular-nums shrink-0">{formatClockTime(line.atUnix)}</span>
					{#if line.side === 'note'}
						<span class="text-zinc-400 italic">* {line.text}</span>
					{:else if line.side === 'in'}
						<ArrowDown class="w-3 h-3 text-blue-600 shrink-0" />
						<span class="text-zinc-800 break-words whitespace-pre-wrap">{line.text}</span>
					{:else}
						<ArrowUp class="w-3 h-3 shrink-0 {line.dropped ? 'text-zinc-300' : 'text-emerald-600'}" />
						<span class="break-words whitespace-pre-wrap {line.dropped ? 'text-zinc-400 line-through' : 'text-zinc-800'}">{line.text}</span>
						{#if line.dropped}
							<span class="text-[9px] text-amber-600 shrink-0">({line.dropped})</span>
						{/if}
						{#if line.terminal}
							<span class="text-[9px] text-zinc-400 shrink-0">(the answer)</span>
						{/if}
					{/if}
				</div>
			{/each}
		{/if}
	</div>
</div>
