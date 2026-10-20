<script lang="ts">
	// The three questions every command that takes triggers down answers,
	// prefilled with the defaults they all share: what happens to work
	// that arrives (the door), to work already waiting (held), and to the
	// runs going right now. An answer left alone is sent as no answer, so
	// the default applies; the CLI prints what it did.
	import type { Door, Held, OwnerTriggers, Running, TakeDownChoice } from '../../../../protocol';
	import { describeOwnerTriggers, TAKE_DOWN_DEFAULT } from '../../../../protocol';

	let {
		title,
		choice = $bindable(),
		takesDown = [],
	}: {
		/// What these answers are for ("Turning triggers off or resyncing").
		title: string;
		choice: TakeDownChoice;
		/// For infra: the triggers whose cut contains it, which go down
		/// with it, by owner.
		takesDown?: OwnerTriggers[];
	} = $props();

	const door = $derived(choice.door ?? TAKE_DOWN_DEFAULT.door);
	const held = $derived(choice.held ?? TAKE_DOWN_DEFAULT.held);
	const running = $derived(choice.running ?? TAKE_DOWN_DEFAULT.running);

	const DOORS: Array<{ value: Door; label: string; detail: string }> = [
		{ value: 'take', label: 'Hold it', detail: 'What arrives waits for the trigger to be back. A question stays listed for the people who answer it.' },
		{ value: 'hide', label: 'Hold it, hidden', detail: 'What arrives waits, out of sight of the people who answer questions.' },
		{ value: 'refuse', label: 'Refuse it', detail: 'Nothing is taken while the trigger is down.' },
	];
	const HELD: Array<{ value: Held; label: string; detail: string }> = [
		{ value: 'keep', label: 'Keep it', detail: 'What already waits stays, for when the trigger is back.' },
		{ value: 'drop', label: 'Drop it', detail: 'Held arrivals are deleted and the paused runs cancelled.' },
	];
	// A Record, so a Running answer added on the Rust side fails the
	// type check here until it has a label.
	const RUNNING: Record<Running, { label: string; detail: string }> = {
		wait: {
			label: 'Wait for them',
			detail: 'They finish on the version they started on, and the command waits for them before it goes on, however long that takes.',
		},
		cancel: { label: 'Cancel them', detail: 'They stop now.' },
	};
	const RUNNING_ANSWERS = Object.keys(RUNNING) as Running[];

	function setDoor(value: Door) {
		// A refusing door takes nothing already, so a time limit on it
		// means nothing (weft refuses the pair).
		choice = { ...choice, door: value, ...(value === 'refuse' ? { doorForMinutes: undefined } : {}) };
	}
	function setDoorFor(raw: string) {
		const minutes = Number(raw);
		choice = { ...choice, doorForMinutes: raw.trim() === '' || !Number.isInteger(minutes) || minutes < 1 ? undefined : minutes };
	}
</script>

<div class="space-y-2">
	<div class="text-[11px] font-semibold uppercase tracking-wide text-zinc-500">{title}</div>
	{#if takesDown.length > 0}
		<div class="text-[11px] text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1">
			Goes down with it: {describeOwnerTriggers(takesDown)}
		</div>
	{/if}
	<fieldset class="space-y-1">
		<legend class="text-[11px] text-zinc-600">What arrives while it is down</legend>
		{#each DOORS as option (option.value)}
			<label class="flex items-start gap-2 text-xs cursor-pointer" title={option.detail}>
				<input type="radio" checked={door === option.value} onchange={() => setDoor(option.value)} />
				<span>{option.label}</span>
			</label>
		{/each}
		{#if door !== 'refuse'}
			<label class="flex items-center gap-2 text-xs text-zinc-600">
				<span>then refuse after</span>
				<input
					type="number"
					min="1"
					step="1"
					class="w-16 px-1 py-0.5 border rounded"
					placeholder="never"
					value={choice.doorForMinutes ?? ''}
					oninput={(e) => setDoorFor((e.currentTarget as HTMLInputElement).value)}
				/>
				<span>minutes</span>
			</label>
		{/if}
	</fieldset>
	<fieldset class="space-y-1">
		<legend class="text-[11px] text-zinc-600">What already waits</legend>
		{#each HELD as option (option.value)}
			<label class="flex items-start gap-2 text-xs cursor-pointer" title={option.detail}>
				<input type="radio" checked={held === option.value} onchange={() => (choice = { ...choice, held: option.value })} />
				<span>{option.label}</span>
			</label>
		{/each}
	</fieldset>
	<fieldset class="space-y-1">
		<legend class="text-[11px] text-zinc-600">The runs going now</legend>
		{#each RUNNING_ANSWERS as value (value)}
			<label class="flex items-start gap-2 text-xs cursor-pointer" title={RUNNING[value].detail}>
				<input type="radio" checked={running === value} onchange={() => (choice = { ...choice, running: value })} />
				<span>{RUNNING[value].label}</span>
			</label>
		{/each}
	</fieldset>
</div>
