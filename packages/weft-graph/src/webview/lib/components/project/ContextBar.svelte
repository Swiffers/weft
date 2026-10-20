<script lang="ts">
	// The contextual bar: what the editor can do with the selected nodes,
	// drawn inside the bottom bar. Run runs the selected nodes that are
	// neither triggers nor infra, as targets. Then each lifecycle action
	// once, triggers and infra apart, with how many selected nodes it
	// applies to when that is more than one. The options answer how triggers go down, prefilled
	// with each command's defaults, and the instance picker says whose
	// copies are acted on. Acting on everything is selecting everything
	// (Ctrl+A).
	import { Play, ChevronDown, ChevronUp, Search, Zap, Square, Database, RefreshCw, Trash2 } from '@lucide/svelte';
	import type {
		ActionAvailability,
		InstanceTarget,
		LifecycleAction,
		ReleaseHeld,
		TakeDownChoice,
	} from '../../../../protocol';
	import { INSTANCE_ID_PATTERN } from '../../../../run-spec';
	import { actionUnavailable, contextActions, optionsFor, takenDownWith, type InfraVerb, type SelectedNode, type TriggerVerb } from '../../lifecycle';
	import { sessionInstanceTargets } from '../../instance-target';
	import TakeDownOptions from './TakeDownOptions.svelte';

	let {
		selected,
		runTargets,
		status,
		project,
		blocked,
		offered,
		runWhy,
		onRun,
		onLifecycle,
	}: {
		/// Every selected node, by its place.
		selected: SelectedNode[];
		/// The places Run runs (`runTargetsOf`): no Run when empty.
		runTargets: string[];
		/// The latest status answer (the empty one before any arrived).
		status: ActionAvailability;
		/// Whose instances the picker remembers its pick for.
		project: string;
		/// Something is in flight (a verb, a build): no action now.
		blocked: boolean;
		/// Whether weft takes this verb right now (`isVerbOffered`).
		offered: (verb: TriggerVerb | InfraVerb) => boolean;
		/// Why Run cannot go, or `undefined` when it may: the editor's
		/// one rule, which every run gesture reads too.
		runWhy: string | undefined;
		/// Runs the selection, through the editor's one run funnel.
		onRun: () => void;
		onLifecycle: (action: LifecycleAction) => void;
	} = $props();

	// ─── Whose copies ─────────────────────────────────────────────
	/// The pick made while this bar is up; before one, the session's.
	let picked = $state<{ project: string; target: InstanceTarget | undefined } | undefined>(undefined);
	const target = $derived(picked?.project === project ? picked.target : sessionInstanceTargets.get(project));
	function pick(next: InstanceTarget | undefined) {
		picked = { project, target: next };
		sessionInstanceTargets.set(project, next);
		pickerOpen = false;
		search = '';
	}
	let pickerOpen = $state(false);
	let search = $state('');
	const known = $derived(status.instances.map((i) => i.instance));
	const matching = $derived(known.filter((id) => id.toLowerCase().includes(search.trim().toLowerCase())));
	/// An id typed that weft holds nothing for yet: an instance turned on
	/// for the first time is still picked by its id.
	const typedNew = $derived(
		search.trim() !== '' && INSTANCE_ID_PATTERN.test(search.trim()) && !known.includes(search.trim()) ? search.trim() : null,
	);
	const targetLabel = $derived(
		target === undefined
			? 'shared'
			: target === 'all_instances'
				? 'all instances'
				: target === 'all'
					? 'shared and all instances'
					: target.join(', '),
	);

	// ─── What applies ─────────────────────────────────────────────
	const actions = $derived(contextActions(selected, status, target));
	const showPicker = $derived(actions.anyPerInstance || target !== undefined);

	// ─── Options ──────────────────────────────────────────────────
	let optionsOpen = $state(false);
	let triggerChoice = $state<TakeDownChoice>({});
	let infraChoice = $state<TakeDownChoice>({});
	let releaseHeld = $state<ReleaseHeld | undefined>(undefined);
	const options = $derived(optionsFor(actions));
	const infraTakesDown = $derived(takenDownWith([...new Set(options.infraGoingDown.flatMap((a) => a.places))], status));

	const TRIGGER_LABELS: Record<TriggerVerb, string> = { deactivate: 'Turn off', activate: 'Turn on', resync: 'Resync' };
	const INFRA_LABELS: Record<InfraVerb, string> = {
		infra_start: 'Start',
		infra_stop: 'Stop',
		infra_terminate: 'Terminate',
		infra_upgrade: 'Upgrade',
	};

	function actOnTriggers(verb: TriggerVerb, places: string[]) {
		onLifecycle(
			verb === 'activate'
				? { verb, triggers: places, instances: target, ...(releaseHeld ? { held: releaseHeld } : {}) }
				: { verb, triggers: places, instances: target, takeDown: triggerChoice },
		);
	}
	function actOnInfra(verb: InfraVerb, places: string[]) {
		onLifecycle(
			verb === 'infra_start'
				? { verb, infra: places, instances: target }
				: { verb, infra: places, instances: target, takeDown: infraChoice },
		);
	}
	const why = (verb: TriggerVerb | InfraVerb) => actionUnavailable({ blocked, offered: offered(verb) });

	// The bottom bar's look: big buttons, uppercase labels.
	const btn = 'flex items-center gap-2 px-3 py-1.5 rounded-lg transition-colors';
	const btnDisabled = 'disabled:opacity-50 disabled:cursor-not-allowed';
	const labelCss = 'font-medium text-[11px] uppercase tracking-wider';
	const counted = (places: string[]) => (places.length > 1 ? ` (${places.length})` : '');
	const INFRA_CSS: Record<InfraVerb, string> = {
		infra_start: 'bg-blue-50 text-blue-600 border border-blue-200 hover:bg-blue-100',
		infra_stop: 'bg-zinc-100 text-zinc-700 border border-zinc-200 hover:bg-zinc-200',
		infra_terminate: 'bg-red-50 text-red-600 border border-red-200 hover:bg-red-100',
		infra_upgrade: 'bg-amber-50 text-amber-700 border border-amber-300 hover:bg-amber-100',
	};
	const TRIGGER_CSS: Record<TriggerVerb, string> = {
		activate: 'bg-zinc-900 text-white hover:bg-zinc-800',
		deactivate: 'bg-emerald-600 text-white hover:bg-emerald-700',
		resync: 'bg-amber-50 text-amber-700 border border-amber-300 hover:bg-amber-100',
	};
	const divider = 'w-px h-6 bg-zinc-200 mx-1';
</script>

<div class="relative flex flex-wrap items-center justify-center gap-1.5">
	{#if optionsOpen && options.any}
		<div class="absolute bottom-full mb-3 left-1/2 -translate-x-1/2 w-[520px] max-w-[90vw] bg-white border border-zinc-200 rounded-xl shadow-xl p-3 grid grid-cols-2 gap-4 text-zinc-800">
			{#if options.takesTriggersDown}
				<TakeDownOptions title="Turning triggers off or resyncing" bind:choice={triggerChoice} />
			{/if}
			{#if options.infraGoingDown.length > 0}
				<TakeDownOptions title="Stopping, terminating or upgrading infra" bind:choice={infraChoice} takesDown={infraTakesDown} />
			{/if}
			{#if options.turnsTriggersOn}
				<fieldset class="space-y-1">
					<legend class="text-[11px] font-semibold uppercase tracking-wide text-zinc-500">Turning triggers on</legend>
					<div class="text-[11px] text-zinc-600">What they held while they were down</div>
					<label class="flex items-center gap-2 text-xs cursor-pointer" title="Run it, paced so new arrivals still get through.">
						<input type="radio" checked={(releaseHeld ?? 'run') === 'run'} onchange={() => (releaseHeld = 'run')} />
						<span>Run it</span>
					</label>
					<label class="flex items-center gap-2 text-xs cursor-pointer" title="Delete the held arrivals and cancel the paused runs.">
						<input type="radio" checked={releaseHeld === 'drop'} onchange={() => (releaseHeld = 'drop')} />
						<span>Drop it</span>
					</label>
				</fieldset>
			{/if}
		</div>
	{/if}

	{#if runTargets.length > 0}
		<button
			class="{btn} {btnDisabled} px-6 bg-zinc-900 text-white shadow hover:bg-zinc-800"
			onclick={onRun}
			disabled={runWhy !== undefined}
			title={runWhy ?? `Run ${runTargets.join(', ')} and what they need`}
		>
			<Play class="w-3.5 h-3.5" />
			<span class={labelCss}>{runTargets.length === 1 ? 'Run 1 target' : `Run ${runTargets.length} targets`}</span>
		</button>
	{/if}

	{#if actions.triggers.length > 0}
		{#if runTargets.length > 0}
			<div class={divider}></div>
		{/if}
		{#each actions.triggers as action (action.verb)}
			<button
				class="{btn} {btnDisabled} {TRIGGER_CSS[action.verb]}"
				onclick={() => actOnTriggers(action.verb, action.places)}
				disabled={why(action.verb) !== undefined}
				title={why(action.verb) ?? action.places.join(', ')}
			>
				{#if action.verb === 'activate'}
					<Zap class="w-3.5 h-3.5" />
				{:else if action.verb === 'deactivate'}
					<Square class="w-3.5 h-3.5" />
				{:else}
					<RefreshCw class="w-3.5 h-3.5" />
				{/if}
				<span class={labelCss}>{TRIGGER_LABELS[action.verb]}{counted(action.places)}</span>
			</button>
		{/each}
	{/if}

	{#if actions.infra.length > 0}
		{#if runTargets.length > 0 || actions.triggers.length > 0}
			<div class={divider}></div>
		{/if}
		{#each actions.infra as action (action.verb)}
			<button
				class="{btn} {btnDisabled} {INFRA_CSS[action.verb]}"
				onclick={() => actOnInfra(action.verb, action.places)}
				disabled={why(action.verb) !== undefined}
				title={why(action.verb) ?? action.places.join(', ')}
			>
				{#if action.verb === 'infra_start'}
					<Database class="w-3.5 h-3.5" />
				{:else if action.verb === 'infra_stop'}
					<Square class="w-3.5 h-3.5" />
				{:else if action.verb === 'infra_upgrade'}
					<RefreshCw class="w-3.5 h-3.5" />
				{:else}
					<Trash2 class="w-3.5 h-3.5" />
				{/if}
				<span class={labelCss}>{INFRA_LABELS[action.verb]}{counted(action.places)}</span>
			</button>
		{/each}
	{/if}

	{#if options.any}
		<button
			class="{btn} px-2 bg-white text-zinc-600 border border-zinc-200 hover:bg-zinc-50"
			onclick={() => (optionsOpen = !optionsOpen)}
			title="How triggers go down and come back"
		>
			<span class={labelCss}>Options</span>
			<ChevronUp class="w-3.5 h-3.5 transition-transform {optionsOpen ? 'rotate-180' : ''}" />
		</button>
	{/if}

	{#if showPicker}
		<div class={divider}></div>
		<div class="relative">
			<button
				class="{btn} px-2 bg-white text-zinc-700 border border-zinc-200 hover:bg-zinc-50"
				onclick={() => (pickerOpen = !pickerOpen)}
				title="Whose copies the actions act on"
			>
				<span class={labelCss}>Instance: <span class="normal-case tracking-normal">{targetLabel}</span></span>
				<ChevronDown class="w-3.5 h-3.5" />
			</button>
			{#if pickerOpen}
				<div class="absolute bottom-full mb-3 right-0 z-50 w-60 bg-white border rounded-lg shadow-lg p-1 text-xs">
					<div class="flex items-center gap-1 px-2 py-1 border-b mb-1">
						<Search class="w-3 h-3 text-zinc-400" />
						<!-- svelte-ignore a11y_autofocus -->
						<input class="w-full outline-none" placeholder="Find or type an instance id" bind:value={search} autofocus />
					</div>
					<button class="w-full text-left px-2 py-1.5 rounded hover:bg-muted" onclick={() => pick(undefined)}>
						shared <span class="text-zinc-400">(the program's own)</span>
					</button>
					<button class="w-full text-left px-2 py-1.5 rounded hover:bg-muted" onclick={() => pick('all_instances')}>
						all instances <span class="text-zinc-400">({known.length})</span>
					</button>
					<div class="max-h-48 overflow-y-auto">
						{#each matching as id (id)}
							<button class="w-full text-left px-2 py-1.5 rounded hover:bg-muted font-mono" onclick={() => pick([id])}>{id}</button>
						{/each}
					</div>
					{#if typedNew}
						<button class="w-full text-left px-2 py-1.5 rounded hover:bg-muted" onclick={() => pick([typedNew!])}>
							use <span class="font-mono">{typedNew}</span>
						</button>
					{/if}
				</div>
			{/if}
		</div>
	{/if}

	{#if actions.needInstance.length > 0}
		<span class="text-[11px] text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-2 py-1">
			Pick an instance (or all) to act on {actions.needInstance.join(', ')}: each instance has its own.
		</span>
	{:else if actions.sharedLeftOut.length > 0}
		<span class="text-[11px] text-zinc-600 bg-zinc-50 border border-zinc-200 rounded-lg px-2 py-1">
			{actions.sharedLeftOut.join(', ')} {actions.sharedLeftOut.length === 1 ? 'is' : 'are'} shared: pick "shared" to act on {actions.sharedLeftOut.length === 1 ? 'it' : 'them'}.
		</span>
	{/if}
</div>
