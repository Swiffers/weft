// A visitor's upload of one file through an upload slot.
//
// A program hands the page a slot (`weft-upload:<token>`): a signed grant
// for one file. The page gives it to `uploadFile` with the file, which
// sends the file in parts through the install and answers the slot back,
// and the page then sends that slot to its route in a field declared as a
// file. The route receives the stored file; the bytes never pass through
// the route's own call.
//
// Each step is a POST to `<base>/files/upload/<verb>` carrying the slot
// beside the verb's own fields: `check` (would the install take this
// file?), `begin`, `parts`, `part-done`, `resume`, `complete`, `abort`.
// The parts themselves go to presigned links (through the site, or
// straight to the store), and the signature in a link is its credential,
// so a part PUT carries no token.

import { WEFT_PASS_THROUGH_PATH } from './transport';
import { trimTrailingSlashes } from './url';
import { Sha256 } from './sha256';

/** What an upload slot is written as: `weft-upload:<token>`. */
// SYNC: SLOT_PREFIX <-> crates/weft-core/src/storage/slot.rs SLOT_PREFIX
export const SLOT_PREFIX = 'weft-upload:';

/** The header a `409` carries when the slot's upload is being completed
 *  right now (an answer to `complete`, or to `check` or `begin` from a
 *  page reloaded meanwhile): the answer is coming, ask again shortly. */
// SYNC: COMPLETING_HEADER <-> crates/weft-core/src/storage/mod.rs COMPLETING_HEADER
export const COMPLETING_HEADER = 'x-weft-completing';

/** Where the verbs live under the weft base. */
// SYNC: 'files/upload' <-> crates/weft-dispatcher/src/files_door.rs upload, src/server/passthrough.ts PASSED_DOORS
const UPLOAD_PATH = 'files/upload';

/** Every verb's body: the slot, beside the verb's own fields. */
// SYNC: Slotted <-> crates/weft-core/src/storage/slot.rs Slotted
export type Slotted<T> = { slot: string } & T;

/** `begin`, and `check` before it: the file about to be sent. `mimeType`
 *  is `File.type`, empty when the browser names none (the file's first
 *  bytes then do). `check` answers what `begin` would refuse, before the
 *  page reads the file to hash it, and reserves nothing. */
// SYNC: SlotBegin <-> crates/weft-core/src/storage/slot.rs SlotBeginRequest
export interface SlotBegin {
	filename: string;
	mimeType: string;
	sizeBytes: number;
	/** The whole file's SHA-256, lowercase hex: when the install already
	 *  holds that content, the upload is made from it and no byte is sent. */
	sha256?: string;
}

// SYNC: UploadBeginAnswer <-> crates/weft-core/src/storage/mod.rs UploadBeginResponse
export interface UploadBeginAnswer {
	key: string;
	partSize: number;
	/** The install already holds this content: the upload is done. */
	alreadyStored: boolean;
	/** This slot's upload was already begun: ask `resume` what is missing. */
	resume: boolean;
}

/** One part about to be sent. Part `n` (from 1) holds the file's bytes
 *  from `(n - 1) * partSize`, and every part but the last is `partSize`. */
// SYNC: PartAsk <-> crates/weft-core/src/storage/mod.rs PartAsk
export interface PartAsk {
	partNumber: number;
	sizeBytes: number;
}

// SYNC: UploadPartsBody <-> crates/weft-core/src/storage/mod.rs UploadPartsRequest
export interface UploadPartsBody {
	key: string;
	parts: PartAsk[];
}

/** A reserved part: PUT exactly `sizeBytes` bytes, the file's from
 *  `offsetBytes`, to `url`. */
// SYNC: PresignedPart <-> crates/weft-core/src/storage/mod.rs PresignedPart
export interface PresignedPart {
	partNumber: number;
	sizeBytes: number;
	offsetBytes: number;
	url: string;
}

// SYNC: UploadPartsAnswer <-> crates/weft-core/src/storage/mod.rs UploadPartsResponse
export interface UploadPartsAnswer {
	parts: PresignedPart[];
}

/** `part-done`: a part landed. `etag` is the store's `ETag` answer header
 *  as it came, quotes included. */
// SYNC: PartDoneBody <-> crates/weft-core/src/storage/mod.rs PartDoneRequest
export interface PartDoneBody {
	key: string;
	partNumber: number;
	etag: string;
}

/** `complete`, `resume` and `abort` name the upload alone. */
// SYNC: UploadKeyBody <-> crates/weft-core/src/storage/mod.rs UploadCompleteRequest, UploadResumeRequest, UploadAbortRequest
export interface UploadKeyBody {
	key: string;
}

/** `resume`: fresh links for the parts reserved but never reported done,
 *  and where new parts start (`reservedBytes`, which is past the end of
 *  the last missing part whenever a part in the middle is the one missing). */
// SYNC: UploadResumeAnswer <-> crates/weft-core/src/storage/mod.rs UploadResumeResponse
export interface UploadResumeAnswer {
	partSize: number;
	missing: PresignedPart[];
	reservedBytes: number;
}

/** What the install refused, in its own words, with the status it
 *  answered. A page can show `message` to the visitor. */
export class UploadError extends Error {
	constructor(
		message: string,
		readonly status?: number,
	) {
		super(message);
		this.name = 'UploadError';
	}
}

export interface UploadOptions {
	/** Where the page reaches weft: its own site's pass-through by default
	 *  (`WEFT_PASS_THROUGH_PATH`), or the install's address. */
	base?: string;
	/** Defaults to the global `fetch`. */
	fetcher?: typeof fetch;
	/** How many parts are reserved per `parts` call. */
	batch?: number;
	/** How many parts are sent at once. */
	parallel?: number;
	/** How many drops in a row, with no part landing between them, are
	 *  picked up again before the upload fails, and the wait before the
	 *  first (doubling with each drop in a row). */
	retry?: { attempts: number; firstMs: number; longestMs: number };
	/** Defaults to a timer; tests pass one that does not wait. */
	wait?: (ms: number) => Promise<void>;
	/** Stops the upload: the file stops being read, no new part goes, the
	 *  parts in flight are cut, and the install is told to end the upload
	 *  (`abort`). `uploadFile` then throws the signal's reason. */
	signal?: AbortSignal;
}

/** How much of the file has landed, out of its size. */
export type UploadProgress = (doneBytes: number, totalBytes: number) => void;

// The pieces the file is read in to hash it: big enough that the hash
// runs at full speed, small enough that a page never holds much at once.
const HASH_PIECE = 8 << 20;

/** Send `file` through the upload `slot`, and answer the slot, which is
 *  what the page sends its route in a file field. The install first says
 *  whether it takes the file at all, so a refusal (a file too big, a type
 *  the slot does not accept, a slot that ended) comes before the file is
 *  read, at once, in the install's own sentence. A part that does not
 *  land (the network dropped, its link expired, the store failed) is sent
 *  again after asking the install what is missing, with a growing wait,
 *  and once it has been picked up `retry.attempts` times in a row with no
 *  part landing, the upload fails naming what went wrong. Once every part
 *  landed, the one step left is `complete`, and a lost answer is picked up
 *  by asking it again (the install answers a repeat with the file it
 *  became). An upload that fails or is stopped once begun is ended on the
 *  install (`abort`), unless a `complete` sent for it may have landed; if
 *  ending it fails too, the error names both. */
export async function uploadFile(
	slot: string,
	file: File,
	onProgress?: UploadProgress,
	options: UploadOptions = {},
): Promise<string> {
	if (!slot.startsWith(SLOT_PREFIX)) {
		throw new UploadError(`an upload slot starts with '${SLOT_PREFIX}', and this one does not`);
	}
	const base = trimTrailingSlashes(options.base ?? WEFT_PASS_THROUGH_PATH);
	const fetcher = options.fetcher ?? ((...args) => fetch(...args));
	const batch = Math.max(1, options.batch ?? 8);
	const parallel = Math.max(1, options.parallel ?? 4);
	const retry = options.retry ?? { attempts: 5, firstMs: 1_000, longestMs: 30_000 };
	const signal = options.signal;
	const timer = options.wait ?? ((ms) => waitUnlessStopped(ms, signal));
	const wait = async (ms: number) => {
		await timer(ms);
		signal?.throwIfAborted();
	};
	const total = file.size;
	const progress = (done: number) => onProgress?.(done, total);

	// `live` is false only for `abort`, which goes after the signal fired.
	const verb = async <T>(name: string, body: object, live = true): Promise<T | undefined> => {
		const answer = await call(
			fetcher,
			`${base}/${UPLOAD_PATH}/${name}`,
			{ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ slot, ...body }) },
			live ? signal : undefined,
		);
		if (!answer.ok) throw await refusal(answer, `the install's '${name}' step`);
		if (answer.status === 204) return undefined;
		return (await answer.json()) as T;
	};

	// Drops in a row with nothing landed between them: what `retry.attempts`
	// counts, and what the wait grows with.
	let drops = 0;
	const persist = async (step: () => Promise<void>) => {
		for (;;) {
			try {
				await step();
				drops = 0;
				return;
			} catch (e) {
				if (!(e instanceof Dropped)) throw e;
				if (drops >= retry.attempts) {
					throw new UploadError(
						`the upload of ${file.name} stopped and did not pick up again after ${retry.attempts} tries: ${e.message}`,
						e.status,
					);
				}
				await wait(Math.min(retry.firstMs * 2 ** drops, retry.longestMs));
				drops++;
			}
		}
	};

	// Ask `step` again, after a wait, while the slot's upload is being
	// completed (this page's, or the one of a page reloaded meanwhile).
	const whileCompleting = async <T>(step: () => Promise<T>): Promise<T> => {
		for (let attempt = 0; ; attempt++) {
			try {
				return await step();
			} catch (e) {
				const delay = e instanceof Completing ? completingWait(attempt) : null;
				if (delay === null) throw e instanceof Completing ? new UploadError(e.message, 409) : e;
				await wait(delay);
			}
		}
	};

	const asked = { filename: file.name, mimeType: file.type, sizeBytes: total } satisfies SlotBegin;
	// A slot whose upload is being completed is taken once it lands: the
	// begin then answers that it is already stored.
	await persist(async () => {
		await whileCompleting(() => verb('check', asked));
	});
	const begin: SlotBegin = { ...asked, sha256: await hashOf(file, signal) };

	let key: string | undefined;
	// The upload whose every part landed: from then on the only step left,
	// and the only one a retry takes, is `complete`.
	let landed: string | undefined;
	// Whether a `complete` sent may have landed (its answer lost, cut, or
	// "being completed"): an upload that may be complete is never aborted.
	let completeMayHaveLanded = false;
	let partSize = 0;
	// The parts reserved but not landed, and where the store stopped carving.
	let pending: PresignedPart[] = [];
	let reserved = 0;
	let done = 0;

	// Learn where the upload stands: what is missing, where new parts start.
	const pickUp = async (upload: string) => {
		const resumed = await verb<UploadResumeAnswer>('resume', { key: upload } satisfies UploadKeyBody);
		if (!resumed) throw new UploadError("the install's 'resume' step answered nothing");
		partSize = resumed.partSize;
		pending = checkedParts(resumed.missing, partSize, total);
		reserved = resumed.reservedBytes;
		done = reserved - pending.reduce((sum, part) => sum + part.sizeBytes, 0);
		progress(done);
	};

	const sendPart = async (upload: string, part: PresignedPart) => {
		const answer = await call(
			fetcher,
			part.url,
			{
				method: 'PUT',
				// The slice has no type of its own, so no `Content-Type` goes
				// out, and its exact size goes as `Content-Length`: the link
				// was signed for that size.
				body: file.slice(part.offsetBytes, part.offsetBytes + part.sizeBytes),
			},
			signal,
		);
		if (!answer.ok) throw await refusal(answer, `part ${part.partNumber}`, true);
		const etag = answer.headers.get('etag');
		if (!etag) {
			throw new UploadError(
				`the store took part ${part.partNumber} but its answer carried no ETag (a store reached cross-origin has to expose it)`,
			);
		}
		await verb('part-done', { key: upload, partNumber: part.partNumber, etag } satisfies PartDoneBody);
		drops = 0;
		done += part.sizeBytes;
		progress(done);
	};

	const transfer = async () => {
		if (landed !== undefined) return complete(landed);
		if (key === undefined) {
			const begun = await whileCompleting(() => verb<UploadBeginAnswer>('begin', begin));
			if (!begun) throw new UploadError("the install's 'begin' step answered nothing");
			if (begun.alreadyStored) {
				done = total;
				progress(done);
				return;
			}
			key = begun.key;
			partSize = begun.partSize;
			if (begun.resume) await pickUp(key);
			else progress(done);
		} else {
			await pickUp(key);
		}
		const upload = key;
		if (reserved < total && !(partSize > 0)) {
			throw new UploadError(`the install gave the upload of ${file.name} a part size of ${partSize} bytes`);
		}
		await inPool(pending, parallel, (part) => sendPart(upload, part));
		pending = [];
		while (reserved < total) {
			const asks: PartAsk[] = [];
			for (let at = reserved; at < total && asks.length < batch; at += partSize) {
				asks.push({ partNumber: Math.floor(at / partSize) + 1, sizeBytes: Math.min(partSize, total - at) });
			}
			const got = await verb<UploadPartsAnswer>('parts', { key: upload, parts: asks } satisfies UploadPartsBody);
			if (!got) throw new UploadError("the install's 'parts' step answered nothing");
			reserved += asks.reduce((sum, ask) => sum + ask.sizeBytes, 0);
			await inPool(checkedParts(got.parts, partSize, total), parallel, (part) => sendPart(upload, part));
		}
		landed = upload;
		await complete(upload);
	};

	const complete = (upload: string) =>
		whileCompleting(async () => {
			try {
				await verb('complete', { key: upload } satisfies UploadKeyBody);
			} catch (e) {
				// Only the install's refusal says this one did not land.
				if (!(e instanceof UploadError)) completeMayHaveLanded = true;
				throw e;
			}
		});

	try {
		await persist(transfer);
		return slot;
	} catch (e) {
		if (key === undefined || completeMayHaveLanded) throw e;
		// Begun and not finishing: the install ends it, so its parts and
		// its reservation do not wait for the sweep.
		try {
			await verb('abort', { key } satisfies UploadKeyBody, false);
		} catch (ending) {
			throw new UploadError(
				`${messageOf(e)} (and ending the upload on the install failed too: ${messageOf(ending)})`,
				e instanceof UploadError || e instanceof Dropped ? e.status : undefined,
			);
		}
		throw e;
	}
}

function messageOf(e: unknown): string {
	return e instanceof Error ? e.message : String(e);
}

/** A timer that ends early, throwing the reason, when `signal` fires. */
function waitUnlessStopped(ms: number, signal: AbortSignal | undefined): Promise<void> {
	return new Promise<void>((resolve, reject) => {
		const stop = () => {
			clearTimeout(timer);
			reject(signal?.reason);
		};
		const timer = setTimeout(() => {
			signal?.removeEventListener('abort', stop);
			resolve();
		}, ms);
		signal?.addEventListener('abort', stop, { once: true });
	});
}

/** A failure that sending again may get past: the network dropped, the
 *  install or the store failed (a 5xx), or a part's link was refused (a
 *  403, which is what an expired link gets; the resume signs fresh ones). */
class Dropped extends Error {
	constructor(
		message: string,
		readonly status?: number,
	) {
		super(message);
	}
}

/** The upload is being completed right now: ask again shortly. */
class Completing extends Error {}

/** One request. A request cut by `signal` throws its reason, never a drop
 *  to pick up from. */
async function call(fetcher: typeof fetch, url: string, init: RequestInit, signal?: AbortSignal): Promise<Response> {
	signal?.throwIfAborted();
	try {
		return await fetcher(url, signal ? { ...init, signal } : init);
	} catch (e) {
		signal?.throwIfAborted();
		throw new Dropped(`${url.split('?')[0]} did not answer (${e instanceof Error ? e.message : String(e)})`);
	}
}

/** What a failed answer means: a drop to pick up from, the completion
 *  under way, or the install's refusal in its own words. */
async function refusal(answer: Response, what: string, isPart = false): Promise<Error> {
	const text = (await answer.text().catch(() => '')).trim();
	const said = text || `${what} answered ${answer.status}`;
	if (answer.status >= 500 || (isPart && answer.status === 403)) {
		return new Dropped(`${what} answered ${answer.status}${text ? `: ${text}` : ''}`, answer.status);
	}
	if (answer.status === 409 && answer.headers.has(COMPLETING_HEADER)) return new Completing(said);
	return new UploadError(said, answer.status);
}

/** The parts the store handed out, checked before any of them picks a
 *  slice of the file: a part bigger than a part, or reaching past the end
 *  of the file, would send the wrong bytes under an honest part number. */
function checkedParts(parts: PresignedPart[], partSize: number, total: number): PresignedPart[] {
	const bad = parts.find((p) => p.sizeBytes > partSize || p.offsetBytes + p.sizeBytes > total);
	if (bad) {
		throw new UploadError(
			`the install placed part ${bad.partNumber} at byte ${bad.offsetBytes} with ${bad.sizeBytes} bytes, which does not fit a ${total} byte file in parts of up to ${partSize} bytes`,
		);
	}
	return [...parts].sort((a, b) => a.offsetBytes - b.offsetBytes);
}

/** Run `work` on every item, `width` at a time. On the first failure no
 *  new item starts, the ones already running finish, and that failure is
 *  thrown, so no part is left half sent behind the caller's back. */
async function inPool<T>(items: T[], width: number, work: (item: T) => Promise<void>): Promise<void> {
	let next = 0;
	let failure: unknown;
	const lane = async () => {
		while (failure === undefined && next < items.length) {
			const item = items[next++]!;
			try {
				await work(item);
			} catch (e) {
				failure ??= e;
			}
		}
	};
	await Promise.all(Array.from({ length: Math.min(width, items.length) }, lane));
	if (failure !== undefined) throw failure;
}

/** The file's SHA-256, read one piece at a time, until `signal` fires. */
async function hashOf(file: Blob, signal: AbortSignal | undefined): Promise<string> {
	const hash = new Sha256();
	for (let at = 0; at < file.size; at += HASH_PIECE) {
		signal?.throwIfAborted();
		hash.update(new Uint8Array(await file.slice(at, at + HASH_PIECE).arrayBuffer()));
	}
	return hash.hex();
}

/** How long to wait before asking `complete` again while the upload is
 *  being completed, after `attempt` asks; `null` once the wait is over.
 *  1s doubling, capped at 30s, twelve times: about four minutes, near the
 *  store's completion lease, after which it finishes or ends the upload. */
// SYNC: completion backoff <-> crates/weft-cli/src/commands/assets.rs completing_wait, crates/weft-engine/src/storage.rs COMPLETE_ATTEMPTS
function completingWait(attempt: number): number | null {
	return attempt < 12 ? Math.min(1_000 * 2 ** Math.min(attempt, 5), 30_000) : null;
}
