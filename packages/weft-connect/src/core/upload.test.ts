import { createHash } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { COMPLETING_HEADER, SLOT_PREFIX, uploadFile, UploadError, type PresignedPart } from './upload';

const slot = `${SLOT_PREFIX}tok`;
const instant = { wait: async () => {}, retry: { attempts: 3, firstMs: 0, longestMs: 0 } };

/** A file of `size` bytes whose every byte says where it sits. */
function fileOf(size: number, name = 'clip.mp4', type = 'video/mp4'): { file: File; bytes: Uint8Array } {
	const bytes = new Uint8Array(size);
	for (let i = 0; i < size; i++) bytes[i] = (i * 7 + (i >> 8)) & 0xff;
	return { file: new File([bytes], name, { type }), bytes };
}

/** A dumb install: the seven verbs at `https://site.example/weft/files/upload/<verb>`,
 *  and part links at `https://site.example/weft/files/runtime/<key>?partNumber=<n>`
 *  that keep what was PUT. `failPut` decides, per PUT, to answer a status
 *  instead (or throw, for a dropped connection). `loseCompleteAnswers`
 *  completes the upload and then drops that many answers to `complete`.
 *  `checkCompletingFirst` answers `check` that many times as a slot whose
 *  upload (another page's) is being completed, which then lands. Once
 *  completed it answers as the broker does: a repeat `complete` succeeds,
 *  a `begin` says already stored, `resume` and `abort` are refused. */
function fakeInstall(options: {
	partSize: number;
	alreadyStored?: boolean;
	refuseCheck?: string;
	refuseComplete?: string;
	refuseAbort?: string;
	completingFirst?: number;
	loseCompleteAnswers?: number;
	checkCompletingFirst?: number;
	failPut?: (partNumber: number, nth: number) => number | 'drop' | null;
}) {
	const key = 'runtime/local/project/0b6e8c1a/7f3e2a10';
	const reservedParts = new Map<number, { sizeBytes: number; offsetBytes: number }>();
	const stored = new Map<number, Uint8Array>();
	const etags = new Map<number, string>();
	const puts: number[] = [];
	const verbs: { verb: string; body: Record<string, unknown> }[] = [];
	let begun = false;
	let completing = options.completingFirst ?? 0;
	let losing = options.loseCompleteAnswers ?? 0;
	let checkCompleting = options.checkCompletingFirst ?? 0;
	let completed = false;
	const beingCompleted = () => new Response('being completed', { status: 409, headers: { [COMPLETING_HEADER]: '1' } });

	const link = (partNumber: number): PresignedPart => {
		const r = reservedParts.get(partNumber)!;
		return { partNumber, ...r, url: `https://site.example/weft/files/${key}?partNumber=${partNumber}&X-Amz-Signature=ab%2F` };
	};

	const fetcher = (async (url: string, init: RequestInit) => {
		const u = new URL(url);
		if (init.method === 'PUT') {
			const partNumber = Number(u.searchParams.get('partNumber'));
			puts.push(partNumber);
			const failed = options.failPut?.(partNumber, puts.filter((n) => n === partNumber).length);
			if (failed === 'drop') throw new TypeError('fetch failed');
			if (failed) return new Response('Request has expired', { status: failed });
			const body = new Uint8Array(await new Response(init.body as Blob).arrayBuffer());
			if (body.length !== reservedParts.get(partNumber)?.sizeBytes) return new Response('wrong length', { status: 400 });
			stored.set(partNumber, body);
			return new Response(null, { status: 200, headers: { etag: `"etag-${partNumber}"` } });
		}
		const verb = u.pathname.replace('/weft/files/upload/', '');
		const body = JSON.parse(init.body as string) as Record<string, unknown>;
		verbs.push({ verb, body });
		if (body.slot !== slot) return new Response('not this slot', { status: 403 });
		const json = (value: unknown) => new Response(JSON.stringify(value), { headers: { 'content-type': 'application/json' } });
		switch (verb) {
			case 'check':
				if (checkCompleting > 0) {
					checkCompleting--;
					if (checkCompleting === 0) completed = true;
					return beingCompleted();
				}
				return options.refuseCheck ? new Response(options.refuseCheck, { status: 413 }) : new Response(null, { status: 204 });
			case 'begin': {
				if (completed) return json({ key, partSize: 0, alreadyStored: true, resume: false });
				const resume = begun;
				begun = true;
				return json({ key, partSize: options.partSize, alreadyStored: options.alreadyStored ?? false, resume });
			}
			case 'parts': {
				const asks = body.parts as { partNumber: number; sizeBytes: number }[];
				for (const ask of asks) {
					reservedParts.set(ask.partNumber, { sizeBytes: ask.sizeBytes, offsetBytes: (ask.partNumber - 1) * options.partSize });
				}
				return json({ parts: asks.map((a) => link(a.partNumber)) });
			}
			case 'part-done':
				etags.set(body.partNumber as number, body.etag as string);
				return new Response(null, { status: 204 });
			case 'resume': {
				if (completed) return new Response(`upload '${key}' already completed; nothing to resume`, { status: 409 });
				const missing = [...reservedParts.keys()].filter((n) => !etags.has(n)).map(link);
				const reservedBytes = [...reservedParts.values()].reduce((s, p) => s + p.sizeBytes, 0);
				return json({ partSize: options.partSize, missing, reservedBytes });
			}
			case 'complete':
				if (completed) {
					if (losing > 0) {
						losing--;
						throw new TypeError('fetch failed');
					}
					return new Response(null, { status: 204 });
				}
				if (completing > 0) {
					completing--;
					return beingCompleted();
				}
				if (options.refuseComplete) return new Response(options.refuseComplete, { status: 400 });
				completed = true;
				if (losing > 0) {
					losing--;
					throw new TypeError('fetch failed');
				}
				return new Response(null, { status: 204 });
			case 'abort':
				if (completed) return new Response(`'${key}' already completed; delete the file instead of aborting`, { status: 409 });
				if (options.refuseAbort) return new Response(options.refuseAbort, { status: 409 });
				return new Response(null, { status: 204 });
			default:
				return new Response('no such verb', { status: 404 });
		}
	}) as unknown as typeof fetch;

	/** The parts landed, put back together in order. */
	const assembled = () => {
		const numbers = [...stored.keys()].sort((a, b) => a - b);
		const out = new Uint8Array(numbers.reduce((s, n) => s + stored.get(n)!.length, 0));
		let at = 0;
		for (const n of numbers) {
			out.set(stored.get(n)!, at);
			at += stored.get(n)!.length;
		}
		return out;
	};
	return { key, fetcher, puts, verbs, etags, assembled, isCompleted: () => completed };
}

const base = 'https://site.example/weft/';

describe('uploading a file through a slot', () => {
	it('sends every part once with its own bytes, reports each, and completes', async () => {
		const { file, bytes } = fileOf(25);
		const install = fakeInstall({ partSize: 8 });
		const seen: [number, number][] = [];
		const answered = await uploadFile(slot, file, (done, total) => seen.push([done, total]), {
			base,
			fetcher: install.fetcher,
			batch: 2,
			...instant,
		});
		expect(answered).toBe(slot);
		expect(install.isCompleted()).toBe(true);
		expect([...install.puts].sort()).toEqual([1, 2, 3, 4]);
		expect(install.assembled()).toEqual(bytes);
		// The etag goes back as the store gave it, quotes included.
		expect(install.etags.get(4)).toBe('"etag-4"');
		// The install is asked whether it takes the file before it is read.
		expect(install.verbs[0]).toEqual({ verb: 'check', body: { slot, filename: 'clip.mp4', mimeType: 'video/mp4', sizeBytes: 25 } });
		const begin = install.verbs[1];
		expect(begin).toEqual({
			verb: 'begin',
			body: { slot, filename: 'clip.mp4', mimeType: 'video/mp4', sizeBytes: 25, sha256: createHash('sha256').update(bytes).digest('hex') },
		});
		// Parts are asked for in batches, numbered by their place in the file.
		const asked = install.verbs.filter((v) => v.verb === 'parts').map((v) => v.body.parts);
		expect(asked).toEqual([
			[{ partNumber: 1, sizeBytes: 8 }, { partNumber: 2, sizeBytes: 8 }],
			[{ partNumber: 3, sizeBytes: 8 }, { partNumber: 4, sizeBytes: 1 }],
		]);
		expect(install.verbs.at(-1)?.verb).toBe('complete');
		expect(seen.at(-1)).toEqual([25, 25]);
		expect(seen.every(([, total]) => total === 25)).toBe(true);
	});

	it('sends no byte when the install already holds the content', async () => {
		const { file } = fileOf(20);
		const install = fakeInstall({ partSize: 8, alreadyStored: true });
		const seen: number[] = [];
		await uploadFile(slot, file, (done) => seen.push(done), { base, fetcher: install.fetcher, ...instant });
		expect(install.puts).toEqual([]);
		expect(install.verbs.map((v) => v.verb)).toEqual(['check', 'begin']);
		expect(seen).toEqual([20]);
	});

	it('sends a part that did not land again, alone, after asking what is missing', async () => {
		const { file, bytes } = fileOf(30);
		// Part 2's link answers 403 once (an expired link), part 3's
		// connection drops once; the others land the first time.
		const install = fakeInstall({
			partSize: 8,
			failPut: (n, nth) => (nth === 1 && n === 2 ? 403 : nth === 1 && n === 3 ? 'drop' : null),
		});
		await uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, batch: 4, parallel: 1, ...instant });
		expect(install.isCompleted()).toBe(true);
		expect(install.assembled()).toEqual(bytes);
		// One pass up to the 403, a resume sending part 2, then part 3
		// dropped, a resume sending part 3: what landed is never sent again.
		expect(install.puts).toEqual([1, 2, 2, 3, 3, 4]);
		expect(install.verbs.filter((v) => v.verb === 'resume')).toHaveLength(2);
		expect(install.verbs.filter((v) => v.verb === 'begin')).toHaveLength(1);
	});

	it('fails naming what failed once the tries are spent', async () => {
		const { file } = fileOf(10);
		const install = fakeInstall({ partSize: 8, failPut: (n) => (n === 2 ? 503 : null) });
		const failed = uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, ...instant });
		await expect(failed).rejects.toThrow(/clip\.mp4 stopped and did not pick up again after 3 tries: part 2 answered 503/);
		expect(install.puts.filter((n) => n === 2)).toHaveLength(4);
		expect(install.isCompleted()).toBe(false);
		// The upload is ended on the install, not left to the sweep.
		expect(install.verbs.at(-1)).toEqual({ verb: 'abort', body: { slot, key: install.key } });
	});

	it('counts drops in a row: a part landing starts the count, and the wait, over', async () => {
		const { file, bytes } = fileOf(32);
		// Every part drops the first time it is sent: four drops, never two in a row.
		const install = fakeInstall({ partSize: 8, failPut: (_n, nth) => (nth === 1 ? 'drop' : null) });
		const waits: number[] = [];
		await uploadFile(slot, file, undefined, {
			base,
			fetcher: install.fetcher,
			batch: 1,
			parallel: 1,
			retry: { attempts: 1, firstMs: 10, longestMs: 1_000 },
			wait: async (ms) => {
				waits.push(ms);
			},
		});
		expect(install.isCompleted()).toBe(true);
		expect(install.assembled()).toEqual(bytes);
		expect(waits).toEqual([10, 10, 10, 10]);
	});

	it("hears the install's refusal before reading the file", async () => {
		const { file } = fileOf(10);
		const sentence = 'the file weighs 10 bytes, and this upload takes at most 5';
		const install = fakeInstall({ partSize: 8, refuseCheck: sentence });
		const read = vi.spyOn(file, 'slice');
		await expect(uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, ...instant })).rejects.toThrow(
			new UploadError(sentence, 413),
		);
		expect(read).not.toHaveBeenCalled();
		expect(install.verbs.map((v) => v.verb)).toEqual(['check']);
	});

	it('stops reading the file when stopped, with nothing begun to end', async () => {
		const { file } = fileOf(10);
		const install = fakeInstall({ partSize: 8 });
		const stop = new AbortController();
		const fetcher = (async (url: string, init: RequestInit) => {
			const answer = await install.fetcher(url, init);
			stop.abort();
			return answer;
		}) as unknown as typeof fetch;
		const read = vi.spyOn(file, 'slice');
		await expect(uploadFile(slot, file, undefined, { base, fetcher, signal: stop.signal, ...instant })).rejects.toMatchObject({
			name: 'AbortError',
		});
		expect(read).not.toHaveBeenCalled();
		expect(install.verbs.map((v) => v.verb)).toEqual(['check']);
	});

	it('stops sending parts when stopped, and ends the upload on the install', async () => {
		const { file } = fileOf(40);
		const install = fakeInstall({ partSize: 8 });
		const stop = new AbortController();
		const fetcher = (async (url: string, init: RequestInit) => {
			if (init.method === 'PUT' && new URL(url).searchParams.get('partNumber') === '2') {
				// The page stops while part 2 is on its way: fetch is cut.
				stop.abort();
				init.signal?.throwIfAborted();
			}
			return install.fetcher(url, init);
		}) as unknown as typeof fetch;
		await expect(
			uploadFile(slot, file, undefined, { base, fetcher, batch: 8, parallel: 1, signal: stop.signal, ...instant }),
		).rejects.toMatchObject({ name: 'AbortError' });
		expect(install.puts).toEqual([1]);
		expect(install.isCompleted()).toBe(false);
		expect(install.verbs.at(-1)).toEqual({ verb: 'abort', body: { slot, key: install.key } });
		expect(install.verbs.filter((v) => v.verb === 'resume')).toHaveLength(0);
	});

	it('names a failure to end the upload beside what made it fail', async () => {
		const { file } = fileOf(10);
		const install = fakeInstall({ partSize: 8, refuseComplete: 'over quota', refuseAbort: 'being completed' });
		const failed = uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, ...instant });
		await expect(failed).rejects.toThrow(new UploadError('over quota (and ending the upload on the install failed too: being completed)', 400));
	});

	it("surfaces the install's refusal in its own words", async () => {
		const { file } = fileOf(10);
		const sentence = 'this upload takes a Video, and the file is an Image';
		const install = fakeInstall({ partSize: 8, refuseComplete: sentence });
		const failed = uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, ...instant });
		await expect(failed).rejects.toThrow(new UploadError(sentence, 400));
		// A refusal is not picked up again, and the upload is ended.
		expect(install.verbs.filter((v) => v.verb === 'resume')).toHaveLength(0);
		expect(install.verbs.at(-1)?.verb).toBe('abort');
	});

	it('asks complete again while the upload is being completed', async () => {
		const { file } = fileOf(10);
		const install = fakeInstall({ partSize: 8, completingFirst: 2 });
		await uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, ...instant });
		expect(install.verbs.filter((v) => v.verb === 'complete')).toHaveLength(3);
		expect(install.isCompleted()).toBe(true);
	});

	it('asks complete again when its answer was lost, and nothing else', async () => {
		const { file, bytes } = fileOf(20);
		// The upload lands, and the answers to the first two completes are lost.
		const install = fakeInstall({ partSize: 8, loseCompleteAnswers: 2 });
		const answered = await uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, ...instant });
		expect(answered).toBe(slot);
		expect(install.assembled()).toEqual(bytes);
		expect(install.verbs.filter((v) => v.verb === 'complete')).toHaveLength(3);
		expect(install.verbs.filter((v) => v.verb === 'resume')).toHaveLength(0);
		expect(install.verbs.filter((v) => v.verb === 'abort')).toHaveLength(0);
	});

	it('never aborts an upload whose complete may have landed', async () => {
		const { file } = fileOf(10);
		// Every complete's answer is lost: the tries run out.
		const install = fakeInstall({ partSize: 8, loseCompleteAnswers: 10 });
		const failed = uploadFile(slot, file, undefined, { base, fetcher: install.fetcher, ...instant });
		await expect(failed).rejects.toThrow(/clip\.mp4 stopped and did not pick up again after 3 tries/);
		expect(install.verbs.filter((v) => v.verb === 'complete')).toHaveLength(4);
		expect(install.verbs.filter((v) => ['resume', 'abort'].includes(v.verb))).toHaveLength(0);
	});

	it("waits out a reloaded page's upload being completed, then takes it", async () => {
		const { file } = fileOf(20);
		// The slot's upload, sent by the page before it reloaded, is being
		// completed when this one asks, and lands after two answers.
		const install = fakeInstall({ partSize: 8, checkCompletingFirst: 2 });
		const seen: number[] = [];
		const answered = await uploadFile(slot, file, (done) => seen.push(done), { base, fetcher: install.fetcher, ...instant });
		expect(answered).toBe(slot);
		expect(install.verbs.map((v) => v.verb)).toEqual(['check', 'check', 'check', 'begin']);
		expect(install.puts).toEqual([]);
		expect(seen).toEqual([20]);
	});

	it('picks up an upload this slot already began', async () => {
		const { file, bytes } = fileOf(20);
		const install = fakeInstall({ partSize: 8 });
		// A first page sends part 1, and is closed while part 2 is on its
		// way: it never answers, and never ends the upload.
		let closed!: () => void;
		const isClosed = new Promise<void>((resolve) => (closed = resolve));
		const closing = (async (url: string, init: RequestInit) => {
			if (init.method === 'PUT' && new URL(url).searchParams.get('partNumber') === '2') {
				closed();
				return new Promise<never>(() => {});
			}
			return install.fetcher(url, init);
		}) as unknown as typeof fetch;
		void uploadFile(slot, file, undefined, { base, fetcher: closing, parallel: 1, ...instant });
		await isClosed;
		// A second one begins, hears the upload is under way, and sends only
		// what is missing.
		const seen: number[] = [];
		await uploadFile(slot, file, (done) => seen.push(done), { base, fetcher: install.fetcher, ...instant });
		expect(install.verbs.filter((v) => v.verb === 'begin')).toHaveLength(2);
		expect(install.verbs.filter((v) => v.verb === 'abort')).toHaveLength(0);
		expect(install.puts).toEqual([1, 2, 3]);
		expect(install.assembled()).toEqual(bytes);
		expect(seen[0]).toBe(8);
		expect(seen.at(-1)).toBe(20);
	});

	it('refuses a string that is not a slot before calling anything', async () => {
		const install = fakeInstall({ partSize: 8 });
		await expect(uploadFile('tok', fileOf(1).file, undefined, { base, fetcher: install.fetcher })).rejects.toThrow(SLOT_PREFIX);
		expect(install.verbs).toEqual([]);
	});
});
