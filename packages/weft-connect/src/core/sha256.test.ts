import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { Sha256 } from './sha256';

const text = (s: string) => new TextEncoder().encode(s);

describe('the SHA-256 fed in pieces', () => {
	it('matches the published vectors', () => {
		expect(new Sha256().hex()).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
		expect(new Sha256().update(text('abc')).hex()).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
		expect(new Sha256().update(text('abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq')).hex()).toBe(
			'248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1',
		);
		// A million 'a's: many blocks, fed as one piece.
		expect(new Sha256().update(new Uint8Array(1_000_000).fill(0x61)).hex()).toBe(
			'cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0',
		);
	});

	it('gives the same digest however the input is cut', () => {
		const bytes = new Uint8Array(300_007);
		for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 31 + (i >> 8)) & 0xff;
		const expected = createHash('sha256').update(bytes).digest('hex');
		// Cuts that land inside a block, on its edge, and pieces smaller
		// than a block, so the carried bytes are exercised every way.
		for (const piece of [1, 7, 55, 56, 63, 64, 65, 1000, 65_536, 300_007]) {
			const hash = new Sha256();
			for (let at = 0; at < bytes.length; at += piece) hash.update(bytes.subarray(at, at + piece));
			expect(hash.hex(), `pieces of ${piece}`).toBe(expected);
		}
	});

	it('pads every length around a block edge right', () => {
		for (let length = 50; length <= 130; length++) {
			const bytes = new Uint8Array(length).fill(length);
			expect(new Sha256().update(bytes).hex(), `${length} bytes`).toBe(createHash('sha256').update(bytes).digest('hex'));
		}
	});
});
