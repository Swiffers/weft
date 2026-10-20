// SHA-256 fed in pieces. `crypto.subtle.digest` takes the whole input at
// once, and a visitor's upload can be bigger than a page may hold in
// memory, so `uploadFile` hashes the file one slice at a time with this.

const K = new Uint32Array([
	0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
	0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
	0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
	0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
	0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
	0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
	0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
	0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

/** A running SHA-256: `update` with each piece in order, then `hex` once. */
export class Sha256 {
	private readonly h = new Uint32Array([
		0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
	]);
	private readonly w = new Uint32Array(64);
	// The bytes of a block not yet full, carried to the next `update`.
	private readonly block = new Uint8Array(64);
	private filled = 0;
	private length = 0;
	private done = false;

	update(bytes: Uint8Array): this {
		if (this.done) throw new Error('sha256: update after the digest was taken');
		this.length += bytes.length;
		let at = 0;
		if (this.filled > 0) {
			const take = Math.min(64 - this.filled, bytes.length);
			this.block.set(bytes.subarray(0, take), this.filled);
			this.filled += take;
			at = take;
			if (this.filled < 64) return this;
			this.compress(this.block, 0);
			this.filled = 0;
		}
		for (; at + 64 <= bytes.length; at += 64) this.compress(bytes, at);
		this.block.set(bytes.subarray(at), 0);
		this.filled = bytes.length - at;
		return this;
	}

	/** The digest as lowercase hex. */
	hex(): string {
		if (this.done) throw new Error('sha256: the digest was already taken');
		this.done = true;
		const bits = this.length * 8;
		const tail = new Uint8Array(this.filled < 56 ? 64 : 128);
		tail.set(this.block.subarray(0, this.filled));
		tail[this.filled] = 0x80;
		// The length in bits, big-endian, in the last eight bytes. A file
		// stays far below 2^53 bits, so a float carries it exactly.
		const view = new DataView(tail.buffer);
		view.setUint32(tail.length - 8, Math.floor(bits / 0x1_0000_0000));
		view.setUint32(tail.length - 4, bits >>> 0);
		for (let at = 0; at < tail.length; at += 64) this.compress(tail, at);
		return Array.from(this.h, (word) => word.toString(16).padStart(8, '0')).join('');
	}

	private compress(bytes: Uint8Array, at: number): void {
		const w = this.w;
		for (let i = 0; i < 16; i++) {
			const j = at + i * 4;
			w[i] = (bytes[j]! << 24) | (bytes[j + 1]! << 16) | (bytes[j + 2]! << 8) | bytes[j + 3]!;
		}
		for (let i = 16; i < 64; i++) {
			const a = w[i - 15]!;
			const b = w[i - 2]!;
			const s0 = ((a >>> 7) | (a << 25)) ^ ((a >>> 18) | (a << 14)) ^ (a >>> 3);
			const s1 = ((b >>> 17) | (b << 15)) ^ ((b >>> 19) | (b << 13)) ^ (b >>> 10);
			w[i] = (w[i - 16]! + s0 + w[i - 7]! + s1) | 0;
		}
		// Fixed-size arrays (8 state words, 64 schedule words, 64 constants),
		// indexed in range, so every read below is a number.
		const H = this.h;
		let a = H[0]!, b = H[1]!, c = H[2]!, d = H[3]!, e = H[4]!, f = H[5]!, g = H[6]!, h = H[7]!;
		for (let i = 0; i < 64; i++) {
			const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
			const ch = (e & f) ^ (~e & g);
			const t1 = (h + S1 + ch + K[i]! + w[i]!) | 0;
			const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
			const maj = (a & b) ^ (a & c) ^ (b & c);
			const t2 = (S0 + maj) | 0;
			h = g;
			g = f;
			f = e;
			e = (d + t1) | 0;
			d = c;
			c = b;
			b = a;
			a = (t1 + t2) | 0;
		}
		H[0] = H[0]! + a;
		H[1] = H[1]! + b;
		H[2] = H[2]! + c;
		H[3] = H[3]! + d;
		H[4] = H[4]! + e;
		H[5] = H[5]! + f;
		H[6] = H[6]! + g;
		H[7] = H[7]! + h;
	}
}
