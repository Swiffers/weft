/** `name` spelled the way a node or group name in a program is: camelCase
 *  (`HttpRequest` -> `httpRequest`, `LLMInference` -> `llmInference`,
 *  `my_group` -> `myGroup`). Words split at `_`, `-` and spaces, at each
 *  lower-to-upper step, and at the last capital of a run of capitals that a
 *  lower case letter follows, unless that letter is a plural's lone `s`
 *  (`userURLs` is `user`, `urls`). */
// SYNC: camelCase <-> crates/weft-core/src/casing.rs Casing::spell (Camel), split_words
export function camelCase(name: string): string {
	return splitWords(name)
		.map((w, i) => (i === 0 ? w : w.charAt(0).toUpperCase() + w.slice(1)))
		.join('');
}

const isLower = (c: string | undefined) => c !== undefined && /[a-z0-9]/.test(c);
const isUpper = (c: string | undefined) => c !== undefined && /[A-Z]/.test(c);

function splitWords(name: string): string[] {
	const chars = [...name];
	const out: string[] = [];
	let word = '';
	chars.forEach((c, i) => {
		if (c === '_' || c === '-' || c === ' ') {
			if (word) { out.push(word); word = ''; }
			return;
		}
		const prev = chars[i - 1];
		const next = chars[i + 1];
		const afterLower = isLower(prev);
		// A lone `s` closing a run of capitals is a plural (`URLs`), not
		// the start of the next word.
		const plural = next === 's' && !/[a-z]/.test(chars[i + 2] ?? '');
		const endsAcronym = isUpper(prev) && next !== undefined && /[a-z]/.test(next) && !plural;
		if (isUpper(c) && (afterLower || endsAcronym) && word) {
			out.push(word);
			word = '';
		}
		word += c.toLowerCase();
	});
	if (word) out.push(word);
	return out;
}
