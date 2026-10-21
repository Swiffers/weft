import { describe, expect, it } from 'vitest';

import { camelCase } from './node-id';

// Mirrors crates/weft-core/src/casing.rs Casing::Camel.spell, so a name the
// editor mints passes the compiler's `not-camel-case` check.
describe('camelCase', () => {
	it.each([
		['Debug', 'debug'],
		['HttpRequest', 'httpRequest'],
		['LLMInference', 'llmInference'],
		['PostgresDatabase', 'postgresDatabase'],
		['userURLs', 'userUrls'],
		['my_group', 'myGroup'],
		['a__data', 'aData'],
		['Http2Request', 'http2Request'],
	])('spells %s as %s', (name, spelled) => expect(camelCase(name)).toBe(spelled));
});
