import { describe, expect, it } from 'vitest';
import { busClosedLine } from './stream-log';

describe('busClosedLine', () => {
	it('a finished bus reads as a quiet close', () => {
		expect(busClosedLine({ kind: 'finished' })).toEqual({ text: '* the bus closed here', failed: false });
	});

	it('a failed bus says it failed and why, as an error', () => {
		expect(busClosedLine({ kind: 'failed', error: 'producer crashed' })).toEqual({
			text: '* the bus failed here: producer crashed',
			failed: true,
		});
	});
});
