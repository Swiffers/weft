import { describe, expect, it } from 'vitest';
import type { CallerInspectorEvent, WindowedCallerMessage } from '../../../protocol';
import { callerCopyText, callerLines, droppedNote } from './caller-log';

const out = (offset: number, data: string): WindowedCallerMessage => ({
	offset,
	direction: 'outbound',
	payload: { kind: 'json', data },
	payloadByteSize: data.length + 2,
	atUnix: 1,
});

describe('callerLines', () => {
	it('marks the messages the record names as never received, and says why', () => {
		const events: CallerInspectorEvent[] = [
			{ kind: 'connected', offset: 0, protocol: 'http', atUnix: 1 },
			{ kind: 'window', firstOffset: 1, lastOffset: 2, messages: [out(1, 'a'), out(2, 'b')], totals: [], atUnix: 1 },
			// `c` pushed `a` out of the queue, and `d` was let go itself.
			{ kind: 'window', firstOffset: 3, lastOffset: 5, messages: [out(3, 'c'), out(4, 'd'), out(5, 'e')], totals: [], dropped: [1, 4], atUnix: 1 },
			// `e` was still waiting when the caller left.
			{ kind: 'disconnected', offset: 6, reason: 'caller hung up', dropped: [5], atUnix: 1 },
		];
		const lines = callerLines(events);
		expect(lines.map((l) => [l.text, l.dropped])).toEqual([
			['the caller connected over http', undefined],
			['a', droppedNote()],
			['b', undefined],
			['c', undefined],
			['d', droppedNote()],
			['e', droppedNote('caller hung up')],
			['the caller went away (caller hung up)', undefined],
		]);
		expect(droppedNote()).toContain('read too slowly');
		expect(droppedNote('caller hung up')).toContain('the exchange ended first (caller hung up)');
		const text = callerCopyText(lines);
		expect(text).toContain(`program: a (${droppedNote()})`);
		expect(text).toContain(`program: e (${droppedNote('caller hung up')})`);
		expect(text).not.toContain('program: b (');
	});

	it('a conversation with no drops marks nothing', () => {
		const lines = callerLines([{ kind: 'window', firstOffset: 1, lastOffset: 1, messages: [out(1, 'a')], totals: [], atUnix: 1 }]);
		expect(lines[0].dropped).toBeUndefined();
	});
});
