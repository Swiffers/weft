/// The back-and-forth between a run and its caller, as lines a person
/// reads: what the caller panel shows and what its copy button copies.
import type { CallerInspectorEvent } from '../../../protocol';
import { formatClockTime, formatStreamBody } from './stream-log';

/// One line per thing that happened, in the order it happened.
export interface CallerLine {
  atUnix: number;
  /// 'in' the caller spoke, 'out' the program did, 'note' neither
  /// (connected, errored, hung up).
  side: 'in' | 'out' | 'note';
  text: string;
  /// The program's last word on this exchange.
  terminal?: boolean;
  /// The program sent it and the caller never received it, and why
  /// (`droppedNote`).
  dropped?: string;
}

/// Why one of the program's messages never reached the caller, as its
/// line says it: the caller read too slowly (a window names it), or the
/// exchange ended while it was still waiting to go out (the row saying
/// the caller left names it, with how it ended).
export function droppedNote(ended?: string): string {
  return ended === undefined
    ? 'never reached the caller: it read too slowly'
    : `never reached the caller: the exchange ended first (${ended})`;
}

/// The exchange's events as lines.
///
/// A window row is not a thing that happened: it is the journal's way of
/// writing a second's worth of messages as one row, so it is unpacked
/// back into the messages it holds. A window can name a message of an
/// earlier window as dropped (one pushed out of the queue toward a slow
/// caller by a later one), and the row saying the caller left names
/// the messages still waiting then, so every drop is gathered before any
/// line is drawn.
export function callerLines(events: CallerInspectorEvent[]): CallerLine[] {
  const dropped = new Map<number, string>();
  for (const event of events) {
    if (event.kind === 'window') {
      for (const offset of event.dropped ?? []) dropped.set(offset, droppedNote());
    } else if (event.kind === 'disconnected') {
      for (const offset of event.dropped ?? []) dropped.set(offset, droppedNote(event.reason));
    }
  }
  return events.flatMap((event): CallerLine[] => {
    switch (event.kind) {
      case 'connected':
        return [{ atUnix: event.atUnix, side: 'note', text: `the caller connected over ${event.protocol}` }];
      case 'errored':
        return [{ atUnix: event.atUnix, side: 'note', text: `something went wrong: ${event.message}` }];
      case 'disconnected':
        return [{ atUnix: event.atUnix, side: 'note', text: `the caller went away (${event.reason})` }];
      case 'window':
        return event.messages.map((m) => ({
          atUnix: m.atUnix,
          side: m.direction === 'inbound' ? 'in' : 'out',
          text: formatStreamBody(m.payload, m.payloadByteSize, m.trimmed, 'message'),
          terminal: m.terminal,
          dropped: dropped.get(m.offset),
        }));
    }
  });
}

/// The same conversation as plain text, for pasting into a message to
/// somebody who cannot open the panel.
export function callerCopyText(lines: CallerLine[]): string {
  return lines
    .map((l) => {
      const who = l.side === 'in' ? 'caller' : l.side === 'out' ? 'program' : '*';
      const note = l.dropped ? ` (${l.dropped})` : '';
      return `${formatClockTime(l.atUnix)}  ${who}: ${l.text}${note}`;
    })
    .join('\n');
}
