# Streams and buses in Rust

For which of the two you want, go and read
[streams and buses](../language/streams-and-buses.md). This is the Rust on both
sides.

## Producing a stream

Declare a `Generator[T]` output, then emit as many times as you like:

```rust
for chunk in response_chunks {
    ctx.pulse_downstream(NodeOutput::new().set("chunk", chunk)).await?;
}
```

Each emission is one item, checked against the element type. When your body
returns, the port closes and that is the end of the stream. To end it early,
`ctx.close_port("chunk")`. After a close, nothing can follow.

If your node has to wait on a signal, wait before the first item. Once you
yielded on or closed a stream output, `ctx.await_signal` is refused: a pause
replays the body from the top, and items that already went out live cannot be
sent again.

`pulse_downstream` sends and carries on. If you are producing faster than the
consumer reads, use `yield_downstream`, which waits until each item is taken.

If you do neither and run far ahead, you hit the cap at 4096 un-taken items and
the emit fails, telling you the three ways out: yield instead of pulse, let the
consumer catch up, or `ctx.set_max_buffered_items("chunk", n)`.

## Consuming a stream

Read the port like any input, then pull:

```rust
let rows: Generator<Row> = ctx.inputs.get("rows")?;

while let Some(row) = rows.next().await? {
    // one row at a time
}
```

Your body starts when the first item arrives, so you are running while the
producer is still producing.

`next()` waits for the next item and gives you `None` once, at a clean end.
`try_next()` is the same without waiting, answering `Item`, `Empty` or
`Finished`. `drain()` takes the lot and waits for the end. `end()` tells you
whether the producer finished or failed, or `None` while it is still open.

The `?` matters. If the producer failed, that is where you find out, rather
than getting a clean end you would mistake for an empty stream. `drain` errors
without handing back the partial list, for the same reason.

Reading the port twice gives you two handles over one stream, sharing a
position, not two copies.

**Your node cannot durably suspend.** A stream cannot be replayed, so
`ctx.await_signal` in a stream consumer is refused. Do the waiting upstream or
downstream.

## Hosting a bus

```rust
let bus = ctx.open_bus("channel", BusOptions::default(), "host").await?;
bus.send("text", json!({ "text": "hello" })).node_err("sending the greeting")?;
```

`open_bus` does the whole producer move: makes the bus, emits the marker on
that port, and registers your name.

If the work that makes your pieces can fail (a provider's call, a handshake, a
first query), open the bus on the first piece instead of before the work.
Until the marker goes out, your node has opened no stream. If it fails then, a
program that wired your `_error` still gets the failure there, and the port
closes as if nothing was coming, so the readers never start. `ctx.pending_bus` holds the
options until then:

```rust
let mut bus = ctx.pending_bus("stream", BusOptions::default(), "llm");
let mut chunks = client.stream(&prompt).await.node_err("starting the stream")?;
while let Some(chunk) = chunks.next().await {
    bus.open().await?.send("delta", json!(chunk)).node_err("sending a delta")?;
}
```

The first `open()` makes the bus and emits its marker, and every later one
hands back the same bus. If you never call it, the port closes like any port
you never mentioned. `bus.opened()` tells you whether it is open yet without
opening it. `LlmStream` works this way.

## Joining one

```rust
let bus = ctx.join_bus("channel", "translator")?;
bus.wait_for("host").await.node_err("waiting for the host")?;

let mut cursor = bus.cursor();
while let Some(msg) = cursor.next().await {
    // ...
}
```

`join_bus` resolves the bus on that input and registers your name. A bus you
joined ends with your body, as one you made does, so a reader that dies
mid-conversation never leaves the producer waiting on it.

For an observer that must **not** end the bus when it is done (a debug tap, or
a node streaming the bus out to a caller), use `ctx.bus_from_input("channel")`
instead.

If your node has a slow warmup, call `join_bus` only once it is ready, because
registering is what everybody else's `wait_for` waits on. Each reader has its
own cursor, starting where it joined, so reading takes nothing away from the
others.

`wait_for(name)` parks until that name is live, or returns an error if the bus
closed first. You never have to decide whether a name will ever turn up: the
engine watches the run, and when a wait can no longer be satisfied it ends the
bus, failed and saying the run was stuck, and every waiting cursor wakes.

## How a bus ends

weft ends every bus your firing made or joined when your body returns, and
says how: **finished** if the body returned `Ok`, **failed** with the error if
it returned one, panicked, or was cancelled. Pausing on `ctx.await_signal`
ends it failed too, because the body that resumes opens a new bus and never
finishes this one. A task you spawned does not keep a bus open: the bus ends
with the body, whoever still holds a handle.

Dropping a handle ends nothing. If you are done sending before your body is
done working, `bus.close()` ends the bus now, and always as finished. To end
it failed, return the error from your body.

A reader finds out once its cursor runs out. `next()` returns `None` at the
end either way, so ask `bus.end()`:

```rust
while let Some(msg) = cursor.next().await {
    // ...
}
if let Some(StreamEnd::Failed { error }) = bus.end() {
    weft::node_bail!("the stream broke off: {error}");
}
```

`end()` is `None` while the bus is open. What a failed end means is up to your
node. If it answers a caller from a bus, end the answer with `fail` (go and
read [talking to a live caller](live-callers.md)).

## Choosing options

```rust
use weft::bus::{BusOptions, BusPayloadKind};

BusOptions {
    journal: ctx.journal_policy()?,
    window: Some(256),
    payload: BusPayloadKind::Bytes,
    ..BusOptions::default()
}
```

| Field | Default | What it decides |
|---|---|---|
| `journal` | `JournalPolicy::default()`: journaled, one-second window | What the record keeps of this bus and how often it writes a row. `ctx.journal_policy()?` hands over what your node's users set, `JournalPolicy::ephemeral()` keeps payloads out whatever they set (what `ElevenLabsSpeak` does) |
| `window` | `None`, which is 64 messages | How far back a reader can reach |
| `payload` | `BusPayloadKind::Json` | JSON or bytes. Frozen at creation; sending the other shape is refused |
| `meta` | `Value::Null` | Details every reader can read off the marker: a sample rate, dimensions. Keep it small, it rides the marker |

If you want your node's users to choose between the two modes and pick the
window, set `features.journalSettings` in its `metadata.json` (go and read
[features](metadata.md#features)) and pass `ctx.journal_policy()?` as above.
For what each mode keeps and what the window does, go and read [two
modes](../language/streams-and-buses.md#two-modes).

## What a send tells you

Failures are values, never silent drops: the bus was closed, you never
registered, you sent the wrong payload shape, or the record is degraded.

## A bus dies with its worker

A bus is not rebuilt when a worker restarts: a node that reads its port
afterwards fails with `no bus with id ... is live in this run`. Anything that has to survive goes in
[storage](storage.md), or out as an ordinary output of a node that finishes.
