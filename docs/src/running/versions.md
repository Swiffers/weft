# Versions, seeds and frozen examples

Every run records the code it ran, so you can go back and look, run something
again against a change, and see what moved.

## A version

A snapshot of your project's files, taken whenever you run, activate, or type
`weft checkpoint`.

| In it | Not in it |
|---|---|
| Every `.weft` file, at any depth | Anything hidden at the top: `.git`, `.env`, `.weft` |
| `weft.toml` | `layouts/`, because dragging a box is not a change to your program |
| `src/**` and `nodes/**`, including a node that sits beside your code and the standard library in `nodes/base_catalog/`, edits and all | `target/` at the root, and `node_modules` anywhere |
| `assets/**`, `prompts/**`, `scripts/**`, `sql/**` | `front/`, which ships through your project's CI |
| `examples/**`, so going back restores the examples that existed then | |

A file identical to one an earlier version held costs nothing to store, so
the standard library is stored once and every later version reuses it. An
install builds a version from exactly the files it holds and nothing of its
own: if you deleted `nodes/base_catalog/`, the version has no standard
library.

## head

**head** is where you are: the version your next checkpoint sits under, and the
run a `--seed` inherits from.

| Command | What it does |
|---|---|
| `weft tree` | The whole tree: every version with what changed against its parent, its runs beneath it, head marked |
| `weft checkpoint [<label>]` | Records the files as a version. No run, no build |
| `weft branch <version>` | Puts those files back and moves head |
| `weft branch <run-id>` | The same, and points head's run at that one, so the next seed inherits from it |

`weft branch` refuses on a dirty tree and names the files, rather than throwing
your work away:

```text
the tree has changes since head a1b2c3d:
  src/main.weft
`weft checkpoint` keeps them as a version, or `--discard` throws them away
```

## Seeding

```bash
weft run --seed
```

Reuse everything from head's run whose slice of the program did not change, and
run only the rest. That is the loop when you are iterating on node nine of a
twelve-node program and nodes one to eight cost real money.

If you want to reuse a different run, name it: `weft run --seed=<run-id>` takes
any finished run of the project, including one a trigger started or one that
failed. Keep the `=`: `weft run --seed <word>` reads the word as the name of
a saved example, not as a run. A run that is still going is refused until it
ends or you `weft stop` it.

If a node that runs again waited for somebody in that run, the answers they
gave it are replayed, so it carries on instead of asking again. That only
holds while the node's own code is unchanged; if it changed, the node asks
again and weft says so:

```text
'review' runs again and asks again: its code changed since the seed run answered it
```

### What will not be reused

| | Why |
|---|---|
| Anything whose code or dependencies changed | It is a different thing now. weft says so by name |
| Anything downstream of something that changed | Its input is different |
| Anything that never finished: failed, cancelled, still running, or parked on somebody | There is no result to reuse |
| Anything that emitted a live handle, like a bus | The handle belongs to a worker that is gone |
| A whole loop, if any iteration of it is invalid | A loop is reused whole or not at all |
| Anything downstream of an explicit `--emit` or `--fire` | You asked for a fresh event, so its consumers are fresh |
| Anything past `--seed-before` or `--seed-until` | You said where to stop |
| A starting value you supplied that differs from the one the earlier run used | Different input, different work |

The invalidation spreads: if a node is not reusable, nothing downstream of it
is either.

### When a reused node's file changed

A saved result that names a stored file says what the file held then. If a
node that runs again would read it, and the file has been edited since (a
later node added to it, another run changed it) or is gone, the run is refused
before anything starts. A run that reuses the node and runs nothing that reads
the file goes ahead:

```text
--seed would reuse what 'ask' produced on 'historyFile', but the file it names,
'conversation.json', has changed since (that run left it at version 4, it is at
version 6 now), so the saved result no longer says what that node made. Hand
the file in yourself (`--emit ask='{"historyFile": ...}'`), or run 'ask' again
by keeping it out of the seed (`--seed-before ask`).
```

### The one that surprises people

A `--from` start is reused like any other node. If its code and the values you hand it
are what they were in the seed run, it keeps that run's result and does not
run. The run reports success and nothing ran, which is correct and invisible,
so weft says it out loud:

```text
'classify' keeps the result run 3f2a1111-0c4e-4b1a-9d2e-5a7b8c9d0e1f gave it:
its code and what it is handed are as they were. Run without --seed to run it again.
```

## Frozen examples

An example is a run you keep: its starting values, the answers people gave, and
the outputs you accept as right.

```bash
weft run --from classify='{"text":"..."}' --save angry-customer
weft freeze angry-customer
```

`--save` writes the starting parameters. `freeze` takes a run that completed
and records its accepted outputs alongside them. If a saved file still uses a
spelling an older weft wrote (`frozen_from`, `gave_up`, `keep_for`), it still
reads, and reading it leaves the file as it is. It takes today's spelling the
next time weft writes it, with `--save` or `freeze`.

Then, after a change:

```bash
weft run angry-customer
weft diff <new-run-id> example:angry-customer
```

`diff` shows what moved on the wires. You, or Tangle, judge whether the change
is acceptable. Freezing the new run replaces what is accepted.

`--expect <node>` marks the outputs that matter, so a later diff leads with
them rather than burying them.

| Command | What it does |
|---|---|
| `weft examples` | What is saved, which are frozen, and each one's latest run |
| `weft freeze <name> [<run-id>]` | Freeze a run. Without a run id, head's run |
| `weft diff <left> <right>` | Compare two runs. A side is a run id or `example:<name>` |
| `weft run <name>` | Run the current code with that example's parameters |

A freeze **replaces** the example whole, and says so. Only a completed run can
be frozen: a failed or cancelled one has nothing to accept.

## Cleaning up

```bash
weft prune <version>
```

Deletes that version, every version under it, and every run beneath them.

It refuses on head's version, on any version a frozen example came from, and
while any run underneath is still going.
