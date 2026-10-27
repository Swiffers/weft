# What a node is

A folder with two files in it.

```text
word_count/
  metadata.json    what it takes and what it gives back
  mod.rs           what it does
```

The `metadata.json` is what makes the folder a node. Put the folder anywhere
under your project's `nodes/` or `src/` folder and weft finds it. The two
folders make one catalog, so a name (a node type, a service, a package) has to
be unique across both.

| File | Needed | What it is |
|---|---|---|
| `metadata.json` | Yes | The declared surface: ports, types, settings. Go and read [metadata.json](metadata.md) |
| `mod.rs` | Yes, to run | The Rust: one `impl Node` |
| `deps.toml` | No | Extra crates, system packages, build environment |
| `tests.rs` | No | Its own tests. Go and read [testing a node](testing.md) |
| `images/<name>/Dockerfile` | Infra nodes only | A container this node needs built |

## A folder with metadata and no code

That is not an error. weft finds it, warns that it has no `mod.rs` yet, and leaves it
out of the build. A program that never names it builds fine, and a program that
does is told which folder is waiting for its Rust:

```text
node type 'WordCount' is not ready yet: node 'WordCount' at <project>/nodes/word_count
has no mod.rs yet; it is left out until it does
```

So you can write the `metadata.json` first and the Rust later.

## Packages

Drop a `package.toml` in a folder and it becomes a package. Every subfolder
holding a `metadata.json` is then one of its nodes, found automatically. There
is no list to maintain.

```text
slack/
  package.toml        [package] name = "slack", plus shared dependencies
  api.rs              shared code, reached from a node as `super::api`
  metadata.json       optional defaults every node here inherits
  access/
    metadata.json
    mod.rs
  send_message/
    metadata.json
    mod.rs
```

For the shared code, the defaults and the dependencies, go and read
[packaging](packaging.md).

## The trait

```rust
#[async_trait]
pub trait Node: NodeManifest + Send + Sync {
    async fn run(&self, ctx: RunContext) -> WeftResult<()>;
    // everything below has a default
    fn node_type(&self) -> &'static str;
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag) -> WeftResult<InfraSpec>;
    async fn setup_trigger(&self, ctx: RunContext) -> WeftResult<()>;
    fn tests(&self) -> Vec<NodeTest>;
}
```

`run` is the only one you have to write.

`provision_infra` and `setup_trigger` have defaults that fail loudly, naming
your node, if your metadata said you implement them and you did not. So
`requiresInfra: true` without a `provision_infra` is an error you get told
about when the infra is provisioned, not a silent nothing.

You never check which phase you are in. The engine reads your metadata and
calls the right body. For a plain node that means `run` is called in every
phase, including while a trigger is being set up, because a value feeding a
trigger's settings has to be produced then too. For a trigger, `run` only fires
on a real event, with the payload on `ctx.wake`.

## How it gets into the binary

There is no registration file: the compiler generates the lookup table from
the folders it finds.

It only compiles the packages your program uses, each as its own cargo crate.
For what that buys you, go and read [shared code](packaging.md#shared-code).

Your program's definition is not baked into the binary. The worker fetches it
by hash at run time, which is why rewiring your graph without touching any Rust
reuses the worker already compiled (a program on your machine, an image on
a cloud install).

## The one macro

```rust
#[derive(NodeManifest)]
pub struct TextNode;
```

That reads the `metadata.json` sitting beside your file and attaches it to your
type. It reads the package defaults too, so what your code sees at run time is
the same merged document the compiler saw.

It checks at compile time that the file is there, is valid JSON, is an object,
and has a string `type`. Editing the JSON rebuilds the crate.

The struct's name is how it finds the file, so `TextNode` has to live in the
same folder as its metadata. By convention the struct is the node's `type` with
`Node` on the end.

Next, [write one](your-first-node.md).
