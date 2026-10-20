# Packaging

Several nodes that belong together, sharing code and dependencies.

```text
slack/
  package.toml
  api.rs                shared code
  metadata.json         optional defaults for every node here
  access/
    metadata.json
    mod.rs
  send_message/
    metadata.json
    mod.rs
    deps.toml
```

`package.toml` is short:

```toml
[package]
name = "slack"

[dependencies]
# cargo deps every node here gets
```

Its presence is what makes the folder a package, and the members are found by
looking rather than listed.

## Shared code

Any `.rs` at the package root becomes a module, and a node reaches it as
`super`:

```rust
use super::api;
```

That is where the request-building and the error-reading for one service goes,
written once instead of in every node that talks to it.

Each package compiles as its own crate, so a node sees its own package's shared
files and nothing else. Another package's code is never on your path.

It also means editing one node recompiles that package and relinks, rather than
rebuilding the world.

A shared file whose name is not a valid Rust identifier fails at build time
naming the file.

## Defaults

A `metadata.json` at the package root is a partial document every node
inherits, key by key, with the node's own value winning.

Put `types`, `tags`, an `icon`, a `color` or a `service` recipe there once.

`type`, `label` and `description` can never be defaults. They are one node's
identity, and the compiler refuses a package file carrying any of them:

```text
package-level metadata.json must not set `type`: it is one node's identity,
not a package default
```

The merge is shallow: a node's `types` replaces the package's rather than
adding to it.

## When a folder does not load

If a node or a package has something wrong with it, only that folder is left
out. Every other node still loads, and a program that never uses the broken
one still compiles. A program that does use it fails with the reason:

```text
node 'Poll' failed to load: package name 'feed' is used by more than one
folder (nodes/rss/feed, nodes/mine/feeds); ...
```

Names have to be unique across all your node folders: a node type, a service,
and a package name (a package is named by its `package.toml`, a bare node by
its folder). If two folders claim the same name, both are left out and each
error names the other, so you pick which one to rename.

If you write a `package.toml` before any of its nodes, the package just waits,
and it loads once a node folder with a `metadata.json` and a `mod.rs` is there.
`weft describe-nodes` lists everything that was left out, and why.

## deps.toml

Per node, and every section is optional.

```toml
[dependencies]
reqwest = { workspace = true }

[system.runtime.apt]
default = ["ffmpeg"]

[system.build.apt]
debian_12 = ["pkg-config", "libssl-dev"]

[build.env]
SOME_PATH = "{{catalog_path}}/vendor"
```

| Section | What it is for |
|---|---|
| `[dependencies]` | Cargo crates this node needs |
| `[build-dependencies]` | Cargo crates for the package's `build.rs` (see [a build script](#a-build-script)) |
| `[system.build.<manager>]` | OS packages needed to **compile**, thrown away before the runtime image is sealed |
| `[system.runtime.<manager>]` | OS packages needed to **run** |
| `[build.env]` | Environment during `cargo build`. `{{catalog_path}}` expands to your node's folder inside the builder |

Managers are `apt`, `apk`, `yum` and `brew`. Under each, a key per distribution
like `debian_12` or `alpine_3_19`, or `default` for all of them. weft looks for
the exact key, falls back to `default`, and fails only when neither is there.

`[build.env]` only sets variables. If you need build logic, write a build
script.

## A build script

If your package needs one, put `build.rs` at the package root (for a node with
no `package.toml`, in its own folder). It runs when cargo builds your package's
crate, and the `[build-dependencies]` of every node's `deps.toml` in the
package are its dependencies. A `[build-dependencies]` section in a package
with no `build.rs` at its root fails the whole build, naming the package.

Your package is a library crate, so its build script reaches only what cargo
lets a library's build script reach:

| The script | It reaches |
|---|---|
| Prints `cargo:rustc-env=...` or `cargo:rustc-cfg=...`, or writes files to `OUT_DIR` | Your package's own code |
| Prints `cargo:rustc-link-lib=...` or `cargo:rustc-link-search=...` | The worker binary that links your package |
| Prints `cargo:rustc-link-arg=...` | Nothing: the worker binary never sees it |

So if your package links a native library, name it with `rustc-link-lib` and
`rustc-link-search` rather than passing raw linker arguments.

## When a package does not compile

If one package's Rust does not compile, its script fails, or one of its
dependencies does not build, weft builds the worker without that package rather
than failing the whole build. That needs cargo to print an error that traces
to the package: when it stops before blaming any (crates.io or the network
unreachable, the machine out of disk), the whole build fails and says so, and
building again once that is over tries every package. The command you ran (a `weft run`, say) still does its job if it does not
need that package,
and ends with a warning naming the package and the first error cargo printed:

```text
the worker was built without it, so nothing that uses its nodes runs: package
'media' didn't compile: error[E0308]: mismatched types; its full errors are in
the build log: <where the log is>
```

A run whose nodes never use the package runs as usual. A run, a trigger firing or a
trigger setup that needs one of its nodes is refused before it starts, with
the same reason. The package is built again once its files change, so fix
it and run the command again.

## What you get without asking

`weft`, `tokio`, `serde`, `serde_json`, `async-trait`, `anyhow` and `tracing`
are always there. So is `weft-providers`.

`weft` also re-exports `serde_json`, `reqwest`, `reqwest_middleware`,
`async_trait` and `inventory`, so you can name those types without declaring a
dependency on them.

## Local images

An infra node's containers are built from Dockerfiles in its own folder:

```text
postgres/database/
  metadata.json       "images": ["images/credential"]
  mod.rs
  images/credential/
    Dockerfile
    bootstrap.py
```

The path is relative to **the node's own directory**, not the package root, and
the last segment becomes the name you reference in the spec.
