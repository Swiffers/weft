# Custom types

Give a shape a name in a node's `metadata.json`, and every node in the project
can use it.

```json
"types": {
  "ChatMessage": "{ role: String, content: String }",
  "ChatHistory": "List[ChatMessage]"
}
```

Then in the ports of any node in the project, including nodes from other
packages:

```json
"inputs": [ { "name": "messages", "type": "ChatHistory", "required": true } ]
```

A named type is only equal to itself. Declare `"CustomerId": "String"` and a
port that wants a `CustomerId` refuses a plain `String` unless the program puts
a `Cast` in between, while a `CustomerId` goes into a port that wants a
`String` with nothing in between. For how names compare and what fits into what, go and read
[types](../language/types.md#giving-a-type-a-name).

## One table for the whole project

Every `metadata.json` in the project adds its `types` to one table before any
port is read, and the program's own `.weft` files can use these names too. If
several nodes in a package share a type, declare it once in the package root's
`metadata.json`.

Two files may declare the same name with the same shape: the two declarations
count as one. If two `metadata.json` files declare it with different shapes,
the nodes of both are left out of the catalog, each with an error saying the
name is declared twice with different bodies.

A `type` written in a `.weft` file is stricter: it may never reuse a name the
program can already see, including the names `metadata.json` files declare,
even to declare the same shape.

## Records that take extra keys

A record refuses any key it does not declare. If your node's type describes
data that comes from outside and may carry more than you read (a provider's
answer, a webhook's body), end it with `..`:

```json
"types": { "Charge": "{ id: String, amount: Number, .. }" }
```

Such a record accepts a value with extra keys. weft drops them when your node
emits the value and when a `Cast` produces it, so what flows on carries only
`id` and `amount`. A missing field or
a field of the wrong type still fails.

## What travels

The name carries its shape with it. A port typed `ChatMessage` is stored as
`ChatMessage={role: String, content: String}`, so anything reading a saved
project or a journal row understands it with no table to consult, and a run
that started yesterday keeps the shape it started with even if you edit the
declaration today.

If you write that stored form into a port's type yourself and its shape
differs from the declaration (`"type": "ChatMessage={role: String}"`), the
build fails with `named-type-conflict`.

## Making your node a cast

If your node converts its input into whatever type the program gives its
output, the way `Cast` does, name the two ports here:

```json
"features": { "castPorts": { "input": "value", "output": "value" } }
```

Where the node is used, the pair of types its two ports end up with has to be
a conversion `Cast` knows (for the table of those, go and read
[Cast](../language/types.md#cast)), and any other pair is the `cast-not-allowed` error when the program compiles:

```text
no cast from JsonDict into Number; a cast parses text, stringifies data, or
validates an object shape against a declared type
```

Your node does the conversion itself when it runs, the way `Cast` does:

```rust
let target = ctx.output_type("value")?;
let cast = target
    .cast_value(&value)
    .node_err(format!("casting the value into {target}"))?;
```
