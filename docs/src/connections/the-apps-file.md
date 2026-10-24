# The apps file

`access-apps.json` is where the operator puts the applications and keys this
installation offers as one-click connections. Whatever is in it shows up in
everybody's connect panel.

Start from the example and keep what you want:

```bash
cp access-apps.example.json access-apps.json
```

Fill in the services you expect to use often. A service you set up here is one
click forever after, and one you skip asks each person to bring their own the
first time they need it, which is fine for something you touch once.

## The shape

A service name maps to a **list**, always, even with one entry. Keys starting
with `_` are ignored, which is how the example file carries its own notes.
Every key weft defines is camelCase (`tokenUrl`, `signingSecret`), while
`client_id` and `client_secret` keep the spelling OAuth gave them.

```json
{
  "openrouter": [{
    "kind": "api_key",
    "label": "Runtime key",
    "key": "sk-or-..."
  }]
}
```

### A pasted key

| Key | What it is |
|---|---|
| `kind` | `"api_key"` |
| `label` | A name for this entry, unique within the service. A connection made with it is named after the service |
| `key` | The credential. At most one `api_key` per service |

This is what "use ours" hands out. Calls on it spend money on that account, so
keep the file private.

### An OAuth application

```json
{
  "slack": [{
    "kind": "oauth_app",
    "label": "Slack",
    "covers": ["chat:write", "channels:read", "channels:history"],
    "authUrl": "https://slack.com/oauth/v2/authorize",
    "tokenUrl": "https://slack.com/api/oauth.v2.access",
    "client_id": "...",
    "client_secret": "...",
    "events": { "signingSecret": "..." }
  }]
}
```

| Key | What it is |
|---|---|
| `label` | Unique within the service |
| `covers` | Exactly the permissions this app asks for, by their ids from the service's catalogue |
| `authUrl`, `tokenUrl` | The provider addresses this app signs in against, copied from the recipe |
| `origins` | Only if the recipe's own token calls run on another host than `tokenUrl`. Each entry is a bare origin, a scheme and a host with nothing after them, like `https://graph.instagram.com` |
| `client_id`, `client_secret` | From the provider. `client_secret` is absent for a public PKCE app |
| `events` | Only if this app receives pushes. Whatever the recipe verifies them with: `signingSecret` (Slack), `publicKey` (Discord, SendGrid), or for a signed identity token, `audience` and `pushEmail` |

**Permissions are fixed per app.** People pick an application rather than
ticking boxes, which is why you can list several for one service: a cheap tier
that avoids a provider's review process, and a fuller one beside it. Each shows
up as its own labelled choice with its permissions listed.

The addresses are pinned here, and a recipe that tries to substitute a
different sign-in address is refused. A connection through a registered app
only ever talks to the addresses you wrote.

That holds for a recipe's own token calls too, its `afterExchange` and its
`refresh`, because they run with the app's secret at hand. Each one has to go
to the origin of `tokenUrl` or to one listed in `origins`. Instagram swaps and
renews its tokens on `graph.instagram.com` while its `tokenUrl` is on
`api.instagram.com`, so its app lists that origin:

```json
{
  "instagram": [{
    "kind": "oauth_app",
    "label": "Instagram",
    "covers": ["instagram_business_basic", "instagram_business_content_publish"],
    "authUrl": "https://www.instagram.com/oauth/authorize",
    "tokenUrl": "https://api.instagram.com/oauth/access_token",
    "origins": ["https://graph.instagram.com"],
    "client_id": "...",
    "client_secret": "..."
  }]
}
```

Without it, the connect is refused naming the call, the address and `origins`.

You also have to register the callback URL the connect panel shows you as the
app's redirect URL, and re-register it whenever your public address changes.
Some providers, Slack among them, only accept https callbacks.

## Making it take effect

On your machine, weft reads the file each time it needs one of its keys, so an
edit takes effect at once. If the file is gone, weft has no shared apps or
keys until it is back. On a cloud install the file is the `WEFT_ACCESS_APPS`
secret of your fork: if you want to change it, update that secret and run
**install on GCP** again ([deploying to your cloud](../running/cloud.md#once-by-hand)).

If you want to keep it somewhere else, point `WEFT_ACCESS_APPS_FILE` at it
and run `weft daemon start`. If the file is there but cannot be read or parsed, every lookup of its keys
fails with an error naming the file, because
shipping no apps quietly shows up later as a service nobody can connect on a
node the operator thought was configured.

## What it refuses

- A service whose value is not a list
- An entry with no `kind`
- Two entries with the same label in one service
- More than one `api_key` for a service
- An OAuth app with no `covers`
- A permission in `covers` that the service's catalogue does not have
- An `origins` entry that is not a bare origin (a path after the host, a
  query, a user name)
- An `events` block on a service with no webhook transport, or a key in it
  weft does not know
- A key an older weft spelled snake_case (`auth_url`, `token_url`,
  `events.signing_secret`, `events.public_key`, `events.push_email`,
  `_setup_events`). The error names each one, where it sits and its spelling
  now, so an old file tells you exactly what to rename
- Invalid JSON

## Apps a node ships

A node's own `metadata.json` can also ship a public application, with no
secret. It is not a one-click connection: the "Your own" page signs in with it
when the person leaves every app field empty. For what it may carry, go and read [accessApps](../nodes/metadata.md#accessapps).

If your application has a secret, put it in this file instead, or paste it on
the "Your own" page when you connect.

## Encryption

Everything stored is sealed under `CREDENTIAL_ENCRYPTION_KEY`, which is 32
bytes in base64:

```bash
openssl rand -base64 32
```

Set it before you store a credential you care about. Without it weft uses a
public development key from its own source and warns you once, which protects a
database dump from nobody.

Keep it. There is no rotation: change the key and everything sealed under the
old one is unreadable until you put it back. Lose it and you reconnect
everything.
