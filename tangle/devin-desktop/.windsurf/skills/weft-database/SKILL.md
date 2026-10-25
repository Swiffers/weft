---
name: weft-database
description: "Read when a cloud install needs its database: the user has no Postgres for it, or asks which to use. Gets one that scales to zero (Neon), with the user only signing up and pasting one key, and puts its addresses in the fork's secrets. Part of weft-cloud-install."
---

# A database for a cloud install

A cloud install keeps everything in a Postgres it reaches by address, read
from the fork's `WEFT_DATABASE_URL` secret. Any Postgres works; if its
address goes through a pooler, the install also needs a direct address of the
same database in `WEFT_DATABASE_LISTEN_URL`.
When the user has none, offer Neon: a hosted Postgres that sleeps while
nothing uses it, with a free plan. Before they sign up, ask how busy the program will be
and pick the plan with them (Which plan, below). If they would rather use one they already
pay for, take its address instead and skip the rest of this.

## What the user does

Two things, in their browser:

1. Sign up at `https://console.neon.tech/signup`. They need no project
   there: you make it.
2. Make an API key for their organization: in the Neon console, the
   organization's **Settings**, **API keys**, **Create new API key**, and
   paste it to you once. A key made there names the organization by itself,
   which creating a project needs.

The key is a secret. Put it straight into `NEON_API_KEY` in the same command
that uses it, never into a file, and never repeat it in the chat. Say both
steps plainly, and wait until they are done.

## What you do

Make the project through Neon's API. The region is `aws-us-west-2` when the
install is in GCP's `us-west1` (the free-tier default, read weft-cloud-install
for why); otherwise the Neon region nearest the install's GCP region, by its
id (Neon lists them at `https://neon.com/docs/introduction/regions`). Run all of this as one Bash call:
each call starts a fresh shell, so the key and the file name would not
survive into a second one. The `trap` deletes the file holding Neon's answer,
which contains the database's password, when that shell ends:

```bash
NEON_API_KEY='<the key>'
neon_json=$(mktemp)
trap 'rm -f "$neon_json"' EXIT
curl -sS https://console.neon.tech/api/v2/projects \
  -H "Authorization: Bearer $NEON_API_KEY" -H "Content-Type: application/json" \
  -d '{"project": {"name": "weft", "region_id": "<a region id>", "default_endpoint_settings": {"autoscaling_limit_min_cu": 0.25, "autoscaling_limit_max_cu": 1, "suspend_timeout_seconds": 300}}}' > "$neon_json"
jq -e '.connection_uris[0]' "$neon_json" >/dev/null || { cat "$neon_json"; exit 1; }
direct=$(jq -r '.connection_uris[0].connection_uri' "$neon_json")
pooler_host=$(jq -r '.connection_uris[0].connection_parameters.pooler_host' "$neon_json")
printf '%s' "$direct" | sed "s/@[^/:]*/@$pooler_host/" | gh secret set WEFT_DATABASE_URL --repo <fork> &&
  printf '%s' "$direct" | gh secret set WEFT_DATABASE_LISTEN_URL --repo <fork>
```

The `default_endpoint_settings` start the database at the smallest size (0.25
compute unit), let it grow to 1 under load, and put it to sleep after 300
seconds with no queries. Without them Neon picks the size itself, on some
accounts a fixed 1 compute unit, which bills about $77 a month if it never
sleeps.

If it printed Neon's answer and stopped, that is Neon refusing, and the answer
says why (a wrong region id, a limit of the plan). `WEFT_DATABASE_URL` gets the
pooled address (the direct one with its host swapped for `pooler_host`), which
weft does its work on. `WEFT_DATABASE_LISTEN_URL` gets the direct one, because
a pooler cannot hold a `LISTEN`. If `gh` printed an error, the project exists
but one or both secrets are missing (when `WEFT_DATABASE_URL` failed, the
second was never tried): do not run the block again, which would make a second
project. Check which are set with `gh secret list --repo <fork>`, have the user
open the project's **Connect** dialog in the Neon console (the pooled address,
or the direct one with pooling off), and set each missing one with
`gh secret set <NAME> --repo <fork>`, piping in its address. If neither printed anything, both secrets are set: tell the user the
database is ready, and in one or two plain sentences what they got. It runs at
the smallest size (0.25 compute unit), grows up to 1 under load, and sleeps
after 5 minutes with no queries; Neon bills only the hours it is awake, about
$0.11 per compute-unit-hour on the paid Launch plan, and the free plan gives
100 compute-unit-hours a month. If they want it bigger or never asleep, those
are settings of the compute in the Neon console (**Branches**, the compute,
**Edit**), and either one costs more. Then go back to weft-cloud-install.

## What it costs

Neon's free plan gives a project 100 CU-hours a month, which at 0.25 CU is
about 400 hours awake. The database sleeps 5 minutes after the last thing that
used it, and an idle install wakes nothing: weft books a wake-up only for a
moment something is due (a queued event's retry, or the moment a trigger
taken down with a time limit starts refusing what arrives), and its
services hold no busy connection to the database once quiet.
Infrastructure that runs fine lets it sleep too, because a machine reports
only when its state changes, and so does a run waiting on a person or on a
caller who says nothing (a run waiting on a silent caller still keeps its
worker up, and that worker is billed). When the month's hours run out, the
database stops until the next month or a paid plan, and the install stops
with it.

A trigger that keeps a connection open lets the database sleep too. Its
holder hears about changes over Pub/Sub (the install's `weft-holders`
topic) and proves it is alive by pushing a Cloud Tasks check ahead of itself
every 60 seconds, and none of that touches the database. It still costs its
Cloud Run worker pool while it runs. The database wakes when an event
arrives on that connection, or when the connection drops or comes back (a
drop or a return is written once, not on every reconnect attempt).

What wakes it is something happening: a run doing work (its worker reports
every second), a timer or schedule the program set coming due, a poll
that finds something new (a poll that finds nothing touches no database at
all, and a poll that signs in reads its credentials when weft starts the
part that runs polls, and again when they are about to expire, when the service refuses them, or
when the connection changes), an outside event (a route
called, a webhook, an event on a held connection), or a person's command.
Two things keep it awake for as long as they are open, because each holds a
live stream to the install: `weft follow` while it runs, and a VS Code editor
pointed at the install while the person uses it. Once the editor's window
has been out of focus or untouched for 5 minutes, it closes its live
connection so the install can sleep, and picks back up when they return.

## Which plan

Neon's free plan caps two things a month: 100 CU-hours awake, and 5 GB of
data sent out of the database. weft reaches its database over the internet
(the install runs on Google Cloud, Neon on AWS or Azure), so everything it
reads counts against those 5 GB. When either one runs out, Neon suspends the
database until the next month or an upgrade, and the install stops with it:
every call answers 503 with Neon's own message saying the quota is exceeded.
Nothing is deleted.

So before they sign up, ask how busy the program will be. If it is mostly
idle (triggers that fire a few times a day, nothing held open), Neon's free
plan fits, and it stays the default. If it runs schedules every few minutes,
keeps a connection open (a chat or messaging listener), or a site calls it
all day, the database rarely sleeps and the transfer adds up: before the
install is made, offer the install a Postgres of its own on Cloud SQL, in its
own region and network, with no transfer quota and no trip across the
internet. It bills every hour it runs, even while nobody uses the install
(Google's Cloud SQL pricing page has the price for the region), so it is the
user's yes. On a yes you make no Neon project: set the fork's repository
variable `WEFT_DATABASE` to `cloud_sql` (`gh variable set WEFT_DATABASE --body
cloud_sql --repo <fork>`), leave the `WEFT_DATABASE_URL` secret out, and the
install workflow makes the database itself. Its size is the variable
`WEFT_CLOUD_SQL_TIER`, `db-g1-small` (a small shared machine) when unset.

An install already on Neon can move to its own Cloud SQL later, with its
data: `weft maintenance on --on <target>`, the same `WEFT_DATABASE` variable,
the install workflow run again (it makes the database, copies everything
into it, and only then points the install at it; until the install uses
it, every run copies again from scratch, so a failed one leaves the install
where it was and running the workflow again starts over), then `weft
maintenance off --on <target>`. Offer it when an install's program has grown
busy. A Neon project suspended on a quota cannot be copied from until it
answers again: upgrade it to Launch first (or wait for the month to turn),
then move. Once the user
is happy with it, the `WEFT_DATABASE_URL` secret and the Neon project can go.
