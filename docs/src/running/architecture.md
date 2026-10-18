# How the runtime is built

The runtime has four roles, each with one job.

| | What it does | What it never does |
|---|---|---|
| **Worker** | Runs your compiled program | Touch Postgres: it goes through the broker |
| **Dispatcher** | Decides what runs, and answers every request about a project or a run | Run your node code |
| **Listener** | Holds the timers, the open sockets and the subscriptions | Run your node code, or know which project it serves |
| **Supervisor** | Creates and watches the machines and containers your program asked for | Run your node code, or change a project's infrastructure while another supervisor is changing it |

The dispatcher, listener and supervisor, together with the broker, are parts of one
binary, `weft-runtime`, and on your machine one `weft-runtime` process runs
all of them. On a cloud install each is a Cloud Run service of its own,
which scales with its own load and down to zero between calls. Triggers that
keep a connection open run separately, on holders: for how they work, see
[the listener](#the-listener), and for what they cost, see
[what you get](cloud.md#what-you-get). Workers are always
separate: one per program, started by the runtime.

Anything that has to survive a crash goes into Postgres, and every role reads
it back from there.

```mermaid
flowchart TD
    UI["CLI / editor / webhooks"] --> D["Dispatcher"]
    D --> L["Listener"]
    D -->|"run this, hand it this event"| W["Worker: your compiled program"]
    C["Live caller"] --> W
    L -->|"an event"| W
    D -.->|"queued work"| S["Supervisor"]
    D --> PG[("Postgres")]
    L --> B["Broker"]
    S --> B
    W --> B
    B --> PG
```

## Where a run lives

Whether a run survives its worker dying is set by its trigger's `durable` input,
or for a run you start by hand, by `weft run --durable` or `--fast`. For
what it does, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept), and for when each
kind of run waits for the database, [when a run waits for its
writes](the-journal.md#when-a-run-waits-for-its-writes).

Once a run has written anything, it has one row in the database, and the row
says where the run is: waiting for a worker, running on the worker it names,
parked on a wait with no worker, or ended.

When a trigger starts a run (a caller on a `Route` or a `Socket`, or an event
such as a timer, a subscription or a webhook), the run starts on the worker
that took the call or the event, and its row names that worker from its first
write. Every other run starts as a queued row: a run you start with
`weft run`, a paused run picking back up, a run another worker handed back,
and the setup runs that `weft activate` and `weft infra start` make. The
dispatcher hands a queued run to a worker, which claims it (the row names that
worker from then on) and carries it on from its record.

If the worker is still starting, that is no fault: the run waits 40 seconds
and joins that same start.

If the worker answers that it is busy (its own `503`: its memory is nearly
full, or it is stopping), that costs the run no try. It waits and is handed
out again, for as long as the workers keep answering busy. Each wait is as
long as the run has been turned away so far, at least 5 seconds and at most
a minute.

If the workers keep that up for longer than the install's alert delay
(`alert_after_minutes`, 5 minutes by default), `weft status` lists it under
`waiting on weft:`, ending in ``this project's worker has answered busy for
<time> (memory full); raise its memory (`weft workers set --memory
<size>`)``, and on Google Cloud the install sends an alert email. The line
goes away once none of the project's queued runs is still waiting on its
workers (answered busy, refused, or not reached at all) and a run of the
project has since left the queue: either the last one that was waiting
(it reached a worker, it ended, or you cancelled it) or one started after
the last failure. Changing the project's worker settings clears it too, and
tries every queued run again from the start.

Whether an answer counts as a refusal depends on who gave it:

- The worker: any error it answers other than its busy `503`.
- The platform, answering in the worker's place: any `4xx` except `404`,
  `408` and `429`. A `404`, `408`, `429` or `5xx` from it is no answer.
- Google Cloud, when weft asks it to create the worker: a `400`, `401` or
  `403`. A `429`, a `5xx` or a timeout is no answer.
- weft itself, when the project's worker settings together are a shape no
  platform could run.

The run is handed out again 5 seconds after the first refusal and 20
seconds after the second, and the third cancels it, with a reason naming
who said the last no: `its worker refused it 3 times: <error>`, `the
platform refused weft's calls for it 3 times: <error>`, or `its project's
worker settings kept it from any worker 3 times: <error>`. The `<error>`
says what the last no was: the worker's or the platform's status and
answer, or which worker setting no platform could run. If nothing answers at
all, the run is handed out again 5 seconds later, then 20. If the third
try gets no answer either, the run is not ended: it stays queued and waits
until a worker of its project comes alive or a build of the project
registers, and then its tries start over. Either way, `weft status` shows
the error under `waiting on weft:`. If you change the
project's worker settings (`weft workers set` or `reset`), every run still
queued is handed out at once, with its tries forgotten.

If a trigger starts a fast run and its worker dies straight away, the run may
never show up in `weft runs`, because a fast run's record trails a few
milliseconds behind it and may not have reached the database yet.

## Where a call goes

If a caller uses the project's own address, it reaches your program's worker
straight, with nothing of weft's in between. On your machine the worker running the project's current program
opens the project's own port itself, so `http://127.0.0.1:14200/cards` is the
worker answering. For how that port is picked, go and read [answering on a
URL](../language/triggers-and-routes.md#answering-on-a-url). On a cloud install
each project is a Cloud Run service of its own, open to every caller, and that
service's address is the project's address. A `weft domain add --for api`
domain sends its calls straight to that service. `weft activate` prints the
address, and `weft status` shows it. For how a browser opens a socket on a
route that checks callers, go and read [answering on a
socket](../language/triggers-and-routes.md#answering-on-a-socket).

The address an install shares between its projects (`/connect/local/<project id>/...`,
the tunnel's hostname, the install's own domain) still works: the dispatcher
picks the project by the id in the address and the program by the route, from
routes it holds in memory, and passes the call on. It adds headers giving the caller's address and
the `Host` they sent, and the worker trusts those headers only
when the call carries weft's own key. The dispatcher checks nothing else
and writes nothing.

Once a call reaches the worker, the worker does the checking: which route the
call is for, whether its trigger is on, the instance it names and the route's
limits on how many calls it takes, and it asks the broker to check the route's
credentials when the route asks for one. The worker holds the project's routes
in memory, and the broker tells it the moment one changes. Unless the route
asks for a credential or the call carries an instance token (which names the
[instance](instances.md) the call is for), nothing reaches the database before
the run starts.

If a route's trigger is parked, hibernating within its grace window, or still
being set up, the worker answers `503` with a `Retry-After` at once, so a client that retries
gets through once the trigger is on. When a new version of the program
replaces the worker, on your machine the port stays closed while the old
worker hands its runs back or ends them (10 seconds at most, then it is
killed) and the new one starts. A caller
arriving then is refused, as with any server restarting.

If a route still belongs to another version of the program than the one at
the project's address (a new version is taking over), the call gets the same
`503` and `Retry-After`, and lands on the new version once it holds the
route.

## What happens when an event arrives

When a timer fires or a subscription delivers something, the listener holding
it hands the event straight to the worker of its project, through the same
door callers use: the worker checks whether the trigger takes work now and
the trigger's limits, and works out what the run reads. If the trigger is
parked, it is past its limits, or what the run reads is not ready (its
infrastructure is down, its instance has not given a value it needs), the
event waits in the trigger's queue, and the dispatcher hands it over again
once it can run. Otherwise the
worker starts the run as a fast or durable run, whichever its trigger asks
for, and writes what happens into [the journal](the-journal.md) as it goes.

A webhook or a provider's push arrives at the install's own address: the
dispatcher has the listener read it, then hands it to the worker the same
way. An answer to a run that waits (a form, a person's reply) goes through
the dispatcher, which resumes that run. When the listener cannot reach the
worker (the project has no address now, the call fails or goes unanswered),
the event waits in the trigger's queue the same way.

Whoever handed the event over keeps its call to the worker open until the
run ends, so the platform counts the run as work: Cloud Run keeps giving the
worker CPU, and a local install does not stop it as idle.

## The worker

A worker is one compiled program, serving many runs at once. It runs from an
image named after a hash of its contents, so two projects that compile to the
same thing share an image.

On your machine a worker is a Docker container, one per project and build of its program;
changing the project's worker settings (`weft workers set`) starts a fresh one
for the calls that follow. The one at the project's address runs for as long
as any of the project's triggers takes work. Any other starts when a run needs
it and stops after thirty seconds with nothing to do, unless the project keeps
copies warm with `weft workers set --min-instances`. To change the thirty seconds, set
`platform.workerIdleStopSeconds` in the install's `config.json`
(`~/.local/share/weft/config.json`) and restart the daemon.

On a cloud install a worker is a Cloud Run service. Cloud Run starts more
copies of it when calls pile up and removes idle ones when it decides to,
down to none, so a project nobody calls costs nothing. If you keep copies
warm with `--min-instances`, those are billed even while idle. If you want to
change how many calls Cloud Run sends one copy at once (`--concurrency`), the most
copies Cloud Run may start (`--max-instances`), or a copy's CPU and memory
(`--cpu`, `--memory`), `weft workers set` changes them for one project. On
your machine none of these four does anything: there is no Cloud Run sending
calls or starting copies, and weft puts no CPU or memory limit on a worker.

On a cloud install, if a run keeps working after its caller has gone (a Route
with `outlivesCaller` that replies and carries on), the nodes it was running
when the caller left finish on that copy, and the rest of the run moves under
a call weft holds open (for how, go and read [how a run is
kept](../language/triggers-and-routes.md#how-a-run-is-kept)). Cloud Run
throttles the CPU the moment the answer goes out, so if those last nodes are
heavy, turn on `weft workers set --cpu-always-allocated true`. With it, the
copy is billed for as long as it is up.

A thing a worker's runs share (`ctx.shared`, such as a pool of database
connections) lives in the worker too: it goes once no run has used it for
five minutes (`weft workers set --shared-idle-seconds` changes that), and at
the latest when the worker stops. For how a node asks for one,
go and read [the ctx](../nodes/ctx.md#sharing-something-between-runs).

A worker keeps, between runs, what its runs read of the program's
infrastructure and connections: where each piece answers, the connections the
broker handed it, and the ones an infrastructure node published. That rarely
changes between runs, so after the first run a call no longer asks the broker
where things answer or for its connections, and starts its run straight away.
Whenever one of those changes (a piece restarts somewhere else, a connection
is replaced), the broker tells the worker, which drops what it kept and asks
again. Two exceptions: a credential weft hands to a single run is never kept, and a
credential that expires is kept only until it is due a refresh.

If a run may need more than an hour on a cloud install, go and read [how a
run is kept](../language/triggers-and-routes.md#how-a-run-is-kept).

## When a worker is full

A worker takes a set number of calls and events at once. Unless you set it,
that is one for every MiB of the worker's memory (on your machine, the
machine's), and never fewer than 64. If you want another number for a project, `weft workers set
--max-runs-at-once` sets it. A run handed to the worker from the queue (a `weft
run`, a resume) is not counted.

If a call arrives while the worker is full, it waits up to 30 seconds
(`--max-queue-wait-seconds`) for a run to end, before any of its body is read.
At most as many calls can wait as the worker takes at once. A call that waited the whole time, or found no room to wait,
gets a `503` saying `This worker is busy right now; try again in a moment.`,
with a `Retry-After`. An event waits the same way, and one that waited too
long goes in its trigger's queue, where it waits until a worker has room for
it.

If the database falls behind, runs take longer to end (for why, go and read
[when a run waits for its writes](the-journal.md#when-a-run-waits-for-its-writes)),
the calls queued behind them wait longer, and one that waits past
`--max-queue-wait-seconds` gets the same `503`.

The same `503` comes back while memory is more than 90% full, because one run
too many would get the whole worker killed, and every run on it with it. On a
cloud install that is the copy's own memory; on your machine it is the
machine's, whatever is using it. The runs already on the worker carry on. If
you see it often on a cloud install, lower `--max-runs-at-once`. Cloud Run
never sends one copy more than `--concurrency` calls at once (80 unless you
change it), so a lower `--max-runs-at-once` only helps if it is below that. You
can also lower `--concurrency` itself, or give each copy more memory with
`weft workers set --memory`. On your machine, free some memory or lower
`--max-runs-at-once`.

## The dispatcher

It never sees your `nodes/` directory. Your CLI reads your local catalog and
compiles the program before submitting it, so the dispatcher only ever handles
a compiled definition.

The next request may land on another copy of the dispatcher, so it keeps
nothing in memory that another copy would need.

What it does keep is a copy of the rows a call reads every time and that
rarely change: a tenant's routes, the install's domains, a project's worker
settings, which of its infrastructure is up, the connections the install
picked for it and what each instance provides. Every write to those rows makes
Postgres tell every copy of the dispatcher, which drops what it held and reads
it again on the next call, and anything it is about to refuse (no such route,
infrastructure not running) it checks against the rows first. While its
connection that hears those announcements is down, it keeps nothing and reads
every time.

The dispatcher's background work (handing runs to workers, answering what a
program asks of weft, registering triggers) waits on rows in the database, and
Postgres announces each one as it is written. A dispatcher that is up hears
that and starts at once, on your machine and on a cloud install alike. On a
cloud install, where the dispatcher can be at zero, whichever part of weft
wrote the work also calls it to start one.

If you are watching a run in the editor, or waiting on one to end, you hear
about it roughly 10 milliseconds after its rows are written. The broker gathers
which runs got rows and which ended, and tells the dispatcher in one statement
once the write has committed. It announces after the write rather than inside
it, because Postgres makes every write that announces something commit one at
a time, so announcing from inside each write would
line up every write in the database behind the others.

Some work has no write to announce it. A worker that dies just stops
renewing its lease, and only the clock can show that. So the dispatcher
works out the earliest moment a lease could run out, and looks again exactly
then. On your machine that is a timer in the process. Each piece of
background work there also looks on an interval of its own (15 seconds for
the one that notices dead workers, 30 or 60 seconds for most others) in case
an announcement was lost.

A cloud install has no such timer. The dispatcher writes the moment into the
database and books one Cloud Tasks wake for it. When the moment changes, it
books the new wake before it deletes the old one, so a crash at the wrong
time costs one wasted wake, never a missed one.

On a cloud install, if whoever wrote the work cannot reach the dispatcher,
it books a Cloud Tasks wake to call it instead, which Cloud Tasks retries
for up to ten minutes. Until a call gets through, `weft status` lists the
dispatcher under `waiting on weft:`. After ten minutes, the next write for
the dispatcher, or the next part of weft to start, calls it again.

If a piece of background work itself fails on a cloud install, weft books
it again 1 second later, then 5, then 20. If the fourth try
fails too, it stops trying and writes the error to `weft status`, under
`waiting on weft:`. It runs again the next time weft writes work for it,
and once more each time a copy of its service starts, and the line goes
once a try works. That last one is for a fresh install, where Google takes
a minute or two to apply the permissions the install was just given: work
that failed in that window runs again the next time something wakes the
service. Each failed try still answers Cloud
Tasks' call as handled, so the queue does not call it again on its own.
Cloud Tasks only retries a call whose failure could not even be written
down, because the database did not answer. On your machine a failed piece
of background work only logs the error, and runs again at its next
interval or the next write for it.

When a run a worker drives gets an answer or is cancelled, weft announces it
through the install's news (the `weft-news` topic on a cloud install), in
batches. If a batch is refused, weft tries it again after half a second, 2
seconds and 5 seconds, then marks it refused, and `weft status` shows `weft
could not tell the install's news (a cancel or an answer reaching a run a
worker drives); those runs hear of it late` under `waiting on weft:`. Each
time news is written, and when a weft process starts, weft first sends the
new news on its own, so refused news never holds it up, then tries the
refused news again one piece at a time, stopping at the first piece refused
again. The line goes once no refused news is left.

## The listener

One listener serves every project on the install. Each kind of trigger says
what it needs between two events, and the listener does exactly that:

- **A form or a webhook** needs nothing: it is called from outside, and the
  listener only turns the call into a message.
- **A schedule, a delay or a poll** needs a timer. It asks for its next wake,
  and the install's alarm delivers it (a table in Postgres on your machine,
  Cloud Tasks on a cloud install), even across a restart of the runtime.
- **A stream or a socket** needs an open connection, so it needs a process
  that stays up between events. weft calls that process a holder, and it runs
  the listener's code. On
  your machine that is the runtime's one process. On a cloud install the
  holders run in a pool, as many as those connections need and none when
  there are none. Each holder claims the triggers it holds, so no two
  holders hold the same one. A claim lasts until its holder lets go, so there
  is nothing to renew: a holder calls weft only when it has something new to
  say, and Pub/Sub tells it when the triggers to hold change. Since claims never run out,
  a holder that crashed would keep its triggers for good, so each holder
  keeps a dead man's switch: a Cloud Tasks check set three minutes ahead,
  which it moves forward every minute. A holder that dies stops moving it,
  the check goes off, and the broker frees everything that holder claimed,
  for another holder to take. A holder that cannot move its check in time
  lets go of everything and exits before the check goes off, so its
  triggers are never handed to another holder while it still holds them. A
  holder that is shut down gives its triggers up at once.
- **An event subscription** needs a holder only when it dials out to the
  service. On a cloud install, when the service can push the subscribed events
  for one account, weft subscribes with the service instead and renews that
  on a timer, so nothing stays open for it. A subscription to every account
  of your app always dials out.

If the wake of a schedule, a delay, a poll or a subscription renewal keeps
failing (the broker or the service does not answer, or the event it picked
up cannot be handed on), the alarm delivers it again, waiting longer each
time. The listener gets 20 tries at it, and a delivery uses one only when it
gets an answer other than a 5xx: one with no answer at all (the listener
down) or a 5xx (from the listener or from something in front of it) uses
none. After 20 failed tries the listener gives the trigger
up and stops waking it, and its node shows `its wake failed each of the 20
times the listener handled it, the last with: <error>`. If the last one did
what the wake was for but could not set the wake after it, the node shows
`its wake was handled, but the wake after it could not be set on the last of
the 20 times the listener handled it, so nothing wakes it again: <error>`
instead. On your machine the 20 tries take about an hour when the listener
answered every one, and up to about 95 minutes when an outage had already
stretched the wait between tries to its longest, 5 minutes. On a cloud install the count is `wake_attempts` in
`deploy/terraform/gcp/variables.tf`. If the broker answers that only a
person can fix the trigger's connection (it is gone, it needs reconnecting,
or the service refused what was asked, a missing permission for instance:
a 400, 404, 409 or 410), the trigger is given up at once.

A trigger that keeps a connection open (a socket, a stream, an SSE
subscription, a service's events, a webhook subscription it keeps renewed)
retries on its own when that connection fails, and its live panel shows
`reconnecting since <time>: <error>` meanwhile. If the failure is one only a
person can fix (the same broker answers, a credential refused right after it
was fetched, or a setup error that would fail the same way every time), it
stops instead: its live panel shows `stopped: <why>`, and the reason stays on
its node. A service that is busy or limiting how often you call it is still
retried.

If you want a stopped or given-up trigger back, fix what stopped it and
register it again. If it was its connection, reconnecting that connection is
enough: weft brings back every trigger that uses it, the ones that keep a
connection open included. If it was anything else, a setup error for
instance, run `weft resync` once it is fixed. `weft activate` does not bring
it back, because the trigger still counts as on.

A trigger that acts as one of your connections keeps that connection's
credentials for as long as it is registered. If you reconnect the
connection, rotate it or remove it, weft registers again each trigger that
uses it and that no holder holds, and tells the holders to drop what they
kept for it, so nothing goes on with the old credentials. A token refresh
does neither: what was kept already says when it stops being good, and is
fetched again before then.

If you want a new kind of trigger, you only write listener code: the listener
is the only role that tells kinds apart.

## The supervisor

It applies your infrastructure and watches whether what it created is
healthy. Each piece of infrastructure your program asks for (a database, a
model server) is a unit: on your machine a set of Docker containers, on a
cloud install a Compute Engine machine of its own, with its disks and GPUs.

Two supervisors changing the same project at once would overwrite each other,
so before it changes anything a supervisor takes an exclusive lease on the
project. It keeps renewing that lease while the work runs, and if it expires
another supervisor picks the project up. When a supervisor takes up
projects, it takes the ones with something due now first (a command waiting,
a retry whose moment came).

Inside the one supervisor that holds a project, the work on different copies
runs side by side: three instances started together come up together. Work on
the same copy runs in the order it was asked for, so a stop asked after a start
waits for that start.

A supervisor can still die between changing something and recording that it
did. The next one works out what to do from what is actually running, rather
than trusting the record.

On your machine the supervisor looks at the health of what it runs every
thirty seconds. On a cloud install nothing looks while everything is fine, so
infrastructure that runs fine costs no look at all and lets the rest of the
install sleep: the agent on each machine watches its unit right there, for
free, and asks for a look when how the unit stands changes (a container
stopped, a check started failing or passing again), and Compute Engine's own
record of a machine stopping or failing asks too. Once something is not fine,
the supervisor's next look is booked for the exact moment it needs one, such
as when a unit that stopped being ready would count as flaky. What the
supervisor does about an unhealthy unit (restart it, take down the triggers
that read it) is called a health protocol. If one fails, it is tried again
after 5 and then 10 seconds. At the third failure the supervisor gives up on
it and `weft status` lists it under `waiting on weft:`. It tries again once
the units report a change, once they are all healthy, once you start, stop,
upgrade or terminate the project's infra, or once you run `weft infra look`.

The supervisor's own pass over a project can fail too: it could not look at
the project's health, could not delete what the host still holds for copies
that are gone, or ran a command and could not write down how it ended. If
it could not list what the host holds (an error starting `could not list
what the host holds to delete what its gone copies left`) or could not read
which projects it owns (`could not read what it owns to look at its
health`), the whole pass fails, and every project it holds counts a failed
pass. `weft status` shows it under `waiting on weft:` as `weft's supervisor
could not get through this project's infra`. On your machine the line ends
`(tried again at the supervisor's next look)`, and the supervisor never
gives up: its next look tries the project again. On a cloud install the supervisor tries the project again 5 seconds
after the first failure and 10 seconds after the second, and the line ends
with how many passes failed in a row and when it tries again, for example
`(failure 1 in a row; tried again in 5 s)`. At the third failure the
supervisor stops trying, and the line ends ``(failed 3 times in a row, so the
supervisor stopped trying; `weft infra look` tries again)``. A pass over
the project that goes through starts the count again from zero, and so does
a `weft infra look`, which gives the project a fresh round of three tries. The line goes once a pass over the project works.

When a project is removed, the supervisor deletes what the infra host still
holds of it, disks included. A removed project has no status of its own, so
if that fails, every project's `weft status` shows it under `waiting on
weft:` as `weft's supervisor could not delete what the infra host still
holds of removed projects (their disks included)`. The error names each
removed project, what went wrong, and what happens next. On your machine it
ends `(tried again at the supervisor's next look)`. On a cloud install the
supervisor tries again 5 seconds after the first failure and 10 seconds
after the second, and at the third the error ends `(failed 3 times in a
row, so the supervisor stopped trying; its next pass tries again)`. From
then on every pass of the supervisor tries again, and `weft infra look` in
any project starts one. The line goes once the host holds nothing of any
removed project.

A command (a start, a stop, an upgrade) whose run or whose ending keeps
failing would be taken up again forever, so the supervisor gives it up once
it has taken it up 3 times without it ending. This holds on your machine
too. The command ends failed with `the supervisor took this command up 3
times and it never ended (its run, or the write of how it ended, kept
failing), so it gave the command up; issue it again`, the commands it was
holding back go ahead, and `weft status` lists it under `waiting on weft:`
until the ending of a later infra command of the project is written. If you
want it done, fix the cause and issue the command again.

## The broker, and who may talk to what

Only the dispatcher and the broker hold a database connection; the listener,
the supervisor and every worker go through the broker.

Each of those processes keeps one WebSocket open to the broker, and every call
it makes rides that one connection, numbered and answered in whatever order
they finish. A call the broker holds until something happens (new journal
rows) costs nothing while it waits, so a worker takes one seat at the broker
however many calls its runs hold open, and a call whose caller stops waiting
is dropped at the broker too. The broker also pushes down the same connection
the changes a worker keeps a copy of (see [the worker](#the-worker)), and,
for a run the worker drives, its cancel and every answer handed to it. A
connection with nothing to do for a minute closes, and opens again on the
next call, so an idle process holds nothing open at the broker, except a
worker. A worker driving runs keeps its connection open, to hear their
cancels and answers, and an idle one reports to the broker every second
(that it is alive, and how many runs it drives), so its connection opens
again within a second of closing. On Google Cloud a connection is cut after
an hour, the most Cloud Run lets one request last, and the broker says so
when it opens. A busy worker opens a second connection once the first is 30
minutes old (or, if it was idle then, as soon as it gets busy), and moves
over once the broker answers on it. The first connection keeps delivering until then,
so the worker misses nothing and does not have to read the database again to
catch up. A worker whose runs all only wait (on a person's answer, or on the
next message from a caller on a socket) opens no new
connection, because a broker copy that has not met that worker may read the
database to check who it is. Instead, once its connection is within a
minute of the cut and its runs only wait, it stops the way it does when
Cloud Run stops it. The runs that can pause are
handed back for a fresh worker to carry on, the others end with a message
naming the limit, and the worker exits, so the next request starts a new
one. When the connection breaks, it comes back by
itself: a call not yet sent goes out on the new one if it is back within the
call's wait. Past that, or when a sent call got no answer, two kinds of call
are made again because doing so is safe: a batch of records (a batch that
arrives twice is stored only once), and a read of a run's history. A call the broker says it could not even start
(every one of its database connections busy) is sent again too, since nothing
of it ran. Any other call fails the way a request whose connection reset
does, since it may have landed. Uploads and downloads of files stay ordinary
requests, so a big file never holds up the calls behind it.

The broker checks every request. The caller proves who it is with a token its
platform gave it: on your machine, one the runtime signed when it started the
container; on a cloud install, one Google signed for the account the caller
runs as, weft's own or the program's. The broker works out from that token
whose install the caller belongs to and which role it plays, and it checks
that the caller may touch what it asked for. Your
program's worker runs untrusted node code, so it never gets a database
connection.

The broker also handles storage and connection work, including OAuth exchanges
and subscription setup. Your worker calls providers itself, so a slow provider
never holds up the broker for everyone else.

On your machine, the API the CLI talks to has no password: anything that can
reach it can do what a project owner can. It only answers programs on your own
machine (`127.0.0.1`). For the rest of the boundaries, go and read the
[security policy](https://github.com/WeaveMindAI/weft/blob/main/SECURITY.md).

## What happens when something dies

A worker that dies takes its fast runs with it. The exception is a run whose
every branch is waiting on a timer or a form: its whole record is written
before it pauses, so a new worker picks it up when the wait ends. That
exception does not hold while a caller is still on the line and the route
leaves `outlivesCaller` off, or while a bus between its nodes is open: such a
run holds its worker instead of pausing. Nobody can
tell which of a fast run's nodes ran after the last row that reached the
database, and running it again could repeat them, so a fast run whose worker
died ends cancelled and is not run again, with this reason: `the worker running this run went away; a fast run lives in its
worker's memory, so it is not run again (make its trigger durable for a run
that survives its worker)`.

A lost fast run is not marked cancelled the moment its worker dies: the
dispatcher notices and ends it. Every worker reports that it is alive through
the broker once a second, and counts as gone 15 seconds after its last report.
The dispatcher ends a run once its worker has been gone for another 15
seconds, and it looks again exactly when the first such worker's extra 15
seconds run out, so a fast run ends about 30 seconds after its worker last
reported. Its journal holds what reached the database
before the worker died.

A worker whose runs are all only waiting on something outside them (a
person's answer while the run cannot pause, or the next message of a caller
on a socket) stops reporting, so the database is not woken every second for
a wait that can last hours. Its last report says it is alive until the first
of those waits would give up, plus 15 seconds. A wait on a person gives up
when its `holdSecs` runs out. A wait for a caller's next message has no
time limit of its own, so the worker's last report covers it until the
run's hour ends or the route's or socket's `maxSessionSecs` runs out (the
longest one connection may stay open; unset, there is none), whichever
comes first. On your machine a run has no hour limit, so for a route or
socket without `maxSessionSecs` the worker keeps reporting every second. The
worker reports again as soon as one of its runs moves (that first wait
giving up included) or a new run comes, saying again how long it is alive
for. The answer or the cancel that ends
the wait is written by the dispatcher, which also tells the broker copy
holding the worker's connection, without going through the database (on
Google Cloud, over Pub/Sub). That broker copy stays up for the length of the
wait, and pushes the answer or the cancel down the worker's connection, and
only then does the worker read it. Nothing else reads while a run waits: the
worker read its answers once as the run started waiting, and the broker
reads only for the calls a worker makes. So the database sleeps for as long
as the run waits, and wakes for the answer, the cancel, or the run's end,
or when the connection breaks, after which the worker reads once, for all
its runs together, to catch up. On Google Cloud such a wait lasts at most until the worker stops before
its connection is cut, within the hour (see [the
broker](#the-broker-and-who-may-talk-to-what)). If the worker dies during the wait, its
runs are ended (or carried on, if durable) about 30 seconds after that
first wait would have given up.

In that same check, a durable run whose worker died is queued again, and the
dispatcher hands it to another worker at once, which carries it on from its
record. A node that had started but not finished is failed on the new worker
rather than run again, apart from some pure nodes. For which ones,
and what to do about a failed node, go and read [surviving a
restart](../nodes/durable-execution.md#when-the-worker-dies-mid-run).

On a cloud install, when Cloud Run replaces a worker or scales one away, it
sends the worker `SIGTERM`, the signal that asks a process to stop on its own,
and kills it 10 seconds later. Every run on the worker then leaves it if it
can:

- A run that can pause starts no new node. Once the nodes it is running have
  ended, its record is written whole and it is handed back, so another worker
  carries it on straight away. A node still running 5 seconds after the
  `SIGTERM` is stopped where it is, and the next worker fails that node, the
  same as if the worker had died. If its caller is still on the line (a route
  with `outlivesCaller` on), the run carries on with nobody there, and the
  caller hears `the copy of the program serving this request is stopping, so
  the run carries on in another one without this caller`.
- Three kinds of run cannot pause: an unrecorded run, a run tied to the caller
  still on the line (a route without `outlivesCaller`), and a run with a bus
  open, since a bus lives in its worker's memory alone. Each keeps running,
  starting new nodes as usual, until the platform stops the process. If it has
  not ended by then, that is the same as its worker dying: a fast run ends
  cancelled, and a durable one is carried on by another worker. An unrecorded
  run that nothing had written down yet leaves no trace.

At the same moment the worker stops accepting connections. A call that was
already in, but whose run had not started yet, gets a `503` saying `this copy
of the program is stopping; try again in a moment`, so the client can retry
and reach another copy.

If you restart the daemon on your own machine, the new daemon sends the same
`SIGTERM` to every worker the old one left running and removes each once it
has exited, so their runs go the same way. When the new daemon sets up a
project's own address again, it kills that project's old workers 10 seconds
after sending them `SIGTERM`, so the new worker can open its port.
If you would rather not wait for an old worker to exit, `docker rm
--force <container>` stops it now, and the runs still on it end as if it had
died; the daemon's log (`~/.local/share/weft/runtime.log`) names each such
container.

If a worker cannot get a batch of records written (the broker errors or never
answers), the runs in that batch end failed (for why, go and read [why a
failed write stops the run](the-journal.md#why-a-failed-write-stops-the-run)).

If a worker freezes or loses its connection to the broker, it stops reporting,
and its runs go the way a dead worker's do: about 30 seconds later a durable
run moves to another worker and a fast run ends. The run's row carries a
number that goes up every time a worker takes the run or loses it, and the
broker refuses any write made under another number. So from then on the frozen worker's writes to that run
are refused, and it drops the run.

## Running it somewhere new

If you want weft on another platform, write a crate that implements the traits below, from
`crates/weft-platform-traits`: everything that depends on where weft
runs sits behind one of them.

| Interface | What it answers |
|---|---|
| `Runner` | How a program's workers are started and reached |
| `InfraHost` | Where infrastructure units run, and how they are watched |
| `ImageBuilder` | How staged source becomes a runnable image |
| `Alarm` | How a wake set for later is delivered |
| `CallerIdentity` | Who is calling an internal endpoint |
| `IdentityTokens` | How a role proves who it is to another |
| `FrontendHosting` | Where a project's frontend runs, when the install hosts it |
| `DomainHosting` | What stands in front of the install's own domains and holds their certificates |
| `HolderPool` | How many holders run, for the triggers that keep a connection open |

Files go through one more trait, `ObjectStore`. If your platform's store
speaks S3 and you can hand it a key pair, you implement nothing: the S3
client (`object_store_for`) talks to it, the way it talks to SeaweedFS on your
machine. GCP has its own (`GcsObjectStore`), because it reaches Cloud Storage
as the runtime's own service account and has Google sign the links, so no key
exists for an organization's policy to forbid.

Two platforms exist: `weft-platform-local` (Docker on your machine) and
`weft-platform-gcp` (Cloud Run, Compute Engine, Cloud Build, Cloud Tasks). The
install's config names one, and `crates/weft-runtime/src/platform.rs` is the
one place that reads which.
