# Changelog

What changed between versions of weft that you need to know about when you
update. To update, pull and run `./setup.sh` again.

## 2026-10-13 (0.2.269)

**Each branch of a checkout can now run its own install. If your checkout is
not on `main`, or you keep a public tunnel's token in `.env`, there is
something to do.**

If you run weft from a checkout on a branch other than `main`, it now uses
that branch's own install. weft stops with an error until you run
`./setup.sh` there, and the install it sets up starts empty. Your projects
stay on your default install. If you want to keep working with them, check
out `main` or set `WEFT_INSTALL=default`.

If you keep `WEFT_PUBLIC_TUNNEL_TOKEN` and `WEFT_PUBLIC_TUNNEL_HOSTNAME` in
`.env`, move both lines into a `.env.local` beside it, then run `./setup.sh`
again. Every checkout made with `scripts/add-worktree.sh` links to that same
`.env`, so a branch you run `./setup.sh --public-url` on would try to open the
same tunnel, and weft refuses to start a second install on a token
another install already uses. `.env.local` belongs to that checkout alone, and
what it sets wins over `.env`.

What you will notice:

- A branch's install lives under `~/.local/share/weft/installs/`, with its
  own runtime, database and files. For how weft picks the install, go and read
  [Working on several branches at once](CONTRIBUTING.md#working-on-several-branches-at-once).
- If you want to know which install a command would act on, run
  `weft daemon which`.
- When you are done with a branch, `scripts/remove-worktree.sh` removes its
  worktree and its install.
- On a branch, `./setup.sh` installs neither the VS Code extension nor the
  browser extension. If you want to try a branch's VS Code extension,
  `scripts/dev-editor.sh` opens a VS Code window running it from source. If
  the branch's extension is 0.2.269 or newer, that window uses the CLI of that
  checkout's install.
- If you want a public address for a branch, run `./setup.sh --public-url`
  there: it opens the tunnel that branch's `.env.local` names, or else a
  random address that changes each time the tunnel restarts.
- `scripts/run-e2e.sh` runs 4 tests at once by default (it was 16).
- A second `scripts/run-e2e.sh` on the same machine now waits for the first
  instead of refusing.

## 2026-10-12 (0.2.268)

**On Google Cloud, an install nobody is using now lets its database sleep,
and the install workflow must be run again.**

weft no longer looks at its database on a timer. It reads it when something
happens: a run working, a timer or poll you set coming due, a call arriving, a
person answering, or a failure to deal with. A trigger that holds a connection
open, a run waiting on a person, and a quiet caller all leave the database
asleep. Locally nothing changes for you.

What you have to do on a Google Cloud install: run the install workflow
again. It adds a second Cloud Tasks queue, a Pub/Sub topic for the processes
that hold connections, and the `wake_attempts` setting (how many times a
failing timer, poll or renewal is tried before its trigger is given up, 20 by
default). `./setup.sh` upgrades the database on its own.

What you will notice:

- `weft status` lists what weft keeps failing to do under `waiting on weft:`,
  with what happens next, until it works again: a part of weft it cannot
  reach, a background job, the infra supervisor, queued runs its workers
  cannot take, news it could not send, leftovers of removed projects.
- A trigger whose timer, poll or renewal keeps failing is given up after its
  tries, with the reason on its node. `weft resync`, or reconnecting its
  connection, brings it back.
- A resync now runs on its own. It takes down only the triggers that are on,
  brings back exactly those, and refuses builds and other activations while it
  holds the project. If it stops, it goes on by itself once what it waited on
  is back; otherwise `weft resync --resume` takes it up, and
  `weft cancel-activate` gives it up and says which triggers stay down.
- `weft activate` after a park or hibernation asks what to do with the work
  the triggers kept; a resync, an infra upgrade and the CI workflow keep it
  without asking.
- A live caller whose run ends early gets `500` (the program failed), `504` (a
  time limit) or `503` (weft or a person stopped it); a socket closes with
  `1011` or `1013`.
- A live node test can now wait on a signal (a node that polls a job until it
  is done), up to the platform's limit on one request.
- On Google Cloud, the install emails you when weft's own work stays stuck
  (about $0.35 a month). It goes to the first person holding Owner on the
  project, or to the repository secret `WEFT_ALERT_EMAIL` if you add one; set
  `alerts_enabled = false` in `deploy/terraform/gcp/variables.tf` to turn it
  off, and `alert_after_minutes` (5 by default) sets how long a failure weft
  still retries waits before it emails.
- A `weft rm` that fails partway leaves the project marked as being removed:
  `weft status` and the editor say so, every verb refuses with the same line,
  and running `weft rm` again finishes it.
- A trigger that holds a connection open stops, with the reason on its node,
  when only a person can fix its connection; reconnecting the account brings
  it back. A busy or rate-limited service is still retried.
- A node may now emit on a plain output, wait on a signal, and emit more; an
  output sent before the wait is not sent again when the run resumes.
- The cost of a call whose price arrives later (an ElevenLabs dub read back
  after its run waited) is now recorded instead of being booked as unknown.

**Dub Audio (`ElevenLabsDub`) has moved to ElevenLabs's new dubbing API:
update the programs that use it.**

ElevenLabs has marked the dubbing route the node used as legacy, so the node
now uses the new one, where each dub is a project on the ElevenLabs account.

What you have to change:

- If a program dubs a video and expects a video back, or expects an mp3: the
  dub now always comes back as the audio track alone, in FLAC, because that is
  the only output the new API gives. The old route gave back an MP4 for a
  video and an MP3 for audio, so `mimeType` and `filename` change too.
- Take `numSpeakers` and `dropBackgroundAudio` out of your `.weft` files: the
  new API has neither.
- `dubbingId` is now `projectId`. The node used to leave every dub on the
  ElevenLabs account; now it deletes the project once the dub is stored. If
  the node runs on your own key and you want to keep the project, wire
  `projectId`.
- If a program gives a three-letter language code like `spa`, change it to
  the two-letter one, `es`. The new API takes language tags, which use a
  language's two-letter code when it has one, with a region added if you want
  one (`es-MX`).

New inputs:

- `model`: the default, `dubbing_v1`, costs $0.50 a minute as before and dubs
  into 29 languages. If you want one of 92 languages, or ElevenLabs's newer
  model, which it says preserves voice and emotion, pick `dubbing_v2`, at
  $2.20 a minute.
- `keyterms` takes names the dub should get right in the transcription and
  the translation.
- `cloningStrength` (dubbing_v2 only) sets how closely the dubbed voices copy
  the speakers.
- `sourceUrl` takes a public link instead of a file, and ElevenLabs downloads
  the recording itself.
- `transcript` (ElevenLabs Enterprise) takes your own transcript, with its
  translations if you have them, instead of ElevenLabs's.
- `reference` puts your own label on a project you keep.

**New ElevenLabs nodes make images and videos.**

Five new nodes call ElevenLabs's Image & Video API:

- **Generate Image** (`ElevenLabsGenerateImage`) makes an image from a prompt,
  or edits images you give it.
- **Generate Video** (`ElevenLabsGenerateVideo`) makes a video from a prompt,
  and can open and end on images you choose.
- **Video From References** (`ElevenLabsVideoFromReferences`) makes a video
  that uses images, clips or sounds you give it.
- **Lip-Sync Video** (`ElevenLabsLipSync`) makes the person in a picture speak
  an audio track.
- **Run Template** (`ElevenLabsRunTemplate`) runs a published ElevenLabs Flows
  template, yours or one shared with you. Add one input port for each of the
  template's inputs and one output port for each output you want back, each
  named by its id in the template. If you do not know the ids, declare any one
  output and run: the node checks your ports before anything is paid, and its
  refusal lists the template's inputs and outputs with their types.

To use them, you need your own ElevenLabs account on a Pro plan or above, and
an API key with ElevenLabs's Image & Video permission. Connect that key in
weft as your own ElevenLabs connection, with "Images and video" ticked. To
see what these calls cost, look at your usage in your ElevenLabs account:
ElevenLabs does not report a cost per generation, so weft records none.

**A new Dialogue node, and Speak to File takes every setting of
ElevenLabs's speech route.**

The new **Dialogue** (`ElevenLabsDialogue`) node turns a script of lines, each
with its own voice, into one audio file of them talking together. It uses
`eleven_v4` unless you pick `eleven_v3`, and like Speak to File it runs on the
built-in connection. Speak to File (`ElevenLabsSpeakFile`) now picks its model
from ElevenLabs's own list of models that speak text, so a new model shows up
without a weft update.

Both nodes take:

- a language, whether numbers are spelled out, a seed, and the text just
  before and after this clip, so the delivery flows into the clips around it;
- pronunciation dictionaries;
- stitching: each node gives out a `requestId`. If you want a later clip to
  carry on from this one, wire that id into the later node's
  `previousRequestIds`. If you are redoing an earlier clip that leads into
  this one, wire it into that node's `nextRequestIds`. Both nodes must be of
  the same kind;
- audio effects: a filter, the distance from the microphone, a room's reverb,
  a background noise and a place between left and right;
- speaking a professional voice clone with its instant clone (`usePvcAsIvc`),
  which ElevenLabs says can sound more expressive and answer faster;
- zero retention, on Enterprise accounts;
- every output format ElevenLabs has, wav and alaw included.

Speak to File also takes the style and speaker boost voice settings and
Japanese normalization, and Dialogue takes stability and similarity for the
whole script.

If a node you wrote calls ElevenLabs's text to dialogue route, or the
streamed or timestamped text to speech routes, it now runs on the built-in
connection too, instead of asking for your own key, because weft now prices
those routes.

**The other ElevenLabs nodes take more of ElevenLabs's settings, and give
big results as JSON files.**

Most settings ElevenLabs offers on these nodes' routes are now inputs; the
ones it has deprecated are not. Most combinations ElevenLabs would refuse are
refused before anything is paid for: when the program is built if you wrote
the values, and when the node runs if they are wired.

A result that grows with the length of the recording or the call (a
transcript, word timings, a conversation) now leaves the node as a JSON file,
because a value passed between nodes is capped at 100 KB. Small results (a
language, a duration, an id) stay plain values.

- **Transcribe File** (`ElevenLabsTranscribeFile`) now defaults to `scribe_v2`
  (it was `scribe_v1`) and also offers `scribe_v2_medical`. ElevenLabs has
  deprecated `scribe_v1` and no longer documents `scribe_v1_experimental`, so
  if a program names either, change it to `scribe_v2`. The `text`, `words`,
  `entities`, `editedTranscript` and `channels` outputs are gone: they now come
  out together as one `transcript` JSON file (the edited copy under
  `editedText`), so a program that reads any of them reads the file instead.
  New: telling speakers apart, speaker
  roles, key terms, entity detection and redaction, subtitle and document
  exports (srt and others), an edited copy of the transcript following an
  instruction you write in plain words, separate channels, a public link
  instead of a file, and video files.
- **Transcribe** (`ElevenLabsTranscribe`, live) takes the commit strategy and
  the voice detection settings, extra languages, key terms, entity detection,
  a transcript edit, background filtering and μ-law audio, and sends word
  timings, entities and edits along with each segment. The live bus of
  segments is now called `segments` (it was `transcript`), and at the end the
  whole session's text, entities and edited copy come out as one `transcript`
  JSON file, in place of the `text`, `entities` and `editedText` outputs.
- **Music** (`ElevenLabsMusic`) now defaults to `music_v2_5` (it was
  `music_v1`), which answers a 48 kHz, 192 kbps mp3 where `music_v1` answered
  44.1 kHz at 128 kbps; `music_v2` is offered too. ElevenLabs has deprecated
  `music_v1`, so if a program names it, change it to `music_v2_5`. New: a
  composition plan instead of a prompt, lyrics, a seed, finetunes, the output
  format, storing the song so a later plan can build on it, and C2PA signing.
  A new `songId` output names the song.
- **Sound Effect** (`ElevenLabsSoundEffect`) takes the output format and the
  model.
- **Speak** (`ElevenLabsSpeak`, streaming) takes the voice settings, a
  language, number spelling, a seed, SSML, pronunciation dictionaries, the
  chunk schedule and more output formats, and can send the timing of every
  character it speaks. ElevenLabs has deprecated `eleven_turbo_v2_5`, so the
  list no longer offers it; if a program names it, change it to
  `eleven_flash_v2_5`, the replacement ElevenLabs suggests.
- **Change Voice** (`ElevenLabsChangeVoice`) picks its model from ElevenLabs's
  own list of voice changing models, and takes the voice settings, a seed, raw
  PCM input, zero retention and every output format.
- **Isolate Voice** (`ElevenLabsIsolateVoice`) takes raw PCM input. Its result
  is now typed and named as the mp3 ElevenLabs sends back
  (`isolated_take.mp3`), where it used to keep the input's type and name
  (`isolated_take.wav`).
- **Design Voice** (`ElevenLabsDesignVoice`) takes the output format,
  loudness, a seed, guidance, quality, a reference recording and its strength,
  and gives out the line the previews speak. Its previews now default to
  `mp3_44100_128`, where ElevenLabs's own default, which the node used before,
  is `mp3_44100_192`; set `outputFormat` to keep the old quality.
- **Remix Voice** (`ElevenLabsRemixVoice`) is new: it turns a voice you already
  have into new previews from a description of the changes, and Create Voice
  saves the one you pick, as it does a designed one.
- **Create Voice** (`ElevenLabsCreateVoice`) and **Clone Voice**
  (`ElevenLabsCloneVoice`) take labels; Create Voice also gives out the
  voice's preview link and labels, and Clone Voice whether the voice needs
  verifying.
- **Align** (`ElevenLabsAlign`): the word timings, now with each word's loss,
  and the timing of every character come out together as one `alignment` JSON
  file, in place of the `words` and `characters` outputs.
- **Create Agent** (`ElevenLabsCreateAgent`) takes the language model (picked
  from ElevenLabs's list), tools, a knowledge base, built-in tools, the voice
  model and settings, timeouts, data to collect, evaluation criteria and which
  settings a call may change, plus JSON inputs for every other setting.
  `builtInTools` starts with `end_call`, so an agent created by an unchanged
  program can now hang up on its own; remove `end_call` from it if you do not
  want that.
- **Agent Call** (`ElevenLabsAgentCall`) takes per-call changes (first
  message, prompt, language, voice), a ring timeout, call recording and
  answering-machine detection, now calls from Exotel numbers too, and gives
  out the call id of a SIP call.
- **Get Conversation** (`ElevenLabsGetConversation`) also gives out the
  collected data, the evaluation results, the summary title, the success
  score, the duration, why the call ended, its cost in US dollars, and an
  OpenTelemetry trace of the conversation. Its `transcript` is now a JSON file
  holding the text and the turns, in place of the `text` output and the list
  of turns, and `conversation` is a JSON file, written only when something
  reads it.

**weft records ElevenLabs costs more exactly.**

- v4, v3 and Multilingual v2 speech: $0.08 per 1,000 characters (was $0.10).
- v4 Turbo, Flash and Turbo: $0.04 per 1,000 characters (was $0.05).
- v3 Conversational: $0.04 per 1,000 characters (was $0.10).
- Audio in opus, wav or alaw is recorded at its real length. Before, all three
  were read as if they were 128 kbps mp3, so alaw was recorded at half its
  length, opus at a quarter to one and a half times its length depending on
  its bitrate, and wav at up to six times its length.
- Music with no output format chosen was recorded as a 128 kbps file, so a
  song from `music_v2` or `music_v2_5`, which answer at 192 kbps, was recorded
  at one and a half times its length. It is now recorded at its real length.
- Batch transcription (Transcribe File) now records the key terms, transcript
  edit, entity detection and speaker role add-ons, each channel of a
  multichannel recording, and the length ElevenLabs reports, so a
  transcription from a link is no longer recorded as one minute. Live
  transcription (Transcribe) already recorded key terms and entity detection:
  key terms are now $0.08 an hour (was $0.05), entity detection 30% of the
  base rate (was $0.07 an hour), and a transcript edit adds $0.12 an hour.
- If a node you wrote calls ElevenLabs's legacy dubbing route (`/v1/dubbing`
  without `/project`), it now asks for your own ElevenLabs key: weft no longer
  prices that route.

**A node's picker can show only the items that match a filter.**

If you write a node whose `remote_select` should show only some of the items a
service lists, add `where` to the list source: an item is kept only when the
value at each named field (a dotted path like `limits.tier` reaches inside it)
equals the one you give. Speak to File uses it to list only the ElevenLabs
models that speak text: `"where": { "can_do_text_to_speech": true }`.

**When ElevenLabs refuses a call, the error says why.**

The error now shows ElevenLabs's own message instead of its whole reply. When
a setting does not fit the model, it names the setting the way ElevenLabs
spells it (`body.veo-3.1-generate-001.duration_secs` for the node's
`durationSecs`) and says what is wrong with it.

If a node you wrote checks replies with `json_call`, `get_json`, `post_json`
or `checked_send`, it now reads two more error shapes from any service: it
quotes a `detail` object's `message`, and turns FastAPI's list of field errors
(`{"detail": [{"loc": ..., "msg": ...}]}`) into one line naming each field and
what is wrong with it. `put_response` used to quote the raw reply; it now
reads every error shape those four read.

## 2026-10-11 (0.2.267)

**On Google Cloud, a run waiting on a person lets the database sleep.**

A run that cannot pause (a route still answering its caller, a trigger with
`recorded: false`, a live stream between steps) keeps its question in its
worker while it waits. Until now that kept the database awake for the whole
wait. Now nothing touches the database until the answer or a cancel arrives:
the answer reaches the worker through a news topic weft creates in your
Google project. Upgrade as usual (merge upstream into your fork and run the
install workflow again): it creates the topic and the rights that go with it.

A worker on Google Cloud can keep one connection to weft for at most an
hour. If every run on it is still waiting at that point, the worker stops:
runs that can pause carry on elsewhere, and a run that cannot pause ends,
saying which limit ended it. To wait longer, make the run one that can pause.

**Five instance nodes lost "Instance" from their names.**

`StartInstanceInfra`, `StopInstanceInfra`, `TerminateInstanceInfra`,
`ActivateInstanceTriggers` and `DeactivateInstanceTriggers` are now
`StartInfra`, `StopInfra`, `TerminateInfra`, `ActivateTriggers` and
`DeactivateTriggers`. Rename them in your `.weft` files. Their `instance`
input is now optional: if you leave it unwired, the node acts on the
program's own copies, the ones every instance shares.

**GmailSend can send from another address.**

The new optional `from` input sends from one of the account's "Send mail as"
addresses set up in Gmail; any other address fails naming the ones that work.
To read that list, the Google connection needs "Read mail" or "Organize mail"
ticked. Replies are unchanged: they need one of those two or "Read mail
headers".

**A program built before this version needs one rebuild.**

How a waiting step receives its answer changed, so a worker built before this
version fails when a waiting step gets its answer. Resync each program once:
`weft resync` if its triggers are on, `weft build` otherwise. Either one
builds the worker again on this version of weft and moves the program to it.

**Skipping a question ends the step that asked it.**

When a person skips a waiting question (Skip in the browser extension, or
`POST /signal/{token}/skip`), the step that asked now ends skipped: its
outputs close and the steps reading them skip too. It used to answer the
question with null, so if your program reads a null answer as "skipped", it
now sees the step skipped instead. Skipping a trigger is refused with a `409`;
it used to fire the trigger.

**A Google connection can hold Gmail alone.**

The connect check now asks Google who the account is, through two permissions
every connection asks for ("Know who you are" and "See your email address"),
instead of reading Drive. So a connection with only Gmail, or only Calendar,
ticked now passes. If you run your own shared Google app, its `covers` does
not need to list those two.

**Node authors: inputs can be required only when wired.**

`requiredWhenWired: true` on an input means: left unwired, the node runs
without it; wired, a wire that delivers nothing skips the node. It is for an
input whose absence means something, like the instance nodes' `instance`.

**Builds no longer hold one request open.**

`weft build`, `run`, `activate`, `resync` and `infra start` used to keep one
request open to the install for the whole build. The install now answers as
soon as the builds start, and the CLI follows each one to its end. If a cloud
install does not answer, the error names that install instead of telling you
to start the local daemon. Update the VS Code extension too: it needs this
version.

**A route's limits can overshoot briefly after a quiet spell.**

When a worker copy wakes after a quiet spell, its first calls are decided on
what it heard before it went quiet, until weft's broker answers it: under a
second normally, a few seconds when the broker was asleep too. During that
window, copies waking at the same moment can each let in up to the full
`callsAtOnce`. We chose this so the first call after a pause never waits for
the broker to start.

**The editor's activate button names the triggers that are off.**

When some triggers are off while the others run, the second button now says
which ones ("Reactivate a, b" or "Reactivate a, b and 3 more") instead of
"Reactivate the rest".

## 2026-10-09 (extension 0.2.266): weft no longer needs Kubernetes

**Breaking: your run history does not come along.**

Weft used to run on a local Kubernetes cluster. It now runs as one process
next to Docker on your machine, and on Google Cloud as Cloud Run services plus
one small VM. The database behind it starts over with this version, so the old
one cannot be carried forward.

If you installed weft before this version, `./setup.sh` finds the old install
and stops to ask before it goes on:

```
an older weft (the one that ran on a local Kubernetes cluster) is on this machine,
and this version cannot carry it forward. It is going to be wiped: the projects on
your disk are not touched, but their run history, versions and stored connections are.
  Do you want to proceed? [Y/n]
```

If you say yes, it wipes the old install (`scripts/scrub-old-install.sh`) and
carries on with a normal install. If you say no, nothing is touched and nothing
is installed.

What goes:
- The old cluster.
- Every project's run history and versions.
- Your stored connections: you connect your accounts again the first time a
  program asks.
- Every container, volume and image the old weft made, your programs' own
  infrastructure (and its data) included.
- Every named install on the machine (a test cell, say), with its database.
  The question lists the ones it found.

What stays:
- Your project folders. `weft run` in each one registers it again.
- Whether you turned on a public address with `--public-url`.
- The assistant you picked for `weft new`.

If you would rather wipe first by hand, or `./setup.sh` runs without a
terminal to ask on, run `scripts/scrub-old-install.sh` yourself, then
`./setup.sh`. On a machine that never had the old weft, or that was already
wiped, `./setup.sh` is a normal install or update and asks nothing.

**"Member" is now "instance", everywhere.**

What used to be a program's members are now its instances: separate copies of
part of a program, each under its own id. One per person is one way to use
them, one per session another, several per person a third; which person owns
which instances is kept in your program's own database. Every name changed
with it, and the old ones are gone:
- `@per_member` is `@per_instance`, and `@member_filled` is `@instance_filled`.
  Change them in your `.weft` files.
- The `members` catalog package is `instances`, and its nodes follow:
  `CurrentInstance`, `MintInstanceToken`, `WipeInstance`, `ListInstances`,
  `ListInstanceInfra`, `InstanceInfraStatus`, `StartInstanceInfra`,
  `StopInstanceInfra`, `TerminateInstanceInfra`, `ActivateInstanceTriggers`,
  `DeactivateInstanceTriggers`, `GetInstanceValues`, `SetInstanceValues` and
  `InstanceCosts`.
- The headers are `Weft-Instance` and `Weft-Instance-Token`. If your backend or
  frontend sends the old ones, rename them there.
- The `--member` flag is `--instance` on every command, and
  `--all-members` is `--all-instances`.
- A member token is now an instance token: it acts inside one instance of the
  program.

The page is now [programs with instances](docs/src/running/instances.md).

The name "instance" used to mean a named install (a second weft on the same
machine, such as a test cell). That is now called an install: the field in
`~/.local/share/weft/config.json` is `install`.

**weft moves to new ports.**

Weft now answers at `http://127.0.0.1:14111` (it was 9999). Open the dashboard
there, and point anything you configured by hand at it. The VS Code extension
follows on its own unless you set `weft.dispatcherUrl` yourself (if yours
still says the old `http://localhost:9999`, it is ignored and the extension
offers to remove it). The browser
extension remembers the address with each token you saved, so remove those
tokens and add them again.

If you use a Cloudflare tunnel, point it at `127.0.0.1:14112` (it was 9998).

If you want weft on another port, set its variable once when you run
`./setup.sh` (`WEFT_PUBLIC_PORT`, `WEFT_OUTSIDE_PORT`, `WEFT_INTERNAL_PORT`
or `WEFT_POSTGRES_PORT`). Weft saves the ports it runs on in
`~/.local/share/weft/ports.json`, so the port stays moved on later runs and
the CLI and the VS Code extension find it there.
`WEFT_SEAWEED_PORT` moves the object store, and is read on every run.

**A step the worker was running is never run again by itself.**

If a worker went away while a step was running, the next worker used to run
that step again from the top, so an email, a post or a paid call could happen
twice. Now it fails the step instead, saying the worker went away and the step
was not run again. Check what the step did, then re-run from there with
`weft run --seed`, which reuses every step that completed. A step that was
waiting on an answer still carries on when the answer comes, as before.

**Nodes that catch their failures say so with `catchErrors`.**

If you wrote a node with an optional `error` output and `ctx.catch_into_error`,
change it: remove the `error` output from its `metadata.json`, add
`"catchErrors": true` under `features`, and let `run` return its errors as any
node does (`ctx.catch_into_error` is gone). Weft adds the `error` output
itself, and a failure goes there when a program wires it. A node with `catchErrors`
that still declares `error` is refused when it loads. Programs need no change.

**Conversations are kept in a file only.**

The AI nodes (`LlmInference`, `LlmStream`) and `ChatHistoryAppend` no longer
have a `history` input or output. If a program wires `history`, wire
`historyFile` instead: each call adds its turn to that one file and passes the
same file on. If a call gets no file and its `historyFile` output is wired, it
starts a new one. The graph shows each change to the file under "Files
edited" on the step that made it.

**A node changes a stored file with `edit`.**

`ctx.storage(scope).edit(&file, |old| ...)` changes a file in place without
losing another write's change. `replace` still overwrites.
`replace_stream` is gone. Every stored file value now carries a `version`. A
seeded run that would reuse a step whose file was edited since is refused,
naming the step, the port and the file, and offering `--emit` or
`--seed-before`.
