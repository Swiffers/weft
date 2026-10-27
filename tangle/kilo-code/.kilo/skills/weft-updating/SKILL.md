---
name: weft-updating
description: "Read when the user asks to update or upgrade weft, or when something broke right after one: locating the checkout, the git pull plus setup.sh walk, what setup.sh rebuilds and what it preserves, and fixing a failed update."
---

# Updating weft

[the checkout] is the git clone of weft on the user's machine; `./setup.sh` in it is what installed weft. An update is `weft maintenance on`, `git pull` in [the checkout], `./setup.sh`, then `weft maintenance off`, then one command per project. An update never edits a file inside the user's projects.

Projects created with `weft new <name> --assistant kilo-code` (shorthand `kc`; adding `--remember` makes it the default, so plain `weft new` installs it too) have their Tangle persona (`kilo.json` and `.kilo/`) COPIED into the project, so a project keeps the version that created it. After the `./setup.sh`, `weft tangle update` inside a project re-copies its Tangle from [the checkout]; run it in each project the user wants on the new version. It replaces every file Tangle owns and leaves anything the assistant wrote beside them alone, so the diff is worth reading.

## Finding the checkout

`cat ~/.local/share/weft/repo-root` prints [the checkout]. `weft --version` prints the running version, and `git -C <checkout> log --oneline -1` shows which commit is installed.

## The walk

setup.sh has two paths. [the prebuilt path] downloads the CLI and extension CI already built and pulls the daemon images: minutes, no Rust or Node toolchain needed. [the compile path] builds everything locally: slower, not broken, and it needs a Rust toolchain (plus Node and pnpm for the extensions). [the checkout] with no local changes, at a commit CI has built, takes [the prebuilt path]; any local change at all, or `--from-source`, takes [the compile path].

1. **Look before pulling.** `git -C <checkout> status --short` and `git -C <checkout> log --oneline -3`. Local changes force [the compile path], and a pull over them can conflict. You tell the user both before moving, and you never clean the changes up yourself.
2. **Switch everything off.** `weft maintenance on` takes down every trigger that is on in every project and stops every infra copy that runs, so the old weft and the new one never run side by side, then waits for the runs going to finish. setup.sh refuses to replace the running weft while anything is on, and says so. `--running cancel` cancels those runs instead of waiting: that loses work, so it is the user's call, never yours. If it says something is still on (a trigger being switched on, infra being started), wait for that, then run it again: a second `on` only adds.
3. **Pull.** `git -C <checkout> pull`. On conflicts you show the user the conflicted files, resolve with their input, then re-run the pull. If you catch yourself typing `git stash`, `git checkout .` or `git reset --hard` in [the checkout], stop and write: "Wait. Their changes." Then show the user the files and ask.
4. **Run setup.** `cd <checkout> && ./setup.sh`. The default refreshes the CLI, the daemon (its images rebuilt when they changed, weft's runtime restarted or started), and the VS Code extension; the browser extension is opt-in with `--browser` (it signs for Firefox, which is slow; `--no-sign` skips signing).
5. **Bring it back.** `weft maintenance off` builds every project `on` took something down in again on the new weft, starts the infra `on` stopped on that build, waits until it runs, then brings back exactly the triggers `on` took down. A project last built before weft had `weft maintenance` comes back on its old build, and the report says to run `weft resync` in its folder. If something in a project does not come back, `off` says what and why and still ends the maintenance: everything else comes back, and what did not stays on record. If the message says the project's copy of the standard nodes is older than this weft, `weft catalog update` in its folder replaces that copy; it deletes any edits the user made under `nodes/base_catalog/`, so check with them first. Otherwise fix what the error says (usually something in its folder). Then `weft resync` there builds it, brings back what the update took down, and says what it brought back. If the user wants some of those triggers left off, `weft deactivate` them before the resync. Running `weft maintenance off` again rebuilds from the same files as before, so it does not pick up a change made in the folder.
6. **Verify.** `weft --version` (the new number), `weft daemon status` (running). If the daemon is not coming up, you read `weft daemon logs --tail 100` before touching anything: the refusal names its fix there.
7. **Per project, after the update.** `weft catalog update` syncs this project's stdlib copy to the new weft (a diagnostic naming the catalog or a stale copy is the signal to run it on any day). `weft tangle update` re-copies this project's Tangle personas, skills and commands from the new weft; they are ordinary project files, so nothing else brings them forward. Running projects keep running on the build they were started with; when the user wants a project moved onto the new engine, `weft resync` does it, and its old compiled worker (an image on a cloud install) is reclaimed by a later install once nothing points at it.

## What an update never does

- It never edits the user's projects: not the source, not the connections, not the journal.
- It never starts what the maintenance did not stop: `weft maintenance off` brings back only what `weft maintenance on` took down. Infra the user had stopped before stays stopped: a `weft run` then says `infra not running for: ...`, and `weft infra start` brings it back.
- It never destroys data without asking. The one exception is the update from the weft that ran on a local Kubernetes cluster: its database cannot come along, so setup.sh stops and asks "Do you want to proceed? [Y/n]" before wiping it (run history, versions and stored connections go; project folders stay). That answer is the user's: you relay the question and never answer it for them. Run without a terminal, setup.sh refuses instead and names `scripts/scrub-old-install.sh --yes`; you run that only after the user says yes. The same holds for an install whose database was built before executions were renamed runs: setup.sh asks before wiping that one install's database (run history, versions, stored connections, stored files and the projects' running infra go; project folders, settings and other installs stay), and without a terminal it names `./setup.sh --yes`, which you run only after the user says yes. Otherwise setup.sh's disk hygiene is bounded: build caches are capped, unreferenced images are reclaimed, and the keep-set preserves every running project's current compiled worker and every live project's infra images.
- `--uninstall` and `--purge` are removal operations and never part of this walk.

## When the update breaks

- **setup.sh refuses, saying the install has something on.** Step 2 was skipped, or something came on since: `weft maintenance on`, then `./setup.sh` again.
- **The pull conflicted.** Step 3: with the user, never around them.
- **The build failed.** You read the error before guessing. A missing toolchain on [the compile path] is the most common cause, and the error says so.
- **setup.sh or the daemon names an older weft's Kubernetes cluster.** That is the one-time wipe above: tell the user what it deletes and ask before running `scripts/scrub-old-install.sh --yes`, then `./setup.sh`.
- **The daemon refuses a database "built by an older weft (from before runs were renamed...)".** That is the database wipe above: tell the user what it deletes, and ask before running `./setup.sh --yes`.
- **The daemon refuses to start.** `weft daemon logs --tail 100` names the failing component and the fix. A second `./setup.sh` is safe (it reconciles, so running it twice changes nothing).
- **The version did not change.** Either the pull brought nothing new (`git -C <checkout> log --oneline -1` to confirm) or setup.sh served prebuilt artifacts for the same commit. Neither is stuck.
- **A project misbehaves after the update.** First `weft catalog update` in that project (the stdlib copy lags by default), then a run; if the wrongness survives, it is a real finding and the normal debugging loop applies.

The flags that matter here, all opt-in: `--cli`, `--daemon`, `--vscode`, `--browser` (refresh one component instead of the default set), `--no-daemon` (skip the daemon refresh), `--from-source` (force [the compile path]), `--debug` (debug-profile CLI), `--rebuild` (rebuild the runtime image and recompile the standard worker program even when unchanged).
