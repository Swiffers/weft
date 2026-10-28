//! Folder watch handler. Holds a watch on a folder of the machine its
//! holder runs on and fires once per file that appears in it, changes or
//! goes ([`FolderWatch`]).
//!
//! What fires is decided by looking at the disk, never by the kind of
//! event the operating system reported: those differ between systems (a
//! save can arrive as a write, as a rename over the old file, or as a
//! removal and a creation), and a queue that overflowed reports nothing
//! at all. So the task keeps what it last saw of every file it watches
//! (size and modification time), and when a path has changed and then
//! stayed quiet for a moment ([`QUIET`]), it reads that path again and
//! compares. Writes with no pause longer than that moment are one look; a
//! copy that pauses longer is looked at in the pause (created, at the size
//! it had then) and again once it resumes and pauses (changed). A path
//! that never goes quiet is looked at once per [`QUIET_MOMENTS_AT_MOST`]
//! quiet moments anyway, so it still fires. A file it had not
//! seen is created, one whose size or time moved is changed, one that is
//! no longer there is removed. A folder that came, went or was moved is
//! the same look over everything under it, and a lost queue is that look
//! over the whole folder.
//!
//! What the task last saw is also kept on the signal's row (its
//! `kind_state`, a [`Snapshot`]), so when the task starts it compares the
//! disk against that and fires the differences like any later look: a
//! file that arrived, changed or went while nothing watched (weft stopped
//! or in maintenance, the machine off, the holder restarted, the trigger
//! down behind a door that takes) fires once the watch is back. A watch
//! whose row holds no snapshot (a signal's very first, or one registered
//! afresh) fires nothing for the files already there: it remembers them
//! and writes that down. A registration again carries the snapshot over
//! only when it watches the same folder, `recursive` and pattern
//! ([`KindHandler::source`]) and the trigger listened the whole time
//! (`super::prepare_signal`), so what changed behind a door that refused
//! never fires.
//!
//! The snapshot is written after a look that changed what is known, once
//! that look's fires are out, and at no other time, so a folder nobody
//! touches writes nothing. A stop between a fire and the write fires it
//! again at the next start: at least once, as every fire is. A fire that
//! could not be handed over leaves its file as it was last seen before
//! the change, so the next look of it offers the change again; one that
//! finds the signal held elsewhere, or gone, stops the watch.
//!
//! A folder that is gone (deleted, moved away) fires a removal for each
//! file it held, and is watched for from the nearest folder above it that
//! exists, so it fires a creation for each file it holds again the moment
//! it is back.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::Value;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{info, warn};
use weft_core::live::{LiveFeed, LiveItem};
use weft_core::primitive::{SignalAuth, SignalRouting, SignalSpec, SignalSurface};
use weft_core::signal::listener_protocol::{ProcessOutcome, ProcessTarget};
use weft_core::signal::{FolderChange, FolderEvent, FolderWatch, NamePattern, Signal};

use crate::config::Holders;
use crate::event_context::{FireContext, FireOutcome};
use crate::registry::RegisteredSignal;

use super::event_source::Backoff;
use super::{BetweenFires, KindHandler, LiveCtx, RowState, ServingReport, SpawnCtx};

/// How long a path stays quiet after a change before it is looked at:
/// long enough that a burst of writes to one file is one fire, short
/// enough that a file dropped in starts its run about a second later.
pub const QUIET: Duration = Duration::from_secs(1);

/// The longest a path waits to be looked at once it is first heard, in
/// quiet moments (ten of [`QUIET`]: 10 seconds): a file that keeps
/// changing still fires at least once per this long.
pub const QUIET_MOMENTS_AT_MOST: u32 = 10;

pub struct FolderWatchHandler;

#[async_trait]
impl KindHandler for FolderWatchHandler {
    fn tag(&self) -> &'static str {
        FolderWatch::TAG
    }

    fn between_fires(&self, _spec: &SignalSpec, _kind_state: &Value) -> Result<BetweenFires> {
        Ok(BetweenFires::Holds)
    }

    /// The files a watch covers: its folder, whether the folders under it
    /// count, and the names that do. Which changes fire is not part of it.
    fn source(&self, spec: &SignalSpec) -> Result<Option<Value>> {
        let cfg = parse(spec)?;
        Ok(Some(serde_json::json!({ "folder": cfg.folder, "recursive": cfg.recursive, "pattern": cfg.pattern })))
    }

    /// What it last saw, so what changed since fires.
    fn carry(&self, _spec: &SignalSpec, prior: &Value) -> Result<Value> {
        Snapshot::read(prior)?;
        Ok(prior.clone())
    }

    fn compute_routing(&self, _spec: &SignalSpec) -> Result<SignalRouting> {
        Ok(SignalRouting {
            surface: SignalSurface::Internal,
            auth: SignalAuth::None,
            auth_config: Value::Null,
        })
    }

    /// The folder is refused here, where somebody is watching, when this
    /// install cannot watch it: its holders are on another machine, it is
    /// on a drive that reports no changes, or it is not there.
    async fn settle(&self, spec: &SignalSpec, kind_state: Value, ctx: &SpawnCtx) -> Result<Value> {
        let cfg = parse(spec)?;
        if let Some(why) = unwatchable(&cfg.folder, ctx.config.holders) {
            anyhow::bail!("{why}");
        }
        let folder = Path::new(&cfg.folder);
        if !folder.is_dir() {
            anyhow::bail!(
                "{} is not a folder on this machine{}; create it, or point the trigger at one that is there",
                cfg.folder,
                if folder.exists() { " (it is a file)" } else { " (nothing is there)" },
            );
        }
        Ok(kind_state)
    }

    async fn spawn_task(&self, spec: &SignalSpec, stored: RowState, ctx: SpawnCtx) -> Result<Option<JoinHandle<()>>> {
        let cfg = parse(spec)?;
        let pattern = cfg.name_pattern().map_err(anyhow::Error::msg)?;
        let seen = Snapshot::read(&stored.state)?;
        Ok(Some(tokio::spawn(hold(cfg, pattern, seen, stored.seq, ctx, QUIET))))
    }

    fn process_entry(&self, _sig: &RegisteredSignal, payload: Value) -> ProcessOutcome {
        ProcessOutcome { value: payload, target: ProcessTarget::Entry }
    }

    /// Nothing calls in, so there is no address to show: the display is
    /// what is watched, what fires, and whether the watch is up.
    fn live(&self, ctx: &LiveCtx<'_>) -> LiveFeed {
        let sig = ctx.sig;
        let cfg = match super::config_for_display::<FolderWatch>(sig, "Watching") {
            Ok(cfg) => cfg,
            Err(feed) => return feed,
        };
        let files = cfg.pattern.clone().unwrap_or_else(|| "every file".into());
        let files = if cfg.recursive { format!("{files}, in the folders under it too") } else { files };
        let fires: Vec<&str> = cfg.events.iter().map(|e| e.as_str()).collect();
        let mut items = vec![
            LiveItem::text("Watching", cfg.folder),
            LiveItem::text("Files", files),
            LiveItem::text("Fires on", fires.join(", ")),
        ];
        items.extend(super::serving_item(sig));
        LiveFeed::new(items)
    }

    fn render(&self, _token: &str, _sig: &RegisteredSignal) -> Result<Option<Value>> {
        Ok(None)
    }
}

fn parse(spec: &SignalSpec) -> Result<FolderWatch> {
    serde_json::from_value(spec.config.clone()).map_err(|e| anyhow::anyhow!("malformed folder_watch spec: {e}"))
}

/// Why an install whose holders run where `holders` says can never watch
/// `folder`, or `None` when it can.
fn unwatchable(folder: &str, holders: Holders) -> Option<String> {
    if holders != Holders::OnThisMachine {
        return Some(format!(
            "a folder watch needs the install to run on the machine the folder is on, and this install's \
             listeners run on machines of their own, where {folder} is not; use a local install to watch it"
        ));
    }
    weft_core::working_copy::hears_no_changes(folder, weft_core::working_copy::on_wsl()).then(|| {
        format!(
            "{folder} is on a Windows drive, and WSL tells Linux nothing of the changes made there, so weft \
             cannot watch it. Move the folder into the Linux filesystem (under ~/, for example) and point the \
             trigger there"
        )
    })
}

/// The held task: watch, fire, and when the watch itself fails, say so
/// and set it up again on the ladder. `seen` is what the row says was
/// last seen, at version `seq`.
async fn hold(cfg: FolderWatch, pattern: Option<NamePattern>, seen: Option<Snapshot>, seq: i64, ctx: SpawnCtx, quiet: Duration) {
    let mut say = Say::new(super::serving_sink(&ctx));
    // Kept across a failed watch, so what changed while it was set up
    // again fires once it is.
    let mut folder = Folder::new(PathBuf::from(&cfg.folder), cfg.recursive, pattern);
    // With nothing on the row, looks only remember until one reads the
    // whole folder, and that is written down so the next start has
    // something to compare against.
    let owed = seen.is_none();
    if let Some(seen) = seen {
        folder.remember(seen);
    }
    let mut keep = Keep { fence: Fence::At(seq), owed };
    let mut backoff = Backoff::new();
    loop {
        let started = std::time::Instant::now();
        let (kept, halt) = watch(&cfg, folder, &ctx, &mut keep, &mut say, quiet).await;
        folder = kept;
        let e = match halt {
            Halt::Failed(e) => e,
            Halt::Lost => {
                info!(target: "weft_listener::folder_watch", folder = %cfg.folder, token = %ctx.fire.token(), "the signal is no longer this holder's (its row moved under a write, or a fire found it held elsewhere or gone), so the watch stops here");
                say.say("stopped: this watch was taken over (another holder took it, it was registered again, or it is gone)".into());
                return;
            }
        };
        warn!(target: "weft_listener::folder_watch", folder = %cfg.folder, token = %ctx.fire.token(), error = %format!("{e:#}"), "the folder watch failed; setting it up again");
        say.say(format!("could not watch the folder, trying again shortly: {e:#}"));
        backoff.reset_if_healthy(started.elapsed());
        backoff.wait_then_climb().await;
    }
}

/// What the task has heard from the operating system.
enum Heard {
    /// Something changed at these paths.
    Paths(Vec<PathBuf>),
    /// Events were lost (the queue overflowed): everything is looked at.
    Everything,
}

/// Why a watch stopped.
enum Halt {
    /// The watch itself failed; it is set up again.
    Failed(anyhow::Error),
    /// A write of the snapshot found the row moved, or gone, or a fire
    /// found the signal held elsewhere, or gone: it was registered again,
    /// another holder took it, or it was removed, and each lets go of this
    /// holder's claim, so this task no longer owns it. It stops rather than
    /// fire or write again, and the holder's next look stops its entry.
    Lost,
}

impl From<anyhow::Error> for Halt {
    fn from(e: anyhow::Error) -> Self {
        Halt::Failed(e)
    }
}

/// Watch until the watch stops, which is answered with the folder as last
/// seen. The look as it starts fires what changed only when the folder is
/// [`Folder::known`]; otherwise it only remembers what is there.
async fn watch(cfg: &FolderWatch, folder: Folder, ctx: &SpawnCtx, keep: &mut Keep, say: &mut Say, quiet: Duration) -> (Folder, Halt) {
    let (tx, rx) = unbounded_channel();
    let mut watching = Watching { cfg, ctx, keep, disk: Some(Disk { folder, ears: Ears::new(tx) }), due: Due::new(quiet) };
    let root = watching.root().to_path_buf();
    let e = match watching.look(&root, true).await {
        Ok(there) => {
            say.aimed(there.expect("aimed"));
            watching.run(say, rx).await
        }
        Err(e) => e,
    };
    (watching.disk.take().expect("no look is running").folder, e)
}

struct Watching<'a> {
    cfg: &'a FolderWatch,
    ctx: &'a SpawnCtx,
    keep: &'a mut Keep,
    /// Taken while a look reads the disk off the async threads.
    disk: Option<Disk>,
    due: Due,
}

/// The folder as last seen, and what hears it change: everything a look
/// reads or sets on the disk, moved off the async threads together.
struct Disk {
    folder: Folder,
    ears: Ears,
}

impl Disk {
    /// Watch the folder when it is there (`true`), and the nearest folder
    /// above it otherwise, to hear it come back.
    fn aim(&mut self) -> Result<bool> {
        self.folder.find_real();
        let root = self.folder.root.clone();
        if root.is_dir() {
            self.ears.aim(&root, self.folder.recursive)?;
            return Ok(true);
        }
        let above = root
            .ancestors()
            .skip(1)
            .find(|dir| dir.is_dir())
            .with_context(|| format!("no folder above {} exists to watch for it from", root.display()))?;
        self.ears.aim(above, false)?;
        Ok(false)
    }

    /// Read `scope` again, each folder in it watched as it is read
    /// ([`Ears::reading`]), so a change after the look is heard and one
    /// before it is in what the look saw.
    fn look(&mut self, scope: &Path) -> Result<Vec<Change>, LookError> {
        let ears = &mut self.ears;
        let changes = self.folder.look(scope, &mut |dir| ears.reading(dir));
        self.ears.looked(scope);
        changes
    }
}

/// The paths heard and not yet looked at, each with when it is.
struct Due {
    quiet: Duration,
    longest: Duration,
    /// Each path's first hearing in this wait, and when it is looked at.
    paths: BTreeMap<PathBuf, (Instant, Instant)>,
}

impl Due {
    fn new(quiet: Duration) -> Self {
        Self { quiet, longest: quiet * QUIET_MOMENTS_AT_MOST, paths: BTreeMap::new() }
    }

    /// `path` was heard `now`: it is looked at once it has been quiet a
    /// moment, and never later than the longest wait after it was first
    /// heard, however often it is heard again.
    fn heard(&mut self, path: PathBuf, now: Instant) {
        let (first, at) = self.paths.entry(path).or_insert((now, now));
        *at = (now + self.quiet).min(*first + self.longest);
    }

    /// When the next path is due.
    fn next(&self) -> Option<Instant> {
        self.paths.values().map(|(_, at)| *at).min()
    }

    /// The paths due by `now`, no longer waiting.
    fn take_ready(&mut self, now: Instant) -> Vec<PathBuf> {
        let ready: Vec<PathBuf> = self.paths.iter().filter(|(_, (_, at))| *at <= now).map(|(p, _)| p.clone()).collect();
        for path in &ready {
            self.paths.remove(path);
        }
        ready
    }

    /// Stop waiting on every path under `root`, which is looked at whole.
    fn forget_under(&mut self, root: &Path) {
        self.paths.retain(|path, _| !path.starts_with(root));
    }
}

impl Watching<'_> {
    fn root(&self) -> &Path {
        &self.disk.as_ref().expect("no look is running").folder.root
    }

    async fn run(&mut self, say: &mut Say, mut rx: UnboundedReceiver<Heard>) -> Halt {
        loop {
            let heard = match self.due.next() {
                Some(at) => tokio::select! {
                    heard = rx.recv() => heard,
                    () = tokio::time::sleep_until(at) => {
                        if let Err(e) = self.quiet_ended(say).await {
                            return e;
                        }
                        continue;
                    }
                },
                None => rx.recv().await,
            };
            let now = Instant::now();
            // Everything heard so far goes in one batch: what each path
            // means is read off the disk, off the async threads.
            let mut batch = vec![heard.expect("the watcher holds a sender until this task drops it")];
            while let Ok(more) = rx.try_recv() {
                batch.push(more);
            }
            let disk = self.disk.take().expect("no look is running");
            let (disk, scopes) = off_async_threads(move || {
                let scopes = disk.folder.scopes_of(batch);
                (disk, scopes)
            })
            .await;
            self.disk = Some(disk);
            for scope in scopes {
                self.due.heard(scope, now);
            }
        }
    }

    /// Look at every path whose quiet moment is over. A look at the
    /// folder itself first aims the watch again, since the folder may
    /// have gone or come back.
    async fn quiet_ended(&mut self, say: &mut Say) -> Result<(), Halt> {
        let ready = self.due.take_ready(Instant::now());
        let root = self.root().to_path_buf();
        if ready.contains(&root) {
            // Everything under the folder is looked at anyway.
            self.due.forget_under(&root);
            let there = self.look(&root, true).await?;
            say.aimed(there.expect("aimed"));
            return Ok(());
        }
        for scope in outermost(ready) {
            self.look(&scope, false).await?;
        }
        Ok(())
    }

    /// Read `scope` again, fire for what changed there, then write what is
    /// known now to the row when it moved (or an earlier write is still
    /// owed). Until the folder is [`Folder::known`], a look only remembers
    /// and writes nothing: the row is given a snapshot only once one look
    /// has read the whole folder. With `aim` (a look of the whole folder,
    /// which may have gone or come back), the watch is set up again first,
    /// and whether the folder is there is answered ([`Disk::aim`]). A part
    /// of the folder that cannot be read is logged and left as it was last
    /// seen, so it never reads as its files having gone; its next change
    /// looks again.
    async fn look(&mut self, scope: &Path, aim: bool) -> Result<Option<bool>, Halt> {
        let disk = self.disk.take().expect("one look at a time");
        let owned = scope.to_path_buf();
        let (mut disk, aimed, looked) = off_async_threads(move || {
            let mut disk = disk;
            let aimed = match aim.then(|| disk.aim()).transpose() {
                Ok(aimed) => aimed,
                Err(e) => return (disk, None, Err(LookError::Watch(e))),
            };
            let looked = disk.look(&owned);
            (disk, aimed, looked)
        })
        .await;
        let changes = match looked {
            Ok(changes) => changes,
            Err(LookError::Read(e)) => {
                self.disk = Some(disk);
                warn!(target: "weft_listener::folder_watch", folder = %self.cfg.folder, scope = %scope.display(), error = %format!("{e:#}"), "could not read part of the watched folder; its next change looks again");
                return Ok(aimed);
            }
            Err(LookError::Watch(e)) => {
                self.disk = Some(disk);
                return Err(Halt::Failed(e));
            }
        };
        let folder = &mut disk.folder;
        let mut moved = false;
        for Change { change, at, before } in changes {
            if !self.cfg.fires_on(change.event) {
                moved = true;
                continue;
            }
            info!(target: "weft_listener::folder_watch", token = %self.ctx.fire.token(), path = %change.path, event = change.event.as_str(), "a watched file changed");
            let payload = serde_json::to_value(&change).expect("a folder change serializes");
            match self.ctx.fire.fire(payload, FolderWatch::TAG).await {
                FireOutcome::Fired | FireOutcome::Filtered => moved = true,
                // Not handed over (logged by the fire path): the file stays
                // as it was last seen before the change, so the next look
                // of it, or the next start, offers the change again.
                FireOutcome::EnqueueFailed => folder.undo(at, before),
                FireOutcome::NotHeld | FireOutcome::UnknownSignal => {
                    self.disk = Some(disk);
                    return Err(Halt::Lost);
                }
            }
        }
        if !disk.folder.known || (!moved && !self.keep.owed) {
            self.disk = Some(disk);
            return Ok(aimed);
        }
        // Written after the fires: a stop in between fires them again at
        // the next start, from the snapshot before them (at least once).
        // Made off the async threads: a large folder makes a large one.
        let (disk, snapshot) = off_async_threads(move || {
            let snapshot = serde_json::to_value(disk.folder.snapshot()).expect("a snapshot serializes");
            (disk, snapshot)
        })
        .await;
        self.disk = Some(disk);
        self.keep.save(&self.ctx.fire, snapshot).await.map(|()| aimed)
    }
}

/// Run `work` off the async threads. A panic in it is a bug, and stops the
/// task.
async fn off_async_threads<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work).await.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
}

/// The paths of `paths` no other one of them is inside.
fn outermost(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort();
    let mut out: Vec<PathBuf> = Vec::new();
    for path in paths {
        if !out.last().is_some_and(|kept| path.starts_with(kept)) {
            out.push(path);
        }
    }
    out
}

/// The operating system's change events, aimed at one folder at a time,
/// with the folders under it (`tree`) or without.
///
/// On Linux every folder is a watch of its own. notify's recursive mode
/// adds one per folder too, but gives up on the whole tree at the first
/// folder it may not read (a `lost+found` only root opens). So here a
/// tree's watches are set by the looks: each folder a look reads is
/// watched first ([`Ears::reading`]), and one that cannot be is passed
/// over with a warning, once. Every look sets again the watches of what
/// it reads, so one notify let go (it does so for a folder renamed away
/// or removed) is back at the next look there, and nothing here keeps a
/// copy of which folders are watched. Elsewhere the system watches a tree
/// as one, and notify's recursive mode is used as it is.
struct Ears {
    /// Where the watcher hands what it hears.
    tx: UnboundedSender<Heard>,
    /// `None` until the first aim.
    watcher: Option<RecommendedWatcher>,
    /// Whether the folders under the one aimed at are watched.
    #[cfg(target_os = "linux")]
    tree: bool,
    /// The folders that could not be watched, each warned about once, and
    /// forgotten once a look there no longer finds them.
    #[cfg(target_os = "linux")]
    refused: std::collections::BTreeSet<PathBuf>,
}

/// Why a watch could not be set, saying what to do when the machine ran out.
fn watch_failed(dir: &Path) -> String {
    format!("watch {} (when the machine is out of file watches, raise fs.inotify.max_user_watches)", dir.display())
}

/// A watcher that hands what it hears to `tx`.
fn hear(tx: UnboundedSender<Heard>) -> Result<RecommendedWatcher> {
    notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let heard = match event {
            // Reading a file is no change, and every look reads them.
            Ok(event) if matches!(event.kind, EventKind::Access(_)) => return,
            Ok(event) if event.need_rescan() => Heard::Everything,
            Ok(event) => Heard::Paths(event.paths),
            Err(_) => Heard::Everything,
        };
        // Gone only when the task is, which drops this watcher.
        let _ = tx.send(heard);
    })
    .context("start hearing the operating system's file changes")
}

impl Ears {
    fn new(tx: UnboundedSender<Heard>) -> Self {
        Self {
            tx,
            watcher: None,
            #[cfg(target_os = "linux")]
            tree: false,
            #[cfg(target_os = "linux")]
            refused: Default::default(),
        }
    }

    /// Hear `dir` from now on, and the folders under it when `tree`, with
    /// a watcher of its own: every watch the last one held goes with it.
    /// That covers a folder moved away, whose watch notify keeps hearing
    /// under its old name. `dir` itself failing to be watched is an error.
    fn aim(&mut self, dir: &Path, tree: bool) -> Result<()> {
        let mut watcher = hear(self.tx.clone())?;
        // On Linux the folders under it are watched as each look reads
        // them; the first is the look right after this.
        #[cfg(target_os = "linux")]
        let mode = {
            self.tree = tree;
            RecursiveMode::NonRecursive
        };
        #[cfg(not(target_os = "linux"))]
        let mode = if tree { RecursiveMode::Recursive } else { RecursiveMode::NonRecursive };
        watcher.watch(dir, mode).with_context(|| watch_failed(dir))?;
        self.watcher = Some(watcher);
        Ok(())
    }

    /// Called by a look on each folder it is about to read: on Linux, in a
    /// tree, the folder is watched first, so a change after the look is
    /// heard and one before it is in what the look saw. Watching a folder
    /// already watched sets the same watch again (notify adds to it, it
    /// fails on nothing), so this holds whatever notify let go meanwhile.
    ///
    /// A folder that cannot be read cannot be watched either: it is warned
    /// about once and passed over, and the look names it as unreadable.
    /// Once it can be read, the change to its permissions is heard in the
    /// folder that holds it, which looks at it, and that look watches it.
    /// Out of watches, or any other failure, is an error: the watch is set
    /// up again. A folder that went needs no letting go: notify drops the
    /// watches of a folder removed or renamed away, and those under it, on
    /// its own.
    #[cfg(target_os = "linux")]
    fn reading(&mut self, dir: &Path) -> Result<()> {
        if !self.tree {
            return Ok(());
        }
        let watcher = self.watcher.as_mut().expect("aimed before any look");
        match watcher.watch(dir, RecursiveMode::NonRecursive) {
            Ok(()) => {
                self.refused.remove(dir);
                Ok(())
            }
            // Gone since it was found: the disk changing, and its own
            // event says so.
            Err(notify::Error { kind: notify::ErrorKind::PathNotFound, .. }) => Ok(()),
            Err(notify::Error { kind: notify::ErrorKind::Io(e), .. }) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(notify::Error { kind: notify::ErrorKind::Io(e), .. }) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                if self.refused.insert(dir.to_path_buf()) {
                    warn!(target: "weft_listener::folder_watch", folder = %dir.display(), "a folder inside the watched one cannot be read, so nothing in it is watched; once it can be read, it is");
                }
                Ok(())
            }
            Err(e) => Err(e).with_context(|| watch_failed(dir)),
        }
    }

    /// The system watches a tree as one: nothing to do per folder.
    #[cfg(not(target_os = "linux"))]
    fn reading(&mut self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    /// A look of `scope` is done: a folder there that could not be watched
    /// and is no longer there is forgotten, so the warned-about ones stay
    /// as few as the folders that are.
    #[cfg(target_os = "linux")]
    fn looked(&mut self, scope: &Path) {
        self.refused.retain(|dir| !dir.starts_with(scope) || dir.is_dir());
    }

    #[cfg(not(target_os = "linux"))]
    fn looked(&mut self, _scope: &Path) {}
}

/// What a file looked like the last time it was read: its size, and its
/// modification time in nanoseconds since 1970 (before it when negative),
/// `None` when the system gives none or it is out of that range. Kept in
/// the form the row stores, so a time read back compares equal to the
/// same time read off the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Seen {
    len: u64,
    modified: Option<i64>,
}

/// `time` in nanoseconds since 1970, when it fits.
fn nanos(time: SystemTime) -> Option<i64> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_nanos()).ok(),
        Err(before) => i64::try_from(before.duration().as_nanos()).ok().map(|n| -n),
    }
}

/// Why a look found nothing to compare.
#[derive(Debug)]
enum LookError {
    /// The part looked at could not be read: logged, and left as it was
    /// last seen.
    Read(anyhow::Error),
    /// A folder in it could not be watched (the machine is out of watches,
    /// say), or the folder could not be read before it was ever known
    /// ([`Folder::known`]): the watch is set up again.
    Watch(anyhow::Error),
}

/// What one read of a part of the folder found: every file the watch
/// covers there, and each path under it that could not be read, with why.
struct Scan {
    files: BTreeMap<PathBuf, Seen>,
    unreadable: Vec<(PathBuf, std::io::Error)>,
}

/// What a watch last saw, as the signal's row keeps it (`kind_state`):
/// each file it covered as its path under the folder, its size in bytes
/// and its modification time ([`Seen`]), one array per file
/// (`["in/a.pdf", 3, 1730000000123456789]`), which keeps a large folder's
/// row small. A file whose name is not UTF-8 is left out: it fires nothing
/// anyway. Which folder it was seen in is the spec's to say: a
/// registration again carries the snapshot over only when it watches the
/// same files ([`KindHandler::source`]).
///
/// `signal.kind_state` is not a versioned document (each kind keeps its
/// own keys there; see `weft_runtime::schema::documents`), so a change to
/// this shape has to read the old one, or register the column then.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    files: Vec<(String, u64, Option<i64>)>,
}

impl Snapshot {
    /// What `state` says was last seen, `None` when it holds nothing (the
    /// signal's first watch). A state of another shape is an error.
    fn read(state: &Value) -> Result<Option<Self>> {
        if state.as_object().is_some_and(|keys| keys.is_empty()) {
            return Ok(None);
        }
        serde_json::from_value(state.clone()).map(Some).map_err(|e| anyhow!("malformed folder_watch state on the signal's row: {e}"))
    }
}

/// Where the row's write fence (`kind_state_seq`) stands for this task.
enum Fence {
    /// At this version, which the next write moves on from.
    At(i64),
    /// A write of `wrote` from `from` whose answer never came: it may or
    /// may not have landed, so the next write reads the row first.
    Unsure { from: i64, wrote: Value },
}

/// The task's side of the snapshot on the row.
struct Keep {
    fence: Fence,
    /// What is known moved (or was never written) and the row does not say
    /// it yet, because its write could not be made: the next look writes
    /// it, whatever that look finds. No timer tries again, so nothing
    /// wakes the database while the folder is quiet.
    owed: bool,
}

impl Keep {
    /// Write `snapshot` one version past where the fence stands. A write
    /// that cannot be made is logged and owed. A row that moved under it,
    /// or is gone, is [`Halt::Lost`]: never written again from here.
    async fn save(&mut self, fire: &FireContext, snapshot: Value) -> Result<(), Halt> {
        let from = match &self.fence {
            Fence::At(seq) => *seq,
            Fence::Unsure { from, wrote } => match fire.kind_state_now().await {
                Ok(Some(RowState { seq, .. })) if seq == *from => *from,
                Ok(Some(RowState { state, seq })) if seq == from + 1 && state == *wrote => seq,
                Ok(_) => return Err(Halt::Lost),
                Err(e) => {
                    warn!(target: "weft_listener::folder_watch", token = %fire.token(), error = %format!("{e:#}"), "could not read the signal's row to learn whether the last write landed; the next look tries again");
                    self.owed = true;
                    return Ok(());
                }
            },
        };
        match fire.claim_kind_state(snapshot.clone(), from).await {
            Ok(true) => {
                self.fence = Fence::At(from + 1);
                self.owed = false;
                Ok(())
            }
            Ok(false) => Err(Halt::Lost),
            Err(e) => {
                warn!(target: "weft_listener::folder_watch", token = %fire.token(), error = %format!("{e:#}"), "could not write what the watch saw to the signal's row; the next look tries again");
                self.fence = Fence::Unsure { from, wrote: snapshot };
                self.owed = true;
                Ok(())
            }
        }
    }
}

/// One change a look found: what a fire of it carries, the file's path,
/// and how the file was seen before it (`None`: it was not), for taking
/// the change back when its fire is not handed over ([`Folder::undo`]).
#[derive(Debug)]
struct Change {
    change: FolderChange,
    at: PathBuf,
    before: Option<Seen>,
}

/// The watched folder and what was last seen of every file in it that
/// the watch covers.
struct Folder {
    /// The folder as the trigger names it: every path a fire carries, and
    /// every path kept here, is under it.
    root: PathBuf,
    /// The same folder with every link on the way to it followed, as of
    /// the last aim. Some systems (macOS) report changes under this one.
    real: PathBuf,
    recursive: bool,
    pattern: Option<NamePattern>,
    files: BTreeMap<PathBuf, Seen>,
    /// Whether `files` is something to compare against: a snapshot on the
    /// row said so, or a look read the whole folder. Until then a look
    /// only remembers what it reads and reports no change, so a watch
    /// whose first attempts failed never takes what was already there for
    /// new.
    known: bool,
}

impl Folder {
    fn new(root: PathBuf, recursive: bool, pattern: Option<NamePattern>) -> Self {
        Self { real: root.clone(), root, recursive, pattern, files: BTreeMap::new(), known: false }
    }

    /// Take what a snapshot says was seen as what was last seen.
    fn remember(&mut self, seen: Snapshot) {
        self.files = seen.files.into_iter().map(|(path, len, modified)| (self.root.join(path), Seen { len, modified })).collect();
        self.known = true;
    }

    /// Learn where the folder really is, links followed, from the nearest
    /// folder on the way to it that is there.
    fn find_real(&mut self) {
        let mut rest = Vec::new();
        for at in self.root.ancestors() {
            if let Ok(real) = std::fs::canonicalize(at) {
                self.real = rest.iter().rev().fold(real, |path: PathBuf, part| path.join(part));
                return;
            }
            rest.extend(at.file_name());
        }
        self.real = self.root.clone();
    }

    /// `path` as a path under [`Folder::root`] when it was reported under
    /// the real folder.
    fn named_under_root(&self, path: &Path) -> PathBuf {
        match path.strip_prefix(&self.real) {
            Ok(rel) if !path.starts_with(&self.root) => self.root.join(rel),
            _ => path.to_path_buf(),
        }
    }

    /// The paths to look at again after hearing `heard`
    /// ([`Folder::scope_of`]). Run off the async threads: it reads the disk.
    fn scopes_of(&self, heard: Vec<Heard>) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for heard in heard {
            match heard {
                Heard::Paths(paths) => out.extend(paths.iter().filter_map(|path| self.scope_of(path))),
                Heard::Everything => out.push(self.root.clone()),
            }
        }
        out
    }

    /// What was last seen, as the row keeps it.
    fn snapshot(&self) -> Snapshot {
        let files = self
            .files
            .iter()
            .filter_map(|(path, seen)| {
                let under = path.strip_prefix(&self.root).ok()?.to_str()?;
                Some((under.to_string(), seen.len, seen.modified))
            })
            .collect();
        Snapshot { files }
    }

    /// Take back what a look learned of the file at `at`: it is seen as
    /// it was `before` (`None`: not seen at all), so the next look of it
    /// finds the same change.
    fn undo(&mut self, at: PathBuf, before: Option<Seen>) {
        match before {
            Some(seen) => self.files.insert(at, seen),
            None => self.files.remove(&at),
        };
    }

    /// The path to look at again after a change was heard at `path`: the
    /// whole folder when the change was to it or above it (it came, went
    /// or moved), `path` itself when it is a file the watch covers or a
    /// folder that can hold one, and nothing otherwise. A path reported
    /// under the real folder ([`Folder::real`]) is taken as the same path
    /// under the folder as named.
    fn scope_of(&self, path: &Path) -> Option<PathBuf> {
        if self.root.starts_with(path) || self.real.starts_with(path) {
            return Some(self.root.clone());
        }
        let path = &self.named_under_root(path);
        let rel = path.strip_prefix(&self.root).ok()?;
        if !self.recursive && rel.components().count() != 1 {
            return None;
        }
        if self.named(path) {
            return Some(path.to_path_buf());
        }
        // A folder inside, come or gone, may hold files that match.
        let holds = self.recursive && (path.is_dir() || self.files.range(path.to_path_buf()..).next().is_some_and(|(p, _)| p.starts_with(path)));
        holds.then(|| path.to_path_buf())
    }

    /// Whether a file at `path` would be one the watch covers, by name.
    fn named(&self, path: &Path) -> bool {
        match (&self.pattern, path.file_name()) {
            (None, _) => true,
            (Some(pattern), Some(name)) => pattern.matches(&name.to_string_lossy()),
            (Some(_), None) => false,
        }
    }

    /// Read `scope` again, answer what changed there since the last look
    /// (in path order), and remember what it holds now. What was last seen
    /// under a part of it that cannot be read is kept as it was: none of
    /// it fires, as gone or otherwise, until a look can read it again.
    /// `reading` is called on each folder before it is read ([`Folder::scan`]).
    fn look(&mut self, scope: &Path, reading: &mut dyn FnMut(&Path) -> Result<()>) -> Result<Vec<Change>, LookError> {
        let Scan { files: now, unreadable } = match self.scan(scope, reading) {
            // Without a baseline, only a look of the whole folder can take
            // one, so one that cannot read it fails the watch: it is set up
            // again until it can, never left deaf with nothing to compare.
            Err(LookError::Read(e)) if !self.known && scope == self.root => {
                return Err(LookError::Watch(e.context("could not read the folder to learn what is already in it")));
            }
            scanned => scanned?,
        };
        for (path, e) in &unreadable {
            warn!(target: "weft_listener::folder_watch", path = %path.display(), error = %e, "could not read part of the watched folder; what was last seen there is kept, and its next change looks again");
        }
        let kept = |p: &Path| unreadable.iter().any(|(dir, _)| p.starts_with(dir));
        let before: Vec<PathBuf> = self
            .files
            .range(scope.to_path_buf()..)
            .take_while(|(p, _)| p.starts_with(scope))
            .filter(|(p, _)| !kept(p))
            .map(|(p, _)| p.clone())
            .collect();
        let mut changes: Vec<(PathBuf, FolderEvent, Option<u64>, Option<Seen>)> = Vec::new();
        for path in &before {
            if !now.contains_key(path) {
                changes.push((path.clone(), FolderEvent::Removed, None, self.files.get(path).copied()));
            }
        }
        for (path, seen) in &now {
            match self.files.get(path) {
                None => changes.push((path.clone(), FolderEvent::Created, Some(seen.len), None)),
                Some(old) if old != seen => changes.push((path.clone(), FolderEvent::Changed, Some(seen.len), Some(*old))),
                Some(_) => {}
            }
        }
        for path in before {
            self.files.remove(&path);
        }
        self.files.extend(now);
        if !self.known {
            // Nothing to compare against yet; this is it once the whole
            // folder has been read.
            self.known = scope == self.root;
            return Ok(Vec::new());
        }
        changes.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(changes
            .into_iter()
            .filter_map(|(at, event, size_bytes, before)| match at.to_str() {
                Some(p) => Some(Change { change: FolderChange { path: p.to_string(), event, size_bytes }, at, before }),
                None => {
                    warn!(target: "weft_listener::folder_watch", path = %at.display(), "a watched file's name is not UTF-8, so no run can be handed its path; it fires nothing");
                    None
                }
            })
            .collect())
    }

    /// Every file under `scope` the watch covers, as it is now. Nothing
    /// when `scope`, or the folder itself, is not there. A folder or file
    /// under `scope` that cannot be read (a `lost+found` only root may
    /// open) is passed over and named in [`Scan::unreadable`]; only
    /// `scope` itself failing to read is an error. `reading` is called on
    /// each folder just before it is read, and its error stops the scan.
    fn scan(&self, scope: &Path, reading: &mut dyn FnMut(&Path) -> Result<()>) -> Result<Scan, LookError> {
        let unread = |e: std::io::Error, path: &Path| LookError::Read(anyhow::Error::new(e).context(format!("read {}", path.display())));
        let mut out = Scan { files: BTreeMap::new(), unreadable: Vec::new() };
        if !self.root.is_dir() {
            return Ok(out);
        }
        // The folder itself is followed through a link, as it is watched;
        // a folder linked in below it is not walked.
        let meta = if scope == self.root { std::fs::metadata(scope) } else { std::fs::symlink_metadata(scope) };
        let meta = match meta {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(unread(e, scope)),
        };
        if !meta.is_dir() {
            self.take(scope, &mut out.files).map_err(|e| unread(e, scope))?;
            return Ok(out);
        }
        let mut todo = vec![scope.to_path_buf()];
        while let Some(dir) = todo.pop() {
            reading(&dir).map_err(LookError::Watch)?;
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                // Removed since it was found: the disk changing, and its
                // own event says so.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) if dir == scope => return Err(unread(e, &dir)),
                Err(e) => {
                    out.unreadable.push((dir, e));
                    continue;
                }
            };
            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    // The listing broke off: what it had not reached yet
                    // is unknown, so the whole folder is kept as seen.
                    Err(e) => {
                        out.unreadable.push((dir.clone(), e));
                        break;
                    }
                };
                let path = entry.path();
                let kind = match entry.file_type() {
                    Ok(kind) => kind,
                    Err(e) => {
                        out.unreadable.push((path, e));
                        continue;
                    }
                };
                // A folder linked in is not walked: a link back up would
                // never end.
                if kind.is_dir() {
                    if self.recursive {
                        todo.push(path);
                    }
                } else if let Err(e) = self.take(&path, &mut out.files) {
                    out.unreadable.push((path, e));
                }
            }
        }
        Ok(out)
    }

    /// Add `path` to `out` when it is a file the watch covers.
    fn take(&self, path: &Path, out: &mut BTreeMap<PathBuf, Seen>) -> std::io::Result<()> {
        let inside = path.parent() == Some(self.root.as_path()) || (self.recursive && path.starts_with(&self.root));
        if !inside || !self.named(path) {
            return Ok(());
        }
        // Followed through a link: a link to a file is that file.
        let meta = match std::fs::metadata(path) {
            Ok(meta) => meta,
            // Gone since it was listed, or a link to nothing.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        if meta.is_file() {
            out.insert(path.to_path_buf(), Seen { len: meta.len(), modified: meta.modified().ok().and_then(nanos) });
        }
        Ok(())
    }
}

/// What the watch says about itself on its node, said only when it
/// changes.
struct Say {
    report: ServingReport,
    last: Option<String>,
}

impl Say {
    fn new(report: ServingReport) -> Self {
        Self { report, last: None }
    }

    fn say(&mut self, status: String) {
        if self.last.as_ref() != Some(&status) {
            self.last = Some(status.clone());
            (self.report)(status);
        }
    }

    /// Where the watch is aimed, once what was there has been looked at.
    fn aimed(&mut self, folder_there: bool) {
        self.say(if folder_there { "watching".into() } else { "the folder is gone; it is watched again once it is back".into() });
    }
}

inventory::submit!(&FolderWatchHandler as &dyn KindHandler);

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh folder of its own under the system's temporary folder.
    fn temp() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("weft-folder-watch-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn folder(root: &Path, recursive: bool, pattern: Option<&str>) -> Folder {
        Folder::new(root.to_path_buf(), recursive, pattern.map(|p| NamePattern::parse(p).unwrap()))
    }

    /// A folder named through a link, whose changes are reported under
    /// where it really is (as macOS reports them): each is looked at under
    /// the folder as named.
    #[test]
    fn a_change_reported_under_the_real_folder_is_named_under_the_folder_as_given() {
        let base = temp();
        std::fs::create_dir_all(base.join("real/sub")).unwrap();
        std::os::unix::fs::symlink(base.join("real"), base.join("link")).unwrap();
        let root = base.join("link");
        let mut f = folder(&root, true, Some("*.pdf"));
        f.find_real();
        let real = std::fs::canonicalize(base.join("real")).unwrap();
        assert_eq!(f.real, real);
        assert_eq!(f.scope_of(&real.join("a.pdf")), Some(root.join("a.pdf")));
        assert_eq!(f.scope_of(&real.join("sub")), Some(root.join("sub")), "a folder inside");
        assert_eq!(f.scope_of(&real), Some(root.clone()), "the folder itself");
        assert_eq!(f.scope_of(&root.join("a.pdf")), Some(root.join("a.pdf")), "a path as named stays as it is");
        assert_eq!(f.scope_of(&real.join("a.txt")), None, "a name the pattern refuses");

        // A folder not there yet is found under the nearest one that is.
        let mut gone = folder(&root.join("later/in"), false, None);
        gone.find_real();
        assert_eq!(gone.real, real.join("later/in"));
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// Until one look has read the whole folder, a look only remembers: a
    /// look of a part of it first fires nothing for what was already there.
    #[test]
    fn nothing_fires_until_a_look_has_read_the_whole_folder() {
        let root = temp();
        std::fs::write(root.join("a.txt"), "x").unwrap();
        let mut f = folder(&root, false, None);
        assert!(f.see(&root.join("a.txt")).is_empty());
        assert!(!f.known, "a part is no baseline");
        assert!(f.see(&root).is_empty());
        assert!(f.known);
        std::fs::write(root.join("b.txt"), "y").unwrap();
        assert_eq!(events(&f.see(&root)), vec![("b.txt".into(), "created", Some(1))]);
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A first look that cannot read the folder fails the watch (it is set
    /// up again), and the look once it can takes the baseline, firing
    /// nothing for what was there. Once known, the same failure is a look
    /// logged and kept.
    #[test]
    fn a_folder_never_read_whole_that_cannot_be_read_fails_the_watch() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp();
        std::fs::write(root.join("before.txt"), "x").unwrap();
        let lock = |mode| std::fs::set_permissions(&root, std::fs::Permissions::from_mode(mode)).unwrap();
        lock(0o000);
        // Permissions bind nobody running as root: nothing to show there.
        if std::fs::read_dir(&root).is_ok() {
            lock(0o755);
            std::fs::remove_dir_all(&root).unwrap();
            return;
        }
        let mut f = folder(&root, false, None);
        let err = f.look(&root, &mut |_| Ok(())).unwrap_err();
        assert!(matches!(err, LookError::Watch(_)), "{err:?}");
        assert!(format!("{err:?}").contains("already in it"), "{err:?}");
        assert!(!f.known);

        lock(0o755);
        assert!(f.see(&root).is_empty(), "the baseline fires nothing for what was there");
        assert!(f.known);

        lock(0o000);
        let err = f.look(&root, &mut |_| Ok(())).unwrap_err();
        assert!(matches!(err, LookError::Read(_)), "known: a look that cannot read is logged and kept: {err:?}");
        assert!(f.files.contains_key(&root.join("before.txt")));
        lock(0o755);
        std::fs::remove_dir_all(&root).unwrap();
    }

    impl Folder {
        /// A look with no watch to set.
        fn see(&mut self, scope: &Path) -> Vec<Change> {
            self.look(scope, &mut |_| Ok(())).unwrap()
        }
    }

    fn events(changes: &[Change]) -> Vec<(String, &'static str, Option<u64>)> {
        changes
            .iter()
            .map(|Change { change: c, .. }| (Path::new(&c.path).file_name().unwrap().to_string_lossy().into_owned(), c.event.as_str(), c.size_bytes))
            .collect()
    }

    #[test]
    fn a_look_maps_what_it_reads_to_created_changed_and_removed() {
        let root = temp();
        std::fs::write(root.join("old.txt"), "here before").unwrap();
        let mut f = folder(&root, false, None);
        // The first look only remembers (a signal whose row holds no
        // snapshot fires nothing from it).
        f.see(&root);

        std::fs::write(root.join("new.txt"), "abc").unwrap();
        assert_eq!(events(&f.see(&root.join("new.txt"))), vec![("new.txt".into(), "created", Some(3))]);

        std::fs::write(root.join("new.txt"), "abcdef").unwrap();
        assert_eq!(events(&f.see(&root.join("new.txt"))), vec![("new.txt".into(), "changed", Some(6))]);

        assert!(f.see(&root.join("new.txt")).is_empty(), "nothing moved, nothing fires");

        std::fs::remove_file(root.join("new.txt")).unwrap();
        assert_eq!(events(&f.see(&root.join("new.txt"))), vec![("new.txt".into(), "removed", None)]);

        let created = f.see(&root);
        assert!(created.is_empty(), "old.txt was there from the start: {created:?}");
        assert!(Path::new(&f.files.keys().next().unwrap()).is_absolute(), "paths are absolute");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_folder_that_goes_and_comes_back_fires_for_each_file() {
        let base = temp();
        let root = base.join("inbox");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a"), "1").unwrap();
        std::fs::write(root.join("b"), "22").unwrap();
        let mut f = folder(&root, false, None);
        f.see(&root);

        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(
            events(&f.see(&root)),
            vec![("a".into(), "removed", None), ("b".into(), "removed", None)]
        );

        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a"), "1").unwrap();
        assert_eq!(events(&f.see(&root)), vec![("a".into(), "created", Some(1))]);
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn only_the_files_the_watch_covers_count() {
        let root = temp();
        std::fs::create_dir_all(root.join("sub/deeper")).unwrap();
        std::fs::write(root.join("top.pdf"), "x").unwrap();
        std::fs::write(root.join("top.txt"), "x").unwrap();
        std::fs::write(root.join("sub/inner.pdf"), "x").unwrap();
        std::fs::write(root.join("sub/deeper/deep.pdf"), "x").unwrap();

        let flat = folder(&root, false, Some("*.pdf"));
        let names = |f: &Folder| -> Vec<String> {
            f.scan(&f.root, &mut |_| Ok(())).unwrap().files.keys().map(|p| p.strip_prefix(&root).unwrap().to_string_lossy().into_owned()).collect()
        };
        assert_eq!(names(&flat), vec!["top.pdf"]);
        let deep = folder(&root, true, Some("*.pdf"));
        assert_eq!(names(&deep), vec!["sub/deeper/deep.pdf", "sub/inner.pdf", "top.pdf"]);
        let every = folder(&root, false, None);
        assert_eq!(names(&every), vec!["top.pdf", "top.txt"], "a folder is no file");
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_change_is_looked_at_where_it_can_matter() {
        let root = PathBuf::from("/w/in");
        let mut flat = folder(&root, false, Some("*.pdf"));
        assert_eq!(flat.scope_of(Path::new("/w/in/a.pdf")), Some(root.join("a.pdf")));
        assert_eq!(flat.scope_of(Path::new("/w/in/a.txt")), None, "a name the pattern refuses");
        assert_eq!(flat.scope_of(Path::new("/w/in/sub/a.pdf")), None, "below a folder that is not recursive");
        assert_eq!(flat.scope_of(Path::new("/w/in")), Some(root.clone()), "the folder itself");
        assert_eq!(flat.scope_of(Path::new("/w")), Some(root.clone()), "a folder above it");
        assert_eq!(flat.scope_of(Path::new("/elsewhere/a.pdf")), None);

        let mut deep = folder(&root, true, Some("*.pdf"));
        assert_eq!(deep.scope_of(Path::new("/w/in/sub/a.pdf")), Some(root.join("sub/a.pdf")));
        assert_eq!(deep.scope_of(Path::new("/w/in/gone")), None, "neither a folder now nor one that held files");
        deep.files.insert(root.join("gone/x.pdf"), Seen { len: 1, modified: None });
        assert_eq!(deep.scope_of(Path::new("/w/in/gone")), Some(root.join("gone")), "a folder that held watched files went");
        flat.files.insert(root.join("x.pdf"), Seen { len: 1, modified: None });
        assert_eq!(flat.scope_of(Path::new("/w/in/gone")), None);
    }

    /// A folder inside that cannot be read is passed over and named, and
    /// what was last seen in it stays as it was: its files neither go nor
    /// come back, while the rest of the folder fires as usual.
    #[test]
    fn a_folder_inside_that_cannot_be_read_keeps_what_was_seen_there() {
        use std::os::unix::fs::PermissionsExt;
        let root = temp();
        std::fs::create_dir_all(root.join("locked")).unwrap();
        std::fs::write(root.join("locked/kept.txt"), "x").unwrap();
        std::fs::write(root.join("top.txt"), "x").unwrap();
        let mut f = folder(&root, true, None);
        f.see(&root);

        std::fs::set_permissions(root.join("locked"), std::fs::Permissions::from_mode(0o000)).unwrap();
        // Permissions bind nobody running as root: nothing to show there.
        if std::fs::read_dir(root.join("locked")).is_err() {
            std::fs::write(root.join("new.txt"), "abc").unwrap();
            std::fs::remove_file(root.join("top.txt")).unwrap();
            let scan = f.scan(&root, &mut |_| Ok(())).unwrap();
            assert_eq!(scan.unreadable.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(), vec![root.join("locked")]);
            assert_eq!(
                events(&f.see(&root)),
                vec![("new.txt".into(), "created", Some(3)), ("top.txt".into(), "removed", None)],
                "the rest of the folder fires, and nothing in the locked one reads as gone"
            );
            assert!(f.files.contains_key(&root.join("locked/kept.txt")), "kept as last seen");

            std::fs::set_permissions(root.join("locked"), std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(f.see(&root).is_empty(), "readable again and unchanged: nothing fires");
        }
        std::fs::set_permissions(root.join("locked"), std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A path heard once is due a quiet moment later; heard again and
    /// again, each hearing pushes it back, but never past the longest
    /// wait from when it was first heard.
    #[test]
    fn a_path_that_never_goes_quiet_is_due_at_the_longest_wait() {
        let quiet = Duration::from_millis(100);
        let start = Instant::now();
        let mut due = Due::new(quiet);
        let path = PathBuf::from("/w/in/a.pdf");
        due.heard(path.clone(), start);
        assert_eq!(due.next(), Some(start + quiet));

        // Heard every half quiet moment, for far longer than the longest wait.
        let longest = quiet * QUIET_MOMENTS_AT_MOST;
        let mut now = start;
        while now < start + longest * 3 {
            now += quiet / 2;
            due.heard(path.clone(), now);
            assert!(due.next().unwrap() <= start + longest, "never later than the longest wait");
        }
        assert_eq!(due.next(), Some(start + longest));
        assert_eq!(due.take_ready(start + longest), vec![path.clone()]);
        assert_eq!(due.next(), None);

        // The next hearing starts a wait of its own.
        due.heard(path.clone(), now);
        assert_eq!(due.next(), Some(now + quiet));
    }

    #[test]
    fn nested_paths_are_looked_at_once() {
        let got = outermost(vec![PathBuf::from("/a/b/c"), PathBuf::from("/a/b"), PathBuf::from("/a/bc"), PathBuf::from("/z")]);
        assert_eq!(got, vec![PathBuf::from("/a/b"), PathBuf::from("/a/bc"), PathBuf::from("/z")]);
    }

    #[test]
    fn a_folder_watch_is_refused_where_the_holders_are_elsewhere() {
        let why = unwatchable("/home/q/in", Holders::Pool).expect("refused");
        assert!(why.contains("local install") && why.contains("/home/q/in"), "{why}");
        assert_eq!(unwatchable("/home/q/in", Holders::OnThisMachine), None);
    }

    /// A change taken back is found again by the next look of its file:
    /// a creation, a change and a removal alike.
    #[test]
    fn a_change_taken_back_is_found_again() {
        let root = temp();
        std::fs::write(root.join("changed"), "a").unwrap();
        std::fs::write(root.join("removed"), "b").unwrap();
        let mut f = folder(&root, false, None);
        f.see(&root);
        std::fs::write(root.join("created"), "new").unwrap();
        std::fs::write(root.join("changed"), "abc").unwrap();
        std::fs::remove_file(root.join("removed")).unwrap();
        let first = f.see(&root);
        let found = events(&first);
        for Change { at, before, .. } in first {
            f.undo(at, before);
        }
        assert_eq!(events(&f.see(&root)), found, "each change is offered again");
        assert!(f.see(&root).is_empty(), "and once seen, no more");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
