//! Keeping every folder working copy on this machine in sync with its
//! folder, from inside the local install's own process: a save in the
//! folder reaches its working copy, and a program's write into the
//! working copy reaches the folder. One pass is `weft_assets::sync::sync_folder`;
//! this module decides when a folder gets one.
//!
//! A folder gets a pass when it has changed and then stayed quiet for a
//! moment (a burst of saves, a `git checkout`, is one pass), and at once
//! when the install says its working copy changed. Passes of one folder
//! never overlap: a reason to look that arrives during a pass makes one
//! more pass after it. On start every folder gets a pass, because the
//! person may have kept editing while the install was down.
//!
//! Nothing runs while nothing changes: the disk is heard through the
//! operating system's change events (inotify on Linux), the install
//! through its notices, and the only timer is the quiet moment after a
//! change. The watches are on the folders a version's walk goes into and
//! nothing else, so `.git`, `.weft`, a `target/` or a `node_modules`
//! costs no watch and no wake. They are set again before every pass,
//! which is how a folder created since is watched: the pass walks
//! whatever landed in it before its watch was there.
//!
//! A folder that is gone (deleted, moved away) is said once and left
//! alone, with a watch on the nearest folder above it that still exists,
//! so it is picked up again the moment it is back. A pass that fails is
//! logged with its folder, and the next change tries again. A folder this
//! machine can never hear changes in (a Windows drive under WSL,
//! `weft_core::working_copy::unsyncable_folder`) is said once and never
//! watched.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;
use weft_assets::sync::{sync_folder, FolderCopies, FolderCopy, Notice, Synced};
use weft_assets::tree::{walk_takes, HashCache};
use weft_core::project::coverage::is_conflict_copy;
use weft_core::working_copy::{on_wsl, unsyncable_folder, WorkingCopyId};

/// How long a folder stays quiet after a change before its pass: long
/// enough that a burst of saves is one pass, short enough that a save is
/// in the working copy about a second later.
pub const QUIET: Duration = Duration::from_secs(1);

/// What wakes the service.
enum Event {
    /// Something a version could hold moved in this copy's folder, or on
    /// the way to it.
    Disk(WorkingCopyId),
    /// What the install told.
    Notice(Result<Notice>),
    /// A pass ended, handing its hash cache back (boxed: a pass's answer is
    /// far larger than the other events).
    Done(Box<PassEnded>),
}

/// What a pass hands back when it ends.
struct PassEnded {
    id: WorkingCopyId,
    cache: HashCache,
    outcome: Result<Synced>,
}

/// One folder the service keeps.
struct Folder {
    copy: FolderCopy,
    /// `None` for a folder this machine can never keep in sync.
    watch: Option<Watch>,
    /// Taken by a running pass.
    cache: Option<HashCache>,
    running: bool,
    /// A reason to look arrived during the running pass.
    again: bool,
    /// When the folder's quiet moment ends, after a change.
    due: Option<Instant>,
    /// It is gone, and that was said.
    missing: bool,
}

/// The change events of one folder: the folders under it a version's
/// walk goes into, or, while it is gone, the nearest folder above it.
struct Watch {
    watcher: RecommendedWatcher,
    dirs: BTreeSet<PathBuf>,
}

/// Keep every folder working copy in sync with its folder, for the life
/// of the process. Ends only with an error: the install no longer telling
/// anything, which nothing could ever wake a folder from again.
pub async fn keep_in_sync(copies: Arc<dyn FolderCopies>, quiet: Duration) -> Result<()> {
    let (tx, rx) = unbounded_channel();
    // Heard before the first look at the folders, so nothing changed in
    // between is missed.
    let mut notices = copies.notices()?;
    let told = tx.clone();
    let forward = tokio::spawn(async move {
        loop {
            let notice = notices.next().await;
            let failed = notice.is_err();
            if told.send(Event::Notice(notice)).is_err() || failed {
                return;
            }
        }
    });
    let mut service = Service { copies, quiet, tx, folders: BTreeMap::new() };
    let ended = service.run(rx).await;
    forward.abort();
    ended
}

struct Service {
    copies: Arc<dyn FolderCopies>,
    quiet: Duration,
    tx: UnboundedSender<Event>,
    folders: BTreeMap<WorkingCopyId, Folder>,
}

impl Service {
    async fn run(&mut self, mut rx: UnboundedReceiver<Event>) -> Result<()> {
        self.relist(true).await;
        loop {
            let due = self.folders.values().filter_map(|f| f.due).min();
            let event = match due {
                Some(at) => tokio::select! {
                    event = rx.recv() => event,
                    () = tokio::time::sleep_until(at) => {
                        self.quiet_ended();
                        continue;
                    }
                },
                None => rx.recv().await,
            };
            match event.expect("the service holds a sender of its own channel") {
                Event::Disk(id) => {
                    if let Some(folder) = self.folders.get_mut(&id) {
                        folder.due = Some(Instant::now() + self.quiet);
                    }
                }
                Event::Notice(notice) => match notice.context("hear the install's working copy notices")? {
                    Notice::Changed(id) => {
                        // A copy that is not a folder kept here (a
                        // program's copy) is not this service's; a new
                        // folder arrives with `Added`.
                        if self.folders.contains_key(&id) {
                            self.request_pass(id);
                        }
                    }
                    Notice::Added => self.relist(false).await,
                    Notice::Recheck => self.relist(true).await,
                },
                Event::Done(ended) => {
                    let PassEnded { id, cache, outcome } = *ended;
                    self.pass_done(id, cache, outcome).await
                }
            }
        }
    }

    /// Every folder whose quiet moment is over gets its pass.
    fn quiet_ended(&mut self) {
        let now = Instant::now();
        let due: Vec<WorkingCopyId> = self.folders.iter().filter(|(_, f)| f.due.is_some_and(|at| at <= now)).map(|(id, _)| *id).collect();
        for id in due {
            if let Some(folder) = self.folders.get_mut(&id) {
                folder.due = None;
            }
            self.request_pass(id);
        }
    }

    /// Read the install's folders again: keep the new ones (each with a
    /// first pass), let go of the ones it no longer has, and with
    /// `every`, give every folder a pass.
    async fn relist(&mut self, every: bool) {
        let listed = match self.copies.folders().await {
            Ok(listed) => listed,
            Err(e) => {
                tracing::error!(target: "weft_platform_local::folders", error = %format!("{e:#}"), "could not list the folder working copies; the next notice tries again");
                return;
            }
        };
        let ids: BTreeSet<WorkingCopyId> = listed.iter().map(|c| c.id).collect();
        self.folders.retain(|id, _| ids.contains(id));
        let mut look = Vec::new();
        for copy in listed {
            let id = copy.id;
            if !self.folders.contains_key(&id) {
                self.keep(copy);
                look.push(id);
            } else if every {
                look.push(id);
            }
        }
        for id in look {
            self.request_pass(id);
        }
    }

    /// Start keeping one folder.
    fn keep(&mut self, copy: FolderCopy) {
        let refused = unsyncable_folder(&copy.root.to_string_lossy(), on_wsl());
        let watch = match &refused {
            Some(why) => {
                tracing::error!(target: "weft_platform_local::folders", copy = %copy.id, folder = %copy.root.display(), "{why}");
                None
            }
            None => match self.watcher(&copy) {
                Ok(watcher) => Some(Watch { watcher, dirs: BTreeSet::new() }),
                Err(e) => {
                    tracing::error!(target: "weft_platform_local::folders", copy = %copy.id, folder = %copy.root.display(), error = %format!("{e:#}"), "could not watch the folder; it is not kept in sync");
                    None
                }
            },
        };
        let cache = HashCache::load(&copy.root);
        self.folders.insert(copy.id, Folder { copy, watch, cache: Some(cache), running: false, again: false, due: None, missing: false });
    }

    /// A watcher whose events for `copy` become [`Event::Disk`]: any that
    /// a version could hold, or that is on the way to the folder (the
    /// folder itself going or coming back).
    fn watcher(&self, copy: &FolderCopy) -> Result<RecommendedWatcher> {
        let tx = self.tx.clone();
        let id = copy.id;
        let root = copy.root.clone();
        let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let relevant = match event {
                // Reading a file is no change, and every pass reads them.
                Ok(event) if matches!(event.kind, EventKind::Access(_)) => false,
                Ok(event) => event.need_rescan() || event.paths.iter().any(|path| relevant(&root, path)),
                // Events may have been lost (the queue overflowed): look.
                Err(_) => true,
            };
            if relevant {
                // Gone only when the service is, which drops this watcher.
                let _ = tx.send(Event::Disk(id));
            }
        })?;
        Ok(watcher)
    }

    /// Give `id` a pass now, or right after the one running.
    fn request_pass(&mut self, id: WorkingCopyId) {
        let Some(folder) = self.folders.get_mut(&id) else { return };
        let Some(watch) = folder.watch.as_mut() else { return };
        if folder.running {
            folder.again = true;
            return;
        }
        let root = folder.copy.root.clone();
        if !root.is_dir() {
            if !folder.missing {
                folder.missing = true;
                tracing::warn!(target: "weft_platform_local::folders", copy = %id, folder = %root.display(), "the folder is gone; it is kept in sync again once it is back");
            }
            watch_above(watch, &root);
            return;
        }
        if std::mem::take(&mut folder.missing) {
            tracing::info!(target: "weft_platform_local::folders", copy = %id, folder = %root.display(), "the folder is back; syncing it");
        }
        // Watched before the pass walks, so a change after the walk is
        // heard and one before it is in the walk.
        if let Err(e) = watch_folder(watch, &root) {
            tracing::error!(target: "weft_platform_local::folders", copy = %id, folder = %root.display(), error = %format!("{e:#}"), "could not watch every part of the folder; changes there are synced only with the next pass");
        }
        let mut cache = folder.cache.take().expect("a folder with no pass running holds its cache");
        folder.running = true;
        let copy = folder.copy.clone();
        let copies = self.copies.clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let outcome = async {
                let reach = copies.reach(&copy).await.context("reach the working copy")?;
                sync_folder(&copy.root, &*reach.port, &*reach.fetch, &*reach.store, &mut cache).await
            }
            .await;
            // Never under a folder that went during the pass: saving would
            // make it again.
            if copy.root.is_dir() {
                if let Err(e) = cache.save(&copy.root) {
                    tracing::warn!(target: "weft_platform_local::folders", copy = %copy.id, folder = %copy.root.display(), error = %format!("{e:#}"), "could not save the folder's hash cache; the next start hashes every file again");
                }
            }
            let _ = tx.send(Event::Done(Box::new(PassEnded { id: copy.id, cache, outcome })));
        });
    }

    async fn pass_done(&mut self, id: WorkingCopyId, cache: HashCache, outcome: Result<Synced>) {
        let Some(folder) = self.folders.get_mut(&id) else { return };
        folder.running = false;
        folder.cache = Some(cache);
        let root = folder.copy.root.display().to_string();
        let failed = match outcome {
            Ok(synced) => {
                if !synced.pushed.is_empty() || !synced.written.is_empty() {
                    tracing::info!(target: "weft_platform_local::folders", copy = %id, folder = %root, pushed = ?synced.pushed, written = ?synced.written, "synced the folder");
                }
                for conflict in &synced.conflicts {
                    let said = match (&conflict.folder, &conflict.copy_path) {
                        (Some(_), Some(_)) => "the file changed in the folder and in its working copy; the folder's is kept and the working copy's is beside it (`weft resolve` picks a side)",
                        (None, Some(_)) => "the file was removed in the folder and changed in its working copy; it stays removed and the working copy's is beside it (`weft resolve` picks a side)",
                        (_, None) => "the file changed in the folder and was removed in its working copy; the folder's is kept (delete it for the removal)",
                    };
                    tracing::warn!(target: "weft_platform_local::folders", copy = %id, folder = %root, path = %conflict.path, copy_path = ?conflict.copy_path, "{said}");
                }
                false
            }
            Err(e) => {
                if folder.copy.root.is_dir() {
                    tracing::error!(target: "weft_platform_local::folders", copy = %id, folder = %root, error = %format!("{e:#}"), "could not sync the folder; the next change tries again");
                }
                true
            }
        };
        let again = std::mem::take(&mut folder.again);
        // A copy removed from the install fails its pass: let it go.
        if failed {
            self.relist(false).await;
        }
        if again || failed {
            // After a failure this only says the folder is gone, if it is;
            // a pass runs only when something changed during the last.
            self.look_again(id, again);
        }
    }

    /// After a pass: run another when something changed during it, and
    /// otherwise only see whether the folder is still there.
    fn look_again(&mut self, id: WorkingCopyId, changed: bool) {
        if changed {
            self.request_pass(id);
            return;
        }
        let Some(folder) = self.folders.get_mut(&id) else { return };
        if folder.copy.root.is_dir() {
            return;
        }
        if let Some(watch) = folder.watch.as_mut() {
            if !folder.missing {
                folder.missing = true;
                tracing::warn!(target: "weft_platform_local::folders", copy = %id, folder = %folder.copy.root.display(), "the folder is gone; it is kept in sync again once it is back");
            }
            watch_above(watch, &folder.copy.root);
        }
    }
}

/// Whether an event at `path` concerns the folder at `root`: on the way
/// to it (the folder itself, or one above it, came or went), or inside it
/// and [`matters`].
fn relevant(root: &Path, path: &Path) -> bool {
    if root.starts_with(path) {
        return true;
    }
    let Ok(rel) = path.strip_prefix(root) else { return false };
    matters(root, &rel.to_string_lossy().replace('\\', "/"), path.is_file())
}

/// Whether a change at `rel` (project-relative) can change what the
/// folder syncs: a file a version takes, a conflict copy (the working
/// copy lists them), or a folder the walk goes into, come or gone (a
/// folder moved in or out arrives as one event, for the folder).
fn matters(root: &Path, rel: &str, is_file: bool) -> bool {
    if walk_takes(root, rel) {
        return true;
    }
    let (dir, name) = rel.rsplit_once('/').unwrap_or(("", rel));
    if is_conflict_copy(name) {
        return walked(root, dir);
    }
    !is_file && walked(root, rel)
}

/// Whether the walk of a version goes into the folder `dir`
/// (project-relative, `""` the root): some file in it could be taken.
fn walked(root: &Path, dir: &str) -> bool {
    let inside = |leaf: &str| match dir {
        "" => leaf.to_string(),
        dir => format!("{dir}/{leaf}"),
    };
    walk_takes(root, &inside("x")) || walk_takes(root, &inside("x.weft"))
}

/// Every folder under `root` the walk goes into, `root` included.
fn walked_dirs(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut out = BTreeSet::from([root.to_path_buf()]);
    let mut todo = vec![root.to_path_buf()];
    while let Some(dir) = todo.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            // Removed since its parent was read: the disk changing.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("read {}", dir.display())),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path();
            let rel = path.strip_prefix(root).expect("under root").to_string_lossy().replace('\\', "/");
            if walked(root, &rel) {
                out.insert(path.clone());
                todo.push(path);
            }
        }
    }
    Ok(out)
}

/// Watch exactly the folders the walk goes into under `root`.
fn watch_folder(watch: &mut Watch, root: &Path) -> Result<()> {
    let wanted = walked_dirs(root)?;
    set_watches(watch, wanted)
}

/// Watch only the nearest folder above `root` that exists, to hear it
/// come back.
fn watch_above(watch: &mut Watch, root: &Path) {
    let Some(above) = root.ancestors().skip(1).find(|dir| dir.is_dir()) else { return };
    if let Err(e) = set_watches(watch, BTreeSet::from([above.to_path_buf()])) {
        tracing::error!(target: "weft_platform_local::folders", folder = %root.display(), error = %format!("{e:#}"), "could not watch for the folder coming back; it is synced again with the next notice once it is");
    }
}

fn set_watches(watch: &mut Watch, wanted: BTreeSet<PathBuf>) -> Result<()> {
    for gone in watch.dirs.difference(&wanted) {
        // A folder already removed took its watch with it.
        let _ = watch.watcher.unwatch(gone);
    }
    let mut failed = None;
    let mut watched = BTreeSet::new();
    for dir in &wanted {
        if watch.dirs.contains(dir) {
            watched.insert(dir.clone());
            continue;
        }
        match watch.watcher.watch(dir, RecursiveMode::NonRecursive) {
            Ok(()) => {
                watched.insert(dir.clone());
            }
            // Removed since the walk: the disk changing, and its event says so.
            Err(_) if !dir.exists() => {}
            Err(e) => {
                failed.get_or_insert(anyhow::Error::new(e).context(format!(
                    "watch {} (when the machine is out of file watches, raise fs.inotify.max_user_watches)",
                    dir.display()
                )));
            }
        }
    }
    watch.dirs = watched;
    failed.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use weft_assets::sync::{BlobFetch, Notices, Reach, WorkingCopyPort};
    use weft_assets::AssetStore;
    use weft_core::project::hash::Manifest;
    use weft_core::working_copy::{apply_changes, fold_synced, Applied, Head, SyncReport, WorkingCopy, WorkingCopyKind, WorkingState};

    /// Long enough that a burst of writes made back to back is heard
    /// within it, short enough for a test.
    const TEST_QUIET: Duration = Duration::from_millis(300);
    /// How long a test waits for a pass it expects.
    const PATIENCE: Duration = Duration::from_secs(20);

    /// The install, as dumb as it gets: the folders, one working copy's
    /// files per folder, the stored blobs, every pass in order, and the
    /// notices a test sends.
    #[derive(Default)]
    struct Install {
        folders: Mutex<Vec<FolderCopy>>,
        working: Mutex<BTreeMap<WorkingCopyId, (Manifest, Manifest)>>,
        blobs: Mutex<BTreeMap<String, Vec<u8>>>,
        passes: Mutex<Vec<WorkingCopyId>>,
        counted: tokio::sync::Notify,
        notices: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<Notice>>>,
    }

    struct Heard(tokio::sync::mpsc::UnboundedReceiver<Notice>);

    #[async_trait::async_trait]
    impl Notices for Heard {
        async fn next(&mut self) -> Result<Notice> {
            self.0.recv().await.context("the test stopped telling")
        }
    }

    #[async_trait::async_trait]
    impl FolderCopies for Install {
        fn notices(&self) -> Result<Box<dyn Notices>> {
            Ok(Box::new(Heard(self.notices.lock().unwrap().take().context("heard once")?)))
        }

        async fn folders(&self) -> Result<Vec<FolderCopy>> {
            Ok(self.folders.lock().unwrap().clone())
        }

        async fn reach(&self, copy: &FolderCopy) -> Result<Reach<'_>> {
            self.passes.lock().unwrap().push(copy.id);
            self.counted.notify_waiters();
            Ok(Reach { port: Box::new(Port(self, copy.id)), fetch: Box::new(Store(self)), store: Box::new(Store(self)) })
        }
    }

    struct Port<'a>(&'a Install, WorkingCopyId);

    #[async_trait::async_trait]
    impl WorkingCopyPort for Port<'_> {
        async fn state(&self) -> Result<WorkingState> {
            let (working, synced) = self.0.working.lock().unwrap().get(&self.1).cloned().unwrap_or_default();
            let copy = WorkingCopy {
                id: self.1,
                kind: WorkingCopyKind::Folder,
                host: None,
                path: None,
                head: Head::default(),
                working_version: weft_core::project::hash::manifest_version_id(&working),
                revision: 0,
                conflicts: Vec::new(),
                updated_at: 0,
            };
            Ok(WorkingState { copy, working, synced })
        }

        async fn sync(&self, report: &SyncReport) -> Result<Applied> {
            {
                let mut all = self.0.working.lock().unwrap();
                let (working, synced) = all.entry(self.1).or_default();
                *working = apply_changes(working, &report.changes).map_err(|paths| anyhow::anyhow!("stale {paths:?}"))?;
                *synced = fold_synced(synced, &report.synced);
            }
            self.0.counted.notify_waiters();
            Ok(Applied::Applied { copy: self.state().await?.copy })
        }
    }

    struct Store<'a>(&'a Install);

    #[async_trait::async_trait]
    impl BlobFetch for Store<'_> {
        async fn fetch(&self, hash: &str, dest: &Path) -> Result<()> {
            let bytes = self.0.blobs.lock().unwrap().get(hash).cloned().context("no such blob")?;
            std::fs::write(dest, bytes)?;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl AssetStore for Store<'_> {
        async fn held(&self, hashes: &[String]) -> Result<BTreeMap<String, String>> {
            let blobs = self.0.blobs.lock().unwrap();
            Ok(hashes.iter().filter(|h| blobs.contains_key(*h)).map(|h| (h.clone(), format!("t/asset/{h}"))).collect())
        }

        async fn upload(&self, hash: &str, _mime: &str, _filename: &str, _size: u64, bytes: &mut (dyn std::io::Read + Send)) -> Result<String> {
            let mut all = Vec::new();
            bytes.read_to_end(&mut all)?;
            self.0.blobs.lock().unwrap().insert(hash.to_string(), all);
            Ok(format!("t/asset/{hash}"))
        }

        fn interrupted(&self, _error: &anyhow::Error) -> bool {
            false
        }
    }

    /// One folder kept by a running service.
    struct Rig {
        install: Arc<Install>,
        dir: tempfile::TempDir,
        copy: WorkingCopyId,
        tell: tokio::sync::mpsc::UnboundedSender<Notice>,
        service: tokio::task::JoinHandle<Result<()>>,
    }

    impl Rig {
        /// A folder holding `main.weft`, and the service having made its
        /// first pass over it.
        async fn start() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("project");
            std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join("main.weft"), "a = Text").unwrap();
            let copy = uuid::Uuid::now_v7();
            let (tell, heard) = unbounded_channel();
            let install = Arc::new(Install::default());
            install.folders.lock().unwrap().push(FolderCopy { id: copy, root });
            *install.notices.lock().unwrap() = Some(heard);
            let service = tokio::spawn(keep_in_sync(install.clone(), TEST_QUIET));
            let rig = Rig { install, dir, copy, tell, service };
            rig.passes_reach(1).await;
            rig
        }

        fn root(&self) -> PathBuf {
            self.dir.path().join("project")
        }

        fn passes(&self) -> usize {
            self.install.passes.lock().unwrap().len()
        }

        /// Wait until `holds` says yes, asked again after every pass that
        /// starts or lands; false past the patience.
        async fn until(&self, holds: impl Fn() -> bool) -> bool {
            tokio::time::timeout(PATIENCE, async {
                loop {
                    // Armed before the look, so a pass between the two
                    // is not missed.
                    let counted = self.install.counted.notified();
                    if holds() {
                        return;
                    }
                    counted.await;
                }
            })
            .await
            .is_ok()
        }

        /// Wait until `n` passes have started.
        async fn passes_reach(&self, n: usize) {
            assert!(self.until(|| self.passes() >= n).await, "waited for {n} passes, saw {}", self.passes());
        }

        /// Wait until the working copy holds `path`.
        async fn working_holds(&self, path: &str) {
            let landed = self.until(|| self.install.working.lock().unwrap().get(&self.copy).is_some_and(|(w, _)| w.contains_key(path))).await;
            assert!(landed, "{path} never reached the working copy");
        }

        /// No pass beyond `n` within a few quiet moments: a pass that was
        /// coming would have started by then.
        async fn stays_at(&self, n: usize) {
            tokio::time::sleep(TEST_QUIET * 4).await;
            assert_eq!(self.passes(), n, "{:?}", self.install.passes.lock().unwrap());
        }
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            self.service.abort();
        }
    }

    weft_core::stress_test!(
        name: a_save_in_the_folder_is_one_pass_that_carries_it,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            let rig = Rig::start().await;
            rig.working_holds("main.weft").await;
            std::fs::create_dir(rig.root().join("prompts")).unwrap();
            std::fs::write(rig.root().join("prompts/tone.txt"), "be kind").unwrap();
            rig.passes_reach(2).await;
            rig.working_holds("prompts/tone.txt").await;
            rig.stays_at(2).await;
        }
    );

    weft_core::stress_test!(
        name: a_notice_for_the_copy_is_one_pass,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            let rig = Rig::start().await;
            rig.tell.send(Notice::Changed(rig.copy)).unwrap();
            rig.passes_reach(2).await;
            // A notice for a copy this service does not keep is not its.
            rig.tell.send(Notice::Changed(uuid::Uuid::now_v7())).unwrap();
            rig.stays_at(2).await;
        }
    );

    weft_core::stress_test!(
        name: a_burst_of_saves_is_one_pass,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            let rig = Rig::start().await;
            for n in 0..30 {
                std::fs::write(rig.root().join(format!("f{n}.weft")), format!("n = {n}")).unwrap();
            }
            rig.passes_reach(2).await;
            rig.working_holds("f29.weft").await;
            rig.stays_at(2).await;
        }
    );

    weft_core::stress_test!(
        name: a_folder_that_goes_is_left_alone_until_it_is_back,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            let rig = Rig::start().await;
            std::fs::remove_dir_all(rig.root()).unwrap();
            rig.stays_at(1).await;
            // Not even a notice makes a pass over a folder that is not there.
            rig.tell.send(Notice::Changed(rig.copy)).unwrap();
            rig.stays_at(1).await;
            std::fs::create_dir(rig.root()).unwrap();
            std::fs::write(rig.root().join("main.weft"), "b = Text").unwrap();
            rig.passes_reach(2).await;
        }
    );

    #[test]
    fn what_matters_is_what_a_version_or_the_conflict_list_could_hold() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        assert!(matters(root, "main.weft", true));
        assert!(matters(root, "prompts/tone.txt", true));
        assert!(matters(root, "prompts", false), "a folder moved in whole");
        assert!(matters(root, "prompts/tone.weft-conflict-1a2b3c4d.txt", true), "the working copy lists its conflict copies");
        assert!(!matters(root, ".weft/hash-cache.json", true), "every pass writes it");
        assert!(!matters(root, ".git/index", true));
        assert!(!matters(root, ".git", false));
        assert!(!matters(root, "target/debug/build", false));
        assert!(!matters(root, "nodes/x/node_modules/a.js", true));
        assert!(!matters(root, "README.md", true), "no version holds it");
        // Cargo's output under a node is not walked, so not watched.
        std::fs::create_dir_all(root.join("nodes/x/target")).unwrap();
        std::fs::write(root.join("nodes/x/Cargo.toml"), "").unwrap();
        assert!(!matters(root, "nodes/x/target/debug", false));
        assert!(matters(root, "nodes/x/src/lib.rs", true));
        let dirs = walked_dirs(root).unwrap();
        assert!(dirs.contains(&root.join("nodes/x")) && !dirs.contains(&root.join("nodes/x/target")), "{dirs:?}");
        // The folder itself, or one on its way, is always looked at.
        assert!(relevant(root, root));
        assert!(relevant(root, root.parent().unwrap()));
        assert!(!relevant(root, &root.parent().unwrap().join("elsewhere")));
    }
}
