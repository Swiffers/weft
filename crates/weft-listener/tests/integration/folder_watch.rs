//! Layer-3 contract tests for the folder watch: the real listener code
//! (registration's refusals, bring-up, the held watch over a real folder,
//! the fire path, the snapshot on the row) wired against a fake broker
//! that names the signal's worker and keeps signal rows with the same
//! compare-and-swap on `kind_state_seq` as `held_signals::write_kind_state`,
//! and a fake worker door recording the fires it is handed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use weft_core::signal::{to_spec, FolderEvent, FolderWatch};
use weft_core::signal::listener_protocol::{PriorRow, StartMode};
use weft_listener::kinds::{bring_up, prepare_signal, stop_here, SignalIdentity};
use weft_listener::{Holders, ListenerConfig, ListenerState};

/// The task store: an entry's event never becomes a task.
struct FakeTasks;

#[async_trait::async_trait]
impl weft_task_store::TaskStoreClient for FakeTasks {
    async fn enqueue_dedup(&self, _: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
        unreachable!("an entry's event never becomes a task")
    }
    async fn wait_for_terminal(&self, _: uuid::Uuid, _: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
        unreachable!("not used by the serving side")
    }
}

/// The project's worker, as the listener hands it events.
#[derive(Default)]
pub(crate) struct FakeDoors {
    fired: Mutex<Vec<weft_core::door_fire::DoorFire>>,
    /// The worker answers that this copy no longer holds the signal.
    not_held: AtomicBool,
}

#[async_trait::async_trait]
impl weft_listener::fire_sink::WorkerDoors for FakeDoors {
    async fn fire(&self, _: uuid::Uuid, _: &str, fire: &weft_core::door_fire::DoorFire) -> anyhow::Result<weft_core::door_fire::Fired> {
        if self.not_held.load(Ordering::SeqCst) {
            return Ok(weft_core::door_fire::Fired::NotHeld);
        }
        self.fired.lock().unwrap().push(fire.clone());
        Ok(weft_core::door_fire::Fired::Started { run_id: weft_core::door_fire::run_of_fire(fire.fire_id) })
    }
}

/// The signal rows the fake broker keeps, by token.
#[derive(Default)]
struct Rows {
    rows: Mutex<HashMap<String, Value>>,
    /// Every write of a kind state that landed.
    writes: AtomicUsize,
    /// The next write lands but its answer is lost (answered 500).
    lose_next_answer: AtomicBool,
    /// The broker cannot be asked where a fire goes (answered 500), so no
    /// fire is handed over.
    fires_fail: AtomicBool,
}

impl Rows {
    fn put(&self, row: Value) {
        self.rows.lock().unwrap().insert(row["token"].as_str().unwrap().to_string(), row);
    }

    fn row(&self, token: &str) -> Value {
        self.rows.lock().unwrap()[token].clone()
    }

    /// Another writer moves the row on, as a registration again or another
    /// holder's write would.
    fn move_on(&self, token: &str) {
        let mut rows = self.rows.lock().unwrap();
        let row = rows.get_mut(token).unwrap();
        row["kind_state_seq"] = json!(row["kind_state_seq"].as_i64().unwrap() + 1);
    }
}

/// The fake broker: every signal's worker answers at one address, and
/// the rows in `rows` are read and written as the broker's would be.
async fn spawn_broker_with(rows: Arc<Rows>) -> String {
    use axum::routing::post;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let held = rows.clone();
    let targets = rows.clone();
    let app = axum::Router::new()
        .route(
            "/v1/signal/fire_target",
            post(move || {
                let rows = targets.clone();
                async move {
                    if rows.fires_fail.load(Ordering::SeqCst) {
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                    Ok(axum::Json(json!({ "project_id": uuid::Uuid::nil(), "is_resume": false, "address": "http://worker.test" })))
                }
            }),
        )
        .route(
            "/v1/signal/get_held",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = held.clone();
                async move { axum::Json(json!({ "row": rows.rows.lock().unwrap().get(req["token"].as_str().unwrap()).cloned() })) }
            }),
        )
        .route(
            "/v1/signal/write_kind_state",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = rows.clone();
                async move {
                    let mut all = rows.rows.lock().unwrap();
                    let Some(row) = all.get_mut(req["token"].as_str().unwrap()) else {
                        return Ok(axum::Json(json!({ "written": false })));
                    };
                    let at = row["kind_state_seq"].as_i64().unwrap();
                    let written = req["from_seq"].as_i64().unwrap() == at;
                    if written {
                        row["kind_state"] = req["kind_state"].clone();
                        row["kind_state_seq"] = json!(at + 1);
                        rows.writes.fetch_add(1, Ordering::SeqCst);
                    }
                    if rows.lose_next_answer.swap(false, Ordering::SeqCst) {
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                    Ok(axum::Json(json!({ "written": written })))
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, weft_broker_client::line::server::with_line(app)).await.unwrap() });
    format!("http://{addr}")
}

pub(crate) async fn spawn_broker() -> String {
    spawn_broker_with(Arc::default()).await
}

pub(crate) fn listener_state(broker_base: String, doors: Arc<FakeDoors>, holders: Holders) -> ListenerState {
    let replica = format!("test-listener-{}", uuid::Uuid::new_v4().simple());
    ListenerState::new(
        ListenerConfig { replica: replica.clone(), holds_here: true, holders },
        Arc::new(FakeTasks),
        weft_broker_client::BrokerLink::new(
            broker_base,
            weft_broker_client::TokenSource::role(
                Arc::new(weft_platform_traits::FixedToken("test-token".into())),
                replica,
                weft_platform_traits::CoreRole::Listener,
            ),
        ),
        Arc::new(weft_platform_traits::FakeAlarm::new()),
        doors,
        weft_listener::HolderPlatform {
            news: Arc::new(weft_platform_traits::FakeHolderNews::default()),
            liveness: Arc::new(weft_platform_traits::OneProcessHolds),
        },
    )
}

/// A fresh folder of its own under the system's temporary folder.
fn temp() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("weft-folder-watch-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn watch_of(folder: &Path) -> FolderWatch {
    FolderWatch { folder: folder.to_string_lossy().into_owned(), recursive: false, pattern: Some("*.pdf".into()), events: FolderEvent::ALL.to_vec() }
}

/// A held row of `watch` under `token`, as the broker hands it over.
fn row_of(token: &str, watch: &FolderWatch, kind_state: Value, seq: i64) -> Value {
    json!({
        "token": token, "tenant_id": "tenant-a", "for_instance": null, "node_id": "watch",
        "spec_json": serde_json::to_string(&to_spec(watch.clone())).unwrap(), "is_resume": false, "run_id": null,
        "surface_kind": "internal", "mount_path": null, "mount_methods": [], "auth_kind": "none",
        "auth_config": null, "kind_state": kind_state, "kind_state_seq": seq, "holds": true, "serving": null, "generation": 1
    })
}

/// Bring the row the broker keeps under `token` up, as a holder taking it
/// would, and wait until it watches.
async fn take(state: &ListenerState, rows: &Rows, token: &str, mode: StartMode) {
    bring_up(state, serde_json::from_value(rows.row(token)).unwrap(), mode).await.expect("the watch comes up");
    wait_until(|| status(state, token) == "watching", "the watch to be up").await;
}

fn status(state: &ListenerState, token: &str) -> String {
    state.registry.get(token).map(|s| s.serving.lock().status.clone()).unwrap_or_default()
}

/// Stop the watch held here, as a holder stopping would, and give the
/// task a moment to be gone.
async fn stop(state: &ListenerState, token: &str) {
    stop_here(state, token);
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// The files a row's snapshot holds, by name.
fn snapshot_names(row: &Value) -> Vec<String> {
    row["kind_state"]["files"].as_array().unwrap().iter().map(|f| f[0].as_str().unwrap().to_string()).collect()
}

fn payloads(doors: &FakeDoors) -> Vec<Value> {
    doors.fired.lock().unwrap().iter().map(|f| f.payload.clone()).collect()
}

/// Longer than a look takes to come after a change ([`QUIET`] plus the
/// look itself), for a test that checks nothing happened.
const SETTLED: Duration = Duration::from_millis(2500);

fn identity(token: &str, kind: FolderWatch) -> SignalIdentity {
    SignalIdentity { token: token.into(), tenant_id: "tenant-a".into(), node_id: "watch".into(), is_resume: false, run_id: None, spec: to_spec(kind) }
}

async fn wait_until(mut check: impl FnMut() -> bool, what: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !check() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn registration_refuses_a_folder_this_install_cannot_watch() {
    let folder = temp();
    let doors = Arc::new(FakeDoors::default());

    // A cloud's holders run elsewhere: refused, saying why.
    let cloud = listener_state(spawn_broker().await, doors.clone(), Holders::Pool);
    let err = prepare_signal(&cloud, identity("t1", watch_of(&folder)), None, None, 0).await.unwrap_err();
    assert!(format!("{err:#}").contains("local install"), "{err:#}");

    // A folder that is not there is refused where somebody sees it.
    let local = listener_state(spawn_broker().await, doors, Holders::OnThisMachine);
    let missing = folder.join("nope");
    let err = prepare_signal(&local, identity("t2", watch_of(&missing)), None, None, 0).await.unwrap_err();
    assert!(format!("{err:#}").contains("nothing is there"), "{err:#}");

    let prepared = prepare_signal(&local, identity("t3", watch_of(&folder)), None, None, 0).await.expect("a folder that is there registers");
    assert!(prepared.holds, "a folder watch is held");
    std::fs::remove_dir_all(&folder).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_dropped_in_the_folder_fires_with_its_path() {
    let folder = temp();
    std::fs::write(folder.join("before.pdf"), "there first").unwrap();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &watch_of(&folder), json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;

    std::fs::write(folder.join("new.pdf"), "abc").unwrap();
    std::fs::write(folder.join("notes.txt"), "not a pdf").unwrap();
    wait_until(|| !doors.fired.lock().unwrap().is_empty(), "the fire").await;
    let path = folder.join("new.pdf").to_string_lossy().into_owned();
    assert_eq!(doors.fired.lock().unwrap()[0].payload, json!({ "path": path, "event": "created", "sizeBytes": 3 }));

    std::fs::remove_file(folder.join("new.pdf")).unwrap();
    wait_until(|| doors.fired.lock().unwrap().len() >= 2, "the removal").await;
    let payloads: Vec<Value> = doors.fired.lock().unwrap().iter().map(|f| f.payload.clone()).collect();
    assert_eq!(payloads[1], json!({ "path": path, "event": "removed" }));
    assert_eq!(payloads.len(), 2, "the file there first and the one the pattern refuses fired nothing: {payloads:?}");
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A watch over the folders under it starts even when one of them cannot
/// be read: a file made in a sibling fires. Once the locked folder can be
/// read, the change to it is heard, what it holds fires, and what is made
/// in it later is heard too.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_folder_inside_that_cannot_be_read_does_not_stop_the_watch() {
    use std::os::unix::fs::PermissionsExt;
    let folder = temp();
    std::fs::create_dir_all(folder.join("locked")).unwrap();
    std::fs::create_dir_all(folder.join("open")).unwrap();
    std::fs::set_permissions(folder.join("locked"), std::fs::Permissions::from_mode(0o000)).unwrap();
    let unlock = || std::fs::set_permissions(folder.join("locked"), std::fs::Permissions::from_mode(0o755)).unwrap();
    // Permissions bind nobody running as root: nothing to show there.
    if std::fs::read_dir(folder.join("locked")).is_ok() {
        unlock();
        std::fs::remove_dir_all(&folder).unwrap();
        return;
    }
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &FolderWatch { recursive: true, ..watch_of(&folder) }, json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;

    std::fs::write(folder.join("open/a.pdf"), "abc").unwrap();
    wait_until(|| !payloads(&doors).is_empty(), "the fire from the readable folder").await;
    let path = |rel: &str| folder.join(rel).to_string_lossy().into_owned();
    assert_eq!(payloads(&doors), vec![json!({ "path": path("open/a.pdf"), "event": "created", "sizeBytes": 3 })]);

    unlock();
    std::fs::write(folder.join("locked/b.pdf"), "b").unwrap();
    wait_until(|| payloads(&doors).len() >= 2, "the file in the folder once it can be read").await;
    assert_eq!(payloads(&doors)[1], json!({ "path": path("locked/b.pdf"), "event": "created", "sizeBytes": 1 }));
    std::fs::write(folder.join("locked/c.pdf"), "cc").unwrap();
    wait_until(|| payloads(&doors).len() >= 3, "a file made there later").await;
    assert_eq!(payloads(&doors)[2], json!({ "path": path("locked/c.pdf"), "event": "created", "sizeBytes": 2 }));
    assert_eq!(status(&state, &token), "watching");
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A new watch whose first attempts fail (a folder that cannot be read
/// cannot be watched) takes what is there once it is up, and fires
/// nothing for it: only what changes after.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_watch_that_failed_at_first_fires_nothing_for_what_was_there() {
    use std::os::unix::fs::PermissionsExt;
    let folder = temp();
    std::fs::write(folder.join("before.pdf"), "there first").unwrap();
    std::fs::set_permissions(&folder, std::fs::Permissions::from_mode(0o000)).unwrap();
    let unlock = || std::fs::set_permissions(&folder, std::fs::Permissions::from_mode(0o755)).unwrap();
    // Permissions bind nobody running as root: nothing to show there.
    if std::fs::read_dir(&folder).is_ok() {
        unlock();
        std::fs::remove_dir_all(&folder).unwrap();
        return;
    }
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &watch_of(&folder), json!({}), 1));
    bring_up(&state, serde_json::from_value(rows.row(&token)).unwrap(), StartMode::New).await.expect("the watch is taken");
    wait_until(|| status(&state, &token).starts_with("could not watch"), "the first attempt to fail").await;

    unlock();
    wait_until(|| status(&state, &token) == "watching", "the watch to be up").await;
    wait_until(|| rows.row(&token)["kind_state_seq"] == 2, "the baseline").await;
    assert_eq!(snapshot_names(&rows.row(&token)), vec!["before.pdf"]);
    std::fs::write(folder.join("new.pdf"), "abc").unwrap();
    wait_until(|| !payloads(&doors).is_empty(), "the fire for the new file").await;
    let path = folder.join("new.pdf").to_string_lossy().into_owned();
    assert_eq!(payloads(&doors), vec![json!({ "path": path, "event": "created", "sizeBytes": 3 })], "the file there first fired nothing");
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A folder under the watched one that is renamed away and back (notify
/// lets go of its watch at the first rename) is watched again at the look
/// its return brings: a file written into it afterwards still fires.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_folder_renamed_away_and_back_is_still_watched() {
    let folder = temp();
    std::fs::create_dir_all(folder.join("sub")).unwrap();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &FolderWatch { recursive: true, ..watch_of(&folder) }, json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;

    std::fs::rename(folder.join("sub"), folder.join("tmp")).unwrap();
    std::fs::rename(folder.join("tmp"), folder.join("sub")).unwrap();
    // The look the renames bring, which watches `sub` again.
    tokio::time::sleep(SETTLED).await;
    std::fs::write(folder.join("sub/a.pdf"), "abc").unwrap();
    wait_until(|| !payloads(&doors).is_empty(), "the fire from the folder that came back").await;
    let path = folder.join("sub/a.pdf").to_string_lossy().into_owned();
    assert_eq!(payloads(&doors), vec![json!({ "path": path, "event": "created", "sizeBytes": 3 })]);
    std::fs::remove_dir_all(&folder).unwrap();
}

/// The first watch a signal ever has fires nothing for what is there and
/// writes it down; once stopped, a file created, one changed and one
/// removed while nothing watched each fire as the watch is taken again,
/// in path order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_fires_what_changed_while_nothing_watched() {
    let folder = temp();
    std::fs::write(folder.join("changed.pdf"), "a").unwrap();
    std::fs::write(folder.join("kept.pdf"), "same").unwrap();
    std::fs::write(folder.join("removed.pdf"), "gone soon").unwrap();
    std::fs::write(folder.join("notes.txt"), "not a pdf").unwrap();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    let watch = watch_of(&folder);
    rows.put(row_of(&token, &watch, json!({}), 1));

    take(&state, &rows, &token, StartMode::New).await;
    let first = rows.row(&token);
    assert_eq!(first["kind_state_seq"], 2, "the first look's baseline is written once: {first}");
    assert_eq!(snapshot_names(&first), vec!["changed.pdf", "kept.pdf", "removed.pdf"], "only what the watch covers");
    assert_eq!(first["kind_state"].as_object().unwrap().keys().collect::<Vec<_>>(), vec!["files"], "the files and nothing else");
    assert_eq!(first["kind_state"]["files"][0][1], 1, "each file keeps its size");
    assert!(first["kind_state"]["files"][0][2].is_i64(), "and its modification time");
    assert!(payloads(&doors).is_empty(), "the files there at the very first watch fire nothing");

    stop(&state, &token).await;
    std::fs::write(folder.join("created.pdf"), "new").unwrap();
    std::fs::write(folder.join("changed.pdf"), "abcdef").unwrap();
    std::fs::remove_file(folder.join("removed.pdf")).unwrap();

    take(&state, &rows, &token, StartMode::Restore).await;
    wait_until(|| payloads(&doors).len() >= 3, "the three fires").await;
    let path = |name: &str| folder.join(name).to_string_lossy().into_owned();
    assert_eq!(
        payloads(&doors),
        vec![
            json!({ "path": path("changed.pdf"), "event": "changed", "sizeBytes": 6 }),
            json!({ "path": path("created.pdf"), "event": "created", "sizeBytes": 3 }),
            json!({ "path": path("removed.pdf"), "event": "removed" }),
        ]
    );
    wait_until(|| rows.row(&token)["kind_state_seq"] == 3, "the new snapshot").await;
    assert_eq!(snapshot_names(&rows.row(&token)), vec!["changed.pdf", "created.pdf", "kept.pdf"]);
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A restart fires only the events the trigger asked for, and the row
/// still moves past the ones it did not, so they never fire later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_fires_only_the_events_asked_for() {
    for only in FolderEvent::ALL {
        let folder = temp();
        std::fs::write(folder.join("changed.pdf"), "a").unwrap();
        std::fs::write(folder.join("removed.pdf"), "b").unwrap();
        let doors = Arc::new(FakeDoors::default());
        let rows = Arc::new(Rows::default());
        let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
        let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
        let watch = FolderWatch { events: vec![only], ..watch_of(&folder) };
        rows.put(row_of(&token, &watch, json!({}), 1));
        take(&state, &rows, &token, StartMode::New).await;
        stop(&state, &token).await;

        std::fs::write(folder.join("created.pdf"), "new").unwrap();
        std::fs::write(folder.join("changed.pdf"), "abcdef").unwrap();
        std::fs::remove_file(folder.join("removed.pdf")).unwrap();
        take(&state, &rows, &token, StartMode::Restore).await;
        wait_until(|| rows.row(&token)["kind_state_seq"] == 3, "the new snapshot").await;
        let fired: Vec<Value> = payloads(&doors).iter().map(|p| p["event"].clone()).collect();
        assert_eq!(fired, vec![json!(only.as_str())], "with events [{}]", only.as_str());
        assert_eq!(snapshot_names(&rows.row(&token)), vec!["changed.pdf", "created.pdf"]);

        stop(&state, &token).await;
        take(&state, &rows, &token, StartMode::Restore).await;
        tokio::time::sleep(SETTLED).await;
        assert_eq!(payloads(&doors).len(), 1, "what the row moved past fires nothing at the next start");
        std::fs::remove_dir_all(&folder).unwrap();
    }
}

/// A look that changes nothing known writes nothing: a restart over an
/// untouched folder, a change the pattern refuses, a change to a file's
/// mode alone. A real change after them writes once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_look_that_changed_nothing_writes_nothing() {
    use std::os::unix::fs::PermissionsExt;
    let folder = temp();
    std::fs::write(folder.join("a.pdf"), "a").unwrap();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &watch_of(&folder), json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;
    assert_eq!(rows.writes.load(Ordering::SeqCst), 1, "the baseline");

    stop(&state, &token).await;
    take(&state, &rows, &token, StartMode::Restore).await;
    std::fs::write(folder.join("notes.txt"), "not a pdf").unwrap();
    std::fs::set_permissions(folder.join("a.pdf"), std::fs::Permissions::from_mode(0o600)).unwrap();
    tokio::time::sleep(SETTLED).await;
    assert_eq!(rows.writes.load(Ordering::SeqCst), 1, "nothing known moved, so nothing was written");
    assert!(payloads(&doors).is_empty());

    std::fs::write(folder.join("b.pdf"), "b").unwrap();
    wait_until(|| rows.writes.load(Ordering::SeqCst) == 2, "the write after a real change").await;
    assert_eq!(payloads(&doors).len(), 1);
    tokio::time::sleep(SETTLED).await;
    assert_eq!(rows.writes.load(Ordering::SeqCst), 2, "one write per look that changed something");
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A write that finds the row moved under it (registered again, or taken
/// by another holder) means the watch is no longer this task's: it says
/// so and stops, firing nothing more and writing nothing more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watch_whose_row_moved_under_its_write_stops() {
    let folder = temp();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &watch_of(&folder), json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;

    rows.move_on(&token);
    let moved = rows.row(&token);
    std::fs::write(folder.join("a.pdf"), "a").unwrap();
    wait_until(|| status(&state, &token).starts_with("stopped:"), "the watch to stop").await;
    assert!(status(&state, &token).contains("taken over"), "{}", status(&state, &token));
    assert_eq!(payloads(&doors).len(), 1, "the look's fire went out before its write found the row moved");
    assert_eq!(rows.row(&token), moved, "the row stands as the other writer left it");

    std::fs::write(folder.join("b.pdf"), "b").unwrap();
    tokio::time::sleep(SETTLED).await;
    assert_eq!(payloads(&doors).len(), 1, "a stopped watch fires nothing");
    assert_eq!(rows.row(&token), moved);
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A write that landed but whose answer was lost is learned from the row
/// at the next write, which then lands one version on: the watch goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_whose_answer_was_lost_is_learned_from_the_row() {
    let folder = temp();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &watch_of(&folder), json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;
    assert_eq!(rows.row(&token)["kind_state_seq"], 2);

    rows.lose_next_answer.store(true, Ordering::SeqCst);
    std::fs::write(folder.join("a.pdf"), "a").unwrap();
    wait_until(|| rows.row(&token)["kind_state_seq"] == 3, "the write that landed").await;
    std::fs::write(folder.join("b.pdf"), "b").unwrap();
    wait_until(|| rows.row(&token)["kind_state_seq"] == 4, "the next write, one version on").await;
    assert_eq!(snapshot_names(&rows.row(&token)), vec!["a.pdf", "b.pdf"]);
    assert_eq!(status(&state, &token), "watching");
    assert_eq!(payloads(&doors).len(), 2);
    std::fs::remove_dir_all(&folder).unwrap();
}

/// The row a registration again replaces: `watch`'s, holding `seen`.
fn prior(watch: &FolderWatch, seen: &Value, listened: bool) -> PriorRow {
    PriorRow { spec: to_spec(watch.clone()), kind_state: seen.clone(), listened }
}

/// A registration again carries the snapshot over when the trigger
/// listened the whole time and watches the same files, whichever events
/// fire; after a door that refused, or for another folder, `recursive` or
/// pattern, it starts from nothing, so what changed meanwhile never fires.
#[tokio::test]
async fn a_registration_again_carries_the_snapshot_only_when_it_listened_to_the_same_files() {
    let folder = temp();
    std::fs::create_dir_all(folder.join("sub")).unwrap();
    let state = listener_state(spawn_broker().await, Arc::new(FakeDoors::default()), Holders::OnThisMachine);
    let watch = watch_of(&folder);
    let seen = json!({ "files": [["a.pdf", 1, 5]] });
    let prepare = |watch: FolderWatch, prior: PriorRow| {
        let state = &state;
        async move { prepare_signal(state, identity("t", watch), None, Some(&prior), 0).await }
    };

    let again = prepare(watch.clone(), prior(&watch, &seen, true)).await.unwrap();
    assert_eq!(again.kind_state, seen, "listened, same files");
    let narrower = FolderWatch { events: vec![FolderEvent::Created], ..watch.clone() };
    assert_eq!(prepare(narrower, prior(&watch, &seen, true)).await.unwrap().kind_state, seen, "which events fire is not the source");

    let refused = prepare(watch.clone(), prior(&watch, &seen, false)).await.unwrap();
    assert_eq!(refused.kind_state, json!({}), "behind a door that refused, nothing is caught up");
    for other in [
        FolderWatch { recursive: true, ..watch.clone() },
        FolderWatch { pattern: None, ..watch.clone() },
        FolderWatch { folder: folder.join("sub").to_string_lossy().into_owned(), ..watch.clone() },
    ] {
        let fresh = prepare(other.clone(), prior(&watch, &seen, true)).await.unwrap();
        assert_eq!(fresh.kind_state, json!({}), "{other:?}");
    }

    let err = prepare(watch.clone(), prior(&watch, &json!({ "folder": 3 }), true)).await.unwrap_err();
    assert!(format!("{err:#}").contains("malformed folder_watch state"), "{err:#}");
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A fire that could not be handed over leaves its file as it was last
/// seen: the row does not move past it, the next look of the file offers
/// the same change again (a creation, never a change of a file it never
/// delivered), and a restart offers it too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fire_that_was_not_handed_over_is_offered_again() {
    let folder = temp();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &watch_of(&folder), json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;
    assert_eq!(rows.writes.load(Ordering::SeqCst), 1, "the baseline");

    rows.fires_fail.store(true, Ordering::SeqCst);
    std::fs::write(folder.join("a.pdf"), "a").unwrap();
    tokio::time::sleep(SETTLED).await;
    assert!(payloads(&doors).is_empty(), "nothing was handed over");
    assert_eq!(rows.writes.load(Ordering::SeqCst), 1, "and the row did not move past it");
    assert_eq!(status(&state, &token), "watching");

    rows.fires_fail.store(false, Ordering::SeqCst);
    std::fs::write(folder.join("a.pdf"), "a").unwrap();
    wait_until(|| !payloads(&doors).is_empty(), "the change offered again").await;
    let path = folder.join("a.pdf").to_string_lossy().into_owned();
    assert_eq!(payloads(&doors), vec![json!({ "path": path, "event": "created", "sizeBytes": 1 })]);
    wait_until(|| rows.writes.load(Ordering::SeqCst) == 2, "the write once it went").await;

    rows.fires_fail.store(true, Ordering::SeqCst);
    std::fs::remove_file(folder.join("a.pdf")).unwrap();
    tokio::time::sleep(SETTLED).await;
    rows.fires_fail.store(false, Ordering::SeqCst);
    stop(&state, &token).await;
    take(&state, &rows, &token, StartMode::Restore).await;
    wait_until(|| payloads(&doors).len() == 2, "the removal offered at the next start").await;
    assert_eq!(payloads(&doors)[1], json!({ "path": path, "event": "removed" }));
    std::fs::remove_dir_all(&folder).unwrap();
}

/// A fire that finds the signal no longer held here stops the watch at
/// once: it writes nothing past what it could not deliver, and fires
/// nothing more.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fire_that_finds_the_signal_held_elsewhere_stops_the_watch() {
    let folder = temp();
    let doors = Arc::new(FakeDoors::default());
    let rows = Arc::new(Rows::default());
    let state = listener_state(spawn_broker_with(rows.clone()).await, doors.clone(), Holders::OnThisMachine);
    let token = format!("sig-{}", uuid::Uuid::new_v4().simple());
    rows.put(row_of(&token, &watch_of(&folder), json!({}), 1));
    take(&state, &rows, &token, StartMode::New).await;

    doors.not_held.store(true, Ordering::SeqCst);
    std::fs::write(folder.join("a.pdf"), "a").unwrap();
    wait_until(|| status(&state, &token).starts_with("stopped:"), "the watch to stop").await;
    assert!(status(&state, &token).contains("taken over"), "{}", status(&state, &token));
    assert_eq!(rows.writes.load(Ordering::SeqCst), 1, "nothing written past the fire it could not deliver");

    doors.not_held.store(false, Ordering::SeqCst);
    std::fs::write(folder.join("b.pdf"), "b").unwrap();
    tokio::time::sleep(SETTLED).await;
    assert!(payloads(&doors).is_empty(), "a stopped watch fires nothing");
    assert_eq!(rows.writes.load(Ordering::SeqCst), 1);
    std::fs::remove_dir_all(&folder).unwrap();
}
