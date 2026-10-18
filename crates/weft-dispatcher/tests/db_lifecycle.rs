//! Layer-3 tests for the dispatcher's OWN SQL, against a REAL Postgres.
//! The dispatcher's correctness-critical decisions (task stamping, the
//! atomic run birth, the lifecycle writes) live in SQL statements
//! the fake stores never execute, so a faked layer cannot catch their
//! bugs. These tests exercise the actual statements.
//!
//! Each test gets a fresh isolated database via `#[sqlx::test]` (it reads
//! `$DATABASE_URL`, creates a random DB, drops it after). When
//! `DATABASE_URL` is unset the macro skips the test, so a dev box without
//! Postgres still builds. The schema is the REAL migration path in
//! production order: one pass over the dispatcher's groups
//! (`app::DISPATCHER_GROUPS`), then the journal and store wrap the pool,
//! the way a booting dispatcher does.
//!
//! Gated behind the `db-tests` feature (off by default) so a plain
//! `cargo test --workspace` needs no Postgres; `scripts/run-db-tests.sh`
//! runs it against a throwaway Postgres.
#![cfg(feature = "db-tests")]

use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use weft_core::activation::ActivationKey;
use weft_core::instance::{InstanceId, Owner};
use weft_core::ProjectDefinition;
use weft_dispatcher::activation_store::{
    ActivationLifecycle, ActivationStoreOps, LifecycleWrite, PostgresActivationStore, ProjectStatus, SignalsGoing,
};
use weft_dispatcher::api::project::owners_with_triggers_on;
use weft_dispatcher::api::signal::{instance_waiting_tokens, signals_visible_to, waiting_fires};
use weft_task_store::parked_fires::{WaitsFor, WaitsOn};
use weft_dispatcher::parked_drain::{due_tokens, next_due};
use weft_journal::record::{Queued, Then};
use weft_journal::RunEvent;
use weft_task_store::parked_fires::{park, ParkAppend, ParkRefusal, Waiting};
use weft_dispatcher::journal::postgres::PostgresJournal;
use weft_dispatcher::journal::{Journal, SignalRegistration};
use weft_dispatcher::versions::VersionStoreOps;

const TENANT: &str = "tenant-1";

/// The real boot-time migration path: the dispatcher's groups in
/// dependency order, in one pass. Returns the journal + the project store, the two handles the
/// SQL under test goes through.
async fn setup(pool: &PgPool) -> (PostgresJournal, weft_dispatcher::ProjectStore) {
    weft_task_store::apply_groups(pool, weft_dispatcher::app::DISPATCHER_GROUPS).await.expect("core schema");
    let journal = PostgresJournal::from_pool(pool.clone());
    let projects: weft_dispatcher::ProjectStore =
        std::sync::Arc::new(weft_dispatcher::PostgresProjectStore::new(pool.clone()));
    (journal, projects)
}

/// A minimal, empty-but-valid project definition for `id`.
fn empty_project(id: Uuid) -> ProjectDefinition {
    serde_json::from_value(json!({ "id": id, "nodes": [], "edges": [] }))
        .expect("minimal ProjectDefinition")
}

/// Register a project with `binary_hash` as its running image, the state
/// every enqueue-stamp read depends on.
async fn seed_project(
    projects: &weft_dispatcher::ProjectStore,
    id: Uuid,
    binary_hash: &str,
) {
    projects
        .register_with_hashes(
            empty_project(id),
            "db-rig",
            "",
            TENANT,
            Some(binary_hash),
            Some("def-1"),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("register project");
}

/// The birth of run `run_id` of `project_id`, in `phase`, on image
/// `binary_hash`.
fn birth(run_id: weft_core::RunId, project_id: Uuid, phase: weft_core::context::Phase, binary_hash: &str) -> RunEvent {
    RunEvent::RunStarted {
        run_id, project_id, entry_node: "entry".into(), phase,
        definition_hash: Some("def-1".into()), binary_hash: Some(binary_hash.into()),
        source_version: (phase == weft_core::context::Phase::TriggerSetup).then(|| "source".into()),
        run_kind: weft_core::run::RunKind::Project, selection: None, seed: None, instance: None, stand_in: None,
        fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 1,
        settings: Default::default(),
    }
}

/// Queue the run `events` start (`events[0]` its birth), the way the
/// dispatcher starts a run by hand or a setup run.
async fn queue(journal: &PostgresJournal, events: &[RunEvent], for_activation: bool) -> anyhow::Result<bool> {
    journal.queue_run(Queued {
        events, tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT, watch_end: false,
        stale: &[], spec: None, example: None,
    }, for_activation).await
}

/// A fire run of `project_id`, queued.
async fn queued_run(journal: &PostgresJournal, project_id: Uuid) -> weft_core::RunId {
    let run_id = weft_core::new_run_id();
    assert!(queue(journal, &[birth(run_id, project_id, weft_core::context::Phase::Fire, "bin-A")], false).await.unwrap());
    run_id
}

/// End run `run_id`, which nobody drives, completed.
async fn complete(journal: &PostgresJournal, run_id: weft_core::RunId) -> RunEvent {
    let completed = RunEvent::RunCompleted { run_id, at_unix: 2 };
    let written = journal.append(run_id, std::slice::from_ref(&completed), Then::Stays).await.unwrap();
    assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
    completed
}

/// One column of run `run_id`'s row.
async fn run_column<T>(pool: &PgPool, column: &str, run_id: weft_core::RunId) -> Option<T>
where
    T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Unpin,
{
    sqlx::query_scalar(&format!("SELECT {column} FROM run WHERE run_id = $1"))
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .unwrap()
}

/// A minimal entry-signal registration.
fn entry_signal(token: &str, project_id: Uuid) -> SignalRegistration {
    SignalRegistration {
        instance: None,
        activation_trigger: None,
        source_version: None,
        setup_run_id: None,
        program: None,
        token: token.to_string(),
        tenant_id: TENANT.to_string(),
        project_id,
        run_id: None,
        node_id: "feed".to_string(),
        is_resume: false,
        spec_json: "{}".to_string(),
        consumer_kind: None,
        tags: Vec::new(),
        consumer_payload: None,
        surface_kind: "task_callback".to_string(),
        mount_path: None,
        mount_methods: Vec::new(),
        auth_kind: "none".to_string(),
        auth_config: None,
        kind_state: json!({}),
        kind_state_seq: 0,
        access_id: None,
        port_snapshot: None,
        holds: false,
        generation: 0,
    }
}

// ----- a run keeps its image -----------------------------------------------

/// A run keeps the image it was born on across project edits: delivery
/// reads its row, and so does every claim that carries it on after an
/// answer.
#[sqlx::test]
async fn a_queued_run_keeps_the_image_it_was_born_on(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run_id = weft_core::new_run_id();
    let start = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
    seed_project(&projects, id, "bin-B").await;
    assert!(queue(&journal, &[start], false).await.unwrap());
    assert_eq!(run_column::<String>(&pool, "binary_hash", run_id).await.as_deref(), Some("bin-A"));
    assert_eq!(run_column::<String>(&pool, "state", run_id).await.as_deref(), Some("queued"));
}

// ----- atomic run birth ------------------------------------------

#[sqlx::test]
async fn registered_sources_belong_to_the_exact_program(pool: PgPool) {
    let (_, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    let implementations = std::collections::BTreeMap::new();
    let source = std::collections::BTreeMap::from([("main.weft".into(), "original-file".into())]);
    projects.register_with_hashes(empty_project(id), "sources", "", TENANT,
        Some("binary"), Some("graph"), None, None, Some(&implementations), Some(&source), None).await.unwrap();
    let program = projects.running_program_identity(id).await.unwrap().unwrap();
    assert_eq!(projects.program_source(id, &program).await.unwrap(), source);
    // A later build of other code moves the running program, and the
    // old identity no longer answers for it.
    projects.register_with_hashes(empty_project(id), "sources", "", TENANT,
        Some("different-binary"), Some("graph"), None, None, Some(&implementations), None, None).await.unwrap();
    assert!(projects.program_source(id, &program).await.is_err());
    let changed = weft_core::project::hash::ProgramIdentity { binary_hash: "different-binary".into(), ..program };
    assert!(projects.program_source(id, &changed).await.is_err(), "changing code cannot relabel old sources");
}

/// The program identity every setup birth here names (`trigger_setup_birth`).
fn setup_program() -> weft_core::project::hash::ProgramIdentity {
    weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-1".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    }
}

/// The birth of trigger setup `run_id` of project `id`, from
/// version `source`.
fn trigger_setup_birth(id: Uuid, run_id: Uuid) -> RunEvent {
    birth(run_id, id, weft_core::context::Phase::TriggerSetup, "bin-A")
}

#[sqlx::test]
async fn pruning_a_source_waits_for_setup_and_removes_its_unused_bake(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let versions = weft_dispatcher::versions::PostgresVersionStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    sqlx::query("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)")
        .bind(id).execute(&pool).await.unwrap();
    let run_id = Uuid::new_v4();
    let birth = trigger_setup_birth(id, run_id);
    queue(&journal, std::slice::from_ref(&birth), false).await.unwrap();
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err());
    let complete = complete(&journal, run_id).await;
    assert!(versions.delete_versions(id, &["source".into()]).await.is_err(), "publication still owns this source");
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
    journal.finish_trigger_setup(run_id, Some(&bake)).await.unwrap();
    versions.delete_versions(id, &["source".into()]).await.unwrap();
    assert!(journal.trigger_bakes(id, None).await.unwrap().is_empty());
    assert!(versions.version(id, "source").await.unwrap().is_none());
}

/// The feed trigger's shared activation, and an entry signal it governs.
fn feed_key() -> ActivationKey {
    ActivationKey::new("feed", Owner::Shared)
}

fn governed_entry(token: &str, project_id: Uuid, setup: Uuid) -> SignalRegistration {
    let mut entry = entry_signal(token, project_id);
    entry.activation_trigger = Some("feed".into());
    entry.setup_run_id = Some(setup);
    entry
}

#[sqlx::test]
async fn a_rearm_claim_refuses_a_trigger_that_left_active(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active), None)
        .await.unwrap(), Err(weft_dispatcher::activation_store::ClaimRefused::NotExpected)), "a re-arm never creates a trigger that was never activated");
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run_id, None, None).await.unwrap().is_ok());
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::parked(), false, None).await.unwrap().expect("owned");
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active), None)
        .await.unwrap(), Err(weft_dispatcher::activation_store::ClaimRefused::NotExpected)), "a deactivate between the look and the claim wins: the re-arm refuses");
    let again = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], again, None, None).await.unwrap().is_ok());
    activations.end_activating(id, again, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    assert!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), Some(ProjectStatus::Active), None)
        .await.unwrap().is_ok(), "still Active: the re-arm claims");
}

#[sqlx::test]
async fn an_instances_wipe_takes_its_row_and_its_signals_in_one_write(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), setup_run_id, None, None).await.unwrap().is_ok());
    let mut entry = governed_entry("ada-wiped-entry", id, setup_run_id);
    entry.instance = Some(ada.clone());
    journal.signal_insert(&entry).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");

    let written = activations
        .set_lifecycle_guarded(id, std::slice::from_ref(&ada_key), &ActivationLifecycle::wiped(), SignalsGoing::Activations, None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { unlisten } = written else { panic!("applied: {written:?}") };
    assert_eq!(unlisten.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["ada-wiped-entry"],
        "the signals are handed back for the listener cleanup");
    assert!(activations.list(id).await.unwrap().is_empty(), "the instance's wiped row is gone");
    assert!(journal.signal_get("ada-wiped-entry").await.unwrap().is_none(), "and its signal went with it");
}

/// A park keeps the signals and leaves them listening: what they hear
/// waits for the trigger to be back on, so the write hands nothing back
/// for the listener to let go of.
#[sqlx::test]
async fn a_park_keeps_its_signals_listening(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run_id, None, None).await.unwrap().is_ok());
    journal.signal_insert(&governed_entry("parked-entry", id, setup_run_id)).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");

    let written = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, None)
        .await
        .unwrap();
    let LifecycleWrite::Applied { unlisten } = written else { panic!("applied: {written:?}") };
    assert!(unlisten.is_empty(), "a parked trigger goes on listening: {unlisten:?}");
    assert!(journal.signal_get("parked-entry").await.unwrap().is_some(), "and its signal stays stored");
}

/// A hibernation takes work until its grace window ends, then stops: the
/// end flips only the windows that are over, keeps them hibernating (the
/// row stays, so `weft activate` brings it back), and says when the next
/// window still open ends.
#[sqlx::test]
async fn a_hibernation_stops_taking_work_once_its_window_ends(pool: PgPool) {
    use weft_dispatcher::activation_store::{end_grace_windows, grace_windows_over, next_grace_end};
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (over, open) = (feed_key(), ActivationKey::new("reads", Owner::Shared));
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[over.clone(), open.clone()], setup_run_id, None, None).await.unwrap().is_ok());
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    for (key, deadline) in [(&over, 100), (&open, i64::MAX / 2)] {
        activations
            .set_lifecycle_guarded(id, std::slice::from_ref(key), &ActivationLifecycle::hibernating(deadline), SignalsGoing::Kept, None)
            .await
            .unwrap();
    }
    assert_eq!(next_grace_end(&pool).await.unwrap(), Some(100));

    // The windows are over by the database's clock: one in the past, one
    // far ahead.
    assert_eq!(grace_windows_over(&pool).await.unwrap(), vec![(id, over.clone())]);
    assert_eq!(end_grace_windows(&pool, id, &[over.clone(), open.clone()]).await.unwrap(), 1, "only the window that is over ends");
    assert!(grace_windows_over(&pool).await.unwrap().is_empty(), "taking no work, it is over no longer");
    assert_eq!(end_grace_windows(&pool, id, std::slice::from_ref(&over)).await.unwrap(), 0, "a window ends once");
    let listed = activations.list(id).await.unwrap();
    let lifecycle = |key: &ActivationKey| listed.iter().find(|a| &a.key == key).expect("kept").lifecycle.clone();
    assert!(!lifecycle(&over).accepting_fires, "the window that is over takes no more work");
    assert_eq!(lifecycle(&over).mode(), weft_core::activation::ActivationMode::Down(weft_core::DeactivationMode::Hibernate), "and is still a hibernation");
    assert!(lifecycle(&open).accepting_fires, "the window still open takes work");
    assert_eq!(next_grace_end(&pool).await.unwrap(), Some(i64::MAX / 2));
}

/// A hibernation ends at its time even when the listener keeps one of its
/// signals: the window ends, nothing looks again for it, the signal is
/// marked kept and the refusal written down for `weft status`. The next
/// pass tells the listener again about the kept signal, and once it lets
/// go both are cleared.
#[sqlx::test]
async fn a_refused_let_go_still_ends_the_window_and_is_written_down(pool: PgPool) {
    use weft_dispatcher::activation_store::{grace_windows_over, next_grace_end};
    use weft_dispatcher::reaper::end_grace_windows_over;
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (key, later) = (feed_key(), ActivationKey::new("reads", Owner::Shared));
    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[key.clone(), later.clone()], setup_run_id, None, None).await.unwrap().is_ok());
    journal.signal_insert(&governed_entry("tok-feed", id, setup_run_id)).await.unwrap();
    activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    let window_over = ActivationLifecycle::hibernating(100);
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&key), &window_over, SignalsGoing::Kept, None).await.unwrap();

    let kept = |signals: Vec<SignalRegistration>| async move {
        anyhow::Ok(signals.into_iter().map(|s| (s.token, "the listener is down".to_string())).collect::<Vec<_>>())
    };
    assert_eq!(end_grace_windows_over(&pool, kept).await.unwrap(), vec![id]);
    assert!(grace_windows_over(&pool).await.unwrap().is_empty(), "the window ended whatever the listener answered");
    assert_eq!(next_grace_end(&pool).await.unwrap(), None, "nothing to look at again");
    let marked = || {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, Option<String>>("SELECT let_go_failed FROM signal WHERE token = 'tok-feed'").fetch_one(&pool).await.unwrap()
        }
    };
    assert_eq!(marked().await.as_deref(), Some("the listener is down"), "the kept signal is marked with why");
    let written: Vec<(String, String)> = sqlx::query_as("SELECT callee, error FROM unanswered_call").fetch_all(&pool).await.unwrap();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].0, format!("let-go:{id}"), "written down for the project whose signal was kept, not as the loop failing");
    assert!(written[0].1.contains("the listener is down") && !written[0].1.contains("tok-feed"), "says why, never the token: {}", written[0].1);

    // The next pass tells the listener again about the kept signal; once
    // it lets go, the mark and the failure go.
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&later), &window_over, SignalsGoing::Kept, None).await.unwrap();
    let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let let_go = |signals: Vec<SignalRegistration>| {
        let asked = asked.clone();
        async move {
            asked.lock().unwrap().extend(signals.into_iter().map(|s| s.token));
            anyhow::Ok(Vec::<(String, String)>::new())
        }
    };
    assert_eq!(end_grace_windows_over(&pool, let_go).await.unwrap(), vec![id]);
    assert!(asked.lock().unwrap().contains(&"tok-feed".to_string()), "the kept signal is let go of again");
    assert_eq!(marked().await, None);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM unanswered_call").fetch_one(&pool).await.unwrap();
    assert_eq!(left, 0);
}

#[sqlx::test]
async fn activation_cleanup_cannot_finish_or_wipe_a_newer_activation(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], first, None, None).await.unwrap().is_ok());
    assert!(matches!(activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), None, None).await.unwrap(),
        Err(weft_dispatcher::activation_store::ClaimRefused::Claimed)), "one activation of a trigger at a time");
    journal.signal_insert(&governed_entry("old-entry", id, first)).await.unwrap();
    let removed = activations.end_activating(id, first, &ActivationLifecycle::wiped(), true, None).await.unwrap().unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].token, "old-entry");
    assert!(journal.signal_get("old-entry").await.unwrap().is_none());
    assert!(queue(&journal, &[trigger_setup_birth(id, first)], true).await.is_err(),
        "a cancelled activation cannot later start its setup");
    assert!(journal.events_log(first).await.unwrap().is_empty());

    let second = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], second, None, None).await.unwrap().is_ok());
    journal.signal_insert(&governed_entry("new-entry", id, second)).await.unwrap();
    let program = weft_core::project::hash::ProgramIdentity {
        definition_hash: "def-2".into(), binary_hash: "bin-A".into(), implementations: Default::default(),
    };
    assert!(!activations.record_activation_source(id, first, &program, "first-source").await.unwrap(),
        "the ended activation cannot record over the newer one");
    assert!(activations.record_activation_source(id, second, &program, "second-source").await.unwrap());
    for to in [ActivationLifecycle::wiped(), ActivationLifecycle::active()] {
        assert!(activations.end_activating(id, first, &to, true, None).await.unwrap().is_none());
        assert!(journal.signal_get("new-entry").await.unwrap().is_some());
        let row = activations.list(id).await.unwrap().remove(0);
        assert_eq!(row.lifecycle.activating_run_id, Some(second));
        assert_eq!(row.source_version.as_deref(), Some("second-source"));
    }
    assert!(activations.end_activating(id, second, &ActivationLifecycle::active(), false, None).await.unwrap().is_some());
    let row = activations.list(id).await.unwrap().remove(0);
    assert_eq!((row.lifecycle.status, row.source_version.as_deref()), (ProjectStatus::Active, Some("second-source")));
}

/// Each owner's copy of a trigger is its own activation: an instance's
/// activation neither blocks nor ends the shared one or another
/// instance's, and taking one down removes only the signals it governs.
#[sqlx::test]
async fn an_instances_activation_is_its_own_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let bob_key = ActivationKey::new("feed", Owner::Instance(bob.clone()));

    let shared_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], shared_setup, None, None).await.unwrap().is_ok());
    let ada_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), ada_setup, None, None).await.unwrap().is_ok(),
        "the shared activation in flight does not block an instance's");
    assert!(matches!(activations.try_begin_activating(id, &[bob_key.clone(), ada_key.clone()], Uuid::new_v4(), None, None).await.unwrap(),
        Err(weft_dispatcher::activation_store::ClaimRefused::Claimed)), "a claim over a busy key claims nothing");
    let bob_setup = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&bob_key), bob_setup, None, None).await.unwrap().is_ok());

    let mut ada_entry = governed_entry("ada-entry", id, ada_setup);
    ada_entry.instance = Some(ada.clone());
    let mut wrong = ada_entry.clone();
    wrong.token = "wrong".into();
    wrong.setup_run_id = Some(bob_setup);
    assert!(journal.signal_insert(&wrong).await.is_err(), "bob's setup cannot arm ada's trigger");
    journal.signal_insert(&ada_entry).await.unwrap();
    let mut bob_entry = governed_entry("bob-entry", id, bob_setup);
    bob_entry.instance = Some(bob.clone());
    journal.signal_insert(&bob_entry).await.unwrap();

    for setup in [shared_setup, ada_setup, bob_setup] {
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    assert!(matches!(
        activations.set_lifecycle_guarded(id, std::slice::from_ref(&ada_key), &ActivationLifecycle::wiped(), SignalsGoing::Kept, None).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    let statuses: Vec<(Option<String>, ProjectStatus)> = activations.list(id).await.unwrap().into_iter()
        .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect();
    assert_eq!(statuses, vec![
        (None, ProjectStatus::Active),
        (Some("bob".into()), ProjectStatus::Active),
    ], "an instance's wiped trigger leaves no row, like one never activated");

    let begun = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&bob_key), begun, None, None).await.unwrap().is_ok());
    let refused = activations.set_lifecycle_guarded(id, &[feed_key(), bob_key], &ActivationLifecycle::wiped(), SignalsGoing::Kept, None).await.unwrap();
    assert!(matches!(&refused, LifecycleWrite::Rejected { blocker } if blocker.contains("instance 'bob'")), "{refused:?}");
    let removed = activations.end_activating(id, begun, &ActivationLifecycle::wiped(), true, None).await.unwrap().unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["bob-entry"]);
    assert!(journal.signal_get("ada-entry").await.unwrap().is_some(), "ada's signal is ada's activation's to remove");
}

/// An instance's activation cancelled (or reaped) mid-way lands wiped, so
/// its row goes with the signals it registered, as if never activated;
/// the shared one cancelled the same way keeps its row, inactive.
#[sqlx::test]
async fn a_cancelled_instances_activation_leaves_no_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));

    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key(), ada_key.clone()], setup_run_id, None, None).await.unwrap().is_ok());
    let mut ada_entry = governed_entry("ada-entry", id, setup_run_id);
    ada_entry.instance = Some(ada.clone());
    journal.signal_insert(&ada_entry).await.unwrap();
    journal.signal_insert(&governed_entry("shared-entry", id, setup_run_id)).await.unwrap();

    let mut removed: Vec<String> = activations
        .end_activating(id, setup_run_id, &ActivationLifecycle::wiped(), true, None)
        .await
        .unwrap()
        .expect("owned")
        .into_iter()
        .map(|s| s.token)
        .collect();
    removed.sort();
    assert_eq!(removed, vec!["ada-entry".to_string(), "shared-entry".to_string()]);
    let rows: Vec<(Option<String>, ProjectStatus)> = activations.list(id).await.unwrap().into_iter()
        .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect();
    assert_eq!(rows, vec![(None, ProjectStatus::Inactive)], "ada's row goes; the shared one stays");
    let again = activations.try_begin_activating(id, std::slice::from_ref(&ada_key), Uuid::new_v4(), None, None).await.unwrap();
    assert!(again.expect("claimed").is_empty(), "ada activates again as never activated");
}

/// A re-arm's values are stored in the transaction that lands its
/// activation Active, and never by an activation that no longer owns its
/// triggers.
#[sqlx::test]
async fn a_rearm_stores_its_values_as_it_lands(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ada = InstanceId::new("ada").unwrap();
    let ada_key = ActivationKey::new("feed", Owner::Instance(ada.clone()));
    let writes = [weft_access_store::InstanceValueWrite {
        node: "feed".into(),
        field: "channel".into(),
        value: json!("news"),
        connection: None,
    }];
    let store = weft_dispatcher::activation_store::ValuesStore { tenant: TENANT, instance: &ada, writes: &writes, cleared: &[] };
    let stored = || async { weft_access_store::instance_values(&pool, TENANT, id, &ada).await.unwrap() };

    let setup_run_id = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, std::slice::from_ref(&ada_key), setup_run_id, None, None).await.unwrap().is_ok());
    assert!(activations.end_activating(id, Uuid::new_v4(), &ActivationLifecycle::active(), false, Some(&store))
        .await.unwrap().is_none());
    assert!(stored().await.is_empty(), "a landing that owns nothing stores nothing");
    assert!(activations.end_activating(id, setup_run_id, &ActivationLifecycle::active(), false, Some(&store))
        .await.unwrap().is_some());
    assert_eq!(stored().await.get("feed").and_then(|fields| fields.get("channel")), Some(&json!("news")));
}

/// An instance's trigger wiped leaves no row, whether the wipe lands at
/// once or after its drain; the shared trigger wiped keeps its row, and
/// an instance hibernated or parked keeps its own. The instance activates again
/// from nothing.
#[sqlx::test]
async fn a_wiped_instances_row_is_forgotten(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let instance = |name: &str| ActivationKey::new("feed", Owner::Instance(InstanceId::new(name).unwrap()));
    let (ada, bob, cyd) = (instance("ada"), instance("bob"), instance("cyd"));
    for key in [feed_key(), ada.clone(), bob.clone(), cyd.clone()] {
        let setup = Uuid::new_v4();
        assert!(activations.try_begin_activating(id, std::slice::from_ref(&key), setup, None, None).await.unwrap().is_ok());
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    let rows = || async {
        activations.list(id).await.unwrap().into_iter()
            .map(|a| (a.key.instance().map(|m| m.as_str().to_string()), a.lifecycle.status)).collect::<Vec<_>>()
    };

    assert!(matches!(
        activations.set_lifecycle_guarded(id, &[feed_key(), ada.clone(), cyd.clone()], &ActivationLifecycle::wiped(), SignalsGoing::Kept, None).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    assert_eq!(rows().await, vec![(None, ProjectStatus::Inactive), (Some("bob".into()), ProjectStatus::Active)]);

    let draining = ActivationLifecycle::deactivating_to(ActivationLifecycle::wiped(), i64::MAX);
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&bob), &draining, SignalsGoing::Kept, None).await.unwrap();
    assert_eq!(rows().await[1], (Some("bob".into()), ProjectStatus::Deactivating), "the row stays while it drains");
    assert!(activations.cas_status(id, &bob, ProjectStatus::Deactivating, ProjectStatus::Inactive).await.unwrap());
    assert_eq!(rows().await, vec![(None, ProjectStatus::Inactive)], "the drain landing forgets the instance's row");
    assert!(!activations.cas_status(id, &bob, ProjectStatus::Deactivating, ProjectStatus::Inactive).await.unwrap(),
        "a second landing finds nothing");

    let setup = Uuid::new_v4();
    let previous = activations.try_begin_activating(id, std::slice::from_ref(&ada), setup, None, None).await.unwrap().expect("claimed");
    assert!(previous.is_empty(), "ada activates again as never activated");
    activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    activations.set_lifecycle_guarded(id, std::slice::from_ref(&ada), &ActivationLifecycle::parked(), SignalsGoing::Kept, None).await.unwrap();
    assert_eq!(rows().await[1], (Some("ada".into()), ProjectStatus::Inactive), "a parked instance keeps the row");
}

/// Who a plain `weft resync` brings up to date, and who `weft deactivate
/// --all-instances` takes down: every owner with a trigger on, read off the
/// activation rows, the program first, instances in id order, and nobody
/// whose triggers are off or still coming up.
#[sqlx::test]
async fn the_owners_with_triggers_on_come_from_the_rows(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let instance = |name: &str| Owner::Instance(InstanceId::new(name).unwrap());
    let owners = || async { owners_with_triggers_on(&activations, id).await.unwrap() };
    assert!(owners().await.is_empty(), "no row, nobody's triggers are on");

    for owner in [Owner::Shared, instance("bob"), instance("ada")] {
        let setup = Uuid::new_v4();
        assert!(activations.try_begin_activating(id, &[ActivationKey::new("feed", owner)], setup, None, None).await.unwrap().is_ok());
        activations.end_activating(id, setup, &ActivationLifecycle::active(), false, None).await.unwrap().expect("owned");
    }
    assert!(activations.try_begin_activating(id, &[ActivationKey::new("feed", instance("cyd"))], Uuid::new_v4(), None, None).await.unwrap().is_ok());
    assert_eq!(owners().await, vec![Owner::Shared, instance("ada"), instance("bob")], "cyd's is still activating");

    let ada = ActivationKey::new("feed", instance("ada"));
    assert!(matches!(
        activations.set_lifecycle_guarded(id, &[ada, feed_key()], &ActivationLifecycle::wiped(), SignalsGoing::Kept, None).await.unwrap(),
        LifecycleWrite::Applied { .. }
    ));
    assert_eq!(owners().await, vec![instance("bob")], "an instance's triggers stay on when the program's go off");
}

#[sqlx::test]
async fn trigger_bake_ownership_publication_and_project_cleanup(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    sqlx::query("INSERT INTO project_version (project_id, id, manifest, created_at) VALUES ($1, 'source', '{}', 0)")
        .bind(id).execute(&pool).await.unwrap();
    let first = Uuid::new_v4();
    let birth = trigger_setup_birth(id, first);
    queue(&journal, std::slice::from_ref(&birth), false).await.unwrap();
    let complete = complete(&journal, first).await;
    let bake = weft_dispatcher::journal::TriggerBake::from_events(&[birth, complete], &setup_program()).unwrap().unwrap();
    journal.finish_trigger_setup(first, Some(&bake)).await.unwrap();

    let second = Uuid::new_v4();
    assert!(activations.try_begin_activating(id, &[feed_key()], second, None, None).await.unwrap().is_ok());
    assert!(queue(&journal, &[trigger_setup_birth(id, Uuid::new_v4())], true).await.is_err(),
        "a setup no activation is setting up is never born as one");
    queue(&journal, &[trigger_setup_birth(id, second)], true).await.unwrap();
    let mut entry = governed_entry("baked-entry", id, first);
    entry.program = Some(bake.program.clone());
    entry.source_version = Some(bake.source_version.clone());
    assert!(journal.signal_insert(&entry).await.is_err(), "a setup that owns no activation cannot arm");
    entry.setup_run_id = Some(second);
    journal.signal_insert(&entry).await.unwrap();
    let armed = journal.signal_get("baked-entry").await.unwrap().unwrap();
    assert_eq!(armed.program, entry.program);
    assert_eq!(armed.source_version, entry.source_version);
    assert_eq!(armed.setup_run_id, Some(second));
    activations.end_activating(id, second, &ActivationLifecycle::active(), false, None).await.unwrap();
    assert!(journal.signal_insert(&entry).await.is_err(), "no late registration after activation ends");
    journal.finish_trigger_setup(second, None).await.unwrap();
    assert_eq!(journal.trigger_bakes(id, None).await.unwrap()[0].run_id, first);
    journal.delete_run(first).await.unwrap();
    assert_eq!(journal.trigger_bakes(id, None).await.unwrap()[0].run_id, first);
    assert!(journal.trigger_bakes(Uuid::new_v4(), None).await.unwrap().is_empty());
    projects.remove(id).await.unwrap();
    assert!(journal.trigger_bakes(id, None).await.unwrap().is_empty());
}

/// How many rows of `table` belong to run `run_id`.
async fn rows_of(pool: &PgPool, table: &str, run_id: weft_core::RunId) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE run_id = $1"))
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Queueing a run is ONE transaction: its row and its record's first row
/// commit together or not at all. A run of a project with no row is
/// refused with nothing written, and a run queued again by its id (a
/// retried start) writes nothing twice.
#[sqlx::test]
async fn queueing_a_run_is_atomic_and_happens_once(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let missing = Uuid::new_v4();
    let lost = weft_core::new_run_id();
    let err = queue(&journal, &[birth(lost, missing, weft_core::context::Phase::Fire, "bin-A")], false).await.unwrap_err();
    assert!(format!("{err:#}").contains("has no row"), "{err:?}");
    assert_eq!((rows_of(&pool, "run", lost).await, rows_of(&pool, "run_log", lost).await), (0, 0), "a refused start leaves nothing");

    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run_id = weft_core::new_run_id();
    let start = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
    let kick = RunEvent::NodeKicked {
        run_id, node_id: "entry".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 1,
    };
    assert!(queue(&journal, &[start.clone(), kick.clone()], false).await.unwrap());
    assert_eq!((rows_of(&pool, "run", run_id).await, rows_of(&pool, "run_log", run_id).await), (1, 1),
        "the birth and its kicks are the record's first row");
    assert_eq!(journal.events_log(run_id).await.unwrap().len(), 2);
    assert!(!queue(&journal, &[start, kick], false).await.unwrap(), "a retried start finds its run");
    assert_eq!(rows_of(&pool, "run_log", run_id).await, 1, "and writes nothing twice");
}

/// Seed a live worker of `project_id` on image `binary_hash`, driving
/// `in_flight` runs per trigger token.
async fn seed_worker(pool: &PgPool, replica: &str, project_id: Uuid, binary_hash: &str, in_flight: serde_json::Value, alive: bool) {
    let until = weft_dispatcher::lease::now_unix() + if alive { 60 } else { -60 };
    sqlx::query(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix, binary_hash, in_flight) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (replica) DO UPDATE SET leased_until_unix = EXCLUDED.leased_until_unix, in_flight = EXCLUDED.in_flight",
    )
    .bind(replica)
    .bind(project_id)
    .bind(TENANT)
    .bind(until)
    .bind(binary_hash)
    .bind(in_flight)
    .execute(pool)
    .await
    .unwrap();
}

/// The install's news the writes left for `run`, by kind, oldest first.
async fn news_of(pool: &PgPool, run: weft_core::RunId) -> Vec<(Uuid, String)> {
    sqlx::query_as("SELECT project_id, kind FROM weft_news WHERE run_id = $1 ORDER BY seq").bind(run).fetch_all(pool).await.unwrap()
}

/// Claim queued run `run_id` of `project_id` for `owner`, the way
/// a worker takes it.
async fn claim(pool: &PgPool, run_id: weft_core::RunId, project_id: Uuid, owner: &str) {
    let mut conn = pool.acquire().await.unwrap();
    weft_journal::record::claim(&mut conn, run_id, project_id, owner).await.unwrap().expect("claimed");
}

/// A drain counts what the project's live workers say they drive, plus
/// the runs on record that are queued or driven by a live worker. A run
/// parked on a wait is not going, and neither is a run whose worker's
/// lease lapsed past its claim's grace (the lost-run sweep owns it).
#[sqlx::test]
async fn the_drain_counts_what_workers_drive_and_the_runs_queued_or_driven(pool: PgPool) {
    use weft_dispatcher::drain::{going, DrainScope, Reaching};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let every = weft_core::instance::Copies::Shared;
    let scope = DrainScope { project_id: id, reaching: Reaching::Copies(&every), except: None };
    let older = DrainScope { project_id: id, reaching: Reaching::OtherImages("bin-B"), except: None };
    assert_eq!(going(&pool, &scope).await.unwrap(), 0);

    let run = queued_run(&journal, id).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 1, "queued for a worker");
    claim(&pool, run, id, "worker-a").await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 1, "within its claim's grace, before its worker's lease is written");
    sqlx::query("UPDATE run SET delivered_until = 0 WHERE run_id = $1").bind(run).execute(&pool).await.unwrap();
    assert_eq!(going(&pool, &scope).await.unwrap(), 0, "past the grace, its owner has no live lease: lost, not going");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({ "tok": 2 }), true).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 3, "two its worker states, plus the run on record");
    assert_eq!(going(&pool, &older).await.unwrap(), 3, "all of it runs on an image other than bin-B");
    seed_worker(&pool, "worker-b", id, "bin-B", json!({ "tok": 4 }), true).await;
    assert_eq!(going(&pool, &older).await.unwrap(), 3, "the worker on bin-B is not older than bin-B");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({ "tok": 2 }), false).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 4, "a worker whose lease lapsed says nothing, and its run is lost");

    sqlx::query("UPDATE run SET state = 'parked', owner = NULL WHERE run_id = $1").bind(run).execute(&pool).await.unwrap();
    seed_worker(&pool, "worker-b", id, "bin-B", json!({}), true).await;
    assert_eq!(going(&pool, &scope).await.unwrap(), 0, "a parked run is not going");
}

/// A run whose owner's lease ran out is let go of: a durable one is
/// queued again for the next worker, its epoch raised so a late batch of
/// the old owner is refused; a fast one lived in its worker's memory and
/// ends, cancelled. A run whose owner is alive is left alone, and so is one
/// within the grace its claim gave it, before its worker's lease is
/// written.
#[sqlx::test]
async fn a_lost_run_is_queued_again_when_durable_and_ended_when_fast(pool: PgPool) {
    use weft_dispatcher::journal::Lost;
    use weft_core::run_settings::{Keeping, RunSettings};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let start = |keeping: Keeping| {
        let run_id = weft_core::new_run_id();
        let mut started = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
        if let RunEvent::RunStarted { settings, .. } = &mut started {
            *settings = RunSettings::new(keeping, true).unwrap();
        }
        (run_id, started)
    };
    let now = weft_dispatcher::lease::now_unix();
    let (durable, started) = start(Keeping::Durable);
    queue(&journal, &[started], false).await.unwrap();
    claim(&pool, durable, id, "worker-a").await;
    let epoch: i32 = run_column(&pool, "epoch", durable).await.unwrap();

    seed_worker(&pool, "worker-a", id, "bin-A", json!({}), true).await;
    assert_eq!(journal.let_go_of_lost(durable, now, None).await.unwrap(), Lost::NotLost, "its owner is alive");
    seed_worker(&pool, "worker-a", id, "bin-A", json!({}), false).await;
    assert_eq!(journal.let_go_of_lost(durable, now, None).await.unwrap(), Lost::NotLost, "within its claim's grace");
    let past_grace = weft_journal::record::delivered_until(now) + 5;
    assert_eq!(journal.let_go_of_lost(durable, past_grace, None).await.unwrap(), Lost::Requeued);
    assert_eq!(run_column::<String>(&pool, "state", durable).await.as_deref(), Some("queued"));
    assert_eq!(run_column::<Option<String>>(&pool, "owner", durable).await, Some(None));
    assert!(run_column::<i32>(&pool, "epoch", durable).await.unwrap() > epoch, "a late batch of the old owner is refused");

    let (fast, started) = start(Keeping::Fast);
    queue(&journal, &[started], false).await.unwrap();
    claim(&pool, fast, id, "worker-a").await;
    assert_eq!(journal.let_go_of_lost(fast, past_grace, None).await.unwrap(), Lost::Ended);
    assert_eq!(run_column::<String>(&pool, "state", fast).await.as_deref(), Some("ended"));
    assert!(matches!(journal.events_log(fast).await.unwrap().last(), Some(RunEvent::RunCancelled { .. })));
    assert_eq!(journal.let_go_of_lost(fast, past_grace, None).await.unwrap(), Lost::NotLost, "an ended run is not lost");
}

/// A worker whose runs only wait vouched for itself until their waits
/// give up: its lease holds that long through the broker's tick, held to
/// the longest a wait can hold; its runs are still worked on and not lost,
/// it still counts among its program's copies, and the `lapses` loop looks
/// once the vouch could lapse, not before: the grace its claim gave it
/// counts for nothing while its worker's lease outlives it. Once the vouch
/// passed with the worker gone, its run is lost.
#[sqlx::test]
async fn a_vouching_workers_runs_are_its_own_until_its_vouch_passes(pool: PgPool) {
    use weft_dispatcher::journal::Lost;
    use weft_task_store::runs::in_flight;
    use weft_task_store::worker_door::lease_until;
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let now = weft_dispatcher::lease::now_unix();
    let run = queued_run(&journal, id).await;
    claim(&pool, run, id, "worker-a").await;
    let tick = |replica: &'static str, vouch: Option<i64>| {
        let pool = pool.clone();
        async move {
            let answer: serde_json::Value = sqlx::query_scalar("SELECT weft_door_tick($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)")
                .bind(replica)
                .bind(id)
                .bind(TENANT)
                .bind("bin-A")
                .bind(lease_until(now, 15, vouch))
                .bind(json!({ "t1": 1 }))
                .bind(0i64)
                .bind(Vec::<String>::new())
                .bind(Vec::<i64>::new())
                .bind(false)
                .fetch_one(&pool)
                .await
                .unwrap();
            answer["copies"].as_i64().unwrap()
        }
    };
    assert_eq!(tick("worker-a", Some(now + 600)).await, 1);
    assert_eq!(tick("worker-b", None).await, 2, "the vouching copy still counts");
    assert!(in_flight(&pool, run).await.unwrap(), "vouched: still worked on");
    assert_eq!(journal.let_go_of_lost(run, now + 500, None).await.unwrap(), Lost::NotLost, "its own until its vouch passes");
    sqlx::query("DELETE FROM worker_lease WHERE replica = 'worker-b'").execute(&pool).await.unwrap();
    let margin = weft_task_store::worker_door::WORKER_LEASE_SECS;
    let earliest = |at: i64| {
        let pool = pool.clone();
        async move { weft_dispatcher::lapses::earliest_lapse(&pool, at).await.unwrap() }
    };
    let grace: i64 = run_column(&pool, "delivered_until", run).await.expect("claimed with a grace");
    assert!(grace < now + 600, "the vouch outlives the claim's grace");
    assert_eq!(earliest(now).await, Some(now + 600 + margin), "once the vouch could lapse, not at the grace's end");
    assert_eq!(journal.let_go_of_lost(run, now + 700, None).await.unwrap(), Lost::Ended, "its vouch passed and its worker gone: lost");
}

/// A cancel asked of a run a worker drives, and an answer handed to one,
/// wake this process's news teller once they commit, which tells them.
#[sqlx::test]
async fn a_cancel_and_a_handed_answer_are_told_as_news(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let bus = std::sync::Arc::new(weft_platform_traits::FakeNews::default());
    weft_task_store::announce::tell_news_through(&pool, bus.clone());
    let told = |kind: weft_platform_traits::NewsKind, run: weft_core::RunId| {
        let bus = bus.clone();
        async move {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !bus.announced.lock().iter().flatten().any(|news| news.kind == kind && news.run == run) {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{kind:?} of {run} told"));
        }
    };
    let answered = queued_run(&journal, id).await;
    claim(&pool, answered, id, "worker-a").await;
    seed_wait(&journal, "tok-told", id, answered).await;
    assert!(matches!(journal.answer("tok-told", &weft_core::primitive::WaitAnswer::Given { value: json!(1) }).await.unwrap(), weft_dispatcher::journal::Answered::Reached { .. }));
    told(weft_platform_traits::NewsKind::Answer, answered).await;
    let cancelled = queued_run(&journal, id).await;
    claim(&pool, cancelled, id, "worker-a").await;
    assert!(journal.cancel_run(cancelled, None, &weft_core::run::CancelCause::User).await.unwrap().requested);
    told(weft_platform_traits::NewsKind::Cancel, cancelled).await;
}

/// A cancel ends a run nobody drives on the spot, after its last row; a
/// run a worker drives is asked to stop, and its worker writes the ending.
#[sqlx::test]
async fn a_cancel_ends_a_waiting_run_and_asks_a_driven_one_to_stop(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let cause = weft_core::run::CancelCause::User;

    let waiting = queued_run(&journal, id).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(waiting).execute(&pool).await.unwrap();
    let written = journal.cancel_run(waiting, None, &cause).await.unwrap();
    assert!(!written.requested);
    assert_eq!(written.node_cancellations, Some(0));
    assert_eq!(run_column::<String>(&pool, "state", waiting).await.as_deref(), Some("ended"));
    assert!(matches!(journal.events_log(waiting).await.unwrap().last(), Some(RunEvent::RunCancelled { .. })));

    let driven = queued_run(&journal, id).await;
    claim(&pool, driven, id, "worker-a").await;
    let written = journal.cancel_run(driven, None, &cause).await.unwrap();
    assert!(written.requested, "its worker is asked");
    assert_eq!(news_of(&pool, driven).await, vec![(id, "cancel".to_string())], "and the install's news says so, with the write");
    assert!(news_of(&pool, waiting).await.is_empty(), "a run nobody drives makes no news");
    assert_eq!(written.node_cancellations, None);
    assert_eq!(run_column::<String>(&pool, "state", driven).await.as_deref(), Some("running"), "its worker ends it");
    let asked: Option<serde_json::Value> = run_column(&pool, "cancel_requested", driven).await;
    assert!(asked.is_some());
    assert_eq!(journal.events_log(driven).await.unwrap().len(), 1, "nothing written into a record its worker writes");
}

/// A cancel queues the last call of each wait that carries one, in its own
/// commit: one `cancel_call` task per wait, carrying the call and the
/// connection it is sent as, deduplicated on the wait, and none for a wait
/// that carries no call.
#[sqlx::test]
async fn a_cancel_queues_the_last_call_of_each_wait_that_carries_one(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&journal, id).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(run).execute(&pool).await.unwrap();
    seed_wait(&journal, "tok-plain", id, run).await;
    let mut calling = entry_signal("tok-calling", id);
    calling.node_id = "render".into();
    calling.is_resume = true;
    calling.run_id = Some(run);
    let mut spec = weft_core::primitive::SignalSpec::of_kind("poll_endpoint", json!({ "url": "https://queue.example/requests/7/status" }));
    spec.on_cancel = Some(weft_core::signal::CancelCall::put("https://queue.example/requests/7/cancel"));
    calling.spec_json = serde_json::to_string(&spec).unwrap();
    journal.signal_insert(&calling).await.expect("seed the wait with a last call");

    let written = journal.cancel_run(run, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!((written.removed.len(), written.cancel_calls), (2, 1));
    let tasks: Vec<(String, Option<String>, Option<Uuid>, serde_json::Value)> =
        sqlx::query_as("SELECT kind, dedup_key, run_id, payload FROM task WHERE kind = 'cancel_call'").fetch_all(&pool).await.unwrap();
    assert_eq!(tasks.len(), 1, "{tasks:?}");
    let (_, dedup, task_run, payload) = &tasks[0];
    assert_eq!(dedup.as_deref(), Some(weft_task_store::kinds::cancel_call_key("tok-calling").as_str()));
    assert_eq!(*task_run, Some(run));
    let call: weft_core::signal::listener_protocol::CancelCallRequest = serde_json::from_value(payload.clone()).unwrap();
    assert_eq!(call.token, "tok-calling");
    assert_eq!(call.spec.on_cancel, Some(weft_core::signal::CancelCall::put("https://queue.example/requests/7/cancel")));

    let again = journal.cancel_run(run, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!((again.removed.len(), again.cancel_calls), (0, 0), "the waits went with the first cancel");

    // A run a worker drives, asked to cancel while one of its nodes was
    // still registering its wait: the wait lands after the cancel, and
    // the removal at the run's ending sends its call all the same.
    let driven = queued_run(&journal, id).await;
    claim(&pool, driven, id, "worker-a").await;
    assert!(journal.cancel_run(driven, None, &weft_core::run::CancelCause::User).await.unwrap().requested);
    let mut late = calling.clone();
    late.token = "tok-late".into();
    late.run_id = Some(driven);
    journal.signal_insert(&late).await.expect("a wait registered after the cancel");
    // Its worker saw the cancel and ended the run so.
    sqlx::query("UPDATE run SET state = 'ended', outcome = 'cancelled', owner = NULL WHERE run_id = $1").bind(driven).execute(&pool).await.unwrap();
    // As `run_ends` calls it: holding the run's row in a transaction of its
    // own, so the removal must not wait on that row.
    let mut holding = pool.begin().await.unwrap();
    weft_journal::record::lock_in(&mut holding, driven).await.unwrap();
    let removed = tokio::time::timeout(std::time::Duration::from_secs(10), journal.signal_remove_for_run_id(driven))
        .await
        .expect("the removal never waits on the run's row")
        .unwrap();
    holding.commit().await.unwrap();
    assert_eq!(removed.len(), 1);
    let queued: Vec<Option<String>> =
        sqlx::query_scalar("SELECT dedup_key FROM task WHERE kind = 'cancel_call' AND run_id = $1").bind(driven).fetch_all(&pool).await.unwrap();
    assert_eq!(queued, vec![Some(weft_task_store::kinds::cancel_call_key("tok-late"))]);

    // A run that ended any other way takes its waits down without a call,
    // even one asked to cancel that finished before its worker saw it.
    let finished = queued_run(&journal, id).await;
    let mut kept = calling.clone();
    kept.token = "tok-kept".into();
    kept.run_id = Some(finished);
    journal.signal_insert(&kept).await.expect("a wait of a run that ends on its own");
    sqlx::query("UPDATE run SET state = 'ended', outcome = 'completed', cancel_requested = '\"user\"'::jsonb WHERE run_id = $1")
        .bind(finished)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(journal.signal_remove_for_run_id(finished).await.unwrap().len(), 1);
    let none: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task WHERE kind = 'cancel_call' AND run_id = $1").bind(finished).fetch_one(&pool).await.unwrap();
    assert_eq!(none, 0);

    // Removing the project cuts short a run whose worker never wrote the
    // cancel: its wait goes with the project, and its call is queued and
    // kept with it.
    let cut = queued_run(&journal, id).await;
    claim(&pool, cut, id, "worker-b").await;
    assert!(journal.cancel_run(cut, None, &weft_core::run::CancelCause::User).await.unwrap().requested);
    let mut stranded = calling.clone();
    stranded.token = "tok-stranded".into();
    stranded.run_id = Some(cut);
    journal.signal_insert(&stranded).await.expect("a wait registered after the cancel");
    projects.remove(id).await.unwrap().expect("the project was there");
    let kept: Vec<Option<String>> = sqlx::query_scalar("SELECT dedup_key FROM task WHERE kind = 'cancel_call' ORDER BY dedup_key")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(kept.contains(&Some(weft_task_store::kinds::cancel_call_key("tok-stranded"))), "{kept:?}");
    assert!(kept.contains(&Some(weft_task_store::kinds::cancel_call_key("tok-calling"))), "a queued call outlives the project: {kept:?}");
    assert!(!kept.contains(&Some(weft_task_store::kinds::cancel_call_key("tok-kept"))), "{kept:?}");
}

/// A charge an earlier drive opened and no response priced is booked
/// unknown when the dispatcher cancels the parked run: one `CostReported`
/// closing it, before the ending, in the same write.
#[sqlx::test]
async fn a_cancel_books_a_parked_runs_open_charge_unknown(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&journal, id).await;
    let opened = RunEvent::ChargeOpened {
        run_id: run,
        node_id: "dub".into(),
        frames: Vec::new(),
        service: "jobs".into(),
        charge: "job-1".into(),
        scratch: json!({ "model": "m" }),
        origin: weft_core::CredentialOwner::Platform,
        at_unix: 2,
    };
    let written = journal.append(run, std::slice::from_ref(&opened), Then::Stays).await.unwrap();
    assert!(matches!(written, weft_journal::record::Appended::At(_)), "{written:?}");
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(run).execute(&pool).await.unwrap();

    let written = journal.cancel_run(run, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!(written.node_cancellations, Some(0));
    let events = journal.events_log(run).await.unwrap();
    let costs: Vec<&RunEvent> = events.iter().filter(|event| matches!(event, RunEvent::CostReported { .. })).collect();
    assert_eq!(costs.len(), 1, "{events:?}");
    assert!(
        matches!(costs[0], RunEvent::CostReported { node_id, service, charge: Some(charge), amount_usd: None, .. }
            if node_id == "dub" && service == "jobs" && charge == "job-1"),
        "{:?}", costs[0]
    );
    assert!(matches!(&events[events.len() - 2..], [RunEvent::CostReported { .. }, RunEvent::RunCancelled { .. }]), "{events:?}");
    assert_eq!(weft_journal::ChargeBook::of_record(&events).into_open().count(), 0, "the record holds no open charge");
}

/// An ending somebody waits on is found off the run's row, whether or not
/// its announcement was heard: the look after a lost notification finds
/// it, two dispatchers never take it at once, and once handled it is not
/// found again.
#[sqlx::test]
async fn an_ending_with_work_left_is_found_without_its_announcement(pool: PgPool) {
    use weft_dispatcher::run_ends::{clear_flags, take_flagged};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run_id = weft_core::new_run_id();
    let started = birth(run_id, id, weft_core::context::Phase::Fire, "bin-A");
    assert!(journal.queue_run(Queued {
        events: std::slice::from_ref(&started), tenant: TENANT, keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
        watch_end: true, stale: &[], spec: None, example: None,
    }, false).await.unwrap());
    let quiet = queued_run(&journal, id).await;
    complete(&journal, run_id).await;
    complete(&journal, quiet).await;

    let mut one = pool.begin().await.unwrap();
    let found = take_flagged(&mut one, run_id).await.unwrap().expect("the watched ending is flagged");
    assert!(found.watch_end);
    let mut other = pool.begin().await.unwrap();
    assert!(take_flagged(&mut other, quiet).await.unwrap().is_none(), "nobody waits on the other ending");
    assert!(take_flagged(&mut other, run_id).await.unwrap().is_none(), "held by the first look");
    other.rollback().await.unwrap();
    clear_flags(&mut one, &found).await.unwrap();
    one.commit().await.unwrap();
    let mut again = pool.begin().await.unwrap();
    assert!(take_flagged(&mut again, run_id).await.unwrap().is_none(), "handled once");
}

/// The retention loop erases a run once it ended longer ago than it is
/// kept for, with its record, and never a run that has not ended, however
/// old.
#[sqlx::test]
async fn retention_erases_ended_runs_past_their_keep_and_never_a_waiting_one(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let ended = queued_run(&journal, id).await;
    complete(&journal, ended).await;
    let parked = queued_run(&journal, id).await;
    sqlx::query("UPDATE run SET state = 'parked', started_at = 0 WHERE run_id = $1").bind(parked).execute(&pool).await.unwrap();
    let keep_until: i64 = run_column(&pool, "keep_until", ended).await.expect("an ended run has its keep");

    assert_eq!(journal.erase_expired(keep_until, 100).await.unwrap().0, 0, "kept until its keep runs out");
    assert_eq!(journal.erase_expired(i64::MAX, 100).await.unwrap().0, 1);
    assert_eq!((rows_of(&pool, "run", ended).await, rows_of(&pool, "run_log", ended).await), (0, 0));
    assert_eq!(rows_of(&pool, "run", parked).await, 1, "a parked run is never erased");
}

/// The referenced-image set (the keep-set image pruning deletes against)
/// must cover a project's current hash, the image of every run that has
/// not ended (a run waiting on a form resumes on the image it started
/// on), every live worker's image (it may drive a run its door just bore),
/// every infra image ref in any project's tag map, AND the refs recorded
/// on live infra units (a unit left UP across a sync stays frozen at its
/// recorded image, which can be older than the project's current map):
/// `weft clean --images` deletes everything outside this set, so any of
/// them escaping it would be deleted out from under a running workload.
/// A finished run's image and a lapsed worker's drop out; a project with
/// no infra tags contributes none; a blank ref is skipped (matches no
/// image); a unit stamped before refs were recorded contributes nothing.
#[sqlx::test]
async fn referenced_images_cover_projects_runs_workers_maps_and_unit_refs(
    pool: PgPool,
) {
    let (journal, projects) = setup(&pool).await;
    let plain = Uuid::new_v4();
    seed_project(&projects, plain, "hash-current").await;
    // A second project whose committed infra sync recorded two nodes'
    // image tags; both refs are the supervisor's to apply, so both are
    // referenced no matter which node they belong to. One blank ref is
    // skipped (a buggy writer's blank matches no image and must not be
    // laundered into the set).
    let with_infra = Uuid::new_v4();
    seed_project(&projects, with_infra, "hash-infra-project").await;
    sqlx::query("UPDATE project SET infra_image_tags_json = $1 WHERE id = $2")
        .bind(serde_json::json!({
            "bridge-node": { "bridge": "weft-infra-bridge:live1", "blank": "" },
            "engine-node": { "engine": "weft-infra-engine:live2" },
        }))
        .bind(with_infra)
        .execute(&pool)
        .await
        .expect("seed infra image tags");
    // An infra_node row for that project: one unit UP and frozen at an
    // image OLDER than the map above (the case the map alone cannot
    // cover), one unit stamped before refs were recorded (contributes
    // nothing), one unit long gone terminal.
    sqlx::query(
        "INSERT INTO infra_node \
         (project_id, node_id, copy_id, status, units_json) \
         VALUES ($1, 'n1', 'inst1', 'running', $2)",
    )
    .bind(with_infra)
    .bind(serde_json::json!({
        "frozen": {
            "status": "running",
            "stop_behavior": { "kind": "keep_running" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30,
            "image_refs": ["weft-infra-bridge:0ld-frozen"]
        },
        "legacy": {
            "status": "running",
            "stop_behavior": { "kind": "keep_running" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30
        },
        "terminal": {
            "status": "stopped",
            "stop_behavior": { "kind": "stop" },
            "flaky_after_seconds": 30,
            "recovery_after_seconds": 30,
            "image_refs": ["weft-infra-bridge:terminal-gone"]
        }
    }))
    .execute(&pool)
    .await
    .expect("seed infra_node units");
    // A live worker's image stays (it may drive a run its door just bore,
    // before anything of it is on record); a worker whose lease lapsed is
    // gone.
    seed_worker(&pool, "worker-live", plain, "hash-live-worker", json!({}), true).await;
    seed_worker(&pool, "worker-gone", plain, "hash-gone-worker", json!({}), false).await;

    // A run still waiting (a form, a timer) resumes on the image it
    // started on, so its image stays; a finished run's does not.
    for (hash, finished) in [("hash-waiting-run", false), ("hash-finished-run", true)] {
        let run_id = weft_core::new_run_id();
        queue(&journal, &[birth(run_id, plain, weft_core::context::Phase::Fire, hash)], false).await.unwrap();
        if finished {
            complete(&journal, run_id).await;
        }
    }

    let referenced = weft_dispatcher::api::project::referenced_images_query(&mut pool.acquire().await.expect("a connection"), weft_dispatcher::build::prune::ImageScope::All)
        .await
        .expect("referenced set");
    let mut hashes = referenced.worker_hashes.clone();
    hashes.sort();
    assert_eq!(
        hashes,
        vec![
            "hash-current".to_string(),
            "hash-infra-project".to_string(),
            "hash-live-worker".to_string(),
            "hash-waiting-run".to_string(),
        ],
        "project hashes + live workers' images + the images of runs that have not ended; \
         a lapsed worker's image and finished runs' images out"
    );
    assert_eq!(
        referenced.infra_refs,
        vec![
            "weft-infra-bridge:0ld-frozen".to_string(),
            "weft-infra-bridge:live1".to_string(),
            "weft-infra-bridge:terminal-gone".to_string(),
            "weft-infra-engine:live2".to_string(),
        ],
        "every ref of every project's tag map AND every recorded unit \
         ref is referenced (all rows, all units: over-keeping is the \
         safe direction); the blank ref and the ref-less legacy unit \
         contribute nothing"
    );
}

// ----- parked fires: one row per event, FIFO per trigger ------------------

/// An event waiting for its trigger, its `attempts`-th failure behind it,
/// due at `not_before`.
fn parked(attempts: u32, not_before: i64) -> Waiting {
    Waiting { fire_id: Uuid::new_v4(), payload: json!({ "v": 1 }), caller: None, attempts, not_before, waits_for: None, failures: 0, skipped: false }
}

/// Seed one signal row for `token`. Entry rows are keyed by
/// `(project_id, node_id)`, so the node id is derived from the token:
/// many tokens per project, no collisions.
async fn seed_parked_signal(journal: &PostgresJournal, token: &str, project: Uuid) {
    let mut sig = entry_signal(token, project);
    sig.node_id = format!("trigger-{token}");
    sig.activation_trigger = Some(sig.node_id.clone());
    journal
        .signal_insert(&sig)
        .await
        .expect("seed signal row");
}

/// A wait `token` of run `run_id`.
async fn seed_wait(journal: &PostgresJournal, token: &str, project: Uuid, run_id: weft_core::RunId) {
    let mut wait = entry_signal(token, project);
    wait.node_id = format!("wait-{token}");
    wait.is_resume = true;
    wait.run_id = Some(run_id);
    journal.signal_insert(&wait).await.expect("seed the wait");
}

/// An answer reaches its run once: a run nobody drives gets it in its
/// record and is queued to carry on; a run a worker drives has it handed
/// to that worker (nobody else writes its record); a run that ended takes
/// nothing. The wait's signal goes in every case, so a second answer finds
/// nothing, and an entry's token is never a wait.
#[sqlx::test]
async fn an_answer_reaches_its_run_once(pool: PgPool) {
    use weft_dispatcher::journal::Answered;
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    assert!(matches!(journal.answer("tok-entry", &weft_core::primitive::WaitAnswer::Given { value: json!(1) }).await.unwrap(), Answered::Gone), "an entry is not a wait");

    let parked_run = queued_run(&journal, project).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(parked_run).execute(&pool).await.unwrap();
    seed_wait(&journal, "tok-parked", project, parked_run).await;
    let Answered::Reached { consumed } = journal.answer("tok-parked", &weft_core::primitive::WaitAnswer::Given { value: json!("yes") }).await.unwrap() else { panic!("reached") };
    assert_eq!(consumed.token, "tok-parked");
    assert!(journal.signal_get("tok-parked").await.unwrap().is_none(), "answered once");
    assert_eq!(run_column::<String>(&pool, "state", parked_run).await.as_deref(), Some("queued"), "queued to carry on");
    assert!(matches!(journal.events_log(parked_run).await.unwrap().last(),
        Some(RunEvent::SuspensionResolved { token, value, .. }) if token == "tok-parked" && value == &json!("yes")));
    assert!(matches!(journal.answer("tok-parked", &weft_core::primitive::WaitAnswer::Given { value: json!("again") }).await.unwrap(), Answered::Gone));

    let driven = queued_run(&journal, project).await;
    claim(&pool, driven, project, "worker-a").await;
    seed_wait(&journal, "tok-driven", project, driven).await;
    static CHANNELS: &[&str] = &[weft_task_store::parked_fires::PARKED_FIRE_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    assert!(matches!(journal.answer("tok-driven", &weft_core::primitive::WaitAnswer::Given { value: json!(2) }).await.unwrap(), Answered::Reached { .. }));
    assert_eq!(news_of(&pool, driven).await, vec![(project, "answer".to_string())], "the install's news says it was handed, with the write");
    assert!(news_of(&pool, parked_run).await.is_empty(), "an answer written into a record makes no news");
    let named = weft_task_store::runs::run_payload(project, driven);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    assert!(
        heard.woken_before(deadline, |c, p| c == weft_task_store::parked_fires::PARKED_FIRE_CHANNEL && p == named).await.unwrap(),
        "the database announces the answer handed, naming its run"
    );
    assert_eq!(journal.events_log(driven).await.unwrap().len(), 1, "nothing written into a record its worker writes");
    assert_eq!(
        weft_task_store::parked_fires::answers_for(&pool, driven).await.unwrap(),
        vec![("tok-driven".to_string(), weft_core::primitive::WaitAnswer::Given { value: json!(2) })]
    );

    // A skip is an answer of its own kind, never a value: written as such
    // into a parked run's record, and handed as such to a driven run's
    // worker.
    let skipped_run = queued_run(&journal, project).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(skipped_run).execute(&pool).await.unwrap();
    seed_wait(&journal, "tok-skipped", project, skipped_run).await;
    assert!(matches!(journal.answer("tok-skipped", &weft_core::primitive::WaitAnswer::Skipped).await.unwrap(), Answered::Reached { .. }));
    assert!(matches!(journal.events_log(skipped_run).await.unwrap().last(),
        Some(RunEvent::SuspensionSkipped { token, .. }) if token == "tok-skipped"));
    seed_wait(&journal, "tok-driven-skipped", project, driven).await;
    assert!(matches!(journal.answer("tok-driven-skipped", &weft_core::primitive::WaitAnswer::Skipped).await.unwrap(), Answered::Reached { .. }));
    assert_eq!(
        weft_task_store::parked_fires::answers_for(&pool, driven).await.unwrap().last(),
        Some(&("tok-driven-skipped".to_string(), weft_core::primitive::WaitAnswer::Skipped))
    );

    let ended = queued_run(&journal, project).await;
    seed_wait(&journal, "tok-ended", project, ended).await;
    complete(&journal, ended).await;
    assert!(matches!(journal.answer("tok-ended", &weft_core::primitive::WaitAnswer::Given { value: json!(3) }).await.unwrap(), Answered::RunEnded { .. }));
    assert!(journal.signal_get("tok-ended").await.unwrap().is_none());

    // An answer queued while the run's trigger was not live is the wait's
    // one answer: a later one finds the wait answered, and the queued one
    // stays for the drain to hand over.
    let queued_for = queued_run(&journal, project).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(queued_for).execute(&pool).await.unwrap();
    seed_wait(&journal, "tok-queued", project, queued_for).await;
    assert_eq!(park(&pool, "tok-queued", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);
    assert!(matches!(journal.answer("tok-queued", &weft_core::primitive::WaitAnswer::Given { value: json!("second") }).await.unwrap(), Answered::Gone));
    assert!(journal.signal_get("tok-queued").await.unwrap().is_some(), "the queued answer still has its wait");
}

/// The consumer listing decodes the same row shape as every journal
/// read, so a column added to the row reaches this query too: one
/// visible entry signal lists, with its node (a trigger with no
/// activation row yet is visible).
#[sqlx::test]
async fn the_consumer_listing_reads_the_whole_signal_row(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;

    let listed = signals_visible_to(&pool, TENANT, &[], &[], None).await.expect("listing decodes");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0].token, "tok-entry");
    assert_eq!(listed[0].node_id, "trigger-tok-entry");
    assert!(signals_visible_to(&pool, "someone-else", &[], &[], None).await.unwrap().is_empty());
}

/// The park names its refusal instead of returning "0 rows": a re-run
/// that finds its own event queued (nothing lost), a wait already
/// answered, an entry queue at its cap (a refused NEW event, a loss the
/// caller must say out loud), and a vanished signal (the project was wiped
/// under the fire) are four different facts. A signal going takes its
/// queue along.
#[sqlx::test]
async fn a_park_names_its_refusal(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-entry", project).await;
    let waiting_run = queued_run(&journal, project).await;
    seed_wait(&journal, "tok-resume", project, waiting_run).await;

    let first = parked(0, 0);
    assert_eq!(park(&pool, "tok-entry", &first, None).await.unwrap(), ParkAppend::Parked);
    assert_eq!(
        park(&pool, "tok-entry", &Waiting { attempts: 9, not_before: 99, ..first.clone() }, None).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::AlreadyQueued),
        "a re-run of a task that already parked this event finds it"
    );

    park(&pool, "tok-resume", &parked(0, 0), None).await.unwrap();
    assert_eq!(
        park(&pool, "tok-resume", &parked(0, 0), None).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::ResumeAlreadyAnswered),
        "one answer resolves one wait; a second is a duplicate"
    );

    // Fill the entry queue to its cap in one write, then a NEW event is
    // refused (a loss, named as such).
    sqlx::query(
        "INSERT INTO parked_fire (token, fire_id, payload, attempts, not_before, is_resume) \
         SELECT 'tok-entry', gen_random_uuid(), '{}', 0, 0, FALSE FROM generate_series(2, $1)",
    )
    .bind(weft_task_store::parked_fires::MAX_PARKED_ENTRY_FIRES)
    .execute(&pool)
    .await
    .expect("fill the queue to the cap");
    assert_eq!(park(&pool, "tok-entry", &parked(0, 0), None).await.unwrap(), ParkAppend::Refused(ParkRefusal::QueueFull));

    journal.signal_remove_many(&["tok-entry".to_string()]).await.unwrap();
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM parked_fire WHERE token = 'tok-entry'").fetch_one(&pool).await.unwrap();
    assert_eq!(left, 0, "a signal going takes its queue along");
    assert_eq!(
        park(&pool, "tok-entry", &parked(0, 0), None).await.unwrap(),
        ParkAppend::Refused(ParkRefusal::RowGone),
        "a vanished signal means the trigger was wiped under the fire"
    );
}

/// The sweep's selection, against the real statement: only triggers that
/// are live, only tokens whose HEAD is due. A backing-off head blocks its
/// whole token (FIFO: a later event may not overtake it).
#[sqlx::test]
async fn the_sweep_selects_only_due_heads_on_active_triggers(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let active = Uuid::new_v4();
    let inactive = Uuid::new_v4();
    seed_project(&projects, active, "bin-A").await;
    seed_project(&projects, inactive, "bin-B").await;
    // The due trigger's activation is Active, the other project's is
    // parked; the rest have no activation row, which reads as live.
    let activations = PostgresActivationStore::new(pool.clone());
    for (project, trigger, to) in [
        (active, "trigger-tok-due", ActivationLifecycle::active()),
        (inactive, "trigger-tok-inactive", ActivationLifecycle::parked()),
    ] {
        let setup_run_id = Uuid::new_v4();
        let key = ActivationKey::new(trigger, Owner::Shared);
        assert!(activations.try_begin_activating(project, &[key], setup_run_id, None, None).await.unwrap().is_ok());
        activations.end_activating(project, setup_run_id, &to, false, None).await.unwrap().expect("owned");
    }

    let now = weft_dispatcher::lease::now_unix();
    seed_parked_signal(&journal, "tok-due", active).await;
    park(&pool, "tok-due", &parked(0, now - 10), None).await.unwrap();
    seed_parked_signal(&journal, "tok-later", active).await;
    park(&pool, "tok-later", &parked(3, now + 300), None).await.unwrap();
    park(&pool, "tok-later", &parked(0, now - 10), None).await.unwrap();
    seed_parked_signal(&journal, "tok-inactive", inactive).await;
    park(&pool, "tok-inactive", &parked(0, now - 10), None).await.unwrap();

    assert_eq!(due_tokens(&pool, now).await.unwrap(), ["tok-due"],
        "due heads on live triggers only; a backing-off head holds its queue, and a parked trigger's queue is left alone");
    assert_eq!(next_due(&pool).await.unwrap(), Some(now - 10));
}

/// One trigger's events are handed over in the order they came, and two
/// drains never take one head: the second finds the head locked and takes
/// nothing (not the event behind it, which would overtake it).
#[sqlx::test]
async fn two_drains_never_take_one_head_and_events_leave_in_order(pool: PgPool) {
    use weft_task_store::parked_fires::{remove_in, restamp_in, take_head};
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok", project).await;
    let (first, second) = (parked(0, 0), parked(0, 0));
    park(&pool, "tok", &first, None).await.unwrap();
    park(&pool, "tok", &second, None).await.unwrap();
    let now = weft_dispatcher::lease::now_unix();

    let mut one = pool.begin().await.unwrap();
    let head = take_head(&mut one, "tok", now).await.unwrap().expect("the head");
    assert_eq!(head.waiting.fire_id, first.fire_id, "first in, first out");
    let mut other = pool.begin().await.unwrap();
    assert!(take_head(&mut other, "tok", now).await.unwrap().is_none(), "a held head is skipped, and nothing overtakes it");
    other.rollback().await.unwrap();

    // A head that could not be handed over stays the head, backing off.
    let turn = weft_task_store::parked_fires::Turn { attempts: 1, not_before: now + 60, waits_for: None, failures: 0 };
    restamp_in(&mut one, &head, &turn).await.unwrap();
    one.commit().await.unwrap();
    let mut again = pool.begin().await.unwrap();
    assert!(take_head(&mut again, "tok", now).await.unwrap().is_none(), "backing off, it holds its queue");
    let head = take_head(&mut again, "tok", now + 60).await.unwrap().expect("due again");
    assert_eq!((head.waiting.fire_id, head.waiting.attempts), (first.fire_id, 1));
    remove_in(&mut again, &head).await.unwrap();
    again.commit().await.unwrap();

    let mut last = pool.begin().await.unwrap();
    assert_eq!(take_head(&mut last, "tok", now).await.unwrap().expect("the next").waiting.fire_id, second.fire_id);
}

/// An event parked because its instance has not filled a value never comes
/// due on a timer: the sweep and its next-due read pass its head by,
/// whatever its stamp says. The instance's rows are what a change of their
/// values routes again, and `weft status` counts them per trigger with
/// the latest reason.
#[sqlx::test]
async fn fires_waiting_on_an_instance_value_wait_for_the_instance(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-ada", project).await;
    seed_parked_signal(&journal, "tok-timer", project).await;
    sqlx::query("UPDATE signal SET instance_id = 'ada' WHERE token = 'tok-ada'")
        .execute(&pool)
        .await
        .expect("make tok-ada ada's");
    let gap = |reason: &str| Waiting { waits_for: WaitsFor::new(WaitsOn::Instance, reason), ..parked(1, 0) };
    park(&pool, "tok-ada", &gap("instance 'ada' at 'answer': 'key' is not filled"), None).await.unwrap();
    park(&pool, "tok-ada", &gap("instance 'ada' at 'answer': 'model' is not filled"), None).await.unwrap();
    park(&pool, "tok-timer", &parked(1, 0), None).await.unwrap();

    let now = weft_dispatcher::lease::now_unix();
    assert_eq!(due_tokens(&pool, now).await.unwrap(), ["tok-timer"], "only the timer-retried head is due");
    assert_eq!(next_due(&pool).await.unwrap(), Some(0), "the next-due read sees the timer head alone");

    let ada = InstanceId::new("ada").unwrap();
    assert_eq!(instance_waiting_tokens(&pool, project, &ada).await.unwrap(), ["tok-ada"]);
    assert!(instance_waiting_tokens(&pool, project, &InstanceId::new("bob").unwrap()).await.unwrap().is_empty());

    let waits = waiting_fires(&pool, project).await.unwrap();
    let key = ActivationKey::new("trigger-tok-ada", Owner::from_instance(Some(ada)));
    let waiting = waits.get(&key).expect("ada's trigger waits");
    assert_eq!((waiting.fires, waiting.on), (2, weft_task_store::parked_fires::UnclockedWait::Instance));
    assert_eq!(waiting.reason, "instance 'ada' at 'answer': 'model' is not filled", "the event parked last");
    assert_eq!(waits.len(), 1, "an event retried on its timer is not waiting on an instance");
}

/// A queued event whose hand-over keeps failing is tried twice more
/// shortly, then stops: no clock brings it again, its trigger's queue
/// waits behind it, `weft status` shows it, and what may have fixed it
/// (an activation, a new version, the worker settings) takes it whatever
/// its turn says. One waiting on infra is what infra coming back hands
/// over, and nothing else.
#[sqlx::test]
async fn a_failing_event_stops_its_queue_until_something_may_have_fixed_it(pool: PgPool) {
    use weft_dispatcher::parked_drain::tokens_waiting_on;
    use weft_task_store::parked_fires::{restamp_in, take_head, take_head_now, waiting, Turn};
    let (journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    seed_parked_signal(&journal, "tok-fail", project).await;
    seed_parked_signal(&journal, "tok-infra", project).await;
    let failed = waiting(Uuid::new_v4(), json!({}), None, 0, WaitsOn::Failure, "the worker could not be reached");
    park(&pool, "tok-fail", &failed, None).await.unwrap();
    park(&pool, "tok-fail", &parked(0, 0), None).await.unwrap();
    park(&pool, "tok-infra", &waiting(Uuid::new_v4(), json!({}), None, 0, WaitsOn::Infra, "infra 'db' is not running"), None).await.unwrap();

    let now = weft_dispatcher::lease::now_unix();
    assert!(due_tokens(&pool, now).await.unwrap().is_empty(), "the first failure waits a few seconds; infra waits on no clock");
    assert_eq!(next_due(&pool).await.unwrap(), Some(failed.not_before));
    for tried in 2..=3 {
        let mut tx = pool.begin().await.unwrap();
        let head = take_head(&mut tx, "tok-fail", failed.not_before + 3600).await.unwrap().expect("due again");
        let turn = Turn::next(head.waiting.attempts, head.waiting.failures, WaitsOn::Failure, "the worker could not be reached", now);
        assert_eq!(turn.failures, tried);
        restamp_in(&mut tx, &head, &turn).await.unwrap();
        tx.commit().await.unwrap();
    }
    assert!(due_tokens(&pool, now + 3600).await.unwrap().is_empty(), "stopped: no clock brings it, and its queue waits behind it");
    assert_eq!(next_due(&pool).await.unwrap(), None, "nothing to book a look at");

    let waits = waiting_fires(&pool, project).await.unwrap();
    let stopped = waits.get(&ActivationKey::new("trigger-tok-fail", Owner::Shared)).expect("shown");
    use weft_task_store::parked_fires::UnclockedWait;
    assert_eq!((stopped.fires, stopped.on, stopped.reason.as_str()), (1, UnclockedWait::Failure, "the worker could not be reached"));
    assert_eq!(tokens_waiting_on(&pool, UnclockedWait::Failure, Some(project)).await.unwrap(), ["tok-fail"]);
    assert!(tokens_waiting_on(&pool, UnclockedWait::Failure, Some(Uuid::new_v4())).await.unwrap().is_empty());
    assert_eq!(tokens_waiting_on(&pool, UnclockedWait::Infra, None).await.unwrap(), ["tok-infra"]);

    let mut tx = pool.begin().await.unwrap();
    let head = take_head_now(&mut tx, "tok-fail").await.unwrap().expect("what may have fixed it takes it");
    assert_eq!(head.waiting.fire_id, failed.fire_id);
}

/// Every status reader goes through `infra_node::observe`, which reads
/// the copies with the commands the supervisor has not finished applied:
/// an instance's start queued over its stopped copy reads provisioning at
/// once (not the old `stopped`), a queued stop of a running copy reads
/// stopping, and a start of a copy with no row yet lists it as starting.
#[sqlx::test]
async fn copies_read_with_the_commands_under_way(pool: PgPool) {
    use weft_broker::lifecycle_writes::{issue_command, IssuedCommand};
    use weft_dispatcher::infra_lifecycle_command::{issue_lifecycle, InfraLifecycleVerb, TakeDown};
    use weft_dispatcher::infra_node::{observe, InfraNodeStatus};
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    for (node, instance, status) in [("bridge", Some("ada"), "stopped"), ("db", None, "running"), ("cache", None, "running")] {
        sqlx::query(
            "INSERT INTO infra_node (project_id, node_id, instance_id, copy_id, status) \
             VALUES ($1, $2, $3, 'inst', $4)",
        )
        .bind(project)
        .bind(node)
        .bind(instance)
        .bind(status)
        .execute(&pool)
        .await
        .expect("seed a copy");
    }
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    issue_lifecycle(&pool, TENANT, project, Some("db"), &weft_core::instance::Copies::Shared, TakeDown::Stop { force: false }, None, "disp-1")
        .await
        .unwrap();
    for instance in [&ada, &bob] {
        let spec = json!({});
        let apply = IssuedCommand {
            tenant_id: TENANT,
            project_id: project,
            node_id: Some("bridge"),
            copies: &weft_core::instance::Copies::Instance(instance.clone()),
            verb: InfraLifecycleVerb::Apply,
            spec_json: Some(&spec),
            issued_by_replica: "worker-1",
        };
        issue_command(&pool, &apply).await.unwrap().expect("the project row is there");
    }

    let copies = observe(&pool, project, &empty_project(project)).await.unwrap();
    assert_eq!(copies.status_of("bridge", Some(&ada)), Some(InfraNodeStatus::Provisioning));
    assert_eq!(copies.status_of("db", None), Some(InfraNodeStatus::Stopping));
    assert_eq!(copies.status_of("cache", None), Some(InfraNodeStatus::Running), "nothing under way reads the row");
    assert_eq!(copies.status_of("bridge", Some(&bob)), Some(InfraNodeStatus::Provisioning));
    assert_eq!(copies.starting, vec![("bridge".to_string(), Some(bob))]);
    assert_eq!(copies.status_of("bridge", None), None, "no shared copy, nothing starting one");
}

/// A copy being started counts from when the start was asked for, before
/// the supervisor writes any row: an instance's copy with no row yet, and
/// one restarted over its stopped row (which holds no stamp of its own),
/// both read as provisioning since their apply command was issued, on the
/// database's clock.
#[sqlx::test]
async fn a_starting_copy_counts_from_its_request(pool: PgPool) {
    use weft_broker::lifecycle_writes::{issue_command, IssuedCommand};
    use weft_dispatcher::infra_lifecycle_command::InfraLifecycleVerb;
    use weft_dispatcher::infra_node::observe;
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    sqlx::query("INSERT INTO infra_node (project_id, node_id, instance_id, copy_id, status) VALUES ($1, 'bridge', 'ada', 'inst', 'stopped')")
        .bind(project)
        .execute(&pool)
        .await
        .expect("seed ada's stopped copy");
    let ada = InstanceId::new("ada").unwrap();
    let bob = InstanceId::new("bob").unwrap();
    let mut issued = Vec::new();
    for (instance, asked_unix) in [(&ada, 1_000_i64), (&bob, 2_000)] {
        let spec = json!({});
        let apply = IssuedCommand {
            tenant_id: TENANT,
            project_id: project,
            node_id: Some("bridge"),
            copies: &weft_core::instance::Copies::Instance(instance.clone()),
            verb: InfraLifecycleVerb::Apply,
            spec_json: Some(&spec),
            issued_by_replica: "worker-1",
        };
        let id = issue_command(&pool, &apply).await.unwrap().expect("the project row is there");
        // Asked well in the past, so the count cannot pass by luck.
        sqlx::query("UPDATE infra_lifecycle_command SET issued_at_unix = $2 WHERE id = $1")
            .bind(id)
            .bind(asked_unix)
            .execute(&pool)
            .await
            .unwrap();
        issued.push(asked_unix);
    }

    let copies = observe(&pool, project, &empty_project(project)).await.unwrap();
    let (db_now,): (i64,) = sqlx::query_as("SELECT EXTRACT(EPOCH FROM NOW())::BIGINT").fetch_one(&pool).await.unwrap();
    let restarted = copies.progress_of("bridge", Some(&ada)).expect("a restart over a stopped row shows progress");
    assert_eq!(restarted.since_unix, issued[0]);
    let fresh = copies.progress_of("bridge", Some(&bob)).expect("a start with no row yet shows progress");
    assert_eq!(fresh.since_unix, issued[1]);
    assert_eq!(fresh.waiting, None, "nothing has reported what it waits on yet");
    assert!(fresh.as_of_unix <= db_now && fresh.as_of_unix > issued[1], "read on the database's clock: {}", fresh.as_of_unix);

    // Once the supervisor stamps the row later, the request still counts.
    sqlx::query("UPDATE infra_node SET status = 'provisioning', provisioning_since_unix = 5000 WHERE project_id = $1")
        .bind(project)
        .execute(&pool)
        .await
        .unwrap();
    let copies = observe(&pool, project, &empty_project(project)).await.unwrap();
    assert_eq!(copies.progress_of("bridge", Some(&ada)).map(|p| p.since_unix), Some(issued[0]), "the request came first");
}

/// What holds the program's own verbs back is infra work on the copies
/// the bar starts and stops: an instance's copy coming up holds nothing
/// back, the shared copies' work does, and so does the project going.
#[sqlx::test]
async fn only_work_on_the_shared_copies_holds_the_program_back(pool: PgPool) {
    use weft_dispatcher::infra_lifecycle_command::{any_in_flight, issue_lifecycle, TakeDown};
    use weft_core::instance::Copies;
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let stop = |copies: Copies| {
        let pool = pool.clone();
        async move {
            issue_lifecycle(&pool, TENANT, project, Some("db"), &copies, TakeDown::Stop { force: false }, None, "disp-1")
                .await
                .unwrap()
        }
    };
    let finish = |id: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = 1 WHERE id = $1").bind(id).execute(&pool).await.unwrap();
        }
    };

    let ada = stop(Copies::Instance(InstanceId::new("ada").unwrap())).await;
    assert!(!any_in_flight(&pool, project).await.unwrap(), "an instance's copy holds nothing back");
    finish(ada).await;
    let shared = stop(Copies::Shared).await;
    assert!(any_in_flight(&pool, project).await.unwrap(), "the shared copies' work does");
    finish(shared).await;
    stop(Copies::Every).await;
    assert!(any_in_flight(&pool, project).await.unwrap(), "and so does the project going");
}

/// The claim answers every named row as it was before, and a failure
/// before setup touched anything puts each back as it was: a parked
/// trigger parked, and one never activated without a row, so it reads
/// as never activated rather than as wiped.
#[sqlx::test]
async fn a_failed_activation_puts_the_triggers_back_as_the_claim_found_them(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let fresh = ActivationKey::new("fresh", Owner::Shared);
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, None).await.unwrap().expect("claim");
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    activations.set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, None).await.unwrap();

    let second = Uuid::new_v4();
    let previous = activations
        .try_begin_activating(id, &[feed_key(), fresh.clone()], second, None, None)
        .await
        .unwrap()
        .expect("claim");
    assert_eq!(previous.len(), 1, "only the trigger activated before had a row");
    assert_eq!(previous[0].key, feed_key());
    assert_eq!(previous[0].lifecycle.mode().as_str(), "park");
    let during: Vec<_> = activations.list(id).await.unwrap();
    assert!(during.iter().all(|a| a.lifecycle.status == ProjectStatus::Activating), "both read activating at once");

    assert!(activations.restore(id, second, &previous).await.unwrap());
    let after = activations.list(id).await.unwrap();
    assert_eq!(after.len(), 1, "the never-activated trigger has no row again");
    assert_eq!(after[0].key, feed_key());
    assert_eq!(after[0].lifecycle.mode().as_str(), "park");
    assert!(!activations.restore(id, second, &previous).await.unwrap(), "a second restore finds no claim");
}

/// Everywhere one run lives, so a table dropped from the erase list
/// cannot ship quietly.
async fn footprint(pool: &PgPool, run_id: weft_core::RunId) -> i64 {
    let mut rows = 0;
    for table in ["run", "run_log", "run_tag", "trigger_setup", "run_search_queue", "run_search"] {
        rows += rows_of(pool, table, run_id).await;
    }
    rows + sqlx::query_scalar::<_, i64>(
        "SELECT (SELECT COUNT(*) FROM signal WHERE run_id = $1 AND is_resume) \
              + (SELECT COUNT(*) FROM parked_fire WHERE run_id = $1)",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Removing a project frees the space its history took: every table a
/// run touches loses its rows, for every run of that
/// project and no other's, in the removal's own transaction, so a run
/// started for it afterwards finds no project and nothing is orphaned.
///
/// This is the test the erase list needs, because a table left out of
/// it fails silently. The rows are simply still there, on a path
/// nobody runs twice, reachable by nothing.
#[sqlx::test]
async fn removing_a_projects_runs_frees_every_table_they_touched(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let doomed = Uuid::new_v4();
    let neighbour = Uuid::new_v4();
    seed_project(&projects, doomed, "bin-A").await;
    seed_project(&projects, neighbour, "bin-A").await;

    let first = queued_run(&journal, doomed).await;
    let second = queued_run(&journal, doomed).await;
    let survivor = queued_run(&journal, neighbour).await;

    // Everything else a run leaves behind, on the first run.
    let mut tx = pool.begin().await.unwrap();
    weft_journal::tags::tag_run_in(&mut tx, first, &["user_7".to_string()], 10)
        .await
        .expect("tag");
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO trigger_setup (project_id, run_id) VALUES ($1, $2)")
        .bind(doomed)
        .bind(first)
        .execute(&pool)
        .await
        .expect("trigger_setup");
    for (token, run_id, is_resume) in [
        ("resume-tok", Some(first), true),
        ("entry-tok", None, false),
    ] {
        sqlx::query(
            "INSERT INTO signal \
             (token, tenant_id, project_id, run_id, node_id, is_resume, spec_json, created_at) \
             VALUES ($1, $2, $3, $4, 'wait', $5, '{}', 1)",
        )
        .bind(token)
        .bind(TENANT)
        .bind(doomed)
        .bind(run_id)
        .bind(is_resume)
        .execute(&pool)
        .await
        .expect("signal");
    }
    let mut tx = pool.begin().await.unwrap();
    weft_task_store::parked_fires::hand_answer_in(&mut tx, "answered-tok", first, &weft_core::primitive::WaitAnswer::Given { value: json!(1) }).await.expect("a handed answer");
    sqlx::query("INSERT INTO run_search_queue (run_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(first)
        .execute(&mut *tx)
        .await
        .expect("waiting for the index");
    tx.commit().await.unwrap();
    assert!(footprint(&pool, first).await > 0, "the run left something behind to erase");

    let removed = projects.remove(doomed).await.expect("remove the project").expect("it was there");
    let mut tokens: Vec<String> = removed.into_iter().map(|s| s.token).collect();
    tokens.sort();
    assert_eq!(tokens, ["entry-tok", "resume-tok"], "its run's wait and its entry, for the listener to let go of");

    assert_eq!(footprint(&pool, first).await, 0, "nothing of the first run is left");
    assert_eq!(footprint(&pool, second).await, 0, "nothing of the second run is left");
    assert!(footprint(&pool, survivor).await > 0, "the neighbour project is untouched");

    // A run started for it from now on finds no project.
    let late = weft_core::new_run_id();
    let refused = queue(&journal, &[birth(late, doomed, weft_core::context::Phase::Fire, "bin-A")], false).await;
    let why = format!("{:#}", refused.unwrap_err());
    assert!(why.contains("has no row"), "a late start is refused: {why}");
    assert_eq!(footprint(&pool, late).await, 0);
}

/// Removing a project takes, in one transaction, everything kept under
/// it that its runs needed or its triggers ran on: its signals (handed back
/// for the listener to let go of), its queued and claimed work, its
/// recorded programs, its version tree and its infra lease. Another
/// project's are never touched, and a project already gone removes
/// nothing.
#[sqlx::test]
async fn removing_a_project_takes_everything_kept_under_it(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let live = Uuid::from_u128(1);
    let removed = Uuid::from_u128(2);
    seed_project(&projects, live, "bin").await;
    seed_project(&projects, removed, "bin").await;
    let queue = |project: Uuid, key: &'static str| {
        let pool = pool.clone();
        async move {
            weft_task_store::tasks::enqueue_dedup(
                &pool,
                weft_task_store::tasks::NewTask {
                    kind: weft_task_store::TaskKind::RegisterSignal,
                    project_id: Some(project),
                    dedup_key: Some(key.into()),
                    run_id: None,
                    tenant_id: TENANT.into(),
                    payload: json!({}),
                },
            )
            .await
            .unwrap();
        }
    };
    queue(live, "a").await;
    queue(removed, "b").await;
    queue(removed, "claimed").await;
    sqlx::query("UPDATE task SET status = 'claimed' WHERE dedup_key = 'claimed'").execute(&pool).await.unwrap();
    journal.signal_insert(&entry_signal("live-entry", live)).await.unwrap();
    journal.signal_insert(&entry_signal("gone-entry", removed)).await.unwrap();
    for (project, replica) in [(live, "sup-live"), (removed, "sup-gone")] {
        sqlx::query(
            "INSERT INTO infra_owner (project_id, supervisor_replica, tenant_id, leased_until_unix) VALUES ($1, $2, $3, $4)",
        )
        .bind(project)
        .bind(replica)
        .bind(TENANT)
        .bind(weft_dispatcher::lease::now_unix() + 60)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO infra_health_protocol (project_id, protocol, consecutive_failures) VALUES ($1, 'restart', 1)")
            .bind(project)
            .execute(&pool)
            .await
            .unwrap();
    }
    let programs = |project: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM project_definition WHERE project_id = $1")
                .bind(project)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert!(programs(removed).await > 0, "the project recorded its program");

    let signals = projects.remove(removed).await.unwrap().expect("the project was there");
    assert_eq!(signals.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["gone-entry"], "handed back to let go of");
    let tasks: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT project_id, dedup_key FROM task ORDER BY dedup_key").fetch_all(&pool).await.unwrap();
    assert_eq!(tasks, vec![(live, "a".to_string())], "its work went, the claimed one too");
    assert!(journal.signal_get("live-entry").await.unwrap().is_some());
    assert_eq!(programs(removed).await, 0);
    assert!(programs(live).await > 0);
    let leases: Vec<Uuid> = sqlx::query_scalar("SELECT project_id FROM infra_owner").fetch_all(&pool).await.unwrap();
    assert_eq!(leases, vec![live], "its supervisor lease went with its row");
    let health: Vec<Uuid> = sqlx::query_scalar("SELECT project_id FROM infra_health_protocol").fetch_all(&pool).await.unwrap();
    assert_eq!(health, vec![live], "what its health protocols did went too");
    assert!(projects.remove(removed).await.unwrap().is_none(), "nothing left to remove");
}

/// Once `weft rm` marked a project, no run of it is born: neither one the
/// dispatcher queues nor one a worker bears in a batch, so the take-down
/// that follows reaches every run the project has. Another project's runs
/// are still born, and a project with no row cannot be marked.
#[sqlx::test]
async fn a_project_being_removed_has_no_run_born(pool: PgPool) {
    use weft_journal::frame::{Batch, BatchHead, RunHead};
    use weft_journal::record::{Born, Fate, Written};
    let (journal, projects) = setup(&pool).await;
    let (doomed, kept) = (Uuid::from_u128(1), Uuid::from_u128(2));
    seed_project(&projects, doomed, "bin").await;
    seed_project(&projects, kept, "bin").await;
    assert!(projects.mark_removing(doomed).await.unwrap());
    assert!(!projects.mark_removing(Uuid::from_u128(3)).await.unwrap(), "no row to mark");
    assert!(projects.get(doomed).await.unwrap().expect("still there").removing, "it reads as being removed");
    let registered = projects
        .register_with_hashes(empty_project(doomed), "doomed", "", TENANT, None, None, None, None, None, None, None)
        .await
        .expect_err("a project being removed is not registered over");
    assert!(weft_dispatcher::project_store::ProjectRemoving::is(&registered), "{registered:#}");

    let refused = queue(&journal, &[birth(weft_core::new_run_id(), doomed, weft_core::context::Phase::Fire, "bin")], false)
        .await
        .expect_err("a queued run of a project being removed is refused");
    assert!(refused.to_string().contains("is being removed"), "{refused}");
    queued_run(&journal, kept).await;

    let bear = |project: Uuid| {
        let pool = pool.clone();
        async move {
            let run_id = weft_core::new_run_id();
            let events = vec![birth(run_id, project, weft_core::context::Phase::Fire, "bin")];
            let row = weft_journal::stored::encode(&events);
            let head = BatchHead {
                durable: true,
                lane: 0,
                runs: vec![RunHead {
                    run_id,
                    epoch: 1,
                    first_seq: 0,
                    row_sizes: vec![row.len() as u32],
                    born: Born::of(&events[0], weft_core::run_settings::KeepFor::WEFT_DEFAULT),
                    written: Written::of(&events),
                    wrote_files: false,
                }],
                selections: Vec::new(),
            };
            let batch = Batch { head, rows: vec![row.as_slice()] };
            let mut conn = pool.acquire().await.unwrap();
            let answers = weft_journal::record::record_batch(&mut conn, "w1", TENANT, "w1:0", &batch).await.unwrap();
            (answers[0].fate, run_column::<String>(&pool, "state", run_id).await)
        }
    };
    assert_eq!(bear(doomed).await, (Fate::ProjectRemoving, None), "a worker's birth for a project being removed goes nowhere, and says why");
    assert_eq!(bear(kept).await, (Fate::Accepted, Some("running".to_string())));
}

/// Removing a project takes its instances' connections, values and tokens:
/// each acts only in this project, and nobody could list them to delete
/// them once it is gone. The author's own connections stay theirs.
#[sqlx::test]
async fn removing_a_project_takes_what_its_instances_had(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let grant = |instance: Option<&'static str>| {
        let pool = pool.clone();
        async move {
            let grant = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO access_grant (id, tenant_id, service, spec_json, values_sealed, project_id, instance_id) \
                 VALUES ($1, $2, 'svc', '{}', '', $3, $4)",
            )
            .bind(grant).bind(TENANT).bind(id).bind(instance)
            .execute(&pool).await.expect("grant");
            grant
        }
    };
    let adas = grant(Some("ada")).await;
    let authors = grant(None).await;
    sqlx::query(
        "INSERT INTO instance_value (tenant_id, project_id, instance_id, node, field, value, grant_id) \
         VALUES ($1, $2, 'ada', 'post', 'account', '{}', $3), ($1, $2, 'ada', 'digest', 'cron', '\"0 0 3 * * *\"', NULL)",
    )
    .bind(TENANT).bind(id).bind(adas).execute(&pool).await.expect("values");
    let token = |hash: &str, instance: Option<InstanceId>, expires_at: Option<u64>| weft_dispatcher::journal::SignalToken {
        id: Uuid::new_v4(),
        token_hash: hash.into(),
        recognizer: "wft-test-...".into(),
        tenant_id: TENANT.into(),
        name: None,
        allowed_projects: vec![id],
        allowed_tags: Vec::new(),
        allowed_displays: Vec::new(),
        all_displays: false,
        created_at: 0,
        instance,
        expires_at,
        kind: weft_core::signal_token::TokenKind::Caller,
    };
    journal.mint_signal_token(&token("ada-token", Some(InstanceId::new("ada").unwrap()), Some(10))).await.unwrap();
    journal.mint_signal_token(&token("author-token", None, None)).await.unwrap();

    projects.remove(id).await.unwrap();
    let grants: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM access_grant").fetch_all(&pool).await.unwrap();
    assert_eq!(grants, vec![authors], "the author's connection stays theirs");
    let values: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM instance_value").fetch_one(&pool).await.unwrap();
    assert_eq!(values, 0, "an instance's values go with the project, connections or not");
    assert!(journal.get_signal_token("ada-token").await.unwrap().is_none());
    assert!(journal.get_signal_token("author-token").await.unwrap().is_some(), "the author revokes their own tokens");
}

/// Taking a whole project down removes every signal it has, entry
/// signals included (their run is NULL), and keeps only the waits of
/// the run it is told to spare.
#[sqlx::test]
async fn a_whole_project_take_down_removes_its_entry_signals(pool: PgPool) {
    use weft_dispatcher::journal::postgres::remove_project_signals_except;
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let spared = queued_run(&journal, id).await;
    let mut wait = entry_signal("spared-wait", id);
    wait.is_resume = true;
    wait.run_id = Some(spared);
    wait.node_id = "ask".into();
    journal.signal_insert(&entry_signal("entry", id)).await.unwrap();
    journal.signal_insert(&wait).await.unwrap();

    let removed = remove_project_signals_except(&pool, id, Some(spared)).await.unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["entry"]);
    let removed = remove_project_signals_except(&pool, id, None).await.unwrap();
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), vec!["spared-wait"]);
}

/// A per-instance trigger has one entry per instance at the same place, and
/// each copy reads its own: an instance's display never shows another
/// instance's registration, and the shared read sees none of them.
#[sqlx::test]
async fn an_entry_is_read_for_its_own_instance(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    for (token, instance) in [("tok-a", "alice"), ("tok-b", "bob")] {
        let signal = SignalRegistration {
            instance: Some(weft_core::instance::InstanceId::new(instance).unwrap()),
            ..entry_signal(token, id)
        };
        journal.signal_insert(&signal).await.expect("insert an instance's entry");
    }
    for (token, instance) in [("tok-a", "alice"), ("tok-b", "bob")] {
        let instance = weft_core::instance::InstanceId::new(instance).unwrap();
        let entry = journal.signal_entry_at(id, "feed", Some(&instance)).await.unwrap().expect("its entry");
        assert_eq!(entry.token, token);
    }
    assert!(journal.signal_entry_at(id, "feed", None).await.unwrap().is_none(), "no shared copy exists");
}

async fn seed_dispatcher_command(pool: &PgPool, project_id: Uuid, verb: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO infra_lifecycle_command (tenant_id, project_id, verb, issued_by_replica, issued_at_unix) \
         VALUES ($1, $2, $3, 'test-instance', $4) RETURNING id",
    )
    .bind(TENANT)
    .bind(project_id)
    .bind(verb)
    .bind(weft_dispatcher::lease::now_unix())
    .fetch_one(pool)
    .await
    .expect("seed a dispatcher command")
}

async fn complete_command(pool: &PgPool, id: i64) {
    sqlx::query("UPDATE infra_lifecycle_command SET completed_at_unix = $1 WHERE id = $2")
        .bind(weft_dispatcher::lease::now_unix())
        .bind(id)
        .execute(pool)
        .await
        .expect("complete command");
}

/// A drain under a command's claim keeps it until its next look however
/// far that is, so nothing renews it on a clock, and nobody else claims it
/// meanwhile; a cancel of the command is announced once, as it is asked,
/// so the drain stops on it rather than reading the flag on a clock.
#[sqlx::test]
async fn a_drains_claim_lasts_to_its_next_look_and_a_cancel_is_announced(pool: PgPool) {
    use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};
    use weft_dispatcher::lifecycle_claimer::{claim_one, CommandClaim};
    let (_journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let command = seed_dispatcher_command(&pool, id, "deactivate").await;
    assert_eq!(claim_one(&pool, "r1").await.unwrap().expect("claimed").id, command);

    let next_look = weft_dispatcher::lease::now_unix() + 3600;
    assert!(CommandClaim { id: command, replica: "r1" }.keep_until(&pool, next_look).await.unwrap());
    let lapse: i64 = sqlx::query_scalar("SELECT claimed_at_unix FROM infra_lifecycle_command WHERE id = $1").bind(command).fetch_one(&pool).await.unwrap();
    let ttl = weft_broker_client::lifecycle_command::CLAIM_LEASE_TTL.as_secs() as i64;
    assert_eq!(lapse + ttl, next_look, "the claim lapses at the drain's next look");
    assert!(claim_one(&pool, "r2").await.unwrap().is_none(), "nobody else takes it meanwhile");
    assert!(!CommandClaim { id: command, replica: "r2" }.keep_until(&pool, next_look).await.unwrap(), "only its holder keeps it");

    static CHANNELS: &[&str] = &[INFRA_COMMAND_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    let cancel = || sqlx::query("UPDATE infra_lifecycle_command SET cancel_requested = TRUE WHERE id = $1").bind(command).execute(&pool);
    let cancelled = |c: &str, p: &str| c == INFRA_COMMAND_CHANNEL && InfraCommandSignal::parse(p) == Some(InfraCommandSignal::Cancel { id: command });
    cancel().await.unwrap();
    let deadline = || tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    assert!(heard.woken_before(deadline(), cancelled).await.unwrap(), "a cancel names its command");
    cancel().await.unwrap();
    assert!(!heard.woken_before(deadline(), cancelled).await.unwrap(), "asked once, announced once");
}

/// A cancel completes a supervisor command nobody claimed, and only flags
/// one a supervisor claimed: it stays pending until the supervisor, which
/// hears the flag, completes it with what it really did.
#[sqlx::test]
async fn cancelling_a_claimed_supervisor_command_leaves_it_pending_for_the_supervisor(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let running = seed_dispatcher_command(&pool, id, "stop").await;
    let waiting = seed_dispatcher_command(&pool, id, "apply").await;
    sqlx::query("UPDATE infra_lifecycle_command SET claimed_by_replica = 'sup-1', claimed_at_unix = 0 WHERE id = $1")
        .bind(running)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(weft_dispatcher::infra_lifecycle_command::request_cancel_owner(&pool, id, None).await.unwrap(), 2);
    let row = |command: i64| {
        sqlx::query_as::<_, (bool, Option<i64>, Option<String>)>(
            "SELECT cancel_requested, completed_at_unix, outcome FROM infra_lifecycle_command WHERE id = $1",
        )
        .bind(command)
        .fetch_one(&pool)
    };
    let (flagged, completed, _) = row(running).await.unwrap();
    assert!(flagged && completed.is_none(), "the claimed one is flagged and left to its supervisor");
    let (_, completed, outcome) = row(waiting).await.unwrap();
    assert!(completed.is_some(), "the unclaimed one is done at once");
    assert_eq!(outcome.as_deref(), Some("cancelled"));
}

/// A connection made again, or gone, marks the signals acting as it that
/// no holder holds, and only those, and tells the holders; made again, it
/// also brings back a held one stopped over it; a rename or a
/// token refresh marks and tells nothing. A registration again keeps the mark
/// until the listener brought it up, and one by anyone else since makes the
/// row no longer the caller's.
#[sqlx::test]
async fn a_changed_connection_marks_its_signals_for_registering_again(pool: PgPool) {
    use weft_dispatcher::journal::postgres::{access_taken_afresh, reregister_for_access, signals_with_stale_access};
    setup(&pool).await;
    let (project, grant) = (Uuid::new_v4(), Uuid::new_v4());
    sqlx::query("INSERT INTO access_grant (id, tenant_id, service, spec_json, values_sealed) VALUES ($1, $2, 'svc', '{}', 'sealed')")
        .bind(grant)
        .bind(TENANT)
        .execute(&pool)
        .await
        .expect("a connection");
    for (token, access, holds) in [("poll", Some(grant), false), ("held", Some(grant), true), ("plain", None, false)] {
        sqlx::query(
            "INSERT INTO signal (token, tenant_id, project_id, node_id, is_resume, spec_json, created_at, access_id, holds) \
             VALUES ($1, $2, $3, $1, FALSE, '{}', 1, $4, $5)",
        )
        .bind(token)
        .bind(TENANT)
        .bind(project)
        .bind(access.map(|a| a.to_string()))
        .bind(holds)
        .execute(&pool)
        .await
        .expect("signal");
    }
    let stale_tokens = || async { signals_with_stale_access(&pool).await.unwrap().into_iter().map(|s| s.token).collect::<Vec<_>>() };
    assert!(stale_tokens().await.is_empty(), "nothing changed yet");
    let mut told = sqlx::postgres::PgListener::connect_with(&pool).await.unwrap();
    told.listen(weft_dispatcher::holders::ACCESS_REMADE_CHANNEL).await.unwrap();
    let mut next_told = async || {
        tokio::time::timeout(std::time::Duration::from_secs(10), told.recv()).await.expect("the holders are told").unwrap().payload().to_string()
    };

    sqlx::query("UPDATE access_grant SET label = 'renamed', values_sealed = 'refreshed', expires_at = now() WHERE id = $1")
        .bind(grant)
        .execute(&pool)
        .await
        .unwrap();
    assert!(stale_tokens().await.is_empty(), "a rename or a token refresh changes nothing a registration read");
    // Notices arrive in commit order: the mark comes first only if the
    // refresh told the holders nothing.
    sqlx::query("SELECT pg_notify($1, 'mark')").bind(weft_dispatcher::holders::ACCESS_REMADE_CHANNEL).execute(&pool).await.unwrap();
    assert_eq!(next_told().await, "mark", "a token refresh does not tell the holders");

    // A reconnect or a rotate (`weft_access_store` flows), over a signal
    // given up for its connection.
    sqlx::query("UPDATE signal SET held_failed = 'refused' WHERE token IN ('poll', 'held')").execute(&pool).await.unwrap();
    sqlx::query("UPDATE access_grant SET reconnects = reconnects + 1 WHERE id = $1").bind(grant).execute(&pool).await.unwrap();
    assert_eq!(next_told().await, grant.to_string(), "a reconnect tells the holders");
    let held_given_up: Option<String> = sqlx::query_scalar("SELECT held_failed FROM signal WHERE token = 'held'").fetch_one(&pool).await.unwrap();
    assert_eq!(held_given_up, None, "a held signal stopped over its connection is held again");
    let stale = signals_with_stale_access(&pool).await.unwrap();
    assert_eq!(stale.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), ["poll"], "a holder hears it itself");
    let fresh = reregister_for_access(&pool, &stale[0]).await.unwrap().expect("still the read row");
    assert_ne!(fresh.generation, stale[0].generation, "a new registration");
    let (seq, given_up): (i64, Option<String>) = sqlx::query_as("SELECT kind_state_seq, held_failed FROM signal WHERE token = 'poll'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(seq, stale[0].kind_state_seq + 1, "a write from the registration it replaced is refused");
    assert_eq!(given_up, None, "its give-up is forgotten");
    assert_eq!(stale_tokens().await, ["poll"], "marked until the listener brought it up");
    assert!(reregister_for_access(&pool, &stale[0]).await.unwrap().is_none(), "an older read is not the caller's");
    access_taken_afresh(&pool, &fresh.token, fresh.generation).await.unwrap();
    assert!(stale_tokens().await.is_empty());

    sqlx::query("UPDATE signal SET held_failed = 'refused' WHERE token = 'held'").execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM access_grant WHERE id = $1").bind(grant).execute(&pool).await.unwrap();
    assert_eq!(stale_tokens().await, ["poll"], "a connection that went marks it too");
    let held_given_up: Option<String> = sqlx::query_scalar("SELECT held_failed FROM signal WHERE token = 'held'").fetch_one(&pool).await.unwrap();
    assert_eq!(held_given_up.as_deref(), Some("refused"), "a connection that went brings nothing back");
    assert_eq!(next_told().await, grant.to_string(), "and tells the holders");
}

/// A person asking for a look keeps when they asked on the project (what
/// arms its given-up health protocols again), starts a fresh round of the
/// supervisor's tries over it, and sends the notice that wakes the
/// supervisor for that project.
#[sqlx::test]
async fn asking_for_a_look_wakes_the_supervisor_for_the_project(pool: PgPool) {
    use weft_broker_client::lifecycle_command::{INFRA_COMMAND_CHANNEL, LOOK_WAKE};
    setup(&pool).await;
    let id = Uuid::new_v4();
    static CHANNELS: &[&str] = &[INFRA_COMMAND_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    weft_dispatcher::infra_lifecycle_command::person_asks_for_look(&pool, id).await.unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let look = weft_broker_client::lifecycle_command::look_payload(id);
    assert!(
        heard.woken_before(deadline, |c, p| c == LOOK_WAKE.channel && (LOOK_WAKE.concerns)(p) && p == look).await.unwrap(),
        "the look names its project"
    );
    let asked: Option<i64> = sqlx::query_scalar("SELECT asked_at_unix FROM infra_health_look WHERE project_id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(asked.is_some(), "the person's look is kept on the project");

    // Passes over the project that failed until the supervisor stopped
    // trying: a look starts a fresh round of tries.
    sqlx::query("UPDATE infra_health_look SET failures = 4, retry_at_unix = 99 WHERE project_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    weft_dispatcher::infra_lifecycle_command::person_asks_for_look(&pool, id).await.unwrap();
    let (failures, retry_at): (i32, Option<i64>) =
        sqlx::query_as("SELECT failures, retry_at_unix FROM infra_health_look WHERE project_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((failures, retry_at), (0, None), "a fresh round");
}

/// A project's health verbs are claimed one at a time, in issue order: a
/// recovery is not claimed while the park before it is still running (it
/// could land last and leave the triggers down), while an upgrade of the
/// same project is neither held behind them nor holds them.
#[sqlx::test]
async fn health_verbs_of_a_project_are_claimed_in_order(pool: PgPool) {
    use weft_dispatcher::infra_lifecycle_command::InfraLifecycleVerb;
    use weft_dispatcher::lifecycle_claimer::claim_one;
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let park = seed_dispatcher_command(&pool, project, "deactivate").await;
    let recover = seed_dispatcher_command(&pool, project, "reactivate").await;
    let upgrade = seed_dispatcher_command(&pool, project, "upgrade").await;

    let first = claim_one(&pool, "disp-1").await.unwrap().expect("the park");
    assert_eq!((first.id, first.verb), (park, InfraLifecycleVerb::Deactivate));
    let second = claim_one(&pool, "disp-2").await.unwrap().expect("the upgrade");
    assert_eq!(second.id, upgrade, "the recovery waits on the park; the upgrade does not");
    assert!(claim_one(&pool, "disp-2").await.unwrap().is_none(), "nothing else is claimable yet");

    complete_command(&pool, park).await;
    let third = claim_one(&pool, "disp-2").await.unwrap().expect("the recovery");
    assert_eq!((third.id, third.verb), (recover, InfraLifecycleVerb::Reactivate));
}

/// One upgrade of the same copies at a time: a second is refused naming the
/// one in flight, another owner's goes ahead, and once the first ends a new
/// one is issued.
#[sqlx::test]
async fn a_second_upgrade_of_the_same_copies_is_refused(pool: PgPool) {
    use weft_dispatcher::infra_lifecycle_command::{issue_upgrade, RunningPolicy, UpgradeIssued, UpgradeWork};
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    let work = UpgradeWork {
        nodes: Vec::new(),
        running_policy: RunningPolicy::Cancel,
        drain_timeout_secs: 60,
        trigger_deactivation: None,
        binary_hash: None,
        definition_hash: None,
        infra_hash: None,
        stopped: false,
    };
    let ada = InstanceId::new("ada").unwrap();
    let issue = |instance: Option<&InstanceId>| {
        let (pool, work, instance) = (pool.clone(), work.clone(), instance.cloned());
        async move { issue_upgrade(&pool, TENANT, project, instance.as_ref(), &work, "disp-1").await.unwrap() }
    };

    let UpgradeIssued::Issued(first) = issue(None).await else { panic!("the first upgrade is issued") };
    assert_eq!(issue(None).await, UpgradeIssued::AlreadyInFlight(first));
    assert!(matches!(issue(Some(&ada)).await, UpgradeIssued::Issued(_)), "an instance's copies are other copies");

    complete_command(&pool, first).await;
    assert!(matches!(issue(None).await, UpgradeIssued::Issued(id) if id != first));
}

// ----- held signals: the holders' claims and their count --------------------

/// A held entry signal written the way a registration writes it.
fn held_signal(token: &str, project_id: Uuid, seq: i64) -> SignalRegistration {
    SignalRegistration { holds: true, kind_state_seq: seq, ..entry_at(token, project_id) }
}

/// An entry signal at a place of its own, so several sit in one project.
fn entry_at(token: &str, project_id: Uuid) -> SignalRegistration {
    SignalRegistration { node_id: token.to_string(), ..entry_signal(token, project_id) }
}

/// A registration that rewrites a held row takes its holder's claim away,
/// so the holder running the old row stops it and the new one comes up as
/// the row now reads.
#[sqlx::test]
async fn a_rewritten_held_row_lets_its_holder_go(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    journal.signal_insert(&held_signal("sse", id, 0)).await.unwrap();
    let first = journal.signal_get("sse").await.unwrap().unwrap().generation;
    sqlx::query("UPDATE signal SET held_by = 'h1', serving = '{\"status\":\"up\"}', held_failed = 'gave up' WHERE token = 'sse'")
        .execute(&pool)
        .await
        .unwrap();
    journal.signal_insert(&held_signal("sse", id, 1)).await.unwrap();
    let (held_by, serving, held_failed, holds): (Option<String>, Option<serde_json::Value>, Option<String>, bool) =
        sqlx::query_as("SELECT held_by, serving, held_failed, holds FROM signal WHERE token = 'sse'").fetch_one(&pool).await.unwrap();
    assert_eq!((held_by, serving, held_failed, holds), (None, None, None, true), "a rewrite is let go of and tried again");
    let second = journal.signal_get("sse").await.unwrap().unwrap().generation;
    assert!(second > first, "a rewrite is a new registration: {first} then {second}");
}

/// The holders are counted from the held rows a listener holds: a parked
/// activation's and a signal that holds nothing do not count.
#[sqlx::test]
async fn the_holders_count_the_held_rows_of_live_activations(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 0);
    journal.signal_insert(&held_signal("one", id, 0)).await.unwrap();
    journal.signal_insert(&held_signal("two", id, 0)).await.unwrap();
    journal.signal_insert(&entry_at("form", id)).await.unwrap();
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 2);
    sqlx::query(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, 'wiped', 'inactive', FALSE, FALSE, 0)",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE signal SET activation_trigger = 'wiped' WHERE token = 'two'").execute(&pool).await.unwrap();
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 1, "a wiped activation's row holds nothing");
    sqlx::query("UPDATE trigger_activation SET accepting_fires = TRUE, fires_visible_to_consumers = TRUE WHERE trigger = 'wiped'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 2, "a parked one's goes on holding: what it hears waits");
    sqlx::query("UPDATE signal SET held_failed = 'misconfigured' WHERE token = 'one'").execute(&pool).await.unwrap();
    assert_eq!(weft_dispatcher::holders::held_count(&pool).await.unwrap(), 1, "a row its holder gave up on needs no holder");
}

/// The holders run as many copies as the held signals need, none for none,
/// and look again only when the count is announced to have changed. A
/// process that has set the pool asks it to change only when the count it
/// needs differs from the one last set, by whichever replica set it; one
/// that has not set it yet (a replica just started) sets it whatever was
/// written, since the pool may have changed behind weft's back. The
/// holders are told of every change of the held set all the same.
#[sqlx::test]
async fn the_holders_are_sized_to_the_held_signals(pool: PgPool) {
    use std::sync::atomic::AtomicBool;
    use weft_dispatcher::holders::size;
    use weft_task_store::drain::DrainStep;
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let (holders, here) = (weft_platform_traits::FakeHolderPool::default(), AtomicBool::new(false));
    let news = weft_platform_traits::FakeHolderNews::default();
    assert_eq!(size(&pool, &holders, &news, 2, &here).await.unwrap(), DrainStep::Done);
    assert_eq!(*holders.sizes.lock(), vec![0], "never set: the pool is set even to none");
    size(&pool, &holders, &news, 2, &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![0], "set to none already: not asked again");
    for t in ["a", "b", "c"] {
        journal.signal_insert(&held_signal(t, id, 0)).await.unwrap();
    }
    assert_eq!(size(&pool, &holders, &news, 2, &here).await.unwrap(), DrainStep::Done);
    journal.signal_insert(&held_signal("d", id, 0)).await.unwrap();
    let (sibling, there) = (weft_platform_traits::FakeHolderPool::default(), AtomicBool::new(false));
    size(&pool, &sibling, &news, 2, &there).await.unwrap();
    assert_eq!(*sibling.sizes.lock(), vec![2], "a replica just started sets the pool to the count written, as it may have drifted");
    size(&pool, &sibling, &news, 2, &there).await.unwrap();
    assert_eq!(*sibling.sizes.lock(), vec![2], "and then trusts it");
    size(&pool, &holders, &news, 2, &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![0, 2], "four signals still need the two holders the other replica set");
    journal.signal_remove_many(&["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]).await.unwrap();
    size(&pool, &holders, &news, 2, &here).await.unwrap();
    assert_eq!(*holders.sizes.lock(), vec![0, 2, 0]);
    assert_eq!(
        *news.told.lock(),
        vec![weft_platform_traits::HolderNote::HeldSetChanged; 7],
        "each sizing tells the holders, after the pool is sized for it"
    );
}

/// A held row coming or going announces itself at its commit, which is
/// what wakes the holder sizing, and so does one losing its holder or
/// given up on; a row that holds nothing stays silent.
#[sqlx::test]
async fn a_held_row_coming_or_going_wakes_the_holder_sizing(pool: PgPool) {
    use weft_dispatcher::holders::HELD_SIGNALS_CHANNEL;
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    static CHANNELS: &[&str] = &[HELD_SIGNALS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    async fn woken(heard: &mut weft_task_store::pg_signal::Subscription) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, _| c == HELD_SIGNALS_CHANNEL).await.unwrap()
    }
    journal.signal_insert(&entry_at("form", id)).await.unwrap();
    assert!(!woken(&mut heard).await, "a signal that holds nothing is silent");
    journal.signal_insert(&held_signal("sse", id, 0)).await.unwrap();
    assert!(woken(&mut heard).await, "a held one coming");
    sqlx::query("UPDATE signal SET held_by = 'h1' WHERE token = 'sse'").execute(&pool).await.unwrap();
    assert!(!woken(&mut heard).await, "a holder taking it changes no holder's work");
    sqlx::query("UPDATE signal SET held_by = NULL WHERE token = 'sse'").execute(&pool).await.unwrap();
    assert!(woken(&mut heard).await, "its holder letting go (or its check going off) frees it for another");
    sqlx::query("UPDATE signal SET held_failed = 'misconfigured' WHERE token = 'sse'").execute(&pool).await.unwrap();
    assert!(woken(&mut heard).await, "a give-up takes it out of the held set");
    journal.signal_remove_many(&["sse".to_string()]).await.unwrap();
    assert!(woken(&mut heard).await, "and going");
    sqlx::query(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, 'parked', 'active', TRUE, TRUE, 0)",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(woken(&mut heard).await, "an activation coming may change which rows are held");
    sqlx::query("UPDATE trigger_activation SET status = 'inactive' WHERE project_id = $1 AND trigger = 'parked'").bind(id).execute(&pool).await.unwrap();
    assert!(woken(&mut heard).await, "an activation parked changes which rows are held");
    sqlx::query("DELETE FROM trigger_activation WHERE project_id = $1 AND trigger = 'parked'").bind(id).execute(&pool).await.unwrap();
    assert!(woken(&mut heard).await, "and one forgotten too");
}

/// The rows a dispatcher holds in memory (`weft_dispatcher::held`) say
/// when they change, naming whose: a tenant's public entries and the
/// activations that arm them, a project's worker levers, a project's infra
/// copies. A signal that is no public entry is silent: a run waiting on a
/// reply writes one, and a tenant's routes are not read again for it.
#[sqlx::test]
async fn the_held_rows_announce_their_changes(pool: PgPool) {
    use weft_dispatcher::held::{INFRA_STATUS_CHANNEL, ROUTES_CHANNEL, WORKER_SETTINGS_CHANNEL};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    static CHANNELS: &[&str] = &[ROUTES_CHANNEL, WORKER_SETTINGS_CHANNEL, INFRA_STATUS_CHANNEL];
    let watch = weft_task_store::pg_signal::PgSignalWatch::start(&pool.connect_options(), CHANNELS, weft_task_store::pg_signal::Listening::Always).await.unwrap();
    let mut heard = watch.subscribe();
    async fn heard_on(heard: &mut weft_task_store::pg_signal::Subscription, channel: &'static str, key: &str) -> bool {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        heard.woken_before(deadline, |c, p| c == channel && p == key).await.unwrap()
    }
    let project = id.to_string();

    journal.signal_insert(&entry_at("callback", id)).await.unwrap();
    assert!(!heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a signal that is no public entry is silent");
    let route = SignalRegistration {
        surface_kind: "public_entry".into(),
        mount_path: Some(format!("/{TENANT}/{id}/notes")),
        ..entry_at("route", id)
    };
    journal.signal_insert(&route).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a public entry coming names its tenant");
    sqlx::query(
        "INSERT INTO trigger_activation (project_id, trigger, status, accepting_fires, fires_visible_to_consumers, updated_at) \
         VALUES ($1, 'route', 'active', TRUE, TRUE, 0)",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "an activation coming");
    sqlx::query("UPDATE trigger_activation SET status = 'inactive' WHERE project_id = $1").bind(id).execute(&pool).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "an activation taken down");
    journal.signal_remove_many(&["route".to_string()]).await.unwrap();
    assert!(heard_on(&mut heard, ROUTES_CHANNEL, TENANT).await, "a public entry going");

    sqlx::query("UPDATE project SET worker_settings_json = '{\"minInstances\": 1}'::jsonb WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(heard_on(&mut heard, WORKER_SETTINGS_CHANNEL, &project).await, "a project's worker levers changing name it");

    sqlx::query("INSERT INTO infra_node (project_id, node_id, status) VALUES ($1, 'db', 'provisioning')")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "an infra copy coming names its project");
    sqlx::query("UPDATE infra_node SET status = 'running' WHERE project_id = $1").bind(id).execute(&pool).await.unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and its status changing");
    // A worker keeps where each piece answers, so a move is news too.
    sqlx::query("UPDATE infra_node SET endpoints_json = '{\"main\": \"db:5432\"}'::jsonb WHERE project_id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and where it answers changing");
    // A program registered again may declare other infra.
    sqlx::query("UPDATE project SET project_json = project_json || ' ' WHERE id = $1").bind(id).execute(&pool).await.unwrap();
    assert!(heard_on(&mut heard, INFRA_STATUS_CHANNEL, &project).await, "and the infra it declares changing");
}

/// What an infra says changed is written where its node put it, with no
/// node running: a password into the connection the node published, a
/// baked output over the saved one. A push naming anything the node did
/// not hand weft, or a copy that is gone, is refused and writes nothing.
#[sqlx::test]
async fn what_an_infra_says_changed_is_written_where_its_node_put_it(pool: PgPool) {
    let (_journal, projects) = setup(&pool).await;
    let project = Uuid::new_v4();
    seed_project(&projects, project, "bin-A").await;
    sqlx::query(
        "INSERT INTO infra_node (project_id, node_id, copy_id, status, baked_json) VALUES ($1, 'db', 'c1', 'running', $2)",
    )
    .bind(project)
    .bind(json!({ "address": "db:5432" }))
    .execute(&pool)
    .await
    .unwrap();
    let spec: weft_core::AccessSpec = serde_json::from_value(json!({
        "service": "selfrun",
        "acquisition": { "kind": "static", "fields": [{ "name": "host", "secret": false }, { "name": "password" }] },
    }))
    .unwrap();
    weft_access_store::publish_grant(
        &pool,
        TENANT,
        weft_access_store::PublishAccess {
            spec,
            project_id: project,
            instance: None,
            node_id: "db".into(),
            values: [("host".to_string(), "db".to_string()), ("password".to_string(), "p1".to_string())].into(),
            label: None,
        },
    )
    .await
    .unwrap();
    let password = || async {
        let sealed: String = sqlx::query_scalar("SELECT values_sealed FROM access_grant WHERE project_id = $1 AND published_by_node = 'db'")
            .bind(project)
            .fetch_one(&pool)
            .await
            .unwrap();
        weft_access_store::open_json(&sealed).unwrap()["password"].as_str().unwrap().to_string()
    };
    let baked = || async {
        sqlx::query_scalar::<_, serde_json::Value>("SELECT baked_json FROM infra_node WHERE project_id = $1 AND node_id = 'db'")
            .bind(project)
            .fetch_one(&pool)
            .await
            .unwrap()
    };

    let reset = weft_core::infra::bake::PushedValues {
        connection: [("password".to_string(), "p2".to_string())].into(),
        outputs: [("address".to_string(), json!("db:6543"))].into(),
    };
    weft_access_store::write_pushed_values(&pool, project, "c1", &reset).await.unwrap();
    assert_eq!(password().await, "p2", "the connection holds the new password");
    assert_eq!(baked().await, json!({ "address": "db:6543" }));

    // Only what the node handed weft can change, and nothing is written
    // when any of it is refused.
    let refused = |e: anyhow::Error| match e.downcast::<weft_access_store::AccessError>() {
        Ok(weft_access_store::AccessError::Invalid(why)) => why,
        other => panic!("not a refusal: {other:?}"),
    };
    let stray = weft_core::infra::bake::PushedValues {
        connection: [("password".to_string(), "p3".to_string())].into(),
        outputs: [("status".to_string(), json!("up"))].into(),
    };
    let why = refused(weft_access_store::write_pushed_values(&pool, project, "c1", &stray).await.unwrap_err());
    assert!(why.contains("no baked output 'status'"), "{why}");
    assert_eq!(password().await, "p2", "a refused push changes nothing");
    let unknown = weft_core::infra::bake::PushedValues { connection: [("user".to_string(), "root".to_string())].into(), ..Default::default() };
    let why = refused(weft_access_store::write_pushed_values(&pool, project, "c1", &unknown).await.unwrap_err());
    assert!(why.contains("stores no 'user'"), "{why}");
    let why = refused(weft_access_store::write_pushed_values(&pool, project, "gone", &reset).await.unwrap_err());
    assert!(why.contains("no longer running"), "{why}");
}

/// An activation whose driver stopped beating is stuck only once no setup
/// run of its goes: while its setup run is parked on a person (or queued,
/// or driven) the driver rests on purpose, and the run is judged by its own
/// worker and its ending. A driver that died with no setup run going is.
/// The `lapses` loop looks at the stale heartbeat only then.
#[sqlx::test]
async fn an_activation_whose_setup_run_goes_is_not_stuck(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let setup_run = weft_core::new_run_id();
    assert!(activations.try_begin_activating(id, &[feed_key()], setup_run, None, None).await.unwrap().is_ok());
    assert!(queue(&journal, &[birth(setup_run, id, weft_core::context::Phase::TriggerSetup, "bin-A")], true).await.unwrap());
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(setup_run).execute(&pool).await.unwrap();
    sqlx::query("UPDATE trigger_activation SET heartbeat_unix = 0").execute(&pool).await.unwrap();
    let now = weft_dispatcher::lease::now_unix();
    assert!(activations.list_stuck(now).await.unwrap().is_empty(), "its setup run waits on a person");
    assert_eq!(weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap(), None, "and nothing is looked at for it");

    complete(&journal, setup_run).await;
    let stuck = activations.list_stuck(now).await.unwrap();
    assert_eq!(stuck.iter().map(|s| s.run_id).collect::<Vec<_>>(), vec![setup_run], "no run goes and nobody drives it");
}

/// A claimed task lapses when its claimant stops renewing it, which the
/// picker rescues; one resting on a run has no lapse while that run goes,
/// and once the run ended a claim's length ago the `lapses` loop looks.
#[sqlx::test]
async fn a_resting_task_claim_lapses_only_after_its_run_ended(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let task = weft_task_store::tasks::enqueue(
        &pool,
        weft_task_store::tasks::NewTask {
            kind: weft_task_store::TaskKind::ProgramCall,
            project_id: Some(id),
            dedup_key: None,
            run_id: None,
            tenant_id: TENANT.into(),
            payload: json!({}),
        },
    )
    .await
    .unwrap();
    let weft_task_store::tasks::Claim::Task(claimed) = weft_task_store::tasks::claim_next(&pool, "disp-1").await.unwrap() else {
        panic!("claimed");
    };
    assert_eq!(claimed.id, task);
    // What a picker run dry reads: when the earliest renewed claim lapses.
    let claim_lapse = || {
        let pool = pool.clone();
        async move {
            match weft_task_store::tasks::claim_next(&pool, "disp-2").await.unwrap() {
                weft_task_store::tasks::Claim::Dry { next_lapse } => next_lapse,
                weft_task_store::tasks::Claim::Task(task) => panic!("nothing is claimable, yet {} was claimed", task.id),
            }
        }
    };
    let lapse = claim_lapse().await.expect("a renewed claim lapses");
    assert!(lapse > weft_dispatcher::lease::now_unix());

    let run = queued_run(&journal, id).await;
    assert!(weft_task_store::tasks::rest_claim(&pool, task, "disp-1", run).await.unwrap());
    assert_eq!(claim_lapse().await, None, "a resting claim has no lapse");
    assert!(weft_task_store::tasks::heartbeat(&pool, task, "disp-1").await.unwrap(), "still ours");
    assert_eq!(claim_lapse().await, None, "and a beat leaves it resting");
    let now = weft_dispatcher::lease::now_unix();
    assert_eq!(weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap(), None, "its run goes");

    complete(&journal, run).await;
    let ended = now;
    sqlx::query("UPDATE run SET ended_at = $2 WHERE run_id = $1").bind(run).bind(ended).execute(&pool).await.unwrap();
    let at = weft_dispatcher::lapses::earliest_lapse(&pool, now).await.unwrap().expect("its run ended");
    assert_eq!(at, ended + weft_task_store::tasks::claim_duration_secs());
    assert!(weft_task_store::tasks::resume_claim(&pool, task, "disp-1").await.unwrap(), "its claimant takes it back");
    assert!(claim_lapse().await.is_some());
}

/// A run its workers refuse waits a little longer each time, and the third
/// refusal is the one that ends it, through the one cancel, with why; an
/// ended run is refused no more. A worker still starting gives the run a
/// longer wait and counts no refusal.
#[sqlx::test]
async fn a_run_refused_three_times_ends_with_why(pool: PgPool) {
    use weft_dispatcher::delivery::{refused, refused_cause, starting_wait_secs, wait_for_worker, RefusedBy, REFUSALS_TO_END, RETRY_WAITS};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&journal, id).await;
    let now = weft_dispatcher::lease::now_unix();

    wait_for_worker(&pool, run).await.unwrap();
    let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
    assert!(until >= now + starting_wait_secs(), "joins the same bring-up later");
    assert_eq!(run_column::<i16>(&pool, "refusals", run).await, Some(0));

    for (n, wait) in RETRY_WAITS.iter().enumerate() {
        assert_eq!(refused(&pool, run).await.unwrap(), Some(n as i16 + 1));
        let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
        assert!(until >= now + wait && until <= now + wait + 2, "refusal {} waits {wait}s: {until}", n + 1);
    }
    assert_eq!(refused(&pool, run).await.unwrap(), Some(REFUSALS_TO_END), "the refusal that ends it");
    // What the delivery then does, through `cancel_run_id`.
    journal.cancel_run(run, None, &refused_cause(RefusedBy::Worker, "the worker answered 500")).await.unwrap();
    assert_eq!(run_column::<String>(&pool, "state", run).await.as_deref(), Some("ended"));
    let cause: serde_json::Value = run_column(&pool, "cancel_cause", run).await.unwrap();
    assert!(cause.to_string().contains("its worker refused it 3 times: the worker answered 500"), "{cause}");
    assert_eq!(refused(&pool, run).await.unwrap(), None, "an ended run is not refused again");
}

/// Changing a project's worker levers is what a person does about workers
/// that refused its runs or could not be reached: its queued runs start
/// their tries again, a waiting one takeable at once, while one out with a
/// delivery right now stays with it; another project's are left as they
/// are.
#[sqlx::test]
async fn new_worker_levers_hand_the_refused_runs_out_again(pool: PgPool) {
    use weft_dispatcher::delivery::{refused, unreached};
    let (journal, projects) = setup(&pool).await;
    let (id, other) = (Uuid::new_v4(), Uuid::new_v4());
    seed_project(&projects, id, "bin-A").await;
    seed_project(&projects, other, "bin-A").await;
    let run = queued_run(&journal, id).await;
    let handed = queued_run(&journal, id).await;
    let elsewhere = queued_run(&journal, other).await;
    for refused_run in [run, run, elsewhere] {
        refused(&pool, refused_run).await.unwrap();
    }
    unreached(&pool, handed).await.unwrap();
    // Taken again: out with a delivery until its lease runs out.
    let lease = weft_dispatcher::lease::now_unix() + 30;
    sqlx::query("UPDATE run SET delivered_until = $2, waits = FALSE WHERE run_id = $1")
        .bind(handed)
        .bind(lease)
        .execute(&pool)
        .await
        .unwrap();
    projects.set_worker_overrides(id, &weft_platform_traits::WorkerOverrides::default()).await.unwrap();
    assert_eq!(run_column::<i16>(&pool, "refusals", run).await, Some(0));
    assert_eq!(run_column::<Option<i64>>(&pool, "delivered_until", run).await, Some(None), "takeable at once");
    assert_eq!(run_column::<i16>(&pool, "unreached", handed).await, Some(0), "its tries start again");
    assert_eq!(run_column::<Option<i64>>(&pool, "delivered_until", handed).await, Some(Some(lease)), "its hand-off is left to it");
    assert_eq!(run_column::<i16>(&pool, "refusals", elsewhere).await, Some(1), "another project's run is left alone");
}

/// A run whose workers cannot be reached waits a little longer each time,
/// and the third such hand-off stops its tries without ending it: nothing
/// books it, until a worker of its project comes alive after that (which
/// wakes the delivery, as no other project's worker does), or a build of
/// the project registers, either of which starts its tries again. A run
/// its worker claimed before the call broke counts nothing.
#[sqlx::test]
async fn a_run_whose_workers_cannot_be_reached_waits_for_what_can_fix_it(pool: PgPool) {
    use weft_dispatcher::delivery::{next_hand_out, take, unreached, RETRY_WAITS, UNREACHED_TO_STOP};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&journal, id).await;
    let now = weft_dispatcher::lease::now_unix();
    for (n, wait) in RETRY_WAITS.iter().enumerate() {
        assert_eq!(unreached(&pool, run).await.unwrap(), Some(n as i16 + 1));
        let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
        assert!(until >= now + wait && until <= now + wait + 2, "try {} waits {wait}s: {until}", n + 1);
    }
    assert_eq!(unreached(&pool, run).await.unwrap(), Some(UNREACHED_TO_STOP), "the try that stops them");
    assert_eq!(run_column::<String>(&pool, "state", run).await.as_deref(), Some("queued"), "the run goes on waiting");
    let stopped_at: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
    assert_eq!(next_hand_out(&pool).await.unwrap(), None, "nothing books it");

    // A worker's lease coming alive wakes the delivery only for a project
    // whose runs' tries stopped.
    let mut back = sqlx::postgres::PgListener::connect_with(&pool).await.unwrap();
    back.listen(weft_dispatcher::delivery::WORKERS_BACK_CHANNEL).await.unwrap();
    let mut heard = async || {
        tokio::time::timeout(std::time::Duration::from_millis(500), back.recv()).await.ok().map(|n| n.unwrap().payload().to_string())
    };
    let other = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix) VALUES ('w-other', $1, $2, 'bin-A', $3)",
    )
    .bind(other)
    .bind(TENANT)
    .bind(now + 15)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(heard().await, None, "a project with nothing stopped says nothing");

    // A worker of the project that came alive before the tries stopped
    // changes nothing; one that came alive in the second they stopped (it
    // may have come up after the stop) is what the delivery takes it again
    // for.
    sqlx::query(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, alive_since) \
         VALUES ('w-1', $1, $2, 'bin-A', $3, $4)",
    )
    .bind(id)
    .bind(TENANT)
    .bind(now + 15)
    .bind(stopped_at - 1)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(heard().await, Some(id.to_string()), "its worker coming alive wakes the delivery");
    assert!(take(&pool, now + 3600, 32).await.unwrap().is_empty(), "a worker alive since before");
    sqlx::query("UPDATE worker_lease SET alive_since = $1").bind(stopped_at).execute(&pool).await.unwrap();
    let taken: Vec<_> = take(&pool, now, 32).await.unwrap().into_iter().map(|d| d.run_id).collect();
    assert_eq!(taken, [run], "a worker alive since the second they stopped");
    assert_eq!(run_column::<i16>(&pool, "unreached", run).await, Some(0), "its tries start again");

    // Its tries stopped again, a build of the project registering starts
    // them again.
    for _ in 0..UNREACHED_TO_STOP {
        unreached(&pool, run).await.unwrap();
    }
    seed_project(&projects, id, "bin-B").await;
    assert_eq!(run_column::<i16>(&pool, "unreached", run).await, Some(0));
    assert_eq!(run_column::<Option<i64>>(&pool, "delivered_until", run).await, Some(None), "takeable at once");

    // Claimed by its worker before the call broke: nothing is counted.
    assert!(weft_journal::record::claim(&mut pool.acquire().await.unwrap(), run, id, "w-1").await.unwrap().is_some());
    assert_eq!(unreached(&pool, run).await.unwrap(), None, "a claimed run is its worker's");
}

/// A run its workers answer busy is tried again for as long as they do,
/// counting no try: after a pause as long as its busy stretch so far, at
/// least the first wait and at most the cap, so the pause grows and stays
/// booked. Its stretch is kept across answers, and a claim clears it. The
/// claim that leaves no run of the project answered busy also ends what
/// `weft status` shows of its workers answering busy, and so does a change
/// of its worker levers; a claim of one never answered busy, or while
/// another still waits, leaves that alone. A run not queued is not
/// counted. A run they refused or could not be reached with waits on them
/// too, so a busy run going leaves what is written while one of those is
/// queued. A run they failed whose tries started again (a worker of the
/// project came alive) still clears it on reaching a worker.
#[sqlx::test]
async fn a_run_its_workers_keep_answering_busy_is_tried_again_with_a_growing_pause(pool: PgPool) {
    use weft_dispatcher::delivery::{busy, busy_wait_cap_secs, busy_wait_secs, next_hand_out};
    use weft_task_store::unanswered::{failed, Callee};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let run = queued_run(&journal, id).await;
    let calm = queued_run(&journal, id).await;
    let now = weft_dispatcher::lease::now_unix();
    let paused = |pool: PgPool, by: i64| async move {
        let until: i64 = run_column(&pool, "delivered_until", run).await.unwrap();
        let at = weft_dispatcher::lease::now_unix();
        assert!(until >= at - 2 + by && until <= at + by, "paused until {until}, expected {by}s from {at}");
    };

    let lasted = busy(&pool, run).await.unwrap().expect("queued");
    assert!((0..=2).contains(&lasted), "its stretch starts: {lasted}");
    paused(pool.clone(), busy_wait_secs()).await;
    let since: i64 = run_column(&pool, "busy_since", run).await.unwrap();
    assert!(since >= now && since <= now + 2, "{since}");

    // A stretch going is kept, and the pause grows with it.
    let grown = busy_wait_secs() * 4;
    sqlx::query("UPDATE run SET busy_since = busy_since - $2 WHERE run_id = $1").bind(run).bind(grown).execute(&pool).await.unwrap();
    let lasted = busy(&pool, run).await.unwrap().expect("queued");
    assert!(lasted >= grown && lasted <= grown + 2, "{lasted}");
    assert_eq!(run_column::<i64>(&pool, "busy_since", run).await, Some(since - grown), "a stretch going is kept");
    paused(pool.clone(), lasted).await;

    // However long it lasts, the pause stops at the cap, the run stays
    // queued counting no try, and its next try is booked.
    sqlx::query("UPDATE run SET busy_since = busy_since - 3600 WHERE run_id = $1").bind(run).execute(&pool).await.unwrap();
    assert!(busy(&pool, run).await.unwrap().expect("queued") >= 3600);
    paused(pool.clone(), busy_wait_cap_secs()).await;
    assert_eq!(run_column::<i16>(&pool, "unreached", run).await, Some(0), "counting no try");
    assert_eq!(run_column::<String>(&pool, "state", run).await.as_deref(), Some("queued"));
    assert!(next_hand_out(&pool).await.unwrap().is_some(), "its next try is booked");

    // Written down for `weft status`: a claim of a run never answered
    // busy leaves it, so does the claim of a busy run while another still
    // waits on them answering busy, and the claim of the last clears it
    // and its stretch.
    let also = queued_run(&journal, id).await;
    busy(&pool, also).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    let written = || {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM unanswered_call WHERE callee = $1")
                .bind(format!("workers:{id}"))
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    claim(&pool, calm, id, "worker-a").await;
    assert_eq!(written().await, 1, "a run never answered busy says nothing about it");
    claim(&pool, run, id, "worker-a").await;
    assert_eq!(written().await, 1, "another run still waits on them answering busy");
    assert_eq!(run_column::<Option<i64>>(&pool, "busy_since", run).await, Some(None), "a claim clears the stretch");
    assert_eq!(busy(&pool, run).await.unwrap(), None, "a run not queued is not counted");
    claim(&pool, also, id, "worker-a").await;
    assert_eq!(written().await, 0, "the last busy run reaching a worker clears it");

    // Changing the project's worker levers tries its queued runs afresh,
    // and what was written of its workers goes with those tries.
    let waiting = queued_run(&journal, id).await;
    busy(&pool, waiting).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    projects.set_worker_overrides(id, &weft_platform_traits::WorkerOverrides::default()).await.unwrap();
    assert_eq!(written().await, 0, "the levers changing clears it");
    assert_eq!(run_column::<Option<i64>>(&pool, "busy_since", waiting).await, Some(None), "its stretch starts afresh");
    assert_eq!(run_column::<bool>(&pool, "waited_on_workers", waiting).await, Some(false), "and so does its mark");

    // A busy run that goes without reaching a worker, cancelled or
    // erased, takes what was written of its workers with it, as a claim's
    // does.
    let cancelled = queued_run(&journal, id).await;
    busy(&pool, cancelled).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    journal.cancel_run(cancelled, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!(run_column::<Option<i64>>(&pool, "busy_since", cancelled).await, Some(None), "an ended run has no stretch");
    assert_eq!(written().await, 0, "a cancelled busy run clears it");
    let erased = queued_run(&journal, id).await;
    busy(&pool, erased).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "this project's worker has answered busy for 60 minutes").await.unwrap().log();
    journal.delete_run(erased).await.unwrap();
    assert_eq!(written().await, 0, "an erased busy run clears it");

    // One record holds every failure of the project's workers: a busy run
    // going leaves it while a run they could not be reached with, or one
    // they refused, still waits, and the last of those reaching a worker
    // clears it.
    let gone = queued_run(&journal, id).await;
    let unreached_run = queued_run(&journal, id).await;
    busy(&pool, gone).await.unwrap().expect("queued");
    weft_dispatcher::delivery::unreached(&pool, unreached_run).await.unwrap().expect("queued");
    failed(&pool, Callee::Workers(id), None, "connection refused").await.unwrap().log();
    journal.cancel_run(gone, None, &weft_core::run::CancelCause::User).await.unwrap();
    assert_eq!(written().await, 1, "the run they could not be reached with still waits: its failure stays");
    let refused_run = queued_run(&journal, id).await;
    weft_dispatcher::delivery::refused(&pool, refused_run).await.unwrap().expect("queued");
    claim(&pool, unreached_run, id, "worker-a").await;
    assert_eq!(written().await, 1, "the run they refused still waits");
    claim(&pool, refused_run, id, "worker-a").await;
    assert_eq!(written().await, 0, "the last run they failed reaching a worker clears it");

    // A run whose tries stopped and started again (a worker of the
    // project came alive) no longer counts its failures, yet it reaching a
    // worker still clears what was written of them.
    use weft_dispatcher::delivery::{take, UNREACHED_TO_STOP};
    let back = queued_run(&journal, id).await;
    for _ in 0..UNREACHED_TO_STOP {
        weft_dispatcher::delivery::unreached(&pool, back).await.unwrap().expect("queued");
    }
    failed(&pool, Callee::Workers(id), None, "connection refused").await.unwrap().log();
    let stopped_at: i64 = run_column(&pool, "delivered_until", back).await.unwrap();
    sqlx::query(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, binary_hash, leased_until_unix, alive_since) \
         VALUES ('w-back', $1, $2, 'bin-A', $3, $4)",
    )
    .bind(id)
    .bind(TENANT)
    .bind(stopped_at + 15)
    .bind(stopped_at)
    .execute(&pool)
    .await
    .unwrap();
    let taken: Vec<_> = take(&pool, stopped_at, 32).await.unwrap().into_iter().map(|d| d.run_id).collect();
    assert!(taken.contains(&back), "its worker coming alive starts its tries again: {taken:?}");
    assert_eq!(run_column::<i16>(&pool, "unreached", back).await, Some(0), "its tries start again");
    assert_eq!(run_column::<bool>(&pool, "waited_on_workers", back).await, Some(true), "the failure it carried is kept");
    assert_eq!(written().await, 1, "nothing reached a worker yet");
    claim(&pool, back, id, "worker-a").await;
    assert_eq!(written().await, 0, "a run they failed reaching a worker clears it, its tries started again or not");
    assert_eq!(run_column::<bool>(&pool, "waited_on_workers", back).await, Some(false), "a claim clears the mark");
}

/// A drain marks the runs it waits on watched, and the ones waiting to
/// resume, so their ending announces itself; it looks again when the
/// grace of a run just handed to a worker ends, and, once that worker's
/// lease outlives the grace, when the lease could lapse, which for a
/// worker whose runs only wait is its vouch's end.
#[sqlx::test]
async fn a_drain_hears_its_runs_end_and_looks_at_a_vouch_end(pool: PgPool) {
    use weft_dispatcher::drain::{mark_watched, workers_look, DrainScope, Reaching};
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let every = weft_core::instance::Copies::Shared;
    let scope = DrainScope { project_id: id, reaching: Reaching::Copies(&every), except: None };
    let queued = queued_run(&journal, id).await;
    let parked = queued_run(&journal, id).await;
    sqlx::query("UPDATE run SET state = 'parked' WHERE run_id = $1").bind(parked).execute(&pool).await.unwrap();
    let driven = queued_run(&journal, id).await;
    claim(&pool, driven, id, "worker-a").await;

    mark_watched(&pool, &scope).await.unwrap();
    for run in [queued, parked, driven] {
        assert_eq!(run_column::<bool>(&pool, "watch_end", run).await, Some(true));
    }
    let grace: i64 = run_column(&pool, "delivered_until", driven).await.unwrap();
    assert_eq!(workers_look(&pool, &scope).await.unwrap(), Some(grace), "a run just handed is looked at when its grace ends");
    let vouch = weft_dispatcher::lease::now_unix() + 3 * 86_400;
    sqlx::query(
        "INSERT INTO worker_lease (replica, project_id, tenant_id, leased_until_unix, binary_hash) VALUES ('worker-a', $1, $2, $3, 'bin-A')",
    )
    .bind(id)
    .bind(TENANT)
    .bind(vouch)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(workers_look(&pool, &scope).await.unwrap(), Some(vouch), "then one look, at the vouch's end, the grace outlived");

    let mut flagged = pool.begin().await.unwrap();
    complete(&journal, queued).await;
    assert!(weft_dispatcher::run_ends::take_flagged(&mut flagged, queued).await.unwrap().is_some(), "its ending has somebody to tell");
}

/// Retention looks next exactly when the earliest erasable run's keep runs
/// out; a run still flagged past its keep is not erasable, so it never
/// makes retention look in a loop.
#[sqlx::test]
async fn retention_looks_next_at_the_earliest_erasable_keep(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    assert_eq!(journal.next_expiry().await.unwrap(), None);
    let ended = queued_run(&journal, id).await;
    complete(&journal, ended).await;
    sqlx::query("UPDATE run SET keep_until = 120 WHERE run_id = $1").bind(ended).execute(&pool).await.unwrap();
    assert_eq!(journal.next_expiry().await.unwrap(), Some(120));
    let flagged = queued_run(&journal, id).await;
    complete(&journal, flagged).await;
    sqlx::query("UPDATE run SET keep_until = 0, watch_end = TRUE WHERE run_id = $1").bind(flagged).execute(&pool).await.unwrap();
    assert_eq!(journal.next_expiry().await.unwrap(), Some(120), "the flagged run waits for its ending to be handled");
}

/// A resync holds the project's transition from before its take-down to
/// after its triggers are back: it enters with its plan written down and
/// its task enqueued, in one go, and only over the build the project
/// registered; no build, no second resync and no other activation start
/// meanwhile, while its own take-down and its own drive's turn-on go
/// through; none starts while a trigger is mid-flip. One that stopped (its
/// task failed, why written into its plan) still holds it and reads as
/// standing still; taken up again it gets a new drive, and the stopped
/// drive's task writes nothing more. Its task's end lands the transition
/// and keeps the plan as that drive's record. A person giving it up lands
/// the transition with what it left down written into the plan, kept for
/// `weft status` until the next resync begins; its drive turns nothing on
/// after that, and a later cancel finds nothing under way to give up.
#[sqlx::test]
async fn a_resync_keeps_builds_and_other_activations_out_until_it_lets_go(pool: PgPool) {
    use weft_dispatcher::activation_store::ClaimRefused;
    use weft_dispatcher::task_kinds::resync::{task, ResyncOwner, ResyncPlan, Stopped};
    let (_journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, None).await.unwrap().expect("claim");
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    let now = || weft_dispatcher::lease::now_unix();
    let stale = weft_dispatcher::transition::heartbeat_stale_secs();
    let transition = |projects: weft_dispatcher::ProjectStore| async move { projects.transition(id).await.unwrap().unwrap().as_str() };
    let plan = ResyncPlan {
        driver: Uuid::new_v4(),
        owners: vec![ResyncOwner { owner: Owner::Shared, on: vec!["feed".into()], down: Vec::new() }],
        spec: serde_json::from_value(json!({ "mode": "park", "runningPolicy": "cancel" })).unwrap(),
        build: weft_core::builds::BuildHashes { binary_hash: Some("bin-A".into()), ..Default::default() },
        resynced: Vec::new(),
        urls: Vec::new(),
        stopped: None,
        given_up: None,
    };
    let claim = |driver: Option<Uuid>| {
        let activations = activations.clone();
        async move { activations.try_begin_activating(id, &[feed_key()], Uuid::new_v4(), None, driver).await.unwrap().map(|_| ()) }
    };

    let mid_flip = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], mid_flip, None, None).await.unwrap().expect("a manual activate");
    assert_eq!(projects.try_begin_resyncing(id, &plan, task(TENANT, id, plan.driver)).await.unwrap(), None, "no resync while a trigger is mid-flip");
    activations.end_activating(id, mid_flip, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    let moved = ResyncPlan { build: weft_core::builds::BuildHashes { binary_hash: Some("bin-B".into()), ..Default::default() }, ..plan.clone() };
    assert_eq!(projects.try_begin_resyncing(id, &moved, task(TENANT, id, plan.driver)).await.unwrap(), None, "none over a build that moved");

    let driver = projects.try_begin_resyncing(id, &plan, task(TENANT, id, plan.driver)).await.unwrap().expect("entered");
    assert_eq!(transition(projects.clone()).await, "resyncing");
    let kind: String = sqlx::query_scalar("SELECT kind FROM task WHERE id = $1").bind(driver).fetch_one(&pool).await.unwrap();
    assert_eq!(kind, weft_task_store::TaskKind::Resync.as_str(), "its task is enqueued with it");
    let held = projects.resync(id).await.unwrap().expect("its plan is written down");
    assert_eq!((held.plan.clone(), held.driven, held.standing_still()), (plan.clone(), true, None));
    let other = ResyncPlan { driver: Uuid::new_v4(), ..plan.clone() };
    assert_eq!(projects.try_begin_resyncing(id, &other, task(TENANT, id, other.driver)).await.unwrap(), None, "one resync at a time");
    assert!(!projects.try_begin_building(id, now() - stale).await.unwrap(), "no build starts under a resync");

    let other_drive = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, Some(Uuid::new_v4()))
        .await
        .unwrap();
    assert!(matches!(other_drive, LifecycleWrite::ResyncGone), "another drive takes nothing down: {other_drive:?}");
    let down = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, Some(plan.driver))
        .await
        .unwrap();
    assert!(matches!(down, LifecycleWrite::Applied { .. }), "its own take-down goes through: {down:?}");
    assert_eq!(claim(None).await, Err(ClaimRefused::Resyncing), "no other activation under it");
    assert_eq!(claim(Some(Uuid::new_v4())).await, Err(ClaimRefused::ResyncGone), "nor another drive's");
    let again = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], again, None, Some(plan.driver)).await.unwrap().expect("its own turn-on goes through");
    activations.end_activating(id, again, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");

    // Its task stopped: why is written into the plan, which still holds.
    let stopped = ResyncPlan { stopped: Some(Stopped { status: 409, error: "infra down".into(), at_unix: now(), waits_for: None }), ..plan.clone() };
    assert!(projects.write_resync(id, &stopped).await.unwrap());
    sqlx::query("UPDATE task SET status = 'failed', completed_at_unix = $2 WHERE id = $1").bind(driver).bind(now()).execute(&pool).await.unwrap();
    let held = projects.resync(id).await.unwrap().expect("still held");
    assert_eq!(held.standing_still().as_deref(), Some("infra down"), "nothing drives it, and it says why");
    assert_eq!(projects.stopped_resyncs().await.unwrap().into_iter().map(|(p, _)| p).collect::<Vec<_>>(), [id]);
    assert!(projects.settle_transition(None, now() - stale).await.unwrap().is_empty(), "its triggers wait to come back");
    assert!(!projects.try_begin_building(id, now() - stale).await.unwrap(), "no build lands between its two halves");

    // Taken up again: its stop forgotten, a new drive and a new task.
    let resumed = projects.resume_resync(id, TENANT).await.unwrap().expect("held");
    assert_ne!(resumed, driver);
    let held = projects.resync(id).await.unwrap().expect("still held");
    assert!(held.driven && held.plan.stopped.is_none());
    let drive = held.plan.driver;
    assert_ne!(drive, plan.driver, "a new drive");
    assert!(!projects.write_resync(id, &plan).await.unwrap(), "the stopped drive writes nothing more");
    assert_eq!(projects.drive(id, plan.driver).await.unwrap(), None);
    assert_eq!(claim(Some(plan.driver)).await, Err(ClaimRefused::ResyncGone), "and turns nothing on");
    assert_eq!(projects.resume_resync(id, TENANT).await.unwrap(), Some(resumed), "one task drives it");

    // Its task ends it: the transition lands, the plan stays its record.
    assert!(!projects.end_resync(id, plan.driver).await.unwrap(), "only its own drive ends it");
    assert!(projects.end_resync(id, drive).await.unwrap());
    assert_eq!(transition(projects.clone()).await, "none");
    assert_eq!(projects.drive(id, drive).await.unwrap().map(|(p, under_way)| (p.driver, under_way)), Some((drive, false)));
    assert_eq!(projects.resync(id).await.unwrap(), None);
    assert_eq!(projects.resume_resync(id, TENANT).await.unwrap(), None, "nothing to take up");

    // A new one, given up by a person once it took the feed down: the
    // transition lands, and what it left down is written into its plan in
    // the same transaction, for `weft status`.
    let next = ResyncPlan {
        driver: Uuid::new_v4(),
        owners: vec![ResyncOwner { owner: Owner::Shared, on: vec!["feed".into()], down: vec!["feed".into()] }],
        ..plan.clone()
    };
    projects.try_begin_resyncing(id, &next, task(TENANT, id, next.driver)).await.unwrap().expect("entered over the record");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), None, "nothing given up yet");
    let taken = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, Some(next.driver))
        .await
        .unwrap();
    assert!(matches!(taken, LifecycleWrite::Applied { .. }), "{taken:?}");
    let given = projects.give_up_resync(id).await.unwrap().expect("under way");
    let left_down = vec![weft_core::activation::OwnerTriggers { owner: Owner::Shared, triggers: vec!["feed".into()] }];
    assert_eq!(given.driver, next.driver);
    assert_eq!(given.given_up.as_ref().map(|g| &g.left_down), Some(&left_down), "the feed it took down stays down");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), given.given_up, "kept for `weft status`");
    assert_eq!(transition(projects.clone()).await, "none");
    assert_eq!(projects.drive(id, next.driver).await.unwrap(), None, "its drive reads nothing to go on with");
    assert!(!projects.write_resync(id, &next).await.unwrap(), "nor writes");
    assert_eq!(claim(Some(next.driver)).await, Err(ClaimRefused::ResyncGone), "its drive turns nothing on");
    let late = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, Some(next.driver))
        .await
        .unwrap();
    assert!(matches!(late, LifecycleWrite::ResyncGone), "and takes nothing down: {late:?}");
    assert_eq!(projects.resync(id).await.unwrap(), None, "nothing is under way");
    assert!(projects.try_begin_building(id, now() - stale).await.unwrap(), "a build starts once it let go");
    assert_eq!(projects.give_up_resync(id).await.unwrap(), None, "a later cancel finds no resync under way");
    assert_eq!(transition(projects.clone()).await, "building", "and changes nothing: the build it let through keeps the project");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), given.given_up, "what it left down is still shown");

    // The next resync begins over it: what the given-up one left down is
    // no longer shown.
    assert_eq!(projects.settle_transition(Some(id), now() + stale).await.unwrap(), [id], "the build let go");

    // A person takes the feed down on purpose: the record stops naming it,
    // in the take-down's own transaction.
    let decided = activations
        .set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, None)
        .await
        .unwrap();
    assert!(matches!(decided, LifecycleWrite::Applied { .. }), "{decided:?}");
    let given_up = projects.given_up_resync(id).await.unwrap().expect("the record stays");
    assert!(given_up.left_down.is_empty(), "nothing it left down is undecided: {given_up:?}");
    assert_eq!(given_up.at_unix, given.given_up.as_ref().unwrap().at_unix);

    let third = ResyncPlan { driver: Uuid::new_v4(), ..plan.clone() };
    projects.try_begin_resyncing(id, &third, task(TENANT, id, third.driver)).await.unwrap().expect("entered over the record");
    assert_eq!(projects.given_up_resync(id).await.unwrap(), None);
}

/// A reactivation over parked triggers that fails puts them back with
/// everything they kept: the entry row and the event parked on it, the
/// wait of a run the trigger fired and the answer a person sent it. An
/// activation that is cancelled or found dead drops only the trigger's
/// own entry row: the wait and its answer stay with the run, which
/// nothing there ends.
#[sqlx::test]
async fn a_failed_reactivation_keeps_what_the_park_kept(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, None).await.unwrap().expect("claim");
    journal.signal_insert(&governed_entry("kept-entry", id, first)).await.unwrap();
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    let waiting_run = queued_run(&journal, id).await;
    let mut wait = entry_signal("kept-wait", id);
    wait.node_id = "ask".into();
    wait.is_resume = true;
    wait.run_id = Some(waiting_run);
    wait.activation_trigger = Some("feed".into());
    journal.signal_insert(&wait).await.unwrap();
    activations.set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, None).await.unwrap();
    assert_eq!(park(&pool, "kept-entry", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);
    assert_eq!(park(&pool, "kept-wait", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);
    let parked_on = |token: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM parked_fire WHERE token = $1").bind(token).fetch_one(&pool).await.unwrap()
        }
    };

    let failing = Uuid::new_v4();
    let previous = activations.try_begin_activating(id, &[feed_key()], failing, None, None).await.unwrap().expect("claim");
    assert!(activations.restore(id, failing, &previous).await.unwrap());
    let after = activations.list(id).await.unwrap();
    assert_eq!(after[0].lifecycle.mode().as_str(), "park", "the trigger is parked again");
    for token in ["kept-entry", "kept-wait"] {
        assert!(journal.signal_get(token).await.unwrap().is_some(), "{token} kept");
        assert_eq!(parked_on(token).await, 1, "and what waits on {token} too");
    }

    let cancelled = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], cancelled, None, None).await.unwrap().expect("claim");
    let removed = activations
        .end_activating(id, cancelled, &ActivationLifecycle::wiped(), true, None)
        .await
        .unwrap()
        .expect("owned");
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), ["kept-entry"], "only the entry row goes");
    assert!(journal.signal_get("kept-wait").await.unwrap().is_some(), "the run's wait stays");
    assert_eq!(parked_on("kept-wait").await, 1, "with the answer sent to it");
}

/// An activation nobody will finish (cancelled, or its driver dead) over
/// triggers that kept work lands them parked with every signal and what
/// waits on it; over triggers that kept none, it wipes them.
#[sqlx::test]
async fn an_unfinished_activation_keeps_what_its_triggers_kept(pool: PgPool) {
    let (journal, projects) = setup(&pool).await;
    let activations = PostgresActivationStore::new(pool.clone());
    let id = Uuid::new_v4();
    seed_project(&projects, id, "bin-A").await;
    let first = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], first, None, None).await.unwrap().expect("claim");
    journal.signal_insert(&governed_entry("kept-entry", id, first)).await.unwrap();
    activations.end_activating(id, first, &ActivationLifecycle::active(), false, None).await.unwrap().expect("active");
    activations.set_lifecycle_guarded(id, &[feed_key()], &ActivationLifecycle::parked(), SignalsGoing::Kept, None).await.unwrap();
    assert_eq!(park(&pool, "kept-entry", &parked(0, 0), None).await.unwrap(), ParkAppend::Parked);

    let cancelled = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], cancelled, None, None).await.unwrap().expect("claim");
    let removed = activations.end_unfinished(id, cancelled).await.unwrap().expect("owned");
    assert!(removed.is_empty(), "nothing goes");
    assert_eq!(activations.list(id).await.unwrap()[0].lifecycle.mode().as_str(), "park", "parked with what it kept");
    let waiting: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM parked_fire WHERE token = 'kept-entry'").fetch_one(&pool).await.unwrap();
    assert_eq!(waiting, 1, "the event waiting on it stays");
    assert!(activations.end_unfinished(id, cancelled).await.unwrap().is_none(), "ended once");

    sqlx::query("DELETE FROM parked_fire").execute(&pool).await.unwrap();
    let empty = Uuid::new_v4();
    activations.try_begin_activating(id, &[feed_key()], empty, None, None).await.unwrap().expect("claim");
    let removed = activations.end_unfinished(id, empty).await.unwrap().expect("owned");
    assert_eq!(removed.iter().map(|s| s.token.as_str()).collect::<Vec<_>>(), ["kept-entry"], "no work kept: wiped");
    assert_eq!(activations.list(id).await.unwrap()[0].lifecycle.mode().as_str(), "wipe");
}
