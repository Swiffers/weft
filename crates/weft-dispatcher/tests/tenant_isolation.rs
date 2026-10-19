//! Layer-3 contract test: multi-tenant isolation, proven against the in-memory
//! fakes (`FakeProjectStore`, `FakeJournal`) that are the real isolation
//! enforcers. Two tenants register projects, run runs, and mint tokens;
//! the test asserts neither can see, reach, or seize the other's resources.
//!
//! These exercise the SAME scoping code the Postgres impls run (the `WHERE
//! tenant_id = $caller` filters and the cross-tenant register guard), since the
//! fakes mirror those queries. The HTTP gate (`authenticator::authorize_project`)
//! is a thin wrapper over `ProjectStore::tenant_for`, also covered here.

use chrono::Utc;
use uuid::Uuid;
use weft_core::ProjectDefinition;
use weft_dispatcher::authenticator::authorize_run;
use weft_dispatcher::journal::{RunListQuery, SignalToken, Journal, FakeJournal};
use weft_dispatcher::project_store::{FakeProjectStore, ProjectStoreOps};
use weft_dispatcher::tenant::TenantId;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

fn definition(id: Uuid) -> ProjectDefinition {
    ProjectDefinition {
        id,
        nodes: vec![],
        edges: vec![],
        groups: vec![],
        created_at: Utc::now(),
        updated_at: Utc::now(),
        defaults: Default::default(),
        given: Default::default(),
        broken: Vec::new(),
    }
}

async fn register(store: &FakeProjectStore, id: Uuid, name: &str, tenant: &str) {
    store
        .register_with_hashes(definition(id), name, "", tenant, None, None, None, None, None, None, None)
        .await
        .expect("register");
}

#[tokio::test]
async fn list_projects_is_scoped_to_the_caller_tenant() {
    let store = FakeProjectStore::new();
    let a1 = Uuid::new_v4();
    let a2 = Uuid::new_v4();
    let b1 = Uuid::new_v4();
    register(&store, a1, "a-one", TENANT_A).await;
    register(&store, a2, "a-two", TENANT_A).await;
    register(&store, b1, "b-one", TENANT_B).await;

    let a_list = store.list(TENANT_A).await.unwrap();
    let a_ids: Vec<Uuid> = a_list.iter().map(|p| p.id).collect();
    assert!(a_ids.contains(&a1) && a_ids.contains(&a2), "A sees its own");
    assert!(!a_ids.contains(&b1), "A must NOT see B's project");

    let b_list = store.list(TENANT_B).await.unwrap();
    let b_ids: Vec<Uuid> = b_list.iter().map(|p| p.id).collect();
    assert_eq!(b_ids, vec![b1], "B sees only its own");
}

#[tokio::test]
async fn tenant_for_drives_the_per_resource_gate() {
    // `authorize_project` authorizes iff `tenant_for(id) == caller`. Prove the
    // primitive it relies on returns the true owner, so a cross-tenant caller is
    // rejected and a missing project is indistinguishable from a foreign one.
    let store = FakeProjectStore::new();
    let a1 = Uuid::new_v4();
    register(&store, a1, "a-one", TENANT_A).await;

    assert_eq!(store.tenant_for(a1).await.unwrap().as_deref(), Some(TENANT_A));
    // A missing project: None (the gate maps both None and wrong-tenant to 404).
    assert_eq!(store.tenant_for(Uuid::new_v4()).await.unwrap(), None);
}

#[tokio::test]
async fn cross_tenant_project_id_takeover_is_refused() {
    // Tenant B may not re-register tenant A's project id to seize it. The
    // register guard (mirroring the Postgres `WHERE project.tenant_id =
    // EXCLUDED.tenant_id`) rejects the collision, and A's ownership stands.
    let store = FakeProjectStore::new();
    let shared_id = Uuid::new_v4();
    register(&store, shared_id, "a-owned", TENANT_A).await;

    let takeover = store
        .register_with_hashes(definition(shared_id), "b-grab", "", TENANT_B, None, None, None, None, None, None, None)
        .await;
    assert!(takeover.is_err(), "cross-tenant re-register must be refused");
    assert_eq!(
        store.tenant_for(shared_id).await.unwrap().as_deref(),
        Some(TENANT_A),
        "ownership unchanged after a refused takeover"
    );

    // The owner CAN re-register its own project (idempotent update).
    store
        .register_with_hashes(definition(shared_id), "a-owned-v2", "", TENANT_A, None, None, None, None, None, None, None)
        .await
        .expect("owner re-register allowed");
}

#[tokio::test]
async fn list_runs_is_scoped_to_the_caller_tenant() {
    let journal = FakeJournal::new();
    let proj_a = Uuid::new_v4();
    let proj_b = Uuid::new_v4();
    // Mirror the project->tenant mapping the Postgres run seed reads.
    journal.set_project_tenant(proj_a, TENANT_A);
    journal.set_project_tenant(proj_b, TENANT_B);

    let run_id_a = Uuid::new_v4();
    let run_id_b = Uuid::new_v4();
    journal
        .record_event(&started(run_id_a, proj_a))
        .await
        .unwrap();
    journal
        .record_event(&started(run_id_b, proj_b))
        .await
        .unwrap();

    let q = RunListQuery { limit: 100, ..Default::default() };
    let a = journal.list_runs(TENANT_A, &q).await.unwrap();
    assert_eq!(a.total, 1, "A sees only its run");
    assert_eq!(a.runs.len(), 1);
    assert_eq!(a.runs[0].run_id, run_id_a);

    let b = journal.list_runs(TENANT_B, &q).await.unwrap();
    assert_eq!(b.total, 1, "B sees only its run");
    assert_eq!(b.runs.len(), 1);
    assert_eq!(b.runs[0].run_id, run_id_b);
}

#[tokio::test]
async fn a_runs_owner_outlives_its_project() {
    // A run deliberately outlives its project: `weft rm` takes
    // the project, the journal keeps the record of what ran, and
    // `weft clean` is what removes the record. So ownership must be
    // answerable from the run's OWN row forever.
    //
    // The regression: authorization used to resolve the run to a
    // project and then ask the PROJECT STORE who owned it. After a
    // `weft rm` that store has no answer, so every run of a
    // removed project became un-replayable and UNDELETABLE, listed
    // forever with `weft clean` refusing them 404. Both fields now
    // come off the stamped row, which nothing can delete out from
    // under it.
    let store = FakeProjectStore::new();
    let journal = FakeJournal::new();
    let project = Uuid::new_v4();
    let project_id = project;
    register(&store, project, "doomed", TENANT_A).await;
    journal.set_project_tenant(project_id, TENANT_A);
    let run_id = Uuid::new_v4();
    journal.record_event(&started(run_id, project_id)).await.unwrap();

    let owner = journal.run_owner(run_id).await.unwrap().expect("owner while alive");
    assert_eq!(owner.tenant, TENANT_A);
    assert_eq!(owner.project_id, project_id);

    // The project goes; the project store forgets it entirely.
    store.remove(project).await.expect("remove project");
    assert_eq!(store.tenant_for(project).await.unwrap(), None, "project really gone");

    // The run's ownership is unchanged.
    let after = journal.run_owner(run_id).await.unwrap().expect("owner after removal");
    assert_eq!(after, owner, "ownership is stamped, not re-derived");
    // And it is still listed, so what the listing shows stays actionable.
    let q = RunListQuery { limit: 100, ..Default::default() };
    assert_eq!(journal.list_runs(TENANT_A, &q).await.unwrap().total, 1);

    // THE gate itself, with no project store in reach at all: the
    // owner is authorized (so replay and `weft clean` work), and a
    // stranger gets the same 404 an unknown run gets.
    let granted = authorize_run(&journal, &TenantId(TENANT_A.to_string()), run_id)
        .await
        .expect("the owner may still reach its run");
    assert_eq!(granted.project_id, project_id);
    let (refused, _) =
        authorize_run(&journal, &TenantId(TENANT_B.to_string()), run_id)
            .await
            .expect_err("a stranger may not");
    assert_eq!(refused, axum::http::StatusCode::NOT_FOUND);
    let (unknown, _) =
        authorize_run(&journal, &TenantId(TENANT_A.to_string()), Uuid::new_v4())
            .await
            .expect_err("an unknown run is refused the same way");
    assert_eq!(unknown, axum::http::StatusCode::NOT_FOUND, "no existence leak");
}

#[tokio::test]
async fn signal_tokens_are_scoped_to_the_caller_tenant() {
    let journal = FakeJournal::new();
    let tok_a = token("hash-a", TENANT_A);
    let tok_b = token("hash-b", TENANT_B);
    journal.mint_signal_token(&tok_a).await.unwrap();
    journal.mint_signal_token(&tok_b).await.unwrap();

    let a = journal.list_signal_tokens(TENANT_A).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].id, tok_a.id);

    // B cannot revoke A's token (wrong-tenant id matches nothing).
    assert!(
        !journal.revoke_signal_token(tok_a.id, TENANT_B).await.unwrap(),
        "B must not revoke A's token"
    );
    assert_eq!(journal.list_signal_tokens(TENANT_A).await.unwrap().len(), 1, "A's token survives");

    // A revokes its own token.
    assert!(journal.revoke_signal_token(tok_a.id, TENANT_A).await.unwrap());
    assert!(journal.list_signal_tokens(TENANT_A).await.unwrap().is_empty());
}


// ── helpers ─────────────────────────────────────────────────────────────────

/// `weft rm` erases a project's runs first, then takes its recorded
/// programs with its row: no run is left to read them, and nothing else
/// could reach them afterwards.
#[tokio::test]
async fn removing_a_project_takes_its_recorded_programs() {
    let store = FakeProjectStore::new();
    let project = Uuid::new_v4();
    register(&store, project, "doomed", TENANT_A).await;
    store
        .register_with_hashes(definition(project), "doomed", "", TENANT_A, None, Some("ran"), None, None, None, None, None)
        .await
        .expect("record a version");
    assert!(store.definition_for_hash(project, "ran").await.unwrap().is_some());
    assert!(store.remove(project).await.expect("remove project").is_some());
    assert_eq!(store.definition_for_hash(project, "ran").await.unwrap(), None, "the program went with the project");
    assert!(store.remove(project).await.expect("remove again").is_none(), "nothing left to remove");
}

/// A project `weft rm` marked and did not finish removing reads as being
/// removed, is not registered over, and every refusal says to run `weft
/// rm` again.
#[tokio::test]
async fn a_project_being_removed_says_so_and_takes_no_registration() {
    let store = FakeProjectStore::new();
    let project = Uuid::new_v4();
    register(&store, project, "doomed", TENANT_A).await;
    assert!(store.mark_removing(project).await.unwrap());
    let summary = store.get(project).await.unwrap().expect("still there");
    assert!(summary.removing);
    assert!(store.list(TENANT_A).await.unwrap().iter().all(|s| s.removing));
    let (status, message) = summary.unless_removing().expect_err("refused");
    assert_eq!(status, axum::http::StatusCode::CONFLICT);
    assert_eq!(message, weft_core::projects::PROJECT_BEING_REMOVED);
    let registered = store
        .register_with_hashes(definition(project), "doomed", "", TENANT_A, None, Some("late"), None, None, None, None, None)
        .await
        .expect_err("a registration over a project being removed is refused");
    assert!(weft_dispatcher::project_store::ProjectRemoving::is(&registered), "{registered:#}");
    assert!(store.remove(project).await.unwrap().is_some());
}

fn started(run_id: Uuid, project_id: uuid::Uuid) -> weft_journal::RunEvent {
    weft_journal::RunEvent::RunStarted { given: Default::default(),
        run_id,
        project_id,
        entry_node: "entry".to_string(),
        phase: weft_core::context::Phase::Fire,
        definition_hash: Some("h".to_string()),
        binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
        cut_from: None,
        seed: None,
        instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
        settings: Default::default(),
    }
}

fn token(hash: &str, tenant: &str) -> SignalToken {
    SignalToken {
        id: uuid::Uuid::new_v4(),
        kind: weft_core::signal_token::TokenKind::Caller,
        token_hash: hash.to_string(),
        recognizer: "wft-test-…".to_string(),
        tenant_id: tenant.to_string(),
        name: None,
        allowed_projects: vec![],
        allowed_tags: vec![],
        allowed_displays: vec![],
        all_displays: false,
        created_at: 0,
        instance: None,
        expires_at: None,
    }
}
