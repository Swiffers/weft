//! Layer-3 integration tests for the supervisor's ownership loop.
//!
//! The ownership loop is the single site that claims + renews this
//! supervisor's exclusive project leases, and sweeps what the host still
//! runs for projects that are gone. These tests exercise the loop's
//! broker and host contract against the in-memory fakes:
//!   - the tick calls `sync_ownership` with THIS supervisor's replica
//!     (so the broker claims under the right identity),
//!   - the work loops read the owned set via `owned_projects`, and
//!   - a copy whose project is gone is terminated, one whose project
//!     lives is left alone, and
//!   - a project's copies are judged and deleted only under its lease:
//!     one nobody leases is claimed first, and a lease that moves
//!     mid-sweep stops the deletion.
//!
//! The SQL-level exclusivity (two supervisors never claim one project)
//! lives in the broker's transactional claim and is exercised by its
//! database suite; here we pin the loop's call shape.

use weft_infra_supervisor::broker_ops::{BrokerCall, BrokerSupervisorOps};
use weft_infra_supervisor::testing::SupervisorTestRig;
use weft_platform_traits::HostCall;

const P1: uuid::Uuid = uuid::Uuid::from_u128(1);
const GONE: uuid::Uuid = uuid::Uuid::from_u128(9);

#[tokio::test]
async fn ownership_tick_syncs_under_this_supervisors_identity() {
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);

    rig.tick_ownership().await.unwrap();

    let synced = rig
        .broker
        .calls()
        .iter()
        .any(|c| matches!(c, BrokerCall::SyncOwnership { replica, .. } if replica == "test-supervisor"));
    assert!(synced, "ownership tick must sync_ownership(test-supervisor)");
}

/// A pass that ends gives its leases back, so a machine's next word
/// about a project is looked at by whichever process it wakes, and
/// reports the projects it looked at with nothing given up. It books
/// nothing itself: the broker books the next pass as it answers.
#[tokio::test]
async fn a_pass_gives_its_leases_back_and_reports_what_it_settled() {
    use weft_broker_client::protocol::InfraNodeStatus;
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);
    rig.broker.add_infra_node(P1, "db", "i-1", InfraNodeStatus::Running);
    let pass = weft_infra_supervisor::tick(&rig.state).await;
    assert_eq!(pass.settled, vec![P1]);
    let last = rig.broker.calls().into_iter().rev().find(|c| matches!(c, BrokerCall::SyncOwnership { .. }));
    assert!(
        matches!(&last, Some(BrokerCall::SyncOwnership { settled, ending: true, .. }) if settled == &vec![P1]),
        "the pass ends giving every lease back: {last:?}"
    );
    assert!(rig.broker.owned_projects("test-supervisor").await.unwrap().is_empty());
}

/// A gone copy that keeps failing to delete reports its project failed
/// once per pass, with the pass's end (where the broker writes it down
/// and books its retry), and the project is not reported settled.
#[tokio::test]
async fn a_sweep_that_keeps_failing_is_reported_failed_once_per_pass() {
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);
    let removed = weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project: P1,
        node: "removed".into(),
        copy_id: "i-removed".into(),
    };
    rig.host.set_state(&removed, "main", weft_platform_traits::UnitRunState::Ready);
    rig.broker.undeclare(P1, "removed");
    rig.host.fail_terminates_of("i-removed");

    let pass = weft_infra_supervisor::tick(&rig.state).await;
    assert_eq!(rig.broker.failed(), vec![P1]);
    let reported = rig.broker.calls().into_iter().rev().find_map(|c| match c {
        BrokerCall::SyncOwnership { failed, ending, .. } if !failed.is_empty() => Some((failed, ending)),
        _ => None,
    });
    let (failed, ending) = reported.expect("a sync reported the failure");
    assert!(ending, "the pass's end reports it");
    assert!(failed[0].error.contains("could not delete"), "{failed:?}");
    assert!(pass.settled.is_empty(), "a project that failed is not settled");
    weft_infra_supervisor::tick(&rig.state).await;
    assert_eq!(rig.broker.failed(), vec![P1, P1], "each pass that fails reports it once");
    assert!(rig.broker.gave_up().is_empty());
}

/// A project ownable only through its copies on the host stays ownable
/// for the whole pass: the pass's end carries the host's listing too, so
/// the broker books the retry of its failed sweep, and reports it looked
/// at, so the broker takes the retry this pass made.
#[tokio::test]
async fn every_sync_of_a_pass_carries_the_hosts_listing() {
    const IDLE: uuid::Uuid = uuid::Uuid::from_u128(3);
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_infraless_project(IDLE);
    let kept = weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project: IDLE,
        node: "db".into(),
        copy_id: "i-kept".into(),
    };
    rig.host.set_state(&kept, "main", weft_platform_traits::UnitRunState::Ready);
    rig.host.fail_terminates_of("i-kept");

    weft_infra_supervisor::tick(&rig.state).await;

    let syncs: Vec<_> = rig
        .broker
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            BrokerCall::SyncOwnership { held_projects, looked, failed, ending, .. } => Some((held_projects, looked, failed, ending)),
            _ => None,
        })
        .collect();
    assert!(syncs.len() >= 2, "a pass claims, then ends: {syncs:?}");
    assert!(syncs.iter().all(|(held, ..)| held == &Some(vec![IDLE])), "every sync carries the listing: {syncs:?}");
    let (_, looked, failed, ending) = syncs.last().unwrap();
    assert!(ending);
    assert_eq!(looked, &vec![IDLE], "the pass looked at the project it owns");
    assert_eq!(failed.iter().map(|t| t.project_id).collect::<Vec<_>>(), vec![IDLE], "its sweep failed");
}

/// A health look that fails as a whole (what the pass owns did not read)
/// fails the whole pass: its end reports why as a failure over every
/// project the pass holds as it ends (a project a renewal claimed
/// mid-pass included), which the broker counts and writes down per
/// project, and nothing settled.
#[tokio::test]
async fn a_health_look_that_fails_whole_fails_the_whole_pass() {
    use weft_broker_client::protocol::InfraNodeStatus;
    const P2: uuid::Uuid = uuid::Uuid::from_u128(2);
    let rig = SupervisorTestRig::with_tenant("alice");
    for project in [P1, P2] {
        rig.broker.add_project(project);
        rig.broker.add_infra_node(project, "db", "i-1", InfraNodeStatus::Running);
    }
    rig.broker.fail_owned_reads(1);

    let pass = weft_infra_supervisor::tick(&rig.state).await;

    assert!(pass.settled.is_empty(), "nothing was looked at with success");
    let last = rig.broker.calls().into_iter().rev().find_map(|c| match c {
        BrokerCall::SyncOwnership { looked, failed, settled, all_failed, ending, .. } => Some((looked, failed, settled, all_failed, ending)),
        _ => None,
    });
    let (looked, failed, settled, all_failed, ending) = last.expect("the pass ended with a sync");
    assert!(ending);
    assert!(looked.is_empty() && failed.is_empty() && settled.is_empty(), "no project was looked at");
    let all_failed = all_failed.expect("the pass failed as a whole");
    assert!(all_failed.contains("the owned projects did not read"), "{all_failed}");
}

/// A host that cannot be listed: the copies-only listing step fails.
struct UnlistableHost(std::sync::Arc<weft_platform_traits::FakeInfraHost>);

#[async_trait::async_trait]
impl weft_platform_traits::InfraHost for UnlistableHost {
    fn check(&self, node: &weft_core::infra::ResolvedNode) -> Result<(), String> {
        self.0.check(node)
    }
    async fn apply_unit(&self, node: &weft_core::infra::ResolvedNode, unit: &str) -> anyhow::Result<()> {
        self.0.apply_unit(node, unit).await
    }
    async fn stop_unit(&self, node: &weft_core::infra::NodeRef, unit: &str) -> anyhow::Result<()> {
        self.0.stop_unit(node, unit).await
    }
    async fn remove_unit(&self, node: &weft_core::infra::NodeRef, unit: &str) -> anyhow::Result<()> {
        self.0.remove_unit(node, unit).await
    }
    async fn terminate(&self, node: &weft_core::infra::NodeRef, keep_disks: &[String]) -> anyhow::Result<()> {
        self.0.terminate(node, keep_disks).await
    }
    async fn observe(&self, tenant: &str, project: uuid::Uuid) -> anyhow::Result<Vec<weft_platform_traits::UnitObservation>> {
        self.0.observe(tenant, project).await
    }
    async fn copies(&self) -> anyhow::Result<Vec<weft_core::infra::NodeRef>> {
        anyhow::bail!("the host did not list")
    }
    async fn endpoint(&self, node: &weft_core::infra::ResolvedNode, endpoint: &str) -> anyhow::Result<weft_platform_traits::EndpointAt> {
        self.0.endpoint(node, endpoint).await
    }
    async fn logs(
        &self,
        node: &weft_core::infra::NodeRef,
        unit: &str,
        from: &weft_core::infra::wire::LogsFrom,
    ) -> anyhow::Result<Vec<weft_core::infra::wire::LogStream>> {
        self.0.logs(node, unit, from).await
    }
}

/// A pass whose host listing fails sweeps nothing and fails as a whole:
/// every sync says no listing went through (the broker keeps a retry only
/// the listing could make ownable booked), its end reports why, which the
/// broker counts against each project the pass covered, and nothing is
/// settled.
#[tokio::test]
async fn a_pass_whose_listing_fails_fails_as_a_whole() {
    use weft_broker_client::protocol::InfraNodeStatus;
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);
    rig.broker.add_infra_node(P1, "db", "i-1", InfraNodeStatus::Running);
    let mut state = rig.state.clone();
    state.host = std::sync::Arc::new(UnlistableHost(rig.host.clone()));

    let pass = weft_infra_supervisor::tick(&state).await;

    assert!(pass.settled.is_empty(), "a pass that could not sweep settles nothing");
    let syncs: Vec<_> = rig
        .broker
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            BrokerCall::SyncOwnership { held_projects, looked, all_failed, ending, .. } => Some((held_projects, looked, all_failed, ending)),
            _ => None,
        })
        .collect();
    assert!(syncs.iter().all(|(held, ..)| held.is_none()), "no sync carries a listing: {syncs:?}");
    let (_, looked, all_failed, ending) = syncs.last().unwrap();
    assert!(ending);
    assert_eq!(looked, &vec![P1], "the project it owns is still looked at");
    let all_failed = all_failed.as_deref().expect("the pass failed as a whole");
    assert!(all_failed.contains("the host did not list"), "{all_failed}");
    assert!(!rig.broker.calls().iter().any(|c| matches!(c, BrokerCall::GoneCopies { .. })), "nothing is swept");
}

#[tokio::test]
async fn work_loops_read_owned_projects_not_a_global_list() {
    // Both work loops must scope their work to the owned set (via
    // owned_projects), never a global all-projects read.
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);

    rig.tick_health().await.unwrap();

    let read_owned = rig
        .broker
        .calls()
        .iter()
        .any(|c| matches!(c, BrokerCall::OwnedProjects { replica } if replica == "test-supervisor"));
    assert!(read_owned, "health tick must read owned_projects(test-supervisor), not a global project list");
}

/// A project removed without waiting for its terminate leaves its copy
/// running on the host with no row to find it by: the ownership tick
/// terminates it. A copy of a project that still exists is left alone.
#[tokio::test]
async fn a_copy_of_a_gone_project_is_terminated() {
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);
    let copy = |project, copy_id: &str| weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project,
        node: "db".into(),
        copy_id: copy_id.into(),
    };
    rig.host.set_state(&copy(P1, "live"), "db", weft_platform_traits::UnitRunState::Ready);
    rig.host.set_state(&copy(GONE, "orphan"), "db", weft_platform_traits::UnitRunState::Ready);

    rig.tick_ownership().await.unwrap();

    let terminated: Vec<String> = rig
        .host
        .calls()
        .into_iter()
        .filter_map(|c| match c {
            HostCall::Terminate { copy_id, .. } => Some(copy_id),
            _ => None,
        })
        .collect();
    assert_eq!(terminated, vec!["orphan".to_string()]);
}

/// A pass that deletes every copy of a removed project ends with a
/// listing that no longer holds it, which is what clears the broker's
/// record of its leftovers; one whose deletion failed still holds it and
/// reports it failed, which the broker counts as the install's.
#[tokio::test]
async fn a_pass_that_empties_a_removed_project_ends_without_it_in_the_listing() {
    const STUCK: uuid::Uuid = uuid::Uuid::from_u128(10);
    let rig = SupervisorTestRig::with_tenant("alice");
    let orphan = |project, copy_id: &str| weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project,
        node: "db".into(),
        copy_id: copy_id.into(),
    };
    rig.host.set_state(&orphan(GONE, "a"), "db", weft_platform_traits::UnitRunState::Ready);
    rig.host.set_state(&orphan(STUCK, "b"), "db", weft_platform_traits::UnitRunState::Ready);
    rig.host.fail_terminates_of("b");

    weft_infra_supervisor::tick(&rig.state).await;
    let (held, failed) = rig
        .broker
        .calls()
        .into_iter()
        .rev()
        .find_map(|c| match c {
            BrokerCall::SyncOwnership { held_projects, failed, ending: true, .. } => Some((held_projects, failed)),
            _ => None,
        })
        .expect("the pass ended");
    assert_eq!(held, Some(vec![STUCK]), "the emptied project is gone from the listing, the stuck one stays");
    assert_eq!(failed.iter().map(|t| t.project_id).collect::<Vec<_>>(), vec![STUCK]);
}

/// A copy terminated with disks its node keeps holds only those disks
/// and no row. Once the program no longer declares it, it is gone for
/// good and the tick deletes everything, kept disks included. A kept
/// copy the program still declares waits for its next start, a copy
/// still holding a row is the reap's, and another supervisor's project
/// is not judged here.
#[tokio::test]
async fn a_kept_copy_the_program_no_longer_declares_loses_its_kept_disks() {
    const OTHER: uuid::Uuid = uuid::Uuid::from_u128(2);
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);
    rig.broker.add_project(OTHER);
    rig.broker.set_project_owned(OTHER, false);
    let copy = |project, node: &str, copy_id: &str| weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project,
        node: node.into(),
        copy_id: copy_id.into(),
    };
    for c in [copy(P1, "removed", "i-removed"), copy(P1, "db", "i-db"), copy(OTHER, "removed", "i-other")] {
        rig.host.set_state(&c, "main", weft_platform_traits::UnitRunState::Ready);
        weft_platform_traits::InfraHost::terminate(rig.host.as_ref(), &c, &["data".to_string()]).await.unwrap();
    }
    rig.host.set_state(&copy(P1, "orphan", "i-orphan"), "main", weft_platform_traits::UnitRunState::Ready);
    rig.broker.add_infra_node(P1, "orphan", "i-orphan", weft_broker_client::protocol::InfraNodeStatus::Running);
    rig.broker.undeclare(P1, "removed");
    rig.broker.undeclare(P1, "orphan");
    rig.broker.undeclare(OTHER, "removed");
    let before = rig.host.calls().len();

    rig.tick_ownership().await.unwrap();

    let terminated: Vec<HostCall> = rig.host.calls().into_iter().skip(before).collect();
    assert_eq!(terminated, vec![HostCall::Terminate { copy_id: "i-removed".into(), keep: vec![] }]);
    assert!(rig.host.kept_disks("i-removed").is_empty());
    assert_eq!(rig.host.kept_disks("i-db"), ["data"], "a declared copy keeps its disks for its next start");
}

/// One gone copy failing to delete does not stop the sweep: the next
/// gone copy is still deleted, and the failed one is tried again on the
/// next tick.
#[tokio::test]
async fn a_failed_deletion_does_not_stop_the_sweep() {
    let rig = SupervisorTestRig::with_tenant("alice");
    let copy = |copy_id: &str| weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project: GONE,
        node: "db".into(),
        copy_id: copy_id.into(),
    };
    for copy_id in ["a-stuck", "b-orphan"] {
        rig.host.set_state(&copy(copy_id), "db", weft_platform_traits::UnitRunState::Ready);
    }
    rig.host.fail_terminates_of("a-stuck");

    rig.tick_ownership().await.unwrap();

    let held: Vec<String> = weft_platform_traits::InfraHost::copies(rig.host.as_ref())
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.copy_id)
        .collect();
    assert_eq!(held, vec!["a-stuck".to_string()], "the orphan is deleted even though the stuck copy failed first");
}

/// A project a lifecycle command holds is not swept: its apply may be
/// adopting the very copy the sweep judged gone. The next tick after the
/// command lets go deletes it.
#[tokio::test]
async fn a_project_a_command_holds_is_swept_only_after_it_lets_go() {
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);
    let removed = weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project: P1,
        node: "removed".into(),
        copy_id: "i-removed".into(),
    };
    rig.host.set_state(&removed, "main", weft_platform_traits::UnitRunState::Ready);
    rig.broker.undeclare(P1, "removed");

    let command = rig.state.project_locks.share(P1).await;
    rig.tick_ownership().await.unwrap();
    assert!(
        !rig.host.calls().iter().any(|c| matches!(c, HostCall::Terminate { .. })),
        "nothing is judged or deleted while a command holds the project"
    );
    drop(command);
    rig.tick_ownership().await.unwrap();

    assert!(rig.host.calls().contains(&HostCall::Terminate { copy_id: "i-removed".into(), keep: vec![] }));
}

/// A project that still exists but declares no infra and has no row left
/// has no work to own, yet its kept disks sit on the host. The tick
/// claims its lease because the host holds its copy, and then sweeps
/// under that lease; before the claim, nothing judges it.
#[tokio::test]
async fn an_unleased_projects_copy_is_claimed_then_swept() {
    const IDLE: uuid::Uuid = uuid::Uuid::from_u128(3);
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_infraless_project(IDLE);
    let kept = weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project: IDLE,
        node: "db".into(),
        copy_id: "i-kept".into(),
    };
    rig.host.set_state(&kept, "main", weft_platform_traits::UnitRunState::Ready);
    weft_platform_traits::InfraHost::terminate(rig.host.as_ref(), &kept, &["data".to_string()]).await.unwrap();

    let change = rig.tick_ownership().await.unwrap();

    let calls = rig.broker.calls();
    let synced = calls
        .iter()
        .position(|c| matches!(c, BrokerCall::SyncOwnership { held_projects, .. } if held_projects == &Some(vec![IDLE])))
        .expect("the tick asks for the lease of the project the host holds");
    let judged = calls
        .iter()
        .position(|c| matches!(c, BrokerCall::GoneCopies { project, .. } if *project == IDLE))
        .expect("the project is judged once leased");
    assert!(synced < judged, "the lease is claimed before the judgment");
    assert!(rig.host.calls().contains(&HostCall::Terminate { copy_id: "i-kept".into(), keep: vec![] }));
    assert!(rig.host.kept_disks("i-kept").is_empty());
    assert_eq!(
        change,
        Some(weft_infra_supervisor::ownership::OwnershipChange { claimed: vec![IDLE], lost: vec![] }),
        "the lease over the held project is taken on"
    );
    let change = rig.tick_ownership().await.unwrap();
    assert_eq!(
        change,
        Some(weft_infra_supervisor::ownership::OwnershipChange { claimed: vec![], lost: vec![IDLE] }),
        "with nothing left on the host, the lease is not renewed and the project is lost"
    );
}

/// A stop of the shared copies of `project`.
fn stop_of(id: i64, project: uuid::Uuid) -> weft_broker_client::protocol::SupervisorCommandRow {
    weft_broker_client::protocol::SupervisorCommandRow {
        id,
        project_id: project,
        node_id: None,
        verb: weft_broker_client::protocol::InfraLifecycleVerb::Stop,
        spec_json: None,
        force: false,
        copies: weft_core::instance::Copies::Shared,
        cancel_requested: false,
    }
}

/// A command waiting on a project is enough to own it: its lease is
/// never reported lost while the command is pending (the orphan reap's
/// Terminate removes the last `infra_node` row before it completes), and
/// once the command completes, the lease lapses.
#[tokio::test]
async fn a_project_is_never_lost_while_its_command_is_pending() {
    const IDLE: uuid::Uuid = uuid::Uuid::from_u128(3);
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_infraless_project(IDLE);
    rig.broker.enqueue_command(stop_of(1, IDLE));
    let change = rig.tick_ownership().await.unwrap().expect("the tick takes the project on");
    assert_eq!(change.claimed, vec![IDLE]);

    assert_eq!(rig.tick_ownership().await.unwrap(), None, "a pending command keeps the lease");

    assert!(rig.tick_lifecycle().await.unwrap(), "the owner runs the command");
    let change = rig.tick_ownership().await.unwrap().expect("the lease lapses once the command completed");
    assert_eq!(change.lost, vec![IDLE]);
}

/// A command on a project with no infra and nothing on the host still
/// finds an owner, which runs and completes it.
#[tokio::test]
async fn a_pending_command_on_a_project_with_nothing_is_claimed_and_run() {
    const IDLE: uuid::Uuid = uuid::Uuid::from_u128(3);
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_infraless_project(IDLE);
    rig.broker.enqueue_command(stop_of(1, IDLE));

    rig.tick_ownership().await.unwrap();
    assert!(rig.tick_lifecycle().await.unwrap(), "the command is claimed and run");

    let completed: Vec<i64> = rig.broker.completed_commands().iter().map(|(id, _, _)| *id).collect();
    assert_eq!(completed, vec![1]);
}

/// Another supervisor taking the lease between the judgment and the
/// deletion (it could be applying the re-added node, adopting the very
/// disks judged gone) stops the sweep: nothing is deleted.
#[tokio::test]
async fn a_sweep_whose_lease_moved_after_judging_deletes_nothing() {
    let rig = SupervisorTestRig::with_tenant("alice");
    rig.broker.add_project(P1);
    let removed = weft_core::infra::NodeRef {
        tenant: "alice".into(),
        project: P1,
        node: "removed".into(),
        copy_id: "i-removed".into(),
    };
    rig.host.set_state(&removed, "main", weft_platform_traits::UnitRunState::Ready);
    rig.broker.undeclare(P1, "removed");
    rig.broker.displace_on_judgment(P1);

    rig.tick_ownership().await.unwrap();

    assert!(
        !rig.host.calls().iter().any(|c| matches!(c, HostCall::Terminate { .. })),
        "a copy is deleted only while this supervisor still holds the project's lease"
    );
}
