//! Ownership loop. The single site that claims + renews this
//! supervisor's EXCLUSIVE leases over project infrastructure.
//!
//! Each tick calls the broker's `sync_ownership`, which (atomically)
//! renews every project this supervisor already owns and claims a BATCH
//! of more unowned-or-expired projects' infra. Both work loops
//! (lifecycle, health) then act ONLY on the owned set (read via
//! `owned_projects`), so two supervisors never act on the same project's
//! infrastructure at once. A crashed supervisor stops renewing; its
//! leases expire after `infra_owner_lease_secs` and a live supervisor
//! adopts the projects on a later tick.
//!
//! Each tick also sweeps what the host holds for copies that are gone
//! for good (their project was removed, or the program no longer
//! declares them), deleting all of it, the disks a node listed in
//! `keepOnTerminate` included. The sweep only touches a project this
//! supervisor holds the lease of, so the tick also asks for the lease of
//! every project the host holds copies of, infra or not.
//! What a tick changed for this supervisor (projects taken on, projects lost)
//! goes to both work loops as an `OwnershipChange`, so each stops acting
//! on a lost project the moment the loss is known.
//!
//! Why a dedicated loop (not folded into the work loops): claiming is an
//! ownership-BREADTH change, while the work loops only ACT on what is
//! owned. Keeping the claim here makes "how many projects this supervisor owns"
//! change in exactly one place, never as a side effect of doing work.

use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, Result};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use weft_core::infra::NodeRef;

use crate::SupervisorState;

/// What one ownership tick changed for this supervisor, for the work loops.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnershipChange {
    /// Projects the tick took on (freshly claimed, or this supervisor's own
    /// lapsed lease revived). A command issued on one while nobody held
    /// it woke no claim of this supervisor's, so the lifecycle loop asks again.
    pub claimed: Vec<Uuid>,
    /// Projects this supervisor no longer owns. Another runs their commands
    /// now, so the lifecycle loop stops whatever it was running for them.
    pub lost: Vec<Uuid>,
}

impl OwnershipChange {
    fn is_empty(&self) -> bool {
        self.claimed.is_empty() && self.lost.is_empty()
    }
}

/// What one supervisor process knows of its ownership between syncs.
#[derive(Debug, Clone, Default)]
pub struct Ownership {
    /// What it owned after its last sync.
    owned: HashSet<Uuid>,
    /// The projects its host held at its latest listing that went
    /// through, less the ones its sweep since deleted every copy of
    /// ([`Ownership::emptied`]), `None` while none did. Every sync carries
    /// them, the ones that do not list the host included, so a project
    /// ownable only through its copies on the host stays ownable for the
    /// whole of a pass: its lease is renewed while the pass sweeps it, and
    /// its failed sweep's retry is booked as the pass ends. A removed
    /// project's failure the broker wrote down is cleared at the first sync
    /// that no longer carries it. A pass whose listing failed carries
    /// `None`, which keeps such a project's retry booked.
    held_projects: Option<Vec<Uuid>>,
}

/// Tick for as long as the process lives, handing every change to each work
/// loop in `changes` (lifecycle and health).
pub async fn run_loop(state: SupervisorState, changes: Vec<UnboundedSender<OwnershipChange>>) -> Result<()> {
    follow(state, Ownership::default(), changes, false).await
}

/// Renew what a pass of a supervisor that scales to zero owns
/// (`ownership`, what its first tick found) for as long as the pass runs,
/// at the loop's interval, handing every change to `changes`. It only
/// renews and claims: the pass listed and swept the host once as it
/// started. The pass drops it when its commands are done.
pub async fn keep_renewing(state: SupervisorState, ownership: Ownership, changes: Vec<UnboundedSender<OwnershipChange>>) -> Result<()> {
    follow(state, ownership, changes, true).await
}

/// Tick, and again at every interval (or when a work loop asks), from
/// `ownership`. In a pass (`in_pass`) the first tick waits for the
/// interval and each one only renews and claims. Otherwise each tick also
/// lists the host and sweeps its gone copies in a task of their own, so a
/// sweep deleting disks for minutes never holds up the renewal; a tick
/// that finds the last sweep still running sweeps nothing.
async fn follow(state: SupervisorState, mut ownership: Ownership, changes: Vec<UnboundedSender<OwnershipChange>>, in_pass: bool) -> Result<()> {
    if in_pass {
        wait_for_next_tick(&state).await;
    }
    let mut sweeping: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    loop {
        let ticked = match in_pass {
            true => renew(&state, &mut ownership, Syncing::MidPass).await,
            false => claim(&state, &mut ownership).await.map(|(change, held)| {
                while sweeping.try_join_next().is_some() {}
                match (held, sweeping.is_empty()) {
                    (Ok(copies), true) => {
                        let state = state.clone();
                        // The next tick lists the host again, which shows
                        // what this sweep deleted.
                        sweeping.spawn(async move {
                            sweep_gone_copies(&state, copies).await;
                        });
                    }
                    (Ok(_), false) => tracing::debug!(target: "weft_infra_supervisor::ownership", "the last sweep of gone copies still runs; this tick sweeps nothing"),
                    // Every tick lists again, so nothing is written down.
                    (Err(e), _) => tracing::error!(
                        target: "weft_infra_supervisor::ownership",
                        error = %format!("{e:#}"),
                        "listing what the host holds failed; its gone copies are swept at the next tick"
                    ),
                }
                change
            }),
        };
        match ticked {
            Ok(Some(change)) => {
                for loop_changes in &changes {
                    loop_changes
                        .send(change.clone())
                        .map_err(|_| anyhow!("a work loop is gone; it no longer follows ownership changes"))?;
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(target: "weft_infra_supervisor::ownership", error = %e, "ownership tick failed"),
        }
        wait_for_next_tick(&state).await;
    }
}

/// The ownership interval, or less when a work loop asks for a tick now.
async fn wait_for_next_tick(state: &SupervisorState) {
    tokio::select! {
        () = state.clock.sleep(state.ownership_interval) => {}
        () = state.ownership_wanted.notified() => {}
    }
}

/// One ownership tick, the sweep left to the caller: list what the host
/// holds, renew + claim (the held projects included), update `ownership`
/// (what this supervisor owned after the previous tick, and its host's
/// latest listing), and hand back what changed, if anything, with the
/// host's listing or why it failed, whose gone copies the caller sweeps
/// ([`sweep_gone_copies`]) under the leases just renewed, while it keeps
/// renewing them; a failed listing is the caller's to report. The sync
/// also reports what this supervisor's looks came to since the last one
/// (looked at, settled, failed, given up), and the broker books the next
/// pass of a supervisor that scales to zero.
pub(crate) async fn claim(state: &SupervisorState, ownership: &mut Ownership) -> Result<(Option<OwnershipChange>, Result<Vec<NodeRef>>)> {
    // A failed listing skips this tick's sweep but never the renewal:
    // losing every lease over a host hiccup would hand the projects over,
    // so the sync carries the latest listing that went through.
    let held = state.host.copies().await;
    if let Ok(copies) = &held {
        let mut held_projects: Vec<Uuid> = copies.iter().map(|c| c.project).collect();
        held_projects.sort_unstable();
        held_projects.dedup();
        ownership.held_projects = Some(held_projects);
    }
    let change = renew(state, ownership, Syncing::Claim).await?;
    Ok((change, held))
}

/// Renew + claim (the host's latest listing included), update
/// `ownership` and hand back what changed, if anything.
async fn renew(state: &SupervisorState, ownership: &mut Ownership, syncing: Syncing) -> Result<Option<OwnershipChange>> {
    let synced = sync(state, ownership.held_projects.as_deref(), syncing).await?;
    tracing::debug!(
        target: "weft_infra_supervisor::ownership",
        replica = %state.replica,
        owned = synced.owned.len(),
        claimed = synced.claimed.len(),
        next_due_unix = ?synced.next_due_unix,
        "ownership synced (renewed + claimed a batch)"
    );
    let now: HashSet<Uuid> = synced.owned.iter().map(|p| p.project_id).collect();
    let mut lost: Vec<Uuid> = ownership.owned.difference(&now).copied().collect();
    lost.sort_unstable();
    ownership.owned = now;
    let change = OwnershipChange { claimed: synced.claimed, lost };
    Ok((!change.is_empty()).then_some(change))
}

/// End a pass of a supervisor that scales to zero: report what the pass
/// looked at, settled and could not get through, and give every lease
/// back, so a machine's next word about a project is looked at by
/// whichever process it wakes. `held_projects` is the host's listing the
/// pass started from ([`Ownership`]), `None` when it failed.
pub async fn end(state: &SupervisorState, held_projects: Option<&[Uuid]>) -> Result<crate::Pass> {
    let synced = sync(state, held_projects, Syncing::End).await?;
    Ok(crate::Pass { settled: synced.settled })
}

impl Ownership {
    /// The projects the host held at the latest listing that went
    /// through, `None` while none did.
    pub fn held_projects(&self) -> Option<&[Uuid]> {
        self.held_projects.as_deref()
    }

    /// This process deleted every copy the listing held of `emptied`
    /// ([`sweep_gone_copies`]): the host holds none of them any more.
    pub fn emptied(&mut self, emptied: &[Uuid]) {
        if let Some(held) = &mut self.held_projects {
            held.retain(|p| !emptied.contains(p));
        }
    }
}

/// Which of a supervisor's syncs this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Syncing {
    /// A tick that lists the host and claims: it reports what the looks
    /// came to.
    Claim,
    /// A renewal in the middle of a pass: it reports nothing, and the
    /// pass's end reports what the pass met.
    MidPass,
    /// A pass ending: it reports, and gives every lease back.
    End,
}

/// What one sync reported settled, and what the broker answered.
struct Synced {
    settled: Vec<Uuid>,
    owned: Vec<weft_broker_client::protocol::SupervisorProject>,
    claimed: Vec<Uuid>,
    next_due_unix: Option<i64>,
}

/// One `sync_ownership`, carrying what this process's looks came to since
/// the last one (handed back if the sync fails, so the next one reports
/// it).
async fn sync(state: &SupervisorState, held_projects: Option<&[Uuid]>, syncing: Syncing) -> Result<Synced> {
    let report = match syncing {
        Syncing::MidPass => crate::health::Report::default(),
        Syncing::Claim | Syncing::End => state.health.lock().await.take_report(),
    };
    let ending = syncing == Syncing::End;
    let req = weft_broker_client::protocol::SupervisorSyncOwnershipRequest {
        replica: state.replica.clone(),
        held_projects: held_projects.map(<[Uuid]>::to_vec),
        looked: report.looked.clone(),
        settled: report.settled.clone(),
        failed: report.failed(),
        gave_up: report.gave_up(),
        all_failed: report.all_failed(),
        ending,
    };
    match crate::patiently(state, || state.broker.sync_ownership(&req)).await {
        Ok(resp) => Ok(Synced { settled: report.settled, owned: resp.owned, claimed: resp.claimed, next_due_unix: resp.next_due_unix }),
        Err(e) => {
            state.health.lock().await.report_again(report);
            Err(e)
        }
    }
}

/// Delete everything the host holds (`copies`, its listing) for a copy
/// that is gone for good.
///
/// A terminate keeps the disks a node listed in `keepOnTerminate`, so
/// the next start of the same copy adopts them, and its row goes. That
/// is right while the copy can come back, and a leak once it cannot:
/// its project was removed (with `--force` not even waiting for the
/// terminate), or the program no longer declares it (the node was
/// removed, or changed sides between shared and per instance), whether
/// the orphan reap terminated it or it was already down with no row
/// left to reap. The host is the only place such a copy still shows,
/// so the sweep starts from the host's listing, and the broker judges
/// which of those copies are gone. A copy still holding a row is never
/// gone here: a user terminate or the reap owns it until the row goes.
///
/// A copy's id is derived from (project, node, instance), so a node added
/// back names the very copy a judgment called gone, and an apply
/// adopting its kept disks must never overlap its deletion. Two guards,
/// both held from the judgment through the deletion:
/// - The project's `infra_owner` lease. The broker judges an existing
///   project only for its owner, the one supervisor that can be applying
///   it (the tick claimed the lease of every project the host holds, so
///   an unleased one is judged by whoever took it). Each gone copy is
///   judged again right before its deletion, the same fence a lifecycle
///   write takes right before its host call, so a lease that moved in
///   between aborts the rest of the project's deletions. What remains is
///   the window every fenced host call has: between that last check and
///   the host call, which a new owner can only enter after this
///   supervisor stopped renewing for a whole lease.
/// - The project's lock (`ProjectLocks`), taken alone, which every
///   lifecycle command of this process shares, so this supervisor's own
///   apply of the project cannot interleave. A project a command holds is
///   skipped this tick rather than waited on.
///
/// A removed project (no row) is judged for anyone: nothing can apply it
/// until it is registered again, which the per-copy check right before
/// each deletion also catches, down to the same window.
///
/// One project or copy failing does not stop the rest; a project whose
/// sweep failed is reported failed (`crate::failed`), and a later pass
/// sweeps it again. A removed project has no lease to fail under: the
/// broker counts its failure as the install's (`Callee::GoneCopies`),
/// clears it once a listing holds none of the project, and books its
/// retry like a project's. Idempotent: a copy another supervisor deleted
/// first is simply gone from the next listing.
///
/// Hands back the projects whose every listed copy it deleted: the host
/// holds none of them any more, which the pass's last sync says
/// ([`Ownership::emptied`]).
pub async fn sweep_gone_copies(state: &SupervisorState, copies: Vec<NodeRef>) -> Vec<Uuid> {
    let mut by_project: BTreeMap<Uuid, Vec<NodeRef>> = BTreeMap::new();
    for copy in copies {
        by_project.entry(copy.project).or_default().push(copy);
    }
    let mut emptied = Vec::new();
    for (project, copies) in by_project {
        let Some(_project) = state.project_locks.try_alone(project) else {
            tracing::debug!(target: "weft_infra_supervisor::ownership", project_id = %project, "a lifecycle command holds the project; its gone copies wait for the next tick");
            continue;
        };
        match sweep_project(state, project, &copies).await {
            Ok(true) => emptied.push(project),
            Ok(false) => {}
            Err(e) => {
                crate::failed(state, project, crate::health::Step::Sweep, &format!("could not delete what the host still holds for its gone copies: {e:#}"))
                    .await;
            }
        }
    }
    emptied
}

/// One project's sweep, under its lock: judge its copies, then delete
/// each gone one after judging it again (the lease fence). Every gone
/// copy is tried; the ones that could not be deleted fail it. True when
/// it deleted every copy in `copies`.
async fn sweep_project(state: &SupervisorState, project: Uuid, copies: &[NodeRef]) -> Result<bool> {
    let Some(gone) = crate::patiently(state, || state.broker.gone_copies(&state.replica, project, copies)).await? else {
        tracing::debug!(target: "weft_infra_supervisor::ownership", project_id = %project, "another supervisor holds the project's lease; its copies are its to sweep");
        return Ok(false);
    };
    let mut failed = Vec::new();
    let mut deleted = 0;
    for copy in copies.iter().filter(|c| gone.contains(&c.copy_id)) {
        match crate::patiently(state, || state.broker.gone_copies(&state.replica, project, std::slice::from_ref(copy))).await? {
            None => {
                tracing::info!(target: "weft_infra_supervisor::ownership", project_id = %project, "the project's lease moved mid-sweep; leaving its copies to the new owner");
                break;
            }
            Some(still) if still.is_empty() => continue,
            Some(_) => {}
        }
        tracing::info!(
            target: "weft_infra_supervisor::ownership",
            project_id = %copy.project,
            node = %copy.node,
            copy_id = %copy.copy_id,
            "the copy is gone for good; deleting what the host still holds for it, kept disks included"
        );
        match state.host.terminate(copy, &[]).await {
            Ok(()) => deleted += 1,
            Err(e) => failed.push(format!("{}: {e:#}", copy.copy_id)),
        }
    }
    match failed.is_empty() {
        true => Ok(deleted == copies.len()),
        false => Err(anyhow!("{}", failed.join("; "))),
    }
}
