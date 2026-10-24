//! The install's side of a local machine's folder sync
//! (`weft_assets::sync::FolderCopies`): which folder working copies there
//! are, how one pass reaches its working copy and its tenant's stored
//! files, and the notices that say a working copy moved. The service that
//! watches the folders is the local platform's
//! (`weft_platform_local::folders`); it lives in the install's own
//! process, so every call here is a direct call into the actions, never
//! HTTP.
//!
//! A pass acts as an operator of the project's own tenant: the folder
//! belongs to the person who registered it, and the sync is their edit.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use weft_assets::sync::{BlobFetch, FolderCopies, FolderCopy, Notice, Notices, Reach, WorkingCopyPort};
use weft_core::storage::key::ParsedKey;
use weft_core::working_copy::{Applied, SyncReport, WorkingCopyId, WorkingCopyRef, WorkingState};
use weft_task_store::pg_signal::{Heard, Subscription};

use crate::actions::{self, Actor, Refused};
use crate::state::DispatcherState;
use crate::tenant::TenantId;
use crate::working_copies::WORKING_COPY_CHANNEL;

/// Every folder working copy on this install, over its state.
pub struct InstallFolders {
    state: DispatcherState,
}

impl InstallFolders {
    pub fn new(state: DispatcherState) -> Self {
        Self { state }
    }
}

fn refused((status, message): Refused) -> anyhow::Error {
    anyhow::anyhow!("{message} ({status})")
}

#[async_trait::async_trait]
impl FolderCopies for InstallFolders {
    fn notices(&self) -> Result<Box<dyn Notices>> {
        self.state.signals.require(WORKING_COPY_CHANNEL)?;
        Ok(Box::new(InstallNotices(self.state.signals.subscribe())))
    }

    /// The folder working copies on this machine: a folder registered
    /// under another machine's name is a folder this process cannot see,
    /// whatever its path says here.
    async fn folders(&self) -> Result<Vec<FolderCopy>> {
        let here = weft_core::working_copy::this_machine().map_err(anyhow::Error::msg)?;
        let rows = self.state.working_copies.all_folders().await.context("list the folder working copies")?;
        rows.into_iter()
            .filter(|row| row.host.as_deref() == Some(here.as_str()))
            .map(|row| {
                let path = row.path.with_context(|| format!("folder working copy {} has no path", row.id))?;
                Ok(FolderCopy { id: row.id, root: PathBuf::from(path) })
            })
            .collect()
    }

    async fn reach(&self, copy: &FolderCopy) -> Result<Reach<'_>> {
        let state = &self.state;
        let row = state.working_copies.get(copy.id).await?.with_context(|| format!("no working copy {}", copy.id))?;
        let project = row.project_id;
        let tenant = state.projects.tenant_for(project).await?.with_context(|| format!("no project {project}"))?;
        Ok(Reach {
            port: Box::new(InstallPort { state, actor: Actor::operator(TenantId(tenant.clone())), project, copy: copy.id }),
            fetch: Box::new(InstallBlobs { state, tenant: tenant.clone() }),
            store: Box::new(crate::storage::asset_store(state, &tenant)),
        })
    }
}

/// One folder's working copy, through the actions a remote syncer's
/// HTTP calls reach.
struct InstallPort<'a> {
    state: &'a DispatcherState,
    actor: Actor,
    project: uuid::Uuid,
    copy: WorkingCopyId,
}

#[async_trait::async_trait]
impl WorkingCopyPort for InstallPort<'_> {
    async fn state(&self) -> Result<WorkingState> {
        actions::versions::working_state(self.state, &self.actor, self.project, WorkingCopyRef { working_copy: Some(self.copy) })
            .await
            .map_err(refused)
    }

    async fn sync(&self, report: &SyncReport) -> Result<Applied> {
        actions::versions::sync(self.state, &self.actor, self.project, self.copy, report).await.map_err(refused)
    }
}

/// The tenant's stored files, streamed to disk.
struct InstallBlobs<'a> {
    state: &'a DispatcherState,
    tenant: String,
}

#[async_trait::async_trait]
impl BlobFetch for InstallBlobs<'_> {
    async fn fetch(&self, hash: &str, dest: &Path) -> Result<()> {
        let key = ParsedKey::asset(&self.tenant, hash).map_err(anyhow::Error::msg)?.to_key();
        // `download_file` makes its file itself, so the empty one the
        // syncer made for it goes first.
        std::fs::remove_file(dest).with_context(|| format!("make room for {}", dest.display()))?;
        crate::storage::download_file(self.state, &key, dest).await
    }
}

/// What the process's signal watch hears, read as working copy notices.
struct InstallNotices(Subscription);

#[async_trait::async_trait]
impl Notices for InstallNotices {
    async fn next(&mut self) -> Result<Notice> {
        loop {
            match self.0.next().await? {
                Heard::Signal { channel, payload } if channel == WORKING_COPY_CHANNEL => return notice(&payload),
                Heard::Signal { .. } | Heard::Lost => continue,
                Heard::Recheck | Heard::Resumed => return Ok(Notice::Recheck),
            }
        }
    }
}

/// A `WORKING_COPY_CHANNEL` payload, `<project>/<working copy>` or
/// `<project>/new`, as a notice.
fn notice(payload: &str) -> Result<Notice> {
    let (_, copy) = payload.split_once('/').with_context(|| format!("working copy notice '{payload}' is not <project>/<copy>"))?;
    if copy == "new" {
        return Ok(Notice::Added);
    }
    let id = copy.parse().with_context(|| format!("working copy notice '{payload}' names no working copy"))?;
    Ok(Notice::Changed(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_payload_reads_as_the_notice_it_is() {
        let project = uuid::Uuid::now_v7();
        let copy = uuid::Uuid::now_v7();
        assert_eq!(notice(&format!("{project}/{copy}")).unwrap(), Notice::Changed(copy));
        assert_eq!(notice(&format!("{project}/new")).unwrap(), Notice::Added);
        assert!(notice("garbage").is_err());
        assert!(notice(&format!("{project}/nope")).is_err());
    }
}
