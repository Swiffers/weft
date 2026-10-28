//! `maintenance_lift` task: `weft maintenance off`, apart from the request
//! that asked for it (`crate::maintenance`).
//!
//! Starting infra and bringing triggers back can take as long as their
//! setup runs do, so the lift is a task the person follows in short looks
//! (`GET /install/maintenance/follow/{task}`), and nothing about it depends
//! on the request staying open. What it brings back is what the
//! maintenance tables hold, each record and mark going as what it names
//! comes back, so a task run again (its process died, the claim lapsed)
//! carries on from what is left.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

/// The lift of `tenant`'s install, one at a time
/// (`weft_task_store::kinds::MAINTENANCE_LIFT_KEY`).
pub fn task(tenant: &str) -> weft_task_store::tasks::NewTask {
    weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::MaintenanceLift,
        project_id: None,
        dedup_key: Some(weft_task_store::kinds::MAINTENANCE_LIFT_KEY.to_string()),
        run_id: None,
        tenant_id: tenant.to_string(),
        payload: serde_json::json!({}),
    }
}

pub struct MaintenanceLiftExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for MaintenanceLiftExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let report = crate::maintenance::lift(state, &task.tenant_id, task.id).await?;
        Ok(serde_json::to_value(report)?)
    }
}
