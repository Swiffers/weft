//! `take_down_wait` task: the wait of a person's deactivate under
//! `running: wait`, apart from the request that asked for it.
//!
//! The triggers are down when the deactivate answers; what is left is
//! the runs they fired finishing on their own version, however long that
//! takes (`crate::drain`). Carried as a task so no request has to stay
//! open for it (a cloud cuts a request at an hour): the person follows it
//! in short looks (`GET /projects/{id}/follow/{task}`), Ctrl+C stops
//! following, and the wait itself ends with the last run. A process that
//! dies mid-wait hands it to another, which counts again from the rows.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

/// The task that waits for the runs the payload's triggers fired,
/// counting its tokens at the workers' door.
pub fn task(tenant: &str, project_id: uuid::Uuid, payload: &weft_task_store::TakeDownWaitPayload) -> weft_task_store::tasks::NewTask {
    weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::TakeDownWait,
        project_id: Some(project_id),
        dedup_key: None,
        run_id: None,
        tenant_id: tenant.to_string(),
        payload: serde_json::to_value(payload).expect("a take-down wait payload serializes"),
    }
}

pub struct TakeDownWaitExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for TakeDownWaitExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let id = task.project_id.ok_or_else(|| anyhow::anyhow!("a take-down wait names no project"))?;
        let weft_task_store::TakeDownWaitPayload { triggers, tokens } = serde_json::from_value(task.payload.clone())?;
        let keys: Vec<weft_core::activation::ActivationKey> = triggers
            .iter()
            .flat_map(|owned| owned.triggers.iter().map(|trigger| weft_core::activation::ActivationKey::new(trigger.clone(), owned.owner.clone())))
            .collect();
        let scope = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Fired { keys: &keys, tokens: &tokens } };
        crate::drain::drain(state, &scope, None, None).await?;
        Ok(Value::Null)
    }
}
