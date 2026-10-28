//! `removal_wait` task: the wait of `weft rm`, apart from the request that
//! took the project down (`crate::api::project::quiesce`).
//!
//! The project is down and marked being removed when the quiesce answers;
//! what is left is what it cancelled being over: the last calls its
//! cancelled runs queued gone out (what `weft rm` deletes next includes
//! what a call needs), and, before `weft rm --journal` erases the
//! project's history, every run ended. However long that takes
//! (`crate::drain`): the person follows it in short looks (`GET
//! /projects/{id}/follow/{task}`), Ctrl+C stops following, and the wait
//! goes on. A process that dies mid-wait hands it to another, which reads
//! again from the rows.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

/// The task that waits for what removing project `project_id` cancelled.
pub fn task(tenant: &str, project_id: uuid::Uuid, payload: &weft_task_store::RemovalWaitPayload) -> weft_task_store::tasks::NewTask {
    weft_task_store::tasks::NewTask {
        kind: weft_task_store::TaskKind::RemovalWait,
        project_id: Some(project_id),
        dedup_key: None,
        run_id: None,
        tenant_id: tenant.to_string(),
        payload: serde_json::to_value(payload).expect("a removal wait payload serializes"),
    }
}

pub struct RemovalWaitExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for RemovalWaitExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let id = task.project_id.ok_or_else(|| anyhow::anyhow!("a removal wait names no project"))?;
        let weft_task_store::RemovalWaitPayload { every_run } = serde_json::from_value(task.payload.clone())?;
        // The runs first: one still going may queue a last call of its own
        // as it lets go.
        if every_run {
            let scope = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::EveryRun };
            crate::drain::drain(state, &scope, None, None).await?;
        }
        crate::api::project::last_calls_sent(state, id).await?;
        Ok(Value::Null)
    }
}
