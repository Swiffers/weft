//! `cancel_call` task: a run was cancelled while it waited on an outside
//! job, and the wait carried the service's own cancel address
//! (`weft_core::signal::cancel_call`). The cancel queued this in the
//! transaction that took the wait down; here the listener sends it, once,
//! as the connection the wait polled with.
//!
//! A call the service refuses, or that cannot reach it, fails the task
//! with why, logged as an error: the job may still be running and
//! charging, and nothing retries a call to an outside service on its own.

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use weft_core::signal::listener_protocol::CancelCallRequest;
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

pub struct CancelCallExecutor;

#[async_trait]
impl TaskExecutor<DispatcherState> for CancelCallExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let call: CancelCallRequest = serde_json::from_value(task.payload.clone())?;
        let Some(on_cancel) = call.spec.on_cancel.clone() else {
            anyhow::bail!("the last call queued for wait {} carries no call", call.token);
        };
        if let Err(e) = state.listener.cancel_call(&call).await {
            tracing::error!(
                target: "weft_dispatcher::cancel_call",
                run_id = ?task.run_id, token = %call.token, method = on_cancel.method.as_http(), url = %on_cancel.url,
                error = %format!("{e:#}"),
                "a cancelled run's last call to its outside service failed; the job it waited on may still be running and charging"
            );
            return Err(e);
        }
        tracing::info!(
            target: "weft_dispatcher::cancel_call",
            run_id = ?task.run_id, token = %call.token, method = on_cancel.method.as_http(), url = %on_cancel.url,
            "sent a cancelled run's last call to its outside service"
        );
        Ok(serde_json::json!({ "sent": true }))
    }
}
