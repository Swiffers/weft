//! Who the broker says sent a call it could not read, and how it answers.
//!
//! Every handler reads its call with `weft_core::wire_shape::Json` (or
//! `Query`), which answers a call that does not read only once the
//! broker knows its caller (a stranger gets the identity refusal), and a
//! body of another shape through the broker's [`AnswersUnreadable`]:
//! [`UNREADABLE_SHAPE`], with a body saying the
//! project needs `weft resync`, which a worker's client turns into one
//! error (`weft_broker_client::WorkerOutdated`). The broker also says it
//! in its own log, naming who sent it, once every while for each sender
//! ([`UnreadableShapes`]): a worker of the previous weft does not know the
//! answer, so for it the broker's log is where a person sees what to do.
//!
//! `axum::Json` and `axum::extract::Query` are refused in this crate
//! (`clippy.toml`), so a new handler cannot read a call any other way.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use weft_core::wire_shape::{unreadable_shape, AnswersUnreadable, OnceAWhile, UNREADABLE_SHAPE};
use weft_platform_traits::identity::{Principal, ROLE_HEADER};
use weft_platform_traits::CoreRole;

use crate::auth::{CallerIdentity, CallerScope, Role};
use crate::state::BrokerState;

impl AnswersUnreadable for BrokerState {
    type Caller = Sender;

    /// A caller the broker does not know gets its identity refusal, and
    /// learns nothing of the endpoint or its shapes.
    async fn caller(&self, headers: &HeaderMap) -> Result<Sender, Response> {
        match crate::auth::verified_principal(self, headers).await {
            Ok(principal) => Ok(Sender::of_principal(&principal, headers)),
            Err(refusal) => Err(refusal.into_response()),
        }
    }

    fn other_shape(&self, sender: Sender, path: &str, why: &str) -> Response {
        self.unreadable_shapes.refuse(sender, path, &why).into_response()
    }
}

/// Who sent a call the broker could not read, as its log names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sender {
    /// One project's worker: an older weft built its program.
    Worker(uuid::Uuid),
    /// The agent beside one of a project's infra copies.
    InfraCopy(uuid::Uuid),
    /// One of weft's own roles (`None` when it named none).
    Role(Option<CoreRole>),
}

impl Sender {
    pub fn of_principal(principal: &Principal, headers: &HeaderMap) -> Self {
        match principal {
            Principal::Worker { project, .. } => Sender::Worker(*project),
            Principal::InfraCopy { project, .. } => Sender::InfraCopy(*project),
            Principal::Core => Sender::Role(
                headers.get(ROLE_HEADER).and_then(|v| v.to_str().ok()).and_then(|raw| CoreRole::parse(raw).ok()),
            ),
        }
    }

    pub fn of_caller(caller: &CallerIdentity) -> Self {
        match (&caller.scope, caller.role) {
            (CallerScope::Tenant { project, .. }, _) => Sender::Worker(*project),
            (CallerScope::ControlPlane, Role::Listener) => Sender::Role(Some(CoreRole::Listener)),
            (CallerScope::ControlPlane, Role::InfraSupervisor) => Sender::Role(Some(CoreRole::Supervisor)),
            (CallerScope::ControlPlane, Role::Worker) => Sender::Role(None),
        }
    }
}

/// When the broker's log last said that each sender's calls do not read.
#[derive(Default)]
pub struct UnreadableShapes {
    said: OnceAWhile<Sender>,
}

impl UnreadableShapes {
    /// The answer to a call from `sender` to `path` whose JSON did not
    /// read as the shape it takes (`why`), said to the log unless it was
    /// said for `sender` a while ago ([`OnceAWhile`]). Also what a handler
    /// answers for a part of a body it reads itself (a task's payload).
    pub fn refuse(&self, sender: Sender, path: &str, why: &dyn std::fmt::Display) -> (StatusCode, String) {
        if self.said.now(sender) {
            match sender {
                Sender::Worker(project) => tracing::error!(
                    target: "weft_broker::wire", %project, path, error = %why,
                    "a worker of project {project} sent a call this broker cannot read: an older weft built it. \
                     Run `weft resync` on that project to rebuild it"
                ),
                Sender::InfraCopy(project) => tracing::error!(
                    target: "weft_broker::wire", %project, path, error = %why,
                    "the agent beside an infra copy of project {project} sent a call this broker cannot read: an older weft built it"
                ),
                Sender::Role(role) => tracing::error!(
                    target: "weft_broker::wire", role = role.map_or("unnamed", CoreRole::as_str), path, error = %why,
                    "one of weft's own roles sent a call this broker cannot read"
                ),
            }
        }
        (UNREADABLE_SHAPE, unreadable_shape(&format!("{path}: {why}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_error() -> serde_json::Error {
        serde_json::from_str::<Vec<(String, String)>>(r#"{"x-api-key":"k"}"#).unwrap_err()
    }

    #[test]
    fn a_call_of_another_shape_is_answered_the_one_way_and_names_the_fix() {
        let shapes = UnreadableShapes::default();
        let (status, body) = shapes.refuse(Sender::Worker(uuid::Uuid::nil()), "/v1/caller/verify", &data_error());
        assert_eq!(status, StatusCode::UPGRADE_REQUIRED);
        assert!(body.contains("`weft resync`"), "{body}");
        assert!(body.contains("/v1/caller/verify: invalid type: map, expected a sequence"), "{body}");
    }
}
