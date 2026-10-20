//! The channel infra lifecycle commands are announced on, and the one
//! notice on it that no command row sends: a request that the supervisor
//! look at a project's infra.
//!
//! The commands' own notices come from the `infra_command_notify` trigger
//! in the dispatcher's `infra_lifecycle_command::GROUP`, once when a
//! command is issued and once when it completes; a health take-down or
//! bring-back that completes also asks for a look at its project from
//! there. Otherwise a look is asked for by whoever learns that a
//! project's infra may have changed how it stands (a machine running it,
//! through the broker; a person, through the dispatcher), on its own
//! connection. Readers parse every payload with
//! `weft_broker_client::lifecycle_command::InfraCommandSignal`.

/// The channel an `infra_lifecycle_command` row notifies on (see the
/// module docs), and where a look is asked for (`look:<project_id>`),
/// which wakes a supervisor that scales to zero to look at it.
// SYNC: 'weft_infra_command' <-> the `infra_command_notify` and
//       `infra_command_cancel_notify` functions in
//       crates/weft-dispatcher/src/infra_lifecycle_command.rs (GROUP)
pub const INFRA_COMMAND_CHANNEL: &str = "weft_infra_command";

/// The [`INFRA_COMMAND_CHANNEL`] payload asking for a look at `project`.
// SYNC: the 'look:' prefix <-> weft_broker_client::lifecycle_command::InfraCommandSignal::parse,
//       the `infra_command_notify` function in crates/weft-dispatcher/src/infra_lifecycle_command.rs (GROUP)
pub fn look_payload(project: uuid::Uuid) -> String {
    format!("look:{project}")
}

/// Ask the supervisor to look at `project`'s infra: announce it, which
/// wakes a supervisor that scales to zero. Inside a transaction, the
/// notice goes out as it commits.
pub async fn ask_for_look<'e>(executor: impl sqlx::PgExecutor<'e>, project: uuid::Uuid) -> sqlx::Result<()> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(INFRA_COMMAND_CHANNEL)
        .bind(look_payload(project))
        .execute(executor)
        .await?;
    Ok(())
}
