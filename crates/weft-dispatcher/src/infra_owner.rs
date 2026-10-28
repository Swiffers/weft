//! The exclusive ownership of a project's infrastructure.
//!
//! `infra_owner(project_id PK, supervisor_replica, tenant_id,
//! leased_until_unix)`: exactly one supervisor replica applies a project's
//! infra at a time, so two never change the same project's infrastructure
//! at once (which would corrupt it). A supervisor claims and renews these
//! leases on its ownership tick (the broker's `sync_ownership`); a dead
//! one's leases expire and a live one adopts them.

use anyhow::Result;

pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "infra_owner",
    tables: &["infra_owner"],
    ddl: &[
        // The EXCLUSIVE ownership lease. One row per project whose infra is
        // currently owned by a supervisor, carrying the tenant the apply
        // path needs.
        r#"CREATE TABLE IF NOT EXISTS infra_owner (
            project_id          UUID PRIMARY KEY,
            supervisor_replica TEXT NOT NULL,
            tenant_id           TEXT NOT NULL,
            leased_until_unix   BIGINT NOT NULL
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_infra_owner_replica
             ON infra_owner(supervisor_replica)"#,
    ],
    seed: &[],
    data_steps: &[],
};

/// Release a project's exclusive supervisor lease (its `infra_owner`
/// row), in the transaction that removes the project
/// (`crate::project_store::ProjectStoreOps::remove`), after its row: the
/// owning supervisor stops reconciling the project the moment the row is
/// gone (its loop only acts on projects it owns), and a lease claimed or
/// renewed reads the project row it is for (`weft_broker::lifecycle_writes`),
/// so none can come back once the removal committed.
pub async fn release_project(db: &impl weft_task_store::db::Client, project_id: uuid::Uuid) -> Result<u64> {
    Ok(db.execute("DELETE FROM infra_owner WHERE project_id = $1", &[&project_id]).await?)
}
