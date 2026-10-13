//! The install's database schema: every group of tables it holds, in one
//! list. The boot applies it in one guarded pass, the migration generator
//! (`examples/schema_migration.rs`) writes migrations from it, and the
//! agreement test (`tests/schema_agreement.rs`) builds from it, so none
//! of the three can leave a group out.

use anyhow::Context as _;
use weft_task_store::SchemaGroup;

/// The parts of the list, in dependency order: each role's groups, as
/// that role's crate orders them. The runtime-file plane names no table
/// outside its own group (its two `REFERENCES runtime_file(key)` stay
/// inside it), so it can sit anywhere; it goes last.
static PARTS: &[&[&SchemaGroup]] = &[
    weft_dispatcher::app::DISPATCHER_GROUPS,
    &[&weft_broker::runtime_store::GROUP],
];

/// Every schema group the install's database holds, in dependency order.
pub fn groups() -> Vec<&'static SchemaGroup> {
    PARTS.iter().flat_map(|part| part.iter().copied()).collect()
}

/// Apply every group in one guarded pass, so every pending migration
/// across every group runs in one global id order (the order the
/// agreement test replays), and a mismatch anywhere surfaces in ONE
/// error naming every stale group together. Serialization across racing
/// boots is the guard's own job: `apply_groups` runs everything in one
/// transaction under its advisory lock. Runs BEFORE any role's state is
/// built; no role applies schema of its own.
pub async fn apply(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    weft_task_store::apply_groups(pool, &groups()).await.context("apply the schema")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two groups of one name would share one stamp and one migration
    /// folder, and a table in two groups would be built twice.
    #[test]
    fn every_group_and_every_table_is_listed_once() {
        let groups = groups();
        let mut names: Vec<&str> = groups.iter().map(|g| g.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(names.len(), before, "a group is listed twice");
        let mut tables: Vec<&str> = groups.iter().flat_map(|g| g.tables.iter().copied()).collect();
        tables.sort_unstable();
        let before = tables.len();
        tables.dedup();
        assert_eq!(tables.len(), before, "a table belongs to two groups");
        assert!(names.contains(&"runtime_file"), "the broker's runtime-file plane is in the list");
    }
}
