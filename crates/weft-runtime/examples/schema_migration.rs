//! Write the migration for whatever you just changed in a `CREATE TABLE`,
//! in any group the install's database holds. Driven by
//! `./setup.sh --migration <name> [--release]`, which provides the
//! throwaway Postgres this builds schemas in.
//!
//! What the generator does, what a draft is, what releasing does, and
//! how a release finds the database you have been working against are
//! documented on `weft_task_store::schema_guard::write_migration`.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    weft_task_store::schema_guard::write_migration(&weft_runtime::schema::groups(), &weft_runtime::schema::documents())
        .await
}
