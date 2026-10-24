//! The tests that need a real Postgres. They are all in this one binary
//! so `.config/nextest.toml` can tell them apart by its name: the default
//! profile leaves out every binary called `db`, the `db` profile runs only
//! them (`scripts/run-db-tests.sh` runs it against a throwaway Postgres).
//! Each module was once a test file of its own; one binary links the
//! dependency tree once instead of once per file.

mod db_build;
mod db_entry_limits;
mod db_install_state;
mod db_lifecycle;
mod db_maintenance;
mod db_project_ports;
mod db_proxy;
mod db_tags;
mod db_tenant_isolation;
mod db_versions;
