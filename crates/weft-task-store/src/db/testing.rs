//! What `#[weft_db_macros::test]` runs a database test through: a
//! database of its own on the server `$DATABASE_URL` names, made before
//! the test and dropped after it, passed or not.

use std::future::Future;
use std::time::Duration;

use futures::FutureExt as _;

use super::{Client as _, Pool, Session, Waits};

/// The most connections a test's pool holds at once, and how long a
/// statement waits for one.
const TEST_POOL: u32 = 10;
const TEST_POOL_WAIT: Duration = Duration::from_secs(30);

/// Run the test `name`, handing `body` a pool on a fresh database.
pub fn run<F, Fut>(name: &str, body: F)
where
    F: FnOnce(Pool) -> Fut,
    Fut: Future<Output = ()>,
{
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a test runtime");
    runtime.block_on(async {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| panic!("{name} needs a Postgres: run it through scripts/run-db-tests.sh, which sets DATABASE_URL"));
        let server = super::config(&url).expect("DATABASE_URL is a Postgres address");
        let database = format!("weft_test_{}", uuid::Uuid::new_v4().simple());
        let admin = Session::connect(&server).await.expect("reach the test server");
        admin.batch_execute(&format!("CREATE DATABASE {database}")).await.expect("make the test's database");
        let mut config = server.clone();
        config.dbname(&database);
        let pool = Pool::open(config, TEST_POOL, TEST_POOL_WAIT, Waits::Expected);
        let outcome = std::panic::AssertUnwindSafe(body(pool.clone())).catch_unwind().await;
        pool.close();
        drop(pool);
        // FORCE: a connection the test left open (a task it spawned, a
        // pool it opened) is ended rather than keeping the database.
        admin.batch_execute(&format!("DROP DATABASE {database} WITH (FORCE)")).await.expect("drop the test's database");
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    });
}
