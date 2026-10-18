//! `weft-runtime carry`: bring a local install's database from the
//! previous generation (`serverless`) into this one (`run`), before the
//! runtime starts on it. `weft daemon start` runs it every time, with the
//! runtime stopped; it does nothing on a database already of this
//! generation.
//!
//! A generation starts when a release changes names inside what weft has
//! stored in ways no SQL migration can reach (here: the event names inside
//! compressed run records, the scope tag of run-scoped files). So the old
//! database is never changed in place. It is set aside, a fresh one is
//! built from today's tables, everything is copied across and rewritten
//! on the way ([`rewrite`]), every row the runtime will read is read back
//! with today's types ([`verify`]), and only then does the fresh database
//! take the install's database name.
//!
//! Every step can be interrupted (Ctrl-C, a crash) and the next run
//! finishes the job, because each one leaves a state this command knows
//! from the databases alone:
//!
//! | `weft` | `weft_gen_serverless` | what is left to do |
//! |---|---|---|
//! | previous generation | absent | set it aside (one rename) |
//! | absent | present | build `weft_carrying` again from scratch, copy, verify, rename it to `weft` |
//! | this generation | present | delete the old file copies, drop the old database |
//! | this generation | absent | nothing |
//!
//! A database rename is one step in Postgres, the old database is only
//! ever read, the copy goes into a database nothing else uses, and file
//! objects are copied before the old ones are deleted, so no instant of it
//! leaves anything lost.

pub mod copy;
pub mod rewrite;
pub mod verify;

use std::str::FromStr as _;

use anyhow::Context as _;
use sqlx::postgres::{PgConnectOptions, PgConnection};
use sqlx::Connection as _;
use weft_task_store::schema_guard::{generation, Generation, PREVIOUS_GENERATION};

/// The database a carry is built in, beside the install's: the install's
/// name with this suffix. Nothing but the carry ever opens it.
const CARRYING: &str = "_carrying";

/// Where the install's databases live, and which object store holds its
/// files.
pub struct Install {
    /// The install's database, as `WEFT_DATABASE_URL` names it.
    pub live: PgConnectOptions,
    pub objects: weft_platform_traits::SharedObjectStore,
}

impl Install {
    fn name(&self) -> String {
        self.live.get_database().unwrap_or("postgres").to_string()
    }

    fn old_name(&self) -> String {
        format!("{}_gen_{PREVIOUS_GENERATION}", self.name())
    }

    fn carrying_name(&self) -> String {
        format!("{}{CARRYING}", self.name())
    }

    fn options_for(&self, database: &str) -> PgConnectOptions {
        self.live.clone().database(database)
    }

    async fn server(&self) -> anyhow::Result<PgConnection> {
        PgConnection::connect_with(&self.options_for("postgres")).await.context("connect to the database server")
    }
}

/// What a carry did, for the line `weft daemon start` prints.
#[derive(Debug, PartialEq, Eq)]
pub enum Done {
    /// The database was already of this generation.
    Nothing,
    /// The database was carried into this generation.
    Carried { runs: u64, files: u64 },
}

/// Bring `install`'s database into this generation, finishing whatever an
/// earlier carry left half done.
pub async fn carry(install: &Install) -> anyhow::Result<Done> {
    let mut done = Done::Nothing;
    loop {
        let mut server = install.server().await?;
        let live = exists(&mut server, &install.name()).await?;
        let old = exists(&mut server, &install.old_name()).await?;
        if live {
            let mut conn = PgConnection::connect_with(&install.live).await.context("connect to the install's database")?;
            let generation = generation(&mut conn).await?;
            conn.close().await?;
            match generation {
                Generation::Previous => {
                    anyhow::ensure!(
                        !old,
                        "the install's database '{}' is from the previous generation, and so is '{}' beside it: \
                         two databases to carry, and only one can become the install's. Keep the one you want \
                         as '{}', drop the other, and start again",
                        install.name(),
                        install.old_name(),
                        install.name()
                    );
                    println!("carrying the database into this generation: setting the old one aside as '{}'", install.old_name());
                    rename(&mut server, &install.name(), &install.old_name()).await?;
                    continue;
                }
                Generation::Older => anyhow::bail!(
                    "the install's database is from before weft moved off Kubernetes and cannot be carried \
                     forward; wipe it with scripts/scrub-old-install.sh, then run ./setup.sh"
                ),
                Generation::Current | Generation::Empty => {
                    if old {
                        finish(install, &mut server).await?;
                    }
                    drop_if_exists(&mut server, &install.carrying_name()).await?;
                    return Ok(done);
                }
            }
        }
        anyhow::ensure!(
            old,
            "the install's database '{}' does not exist, and no earlier database is set aside to carry into it",
            install.name()
        );
        done = build(install, &mut server).await?;
        rename(&mut server, &install.carrying_name(), &install.name()).await?;
        println!("the carried database is the install's now; cleaning up the old one");
    }
}

/// Build the carrying database from the old one: today's tables, every
/// row copied and rewritten, every row read back, every run-scoped file
/// copied to its new key. Starts from scratch whatever an interrupted run
/// left of it.
async fn build(install: &Install, server: &mut PgConnection) -> anyhow::Result<Done> {
    let carrying = install.carrying_name();
    drop_if_exists(server, &carrying).await?;
    sqlx::raw_sql(&format!("CREATE DATABASE \"{carrying}\"")).execute(&mut *server).await.with_context(|| format!("create {carrying}"))?;

    let into = weft_task_store::db::options(2, std::time::Duration::from_secs(30))
        .connect_with(install.options_for(&carrying))
        .await
        .context("open the carrying database")?;
    crate::schema::apply(&into).await.context("build today's tables in the carrying database")?;
    into.close().await;

    let mut from = PgConnection::connect_with(&install.options_for(&install.old_name())).await.context("open the old database")?;
    let mut to = PgConnection::connect_with(&install.options_for(&carrying)).await.context("open the carrying database")?;
    let copied = copy::copy(&mut from, &mut to).await?;
    verify::verify(&mut to).await?;
    let files = copy::copy_files(&mut from, &install.objects).await?;
    to.close().await?;
    from.close().await?;
    Ok(Done::Carried { runs: copied.runs, files })
}

/// The last step: the old file copies deleted, the old database dropped.
async fn finish(install: &Install, server: &mut PgConnection) -> anyhow::Result<()> {
    let mut from = PgConnection::connect_with(&install.options_for(&install.old_name())).await.context("open the old database")?;
    copy::delete_old_files(&mut from, &install.objects).await?;
    from.close().await?;
    sqlx::raw_sql(&format!("DROP DATABASE \"{}\" WITH (FORCE)", install.old_name()))
        .execute(&mut *server)
        .await
        .with_context(|| format!("drop {}", install.old_name()))?;
    println!("dropped the old database '{}'", install.old_name());
    Ok(())
}

async fn exists(server: &mut PgConnection, name: &str) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)").bind(name).fetch_one(server).await?)
}

async fn rename(server: &mut PgConnection, from: &str, to: &str) -> anyhow::Result<()> {
    sqlx::raw_sql(&format!("ALTER DATABASE \"{from}\" RENAME TO \"{to}\""))
        .execute(server)
        .await
        .with_context(|| format!("rename the database {from} to {to} (is something still connected to it?)"))?;
    Ok(())
}

async fn drop_if_exists(server: &mut PgConnection, name: &str) -> anyhow::Result<()> {
    sqlx::raw_sql(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)")).execute(server).await.with_context(|| format!("drop {name}"))?;
    Ok(())
}

/// The install `config` names, for a carry: its database from
/// `WEFT_DATABASE_URL`, its object store from the config.
pub async fn install(config: &weft_platform_traits::InstallConfig) -> anyhow::Result<Install> {
    let url = std::env::var("WEFT_DATABASE_URL").context("WEFT_DATABASE_URL is required; the install puts it in this process's environment")?;
    let live = PgConnectOptions::from_str(&url).context("WEFT_DATABASE_URL is not a Postgres address")?;
    anyhow::ensure!(
        matches!(config.platform, weft_platform_traits::config::PlatformConfig::Local(_)),
        "a carry runs on a local install, where `weft daemon start` runs it"
    );
    let objects = match &config.object_store {
        weft_platform_traits::config::ObjectStoreSettings::S3(s3) => weft_platform_traits::object_store_for(s3).await?,
        weft_platform_traits::config::ObjectStoreSettings::Gcs(_) => {
            anyhow::bail!("a local install keeps its files in its own object store, not Cloud Storage")
        }
    };
    Ok(Install { live, objects })
}
