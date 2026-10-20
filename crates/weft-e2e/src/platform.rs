//! The platform layer: a host-side, test-only window into what the SYSTEM does
//! underneath a running program (which worker drives a run, what the
//! runtime wrote in its log), plus the levers a test needs to drive it:
//! faking a worker crash, and deleting a build's image.
//!
//! ## Why this reaches behind the public API
//!
//! The program layer (the rest of this crate) asserts through the dispatcher's
//! public HTTP API, exactly as the outside world does. But platform facts
//! (which worker replica owns a run, what a run stored) are NOT on that
//! surface, by design: exposing them would add privileged endpoints to the
//! shipped system for a need only tests have. So the platform layer reaches
//! BEHIND the API, the way an operator of a local install can: it reads the
//! install's Postgres directly, reads the runtime's log, and removes worker
//! containers with Docker. This is host-side TEST code only, compiled solely
//! under the `e2e` feature, never into anything shipped.

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

use crate::client::{poll_until, Dispatcher};

/// What a project's container is for.
#[derive(Debug, Clone, Copy)]
pub enum Role {
    /// Serves the project's program.
    Worker,
    /// Part of an infra node's unit.
    Infra,
}

impl Role {
    fn label(self) -> &'static str {
        use weft_platform_local::docker::roles;
        match self {
            Self::Worker => roles::WORKER,
            Self::Infra => roles::INFRA,
        }
    }
}

/// Host-side handle to one install's platform state, aimed at the install
/// the dispatcher it was made from belongs to (the default one, or a
/// [`crate::cell::Cell`]).
pub struct Platform {
    pool: PgPool,
    install: weft_core::infra::Install,
}


impl Platform {
    /// Connect to `disp`'s install's Postgres, at the address its runtime
    /// uses.
    pub async fn connect(disp: &Dispatcher) -> Result<Self> {
        let install = disp.install().clone();
        let url = install.secret("WEFT_DATABASE_URL").map_err(anyhow::Error::msg)?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&url)
            .await
            .context("connect to the install's Postgres")?;
        Ok(Self { pool, install })
    }

    /// The replica that wrote the last row of a run's record: for an ended
    /// run, the worker that ended it (`run.owner` is cleared once a run
    /// ends). None for a run nothing wrote yet.
    pub async fn run_writer(&self, run_id: &Uuid) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT writer FROM run_log WHERE run_id = $1 ORDER BY seq DESC LIMIT 1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .context("read the writer of a run's record")
    }

    /// How many IN-FLIGHT (pending) runtime-file uploads exist under a run's
    /// run scope. The runtime-file key is `<tenant>/run/<run_id>/<id>`, and a
    /// begin reserves a 'pending' row that only flips 'active' at complete, so
    /// this counts uploads that were started but never finished. An interrupted
    /// upload that cleaned up correctly leaves ZERO: the abort deleted the row
    /// (and freed its quota reservation).
    pub async fn runtime_pending_uploads_for_run_id(&self, run_id: &Uuid) -> Result<i64> {
        let pattern = format!("%/run/{run_id}/%");
        sqlx::query_scalar("SELECT COUNT(*) FROM runtime_file WHERE key LIKE $1 AND status = 'pending'")
            .bind(pattern)
            .fetch_one(&self.pool)
            .await
            .context("count pending runtime uploads for run")
    }

    /// A tenant's CHARGED runtime-storage bytes: an ACTIVE file counts by its
    /// size, an in-flight (pending) upload by its reserved bytes. This is the
    /// exact number the broker's quota check enforces, so a test can assert an
    /// interrupted upload freed its reservation.
    pub async fn runtime_charged_bytes(&self, tenant: &str) -> Result<i64> {
        let bytes: Option<i64> = sqlx::query_scalar(
            "SELECT COALESCE(SUM(CASE WHEN status = 'active' THEN size_bytes ELSE reserved_bytes END), 0)::BIGINT \
             FROM runtime_file WHERE tenant_id = $1",
        )
        .bind(tenant)
        .fetch_one(&self.pool)
        .await
        .context("sum tenant charged runtime bytes")?;
        Ok(bytes.unwrap_or(0))
    }

    /// The tenant a run's runtime-file rows belong to, or `None` if the
    /// run stored nothing.
    pub async fn runtime_tenant_for_run_id(&self, run_id: &Uuid) -> Result<Option<String>> {
        let pattern = format!("%/run/{run_id}/%");
        sqlx::query_scalar("SELECT tenant_id FROM runtime_file WHERE key LIKE $1 LIMIT 1")
            .bind(pattern)
            .fetch_optional(&self.pool)
            .await
            .context("read tenant for run's runtime files")
    }

    /// The worker containers running `project_id`'s program right now.
    pub async fn workers_for_project(&self, project_id: &Uuid) -> Result<Vec<String>> {
        self.containers_for_project(project_id, Role::Worker).await
    }

    /// The running containers of `project_id` in one `role`.
    pub async fn containers_for_project(&self, project_id: &Uuid, role: Role) -> Result<Vec<String>> {
        use weft_platform_local::docker::labels;
        let out = tokio::process::Command::new("docker")
            .args([
                "ps",
                "--filter",
                &format!("label={}={}", weft_core::infra::INSTALL_LABEL, self.install.label_value()),
                "--filter",
                &format!("label={}={project_id}", labels::PROJECT),
                "--filter",
                &format!("label={}={}", labels::ROLE, role.label()),
                "--format",
                "{{.Names}}",
            ])
            .output()
            .await
            .context("docker ps")?;
        anyhow::ensure!(out.status.success(), "docker ps failed: {}", String::from_utf8_lossy(&out.stderr));
        Ok(String::from_utf8_lossy(&out.stdout).lines().map(str::to_string).filter(|l| !l.is_empty()).collect())
    }

    /// The images the worker containers of `project_id` run, as the local
    /// platform labelled them when it started each (the ref it ran).
    pub async fn worker_images(&self, project_id: &Uuid) -> Result<Vec<String>> {
        use weft_platform_local::docker::labels;
        let mut images = Vec::new();
        for name in self.workers_for_project(project_id).await? {
            let out = tokio::process::Command::new("docker")
                .args(["inspect", "--format", &format!("{{{{index .Config.Labels \"{}\"}}}}", labels::IMAGE), &name])
                .output()
                .await
                .with_context(|| format!("docker inspect {name}"))?;
            anyhow::ensure!(out.status.success(), "docker inspect {name} failed: {}", String::from_utf8_lossy(&out.stderr));
            images.push(String::from_utf8_lossy(&out.stdout).trim().to_string());
        }
        images.sort();
        images.dedup();
        Ok(images)
    }

    /// The build a run runs on: the binary hash its worker image is
    /// tagged with.
    pub async fn run_binary_hash(&self, run_id: &Uuid) -> Result<String> {
        let hash: Option<String> = sqlx::query_scalar("SELECT binary_hash FROM run WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(&self.pool)
            .await
            .context("read the build a run runs on")?;
        hash.ok_or_else(|| anyhow::anyhow!("run {run_id} names no build"))
    }

    /// Delete the image `image` from this machine, the way a cleanup
    /// outside weft does (`docker rmi`), so no worker of it can start
    /// again.
    pub async fn remove_image(&self, image: &str) -> Result<()> {
        let out = tokio::process::Command::new("docker")
            .args(["rmi", "--force", image])
            .output()
            .await
            .with_context(|| format!("docker rmi --force {image}"))?;
        anyhow::ensure!(out.status.success(), "docker rmi --force {image} failed: {}", String::from_utf8_lossy(&out.stderr));
        Ok(())
    }

    /// Fake a worker crash: remove every worker container of `project_id`
    /// at once, with no chance to finish what it drives. Returns the ones
    /// it removed; an empty answer means nothing was running, which a
    /// resume test treats as a setup miss.
    pub async fn kill_workers(&self, project_id: &Uuid) -> Result<Vec<String>> {
        let running = self.workers_for_project(project_id).await?;
        for name in &running {
            let out = tokio::process::Command::new("docker")
                .args(["rm", "-f", name])
                .output()
                .await
                .with_context(|| format!("docker rm -f {name}"))?;
            anyhow::ensure!(out.status.success(), "docker rm -f {name} failed: {}", String::from_utf8_lossy(&out.stderr));
        }
        Ok(running)
    }

    /// Tell every worker container of `project_id` to stop the way a
    /// platform does (`SIGTERM`), leaving it to wind down on its own.
    /// Returns the ones it told; an empty answer means nothing was running.
    pub async fn stop_workers(&self, project_id: &Uuid) -> Result<Vec<String>> {
        let running = self.workers_for_project(project_id).await?;
        for name in &running {
            let out = tokio::process::Command::new("docker")
                .args(["kill", "--signal", "TERM", name])
                .output()
                .await
                .with_context(|| format!("docker kill --signal TERM {name}"))?;
            anyhow::ensure!(out.status.success(), "docker kill --signal TERM {name} failed: {}", String::from_utf8_lossy(&out.stderr));
        }
        Ok(running)
    }

    /// The last lines weft's runtime wrote.
    pub fn runtime_log(&self) -> Result<String> {
        let path = self.install.dir().join("runtime.log");
        let raw = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(crate::client::tail(&raw, 200_000).to_string())
    }

    /// Wait until `needle` shows in the runtime's log, via
    /// [`crate::client::poll_until`]. `what` names the awaited state for the
    /// timeout message, which also carries the tail of the log so a timeout
    /// is diagnosable on the spot.
    pub async fn wait_for_runtime_log(&self, what: &str, needle: &str, deadline: Duration) -> Result<()> {
        let last = std::sync::Mutex::new(String::new());
        let result = poll_until(what, deadline, Duration::from_secs(2), || {
            let last = &last;
            async move {
                let log = self.runtime_log()?;
                let hit = log.contains(needle);
                *last.lock().expect("log mutex") = log;
                Ok(hit.then_some(()))
            }
        })
        .await;
        result.map_err(|e| {
            let log = last.into_inner().expect("log mutex");
            e.context(format!("runtime log tail:\n{}", crate::client::tail(&log, 2000)))
        })
    }

}
