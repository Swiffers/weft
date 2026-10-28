//! The platform layer: a host-side, test-only window into what the SYSTEM does
//! underneath a running program (which worker drives a run, what the
//! runtime wrote in its log), plus the levers a test needs to drive it:
//! faking a worker crash, and deleting a build's worker program.
//!
//! ## Why this reaches behind the public API
//!
//! The program layer (the rest of this crate) asserts through the dispatcher's
//! public HTTP API, exactly as the outside world does. But platform facts
//! (which worker replica owns a run, what a run stored) are NOT on that
//! surface, by design: exposing them would add privileged endpoints to the
//! shipped system for a need only tests have. So the platform layer reaches
//! BEHIND the API, the way an operator of a local install can: it reads the
//! install's Postgres directly, reads the runtime's log, signals the worker
//! processes its runtime started (from the install's worker registry), and
//! lists infra containers with Docker. This is host-side TEST code only,
//! compiled solely under the `e2e` feature, never into anything shipped.

use anyhow::{Context, Result};
use std::time::Duration;
use uuid::Uuid;
use weft_task_store::db::{self, Client as _, Pool};

use crate::client::{poll_until, Dispatcher};

/// Host-side handle to one install's platform state, aimed at the install
/// the dispatcher it was made from belongs to (the default one, or a
/// [`crate::cell::Cell`]).
pub struct Platform {
    pool: Pool,
    install: weft_core::infra::Install,
}

/// A pool of at most `max_connections` on the install database at `url`:
/// what a test that seeds rows directly (a connection, a recipe) writes
/// through.
pub async fn database(url: &str, max_connections: u32) -> Result<Pool> {
    let pool = Pool::open(db::config(url)?, max_connections, Duration::from_secs(10), db::Waits::Expected);
    // A first connection now, so an install whose database does not answer
    // fails here rather than at the test's first read.
    pool.get().await.context("connect to the install's Postgres")?;
    Ok(pool)
}

impl Platform {
    /// Connect to `disp`'s install's Postgres, at the address its runtime
    /// uses.
    pub async fn connect(disp: &Dispatcher) -> Result<Self> {
        let install = disp.install().clone();
        let url = install.secret("WEFT_DATABASE_URL").map_err(anyhow::Error::msg)?;
        let pool = database(&url, 4).await?;
        Ok(Self { pool, install })
    }

    /// The replica that wrote the last row of a run's record: for an ended
    /// run, the worker that ended it (`run.owner` is cleared once a run
    /// ends). None for a run nothing wrote yet.
    pub async fn run_writer(&self, run_id: &Uuid) -> Result<Option<String>> {
        self.pool
            .scalar_opt("SELECT writer FROM run_log WHERE run_id = $1 ORDER BY seq DESC LIMIT 1", &[run_id])
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
        self.pool
            .scalar("SELECT COUNT(*) FROM runtime_file WHERE key LIKE $1 AND status = 'pending'", &[&pattern])
            .await
            .context("count pending runtime uploads for run")
    }

    /// A tenant's CHARGED runtime-storage bytes: an ACTIVE file counts by its
    /// size, an in-flight (pending) upload by its reserved bytes. This is the
    /// exact number the broker's quota check enforces, so a test can assert an
    /// interrupted upload freed its reservation.
    pub async fn runtime_charged_bytes(&self, tenant: &str) -> Result<i64> {
        let bytes: Option<i64> = self
            .pool
            .scalar(
                "SELECT COALESCE(SUM(CASE WHEN status = 'active' THEN size_bytes ELSE reserved_bytes END), 0)::BIGINT \
                 FROM runtime_file WHERE tenant_id = $1",
                &[&tenant],
            )
            .await
            .context("sum tenant charged runtime bytes")?;
        Ok(bytes.unwrap_or(0))
    }

    /// The tenant a run's runtime-file rows belong to, or `None` if the
    /// run stored nothing.
    pub async fn runtime_tenant_for_run_id(&self, run_id: &Uuid) -> Result<Option<String>> {
        let pattern = format!("%/run/{run_id}/%");
        self.pool
            .scalar_opt("SELECT tenant_id FROM runtime_file WHERE key LIKE $1 LIMIT 1", &[&pattern])
            .await
            .context("read tenant for run's runtime files")
    }

    /// The worker processes running `project_id`'s program right now, from
    /// the install's worker registry: each one still the process its
    /// runtime started (a pid another program took since is not one).
    async fn workers_of(&self, project_id: &Uuid) -> Result<Vec<weft_platform_local::registry::Entry>> {
        use weft_platform_local::registry;
        let mut running = Vec::new();
        for entry in registry::Registry::in_dir(&weft_platform_local::InstallLayout::new(self.install.dir()).workers()).load()?.into_iter().filter(|e| &e.project == project_id) {
            if registry::still_runs(&weft_platform_local::MachineProcesses, &entry).await? {
                running.push(entry);
            }
        }
        Ok(running)
    }

    /// The names of the worker processes running `project_id`'s program.
    pub async fn workers_for_project(&self, project_id: &Uuid) -> Result<Vec<String>> {
        Ok(self.workers_of(project_id).await?.into_iter().map(|e| e.name).collect())
    }

    /// The running infra containers of `project_id`.
    pub async fn infra_containers_for_project(&self, project_id: &Uuid) -> Result<Vec<String>> {
        use weft_platform_local::docker::{labels, roles};
        let out = tokio::process::Command::new("docker")
            .args([
                "ps",
                "--filter",
                &format!("label={}={}", weft_core::infra::INSTALL_LABEL, self.install.label_value()),
                "--filter",
                &format!("label={}={project_id}", labels::PROJECT),
                "--filter",
                &format!("label={}={}", labels::ROLE, roles::INFRA),
                "--format",
                "{{.Names}}",
            ])
            .output()
            .await
            .context("docker ps")?;
        anyhow::ensure!(out.status.success(), "docker ps failed: {}", String::from_utf8_lossy(&out.stderr));
        Ok(String::from_utf8_lossy(&out.stdout).lines().map(str::to_string).filter(|l| !l.is_empty()).collect())
    }

    /// The programs the worker processes of `project_id` run (each as
    /// its worker names it, `weft-worker:<binary hash>`).
    pub async fn worker_images(&self, project_id: &Uuid) -> Result<Vec<String>> {
        let mut images: Vec<String> = self.workers_of(project_id).await?.into_iter().map(|e| e.image).collect();
        images.sort();
        images.dedup();
        Ok(images)
    }

    /// The build a run runs on: the binary hash of the program its worker
    /// runs.
    pub async fn run_binary_hash(&self, run_id: &Uuid) -> Result<String> {
        let hash: Option<String> =
            self.pool.scalar("SELECT binary_hash FROM run WHERE run_id = $1", &[run_id]).await.context("read the build a run runs on")?;
        hash.ok_or_else(|| anyhow::anyhow!("run {run_id} names no build"))
    }

    /// Delete the worker program `image` names from this machine, the way
    /// a cleanup outside weft does, so no worker of it can start again.
    pub async fn remove_image(&self, image: &str) -> Result<()> {
        let program = weft_platform_local::images::Program::of(image).ok_or_else(|| anyhow::anyhow!("{image} names no worker program"))?;
        let dir = program.dir(&weft_platform_local::MachineLayout::here().workers());
        std::fs::remove_dir_all(&dir).with_context(|| format!("remove {}", dir.display()))
    }

    /// The binary hashes of the worker programs this install holds on the
    /// machine.
    pub fn worker_programs(&self) -> Result<Vec<String>> {
        weft_platform_local::images::held_workers(&weft_platform_local::MachineLayout::here().workers(), self.install.label_value())
    }

    /// Fake a worker crash: kill every worker process of `project_id` at
    /// once (`SIGKILL`), with no chance to finish what it drives. Returns
    /// the ones it killed; an empty answer means nothing was running, which
    /// a resume test treats as a setup miss.
    pub async fn kill_workers(&self, project_id: &Uuid) -> Result<Vec<String>> {
        self.signal_workers(project_id, weft_platform_local::processes::Signal::Kill).await
    }

    /// Tell every worker process of `project_id` to stop the way a
    /// platform does (`SIGTERM`), leaving it to wind down on its own.
    /// Returns the ones it told; an empty answer means nothing was running.
    pub async fn stop_workers(&self, project_id: &Uuid) -> Result<Vec<String>> {
        self.signal_workers(project_id, weft_platform_local::processes::Signal::Term).await
    }

    async fn signal_workers(&self, project_id: &Uuid, signal: weft_platform_local::processes::Signal) -> Result<Vec<String>> {
        use weft_platform_local::processes::Processes as _;
        let running = self.workers_of(project_id).await?;
        for entry in &running {
            weft_platform_local::MachineProcesses.signal(entry.pid, signal)?;
        }
        Ok(running.into_iter().map(|e| e.name).collect())
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
