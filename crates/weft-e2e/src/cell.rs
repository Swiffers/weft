//! A cell: a whole weft install of a test's own, beside the install under
//! test.
//!
//! Most tests share the install under test and stay out of each other's way,
//! because each only touches the projects it made. A few cannot: the ones
//! that restart the runtime, or wait on its own timers at a faster pace.
//! Those tests start a cell.
//!
//! A cell is a named install (`weft_core::infra::Install`) on the same
//! machine: its own runtime, Postgres and ports, sharing only the object
//! store and the images. The install under test's `weft` starts it, from
//! the images that install already has, so nothing is built.
//!
//! A cell also chooses how fast its own timers run
//! (`weft_core::time_scale`): at `0.1`, a lease that runs a minute
//! of real time runs out in six seconds, and so does every silence window
//! it depends on, together. Budgets for real work (a boot) keep their real
//! length.
//!
//! Like a project, a cell is removed when the test passes
//! ([`Cell::finish`]) and kept when it fails, so what the test saw is still
//! there to look at.

use std::time::Duration;

use anyhow::{Context, Result};

use crate::client::Dispatcher;

/// A cell of a test's own. See the module docs.
pub struct Cell {
    install: weft_core::infra::Install,
    /// `None` only while [`Self::start`] is still bringing it up.
    dispatcher: Option<Dispatcher>,
    time_scale: f64,
    finished: bool,
}

impl Cell {
    /// Start a cell whose own timers run at `time_scale` times real time
    /// (`1.0` for real time, less for a test that waits on them), and
    /// wait until its dispatcher answers.
    pub async fn start(time_scale: f64) -> Result<Self> {
        // Fits `weft_core::infra::MAX_INSTALL_NAME`: the prefix (which no
        // branch's install starts with) and nine characters of a fresh id.
        let name = format!(
            "{}{}",
            weft_core::infra::E2E_CELL_PREFIX,
            &uuid::Uuid::new_v4().simple().to_string()[..9]
        );
        // Said before anything is made, for the post-mortem of a test the
        // runner stops for running too long (its guards never drop).
        // SYNC: this line <-> scripts/run-e2e.sh (post_mortem)
        eprintln!("weft-e2e: made cell '{name}'");
        let install =
            weft_core::infra::Install::named(&name).map_err(anyhow::Error::msg)?;
        // On the ledger before anything of it exists, so the next run's
        // sweep finds whatever part of it a failure leaves.
        crate::kept::cell_made(&name)?;
        // From here on the cell exists, whole or in part: the guard keeps
        // it for inspection if anything below fails.
        let mut cell = Self { install: install.clone(), dispatcher: None, time_scale, finished: false };
        cell.daemon("start").await?;
        let port = public_port(&install)?;
        let dispatcher = Dispatcher::for_install(&format!("http://127.0.0.1:{port}"), install)?;
        crate::ensure::wait_healthy(&dispatcher).await?;
        cell.dispatcher = Some(dispatcher);
        Ok(cell)
    }

    /// Stop the cell's runtime process and start it again, keeping
    /// everything it stored, and wait until its dispatcher answers: what a
    /// machine's reboot or an upgrade does to an install.
    pub async fn restart(&self) -> Result<()> {
        self.daemon("stop").await?;
        self.daemon("start").await?;
        crate::ensure::wait_healthy(&self.dispatcher()).await
    }

    /// Run `weft daemon <verb>` for this cell, at its pace.
    async fn daemon(&self, verb: &str) -> Result<()> {
        daemon(&self.install, verb, self.time_scale).await
    }

    /// The cell's dispatcher: hand it to [`crate::project::Project::prepare`]
    /// and every project, CLI call and platform probe follows it here.
    pub fn dispatcher(&self) -> Dispatcher {
        self.dispatcher.clone().expect("a started cell has a dispatcher")
    }

    /// How fast the cell's own timers run.
    pub fn time_scale(&self) -> f64 {
        self.time_scale
    }

    /// `real` at the cell's pace: what a test waits for one of the
    /// runtime's own timers to come round.
    pub fn scaled(&self, real: Duration) -> Duration {
        weft_core::time_scale::scaled_by(real, self.time_scale)
    }

    /// Remove the cell: call as the last line of a PASSING test, after every
    /// project in it finished. A failing test never gets here, and the cell
    /// stays for inspection.
    pub async fn finish(mut self) -> Result<()> {
        self.daemon("remove").await?;
        // Removed from here on, whatever the ledger says: the guard must
        // not report a removed cell as kept.
        self.finished = true;
        let name = self.name();
        crate::kept::cell_gone(name).with_context(|| {
            format!(
                "the cell {name} is removed, but striking it off the kept ledger failed; \
                 the next run's sweep removes it again"
            )
        })
    }

    fn name(&self) -> &str {
        self.install.name().expect("a cell is a named install")
    }
}

impl Drop for Cell {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Same policy as a project's teardown guard: Drop cannot await, and a
        // cell a test did not finish is exactly what the reader wants to see.
        let at = self.dispatcher.as_ref().map(|d| format!(" at {}", d.base())).unwrap_or_default();
        // The same `weft` the rig runs ([`crate::client::weft_command`]).
        let weft = crate::ensure::install_under_test()
            .map(|under_test| under_test.bin_dir().join("weft").display().to_string())
            .unwrap_or_else(|e| format!("<the install under test's weft: {e}>"));
        eprintln!(
            "weft-e2e: cell '{name}' NOT finished (test ended early); keeping it for \
             inspection{at}. Its files are in {dir}. Remove it with \
             `WEFT_INSTALL={env} {weft} daemon remove`, or everything this \
             install's runs kept with `scripts/run-e2e.sh --clean`.",
            name = self.name(),
            env = self.install.env_value(),
            dir = self.install.dir().display(),
        );
    }
}

/// The port the cell's runtime answers on, from the config its start
/// wrote.
fn public_port(install: &weft_core::infra::Install) -> Result<u16> {
    let path = install.dir().join("config.json");
    let raw = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: weft_platform_traits::InstallConfig =
        serde_json::from_str(&raw).with_context(|| format!("{} is not an install config", path.display()))?;
    match config.platform {
        weft_platform_traits::PlatformConfig::Local(local) => Ok(local.listen.public.port()),
        weft_platform_traits::PlatformConfig::Gcp(_) => anyhow::bail!("{} is a cloud install's config", path.display()),
    }
}

/// Run `weft daemon <verb>` for the cell `install`, its timers at
/// `time_scale`, with the install under test's `weft`
/// ([`crate::client::weft_command`]) in the repo root. One implementation
/// for a cell's whole life: its test starts, restarts and removes it here,
/// and so does the sweep of a cell a failed test kept
/// ([`crate::cleanup::clean_kept`]).
pub(crate) async fn daemon(install: &weft_core::infra::Install, verb: &str, time_scale: f64) -> Result<()> {
    let name = install.name().expect("a cell is a named install");
    let root = crate::ensure::repo_root()?;
    let started = std::time::Instant::now();
    let out = crate::client::weft_command(install)?
        .args(["daemon", verb])
        .current_dir(&root)
        .env("WEFT_REPO_ROOT", &root)
        .env(weft_core::time_scale::TIME_SCALE_ENV, time_scale.to_string())
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .with_context(|| format!("spawn `weft daemon {verb}` for a cell"))?;
    eprintln!("[e2e] {:.1}s: weft daemon {verb} (cell {name})", started.elapsed().as_secs_f64());
    anyhow::ensure!(
        out.status.success(),
        "`weft daemon {verb}` for the cell {name} failed (exit {:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}
