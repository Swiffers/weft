//! How many runs a worker takes at once (`workers.max_runs_at_once`), and
//! what a call does when they are all taken: it waits for one to end.
//!
//! A call takes a permit at the door before its body is read and gives it
//! back when its run's own work is over (`crate::worker`, the run's slot).
//! With every permit taken, a call waits for one, up to
//! `workers.max_queue_wait_seconds`, and no more calls wait than there are
//! permits; past either, it is answered the busy `503`. A waiting call
//! holds its connection and nothing else. So when the database falls behind
//! and runs hold their permits a little longer (their records wait for room,
//! `crate::journal_writer`), new calls wait in turn, and the worker answers
//! as fast as its records are taken, with no error and no growth.
//!
//! An event waits the same way, and one that waited too long goes in its
//! trigger's queue rather than being refused (`crate::worker`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The variables the platform passes the two levers in
/// ([`weft_platform_traits::WorkerSettings`]).
pub use weft_platform_traits::{MAX_QUEUE_WAIT_ENV, MAX_RUNS_AT_ONCE_ENV};

/// What one run is reckoned to hold, for the default number of permits: a
/// worker takes as many runs at once as its memory holds of these.
const MEMORY_PER_RUN: u64 = 1 << 20;

/// The fewest permits a worker has by default, however little memory it
/// reads.
const FEWEST_BY_DEFAULT: usize = 64;

/// The worker's permits (see the module doc).
pub struct RunPermits {
    permits: Arc<Semaphore>,
    /// How many calls wait for a permit now.
    waiting: AtomicUsize,
    /// The most that may wait at once: as many as there are permits.
    most_waiting: usize,
    /// How long one waits at most.
    wait: Duration,
}

/// One run's permit, given back when it is dropped.
#[derive(Debug)]
pub struct RunPermit {
    _permit: OwnedSemaphorePermit,
}

/// Why a call got no permit: every one was taken, and it waited as long as
/// it may, or there was no room left to wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Busy;

impl RunPermits {
    pub fn new(most: usize, wait: Duration) -> Arc<Self> {
        Arc::new(Self { permits: Arc::new(Semaphore::new(most)), waiting: AtomicUsize::new(0), most_waiting: most, wait })
    }

    /// The permits the platform that started this worker set
    /// ([`MAX_RUNS_AT_ONCE_ENV`], [`MAX_QUEUE_WAIT_ENV`]). With no most set,
    /// the worker takes as many runs as its memory holds of
    /// [`MEMORY_PER_RUN`], and never fewer than [`FEWEST_BY_DEFAULT`]: its
    /// memory budget's (`crate::busy`), or with none (a process sharing a
    /// person's machine), the machine's.
    pub fn from_env(budget: Option<crate::busy::Budget>) -> anyhow::Result<Arc<Self>> {
        let most = match std::env::var(MAX_RUNS_AT_ONCE_ENV) {
            Ok(most) => most.trim().parse::<usize>().map_err(|e| anyhow::anyhow!("{MAX_RUNS_AT_ONCE_ENV} is not a count: {e}"))?,
            Err(_) => default_most(match budget {
                Some(budget) => budget.limit(),
                None => machine_memory(),
            }),
        };
        anyhow::ensure!(most > 0, "{MAX_RUNS_AT_ONCE_ENV} is 0: the worker could take no run");
        let wait = std::env::var(MAX_QUEUE_WAIT_ENV).map_err(|_| {
            anyhow::anyhow!("{MAX_QUEUE_WAIT_ENV} is not set; the platform that started this worker sets it from the project's max_queue_wait_seconds")
        })?;
        let wait = wait.trim().parse::<u64>().map_err(|e| anyhow::anyhow!("{MAX_QUEUE_WAIT_ENV} is not a number of seconds: {e}"))?;
        Ok(Self::new(most, Duration::from_secs(wait)))
    }

    /// A permit, at once when one is free; else after waiting for one (see
    /// the module doc).
    pub async fn take(&self) -> Result<RunPermit, Busy> {
        if let Ok(permit) = self.permits.clone().try_acquire_owned() {
            return Ok(RunPermit { _permit: permit });
        }
        if self.waiting.fetch_add(1, Ordering::AcqRel) >= self.most_waiting {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            return Err(Busy);
        }
        let waited = tokio::time::timeout(self.wait, self.permits.clone().acquire_owned()).await;
        self.waiting.fetch_sub(1, Ordering::AcqRel);
        match waited {
            Ok(Ok(permit)) => Ok(RunPermit { _permit: permit }),
            // The semaphore is never closed; a wait past its time is busy.
            Ok(Err(_)) | Err(_) => Err(Busy),
        }
    }
}

/// All the memory of the machine the worker runs on, in bytes; `None` when
/// it cannot be read.
#[cfg(target_os = "linux")]
fn machine_memory() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo").ok().as_deref().and_then(total_from_meminfo)
}

/// All the memory of the machine the worker runs on, in bytes; `None` when
/// it cannot be read.
#[cfg(target_os = "macos")]
fn machine_memory() -> Option<u64> {
    let mut bytes: u64 = 0;
    let mut size = std::mem::size_of::<u64>();
    // SAFETY: `hw.memsize` is a 64-bit number, written into `bytes`, whose
    // size is passed; nothing is set.
    let read = unsafe {
        libc::sysctlbyname(c"hw.memsize".as_ptr(), (&mut bytes as *mut u64).cast(), &mut size, std::ptr::null_mut(), 0)
    };
    (read == 0 && size == std::mem::size_of::<u64>()).then_some(bytes)
}

/// The machine's memory as `/proc/meminfo` gives it (`MemTotal`), in bytes.
#[cfg(any(target_os = "linux", test))]
fn total_from_meminfo(meminfo: &str) -> Option<u64> {
    let kib = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:")?.trim().strip_suffix("kB")?.trim().parse::<u64>().ok())?;
    Some(kib * 1024)
}

/// The default most runs at once for a worker with `memory` bytes (`None`
/// when it reads none).
fn default_most(memory: Option<u64>) -> usize {
    memory.map_or(FEWEST_BY_DEFAULT, |memory| ((memory / MEMORY_PER_RUN) as usize).max(FEWEST_BY_DEFAULT))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The machine's memory is the total `/proc/meminfo` gives.
    #[test]
    fn the_machine_memory_is_its_total() {
        let meminfo = "MemTotal:        1000 kB\nMemFree:          100 kB\nMemAvailable:     250 kB\n";
        assert_eq!(total_from_meminfo(meminfo), Some(1000 * 1024));
        assert_eq!(total_from_meminfo("MemFree: 100 kB\n"), None);
    }

    /// The default follows the memory, a MiB a run, never below the floor.
    #[test]
    fn the_default_most_follows_the_memory() {
        assert_eq!(default_most(Some(4 << 30)), 4096);
        assert_eq!(default_most(Some(16 << 20)), FEWEST_BY_DEFAULT);
        assert_eq!(default_most(None), FEWEST_BY_DEFAULT);
    }

    /// With every permit taken a call waits for one and gets it when a run
    /// ends; past the wait it is busy; and no more wait than there are
    /// permits.
    #[tokio::test(start_paused = true)]
    async fn a_call_waits_for_a_permit_and_no_longer_than_it_may() {
        let permits = RunPermits::new(1, Duration::from_secs(30));
        let first = permits.take().await.expect("a free permit");
        let waiter = {
            let permits = permits.clone();
            tokio::spawn(async move { permits.take().await })
        };
        tokio::task::yield_now().await;
        // A second waiter finds no room to wait.
        assert_eq!(permits.take().await.unwrap_err(), Busy);
        drop(first);
        let second = waiter.await.unwrap().expect("the permit the run gave back");
        // Every permit taken again: a waiter gives up after the wait.
        let late = {
            let permits = permits.clone();
            tokio::spawn(async move { permits.take().await })
        };
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_eq!(late.await.unwrap().unwrap_err(), Busy);
        drop(second);
        assert!(permits.take().await.is_ok());
    }
}
