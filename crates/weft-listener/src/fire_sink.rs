//! Where a signal the listener serves reports what happened to it: a
//! fire (a timer's tick, an event on a held connection) and the kind's
//! evolving durable state (a feed cursor, a timer's next moment).
//!
//! An entry's event goes straight to the worker's door of its project
//! (`POST <project address>/_weft/fire`, `weft_core::door_fire`), the way a
//! caller does: the worker decides whether it becomes a run now or waits
//! in its trigger's queue. An event whose worker cannot be reached (its
//! project serves no address, the call fails or does not answer) waits in
//! that same queue, put there through the broker, and the install hands it
//! over once the worker is back. An answer to a waiting run is a
//! `FireSignal` task enqueued through the broker, which the dispatcher runs.
//! The listener never calls the dispatcher. A state write goes to the signal row through the
//! broker at once, so the next wake, on whichever copy of the listener it
//! lands, reads it back.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};

use weft_broker_client::BrokerSignalClient;
use weft_core::door_fire::{DoorFire, Fired};
use weft_core::signal::listener_protocol::ProcessTarget;
use weft_task_store::tasks::NewTask;
use weft_task_store::{TaskKind, TaskStoreClient};

/// The worker doors a listener hands events to.
#[async_trait]
pub trait WorkerDoors: Send + Sync {
    /// Hand `fire` to the worker door of `project` at `address`. The call
    /// stays open (on a task of its own) while a run it started goes.
    async fn fire(&self, project: uuid::Uuid, address: &str, fire: &DoorFire) -> Result<Fired>;
}

/// [`WorkerDoors`] over HTTP, with each project's worker key
/// (`weft_core::caller_token::worker_door_key`), derived from the
/// install's caller-ticket secret.
pub struct HttpWorkerDoors {
    http: reqwest::Client,
    secret: Vec<u8>,
}

impl HttpWorkerDoors {
    pub fn new(secret: Vec<u8>) -> Arc<Self> {
        Arc::new(Self { http: weft_core::net::http_client().build().expect("reqwest client"), secret })
    }
}

#[async_trait]
impl WorkerDoors for HttpWorkerDoors {
    async fn fire(&self, project: uuid::Uuid, address: &str, fire: &DoorFire) -> Result<Fired> {
        let key = weft_core::caller_token::worker_door_key(&self.secret, project);
        let send = async {
            Ok(self
                .http
                .post(format!("{}/_weft/fire", address.trim_end_matches('/')))
                .header(weft_platform_traits::WORKER_AUTH_HEADER, weft_platform_traits::worker_auth_value(&key))
                .json(fire)
                .send()
                .await?)
        };
        weft_core::door_fire::hand_over(send, &fire.token, ()).await
    }
}

/// What a fire the sink took became.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivered {
    /// Handed over: a run, a parked fire, an answer queued, or dropped by
    /// the worker for a reason of its own (logged there).
    Taken,
    /// This copy no longer holds the signal: another holder took it.
    NotHeld,
}

/// What makes two fires of one signal the same event.
#[derive(Debug, Clone, Copy)]
pub enum FireIdentity<'a> {
    /// The payload itself: an event carries no name of its own.
    Payload,
    /// A name the kind gives the event (a timer's moment), so a re-fire
    /// with a different payload (a later `actualTime`) is still the same.
    Named(&'a str),
}

/// Listener-wide sink. Cheap to clone. NOT tenant-scoped: the listener
/// holds signals from many tenants, so the tenant travels per fire (it is
/// a property of the signal), never baked into the sink.
#[derive(Clone)]
pub struct FireSignalSink {
    tasks: Arc<dyn TaskStoreClient>,
    signals: Arc<BrokerSignalClient>,
    registry: Arc<crate::registry::Registry>,
    doors: Arc<dyn WorkerDoors>,
    /// The holder's ear for what its signals say (`crate::hold::Holding`):
    /// told when a give-up could not be written, so it looks and writes
    /// it again.
    said: Arc<tokio::sync::Notify>,
    /// What the process keeps of where signals connect and as whom: a
    /// signal given up keeps nothing.
    reach: Arc<crate::reach::Reach>,
}

impl FireSignalSink {
    pub fn new(
        tasks: Arc<dyn TaskStoreClient>,
        signals: Arc<BrokerSignalClient>,
        registry: Arc<crate::registry::Registry>,
        doors: Arc<dyn WorkerDoors>,
        said: Arc<tokio::sync::Notify>,
        reach: Arc<crate::reach::Reach>,
    ) -> Self {
        Self { tasks, signals, registry, doors, said, reach }
    }

    /// Deliver one event of the signal `token`: an entry's straight to its
    /// project's worker door, or into its trigger's queue when that worker
    /// cannot be reached; an answer to a waiting run as a `FireSignal` task.
    /// `tenant_id` is the signal's tenant; the broker checks it against the
    /// signal's real one. `held_by` names the holder a held connection fires
    /// under; the event is not taken once the row is not held under it.
    ///
    /// `identity` makes two fires the same event: a retry of the same
    /// event is born once, and two distinct events are two runs.
    pub async fn fire(
        &self,
        token: &str,
        tenant_id: &str,
        held_by: Option<&str>,
        payload: Value,
        identity: FireIdentity<'_>,
    ) -> Result<Delivered> {
        let fire_id = fire_id(token, identity);
        let Some(target) = self.signals.fire_target(token).await? else {
            return Err(anyhow::Error::new(weft_broker_client::BrokerRefused {
                path: "/v1/signal/fire_target".into(),
                status: reqwest::StatusCode::NOT_FOUND,
                body: format!("no signal is registered under {token}"),
            }));
        };
        // The kind's own work on the event, done once here: the dispatcher
        // hands what it made over without asking again.
        let processed = crate::kinds::process_in(&self.registry, &self.signals, token, payload).await?;
        let payload = match processed.target.for_signal(token, target.is_resume)? {
            ProcessTarget::Drop { reason } => {
                tracing::debug!(target: "weft_listener::fire_sink", %token, ?reason, "the kind dropped the event");
                return Ok(Delivered::Taken);
            }
            ProcessTarget::Resume { run_id } => {
                return self.enqueue_answer(token, tenant_id, held_by, run_id, processed.value, fire_id).await;
            }
            ProcessTarget::Entry => processed.value,
        };
        let fire = DoorFire { token: token.to_string(), fire_id, payload, caller: None, held_by: held_by.map(str::to_string), attempts: 0, released: false };
        let unreached = match target.address.as_deref() {
            None => "its project serves no address now".to_string(),
            Some(address) => match self.doors.fire(target.project_id, address, &fire).await {
                Ok(Fired::NotHeld) => return Ok(Delivered::NotHeld),
                Ok(_) => return Ok(Delivered::Taken),
                Err(e) => format!("{e:#}"),
            },
        };
        self.park(fire, &unreached).await
    }

    /// Put an entry's event whose worker could not be reached in its
    /// trigger's queue, already processed, so the install hands it to the
    /// worker once it is back (`weft_task_store::parked_fires`). Its id is
    /// the fire's: if the worker did take it before the call failed, handing
    /// it over again finds the run already born. A worker out of reach is
    /// a failed hand-over: tried again shortly a few times, then it stops
    /// (`weft_task_store::parked_fires::Turn::next`).
    async fn park(&self, fire: DoorFire, unreached: &str) -> Result<Delivered> {
        let held_by = fire.held_by.clone();
        let parked = weft_task_store::parked_fires::waiting(
            fire.fire_id,
            fire.payload,
            None,
            0,
            weft_task_store::parked_fires::WaitsOn::Failure,
            unreached,
        );
        let token = fire.token;
        use weft_broker_client::protocol::DoorParked;
        match self.signals.park_fire(&token, &parked, held_by.as_deref()).await? {
            // Another holder took the signal: it serves its own events.
            DoorParked::NotHeld => return Ok(Delivered::NotHeld),
            DoorParked::Parked => {
                tracing::info!(target: "weft_listener::fire_sink", %token, reason = %unreached, "an event's worker could not be reached; it waits in its trigger's queue");
            }
            DoorParked::QueueFull => {
                tracing::warn!(target: "weft_listener::fire_sink", %token, reason = %unreached, "an event's worker could not be reached and its trigger's queue is full; the event is dropped");
            }
            DoorParked::Gone | DoorParked::TakesNoWork => {
                tracing::info!(target: "weft_listener::fire_sink", %token, "an event's trigger takes no work any more; the event is dropped");
            }
        }
        Ok(Delivered::Taken)
    }

    /// The `FireSignal` task for `value`, the kind's answer to the waiting
    /// run `run_id`: a retry of the same event collapses onto its
    /// task row while it is still queued.
    async fn enqueue_answer(
        &self,
        token: &str,
        tenant_id: &str,
        held_by: Option<&str>,
        run_id: weft_core::RunId,
        value: Value,
        fire_id: uuid::Uuid,
    ) -> Result<Delivered> {
        self.tasks
            .enqueue_dedup(NewTask {
                kind: TaskKind::FireSignal,
                project_id: None,
                dedup_key: Some(format!("fire:{fire_id}")),
                run_id: Some(run_id),
                tenant_id: tenant_id.to_string(),
                payload: serde_json::to_value(weft_task_store::kinds::FireSignalPayload {
                    token: token.to_string(),
                    run_id,
                    value,
                    held_by: held_by.map(str::to_string),
                })?,
            })
            .await?;
        Ok(Delivered::Taken)
    }

    /// Claim the kind's moment: write its durable state one version past
    /// `from_seq`, only while the row is still at `from_seq`. Whether it
    /// landed.
    pub async fn claim_kind_state(&self, token: &str, kind_state: Value, from_seq: i64) -> Result<bool> {
        self.signals.write_kind_state(token, kind_state, from_seq).await
    }

    /// The kind's durable state on signal `token`'s row and the version it
    /// is at, `None` when no held signal goes by that token.
    pub async fn kind_state_of(&self, token: &str) -> Result<Option<crate::kinds::RowState>> {
        Ok(self.signals.get_held(token).await?.map(|row| crate::kinds::RowState { state: row.kind_state, seq: row.kind_state_seq }))
    }

    /// Give up registration `generation` of signal `token` for good: the
    /// row says why (and lets go of its holder), and whatever runs under
    /// the token here stops, keeping none of the credentials it connected
    /// with (`crate::reach`). It stops here even when the row could not be
    /// written, which is the error answered; a connection held here then
    /// stays this process's claim until its next look writes it
    /// (`Registry::unrecorded`), and any other signal's is its caller's to
    /// try again.
    pub async fn give_up(&self, token: &str, generation: i64, reason: &str) -> Result<()> {
        let written = self
            .signals
            .give_up(&weft_broker_client::protocol::SignalGiveUpRequest { token: token.to_string(), generation, reason: reason.to_string() })
            .await;
        // Only this registration (or an older one) stops: a newer one of
        // the token that came up here since is not what was given up on.
        let mine = crate::registry::Registrations::UpTo(generation);
        let was_down_holding = self.registry.clear_down(token, mine) == Some(true);
        let holding_here = self.registry.remove(token, mine).is_some() || was_down_holding;
        self.reach.forget(token, mine);
        match &written {
            Ok(()) => self.registry.drop_unrecorded(token, mine),
            Err(_) if holding_here => {
                self.registry.keep_unrecorded(token, crate::registry::GaveUp { generation, reason: reason.to_string() });
                self.said.notify_one();
            }
            Err(_) => {}
        }
        written
    }
}

/// A fire's identity: the same for the same named event (a timer's
/// moment), so a re-fire of it is born once; a fresh one for an event that
/// carries no name of its own, whose every delivery is its own event.
fn fire_id(token: &str, identity: FireIdentity<'_>) -> uuid::Uuid {
    /// Namespace of named events' ids: generated once and frozen.
    const NAMED: uuid::Uuid = uuid::Uuid::from_u128(0x6f1d_2a77_93b4_4c0e_8a52_3e1f_9d0c_b7a4);
    match identity {
        FireIdentity::Named(name) => {
            let mut h = Sha256::new();
            h.update(token.as_bytes());
            h.update(b"\0name\0");
            h.update(name.as_bytes());
            uuid::Uuid::new_v5(&NAMED, &h.finalize())
        }
        // Distinct events with identical payloads are distinct fires.
        FireIdentity::Payload => uuid::Uuid::new_v4(),
    }
}
