//! How many holders run (`weft_platform_traits::holder_pool`): counted
//! from the held signals and set on the platform, so the holders start as
//! the first held signal is registered and stop with the last. The
//! holders themselves look only when told (`weft_platform_traits::holders`):
//! every change of the held set is told once the pool is sized for it, and
//! every connection made again or removed as it is announced
//! ([`ACCESS_REMADE_CHANNEL`]).

use weft_broker_client::protocol::{ACTIVATION_LISTENS, SIGNAL_ACTIVATION_JOIN};
use weft_platform_traits::{HolderNews, HolderNote, HolderPool};
use weft_task_store::drain::{DrainLoop, DrainStep, WakeOn};

use crate::state::DispatcherState;

/// Announced when the held set changes: a held signal comes, goes, gives
/// up or loses its holder (the `signal` table's triggers), and when an
/// activation changes status, whether it takes work, or until when (the
/// `trigger_activation` table's): a trigger switched on, wiped, or past
/// its hibernation's grace window changes which signals are held without a
/// write to their rows.
// SYNC: HELD_SIGNALS_CHANNEL <-> crates/weft-dispatcher/src/journal/postgres.rs (signal_held_notify)
pub const HELD_SIGNALS_CHANNEL: &str = "weft_held_signals";

pub(crate) static ON_HELD_SIGNALS: &[WakeOn] = &[WakeOn::any(HELD_SIGNALS_CHANNEL)];

/// The loop that tells the holders a connection's access changed.
pub(crate) const HOLDER_ACCESS: &str = "holder_access";

/// Announced when a connection is made again (a reconnect or a rotate
/// bumps its `reconnects`) or removed, with its id: what a listener
/// keeps of its credentials is then stale. A token refresh is not
/// announced here: what a listener keeps carries the moment the broker
/// said it stops being good, and is fetched again before it.
/// [`crate::held::ACCESS_CHANNEL`] announces every write to a connection,
/// a refresh included, for the copies that keep its row.
// SYNC: ACCESS_REMADE_CHANNEL <-> crates/weft-dispatcher/src/journal/postgres.rs (signal_access_stale)
pub const ACCESS_REMADE_CHANNEL: &str = "weft_access_remade";

pub(crate) static ON_ACCESS: &[WakeOn] = &[WakeOn::any(ACCESS_REMADE_CHANNEL)];

/// How many signals a holder holds now: every held row whose governing
/// activation is one a listener holds and that no holder gave up on, the
/// same rows a holder claims from.
pub async fn held_count(pool: &sqlx::PgPool) -> anyhow::Result<u64> {
    let n: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM signal s {SIGNAL_ACTIVATION_JOIN} \
         WHERE s.holds AND s.held_failed IS NULL AND {ACTIVATION_LISTENS}"
    ))
    .fetch_one(pool)
    .await?;
    Ok(u64::try_from(n).unwrap_or(0))
}

/// Run as many holders as the held signals need, `per_copy` to a holder,
/// then tell the holders the held set changed, so each looks once. Every
/// change is announced, so nothing is looked at again until the next one.
/// A tell that fails fails the pass: a holder that missed it would not
/// take a new signal.
pub async fn size(pool: &sqlx::PgPool, holders: &dyn HolderPool, news: &dyn HolderNews, per_copy: u32) -> anyhow::Result<DrainStep> {
    let copies = weft_platform_traits::holder_pool::copies_for(held_count(pool).await?, per_copy);
    holders.resize(copies).await?;
    news.tell(HolderNote::HeldSetChanged).await?;
    Ok(DrainStep::Done)
}

/// The holder sizing as a loop of the dispatcher: woken when the held set
/// changes ([`HELD_SIGNALS_CHANNEL`]).
pub fn drain_loop(state: &DispatcherState) -> DrainLoop {
    crate::reaper::woken(state, ON_HELD_SIGNALS, "holders", "size the pool of holders", |state| async move {
        size(&state.pg_pool, state.holder_pool.as_ref(), state.holder_news.as_ref(), state.holder_settings.signals_per_copy).await
    })
}

/// Tell the holders a connection was made again or removed, and register
/// again the signals no holder holds whose connection it was, so none
/// keeps a stale credential: woken by each one announced
/// ([`ACCESS_REMADE_CHANNEL`]).
pub fn access_loop(state: &DispatcherState) -> DrainLoop {
    crate::reaper::woken(state, ON_ACCESS, HOLDER_ACCESS, "tell the listeners a connection was made again", |state| async move {
        state.holder_news.tell(HolderNote::AccessChanged).await?;
        reregister_stale_access(&state).await?;
        Ok(DrainStep::Done)
    })
}

/// Register again every signal no holder holds whose connection changed
/// or went (`signal.access_stale`, marked by the change itself, so only
/// the signals acting as that connection). A listener copy keeps a
/// connection's credentials by registration (`weft_listener::reach`) and
/// hears no news, so the new registration is what makes it take them
/// afresh. It comes up through the listener's `/start` naming the row it
/// replaced, whose wakes are cancelled first. A listener that answers
/// that it could not bring one up owns it from then on (it retries, then
/// gives up on the node); one that cannot be reached fails the pass, and
/// the mark stays for the next.
pub async fn reregister_stale_access(state: &DispatcherState) -> anyhow::Result<()> {
    use crate::journal::postgres::{access_taken_afresh, reregister_for_access, signals_with_stale_access};
    for stale in signals_with_stale_access(&state.pg_pool).await? {
        let Some(fresh) = reregister_for_access(&state.pg_pool, &stale).await? else { continue };
        let started = state.listener.start(&fresh.token, weft_core::signal::listener_protocol::StartMode::Restore, Some(&stale)).await;
        match started {
            Ok(()) => {}
            Err(e) if e.downcast_ref::<crate::listener::ListenerRefused>().is_some() => tracing::warn!(
                target: "weft_dispatcher::holders", token = %fresh.token, error = %format!("{e:#}"),
                "a signal registered again for its changed connection did not come up; the listener retries it"
            ),
            Err(e) => return Err(e.context(format!("bring up signal {} again for its changed connection", fresh.token))),
        }
        access_taken_afresh(&state.pg_pool, &fresh.token, fresh.generation).await?;
    }
    Ok(())
}
