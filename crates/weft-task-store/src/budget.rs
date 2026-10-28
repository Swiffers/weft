//! How many connections each process takes from the install's database.
//!
//! A database takes a fixed number of connections at once
//! (`max_connections`), and every process that opens it holds pools of
//! its own. Asked for more than it takes, the database refuses the next
//! connection, and whatever wanted it fails. So each process asks the
//! database at start how many it takes ([`Capacity::read`]), keeps a few
//! back for everyone else, divides the rest by the most processes that
//! can be up at once ([`Sharing`]), and sizes its pools to its share
//! ([`pools`]). Whatever the load, the install as a whole never asks for
//! more than the database takes: when every connection of a copy is in
//! use, the next request waits in that copy's line for one
//! (`crate::throttle` says so), rather than the database refusing it.
//!
//! A local install is one process, which takes the whole database. A
//! cloud install runs copies of the dispatcher and the broker, as many as
//! the platform starts, up to the ceilings in the install config
//! (`InstallConfig::max_copies`); each copy takes an equal share.
//!
//! A pooled address (a pooler such as Neon's in front of the database)
//! answers the database's own `max_connections`: the pooler hands each
//! transaction one of the database's connections, and queues what it
//! cannot serve at once. Sizing to that number keeps weft within what the
//! pooler can serve without queueing, so it is read the same way.

use std::collections::BTreeMap;

use crate::db::{Client, Session};
use weft_platform_traits::CoreRole;

/// Connections kept back for whoever else opens the database: a person's
/// `psql`, a migration the install workflow runs, a test's own pool, the
/// database's own maintenance. Taken from what the database lets weft's
/// user open, on top of the ones it keeps for its superusers.
pub const KEPT_FOR_OTHERS: u32 = 3;

/// What the install's database takes, as it answered at start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capacity {
    /// `max_connections`.
    pub max_connections: u32,
    /// The connections a user that is not a superuser can never open
    /// (`superuser_reserved_connections`, plus `reserved_connections` on
    /// Postgres 16 and later).
    pub reserved: u32,
    /// A tighter limit on weft's own user or database, when one is set
    /// (`pg_roles.rolconnlimit`, `pg_database.datconnlimit`).
    pub limit: Option<u32>,
}

impl Capacity {
    /// Ask the database at `url` (one connection of its own, closed again).
    /// A database still starting is waited for, as a pool's first
    /// connection is (`crate::db`).
    pub async fn read(url: &str) -> anyhow::Result<Capacity> {
        let config = crate::db::config(url)?;
        crate::db::reaching(|| async { Self::query(&Session::connect(&config).await?).await }).await
    }

    async fn query(db: &impl Client) -> crate::db::Result<Capacity> {
        let (max_connections, superuser, reserved, role_limit, database_limit): (i32, i32, i32, i32, i32) = db.one(
            "SELECT current_setting('max_connections')::int, \
                    current_setting('superuser_reserved_connections')::int, \
                    COALESCE(current_setting('reserved_connections', true), '0')::int, \
                    COALESCE((SELECT rolconnlimit FROM pg_roles WHERE rolname = current_user), -1), \
                    COALESCE((SELECT datconnlimit FROM pg_database WHERE datname = current_database()), -1)",
            &[],
        )
        .await?;
        let limit = [role_limit, database_limit].into_iter().filter_map(|l| u32::try_from(l).ok()).min();
        Ok(Capacity {
            max_connections: u32::try_from(max_connections).unwrap_or(0),
            reserved: u32::try_from(superuser + reserved).unwrap_or(0),
            limit,
        })
    }

    /// The connections weft may hold, all its processes together.
    pub fn usable(&self) -> u32 {
        let open_to_weft = self.max_connections.saturating_sub(self.reserved);
        self.limit.map_or(open_to_weft, |limit| limit.min(open_to_weft)).saturating_sub(KEPT_FOR_OTHERS)
    }
}

/// How many processes may hold the database's connections at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sharing {
    /// A local install's one process, which holds them all.
    OneProcess,
    /// The most copies of each role that opens the database
    /// (`InstallConfig::max_copies`); every copy takes an equal share.
    Copies(BTreeMap<CoreRole, u32>),
}

impl Sharing {
    /// How many shares the usable connections are split into.
    pub fn shares(&self) -> u32 {
        match self {
            Sharing::OneProcess => 1,
            Sharing::Copies(copies) => copies.values().sum::<u32>().max(1),
        }
    }

    /// The sharing in words: "one process", "2 dispatchers and 4 brokers".
    fn said(&self) -> String {
        match self {
            Sharing::OneProcess => "the install's one process".to_string(),
            Sharing::Copies(copies) => {
                let each: Vec<String> = copies.iter().map(|(role, n)| format!("up to {n} {role}{}", if *n == 1 { "" } else { "s" })).collect();
                each.join(" and ")
            }
        }
    }
}

/// The pools one process holds on the database, in connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pools {
    /// What every request works through.
    pub work: u32,
    /// The dispatcher's lock pool (`weft_dispatcher::lease`), when the
    /// process runs the dispatcher.
    pub dispatcher_locks: Option<u32>,
    /// The broker's lock pool (its lock on one signal's subscriptions),
    /// when the process runs the broker.
    pub broker_locks: Option<u32>,
    /// The pool the broker writes workers' batches of records on, when
    /// the process runs the broker.
    pub records: Option<u32>,
    /// The one session that listens for the database's announcements
    /// (`crate::pg_signal`).
    pub listen: u32,
    /// This process's share, which the pools add up to at most.
    pub share: u32,
}

/// One pool's place in a share: the fewest it works with, and the most it
/// takes when the share has room (what one process uses at its busiest).
#[derive(Debug, Clone, Copy)]
struct Want {
    least: u32,
    most: u32,
}

/// The work pool: a request holds one at a time, and a lock's holder
/// works through it while the lock pool holds the lock, so two is the
/// least that lets a lock holder and anything else go on at once.
const WORK: Want = Want { least: 2, most: 16 };
/// A lock pool: each lock held is one connection that does no work
/// (`crate::locks`), and a lock waits for a free lock connection without
/// holding anything. No holder takes a second lock from the same pool,
/// and what is held across long work (a sweep, a front deploying, a
/// tenant's signal registrations) is a lease row instead
/// (`crate::locks::with_lease`), so holders only take turns; two let the
/// broker's lock on one signal's subscriptions, held across a provider's
/// subscribe, leave another lock free.
const LOCKS: Want = Want { least: 2, most: 16 };
/// The broker's record pool: one batch at a time is correct, a worker's
/// lane waiting for its turn (`weft_broker::records`).
const RECORDS: Want = Want { least: 1, most: 8 };
/// The listening session.
const LISTEN: u32 = 1;

/// The share is too small for the process to work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooSmall(String);

impl std::fmt::Display for TooSmall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TooSmall {}

/// The pools of a process that runs `roles` (the ones of
/// `weft_platform_traits::config::DATABASE_ROLES` it runs), on a database
/// of `capacity`, shared as `sharing` says. A share with room for every
/// pool at its most gives each its most; a smaller one gives each pool
/// its least, and what is left in proportion to how much more each takes
/// at its most. Refused, naming the levers, when the share cannot give
/// every pool its least.
pub fn pools(capacity: &Capacity, sharing: &Sharing, roles: &[CoreRole]) -> Result<Pools, TooSmall> {
    let dispatcher = roles.contains(&CoreRole::Dispatcher);
    let broker = roles.contains(&CoreRole::Broker);
    // In the order a share's leftovers go: the work pool first.
    let mut wants: Vec<Want> = vec![WORK];
    if dispatcher {
        wants.push(LOCKS);
    }
    if broker {
        wants.extend([LOCKS, RECORDS]);
    }
    let share = capacity.usable() / sharing.shares();
    let least = LISTEN + wants.iter().map(|w| w.least).sum::<u32>();
    if share < least {
        return Err(TooSmall(too_small(capacity, sharing, share, least)));
    }
    let sizes = split(share - LISTEN, &wants);
    let mut sizes = sizes.into_iter();
    let work = sizes.next().expect("the work pool is always wanted");
    let dispatcher_locks = dispatcher.then(|| sizes.next().expect("wanted"));
    let (broker_locks, records) = match broker {
        true => (sizes.next(), sizes.next()),
        false => (None, None),
    };
    Ok(Pools { work, dispatcher_locks, broker_locks, records, listen: LISTEN, share })
}

/// `room` connections split over `wants`: each its least, then what is
/// left in proportion to how much more each takes at its most, the
/// remainder of that going one at a time to the first that still takes
/// more; never past a pool's most.
fn split(room: u32, wants: &[Want]) -> Vec<u32> {
    let mut sizes: Vec<u32> = wants.iter().map(|w| w.least).collect();
    let spare = room - sizes.iter().sum::<u32>();
    let more: u32 = wants.iter().map(|w| w.most - w.least).sum();
    if spare >= more {
        return wants.iter().map(|w| w.most).collect();
    }
    for (size, want) in sizes.iter_mut().zip(wants) {
        *size += u32::try_from(u64::from(spare) * u64::from(want.most - want.least) / u64::from(more)).expect("at most `spare`");
    }
    let mut left = room - sizes.iter().sum::<u32>();
    while left > 0 {
        let Some(at) = sizes.iter().zip(wants).position(|(size, want)| *size < want.most) else { break };
        sizes[at] += 1;
        left -= 1;
    }
    sizes
}

/// What a process whose share is too small says: the numbers, then the
/// levers.
fn too_small(capacity: &Capacity, sharing: &Sharing, share: u32, least: u32) -> String {
    let limit = match capacity.limit {
        Some(limit) => format!(", and weft's user or database is limited to {limit}"),
        None => String::new(),
    };
    let levers = match sharing {
        Sharing::OneProcess => "Give the install's Postgres more connections (its max_connections setting), then start the install again.".to_string(),
        Sharing::Copies(_) => "On Cloud SQL, raise the fork's WEFT_CLOUD_SQL_TIER (and WEFT_CLOUD_SQL_MAX_CONNECTIONS with it, if you set one), \
             or lower WEFT_DISPATCHER_MAX_COPIES or WEFT_BROKER_MAX_COPIES, then run the install workflow again. \
             On a database you bring (WEFT_DATABASE_URL), move to a plan of your provider's that takes more connections, \
             or lower the same copy ceilings."
            .to_string(),
    };
    format!(
        "the database takes {} connections ({} of them kept for its superusers{limit}, and {KEPT_FOR_OTHERS} more left for anyone else), \
         shared by {}: this copy's share is {share}, and it needs at least {least} to run. {levers}",
        capacity.max_connections,
        capacity.reserved,
        sharing.said(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(max_connections: u32) -> Capacity {
        Capacity { max_connections, reserved: 3, limit: None }
    }

    fn cloud(dispatchers: u32, brokers: u32) -> Sharing {
        Sharing::Copies(BTreeMap::from([(CoreRole::Dispatcher, dispatchers), (CoreRole::Broker, brokers)]))
    }

    fn total(p: &Pools) -> u32 {
        p.work + p.dispatcher_locks.unwrap_or(0) + p.broker_locks.unwrap_or(0) + p.records.unwrap_or(0) + p.listen
    }

    /// A local install on Postgres's own default (100) gives its one
    /// process every pool at its most, which still leaves room.
    #[test]
    fn a_local_install_on_a_roomy_database_takes_every_pool_at_its_most() {
        let p = pools(&db(100), &Sharing::OneProcess, &[CoreRole::Dispatcher, CoreRole::Broker]).unwrap();
        assert_eq!(p, Pools { work: 16, dispatcher_locks: Some(16), broker_locks: Some(16), records: Some(8), listen: 1, share: 94 });
        assert!(total(&p) <= p.share);
    }

    /// The default cloud install on the default tier (db-g1-small, 50
    /// connections, 2 dispatchers and 4 brokers) fits: 6 shares of 7, every
    /// copy with each pool at least at its least, and all of them together
    /// within what the database takes.
    #[test]
    fn the_default_cloud_install_fits_the_smallest_default_tier() {
        let capacity = db(50);
        let sharing = cloud(2, 4);
        let broker = pools(&capacity, &sharing, &[CoreRole::Broker]).unwrap();
        let dispatcher = pools(&capacity, &sharing, &[CoreRole::Dispatcher]).unwrap();
        assert_eq!(broker, Pools { work: 3, dispatcher_locks: None, broker_locks: Some(2), records: Some(1), listen: 1, share: 7 });
        assert_eq!(dispatcher, Pools { work: 3, dispatcher_locks: Some(3), broker_locks: None, records: None, listen: 1, share: 7 });
        assert!(2 * total(&dispatcher) + 4 * total(&broker) <= capacity.usable());
    }

    /// A share never hands a pool more than its most, and the pools never
    /// add up to more than the share, at any size.
    #[test]
    fn a_share_of_any_size_stays_within_itself_and_each_pool_within_its_most() {
        for max_connections in 0..400 {
            for roles in [&[CoreRole::Dispatcher][..], &[CoreRole::Broker], &[CoreRole::Dispatcher, CoreRole::Broker]] {
                for sharing in [Sharing::OneProcess, cloud(1, 1), cloud(2, 4), cloud(5, 10)] {
                    let Ok(p) = pools(&db(max_connections), &sharing, roles) else { continue };
                    assert!(total(&p) <= p.share, "{max_connections} {roles:?} {sharing:?}: {p:?}");
                    assert!(p.work >= WORK.least && p.work <= WORK.most, "{p:?}");
                    for locks in [p.dispatcher_locks, p.broker_locks].into_iter().flatten() {
                        assert!((LOCKS.least..=LOCKS.most).contains(&locks), "{p:?}");
                    }
                    if let Some(records) = p.records {
                        assert!((RECORDS.least..=RECORDS.most).contains(&records), "{p:?}");
                    }
                }
            }
        }
    }

    /// A share too small for the least of every pool is refused with the
    /// numbers and the levers a person turns.
    #[test]
    fn a_share_too_small_to_run_is_refused_naming_the_levers() {
        let refused = pools(&db(25), &cloud(2, 4), &[CoreRole::Broker]).unwrap_err().to_string();
        assert!(refused.contains("takes 25 connections") && refused.contains("share is 3") && refused.contains("at least 6"), "{refused}");
        assert!(refused.contains("WEFT_CLOUD_SQL_TIER") && refused.contains("WEFT_BROKER_MAX_COPIES"), "{refused}");
        let local = pools(&db(8), &Sharing::OneProcess, &[CoreRole::Dispatcher, CoreRole::Broker]).unwrap_err().to_string();
        assert!(local.contains("max_connections"), "{local}");
    }

    /// The reserved connections, a tighter limit on weft's user or
    /// database, and the few kept for others all come off what weft takes.
    #[test]
    fn what_the_database_keeps_back_is_not_weft_s() {
        assert_eq!(Capacity { max_connections: 50, reserved: 3, limit: None }.usable(), 44);
        assert_eq!(Capacity { max_connections: 50, reserved: 3, limit: Some(20) }.usable(), 17);
        assert_eq!(Capacity { max_connections: 2, reserved: 3, limit: None }.usable(), 0);
    }
}
