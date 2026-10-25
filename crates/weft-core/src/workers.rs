//! A project's worker levers: copies kept warm, the most copies, calls
//! one copy takes at once, CPU, memory. The install sets each for every
//! project ([`WorkerSettings`]); a project may set any of them for itself
//! ([`WorkerOverrides`]), and what it leaves unset follows the install.
//! Here, in the core, because the CLI, the dispatcher and a program
//! (`ctx.workers()`) all speak them.

use serde::{Deserialize, Serialize};

/// The per-project worker levers. Every one has a default that serves a
/// person running their own things; each can be raised per project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSettings {
    /// Copies kept running when idle. 0 scales to zero (a cold start on
    /// the first call after a quiet stretch); 1 or more never waits for
    /// one, and is billed while idle on a cloud.
    #[serde(default)]
    pub min_instances: u32,
    /// Most copies at once.
    #[serde(default = "WorkerSettings::default_max_instances")]
    pub max_instances: u32,
    /// How many calls Cloud Run sends one copy at once; a local install
    /// does not use it.
    #[serde(default = "WorkerSettings::default_concurrency")]
    pub concurrency: u32,
    /// CPUs per copy on a cloud, as the platform counts them ("1", "2",
    /// "0.5"); unset, one CPU on Cloud Run
    /// ([`WorkerSettings::CLOUD_DEFAULT_CPU`]). A local install does not
    /// cap a copy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Memory per copy on a cloud ("512Mi", "2Gi"). A local install does
    /// not cap a copy.
    #[serde(default = "WorkerSettings::default_memory")]
    pub memory: String,
    /// Extra CPU while a copy starts, where the platform offers it.
    #[serde(default = "WorkerSettings::default_startup_boost")]
    pub startup_boost: bool,
    /// Keep CPU on a copy between calls, where the platform throttles it
    /// otherwise (Cloud Run bills CPU only while a request is open). Needed
    /// when a program keeps working after it answered a live caller; it
    /// bills the copy for as long as it is up.
    #[serde(default)]
    pub cpu_always_allocated: bool,
    /// How long a copy holds a live thing its runs share (`ctx.shared`: a
    /// pool of database connections, ready Python processes) once nothing
    /// used it, in seconds. Longer saves rebuilding it after a quiet
    /// stretch; shorter lets it go sooner. The worker reads it from
    /// [`SHARED_IDLE_ENV`].
    #[serde(default = "WorkerSettings::default_shared_idle_seconds")]
    pub shared_idle_seconds: u64,
    /// The most runs one copy takes at once. A call past it waits for a
    /// run to end (`max_queue_wait_seconds`) instead of being refused.
    /// Unset, a copy takes as many as its memory holds at a MiB each, and
    /// never fewer than 64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_runs_at_once: Option<u32>,
    /// How long a call waits for one of a copy's runs to end when it takes
    /// all it may, in seconds, before it is answered busy (`503`).
    #[serde(default = "WorkerSettings::default_max_queue_wait_seconds")]
    pub max_queue_wait_seconds: u64,
}

/// The variable a worker reads [`WorkerSettings::shared_idle_seconds`]
/// from; every platform sets it on every worker it starts.
pub const SHARED_IDLE_ENV: &str = "WEFT_SHARED_IDLE_SECS";

/// The variable a worker reads [`WorkerSettings::max_runs_at_once`] from,
/// set only when the setting is.
pub const MAX_RUNS_AT_ONCE_ENV: &str = "WEFT_MAX_RUNS_AT_ONCE";

/// The variable a worker reads [`WorkerSettings::max_queue_wait_seconds`]
/// from; every platform sets it on every worker it starts.
pub const MAX_QUEUE_WAIT_ENV: &str = "WEFT_MAX_QUEUE_WAIT_SECS";

impl WorkerSettings {
    /// The variables a platform sets on every worker it starts for these
    /// settings, each with its value.
    pub fn worker_env(&self) -> Vec<(&'static str, String)> {
        let mut env = vec![
            (SHARED_IDLE_ENV, self.shared_idle_seconds.to_string()),
            (MAX_QUEUE_WAIT_ENV, self.max_queue_wait_seconds.to_string()),
        ];
        if let Some(most) = self.max_runs_at_once {
            env.push((MAX_RUNS_AT_ONCE_ENV, most.to_string()));
        }
        env
    }
}

impl WorkerSettings {
    fn default_shared_idle_seconds() -> u64 {
        300
    }
    fn default_max_queue_wait_seconds() -> u64 {
        30
    }
    fn default_max_instances() -> u32 {
        10
    }
    /// Cloud Run's own default for a service: a run mostly waits (on a
    /// model, a database, a person), so one copy serves many, and when its
    /// CPU fills the platform adds copies. A copy nearly out of memory turns
    /// new work away itself (`weft_engine`'s memory guard) rather than
    /// taking a run that would bring the others down.
    fn default_concurrency() -> u32 {
        80
    }
    /// What a copy gets on a cloud whose services need a CPU count when
    /// the project and the install set none.
    pub const CLOUD_DEFAULT_CPU: &'static str = "1";
    fn default_memory() -> String {
        "1Gi".into()
    }
    fn default_startup_boost() -> bool {
        true
    }

    /// Refuse a shape no platform could run, naming the lever.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_instances == 0 {
            return Err("workers.max_instances is 0: nothing could ever run; set it to 1 or more".into());
        }
        if self.min_instances > self.max_instances {
            return Err(format!(
                "workers.min_instances ({}) is above workers.max_instances ({}); lower the first or raise the second",
                self.min_instances, self.max_instances
            ));
        }
        if self.concurrency == 0 {
            return Err("workers.concurrency is 0: a copy could serve nothing; set it to 1 or more".into());
        }
        if self.cpu.as_deref().is_some_and(|cpu| cpu.trim().is_empty()) {
            return Err("workers.cpu is empty; set a CPU count (\"1\", \"0.5\") or unset it for the platform's own".into());
        }
        if self.memory.trim().is_empty() {
            return Err("workers.memory must be set".into());
        }
        if self.max_runs_at_once == Some(0) {
            return Err("workers.max_runs_at_once is 0: a copy could take no run; set it to 1 or more, or unset it to follow the copy's memory".into());
        }
        Ok(())
    }
}

/// What a project may set of [`WorkerSettings`] for itself: a lever
/// replaces the install's setting ([`WorkerSettings::with`]) where a
/// project sets it. Listed here once; the overrides struct, the lever
/// names and every per-lever operation follow from the list, and
/// `every_setting_is_a_lever` holds it to [`WorkerSettings`]' fields.
macro_rules! worker_levers {
    ($($lever:ident: $setting:ty),* $(,)?) => {
        /// A project's own worker levers: each one it sets replaces the
        /// install's ([`WorkerSettings::with`]); unset ones follow the
        /// install.
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct WorkerOverrides {
            $(
                #[serde(default, skip_serializing_if = "Option::is_none")]
                pub $lever: Option<<$setting as Lever>::Set>,
            )*
        }

        impl WorkerOverrides {
            /// Whether no lever is set.
            pub fn is_empty(&self) -> bool {
                *self == Self::default()
            }

            /// Every lever's name, as the flags, the API and a `weft.toml`
            /// spell it.
            pub const LEVERS: &'static [&'static str] = &[$(stringify!($lever)),*];

            /// `self` with every lever `o` sets in its place.
            pub fn merged(&self, o: &WorkerOverrides) -> Self {
                Self { $($lever: o.$lever.clone().or_else(|| self.$lever.clone()),)* }
            }

            /// Put the lever named `name` back on the install's. Refused,
            /// listing the levers, for a name that is not one.
            pub fn unset(&mut self, name: &str) -> Result<(), String> {
                $(if name == stringify!($lever) {
                    self.$lever = None;
                    return Ok(());
                })*
                Err(format!("'{name}' is not a worker lever; the levers are {}", Self::LEVERS.join(", ")))
            }

            /// Whether the lever named `name` is set here.
            pub fn sets(&self, name: &str) -> bool {
                $(if name == stringify!($lever) {
                    return self.$lever.is_some();
                })*
                false
            }
        }

        impl WorkerSettings {
            /// These settings with `o`'s set levers in their place.
            pub fn with(&self, o: &WorkerOverrides) -> Self {
                Self { $($lever: self.$lever.overridden(&o.$lever),)* }
            }
        }
    };
}

worker_levers! {
    min_instances: u32,
    max_instances: u32,
    concurrency: u32,
    cpu: Option<String>,
    memory: String,
    startup_boost: bool,
    cpu_always_allocated: bool,
    shared_idle_seconds: u64,
    max_runs_at_once: Option<u32>,
    max_queue_wait_seconds: u64,
}

/// A setting a project can override: what it sets, and the setting once
/// it did.
pub trait Lever: Clone {
    /// What a project sets.
    type Set: Clone;
    fn overridden(&self, by: &Option<Self::Set>) -> Self;
}

macro_rules! plain_lever {
    ($($ty:ty),*) => {$(
        impl Lever for $ty {
            type Set = $ty;
            fn overridden(&self, by: &Option<$ty>) -> Self {
                by.clone().unwrap_or_else(|| self.clone())
            }
        }
    )*};
}
plain_lever!(u32, u64, bool, String);

/// A setting the install may leave to the platform (`cpu`) or to the
/// copy (`max_runs_at_once`): a project sets a value, never that default
/// back.
macro_rules! optional_lever {
    ($($ty:ty),*) => {$(
        impl Lever for Option<$ty> {
            type Set = $ty;
            fn overridden(&self, by: &Option<$ty>) -> Self {
                by.clone().or_else(|| self.clone())
            }
        }
    )*};
}
optional_lever!(String, u32);

/// `GET/PUT /projects/{id}/workers`: a project's worker levers, where
/// each one comes from, and what its workers run with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkersResponse {
    /// What the install gives every project.
    pub install: WorkerSettings,
    /// What this project sets for itself.
    pub project: WorkerOverrides,
    /// What its workers run with.
    pub effective: WorkerSettings,
}

impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            min_instances: 0,
            max_instances: Self::default_max_instances(),
            concurrency: Self::default_concurrency(),
            cpu: None,
            memory: Self::default_memory(),
            startup_boost: Self::default_startup_boost(),
            cpu_always_allocated: false,
            shared_idle_seconds: Self::default_shared_idle_seconds(),
            max_runs_at_once: None,
            max_queue_wait_seconds: Self::default_max_queue_wait_seconds(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_lever_set() -> WorkerOverrides {
        WorkerOverrides {
            min_instances: Some(1),
            max_instances: Some(2),
            concurrency: Some(3),
            cpu: Some("2".into()),
            memory: Some("2Gi".into()),
            startup_boost: Some(false),
            cpu_always_allocated: Some(true),
            shared_idle_seconds: Some(60),
            max_runs_at_once: Some(200),
            max_queue_wait_seconds: Some(5),
        }
    }

    #[test]
    fn the_lever_names_are_the_wire_names() {
        let wire = serde_json::to_value(every_lever_set()).unwrap();
        let keys: Vec<&str> = wire.as_object().unwrap().keys().map(String::as_str).collect();
        let mut levers = WorkerOverrides::LEVERS.to_vec();
        levers.sort_unstable();
        let mut keys = keys;
        keys.sort_unstable();
        assert_eq!(keys, levers);
        let full = every_lever_set();
        assert!(WorkerOverrides::LEVERS.iter().all(|lever| full.sets(lever)));
    }

    #[test]
    fn every_setting_is_a_lever() {
        let settings = WorkerSettings { cpu: Some("1".into()), max_runs_at_once: Some(100), ..WorkerSettings::default() };
        let wire = serde_json::to_value(settings).unwrap();
        let mut keys: Vec<&str> = wire.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut levers = WorkerOverrides::LEVERS.to_vec();
        levers.sort_unstable();
        assert_eq!(keys, levers);
    }

    #[test]
    fn a_set_lever_replaces_the_setting_and_an_unset_one_keeps_it() {
        let install = WorkerSettings::default();
        let with = install.with(&WorkerOverrides { cpu: Some("2".into()), concurrency: Some(4), ..Default::default() });
        assert_eq!(with, WorkerSettings { cpu: Some("2".into()), concurrency: 4, ..install.clone() });
        assert_eq!(install.with(&WorkerOverrides::default()), install);
    }

    #[test]
    fn unsetting_every_lever_leaves_nothing_and_a_wrong_name_is_refused() {
        let mut o = every_lever_set();
        for lever in WorkerOverrides::LEVERS {
            o.unset(lever).unwrap();
        }
        assert_eq!(o, WorkerOverrides::default());
        let refused = o.unset("gpus").unwrap_err();
        assert!(refused.contains("'gpus' is not a worker lever"), "{refused}");
    }

    #[test]
    fn a_merge_keeps_what_the_new_levers_leave_unset() {
        let base = WorkerOverrides { cpu: Some("1".into()), concurrency: Some(4), ..Default::default() };
        let merged = base.merged(&WorkerOverrides { cpu: Some("2".into()), ..Default::default() });
        assert_eq!(merged, WorkerOverrides { cpu: Some("2".into()), concurrency: Some(4), ..Default::default() });
    }


    #[test]
    fn worker_settings_default_and_round_trip() {
        let s: WorkerSettings = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(s, WorkerSettings::default());
        assert!(s.validate().is_ok());
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(serde_json::from_value::<WorkerSettings>(v).unwrap(), s);
        assert!(serde_json::from_value::<WorkerSettings>(serde_json::json!({ "minInstances": 1 })).is_err());
    }

    #[test]
    fn a_projects_levers_replace_only_what_they_set() {
        let o: WorkerOverrides = serde_json::from_value(serde_json::json!({ "min_instances": 1, "memory": "2Gi" })).unwrap();
        let s = WorkerSettings::default().with(&o);
        assert_eq!((s.min_instances, s.memory.as_str()), (1, "2Gi"));
        assert_eq!(s.max_instances, WorkerSettings::default().max_instances);
        assert_eq!(WorkerSettings::default().with(&WorkerOverrides::default()), WorkerSettings::default());
        assert!(serde_json::from_value::<WorkerOverrides>(serde_json::json!({ "region": "x" })).is_err());
    }

    #[test]
    fn impossible_worker_settings_are_refused_naming_the_lever() {
        let mut s = WorkerSettings { min_instances: 3, max_instances: 2, ..Default::default() };
        assert!(s.validate().unwrap_err().contains("workers.min_instances"));
        s = WorkerSettings { max_instances: 0, ..Default::default() };
        assert!(s.validate().unwrap_err().contains("workers.max_instances"));
        s = WorkerSettings { concurrency: 0, ..Default::default() };
        assert!(s.validate().unwrap_err().contains("workers.concurrency"));
    }
}
