use thiserror::Error;

#[derive(Debug, Error)]
pub enum WeftError {
    #[error("config error: {0}")]
    Config(String),

    #[error("input error: {0}")]
    Input(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("node failed: {0}")]
    NodeFailed(String),

    /// Something the node's package declares it needs from the machine
    /// (a `[[needs]]` entry of its `deps.toml`, by its `name`) is missing
    /// where the worker runs: [`need_missing`]. The engine says it with
    /// the node's type and the way to meet it on that platform
    /// (`crate::context::failure_message`). Like any outcome of the
    /// firing, a wired `_error` catches it: the program was checked
    /// before it started, so this is the machine changing under a run.
    #[error("the machine this worker runs on lacks {0}, which this node needs")]
    NeedMissing(String),

    /// The node called a suspension primitive; the worker should
    /// record the suspension (already done via the ContextHandle)
    /// and exit. On wake, a new worker picks up from the resume
    /// payload.
    #[error("suspended: token {token}")]
    Suspended { token: String },

    #[error("suspension setup failed: {0}")]
    Suspension(String),

    /// A wait the run could not pause on, held in its worker, ran out of
    /// its hold (`holdSecs`) with nothing moving in the run, or was asked
    /// for where the hold is zero. Returned by the waiting `ctx` call, so
    /// the node may handle it like any outcome of its firing.
    #[error("{0}")]
    WaitGaveUp(String),

    /// A person skipped what the waiting `ctx` call waited on (a form's
    /// skip): the node does not run on, its firing ends skipped with every
    /// output closed, the way a node whose input closed does. Returned by the
    /// waiting call, so a body passing errors on with `?` needs no code
    /// of its own for it.
    #[error("{0}")]
    WaitSkipped(String),

    #[error("cancelled")]
    Cancelled,

    #[error("runtime error: {0}")]
    Runtime(#[from] anyhow::Error),
}

pub type WeftResult<T> = Result<T, WeftError>;

/// A node failure carrying exactly `message`. The expression door for
/// node code that detects a bad condition itself: use it inside
/// `map_err`/`ok_or_else` closures that build a rich message, or reach
/// for [`node_bail!`](crate::node_bail) to return it in one statement.
/// The ctx accessors stamp input/config errors themselves, every ctx
/// handle already returns `WeftResult`, outside errors go through
/// `.node_err("doing X")`, and a failure of the work the node worked out
/// itself goes through `node_bail!` / `node_error`, which a program can
/// catch on `_error`. A node names a variant itself only when it spots a
/// mistake in the program (SQL that does not parse, an input value of the
/// wrong shape): `WeftError::Input`, which fails the run even with
/// `_error` wired, because the fix is in the program.
pub fn node_error(message: impl std::fmt::Display) -> WeftError {
    WeftError::NodeFailed(message.to_string())
}

/// The failure of a node that finds a need of its own missing as it runs
/// (a program it launches is not there), named as its `deps.toml` names it
/// (`[[needs]] name`). The node says only what is missing; how to meet it
/// is the platform's to say ([`WeftError::NeedMissing`]).
pub fn need_missing(need: impl std::fmt::Display) -> WeftError {
    WeftError::NeedMissing(need.to_string())
}

/// Fail the node now with this message. The statement door for a
/// node-detected bad condition: expands to
/// `return Err(node_error(format!(...)))`.
#[macro_export]
macro_rules! node_bail {
    ($($arg:tt)*) => {
        return Err($crate::error::node_error(format!($($arg)*)))
    };
}

/// The one door for wrapping an OUTSIDE error (an HTTP library, a JSON
/// parser, any non-weft `Result` or absent `Option`) into a node
/// failure with context.
pub trait NodeErrExt<T> {
    /// Map the error to a node failure reading "`context`: `error`".
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T>;
}

impl<T, E: std::fmt::Display> NodeErrExt<T> for Result<T, E> {
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T> {
        self.map_err(|e| WeftError::NodeFailed(format!("{context}: {e}")))
    }
}

impl<T> NodeErrExt<T> for Option<T> {
    /// `None` becomes a node failure reading exactly `context`, so the
    /// message should state what was missing and why it matters.
    fn node_err(self, context: impl std::fmt::Display) -> WeftResult<T> {
        self.ok_or_else(|| WeftError::NodeFailed(context.to_string()))
    }
}

#[cfg(feature = "runtime")]
impl From<crate::caller::CallerError> for WeftError {
    /// A caller-connection failure surfaces as a node failure (fails
    /// loud, visible in the UI), EXCEPT a disconnect under the
    /// `cancel-on-disconnect` policy, which IS a cancellation of this
    /// run and maps to `Cancelled` so the engine's existing
    /// cancel short-circuit handles it uniformly. The connection layer
    /// fires the per-run cancel flag independently; this mapping
    /// only governs how the awaiting node's `?` propagates.
    fn from(e: crate::caller::CallerError) -> Self {
        use crate::caller::CallerError;
        match e {
            // A disconnect always means the run is being cancelled (the
            // keep-running policy is a silent `Ok(())`, never this error), so
            // it maps cleanly to `Cancelled` with no action to disambiguate.
            CallerError::Disconnected => WeftError::Cancelled,
            other => WeftError::NodeFailed(other.to_string()),
        }
    }
}

#[cfg(test)]
mod node_err_tests {
    use super::*;

    #[test]
    fn node_err_wraps_with_context_and_passes_ok_through() {
        let ok: Result<u32, std::io::Error> = Ok(7);
        assert_eq!(ok.node_err("reading").unwrap(), 7);

        let err: Result<u32, &str> = Err("boom");
        let e = err.node_err("sending to bridge").unwrap_err();
        assert!(matches!(&e, WeftError::NodeFailed(m) if m == "sending to bridge: boom"), "{e}");
    }

    #[test]
    fn node_bail_and_node_error_fail_with_the_message_verbatim() {
        fn bails() -> WeftResult<()> {
            crate::node_bail!("bridge rejected the send: {}", "no session");
        }
        let e = bails().unwrap_err();
        assert!(
            matches!(&e, WeftError::NodeFailed(m) if m == "bridge rejected the send: no session"),
            "{e}"
        );
        assert!(matches!(node_error("boom"), WeftError::NodeFailed(m) if m == "boom"));
    }

    #[test]
    fn node_err_turns_none_into_a_failure_with_the_message_verbatim() {
        assert_eq!(Some(7).node_err("unused").unwrap(), 7);

        let e = None::<u32>.node_err("no messageId on a media message").unwrap_err();
        assert!(matches!(&e, WeftError::NodeFailed(m) if m == "no messageId on a media message"), "{e}");
    }
}
