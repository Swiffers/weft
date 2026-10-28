//! Instance tokens: a credential that acts inside one instance of one
//! project and nothing else, always with an expiry, meant for a browser or
//! an extension working with that instance. Rows of the dispatcher's
//! `signal_token` table with `instance_id` set and `allowed_projects`
//! holding exactly that one project.
//!
//! Minted two ways and stored by one function ([`mint`]): by a person
//! (`weft token mint --instance`, through the dispatcher) and by a program
//! (`ctx.tokens().mint_for_instance`, through the broker, so the value
//! never lands in a task row or a journal). The value leaves once, in the
//! answer; the row keeps only its hash.

use weft_task_store::db::{Client as _, Pool};
use weft_core::instance::InstanceId;

/// One instance token to make.
pub struct InstanceTokenMint<'a> {
    /// Its id. One that already names this instance's token in this
    /// project REPLACES it (a new value, the old one dead): a replayed run
    /// mints under the id it journaled.
    pub id: uuid::Uuid,
    /// The owner it is minted under: the project must be one of its.
    pub tenant: &'a str,
    pub project: uuid::Uuid,
    pub instance: &'a InstanceId,
    pub name: Option<&'a str>,
    /// Signals it reaches, by tag (none: every one of the project's).
    pub allowed_tags: &'a [String],
    /// Displays it reads, in the form the dispatcher stores a display
    /// grant; `all_displays` reads every one of the instance's own copies
    /// (a bridge's pairing code).
    pub allowed_displays: &'a [String],
    pub all_displays: bool,
    /// How long it works, in seconds; a year at most
    /// (`weft_core::signal_token::expiry_at`).
    pub expires_in_secs: u64,
    /// The mint's clock, unix seconds.
    pub now_unix: u64,
}

/// An instance token made: its value, shown this once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintedInstanceToken {
    pub token: String,
    pub recognizer: String,
    pub created_at: u64,
    pub expires_at: u64,
}

/// Why an instance token was not made.
#[derive(Debug, thiserror::Error)]
pub enum MintRefused {
    /// The expiry is outside what a token may live.
    #[error("{0}")]
    Expiry(String),
    /// The project is not one of the owner's (or is gone): one answer for
    /// both, so nobody probes which ids exist elsewhere.
    #[error("no project under that id belongs to this owner")]
    NoProject,
    /// The id names a token of another instance, another project or
    /// another owner, and nothing was written.
    #[error("token id {0} already names another token")]
    IdTaken(uuid::Uuid),
    #[error("mint an instance token: {0:#}")]
    Store(anyhow::Error),
}

impl MintRefused {
    /// The HTTP status a surface answers it with.
    pub fn status(&self) -> u16 {
        match self {
            MintRefused::Expiry(_) => 400,
            MintRefused::NoProject => 404,
            MintRefused::IdTaken(_) => 409,
            MintRefused::Store(_) => 500,
        }
    }
}

/// Make the instance token `mint` asks for. The row is written only while
/// its project is one of the owner's, in the same statement, so a token
/// can never outlive a removal it raced (the removal revokes a project's
/// instance tokens).
pub async fn mint(pool: &Pool, mint: &InstanceTokenMint<'_>) -> Result<MintedInstanceToken, MintRefused> {
    use weft_core::signal_token as names;
    let expires_at = names::expiry_at(mint.now_unix, mint.expires_in_secs).map_err(MintRefused::Expiry)?;
    let token = names::generate_token();
    let recognizer = names::recognizer(&token);
    // Each value is cast: a `$n` in the SELECT list of an INSERT has no
    // type of its own to be prepared with.
    let written = pool
        .execute(
            "INSERT INTO signal_token \
             (id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
              allowed_displays, all_displays, created_at, instance_id, expires_at) \
             SELECT $1::uuid, $2::text, $3::text, $4::text, $5::text, ARRAY[$6::uuid], $7::text[], $8::text[], $9::boolean, \
                 $10::bigint, $11::text, $12::bigint \
             WHERE EXISTS (SELECT 1 FROM project WHERE id = $6 AND tenant_id = $4) \
             ON CONFLICT (id) DO UPDATE SET \
                 token_hash = EXCLUDED.token_hash, recognizer = EXCLUDED.recognizer, name = EXCLUDED.name, \
                 allowed_tags = EXCLUDED.allowed_tags, allowed_displays = EXCLUDED.allowed_displays, \
                 all_displays = EXCLUDED.all_displays, created_at = EXCLUDED.created_at, \
                 expires_at = EXCLUDED.expires_at \
             WHERE signal_token.tenant_id = EXCLUDED.tenant_id \
               AND signal_token.instance_id = EXCLUDED.instance_id \
               AND signal_token.allowed_projects = EXCLUDED.allowed_projects",
            &[
                &mint.id,
                &names::token_hash(&token),
                &recognizer,
                &mint.tenant,
                &mint.name,
                &mint.project,
                &mint.allowed_tags,
                &mint.allowed_displays,
                &mint.all_displays,
                &(mint.now_unix as i64),
                &mint.instance.as_str(),
                &(expires_at as i64),
            ],
        )
        .await
        .map_err(|e| MintRefused::Store(e.into()))?;
    if written == 1 {
        return Ok(MintedInstanceToken { token, recognizer, created_at: mint.now_unix, expires_at });
    }
    // Nothing written: the project is not the owner's, or the id is
    // another token's. Told apart only now, off the hot path.
    let owned: bool = pool
        .scalar("SELECT EXISTS (SELECT 1 FROM project WHERE id = $1 AND tenant_id = $2)", &[&mint.project, &mint.tenant])
        .await
        .map_err(|e| MintRefused::Store(e.into()))?;
    Err(if owned { MintRefused::IdTaken(mint.id) } else { MintRefused::NoProject })
}

/// Revoke instance tokens of `instance` in `project_id`: the one `id`, or
/// every one of them. Answers how many went.
pub async fn revoke(
    pool: &Pool,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &InstanceId,
    id: Option<uuid::Uuid>,
) -> anyhow::Result<u64> {
    Ok(pool
        .execute(
            "DELETE FROM signal_token \
             WHERE tenant_id = $1 AND instance_id = $2 AND allowed_projects = ARRAY[$3::uuid] \
               AND ($4::uuid IS NULL OR id = $4)",
            &[&tenant, &instance.as_str(), &project_id, &id],
        )
        .await?)
}

/// How many instance tokens of `project_id` still work (not expired at
/// `now_unix`), per instance, one entry per instance with at least one.
pub async fn counts(
    pool: &Pool,
    tenant: &str,
    project_id: uuid::Uuid,
    now_unix: i64,
) -> anyhow::Result<Vec<(InstanceId, u32)>> {
    let rows: Vec<(String, i64)> = pool
        .all(
            "SELECT instance_id, count(*)::bigint FROM signal_token \
             WHERE tenant_id = $1 AND instance_id IS NOT NULL AND allowed_projects = ARRAY[$2::uuid] \
               AND expires_at > $3 \
             GROUP BY instance_id ORDER BY instance_id",
            &[&tenant, &project_id, &now_unix],
        )
        .await?;
    rows.into_iter()
        .map(|(instance, n)| {
            let instance = InstanceId::new(instance).map_err(|e| anyhow::anyhow!("signal_token.instance_id: {e}"))?;
            Ok((instance, u32::try_from(n)?))
        })
        .collect()
}
