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

use sqlx::PgPool;
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
pub async fn mint(pool: &PgPool, mint: &InstanceTokenMint<'_>) -> Result<MintedInstanceToken, MintRefused> {
    use weft_core::signal_token as names;
    let expires_at = names::expiry_at(mint.now_unix, mint.expires_in_secs).map_err(MintRefused::Expiry)?;
    let token = names::generate_token();
    let recognizer = names::recognizer(&token);
    let written = sqlx::query(
        "INSERT INTO signal_token \
         (id, token_hash, recognizer, tenant_id, name, allowed_projects, allowed_tags, \
          allowed_displays, all_displays, created_at, instance_id, expires_at) \
         SELECT $1, $2, $3, $4, $5, ARRAY[$6]::uuid[], $7, $8, $9, $10, $11, $12 \
         WHERE EXISTS (SELECT 1 FROM project WHERE id = $6 AND tenant_id = $4) \
         ON CONFLICT (id) DO UPDATE SET \
             token_hash = EXCLUDED.token_hash, recognizer = EXCLUDED.recognizer, name = EXCLUDED.name, \
             allowed_tags = EXCLUDED.allowed_tags, allowed_displays = EXCLUDED.allowed_displays, \
             all_displays = EXCLUDED.all_displays, created_at = EXCLUDED.created_at, \
             expires_at = EXCLUDED.expires_at \
         WHERE signal_token.tenant_id = EXCLUDED.tenant_id \
           AND signal_token.instance_id = EXCLUDED.instance_id \
           AND signal_token.allowed_projects = EXCLUDED.allowed_projects",
    )
    .bind(mint.id)
    .bind(names::token_hash(&token))
    .bind(&recognizer)
    .bind(mint.tenant)
    .bind(mint.name)
    .bind(mint.project)
    .bind(mint.allowed_tags)
    .bind(mint.allowed_displays)
    .bind(mint.all_displays)
    .bind(mint.now_unix as i64)
    .bind(mint.instance.as_str())
    .bind(expires_at as i64)
    .execute(pool)
    .await
    .map_err(|e| MintRefused::Store(e.into()))?;
    if written.rows_affected() == 1 {
        return Ok(MintedInstanceToken { token, recognizer, created_at: mint.now_unix, expires_at });
    }
    // Nothing written: the project is not the owner's, or the id is
    // another token's. Told apart only now, off the hot path.
    let owned: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM project WHERE id = $1 AND tenant_id = $2)")
        .bind(mint.project)
        .bind(mint.tenant)
        .fetch_one(pool)
        .await
        .map_err(|e| MintRefused::Store(e.into()))?;
    Err(if owned { MintRefused::IdTaken(mint.id) } else { MintRefused::NoProject })
}

/// Revoke instance tokens of `instance` in `project_id`: the one `id`, or
/// every one of them. Answers how many went.
pub async fn revoke(
    pool: &PgPool,
    tenant: &str,
    project_id: uuid::Uuid,
    instance: &InstanceId,
    id: Option<uuid::Uuid>,
) -> anyhow::Result<u64> {
    Ok(sqlx::query(
        "DELETE FROM signal_token \
         WHERE tenant_id = $1 AND instance_id = $2 AND allowed_projects = ARRAY[$3]::uuid[] \
           AND ($4::uuid IS NULL OR id = $4)",
    )
    .bind(tenant)
    .bind(instance.as_str())
    .bind(project_id)
    .bind(id)
    .execute(pool)
    .await?
    .rows_affected())
}

/// How many instance tokens of `project_id` still work (not expired at
/// `now_unix`), per instance, one entry per instance with at least one.
pub async fn counts(
    pool: &PgPool,
    tenant: &str,
    project_id: uuid::Uuid,
    now_unix: i64,
) -> anyhow::Result<Vec<(InstanceId, u32)>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT instance_id, count(*)::bigint FROM signal_token \
         WHERE tenant_id = $1 AND instance_id IS NOT NULL AND allowed_projects = ARRAY[$2]::uuid[] \
           AND expires_at > $3 \
         GROUP BY instance_id ORDER BY instance_id",
    )
    .bind(tenant)
    .bind(project_id)
    .bind(now_unix)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(instance, n)| {
            let instance = InstanceId::new(instance).map_err(|e| anyhow::anyhow!("signal_token.instance_id: {e}"))?;
            Ok((instance, u32::try_from(n)?))
        })
        .collect()
}
