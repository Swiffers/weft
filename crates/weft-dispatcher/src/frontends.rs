//! A project's frontends (`weft frontend add|ls|rm|token`): the websites
//! that call the install for their visitors, each with a caller token of
//! its own, scoped to its project.
//!
//! The install keeps the record and the tokens. Where a frontend runs is
//! the platform's (`weft_platform_traits::FrontendHosting`): on GCP the
//! install makes a Cloud Run service its repository's CI deploys to; on a
//! frontend hosted elsewhere it makes nothing, and the frontend only needs
//! its token and the install's address. A project's frontends go with it.
//!
//! Every change that touches what a repository may do (making or removing
//! a hosted frontend) runs under a lock on that repository, held across
//! the check of who else uses it, the cloud change and the record: two of
//! them at once could otherwise each see the other and leave a repository
//! its access with no record of it, or take it from a frontend still using
//! it.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use weft_task_store::db::{self, Client, FromRow, Pool, Transaction};
use weft_core::frontend::{AddFrontendRequest, AddedFrontend, Frontend, FrontendHost, FrontendWithToken, Repository, VisitorAddressRequest};
use weft_core::signal_token::MintTokenRequest;
use weft_platform_traits::FrontendSite;

use crate::actions::{settings, Actor, Refused};
use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;
use crate::tenant::TenantId;

// SYNC: project_frontend <-> crates/weft-core/src/frontend.rs (Frontend)
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "project_frontend",
    tables: &["project_frontend"],
    ddl: &[r#"CREATE TABLE IF NOT EXISTS project_frontend (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            -- Where it runs: 'cloud_run' (the install made its service) or
            -- 'external'.
            host TEXT NOT NULL CHECK (host IN ('cloud_run', 'external')),
            -- The GitHub repository that deploys a frontend the install
            -- hosts (its name, and the id GitHub gave it, which access is
            -- granted to), its service there, and where visitors reach it.
            repo TEXT,
            repo_id BIGINT,
            service TEXT,
            url TEXT,
            -- The caller token it calls with (`signal_token.id`), and the
            -- ones made to replace it and not put in place yet. NULL for a
            -- hosted frontend until its deploy puts its first in place.
            token_id UUID,
            pending_token_ids UUID[] NOT NULL DEFAULT '{}',
            added_unix BIGINT NOT NULL,
            -- Where its server passes the install's doors on to its
            -- visitors (`https://shop.example/weft`), when it does: the
            -- broker builds a link to the project's stored files on it.
            visitor_address TEXT,
            PRIMARY KEY (project_id, name),
            CHECK ((host = 'cloud_run') = (repo IS NOT NULL)),
            CHECK ((repo IS NULL) = (repo_id IS NULL))
        )"#,
        // One visitor address per project: a link has one way to go.
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_project_frontend_visitor_address
            ON project_frontend(project_id) WHERE visitor_address IS NOT NULL"#],
    seed: &[],
    data_steps: &[],
};

struct Row {
    project_id: uuid::Uuid,
    name: String,
    host: String,
    repo: Option<String>,
    repo_id: Option<i64>,
    service: Option<String>,
    url: Option<String>,
    token_id: Option<uuid::Uuid>,
    pending_token_ids: Vec<uuid::Uuid>,
    visitor_address: Option<String>,
}

impl FromRow for Row {
    fn from_row(row: db::Row) -> db::Result<Self> {
        Ok(Self {
            project_id: row.try_get("project_id")?,
            name: row.try_get("name")?,
            host: row.try_get("host")?,
            repo: row.try_get("repo")?,
            repo_id: row.try_get("repo_id")?,
            service: row.try_get("service")?,
            url: row.try_get("url")?,
            token_id: row.try_get("token_id")?,
            pending_token_ids: row.try_get("pending_token_ids")?,
            visitor_address: row.try_get("visitor_address")?,
        })
    }
}

impl Row {
    fn into_frontend(self) -> anyhow::Result<Frontend> {
        let host = FrontendHost::parse(&self.host)
            .ok_or_else(|| anyhow::anyhow!("frontend '{}' is stored with an unknown host '{}'", self.name, self.host))?;
        let repo = match (self.repo, self.repo_id) {
            (Some(name), Some(id)) => Some(Repository { name, id: id as u64 }),
            (None, None) => None,
            _ => anyhow::bail!("frontend '{}' is stored with half a repository", self.name),
        };
        Ok(Frontend {
            name: self.name,
            project: self.project_id,
            host,
            repo,
            service: self.service,
            url: self.url,
            token_id: self.token_id,
            pending_token_ids: self.pending_token_ids,
            visitor_address: self.visitor_address,
        })
    }
}

const COLUMNS: &str = "project_id, name, host, repo, repo_id, service, url, token_id, pending_token_ids, visitor_address";

/// Every frontend of `project`, by name.
pub async fn list(pool: &Pool, project: uuid::Uuid) -> anyhow::Result<Vec<Frontend>> {
    pool.all::<Row>(&format!("SELECT {COLUMNS} FROM project_frontend WHERE project_id = $1 ORDER BY name"), &[&project])
        .await?
        .into_iter()
        .map(Row::into_frontend)
        .collect()
}

async fn get(db: &impl Client, project: uuid::Uuid, name: &str) -> anyhow::Result<Option<Frontend>> {
    db.opt::<Row>(&format!("SELECT {COLUMNS} FROM project_frontend WHERE project_id = $1 AND name = $2"), &[&project, &name])
        .await?
        .map(Row::into_frontend)
        .transpose()
}

/// Whether a frontend other than `name` of `project` deploys from `repo`,
/// in any project: the repository then keeps its right to deploy.
pub async fn repo_still_used(db: &impl Client, repo: &Repository, project: uuid::Uuid, name: &str) -> anyhow::Result<bool> {
    Ok(db
        .scalar(
            "SELECT EXISTS (SELECT 1 FROM project_frontend WHERE repo_id = $1 AND NOT (project_id = $2 AND name = $3))",
            &[&(repo.id as i64), &project, &name],
        )
        .await?)
}

/// Store `frontend`; `false` when the project has one of that name.
pub async fn record(db: &impl Client, frontend: &Frontend, now_unix: i64) -> anyhow::Result<bool> {
    Ok(db
        .execute(
            "INSERT INTO project_frontend (project_id, name, host, repo, repo_id, service, url, token_id, added_unix, visitor_address) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (project_id, name) DO NOTHING",
            &[
                &frontend.project,
                &frontend.name,
                &frontend.host.as_str(),
                &frontend.repo.as_ref().map(|r| r.name.as_str()),
                &frontend.repo.as_ref().map(|r| r.id as i64),
                &frontend.service,
                &frontend.url,
                &frontend.token_id,
                &now_unix,
                &frontend.visitor_address,
            ],
        )
        .await?
        == 1)
}

/// Forget `name` of `project`.
pub async fn forget(db: &impl Client, project: uuid::Uuid, name: &str) -> anyhow::Result<()> {
    db.execute("DELETE FROM project_frontend WHERE project_id = $1 AND name = $2", &[&project, &name]).await?;
    Ok(())
}

/// Serialize every change to the frontend `name` of `project`, for the
/// transaction: always taken before the repository's lock, so two changes
/// never wait on each other's.
async fn lock_frontend(tx: &Transaction<'_>, project: uuid::Uuid, name: &str) -> anyhow::Result<()> {
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended('frontend:' || $1::text || ':' || $2::text, 0))",
        &[&project.to_string(), &name],
    )
    .await?;
    Ok(())
}

/// Serialize every change to what `repo` may do, for the transaction.
async fn lock_repo(tx: &Transaction<'_>, repo: &Repository) -> anyhow::Result<()> {
    tx.execute("SELECT pg_advisory_xact_lock(hashtextextended('frontend_repo:' || $1::text, 0))", &[&repo.id.to_string()]).await?;
    Ok(())
}

fn internal(e: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

fn not_found(name: &str) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, format!("this project has no frontend named '{name}'"))
}

fn site(frontend: &Frontend) -> Option<FrontendSite> {
    frontend.repo.clone().map(|repo| FrontendSite { project: frontend.project, name: frontend.name.clone(), repo })
}

/// A caller token for `name` of `project`, scoped to that project, minted
/// under `owner`: the tenant that owns the project, which every action on a
/// frontend has already held its actor to (`crate::actions::settings`).
async fn mint_token(
    state: &DispatcherState,
    owner: &TenantId,
    project: uuid::Uuid,
    name: &str,
) -> Result<weft_core::signal_token::MintedToken, (StatusCode, String)> {
    let body = MintTokenRequest { allowed_projects: vec![project], ..MintTokenRequest::caller(format!("frontend {name}")) };
    settings::mint(state, owner, body).await
}

/// Revoke `id`. One already gone is fine: a retry finds it so.
async fn revoke_token(state: &DispatcherState, owner: &TenantId, id: uuid::Uuid) -> anyhow::Result<()> {
    state.tokens.revoke_signal_token(id, owner.as_str()).await?;
    Ok(())
}

/// `POST /projects/{id}/frontends`.
pub async fn add(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(project): Path<uuid::Uuid>,
    Json(req): Json<AddFrontendRequest>,
) -> Result<Json<AddedFrontend>, Refused> {
    Ok(Json(settings::frontend_add(&state, &Actor::operator(caller.0), project, req).await?))
}

/// Make the frontend `req` of `project`, owned by `owner`, and, for one the
/// install hosts, its service; for one running elsewhere, its token (shown
/// this once). A hosted one gets no token here: its deploy workflow puts
/// its first in place (`weft target export` makes it), and one made now
/// would be a working credential nobody holds. A step that fails takes
/// back what the steps before it made: the service and the repository's
/// access (unless another frontend uses it), then the token.
pub(crate) async fn make(
    state: &DispatcherState,
    owner: &TenantId,
    project: uuid::Uuid,
    req: AddFrontendRequest,
) -> Result<AddedFrontend, Refused> {
    let taken = || {
        (
            StatusCode::CONFLICT,
            format!("this project has a frontend named '{}' already; `weft frontend rm {}` removes it", req.name, req.name),
        )
    };
    if get(&state.pg_pool, project, &req.name).await.map_err(internal)?.is_some() {
        return Err(taken());
    }
    let minted = match req.host {
        FrontendHost::CloudRun => None,
        FrontendHost::External => Some(mint_token(state, owner, project, &req.name).await?),
    };
    let mut frontend = Frontend {
        name: req.name.clone(),
        project,
        host: req.host,
        repo: req.repo.clone(),
        service: None,
        url: None,
        token_id: minted.as_ref().map(|minted| minted.id),
        pending_token_ids: Vec::new(),
        visitor_address: req.visitor_address.as_deref().map(|a| a.trim_end_matches('/').to_string()),
    };
    let site = site(&frontend);
    // Whether this add asked the cloud for the site: only then is there
    // anything of its own to take back (a second add of a name that won
    // the race never reaches the cloud).
    let mut opened = false;
    let made: anyhow::Result<bool> = async {
        let mut conn = state.pg_pool.get().await?;
        let tx = conn.transaction().await?;
        lock_frontend(&tx, project, &frontend.name).await?;
        if get(&tx, project, &frontend.name).await?.is_some() {
            return Ok(false);
        }
        if let Some(site) = &site {
            lock_repo(&tx, &site.repo).await?;
            opened = true;
            let hosted = state.frontends.open(site).await?;
            frontend.service = Some(hosted.service);
            frontend.url = Some(hosted.url);
        }
        anyhow::ensure!(record(&tx, &frontend, crate::lease::now_unix()).await?, "the frontend's record was not written");
        tx.commit().await?;
        Ok(true)
    }
    .await;
    let failure = match made {
        Ok(true) => return Ok(AddedFrontend { frontend, token: minted.map(|minted| minted.token) }),
        Ok(false) => taken(),
        Err(e) if visitor_address_taken(&e) => another_visitor_address(state, project).await,
        Err(e) => (StatusCode::BAD_GATEWAY, format!("add frontend '{}': {e:#}", req.name)),
    };
    let (status, mut msg) = failure;
    if let (true, Some(site)) = (opened, &site) {
        if let Err(undo) = take_back_site(state, site).await {
            msg.push_str(&format!(
                "\n(and taking back its service and {}'s access failed: {undo:#}; `weft frontend add {}` again, then \
                 `weft frontend rm {}`, finishes it)",
                site.repo.name, req.name, req.name
            ));
        }
    }
    if let Some(minted) = &minted {
        if let Err(undo) = revoke_token(state, owner, minted.id).await {
            msg.push_str(&format!("\n(and its token {} could not be revoked: {undo:#}; `weft token revoke {}` does it)", minted.id, minted.id));
        }
    }
    Err((status, msg))
}

/// Whether `e` is the project's one visitor address being taken already.
fn visitor_address_taken(e: &anyhow::Error) -> bool {
    e.chain()
        .filter_map(|cause| cause.downcast_ref::<db::Error>())
        .filter_map(db::Error::as_db_error)
        .any(|e| e.constraint() == Some("idx_project_frontend_visitor_address"))
}

/// The refusal of a second visitor address, naming the frontend that has
/// the project's one.
async fn another_visitor_address(state: &DispatcherState, project: uuid::Uuid) -> (StatusCode, String) {
    let holder = list(&state.pg_pool, project)
        .await
        .ok()
        .and_then(|all| all.into_iter().find(|f| f.visitor_address.is_some()))
        .map(|f| format!("frontend '{}' ({})", f.name, f.visitor_address.unwrap_or_default()))
        .unwrap_or_else(|| "another frontend".to_string());
    (
        StatusCode::CONFLICT,
        format!(
            "{holder} already carries this project's visitor address, and a link to a stored file has one way to go; \
             `weft frontend visitor-address <name> off` clears it"
        ),
    )
}

/// `PUT /projects/{id}/frontends/{name}/visitor-address`.
pub async fn set_visitor_address(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((project, name)): Path<(uuid::Uuid, String)>,
    Json(req): Json<VisitorAddressRequest>,
) -> Result<Json<Frontend>, Refused> {
    Ok(Json(settings::frontend_visitor_address(&state, &Actor::operator(caller.0), project, &name, &req).await?))
}

/// Set where the frontend `name` of `project` passes the install's doors
/// on to its visitors, or clear it.
pub(crate) async fn set_visitor(
    state: &DispatcherState,
    project: uuid::Uuid,
    name: &str,
    req: &VisitorAddressRequest,
) -> Result<Frontend, Refused> {
    let address = req.visitor_address.as_deref().map(|a| a.trim_end_matches('/'));
    let set = state
        .pg_pool
        .execute("UPDATE project_frontend SET visitor_address = $3 WHERE project_id = $1 AND name = $2", &[&project, &name, &address])
        .await;
    match set {
        Ok(0) => return Err(not_found(name)),
        Ok(_) => {}
        Err(e) => {
            let e = anyhow::Error::from(e);
            if visitor_address_taken(&e) {
                return Err(another_visitor_address(state, project).await);
            }
            return Err(internal(format!("{e:#}")));
        }
    }
    get(&state.pg_pool, project, name).await.map_err(internal)?.ok_or_else(|| not_found(name))
}

/// Undo a failed `open` of `site`: the service, and the repository's access
/// unless another frontend's record names it.
///
/// The failed add let go of the frontend's lock before this takes it
/// again, so another add of the same name may have opened the same
/// service and recorded its frontend in between. That service is then the
/// other frontend's and stays; when it deploys from another repository,
/// this one's right to deploy to it cannot be taken back without taking
/// the service down, and the error says so.
async fn take_back_site(state: &DispatcherState, site: &FrontendSite) -> anyhow::Result<()> {
    let mut conn = state.pg_pool.get().await?;
    let tx = conn.transaction().await?;
    lock_frontend(&tx, site.project, &site.name).await?;
    if let Some(other) = get(&tx, site.project, &site.name).await?.as_ref().and_then(|f| f.repo.as_ref()) {
        anyhow::ensure!(
            other.id == site.repo.id,
            "another request added frontend '{}' meanwhile, deploying from {}, so its service stays, and {} may \
             still deploy to it",
            site.name,
            other.name,
            site.repo.name
        );
        return Ok(());
    }
    lock_repo(&tx, &site.repo).await?;
    let keep_repo = repo_still_used(&tx, &site.repo, site.project, &site.name).await?;
    state.frontends.close(site, keep_repo).await?;
    tx.commit().await?;
    Ok(())
}

/// `GET /projects/{id}/frontends`.
pub async fn list_route(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(project): Path<uuid::Uuid>,
) -> Result<Json<Vec<Frontend>>, Refused> {
    Ok(Json(settings::frontends_list(&state, &Actor::operator(caller.0), project).await?))
}

/// `POST /projects/{id}/frontends/{name}/token`.
pub async fn new_token(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((project, name)): Path<(uuid::Uuid, String)>,
) -> Result<Json<FrontendWithToken>, Refused> {
    Ok(Json(settings::frontend_token_new(&state, &Actor::operator(caller.0), project, &name).await?))
}

/// A new token for the frontend `name` of `project`, owned by `owner`,
/// shown this once, waiting beside the one it has: that one
/// keeps working until a new one is put in place (`token/{id}/done`), so
/// the site never goes without. Every new token made and not yet in place
/// keeps working too: one may already sit in a repository, waiting for its
/// deploy.
///
/// Made under the frontend's lock: a removal either comes first, and this
/// answers that there is no such frontend, or comes after and revokes
/// this token with the rest.
pub(crate) async fn renew_token(
    state: &DispatcherState,
    owner: &TenantId,
    project: uuid::Uuid,
    name: &str,
) -> Result<FrontendWithToken, Refused> {
    let mut conn = state.pg_pool.get().await.map_err(internal)?;
    let tx = conn.transaction().await.map_err(internal)?;
    lock_frontend(&tx, project, name).await.map_err(internal)?;
    if get(&tx, project, name).await.map_err(internal)?.is_none() {
        return Err(not_found(name));
    }
    let minted = mint_token(state, owner, project, name).await?;
    let recorded: anyhow::Result<Row> = async {
        let row = tx
            .one::<Row>(
                &format!(
                    "UPDATE project_frontend SET pending_token_ids = array_append(pending_token_ids, $3::uuid) \
                     WHERE project_id = $1 AND name = $2 RETURNING {COLUMNS}"
                ),
                &[&project, &name, &minted.id],
            )
            .await?;
        tx.commit().await?;
        Ok(row)
    }
    .await;
    match recorded {
        Ok(row) => Ok(FrontendWithToken {
            frontend: row.into_frontend().map_err(internal)?,
            token: minted.token,
            token_id: minted.id,
        }),
        // The write failed: nothing holds the token.
        Err(e) => {
            let mut msg = format!("record the new token of frontend '{name}': {e:#}");
            if let Err(undo) = revoke_token(state, owner, minted.id).await {
                msg.push_str(&format!("\n(and it could not be revoked: {undo:#}; `weft token revoke {}` does it)", minted.id));
            }
            Err((StatusCode::INTERNAL_SERVER_ERROR, msg))
        }
    }
}

/// `POST /projects/{id}/frontends/{name}/token/{token}/done`.
pub async fn token_done(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((project, name, token)): Path<(uuid::Uuid, String, uuid::Uuid)>,
) -> Result<StatusCode, Refused> {
    settings::frontend_token_done(&state, &Actor::operator(caller.0), project, &name, token).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// The new token `token` of the frontend `name` of `project`, owned by
/// `owner`, is in place. It becomes the frontend's token, and every
/// other one (the one it replaces, and new ones never put in place) stops
/// working. Saying it again of the token already in place does nothing, so
/// a deploy says it every time.
///
/// The retired tokens are revoked before the change commits: a revoke
/// that fails leaves the record as it was, so saying it again retries.
pub(crate) async fn put_token_in_place(
    state: &DispatcherState,
    owner: &TenantId,
    project: uuid::Uuid,
    name: &str,
    token: uuid::Uuid,
) -> Result<(), Refused> {
    let mut conn = state.pg_pool.get().await.map_err(internal)?;
    let tx = conn.transaction().await.map_err(internal)?;
    lock_frontend(&tx, project, name).await.map_err(internal)?;
    let row: Option<(Option<uuid::Uuid>, Vec<uuid::Uuid>)> = tx
        .opt("SELECT token_id, pending_token_ids FROM project_frontend WHERE project_id = $1 AND name = $2", &[&project, &name])
        .await
        .map_err(internal)?;
    let Some((current, pending)) = row else { return Err(not_found(name)) };
    if current == Some(token) {
        return Ok(());
    }
    if !pending.contains(&token) {
        return Err((StatusCode::CONFLICT, format!("{token} is not a new token of frontend '{name}'")));
    }
    tx.execute("UPDATE project_frontend SET token_id = $3, pending_token_ids = '{}' WHERE project_id = $1 AND name = $2", &[&project, &name, &token])
        .await
        .map_err(internal)?;
    let retired: Vec<uuid::Uuid> = current.into_iter().chain(pending).filter(|t| *t != token).collect();
    revoke_all(state, owner, &retired).await.map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(())
}

/// `DELETE /projects/{id}/frontends/{name}/token/{token}`.
pub async fn drop_token(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((project, name, token)): Path<(uuid::Uuid, String, uuid::Uuid)>,
) -> Result<StatusCode, Refused> {
    settings::frontend_token_drop(&state, &Actor::operator(caller.0), project, &name, token).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// The new token `token` of the frontend `name` of `project`, owned by
/// `owner`, will never be put in place (an export that failed); it stops
/// working, and the others stay. Revoked before the change commits, like
/// [`put_token_in_place`]'s, so a revoke that fails can be retried.
pub(crate) async fn drop_new_token(
    state: &DispatcherState,
    owner: &TenantId,
    project: uuid::Uuid,
    name: &str,
    token: uuid::Uuid,
) -> Result<(), Refused> {
    let mut conn = state.pg_pool.get().await.map_err(internal)?;
    let tx = conn.transaction().await.map_err(internal)?;
    lock_frontend(&tx, project, name).await.map_err(internal)?;
    let dropped = tx
        .execute(
            "UPDATE project_frontend SET pending_token_ids = array_remove(pending_token_ids, $3::uuid) \
             WHERE project_id = $1 AND name = $2 AND $3::uuid = ANY(pending_token_ids)",
            &[&project, &name, &token],
        )
        .await
        .map_err(internal)?;
    if dropped == 0 {
        return Err((StatusCode::NOT_FOUND, format!("{token} is not a new token of frontend '{name}'")));
    }
    revoke_token(state, owner, token).await.map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(())
}

/// Revoke every one of `ids`, trying all of them; the error names each one
/// still live.
async fn revoke_all(state: &DispatcherState, owner: &TenantId, ids: &[uuid::Uuid]) -> anyhow::Result<()> {
    let mut failed = Vec::new();
    for id in ids {
        if let Err(e) = revoke_token(state, owner, *id).await {
            failed.push(format!("{id} ({e:#})"));
        }
    }
    anyhow::ensure!(failed.is_empty(), "these tokens still work (`weft token revoke <id>` ends them): {}", failed.join(", "));
    Ok(())
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct RemoveQuery {
    /// Forget the frontend and revoke its tokens even when what the cloud
    /// holds for it cannot be removed; the answer names what is left.
    #[serde(default)]
    pub force: bool,
}

/// `DELETE /projects/{id}/frontends/{name}`.
pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((project, name)): Path<(uuid::Uuid, String)>,
    Query(query): Query<RemoveQuery>,
) -> Result<Json<Vec<String>>, Refused> {
    Ok(Json(settings::frontend_remove(&state, &Actor::operator(caller.0), project, &name, query.force).await?))
}

/// Remove the frontend `name` of `project`, owned by `owner`: its service
/// (and its repository's right to deploy, when no other frontend deploys
/// from it), its tokens, then the record. Answers what `force` left on the
/// cloud.
pub(crate) async fn remove_one(
    state: &DispatcherState,
    owner: &TenantId,
    project: uuid::Uuid,
    name: &str,
    force: bool,
) -> Result<Vec<String>, Refused> {
    let left = take_down(state, owner, project, name, force)
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("remove frontend '{name}': {e:#}")))?
        .ok_or_else(|| not_found(name))?;
    Ok(left.into_iter().collect())
}

/// Remove the frontend `name` of `project`: what the cloud holds, its
/// tokens, then its record. `None` when there is none. With `force`, a
/// cloud removal that fails is answered (what is left, named) instead of
/// stopping the rest. The record is read under the frontend's lock, so a
/// token made meanwhile is revoked with the rest, and it is forgotten only
/// once every token is revoked: a revoke that fails leaves the frontend
/// recorded, so removing it again retries.
async fn take_down(
    state: &DispatcherState,
    owner: &TenantId,
    project: uuid::Uuid,
    name: &str,
    force: bool,
) -> anyhow::Result<Option<Option<String>>> {
    let mut left = None;
    let mut conn = state.pg_pool.get().await?;
    let tx = conn.transaction().await?;
    lock_frontend(&tx, project, name).await?;
    let Some(frontend) = get(&tx, project, name).await? else { return Ok(None) };
    if let Some(site) = site(&frontend) {
        lock_repo(&tx, &site.repo).await?;
        let keep_repo = repo_still_used(&tx, &site.repo, project, name).await?;
        if let Err(e) = state.frontends.close(&site, keep_repo).await {
            if !force {
                return Err(e.context("take down its service (`--force` forgets the frontend anyway)"));
            }
            left = Some(format!(
                "frontend '{name}': its service {} and {}'s access may still be on the cloud ({e:#})",
                frontend.service.as_deref().unwrap_or("(unnamed)"),
                site.repo.name
            ));
        }
    }
    let tokens: Vec<uuid::Uuid> = frontend.token_id.into_iter().chain(frontend.pending_token_ids).collect();
    revoke_all(state, owner, &tokens).await?;
    forget(&tx, project, name).await?;
    tx.commit().await?;
    Ok(Some(left))
}

/// Take down every frontend of a project being removed, before anything
/// else of it goes, so a frontend that cannot be removed stops the removal
/// with the rest still whole. With `force`, what is left on the cloud is
/// answered instead.
pub async fn remove_project(
    state: &DispatcherState,
    owner: &str,
    project: uuid::Uuid,
    force: bool,
) -> anyhow::Result<Vec<String>> {
    let owner = TenantId(owner.to_string());
    let mut left = Vec::new();
    for frontend in list(&state.pg_pool, project).await? {
        left.extend(take_down(state, &owner, project, &frontend.name, force).await?.flatten());
    }
    Ok(left)
}
