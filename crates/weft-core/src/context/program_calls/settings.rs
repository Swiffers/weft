//! `ctx.frontends()`, `ctx.workers()` and `ctx.picks()`: a project's
//! settings on the install, the verbs of `weft frontend`, `weft workers`
//! and `weft connect`'s picks.

use crate::error::{WeftError, WeftResult};
use crate::frontend::{AddFrontendRequest, AddedFrontend, Frontend, FrontendHost, Repository, VisitorAddressRequest};
use crate::instance_door::ValuesChanged;
use crate::picks::{ChangePicks, MovePicks, PickInput, Picks, PicksMoved};
use crate::program::settings::SettingsCall;
use crate::program::ProgramCall;
use crate::run_spec::InstanceFieldRef;
use crate::tag::StopSelf;
use crate::workers::{WorkerOverrides, WorkersResponse};

use super::ProjectCalls;

impl<'a> ProjectCalls<'a> {
    /// The project's frontends: the websites that call the install for
    /// their visitors, each with a caller token of its own.
    pub fn frontends(self) -> FrontendCalls<'a> {
        FrontendCalls { at: self }
    }

    /// The project's own worker levers (copies kept warm, the most
    /// copies, runs per copy, CPU, memory); what it leaves unset follows
    /// the install.
    pub fn workers(self) -> WorkerCalls<'a> {
        WorkerCalls { at: self }
    }

    /// The connections the project's own access nodes use on this
    /// install, by place and field, never written in the source.
    pub fn picks(self) -> PickCalls<'a> {
        PickCalls { at: self }
    }
}

async fn settings<T: serde::de::DeserializeOwned>(at: ProjectCalls<'_>, call: SettingsCall) -> WeftResult<T> {
    at.call(ProgramCall::Settings(call), StopSelf::Keep).await
}

/// See [`ProjectCalls::frontends`].
pub struct FrontendCalls<'a> {
    at: ProjectCalls<'a>,
}

impl<'a> FrontendCalls<'a> {
    /// Every frontend of the project, by name, and where each runs.
    pub async fn list(self) -> WeftResult<Vec<Frontend>> {
        settings(self.at, SettingsCall::FrontendsList).await
    }

    /// Make a frontend the install hosts, `name` (lower-case letters,
    /// digits and hyphens, at most 20), deployed by the GitHub repository
    /// `repo` (`weft frontend add <name> --repo`): the install makes its
    /// service and lets that repository deploy to it, and the deploy
    /// workflow gets its token (`weft target export`). A frontend that runs
    /// elsewhere is made with `weft frontend add` instead: its token is in
    /// the answer, and a program's answers are kept.
    pub fn add(self, name: impl Into<String>, repo: Repository) -> FrontendAdd<'a> {
        FrontendAdd { at: self.at, name: name.into(), repo, visitor_address: None }
    }

    /// One frontend of the project, by name.
    pub fn frontend(self, name: impl Into<String>) -> OneFrontend<'a> {
        OneFrontend { at: self.at, name: name.into() }
    }
}

/// A frontend about to be made. See [`FrontendCalls::add`].
pub struct FrontendAdd<'a> {
    at: ProjectCalls<'a>,
    name: String,
    repo: Repository,
    visitor_address: Option<String>,
}

impl FrontendAdd<'_> {
    /// Where its server passes weft's doors on to its visitors (its
    /// `weftPassThrough` mount, `https://shop.example/weft`): links to the
    /// project's stored files then go through the site.
    pub fn visitor_address(mut self, url: impl Into<String>) -> Self {
        self.visitor_address = Some(url.into());
        self
    }

    /// Make it. Answers once its service is ready.
    pub async fn apply(self) -> WeftResult<Frontend> {
        let request = AddFrontendRequest {
            name: self.name,
            host: FrontendHost::CloudRun,
            repo: Some(self.repo),
            visitor_address: self.visitor_address,
        };
        request.validate().map_err(WeftError::Input)?;
        let added: AddedFrontend = settings(self.at, SettingsCall::FrontendAdd { request }).await?;
        Ok(added.frontend)
    }
}

/// One frontend. See [`FrontendCalls::frontend`].
pub struct OneFrontend<'a> {
    at: ProjectCalls<'a>,
    name: String,
}

impl OneFrontend<'_> {
    /// Remove it: its tokens stop working, and a service the install made
    /// for it is deleted. With `force`, it is forgotten even when its
    /// service cannot be removed, and the answer names what stays on the
    /// cloud.
    pub async fn remove(self, force: bool) -> WeftResult<Vec<String>> {
        settings(self.at, SettingsCall::FrontendRemove { name: self.name, force }).await
    }

    /// Set where its server passes weft's doors on to its visitors, or
    /// clear it with `None`. One frontend of a project has one at most.
    pub async fn set_visitor_address(self, url: Option<String>) -> WeftResult<Frontend> {
        if let Some(url) = &url {
            crate::frontend::check_visitor_address(url).map_err(WeftError::Input)?;
        }
        let request = VisitorAddressRequest { visitor_address: url };
        settings(self.at, SettingsCall::FrontendVisitorAddress { name: self.name, request }).await
    }

    /// Its new token `token` is in place (`weft frontend token <name>
    /// --done <id>`): every other token of it stops working. Saying it
    /// again of the token in place does nothing.
    pub async fn token_done(self, token: uuid::Uuid) -> WeftResult<()> {
        settings(self.at, SettingsCall::FrontendTokenDone { name: self.name, token }).await
    }

    /// Its new token `token` will never be put in place: it stops
    /// working, and the others stay.
    pub async fn token_drop(self, token: uuid::Uuid) -> WeftResult<()> {
        settings(self.at, SettingsCall::FrontendTokenDrop { name: self.name, token }).await
    }
}

/// See [`ProjectCalls::workers`].
pub struct WorkerCalls<'a> {
    at: ProjectCalls<'a>,
}

impl WorkerCalls<'_> {
    /// The project's levers, the install's, and what its workers run with.
    pub async fn get(self) -> WeftResult<WorkersResponse> {
        settings(self.at, SettingsCall::WorkersGet).await
    }

    /// Set the levers `levers` sets, keeping the ones the project already
    /// sets (`weft workers set`). A change applies to the running workers
    /// at once; a shape no platform could run is refused, naming the lever.
    pub async fn set(self, levers: WorkerOverrides) -> WeftResult<WorkersResponse> {
        let current = self.at.workers().get().await?;
        self.put(current.project.merged(&levers)).await
    }

    /// Put the levers named in `levers` back on the install's, or every
    /// lever when it is empty (`weft workers reset`).
    pub async fn reset(self, levers: &[&str]) -> WeftResult<WorkersResponse> {
        let current = self.at.workers().get().await?;
        let mut project = if levers.is_empty() { WorkerOverrides::default() } else { current.project };
        for lever in levers {
            project.unset(lever).map_err(WeftError::Input)?;
        }
        self.put(project).await
    }

    /// Replace the project's levers with `levers`: an unset one follows the
    /// install again.
    pub async fn put(self, levers: WorkerOverrides) -> WeftResult<WorkersResponse> {
        settings(self.at, SettingsCall::WorkersPut { levers }).await
    }
}

/// See [`ProjectCalls::picks`].
pub struct PickCalls<'a> {
    at: ProjectCalls<'a>,
}

impl<'a> PickCalls<'a> {
    /// Every pick the install keeps for the project, by place and field,
    /// each as its `{id, identity}` handle.
    pub async fn list(self) -> WeftResult<Picks> {
        settings(self.at, SettingsCall::PicksList).await
    }

    /// Pick the connection `connection` (one of the owner's, to `service`)
    /// for `field` of the node `node`; chain more, then [`PickChange::apply`].
    pub fn set(self, node: impl Into<String>, field: impl Into<String>, connection: uuid::Uuid, service: impl Into<String>) -> PickChange<'a> {
        PickChange { at: self.at, change: ChangePicks::default() }.set(node, field, connection, service)
    }

    /// Forget the pick for `field` of the node `node`; chain more, then
    /// [`PickChange::apply`].
    pub fn clear(self, node: impl Into<String>, field: impl Into<String>) -> PickChange<'a> {
        PickChange { at: self.at, change: ChangePicks::default() }.clear(node, field)
    }

    /// Carry everything the install keeps at the place `from` (its picks
    /// and every instance's values) to the place `to`, after a node moved in
    /// the source (`weft connect --move`). Never merges: `to` has nothing
    /// stored yet.
    pub async fn move_place(self, from: impl Into<String>, to: impl Into<String>) -> WeftResult<PicksMoved> {
        settings(self.at, SettingsCall::PicksMove { request: MovePicks { from: from.into(), to: to.into() } }).await
    }
}

/// A change of picks, made all or none by [`Self::apply`].
pub struct PickChange<'a> {
    at: ProjectCalls<'a>,
    change: ChangePicks,
}

impl PickChange<'_> {
    /// See [`PickCalls::set`].
    pub fn set(mut self, node: impl Into<String>, field: impl Into<String>, connection: uuid::Uuid, service: impl Into<String>) -> Self {
        self.change.set.push(PickInput { node: node.into(), field: field.into(), connection, service: service.into() });
        self
    }

    /// See [`PickCalls::clear`].
    pub fn clear(mut self, node: impl Into<String>, field: impl Into<String>) -> Self {
        self.change.clear.push(InstanceFieldRef { node: node.into(), field: field.into() });
        self
    }

    /// Make the change, all or none; the live triggers that read a changed
    /// pick are set up again before this returns. Answers those triggers.
    pub async fn apply(self) -> WeftResult<Vec<String>> {
        let answer: ValuesChanged = settings(self.at, SettingsCall::PicksChange { request: self.change }).await?;
        Ok(answer.rearmed)
    }
}
