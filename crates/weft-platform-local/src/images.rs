//! Images on the local Docker daemon: built with `docker build`, kept in
//! the daemon's own image store (no registry; the containers the runner
//! and the infra host start use them straight from there).
//!
//! A build runs in this process. After a restart, a build it was running
//! is gone ([`BuildStatus::Gone`]), and the ledger lets the next caller
//! start it again.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use weft_platform_traits::{BuildHandle, BuildRequest, BuildStatus, ImageBuilder, ImageDeleted};

use crate::docker::{self, Docker};

/// How many of a failed build's last output lines its failure keeps.
const FAILURE_LOG_LINES: usize = 60;


pub struct DockerImageBuilder {
    docker: Arc<dyn Docker>,
    /// The install whose names the images go under
    /// (`Install::local_image_ref`).
    install: weft_core::infra::Install,
    /// The install's folder, whose image record a build adds what it uses
    /// to (`weft_core::images::ImageRefs::add_build`).
    install_dir: std::path::PathBuf,
    builds: Arc<parking_lot::Mutex<HashMap<String, Build>>>,
}

enum Build {
    Running(tokio::task::AbortHandle),
    Ended(BuildStatus),
}

impl DockerImageBuilder {
    pub fn new(docker: Arc<dyn Docker>, install: weft_core::infra::Install, install_dir: std::path::PathBuf) -> Self {
        Self { docker, install, install_dir, builds: Arc::default() }
    }
}

/// Give `image_ref` to an image another install already holds under the
/// same content-addressed tag (`Install::tag_of_local_image`), so a
/// content one install built is never built again by the next. `false`
/// when no install holds it, or the one that did removed its name before
/// the tag landed: the caller builds it then.
async fn adopt(docker: &dyn Docker, image_ref: &str) -> anyhow::Result<bool> {
    let Some(tag) = weft_core::infra::Install::tag_of_local_image(image_ref) else {
        return Ok(false);
    };
    let listing = docker::run(
        docker,
        vec![
            "image".into(),
            "ls".into(),
            "--format".into(),
            "{{.Repository}}:{{.Tag}}".into(),
            "--filter".into(),
            format!("reference={tag}"),
            "--filter".into(),
            format!("reference=localhost/*/{tag}"),
        ],
    )
    .await?;
    for held in listing.lines().map(str::trim) {
        if held == image_ref || weft_core::infra::Install::tag_of_local_image(held) != Some(tag) {
            continue;
        }
        if docker.exec(&["tag".into(), held.into(), image_ref.into()]).await?.success() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `docker build` for `req`.
fn build_args(req: &BuildRequest) -> Vec<String> {
    let mut args = vec!["build".to_string(), "--tag".into(), req.image_ref.clone()];
    for (k, v) in &req.build_args {
        args.extend(["--build-arg".into(), format!("{k}={v}")]);
    }
    args.extend(["--file".into(), req.context_dir.join("Dockerfile").display().to_string()]);
    args.push(req.context_dir.display().to_string());
    args
}

/// How a build that produced `image_ref` ended: `result_file`'s bytes read
/// out of the image when the request named one, without running it
/// (`docker create` makes a container that never starts, `docker cp`
/// copies the file out of it, `docker rm` removes it). A named file the
/// image lacks fails the build: the request said the image carries it.
async fn built(docker: &dyn Docker, install: &str, image_ref: &str, result_file: Option<&str>) -> BuildStatus {
    let Some(path) = result_file else { return BuildStatus::Succeeded { result: None } };
    match read_from_image(docker, install, image_ref, path).await {
        Ok(bytes) => BuildStatus::Succeeded { result: Some(bytes) },
        Err(e) => BuildStatus::Failed { reason: format!("the build produced {image_ref}, but its result {path} could not be read from it: {e:#}") },
    }
}

/// The bytes of `path` inside `image_ref`. The copy lands in a temp file
/// first: `docker cp` to stdout is a tar stream, and the `Docker` trait
/// hands output back as text.
///
/// The container is labelled as the image's result reader
/// (`docker::roles::BUILD_RESULT`), so one a crash leaves between the
/// create and the remove is found and removed with its image
/// (`delete_image`) instead of keeping it in use for good.
async fn read_from_image(docker: &dyn Docker, install: &str, image_ref: &str, path: &str) -> anyhow::Result<Vec<u8>> {
    let mut create = vec!["create".to_string()];
    create.extend(docker::label_args(&std::collections::BTreeMap::from([
        (docker::labels::INSTALL, install.to_string()),
        (docker::labels::ROLE, docker::roles::BUILD_RESULT.to_string()),
        (docker::labels::IMAGE, image_ref.to_string()),
    ])));
    // The command is never run; it is there so an image that names none
    // of its own (`FROM scratch` and a file) can still be created.
    create.extend([image_ref.to_string(), "weft-never-run".to_string()]);
    let id = docker::run(docker, create).await?.trim().to_string();
    let copy = std::env::temp_dir().join(format!("weft-build-result-{}", uuid::Uuid::new_v4().simple()));
    let read = async {
        docker::run(docker, vec!["cp".into(), format!("{id}:{path}"), copy.display().to_string()]).await?;
        tokio::fs::read(&copy).await.map_err(|e| anyhow::anyhow!("read the copy of {path} at {}: {e}", copy.display()))
    }
    .await;
    // Whatever the copy did: a leftover container would keep the image in
    // use. Removing it failing never hides how the read went.
    if let Err(e) = docker::remove_containers(docker, std::slice::from_ref(&id)).await {
        tracing::warn!(target: "weft_platform_local::images", container = %id, error = %format!("{e:#}"), "could not remove the container a build result was read from");
    }
    if let Err(e) = tokio::fs::remove_file(&copy).await {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(target: "weft_platform_local::images", file = %copy.display(), error = %e, "could not remove the copy of a build result");
        }
    }
    read
}

/// Where this install keeps the log of the build of `image_ref`: one file
/// per image, which lives as long as the install holds the image
/// (`delete_image` removes it), so a refusal naming it a month later
/// still points at a file. The folder is made when missing.
async fn build_log(install_dir: &std::path::Path, image_ref: &str) -> anyhow::Result<std::path::PathBuf> {
    let dir = install_dir.join("build-logs");
    tokio::fs::create_dir_all(&dir).await.map_err(|e| anyhow::anyhow!("create {}: {e}", dir.display()))?;
    Ok(dir.join(format!("{}.log", image_ref.replace(['/', ':'], "_"))))
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[async_trait]
impl ImageBuilder for DockerImageBuilder {
    /// The install's own name for the image (`Install::local_image_ref`).
    fn image_ref(&self, tag: &str) -> String {
        self.install.local_image_ref(tag)
    }

    /// Takes the image another install on this daemon already holds under
    /// the same tag when there is one (a name added, nothing built), and
    /// runs `docker build` otherwise.
    async fn start(&self, req: BuildRequest) -> anyhow::Result<BuildHandle> {
        // Before anything is built or adopted: from here on a machine-wide
        // sweep elsewhere keeps what this build uses (one already running
        // holds the record, so this waits until it is done). The compile
        // cache key follows the live checkout, so it can differ from the one
        // the install's start recorded; the builder base is the one the
        // start chose, added again at no cost.
        let (dir, uses) = (self.install_dir.clone(), req.uses.clone());
        tokio::task::spawn_blocking(move || {
            weft_core::images::ImageRefs::add_build(&dir, &uses, || {
                tracing::info!(target: "weft_platform_local::images", "a build waits for a cleanup to let go of the install's image record")
            })
        })
            .await
            .map_err(|e| anyhow::anyhow!("record the images a build uses: {e}"))??;
        let name = req.name.clone();
        let image_ref = req.image_ref.clone();
        let docker = self.docker.clone();
        let builds = self.builds.clone();
        let task_name = name.clone();
        let result_file = req.result_file.clone();
        // An image another install on this daemon holds is taken as it is,
        // with nothing built, and its staging let go of at once. Its result
        // is read from it as a build's would be: the same content carries
        // the same file.
        let install = self.install.label_value().to_string();
        if adopt(docker.as_ref(), &image_ref).await? {
            let status = built(docker.as_ref(), &install, &image_ref, result_file.as_deref()).await;
            self.builds.lock().insert(name.clone(), Build::Ended(status));
            return Ok(BuildHandle::named(name));
        }
        let log = build_log(&self.install_dir, &image_ref).await?;
        // Said at once, since the address is handed out now: the whole
        // output replaces it when the build ends.
        tokio::fs::write(&log, "the build is running; its output lands here when it ends\n")
            .await
            .map_err(|e| anyhow::anyhow!("write {}: {e}", log.display()))?;
        let args = build_args(&req);
        // `docker build` reads the context while it runs, after the
        // request that staged it answered: the build task keeps the
        // staging until docker is done with it, and an abort (`release`)
        // drops it with the task; either way the directory goes on a
        // blocking thread (`Staging`).
        let staging = req.staging;
        // Held until the build is registered, so a fast build never
        // records its end before its start.
        let mut table = self.builds.lock();
        let task_log = log.clone();
        let task = tokio::spawn(async move {
            let out = docker.exec(&args).await;
            drop(staging);
            // The whole output is the build's log (`BuildHandle::log_url`):
            // where a person reads every error of a package the worker
            // left out, whose record keeps only the first line.
            let text = match &out {
                Ok(out) => format!("{}\n{}", out.stdout, out.stderr),
                Err(e) => format!("docker build could not run: {e:#}\n"),
            };
            if let Err(e) = tokio::fs::write(&task_log, text).await {
                tracing::warn!(target: "weft_platform_local::images", log = %task_log.display(), error = %e, "could not keep a build's log");
            }
            let status = match out {
                Ok(out) if out.success() => built(docker.as_ref(), &install, &image_ref, result_file.as_deref()).await,
                Ok(out) => BuildStatus::Failed { reason: tail(&format!("{}\n{}", out.stdout, out.stderr), FAILURE_LOG_LINES) },
                Err(e) => BuildStatus::Failed { reason: format!("{e:#}") },
            };
            if let Some(entry) = builds.lock().get_mut(&task_name) {
                *entry = Build::Ended(status);
            }
        });
        table.insert(name.clone(), Build::Running(task.abort_handle()));
        Ok(BuildHandle { external_build_id: name, log_url: Some(log.display().to_string()) })
    }

    async fn poll(&self, handle: &BuildHandle) -> anyhow::Result<BuildStatus> {
        Ok(match self.builds.lock().get(&handle.external_build_id) {
            Some(Build::Running(_)) => BuildStatus::Pending,
            Some(Build::Ended(status)) => status.clone(),
            None => BuildStatus::Gone,
        })
    }

    async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool> {
        Ok(self.docker.exec(&["image".into(), "inspect".into(), image_ref.into()]).await?.success())
    }

    async fn release(&self, handle: &BuildHandle) {
        if let Some(Build::Running(task)) = self.builds.lock().remove(&handle.external_build_id) {
            task.abort();
        }
    }

    /// Removes this install's name for the image (`docker image rm` of
    /// that name). Docker deletes the image itself only with its last
    /// name, so an image another install still holds under its own name
    /// stays for that install.
    async fn delete_image(&self, image_ref: &str) -> anyhow::Result<ImageDeleted> {
        // A result reader a crash left behind holds the image in use and
        // nothing else will ever remove it (`read_from_image`).
        let readers: Vec<String> = docker::containers(
            self.docker.as_ref(),
            self.install.label_value(),
            &[(docker::labels::ROLE, docker::roles::BUILD_RESULT), (docker::labels::IMAGE, image_ref)],
        )
        .await?
        .into_iter()
        .map(|row| row.name)
        .collect();
        docker::remove_containers(self.docker.as_ref(), &readers).await?;
        let args = vec!["image".to_string(), "rm".into(), image_ref.into()];
        let out = self.docker.exec(&args).await?;
        let gone = out.success() || out.stderr.contains("No such image");
        if gone {
            // Its log goes with the install's name for it (`build_log`).
            let log = build_log(&self.install_dir, image_ref).await?;
            if let Err(e) = tokio::fs::remove_file(&log).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(target: "weft_platform_local::images", log = %log.display(), error = %e, "could not remove a deleted image's build log");
                }
            }
        }
        if out.success() {
            Ok(ImageDeleted::Deleted)
        } else if out.stderr.contains("No such image") {
            Ok(ImageDeleted::Absent)
        } else if out.stderr.contains("is using its referenced image") {
            Ok(ImageDeleted::InUse)
        } else {
            docker::DockerOutput::ok(out, &args).map(|_| ImageDeleted::Deleted)
        }
    }
}

/// The most BuildKit's cache may hold. Over it, the least recently used
/// records go first until it fits, so what the last builds used (the
/// current builder base's layers, the compile caches) is the last to go.
// SYNC: the cap and the prune flags <-> setup.sh (the "bound BuildKit cache" prune)
pub const BUILD_CACHE_CAP: &str = "20GB";

/// `docker builder prune` holding BuildKit's cache to [`BUILD_CACHE_CAP`].
///
/// `--all`, because without it BuildKit only considers records it does not
/// count as shared or internal, and on a machine that has built many
/// versions of weft that left 2.4GB of 235GB reclaimable in place. No
/// `--filter until=`: that filter makes every record used inside its
/// window ineligible whatever the cap says, so a busy day of builds grew
/// the cache past the cap with nothing allowed to free it (measured: 221GB
/// freed without it, 20GB with it). A record a build is using right now is
/// never taken either way.
fn build_cache_bound_args() -> Vec<String> {
    ["builder", "prune", "--force", "--all", "--max-used-space", BUILD_CACHE_CAP].map(String::from).to_vec()
}

/// Keep BuildKit's cache under [`BUILD_CACHE_CAP`], least recently used
/// first. The same bound `setup.sh` applies, applied while the install
/// runs, since every project build adds to the cache between installs.
pub async fn bound_build_cache(docker: &dyn Docker) -> anyhow::Result<()> {
    docker::run(docker, build_cache_bound_args()).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docker::fake::FakeDocker;
    use weft_platform_traits::Staging;

    /// A staged directory, removed when dropped, as the dispatcher's
    /// staging temp dir is.
    struct Staged(std::path::PathBuf);

    impl Drop for Staged {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    /// A request for a staged context (a Dockerfile and a file it copies)
    /// that the request holds only through its staging, as a build request
    /// that answered does.
    fn req(name: &str) -> BuildRequest {
        let dir = std::env::temp_dir().join(format!("weft-staged-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").unwrap();
        BuildRequest {
            name: name.into(),
            project_id: uuid::Uuid::from_u128(1),
            tenant: "local".into(),
            context_dir: dir.clone(),
            staging: Staging::new(Staged(dir)),
            image_ref: "weft-worker:abc".into(),
            build_args: vec![("WEFT_COMPILE_LANE".into(), "2".into())],
            uses: Default::default(),
            result_file: None,
        }
    }

    async fn settled(b: &DockerImageBuilder, h: &BuildHandle) -> BuildStatus {
        loop {
            match b.poll(h).await.unwrap() {
                BuildStatus::Pending => tokio::task::yield_now().await,
                other => return other,
            }
        }
    }

    /// Wait for the staged directory to go, which happens on a blocking
    /// thread once the last holder lets go.
    async fn gone(dir: &std::path::Path) {
        for _ in 0..500 {
            if !dir.exists() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the staging {} was never let go of", dir.display());
    }

    /// A Docker whose `docker build` runs until the test lets it end.
    struct HeldBuilds {
        docker: FakeDocker,
        ends: tokio::sync::Semaphore,
    }

    #[async_trait]
    impl Docker for HeldBuilds {
        async fn exec(&self, args: &[String]) -> anyhow::Result<docker::DockerOutput> {
            let out = self.docker.exec(args).await;
            if args.first().is_some_and(|verb| verb == "build") {
                self.ends.acquire().await.unwrap().forget();
            }
            out
        }
    }

    /// `docker build` reads the staged context itself, untouched, and the
    /// build keeps it on disk while it runs, letting go once it ends.
    #[tokio::test]
    async fn a_build_runs_docker_build_on_the_staging_it_keeps() {
        let docker = Arc::new(HeldBuilds { docker: FakeDocker::new(), ends: tokio::sync::Semaphore::new(0) });
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        let first = req("b1");
        let dir = first.context_dir.clone();
        let h = b.start(first).await.unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Pending);
        assert!(dir.join("src/main.rs").exists(), "the staging stays while the build runs");
        let call = &docker.docker.calls_to("build")[0];
        assert_eq!(
            call,
            &["build", "--tag", "weft-worker:abc", "--build-arg", "WEFT_COMPILE_LANE=2", "--file"]
                .into_iter()
                .map(String::from)
                .chain([dir.join("Dockerfile").display().to_string(), dir.display().to_string()])
                .collect::<Vec<_>>(),
            "built from the staged context itself"
        );
        docker.ends.add_permits(1);
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: None });
        gone(&dir).await;

        docker.docker.fail(&["build"], "error: cargo failed");
        docker.ends.add_permits(1);
        let h = b.start(req("b2")).await.unwrap();
        assert!(matches!(settled(&b, &h).await, BuildStatus::Failed { reason } if reason.contains("cargo failed")));

        b.release(&h).await;
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Gone, "a released build is gone");
        assert_eq!(b.poll(&BuildHandle::named("never")).await.unwrap(), BuildStatus::Gone);
    }

    /// A build adds the builder base and compile cache it uses to its
    /// install's image record before it starts, so a sweep keeps them.
    #[tokio::test]
    async fn a_build_records_what_it_uses_in_its_install() {
        let install = std::env::temp_dir().join(format!("weft-install-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&install).unwrap();
        let record = weft_core::images::ImageRefs {
            runtime: "weft-runtime:r".into(),
            standard_worker: "weft-worker:w".into(),
            builder_bases: ["weft-builder-base:b1".to_string()].into(),
            compile_cache_keys: ["k1".to_string()].into(),
        };
        record.save(&install).unwrap();
        let docker = Arc::new(HeldBuilds { docker: FakeDocker::new(), ends: tokio::sync::Semaphore::new(1) });
        let b = DockerImageBuilder::new(docker, weft_core::infra::Install::default_install(), install.clone());
        let mut r = req("b1");
        r.uses = weft_core::images::BuildUses { builder_base: Some("weft-builder-base:b2".into()), compile_cache_key: Some("k2".into()) };
        b.start(r).await.unwrap();
        let now = weft_core::images::ImageRefs::load(&install).unwrap().unwrap();
        assert_eq!(now.builder_bases, ["weft-builder-base:b1".to_string(), "weft-builder-base:b2".to_string()].into());
        assert_eq!(now.compile_cache_keys, ["k1".to_string(), "k2".to_string()].into());
        std::fs::remove_dir_all(&install).unwrap();
    }

    /// A build stopped while it runs lets go of its staging with it.
    #[tokio::test]
    async fn a_stopped_build_lets_go_of_its_staging() {
        let docker = Arc::new(HeldBuilds { docker: FakeDocker::new(), ends: tokio::sync::Semaphore::new(0) });
        let b = DockerImageBuilder::new(docker, weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        let r = req("b1");
        let dir = r.context_dir.clone();
        let h = b.start(r).await.unwrap();
        tokio::task::yield_now().await;
        assert!(dir.exists(), "kept while it runs");
        b.release(&h).await;
        gone(&dir).await;
        assert_eq!(b.poll(&h).await.unwrap(), BuildStatus::Gone);
    }

    /// A content another install already holds is given this install's
    /// name instead of being built; with nobody holding it, it is built.
    #[tokio::test]
    async fn a_content_another_install_holds_is_named_not_built() {
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["image", "ls"], "evil/weft-worker:abc\nweft-worker:abc\n");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::named("cell7").unwrap(), std::env::temp_dir().join("weft-no-install"));
        let mut r = req("b1");
        r.image_ref = b.image_ref("weft-worker:abc");
        assert_eq!(r.image_ref, "localhost/weft-cell7/weft-worker:abc");
        let dir = r.context_dir.clone();
        let h = b.start(r.clone()).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: None });
        assert_eq!(docker.calls_to("tag"), vec![vec!["tag", "weft-worker:abc", "localhost/weft-cell7/weft-worker:abc"]]);
        assert!(docker.calls_to("build").is_empty(), "nothing built");
        assert!(dir.exists(), "the test's own clone still holds it");

        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["image", "ls"], "evil/weft-worker:abc\n");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::named("cell7").unwrap(), std::env::temp_dir().join("weft-no-install"));
        let h = b.start(r).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: None });
        assert!(docker.calls_to("tag").is_empty(), "an image no install holds is never taken");
        assert_eq!(docker.calls_to("build").len(), 1);
        gone(&dir).await;
    }

    /// A Docker whose images all carry one file: `docker cp` out of any
    /// container writes it where the copy was asked to land.
    struct ImageFile {
        docker: FakeDocker,
        bytes: Vec<u8>,
    }

    impl ImageFile {
        fn new(bytes: &[u8]) -> Arc<Self> {
            let docker = FakeDocker::new();
            docker.answer(&["create"], "c1\n");
            Arc::new(Self { docker, bytes: bytes.to_vec() })
        }
    }

    #[async_trait]
    impl Docker for ImageFile {
        async fn exec(&self, args: &[String]) -> anyhow::Result<docker::DockerOutput> {
            let out = self.docker.exec(args).await?;
            if out.success() && args.first().is_some_and(|verb| verb == "cp") {
                std::fs::write(&args[2], &self.bytes).unwrap();
            }
            Ok(out)
        }
    }

    fn asking_for_the_record(name: &str) -> BuildRequest {
        let mut r = req(name);
        r.result_file = Some(weft_core::builds::LEFT_OUT_RECORD_PATH.into());
        r
    }

    /// A build that names a result file answers its bytes, read from a
    /// container that is created, copied out of and removed, never run.
    #[tokio::test]
    async fn a_build_answers_the_result_file_it_names() {
        let docker = ImageFile::new(br#"{"packages":[]}"#);
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        let h = b.start(asking_for_the_record("b1")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(br#"{"packages":[]}"#.to_vec()) });
        let calls = docker.docker.calls();
        let verbs: Vec<&str> = calls.iter().map(|c| c[0].as_str()).collect();
        assert_eq!(verbs, ["image", "build", "create", "cp", "rm"], "built, then read without a start");
        assert_eq!(
            calls[2],
            [
                "create", "--label", "weft-install=default", "--label", "weft.image=weft-worker:abc", "--label", "weft.role=build-result",
                "weft-worker:abc", "weft-never-run",
            ]
        );
        assert_eq!(calls[3][1], "c1:/weft-left-out.json");
        assert!(!std::path::Path::new(&calls[3][2]).exists(), "the copy is removed once read");
        assert_eq!(calls[4], ["rm", "--force", "c1"]);
    }

    /// A container that will not go is logged, and never hides a result
    /// that was read.
    #[tokio::test]
    async fn a_failed_removal_keeps_the_result() {
        let docker = ImageFile::new(b"{}");
        docker.docker.fail(&["rm"], "Error response from daemon: something odd");
        let b = DockerImageBuilder::new(docker, weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        let h = b.start(asking_for_the_record("b1")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
    }

    /// An image without the result file it was asked for is a failed
    /// build naming the file, and its container is removed all the same.
    #[tokio::test]
    async fn a_missing_result_file_fails_the_build() {
        let docker = ImageFile::new(b"");
        docker.docker.fail(&["cp"], "Error response from daemon: Could not find the file /weft-left-out.json in container c1");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        let h = b.start(asking_for_the_record("b1")).await.unwrap();
        let BuildStatus::Failed { reason } = settled(&b, &h).await else { panic!("a build without its result succeeded") };
        assert!(reason.contains("/weft-left-out.json") && reason.contains("Could not find the file"), "{reason}");
        assert_eq!(docker.docker.calls_to("rm"), vec![vec!["rm", "--force", "c1"]], "removed even though the copy failed");
    }

    /// A build that names no result file answers none and creates no
    /// container.
    #[tokio::test]
    async fn a_build_naming_no_result_file_answers_none() {
        let docker = ImageFile::new(b"{}");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        let h = b.start(req("b1")).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: None });
        assert!(docker.docker.calls_to("create").is_empty());
    }

    /// An image taken from another install has its result read too.
    #[tokio::test]
    async fn an_adopted_image_has_its_result_read() {
        let docker = ImageFile::new(b"{}");
        docker.docker.answer(&["image", "ls"], "weft-worker:abc\n");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::named("cell7").unwrap(), std::env::temp_dir().join("weft-no-install"));
        let mut r = asking_for_the_record("b1");
        r.image_ref = b.image_ref("weft-worker:abc");
        let h = b.start(r).await.unwrap();
        assert_eq!(settled(&b, &h).await, BuildStatus::Succeeded { result: Some(b"{}".to_vec()) });
        assert!(docker.docker.calls_to("build").is_empty(), "nothing built");
        let creates = docker.docker.calls_to("create");
        assert_eq!(creates.len(), 1);
        assert_eq!(creates[0][creates[0].len() - 2..], ["localhost/weft-cell7/weft-worker:abc", "weft-never-run"]);
        assert!(creates[0].contains(&format!("{}=cell7", docker::labels::INSTALL)), "{creates:?}");
        assert_eq!(docker.docker.calls_to("rm").len(), 1);
    }

    /// The bound caps every record, least recently used first: `--all`
    /// and no age filter, or most of the cache is never eligible.
    #[tokio::test]
    async fn the_build_cache_bound_caps_every_record() {
        let docker = FakeDocker::new();
        bound_build_cache(&docker).await.unwrap();
        assert_eq!(docker.calls_to("builder")[0], vec!["builder", "prune", "--force", "--all", "--max-used-space", "20GB"]);
    }

    /// A result reader a crash left behind goes with its image, before the
    /// image's name is removed (it would keep the image in use for good),
    /// and so does the image's build log.
    #[tokio::test]
    async fn deleting_an_image_removes_its_left_behind_result_readers_and_its_log() {
        let dir = tempfile::tempdir().unwrap();
        let docker = Arc::new(FakeDocker::new());
        docker.answer(&["ps"], r#"{"Names":"stray_reader","State":"created","Labels":"weft.role=build-result,weft.image=weft-worker:abc"}"#);
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install(), dir.path().to_path_buf());
        let log = build_log(dir.path(), "weft-worker:abc").await.unwrap();
        std::fs::write(&log, "output").unwrap();
        assert_eq!(b.delete_image("weft-worker:abc").await.unwrap(), ImageDeleted::Deleted);
        let ps = &docker.calls_to("ps")[0];
        assert!(ps.contains(&"label=weft.role=build-result".to_string()) && ps.contains(&"label=weft.image=weft-worker:abc".to_string()), "{ps:?}");
        let verbs: Vec<String> = docker.calls().iter().map(|c| c[0].clone()).collect();
        assert_eq!(verbs, ["ps", "rm", "image"], "the reader goes first");
        assert_eq!(docker.calls_to("rm")[0], ["rm", "--force", "stray_reader"]);
        assert!(!log.exists(), "the log goes with the image");
    }

    #[tokio::test]
    async fn deleting_an_image_already_gone_is_fine() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error: No such image: x");
        let b = DockerImageBuilder::new(docker.clone(), weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::Absent);
        docker.fail(&["image", "inspect"], "Error: No such image: x");
        assert!(!b.image_exists("x").await.unwrap());
    }

    #[tokio::test]
    async fn an_image_a_container_still_runs_from_is_kept() {
        let docker = Arc::new(FakeDocker::new());
        docker.fail(&["image", "rm"], "Error response from daemon: conflict: unable to delete x (must be forced) - container 9bcd46ba4da9 is using its referenced image 0e8952e178ee");
        let b = DockerImageBuilder::new(docker, weft_core::infra::Install::default_install(), std::env::temp_dir().join("weft-no-install"));
        assert_eq!(b.delete_image("x").await.unwrap(), ImageDeleted::InUse);
    }
}
