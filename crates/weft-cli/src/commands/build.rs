//! `weft build`: have the install build the current project and
//! register it, without starting anything. The install compiles the
//! version itself and builds what it runs (`weft-dispatcher::build`): its
//! worker (a program compiled on the machine on a local install, an image
//! on the cloud) named by content (`weft-worker:<binary-hash>`), so
//! identical sources across projects share one worker, and its infra
//! images. Also home of the release images' verb (`build-images`).

use anyhow::Result;

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, node_set: weft_core::builds::NodeSet) -> Result<()> {
    let inner = ctx.clone();
    ctx.with_progress(ActionVerb::Build, |progress| async move {
        // Build IS "make the dispatcher's picture of this project match
        // my disk": the image, and the compiled program registered under
        // its own hash, with nothing started. Same path every other verb
        // takes to get there, so a `weft run` straight after is a no-op
        // rather than a second opinion on what the project is.
        //
        // It is also how a person puts back code the dispatcher lost. A
        // run names the hash of the program it ran, and its inputs and
        // outputs are worked out by folding the journal against that
        // program, so registering unchanged files restores exactly what
        // the run needs and it reads again. It records nothing in the
        // VERSION TREE: that is `weft checkpoint`, and a version is a
        // point in a folder's history rather than something a build has
        // an opinion about.
        let handle = super::ensure::ensure_registered(&inner, &progress, node_set).await?;
        handle.warn_broken(&progress);
        progress.complete(&format!(
            "{} ({}) on worker {}",
            handle.name,
            handle.id,
            short_hash(handle.binary_hash())
        ));
        Ok(())
    })
    .await
}

/// `weft build-images [--push | --push-suffix <s>]`: ensure the runtime
/// image, the worker builder base and the standard worker image exist
/// locally under their content-addressed refs, then optionally push each to
/// its registry. The release workflow's verb: it runs on every push to the
/// release branch so a clean checkout's `weft daemon start` pulls its
/// runtime image instead of compiling it, and a cloud install's worker
/// images build FROM a ready base. With a suffix, each ref is pushed as
/// `<ref><suffix>` (one architecture's half; the workflow stitches the bare
/// ref into a multi-arch manifest list afterwards).
/// Ensures run concurrently (independent input sets, per-image buildkit cache
/// mounts); pushes run after ALL ensures, so a failed build never publishes a
/// partial set's siblings out of order. Stdout carries ONLY the bare refs,
/// one per line, for the workflow to capture; progress rides stderr.
/// `--print` stops after resolving: the refs this tree's content hashes
/// to, touching no image. `runtime_binary` is a `weft-runtime` built
/// outside Docker, copied into the runtime image instead of compiled
/// (`images::ensure_runtime_image`).
pub async fn run_build_images(
    push: bool,
    push_suffix: Option<String>,
    print: bool,
    runtime_binary: Option<&std::path::Path>,
) -> Result<()> {
    if print {
        // The SAME list the ensure path prints (`bare_refs` carries
        // the stdout contract), never a second construction of it.
        let shared = crate::images::ReleaseImages::current()?;
        for image_ref in shared.bare_refs() {
            println!("{image_ref}");
        }
        return Ok(());
    }
    let shared = crate::images::ReleaseImages::current()?;
    crate::images::ensure_release_images(&shared, push_suffix.as_deref(), runtime_binary).await?;
    if push || push_suffix.is_some() {
        for image_ref in shared.bare_refs() {
            // The suffixed name is exactly what the ensure materialized
            // (one shared `suffixed_ref` on both sides).
            crate::images::docker_push(&crate::images::suffixed_ref(
                image_ref,
                push_suffix.as_deref(),
            ))
            .await?;
        }
    }
    for image_ref in shared.bare_refs() {
        println!("{image_ref}");
    }
    Ok(())
}

/// 16-char prefix of the SHA-256 hash, for human-facing log lines ONLY (never the
/// worker's tag, which uses the full hash). Short enough to keep progress output
/// legible.
pub fn short_hash(hash: &str) -> String {
    hash.chars().take(16).collect()
}
