//! `weft running-source <dir>`: write the files of the program an install
//! holds into a new folder, `--on <target>` for another install. What the
//! editor's install switch shows as that install's graph, and how a
//! person reads what prod runs without touching their own tree.

use anyhow::{bail, Context};

use weft_assets::sync::BlobFetch;

use super::Ctx;
use weft_core::versions::RunningSource;

pub async fn run(ctx: Ctx, dir: std::path::PathBuf) -> anyhow::Result<()> {
    let project = ctx.project()?;
    let client = ctx.client()?;
    let id = project.id().to_string();
    if dir.exists() {
        bail!("{} already exists; name a folder that does not", dir.display());
    }
    let running: RunningSource = serde_json::from_value(client.get_json(&format!("/projects/{id}/versions/running")).await?)
        .context("parse the running program's files")?;
    // Downloaded beside the destination and moved in whole, so the folder
    // either holds the whole program or does not exist.
    let parent = dir.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(std::path::Path::new("."));
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let staged = tempfile::tempdir_in(parent).with_context(|| format!("stage in {}", parent.display()))?;
    // Every path is checked before anything is written: it came off the
    // wire, and one that is absolute or climbs would land outside the
    // folder.
    for path in running.manifest.keys() {
        weft_assets::tree::validate_destination(staged.path(), path).context("nothing was written")?;
    }
    let blobs = super::copy::InstallBlobs(&client);
    for (path, hash) in &running.manifest {
        let full = staged.path().join(path);
        if let Some(folder) = full.parent() {
            std::fs::create_dir_all(folder).with_context(|| format!("create {}", folder.display()))?;
        }
        blobs.fetch(hash, &full).await.with_context(|| format!("download {path}"))?;
    }
    // Dropping `staged` after the move deletes nothing: its path is gone.
    std::fs::rename(staged.path(), &dir).with_context(|| format!("move the files into {}", dir.display()))?;
    let answer = serde_json::json!({ "version": running.version, "dir": dir });
    if !ctx.json_out(&answer)? {
        println!("version {} written to {}", super::versions::short(&running.version), dir.display());
    }
    Ok(())
}
