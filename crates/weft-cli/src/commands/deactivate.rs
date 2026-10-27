//! `weft deactivate [project]`. Without arg: discover cwd project. No
//! build required: it takes triggers down as `--door`, `--held` and
//! `--running` say (`weft_core::take_down`), each left out taking the
//! command's default, and prints what it did.

use super::Ctx;
use crate::progress::ActionVerb;
use weft_core::deactivation::{DeactivateRequest, DeactivateResponse};
use weft_core::TakeDownChoice;

/// The line a plain deactivate adds when instances still have triggers
/// on: how many, which, and the flag that takes theirs down too. `None`
/// when no instance's are on.
fn instances_still_on_line(instances: &[weft_core::instance::InstanceId], on: Option<&str>) -> Option<String> {
    if instances.is_empty() {
        return None;
    }
    let names = instances.iter().map(|m| m.as_str()).collect::<Vec<_>>().join(", ");
    let (count, have) = match instances.len() {
        1 => ("1 instance".to_string(), "has"),
        n => (format!("{n} instances"), "have"),
    };
    Some(format!(
        "{count} still {have} triggers on ({names}); `{}` takes theirs down too",
        super::weft_on(on, "deactivate --all-instances")
    ))
}

pub async fn run(
    ctx: Ctx,
    project: Option<String>,
    choice: TakeDownChoice,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Deactivate, |progress| async move {
        run_inner(&ctx_inner, &progress, project, choice, scope).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
    choice: TakeDownChoice,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    // The dispatcher resolves the same way; reading it here refuses a
    // contradiction (`--door-for` beside `refuse`) before anything is sent.
    let take_down = choice.resolve().map_err(|why| anyhow::anyhow!("{why}"))?;
    let (client, id, name) = match project {
        Some(id) => (ctx.client()?, id.clone(), id),
        None => super::resolve_project(ctx)?,
    };
    let path = format!("/projects/{id}/deactivate");
    let body = DeactivateRequest { choice, scope };
    progress.drain_wait(take_down.running, crate::progress::DrainOn::Deactivate);
    progress.dispatcher_call_start(&path);
    let answer: DeactivateResponse =
        crate::client::read_answer(client.post_json(&path, &serde_json::to_value(&body)?).await?, "the deactivate's answer")?;
    // The triggers are down; the runs they fired finish on their own
    // version, followed in short looks until they have.
    if let Some(task) = answer.waiting {
        // The wait says nothing on the way (`take_down_wait`).
        super::follow::follow_task(&client, progress, &id, task, async |_| Ok(())).await?;
    }
    progress.dispatcher_call_done(serde_json::to_value(&answer)?);
    if !ctx.json() {
        if let Some(line) = instances_still_on_line(&answer.instances_still_on, ctx.on()) {
            println!("{line}");
        }
    }
    progress.complete(&format!("{name} ({id}): {}", answer.report.describe()));
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_plain_deactivate_names_the_instances_still_on() {
        assert_eq!(super::instances_still_on_line(&[], None), None);
        let one = super::instances_still_on_line(&[weft_core::instance::InstanceId::new("ada").unwrap()], None).unwrap();
        assert!(one.starts_with("1 instance still has triggers on (ada)") && one.contains("--all-instances"), "{one}");
        let two = super::instances_still_on_line(&[
            weft_core::instance::InstanceId::new("ada").unwrap(),
            weft_core::instance::InstanceId::new("bob").unwrap(),
        ], Some("prod"))
        .unwrap();
        assert!(two.contains("weft deactivate --all-instances --on prod"), "the hint acts on the same install: {two}");
        assert!(two.starts_with("2 instances still have triggers on (ada, bob)"), "{two}");
    }
}
