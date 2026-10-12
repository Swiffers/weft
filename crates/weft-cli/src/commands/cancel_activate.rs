//! `weft cancel-activate [project]`. Cancel an in-flight `activate`:
//! the dispatcher cancels the setup run of each trigger still
//! activating, and those triggers land down (parked with what they kept,
//! wiped when they kept nothing). With no `--trigger` and no `--instance`,
//! a resync the project holds, under way or stopped, is given up too, and
//! the answer names the triggers it had taken down and not brought back,
//! which stay down.
//!
//! 409 from the dispatcher for a cancel naming triggers or an instance
//! while a resync holds the project (the resync covers every owner's
//! triggers); 412 when none of them is activating and no resync is held.

use super::Ctx;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, project: Option<String>, scope: weft_core::activation::ActivationScope) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::CancelActivate, |progress| async move {
        run_inner(&ctx_inner, &progress, project, scope).await
    })
    .await
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    project: Option<String>,
    scope: weft_core::activation::ActivationScope,
) -> anyhow::Result<()> {
    let id = super::resolve_project_id(ctx, project)?;
    let client = ctx.client()?;
    let path = format!("/projects/{id}/cancel-activate");
    progress.dispatcher_call_start(&path);
    let answer = client.post_json(&path, &serde_json::to_value(&scope)?).await?;
    let answer: weft_core::activation::CancelActivateResponse = serde_json::from_value(answer).map_err(|e| {
        anyhow::anyhow!("cancel-activate: the dispatcher's answer does not read as one ({e}); upgrade the dispatcher or this CLI so the versions match")
    })?;
    progress.dispatcher_call_done(serde_json::json!({ "project_id": id, "resync_given_up": answer.resync_given_up }));
    progress.complete(&said(&id, answer.resync_given_up.as_deref()));
    Ok(())
}

/// What the person is told the cancel did, naming the triggers a resync
/// it gave up leaves down.
fn said(id: &str, resync_given_up: Option<&[weft_core::activation::OwnerTriggers]>) -> String {
    match resync_given_up {
        None => format!("cancel-activate issued for {id}"),
        Some([]) => format!("gave up the resync of {id}; it had brought back every trigger it took down"),
        Some(left) => {
            let down: Vec<String> = left
                .iter()
                .map(|owner| format!("{} ({})", weft_core::deactivation::whose_triggers(&owner.owner), owner.triggers.join(", ")))
                .collect();
            format!(
                "gave up the resync of {id}; these triggers it took down stay down: {}. `weft activate` turns them on",
                down.join("; ")
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::said;
    use weft_core::activation::OwnerTriggers;
    use weft_core::instance::Owner;

    /// A resync given up names what it leaves down, or says nothing is.
    #[test]
    fn a_given_up_resync_names_what_stays_down() {
        assert_eq!(said("p", None), "cancel-activate issued for p");
        assert!(said("p", Some(&[])).contains("brought back every trigger"));
        let left = [OwnerTriggers { owner: Owner::Shared, triggers: vec!["feed".into(), "door".into()] }];
        assert_eq!(
            said("p", Some(&left)),
            "gave up the resync of p; these triggers it took down stay down: the program's triggers (feed, door). `weft activate` turns them on"
        );
    }
}
