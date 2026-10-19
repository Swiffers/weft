//! `weft resync`. Deactivate-then-activate against a fresh worker
//! image, with the USER'S trigger-deactivation choice (mode + running
//! policy + drain cap; same picker as `weft deactivate`). Used after
//! editing the trigger or fire subgraph. Plain, it brings every trigger
//! that is on up to date, the program's and each instance's; the
//! dispatcher picks whose from its activation rows and answers with the
//! list. Refuses on the dispatcher side if the infra those triggers read
//! isn't running. Only the triggers that are on come down and back up.
//!
//! `weft resync --resume` takes up again a resync that stopped with
//! triggers down, where it stopped, with the choice and the build it
//! began with ([`resume`]): it builds nothing and never starts a new one.

use super::Ctx;
use crate::commands::deactivate::TriggerChoiceFlags;
use crate::progress::ActionVerb;

pub async fn run(ctx: Ctx, scope: weft_core::activation::ActivationScope, opts: TriggerChoiceFlags) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Resync, |progress| async move {
        run_inner(&ctx_inner, &progress, scope, opts).await
    })
    .await
}

/// `weft resync --resume`: `POST /projects/{id}/resync/resume`, followed
/// to its end like a resync.
pub async fn resume(ctx: Ctx) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Resync, |progress| async move {
        let id = super::resolve_project_id(&ctx_inner, None)?;
        let client = ctx_inner.client()?;
        let path = format!("/projects/{id}/resync/resume");
        progress.trigger_register_start();
        progress.dispatcher_call_start(&path);
        let answer = client.post_json(&path, &serde_json::json!({})).await?;
        let resynced = resynced_of(answer)?;
        progress.dispatcher_call_done(serde_json::json!({ "projectId": id, "resynced": resynced }));
        progress.trigger_register_done();
        let whom = resynced.iter().map(weft_core::deactivation::whose_triggers).collect::<Vec<_>>().join(", ");
        progress.complete(&format!("took the resync up again and resynced {whom} in {id}"));
        Ok(())
    })
    .await
}

/// Whose triggers a resync's answer says it brought up to date.
fn resynced_of(answer: serde_json::Value) -> anyhow::Result<Vec<weft_core::instance::Owner>> {
    Ok(serde_json::from_value::<weft_core::activation::ResyncResponse>(answer)
        .map_err(|e| {
            anyhow::anyhow!(
                "resync: the dispatcher's answer does not read as one ({e}); upgrade the dispatcher \
                 or this CLI so the versions match"
            )
        })?
        .resynced)
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    scope: weft_core::activation::ActivationScope,
    opts: TriggerChoiceFlags,
) -> anyhow::Result<()> {
    // Resync only re-registers triggers that are on (the dispatcher
    // refuses anything else with a 409: a parked trigger is brought back
    // with `weft activate`, by choice, never as a side effect), and the
    // dispatcher is the one that knows whose are on. So nothing is
    // checked here: the choice of how they come down goes along when
    // it was given, or off a terminal (where no `--mode` is `wipe`, see
    // `prompt_trigger_deactivation`); a person at a terminal who gave
    // none is asked once the dispatcher says triggers are on. The
    // dispatcher refuses before it builds or takes anything down.
    let (running_policy, drain_timeout) =
        super::ensure::parse_running_choice(opts.running_policy.as_deref(), opts.drain_timeout)?;
    let scripted = ctx.json() || !crate::prompt::is_interactive();
    let given = if scripted {
        Some(super::deactivate::prompt_trigger_deactivation(
            ctx.json(),
            opts.mode.as_deref(),
            opts.grace,
            running_policy,
            drain_timeout,
        )?)
    } else {
        super::deactivate::given_trigger_deactivation(
            false,
            opts.mode.as_deref(),
            opts.grace,
            running_policy,
            drain_timeout,
        )?
    };
    let handle = super::ensure::ensure_registered(ctx, progress, weft_core::builds::NodeSet::Full).await?;
    let path = format!("/projects/{}/resync", handle.id);
    let body = weft_core::deactivation::ResyncRequest { build: handle.built.named(), trigger_deactivation: None, scope };
    progress.drain_wait(running_policy, drain_timeout);
    progress.trigger_register_start();
    progress.dispatcher_call_start(&path);
    let answer = super::deactivate::post_with_trigger_choice(
        &handle.client,
        &path,
        body,
        ctx.json(),
        given,
        running_policy,
        drain_timeout,
    )
    .await?;
    let resynced = resynced_of(answer)?;
    progress.dispatcher_call_done(serde_json::json!({ "projectId": handle.id, "resynced": resynced }));
    progress.trigger_register_done();
    let whom = resynced.iter().map(weft_core::deactivation::whose_triggers).collect::<Vec<_>>().join(", ");
    handle.warn_broken(progress);
    progress.complete(&format!("resynced {whom} in {} ({})", handle.name, handle.id));
    Ok(())
}
