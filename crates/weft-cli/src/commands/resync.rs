//! `weft resync`. Builds, then brings every trigger that is on and behind
//! the build onto it: each taken down as `--door`, `--held` and
//! `--running` say (`weft_core::take_down`), its paused runs moved onto the
//! new build when what is held is kept, and back on once the old version
//! is done. Plain, it is the program's own triggers; `--instance <id>`
//! (repeatable) names instances', `--all-instances` every instance's,
//! `--all` the program's own and every instance's, and `--trigger`
//! narrows any of them. The resync runs apart from the request that asked
//! for it, followed in short looks until it ends
//! (`super::follow::follow_task`). A trigger that is off is never turned on: the answer names it
//! with the `weft activate` that would. The one exception is what the
//! install's maintenance took down and could not bring back (the project
//! did not build on the new weft): that comes back on this build, and the
//! answer says what came back.
//!
//! `weft resync --resume` takes up again a resync that stopped with
//! triggers down, where it stopped, with the answers and the build it
//! began with ([`resume`]): it builds nothing and never starts a new one.

use super::Ctx;
use crate::progress::ActionVerb;
use weft_core::deactivation::{ResyncAnswer, ResyncProgress, ResyncResponse};
use weft_core::TakeDownChoice;

pub async fn run(ctx: Ctx, scope: weft_core::activation::ActivationScope, choice: TakeDownChoice) -> anyhow::Result<()> {
    let ctx_inner = ctx.clone();
    ctx.with_progress(ActionVerb::Resync, |progress| async move {
        run_inner(&ctx_inner, &progress, scope, choice).await
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
        let answer = answer_of(&client, &progress, &id, client.post_json(&path, &serde_json::json!({})).await?).await?;
        progress.dispatcher_call_done(serde_json::to_value(&answer)?);
        progress.trigger_register_done();
        report(&ctx_inner, &progress, &answer, &format!("took the resync of {id} up again"));
        Ok(())
    })
    .await
}

/// What a resync request came to, followed to its end when it is under
/// way.
async fn answer_of(
    client: &crate::client::DispatcherClient,
    progress: &crate::progress::Progress,
    project_id: &str,
    answer: serde_json::Value,
) -> anyhow::Result<ResyncResponse> {
    match crate::client::read_answer(answer, "the resync's answer")? {
        ResyncAnswer::Done(done) => Ok(done),
        ResyncAnswer::UnderWay { task, brought_back } => {
            // What it waits for, said each time it changes: on the progress
            // channel, so a CI log shows it line by line and `--json` gets
            // it as events. It starts as naming none, so a first look that
            // names none says nothing: the drain-wait line already said what
            // the resync waits for, and an empty list never means nothing is
            // left (a run not on record yet is waited for unnamed).
            let mut said = ResyncProgress::default();
            let going = async |where_it_is: Option<serde_json::Value>| {
                let Some(where_it_is) = where_it_is else { return Ok(()) };
                let now: ResyncProgress = crate::client::read_answer(where_it_is, "where the resync is")?;
                if said != now {
                    progress.resync_waits_for(&now);
                    said = now;
                }
                Ok(())
            };
            let followed: ResyncResponse =
                crate::client::read_answer(super::follow::follow_task(client, progress, project_id, task, going).await?, "the resync's answer")?;
            Ok(ResyncResponse { brought_back, ..followed })
        }
    }
}

async fn run_inner(
    ctx: &Ctx,
    progress: &crate::progress::Progress,
    scope: weft_core::activation::ActivationScope,
    choice: TakeDownChoice,
) -> anyhow::Result<()> {
    // Read before anything is built: a contradiction is refused now, not
    // after a build that took a minute.
    let take_down = choice.resolve().map_err(|why| anyhow::anyhow!("{why}"))?;
    let handle = super::ensure::ensure_registered(ctx, progress, weft_core::builds::NodeSet::Full).await?;
    let path = format!("/projects/{}/resync", handle.id);
    let body = weft_core::deactivation::ResyncRequest { build: handle.built.named(), choice, scope };
    progress.drain_wait(take_down.running, crate::progress::DrainOn::Resync);
    progress.trigger_register_start();
    progress.dispatcher_call_start(&path);
    let answer = answer_of(&handle.client, progress, &handle.id, handle.client.post_json(&path, &serde_json::to_value(&body)?).await?).await?;
    progress.dispatcher_call_done(serde_json::to_value(&answer)?);
    progress.trigger_register_done();
    handle.warn_broken(progress);
    report(ctx, progress, &answer, &format!("{} ({})", handle.name, handle.id));
    Ok(())
}

/// What a resync did, as a person reads it: the triggers it took down and
/// brought back (`subject` names the project), the paused runs it moved,
/// and the triggers it left off with the command that turns each on.
fn report(ctx: &Ctx, progress: &crate::progress::Progress, answer: &ResyncResponse, subject: &str) {
    if let Some(back) = &answer.brought_back {
        let (did, failed) = super::maintenance::lifted_lines(back);
        if !did.is_empty() {
            progress.warn(&format!("brought back what the install's update took down: {}", did.join("; ")));
        }
        for why in &failed {
            progress.warn(&format!("still down after the install's update: {why}"));
        }
    }
    for line in off_lines(&answer.off, ctx.on()) {
        progress.warn(&line);
    }
    let whom = answer.resynced.iter().map(weft_core::deactivation::whose_triggers).collect::<Vec<_>>();
    let summary = match whom.is_empty() {
        true => format!("{subject}: no trigger that is on is behind this build; nothing to resync"),
        false => format!(
            "{subject}: resynced {}; {}{}",
            whom.join(", "),
            answer.report.describe(),
            moved_words(answer.moved_runs)
        ),
    };
    progress.complete(&summary);
}

/// "; 2 paused runs moved onto the new build", or nothing.
fn moved_words(moved: u64) -> String {
    match moved {
        0 => String::new(),
        1 => "; 1 paused run moved onto the new build".into(),
        n => format!("; {n} paused runs moved onto the new build"),
    }
}

/// One line per owner whose named triggers are off: a resync turns none
/// on, and this says which command does.
fn off_lines(off: &[weft_core::activation::OwnerTriggers], on: Option<&str>) -> Vec<String> {
    off.iter()
        .map(|owned| {
            let flags: Vec<String> = owned.triggers.iter().map(|t| format!("--trigger {t}")).collect();
            let instance = owned.owner.instance().map(|i| format!(" --instance {i}")).unwrap_or_default();
            format!(
                "{} {} off, and a resync turns nothing on: `{}` does",
                owned.words(),
                if owned.triggers.len() == 1 { "is" } else { "are" },
                super::weft_on(on, &format!("activate {}{instance}", flags.join(" ")))
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use weft_core::activation::OwnerTriggers;
    use weft_core::instance::{InstanceId, Owner};

    /// A trigger left off is named with the activate that turns it on, for
    /// its own owner.
    #[test]
    fn a_trigger_left_off_names_the_activate_that_turns_it_on() {
        let off = [
            OwnerTriggers { owner: Owner::Shared, triggers: vec!["door".into()] },
            OwnerTriggers { owner: Owner::Instance(InstanceId::new("ada").unwrap()), triggers: vec!["inbox".into(), "feed".into()] },
        ];
        let lines = super::off_lines(&off, None);
        assert_eq!(lines[0], "'door' is off, and a resync turns nothing on: `weft activate --trigger door` does");
        assert!(lines[1].contains("`weft activate --trigger inbox --trigger feed --instance ada`") && lines[1].contains(" are off"), "{}", lines[1]);
        assert_eq!(super::moved_words(0), "");
        assert_eq!(super::moved_words(2), "; 2 paused runs moved onto the new build");
    }
}
