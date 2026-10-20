//! `weft instances [project]`: every instance weft holds anything for in
//! the project, with its triggers on, its infra copies and its last run.
//! The same list a program's `ctx.instances().list()` and the
//! `ListInstances` node read (`GET /projects/{id}/instances`).

use anyhow::Context;

use super::Ctx;
use weft_core::program::InstanceHoldings;

pub async fn run(ctx: Ctx, project: Option<String>) -> anyhow::Result<()> {
    let id = super::resolve_project_id(&ctx, project)?;
    let client = ctx.client()?;
    let answer = client.get_json(&format!("/projects/{id}/instances")).await?;
    if ctx.json_out(&answer)? {
        return Ok(());
    }
    let instances: Vec<InstanceHoldings> = serde_json::from_value(answer).context("read the project's instances")?;
    if instances.is_empty() {
        println!("no instance holds anything in {id} yet");
        return Ok(());
    }
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64);
    println!("{:<24}  {:<10}  {:<10}  last run", "instance", "triggers", "infra");
    for held in &instances {
        println!("{}", line(held, now));
    }
    Ok(())
}

/// One instance's row: how many of its triggers are on, out of those it
/// has, how many of its infra copies run, and how long ago it last ran.
fn line(held: &InstanceHoldings, now_unix: i64) -> String {
    let on = held.triggers.iter().filter(|t| t.mode == weft_core::activation::ActivationMode::Active).count();
    let running = held.copies.iter().filter(|c| c.status == weft_core::infra::InfraNodeStatus::Running).count();
    let last = match held.last_run_unix {
        None => "never".to_string(),
        Some(at) => match (now_unix - at).max(0) {
            secs @ 0..120 => format!("{secs}s ago"),
            secs @ 120..7200 => format!("{} min ago", secs / 60),
            secs @ 7200..172_800 => format!("{} h ago", secs / 3600),
            secs => format!("{} days ago", secs / 86_400),
        },
    };
    format!(
        "{:<24}  {:<10}  {:<10}  {last}",
        held.instance.as_str(),
        format!("{on}/{} on", held.triggers.len()),
        format!("{running}/{} up", held.copies.len()),
    )
}

#[cfg(test)]
mod tests {
    use weft_core::activation::ActivationMode;
    use weft_core::instance::InstanceId;
    use weft_core::program::{InstanceHoldings, InstanceTrigger};

    /// A row counts the instance's triggers on and says when it last ran.
    #[test]
    fn a_row_counts_what_is_on_and_when_it_last_ran() {
        let trigger = |mode| InstanceTrigger { trigger: "inbox".into(), mode, waiting: None };
        let held = InstanceHoldings {
            instance: InstanceId::new("ada").unwrap(),
            values: 1,
            connections: 0,
            tokens: 0,
            copies: Vec::new(),
            triggers: vec![trigger(ActivationMode::Active), trigger(ActivationMode::Down(weft_core::Door::Take))],
            last_run_unix: Some(1_000),
        };
        let row = super::line(&held, 1_000 + 300);
        assert!(row.starts_with("ada ") && row.contains("1/2 on") && row.contains("0/0 up") && row.ends_with("5 min ago"), "{row}");
        assert!(super::line(&InstanceHoldings { last_run_unix: None, ..held }, 0).ends_with("never"));
    }
}
