//! Shared by the instance nodes: how they read an instance id (one they
//! always need, or one that, left out, means the program's own copies),
//! and, for the ones that take something down, how the triggers they
//! reach go down (`door`, `doorForMinutes`, `held`, `running`, the same
//! three questions `weft deactivate` and `weft infra stop` answer) and
//! whether the run doing it goes too. One reader, so every such node
//! offers the same choices with the same defaults.

use weft::instance::InstanceId;
use weft::node::NodeOutput;
use weft::program::InfraDownAnswer;
use weft::{node_bail, Door, Held, RunContext, Running, StopSelf, TakeDownChoice, WeftResult};

/// The instance id a node was handed, checked the way every instance id
/// is (`InstanceId::new`: never blank, a plain id), so a bad one fails
/// here naming the input.
pub fn instance(ctx: &RunContext) -> WeftResult<InstanceId> {
    let instance: String = ctx.inputs.get("instance")?;
    match InstanceId::new(instance) {
        Ok(id) => Ok(id),
        Err(why) => node_bail!("instance: {why}"),
    }
}

/// The instance id a node acting on either kind of copy was handed, or
/// `None` when `instance` is unwired, which means the program's own
/// copies. A given id is checked like [`instance`], so an empty one (a
/// value that never arrived) fails rather than reaching the program's
/// own copies by accident.
pub fn instance_if_given(ctx: &RunContext) -> WeftResult<Option<InstanceId>> {
    match ctx.inputs.opt::<String>("instance")? {
        None => Ok(None),
        Some(given) => match InstanceId::new(given) {
            Ok(id) => Ok(Some(id)),
            Err(why) => node_bail!("instance: {why}"),
        },
    }
}

/// How a take-down brings the triggers it reaches down: what happens to
/// work arriving (`door`, refusing after `doorForMinutes` when set), to
/// the work they already hold (`held`), and to the runs going (`running`).
/// A contradiction (`doorForMinutes` beside a refusing door) is refused
/// here, before anything is asked.
pub fn take_down_choice(ctx: &RunContext) -> WeftResult<TakeDownChoice> {
    // Read typed: a word outside the enum fails naming the input, the
    // word, and the words it takes.
    let door: Door = ctx.inputs.get("door")?;
    let held: Held = ctx.inputs.get("held")?;
    let running: Running = ctx.inputs.get("running")?;
    let door_for_minutes = match ctx.inputs.opt::<f64>("doorForMinutes")? {
        None => None,
        Some(minutes) if minutes >= 1.0 && minutes.fract() == 0.0 && minutes <= f64::from(u32::MAX) => Some(minutes as u32),
        Some(minutes) => node_bail!("doorForMinutes must be a whole number of minutes from 1 to {}, got {minutes}", u32::MAX),
    };
    let choice = TakeDownChoice { door: Some(door), door_for_minutes, held: Some(held), running: Some(running) };
    if let Err(why) = choice.resolve() {
        node_bail!("{why}");
    }
    Ok(choice)
}

/// Whether the run doing the take-down goes too, when it is among what
/// the take-down reaches.
pub fn stop_self(ctx: &RunContext) -> WeftResult<StopSelf> {
    Ok(if ctx.inputs.get::<bool>("includeSelf")? { StopSelf::Include } else { StopSelf::Keep })
}

/// What a node that takes a copy down sends on: `done` when the copy is
/// down (taken down now, or already down), `noCopy` when there is no such
/// copy (never started, terminated before, or a mistyped instance id), so
/// a program answering a person can tell them which.
pub fn taken_down(answer: InfraDownAnswer) -> NodeOutput {
    match answer {
        InfraDownAnswer::TakenDown | InfraDownAnswer::AlreadyDown => NodeOutput::new().set("done", true),
        InfraDownAnswer::NoCopy => NodeOutput::new().set("noCopy", true),
    }
}
