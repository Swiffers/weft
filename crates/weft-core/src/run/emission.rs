//! Pulse emissions and their identity.
//!
//! Every act that puts pulses on wires is an EMISSION with an id: a
//! node body's `pulse_downstream` or `close_port` (written to the
//! journal on the `PortEmitted` / `PortClosed` row), a firing's
//! termination sweep, a group boundary forwarding, a loop launching
//! an iteration or emitting outward. Every one of those ids is DERIVED
//! from where the act happens (the node or group, the frames, and for
//! a stream item or an iteration its number), never minted at random:
//! one run fires a node once per frames, so the place names the
//! act. A pulse's id is then derived from its emission id and the wire
//! it lands on. So the live engine and the journal fold, given the
//! same rows and the same program, put the same pulses with the same
//! ids in the same table, any row that names a pulse by id (a stream
//! take, a loop's item launch) resolves identically on both sides, and
//! a body replayed after a pause that emits a port again produces the
//! very pulse already on the wire, which the table keeps once.

use uuid::Uuid;

use crate::frames::LoopFrames;
use crate::pulse::Pulse;

/// One pulse placed on a downstream edge, plus the provenance (which
/// node + port emitted it). The `pulse` is the EXACT pulse pushed onto
/// the live pulse table, held by value rather than re-listing its
/// fields (the value inside is an `Arc`, so this is a pointer bump).
/// `pulse.closed == true` means the pulse is a CLOSURE marker (a
/// structural "nothing arrives here", produced when the source firing
/// terminated without emitting on this port), not a data value.
#[derive(Debug, Clone)]
pub struct PulseEmission {
    pub pulse: Pulse,
    pub source_node: String,
    pub source_port: String,
}

/// The id of the pulse an emission puts on ONE wire: a function of the
/// emission and the whole wire (the port it left from, the node and
/// port it lands on), so the fold re-derives it. The source port is
/// part of it because one emission is one firing's whole bag, and the
/// id names the wire end to end. A data pulse and a closure on the
/// same wire from the same emission are two different pulses (an
/// emission that projects a `?` key delivers a value on one wire and
/// a closure on another).
pub fn pulse_id(
    emission_id: Uuid,
    source_port: &str,
    target_node: &str,
    target_port: &str,
    closed: bool,
) -> Uuid {
    let tag = if closed { "closed" } else { "value" };
    Uuid::new_v5(
        &emission_id,
        format!("{source_port}\0{target_node}\0{target_port}\0{tag}").as_bytes(),
    )
}

/// The emission of a node body's values and closes on its ordinary
/// output ports, and of what the engine puts on them for the firing (a
/// caught failure on `error`, a baked value): one id per firing. Each
/// such port goes out at most once per firing, and the pulse id carries
/// the port, so one id serves every one of them, and a replay of the
/// body after a pause that emits a port again lands on the pulse
/// already there.
///
/// None of the derived emissions here folds the run's id in: a
/// pulse table is one run's, so the location already names the act,
/// and a seeded run folds rows it inherited from another run into ITS
/// table (`weft_journal::seed`), where an id that changed with the
/// run would no longer match the pulse ids those rows name.
pub fn body_emission(node_id: &str, frames: &LoopFrames) -> Uuid {
    derived(&BODY, &[node_id, &frames_key(frames)])
}

/// The emission of the `item`th body emission that yields on a stream
/// (`Generator[T]`) output, counted from 0 per firing: every item is
/// its own pulse on the same wire, so each needs its own id.
pub fn stream_emission(node_id: &str, frames: &LoopFrames, item: u64) -> Uuid {
    derived(&STREAM_ITEM, &[node_id, &frames_key(frames), &item.to_string()])
}

/// The emission of a firing's TERMINATION sweep (the closures on every
/// output port it never mentioned).
pub fn terminal_sweep_emission(node_id: &str, frames: &LoopFrames) -> Uuid {
    derived(&TERMINAL_SWEEP, &[node_id, &frames_key(frames)])
}

/// The emission of a group boundary firing (a `Passthrough` forwarding
/// its inputs, or the closures a gated scope owes the outside).
pub fn boundary_emission(node_id: &str, frames: &LoopFrames) -> Uuid {
    derived(&BOUNDARY, &[node_id, &frames_key(frames)])
}

/// The emission of a loop launching iteration `index` (the body's
/// per-iteration pulses).
pub fn iteration_launch_emission(group_id: &str, parent_frames: &LoopFrames, index: u32) -> Uuid {
    derived(&ITERATION_LAUNCH, &[group_id, &frames_key(parent_frames), &index.to_string()])
}

/// The emission of a loop instance ending (its outward values, or the
/// closures of an abnormal end). One per instance: an instance
/// terminates once.
pub fn loop_termination_emission(group_id: &str, parent_frames: &LoopFrames) -> Uuid {
    derived(&LOOP_TERMINATION, &[group_id, &frames_key(parent_frames)])
}

/// Frames as a stable text key (`"3.0"` for `[3, 0]`, `"@auth"` for a
/// call, empty at root).
fn frames_key(frames: &LoopFrames) -> String {
    crate::frames::frames_text(frames)
}

fn derived(namespace: &Uuid, parts: &[&str]) -> Uuid {
    Uuid::new_v5(namespace, parts.join("\0").as_bytes())
}

const TERMINAL_SWEEP: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x01,
]);
const BOUNDARY: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x02,
]);
const ITERATION_LAUNCH: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x03,
]);
const LOOP_TERMINATION: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x04,
]);
const BODY: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x05,
]);
const STREAM_ITEM: Uuid = Uuid::from_bytes([
    0x5a, 0x1e, 0x9c, 0x0b, 0x7d, 0x2f, 0x4a, 0x8e, 0x9b, 0x3c, 0x1d, 0x6f, 0x2e, 0x8a, 0x4b, 0x06,
]);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frames::Frame;

    #[test]
    fn a_pulse_id_is_a_function_of_the_emission_and_the_wire() {
        let emission = Uuid::new_v4();
        let base = pulse_id(emission, "out", "b", "in", false);
        assert_eq!(base, pulse_id(emission, "out", "b", "in", false));
        assert_ne!(base, pulse_id(emission, "out", "c", "in", false));
        assert_ne!(base, pulse_id(emission, "out", "b", "other", false));
        assert_ne!(base, pulse_id(emission, "out", "b", "in", true));
        assert_ne!(base, pulse_id(Uuid::new_v4(), "out", "b", "in", false));
        assert_ne!(
            base,
            pulse_id(emission, "out2", "b", "in", false),
            "two output ports into one input port are two wires, two pulses"
        );
    }

    #[test]
    fn derived_emissions_separate_by_every_part_and_by_kind() {
        let root: LoopFrames = Vec::new();
        let inner: LoopFrames = vec![Frame::Loop { index: 2 }];
        let sweep = terminal_sweep_emission("n", &root);
        assert_eq!(sweep, terminal_sweep_emission("n", &root));
        assert_ne!(sweep, terminal_sweep_emission("n", &inner));
        assert_ne!(sweep, boundary_emission("n", &root), "same parts, different act");
        assert_ne!(sweep, body_emission("n", &root), "same parts, different act");
        assert_eq!(body_emission("n", &inner), body_emission("n", &inner), "a replayed body emits the same act");
        assert_ne!(body_emission("n", &root), body_emission("n", &inner));
        assert_ne!(stream_emission("n", &root, 0), stream_emission("n", &root, 1), "each stream item is its own act");
        assert_ne!(stream_emission("n", &root, 0), body_emission("n", &root));
        assert_ne!(
            iteration_launch_emission("g", &root, 0),
            iteration_launch_emission("g", &root, 1)
        );
        assert_ne!(
            loop_termination_emission("g", &root),
            loop_termination_emission("g", &inner)
        );
    }
}
