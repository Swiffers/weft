//! Every integration test of this crate, in one binary: each module was
//! once a test file of its own, and one binary links the dependency tree
//! once instead of once per file. nextest still runs each test in its own
//! process.

mod project_defined_meter;
mod skill_example_meter;
