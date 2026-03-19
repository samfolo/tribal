// mod.rs is required here — cargo discovers top-level files in tests/ as
// separate binaries, so harness.rs would conflict with the harness/ directory.
//
// The harness exports its full API surface for future tests; not all of it
// is consumed by the current suite.
#![allow(dead_code, unused_imports)]

pub mod assertions;
pub mod config;
pub mod diagnostics;
pub mod fixtures;
pub mod macros;
pub mod mocks;
pub mod polling;
pub mod server;
pub mod tool_call;
