//! Implementation of the `tribal bootstrap` subcommand.
//!
//! Composes `setup` and `project::register` into a single invocation
//! that mints a token, registers a project, and emits the MCP wire-up
//! snippet — either as JSON (`--json`) or as a polished human-readable
//! handoff. Setup and register write to `io::sink()` so bootstrap can
//! assemble its own output without interleaved progress lines.

mod output;
mod run;

pub(crate) use run::run;
