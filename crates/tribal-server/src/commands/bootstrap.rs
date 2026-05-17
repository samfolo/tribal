//! `tribal bootstrap`: setup + project register in one invocation,
//! emitting JSON or a polished handoff.

mod output;
mod run;

pub(crate) use run::run;
pub use run::{BootstrapOptions, run_async};
