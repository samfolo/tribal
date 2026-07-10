//! `tribal check`: diagnostic command consolidating readiness checks
//! across the operational surface (config, database, project, token,
//! advertised URL, binary uniqueness, and optional provider probes).
//!
//! The submodules split data from presentation:
//!
//! - [`checks`] owns the internal data layer — typed outcomes and the
//!   per-check helpers that produce them.
//! - [`output`] projects outcomes into the wire report and owns the
//!   serialisation entry points the dispatch site calls.

mod checks;
mod output;
mod run;

pub(crate) use run::{CheckConfigSource, CheckReportOptions, run, run_report_async};
#[cfg(feature = "test-helpers")]
pub use {
    output::CheckOutput,
    run::{CheckOptions, run_async},
};
