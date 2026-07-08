//! `tribal check`: diagnostic command consolidating readiness checks
//! across the operational surface (config, database, project, token,
//! advertised URL, binary uniqueness, and optional provider probes).
//!
//! The submodules split data from presentation:
//!
//! - [`checks`] owns the internal data layer — typed outcomes and the
//!   per-check helpers that produce them.
//! - [`output`] owns the wire types and the serialisation entry points
//!   the dispatch site calls.

mod checks;
mod output;
mod run;

#[cfg(not(feature = "test-helpers"))]
pub(crate) use output::CheckOutput;
pub(crate) use run::{CheckReportOptions, run, run_report_async};
#[cfg(feature = "test-helpers")]
pub use {
    output::CheckOutput,
    run::{CheckOptions, run_async},
};
