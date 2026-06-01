//! Per-check modules and shared internal types.
//!
//! Each leaf module under this namespace owns one check: its outcome
//! constructors, its `act` implementation, and its unit tests.  The
//! orchestrator iterates [`CheckStep`] in declared order, calling
//! `preflight` then `act` against a single shared [`CheckState`].

mod advertised_url_reachable;
mod binary_uniqueness;
mod config_parse;
mod config_validate;
mod database_reachable;
mod embedding_profile;
mod migrations_current;
mod project_resolution;
mod provider_probes;
mod skip_rules;
mod state;
mod step;
mod types;
mod valid_token_exists;

pub(super) use skip_rules::SkipMask;
pub(super) use state::CheckState;
pub(super) use step::{CheckStep, Preflight};
pub(super) use types::{CheckName, CheckOutcome, CheckOutcomes};
