//! Per-check modules and shared internal types.
//!
//! Each leaf module under this namespace produces a typed
//! [`CheckOutcome`] for one diagnostic check.  Shared types — status,
//! detail variants, remediation variants — live in [`types`].

mod advertised_url_reachable;
mod binary_uniqueness;
mod config_parse;
mod config_validate;
mod context;
mod database_reachable;
mod migrations_current;
mod project_resolution;
mod types;
mod valid_token_exists;

pub(super) use advertised_url_reachable::run as advertised_url_reachable;
pub(super) use binary_uniqueness::run as binary_uniqueness;
pub(super) use context::CheckContext;
pub(super) use database_reachable::run as database_reachable;
pub(super) use migrations_current::run as migrations_current;
pub(super) use project_resolution::run as project_resolution;
pub(super) use types::{CheckName, CheckOutcome, CheckOutcomes, CheckRemediation};
pub(super) use valid_token_exists::run as valid_token_exists;
