//! Per-check modules and shared internal types.
//!
//! Each leaf module under this namespace produces a typed
//! [`CheckOutcome`] for one diagnostic check.  Shared types — status,
//! detail variants, remediation variants — live in [`types`].

mod config_parse;
mod types;

pub(super) use types::{CheckName, CheckOutcome, CheckRemediation};
