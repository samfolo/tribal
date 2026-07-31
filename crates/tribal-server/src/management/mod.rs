//! Runtime-independent local authority and application services.

use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) mod application;
pub(crate) mod authority;
pub(crate) mod client;
pub(crate) mod config_schema;
pub(crate) mod configuration;
pub(crate) mod connector;
pub(crate) mod custody;
pub(crate) mod lifecycle;
pub(crate) mod operator_check;
pub(crate) mod probe;
pub(crate) mod product;
pub(crate) mod readiness;
pub(crate) mod runtime_control;
pub(crate) mod socket;
pub(crate) mod worker;

/// The instant a receipt records, in milliseconds since the Unix epoch. A
/// clock before the epoch reads as zero rather than failing an observation
/// that has already been made.
pub(crate) fn observed_at_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
