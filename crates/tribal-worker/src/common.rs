//! Shared utilities used across the worker crate.

/// Expect message for triage task batch index unwrap.
pub(crate) const EXPECT_BATCH_INDEX: &str = "triage tasks always have a batch index";

/// Maximum number of characters to include in parse-failure log previews.
pub(crate) const PARSE_PREVIEW_LENGTH: usize = 500;
