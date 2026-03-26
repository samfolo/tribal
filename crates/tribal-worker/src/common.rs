//! Shared utilities used across the worker crate.

/// Expect message for triage task batch index unwrap.
pub(crate) const EXPECT_BATCH_INDEX: &str = "triage tasks always have a batch index";

/// Expect message for task `claimed_at` unwrap at commit time.
pub(crate) const EXPECT_CLAIMED_AT: &str = "task is claimed at commit time";

/// Maximum number of characters to include in parse-failure log previews.
pub(crate) const PARSE_PREVIEW_LENGTH: usize = 500;
