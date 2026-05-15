//! Redaction primitives shared across crates.

/// Placeholder substituted for secret values in any presentation layer
/// that needs to elide them.
pub const REDACTED: &str = "********";
