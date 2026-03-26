#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Shared utilities for the Tribal workspace.
//!
//! Small, dependency-light helpers that are used across multiple crates.
//! The module is flat by default; extract submodules only when a single
//! concern grows large enough to warrant its own file.

mod job_watch;

use std::time::Duration;

pub use job_watch::{JobStateTxs, JobWatchEntry};
use rand::RngExt;
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Pool names
// ---------------------------------------------------------------------------

/// Pool name for MCP read-path connections.
pub const POOL_NAME_MCP: &str = "mcp";

/// Pool name for worker write-path connections.
pub const POOL_NAME_WORKER: &str = "worker";

// ---------------------------------------------------------------------------
// Numeric clamping
// ---------------------------------------------------------------------------

/// Clamps a `usize` to `u32`, saturating at [`u32::MAX`].
#[must_use]
pub fn clamp_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Attempts to convert a value to `i32`, saturating at [`i32::MAX`]
/// if the conversion fails.
#[must_use]
pub fn clamp_to_i32(value: impl TryInto<i32>) -> i32 {
    value.try_into().unwrap_or(i32::MAX)
}

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

/// Length of a SHA-256 hex digest (64 lowercase hex characters).
pub const SHA256_HEX_LENGTH: usize = 64;

/// Computes the lowercase hex-encoded SHA-256 digest of the given content.
#[must_use]
pub fn sha256_hex(content: &str) -> String {
    let digest = Sha256::digest(content.as_bytes());
    format!("{digest:x}")
}

// ---------------------------------------------------------------------------
// Jitter
// ---------------------------------------------------------------------------

/// LCG multiplier from Knuth's MMIX linear congruential generator.
const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;

/// LCG increment from Knuth's MMIX linear congruential generator.
const LCG_INCREMENT: u64 = 1_442_695_040_888_963_407;

/// Produces a deterministic value in `[0, range * 2]` from a retry
/// count and a per-caller seed, using a single LCG step.
///
/// Avoids an RNG dependency while distributing values across both the
/// retry count and the caller identity.
#[must_use]
pub fn deterministic_jitter(retry_count: u32, range: u64, seed: u64) -> u32 {
    let hash = (u64::from(retry_count) ^ seed)
        .wrapping_mul(LCG_MULTIPLIER)
        .wrapping_add(LCG_INCREMENT);
    // Truncation is intentional: the modulo guarantees the value fits in u32.
    #[allow(clippy::cast_possible_truncation)]
    let result = (hash % (range * 2 + 1)) as u32;
    result
}

/// Returns a random [`Duration`] uniformly sampled from `[min, max]`.
///
/// # Panics
///
/// Panics if `min > max`.
#[must_use]
pub fn random_duration_in_range(min: Duration, max: Duration) -> Duration {
    assert!(min <= max, "min must not exceed max");
    let min_ms = min.as_millis();
    let max_ms = max.as_millis();
    let ms = rand::rng().random_range(min_ms..=max_ms);
    Duration::from_millis(u64::try_from(ms).unwrap_or(u64::MAX))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- clamp -------------------------------------------------------------

    #[test]
    fn test_clamp_to_u32_within_range() {
        assert_eq!(clamp_to_u32(42), 42);
        assert_eq!(clamp_to_u32(0), 0);
    }

    #[test]
    fn test_clamp_to_u32_saturates() {
        assert_eq!(clamp_to_u32(usize::MAX), u32::MAX);
    }

    #[test]
    fn test_clamp_to_i32_within_range() {
        assert_eq!(clamp_to_i32(42_u32), 42);
        assert_eq!(clamp_to_i32(0_u32), 0);
    }

    #[test]
    fn test_clamp_to_i32_saturates_u32() {
        assert_eq!(clamp_to_i32(u32::MAX), i32::MAX);
    }

    #[test]
    fn test_clamp_to_i32_saturates_u128() {
        assert_eq!(clamp_to_i32(u128::MAX), i32::MAX);
    }

    // -- hashing -----------------------------------------------------------

    #[test]
    fn test_sha256_hex_length() {
        let hash = sha256_hex("arbitrary content");
        assert_eq!(hash.len(), SHA256_HEX_LENGTH);
    }

    // -- jitter ------------------------------------------------------------

    #[test]
    fn test_deterministic_jitter_varies_with_seed() {
        let a = deterministic_jitter(3, 1000, 1);
        let b = deterministic_jitter(3, 1000, 42);
        let c = deterministic_jitter(3, 1000, 9999);
        assert!(
            a != b || b != c,
            "different seeds should produce different jitter",
        );
    }

    #[test]
    fn test_deterministic_jitter_bounded() {
        for retry_count in 0..100_u32 {
            let result = deterministic_jitter(retry_count, 500, 0);
            assert!(result <= 1000, "result {result} exceeds range * 2 (1000)");
        }
    }

    #[test]
    fn test_random_duration_in_range_bounds() {
        let min = Duration::from_secs(2);
        let max = Duration::from_secs(5);
        for _ in 0..50 {
            let d = random_duration_in_range(min, max);
            assert!(
                d >= min && d <= max,
                "duration {d:?} out of [{min:?}, {max:?}]"
            );
        }
    }

    #[test]
    fn test_random_duration_equal_bounds() {
        let d = Duration::from_secs(3);
        assert_eq!(random_duration_in_range(d, d), d);
    }
}
