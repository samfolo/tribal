//! Retry backoff computation with deterministic jitter.
//!
//! Uses exponential backoff capped at [`BACKOFF_CAP_SECS`] seconds.
//! Jitter is applied as ±[`JITTER_FRACTION`] of the base duration using
//! a deterministic hash (no RNG dependency) so that backoff values are
//! reproducible for a given retry count.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum backoff duration in seconds.  `2^retry_count` is clamped to
/// this value before jitter is applied.
const BACKOFF_CAP_SECS: u64 = 60;

/// Fractional jitter range applied to the base backoff.  A value of
/// `0.2` means ±20%, so the final duration falls within
/// `[base * 0.8, base * 1.2]`.
#[cfg(test)]
pub(crate) const JITTER_FRACTION: f64 = 0.2;

/// LCG multiplier from Knuth's MMIX linear congruential generator.
/// Used by [`deterministic_jitter`] to produce a hash from the retry
/// count without pulling in a full PRNG crate.
const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;

/// LCG increment from Knuth's MMIX linear congruential generator.
const LCG_INCREMENT: u64 = 1_442_695_040_888_963_407;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Computes backoff duration: `min(2^retry_count, 60)` seconds with
/// ±20% deterministic jitter.
///
/// `retry_count` is the post-increment value (i.e. the retry that just
/// happened, starting at 1 for the first failure).
pub(crate) fn backoff_duration(retry_count: u32) -> chrono::Duration {
    let base_secs = 2u64.saturating_pow(retry_count).min(BACKOFF_CAP_SECS);
    let jitter_range = base_secs / 5;
    // Safe: jitter_range ≤ 12 (60 / 5), base_secs ≤ 60; both fit in i64.
    #[allow(clippy::cast_possible_wrap)]
    let jitter = if jitter_range > 0 {
        let offset = deterministic_jitter(retry_count, jitter_range);
        i64::from(offset) - (jitter_range as i64)
    } else {
        0
    };

    #[allow(clippy::cast_possible_wrap)]
    let total_secs = (base_secs as i64) + jitter;
    chrono::Duration::seconds(total_secs.max(1))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Produces a deterministic value in `[0, range * 2]` from the retry
/// count, using a single LCG step.  This avoids an RNG dependency
/// while still distributing jitter across the range.
fn deterministic_jitter(retry_count: u32, range: u64) -> u32 {
    let hash = u64::from(retry_count)
        .wrapping_mul(LCG_MULTIPLIER)
        .wrapping_add(LCG_INCREMENT);
    #[allow(clippy::cast_possible_truncation)]
    let result = (hash % (range * 2 + 1)) as u32;
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
mod tests {
    use super::*;

    /// Lower jitter bound multiplier: `1.0 - JITTER_FRACTION`.
    const LOWER_BOUND: f64 = 1.0 - JITTER_FRACTION;

    /// Upper jitter bound multiplier: `1.0 + JITTER_FRACTION`.
    const UPPER_BOUND: f64 = 1.0 + JITTER_FRACTION;

    #[test]
    fn test_backoff_capped_at_60s() {
        let cap_with_jitter = (BACKOFF_CAP_SECS as f64 * UPPER_BOUND).ceil() as i64;
        for retry in 7..=15 {
            let duration = backoff_duration(retry);
            let secs = duration.num_seconds();
            assert!(
                secs <= cap_with_jitter,
                "retry {retry}: got {secs}, expected <= {cap_with_jitter}",
            );
        }
    }

    #[test]
    fn test_backoff_jitter_within_bounds() {
        for retry in 1..=100 {
            let duration = backoff_duration(retry);
            let base = 2u64.saturating_pow(retry).min(BACKOFF_CAP_SECS);
            let lower = (base as f64 * LOWER_BOUND).floor() as i64;
            let upper = (base as f64 * UPPER_BOUND).ceil() as i64;
            let secs = duration.num_seconds();
            assert!(
                secs >= lower.max(1) && secs <= upper,
                "retry {retry}: expected [{}, {upper}], got {secs}",
                lower.max(1),
            );
        }
    }
}
