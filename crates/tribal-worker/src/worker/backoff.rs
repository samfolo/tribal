//! Retry backoff computation with deterministic jitter.
//!
//! Uses exponential backoff capped at [`BACKOFF_CAP_SECS`] seconds.
//! Jitter is applied as ±[`JITTER_FRACTION`] of the base duration using
//! a deterministic hash seeded by both the retry count and a caller-
//! provided seed (typically derived from the task ID).  This ensures
//! different tasks at the same retry count receive different jitter,
//! avoiding thundering-herd scheduling.
//!
//! Internally, jitter is computed in milliseconds so that low base
//! durations (2 s, 4 s) still produce non-zero jitter.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum backoff duration in seconds.  `2^retry_count` is clamped to
/// this value before jitter is applied.
const BACKOFF_CAP_SECS: u64 = 60;

/// Fractional jitter range applied to the base backoff.  A value of
/// `0.2` means ±20%, so the final duration falls within
/// `[base * 0.8, base * 1.2]`.
///
/// Must be kept consistent with [`JITTER_DIVISOR`] (enforced by a
/// compile-time assertion below).
pub(crate) const JITTER_FRACTION: f64 = 0.2;

/// Integer divisor for the jitter range: `base_secs / JITTER_DIVISOR`
/// yields the jitter range.  Must equal `1 / JITTER_FRACTION` (i.e. 5
/// for a 20% jitter).  Used in [`backoff_duration`] to avoid floating-
/// point arithmetic in the hot path.
const JITTER_DIVISOR: u64 = 5;

// Compile-time check that JITTER_DIVISOR == 1 / JITTER_FRACTION.
// Values are small exact constants so float comparison is safe here.
#[allow(
    clippy::float_cmp,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
const _: () = assert!(
    JITTER_DIVISOR as f64 * JITTER_FRACTION == 1.0,
    "JITTER_DIVISOR must equal 1 / JITTER_FRACTION",
);

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
///
/// `seed` should be unique per task (e.g. derived from the task ID) so
/// that different tasks at the same retry count receive different
/// jitter, preventing thundering-herd scheduling.
pub(crate) fn backoff_duration(retry_count: u32, seed: u64) -> chrono::Duration {
    let base_secs = 2u64.saturating_pow(retry_count).min(BACKOFF_CAP_SECS);
    let base_ms = base_secs * 1000;
    let jitter_range_ms = base_ms / JITTER_DIVISOR;
    // Safe: jitter_range_ms ≤ 12_000 (60_000 / 5), base_ms ≤ 60_000; both fit in i64.
    #[allow(clippy::cast_possible_wrap)]
    let jitter_ms = if jitter_range_ms > 0 {
        let offset = deterministic_jitter(retry_count, jitter_range_ms, seed);
        i64::from(offset) - (jitter_range_ms as i64)
    } else {
        0
    };

    #[allow(clippy::cast_possible_wrap)]
    let total_ms = (base_ms as i64) + jitter_ms;
    chrono::Duration::milliseconds(total_ms.max(1000))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Produces a deterministic value in `[0, range * 2]` from the retry
/// count and a per-task seed, using a single LCG step.  This avoids
/// an RNG dependency while distributing jitter across both the retry
/// count and the task identity.
fn deterministic_jitter(retry_count: u32, range: u64, seed: u64) -> u32 {
    let hash = (u64::from(retry_count) ^ seed)
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
        let cap_ms = (BACKOFF_CAP_SECS as f64 * 1000.0 * UPPER_BOUND).ceil() as i64;
        for retry in 7..=15 {
            let duration = backoff_duration(retry, 0);
            let ms = duration.num_milliseconds();
            assert!(
                ms <= cap_ms,
                "retry {retry}: got {ms}ms, expected <= {cap_ms}ms",
            );
        }
    }

    #[test]
    fn test_backoff_jitter_within_bounds() {
        for retry in 1..=100 {
            let duration = backoff_duration(retry, 0);
            let base_ms = 2u64.saturating_pow(retry).min(BACKOFF_CAP_SECS) * 1000;
            let lower = (base_ms as f64 * LOWER_BOUND).floor() as i64;
            let upper = (base_ms as f64 * UPPER_BOUND).ceil() as i64;
            let ms = duration.num_milliseconds();
            assert!(
                ms >= lower.max(1000) && ms <= upper,
                "retry {retry}: expected [{}, {upper}]ms, got {ms}ms",
                lower.max(1000),
            );
        }
    }

    #[test]
    fn test_backoff_varies_with_seed() {
        let d1 = backoff_duration(3, 1);
        let d2 = backoff_duration(3, 42);
        let d3 = backoff_duration(3, 9999);
        assert!(
            d1 != d2 || d2 != d3,
            "different seeds should produce different jitter",
        );
    }

    #[test]
    fn test_backoff_has_jitter_at_low_retry_counts() {
        let base_1_ms = 2000;
        let base_2_ms = 4000;
        let d1 = backoff_duration(1, 42).num_milliseconds();
        let d2 = backoff_duration(2, 42).num_milliseconds();
        assert!(
            d1 != base_1_ms || d2 != base_2_ms,
            "jitter should be non-zero at low retry counts",
        );
    }
}
