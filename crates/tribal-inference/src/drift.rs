//! Geometry-drift detection for embedding profiles.
//!
//! [`probe_digest`] reduces a probe embedding to a compact, comparable digest:
//! it quantises each coordinate and hashes the result. The contract is
//! quantised-digest equality, not a distance tolerance; a digest cannot
//! express "diverges beyond a threshold", only whether two probes are the same.
//! The quantisation is coarse enough that healthy serving jitter rounds to the
//! same digest (so the no-op decision is stable and duplicate builds are
//! bounded) and fine enough that a real geometry change rounds to a different
//! one.

use tribal_common::sha256_hex;

/// Decimal places each probe coordinate is rounded to before hashing.
const PROBE_QUANTISATION_DECIMALS: i32 = 3;

/// Computes the quantised digest of a probe embedding vector.
///
/// Each coordinate is rounded to [`PROBE_QUANTISATION_DECIMALS`] places and the
/// fixed-point sequence is hashed, so two vectors that agree to that precision
/// share a digest while a coarser change yields a different one.
#[must_use]
pub fn probe_digest(vector: &[f32]) -> String {
    let scale = 10f64.powi(PROBE_QUANTISATION_DECIMALS);
    let quantised: Vec<String> = vector
        .iter()
        .map(|&value| {
            // Embedding coordinates are bounded (cosine geometry keeps them in
            // roughly [-1, 1]), so the rounded, scaled value fits an i64.
            #[allow(clippy::cast_possible_truncation)]
            let fixed = (f64::from(value) * scale).round() as i64;
            fixed.to_string()
        })
        .collect();
    sha256_hex(&quantised.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_vectors_share_a_digest() {
        let a = [0.1_f32, -0.2, 0.3];
        let b = [0.1_f32, -0.2, 0.3];
        assert_eq!(probe_digest(&a), probe_digest(&b));
    }

    #[test]
    fn test_sub_quantum_jitter_keeps_the_digest() {
        // Jitter below the 0.001 quantum rounds to the same fixed point.
        let base = [0.1234_f32, -0.5678, 0.9012];
        let jittered = [0.12339_f32, -0.56781, 0.90115];
        assert_eq!(probe_digest(&base), probe_digest(&jittered));
    }

    #[test]
    fn test_coarse_change_changes_the_digest() {
        let base = [0.1_f32, -0.2, 0.3];
        let changed = [0.5_f32, -0.2, 0.3];
        assert_ne!(probe_digest(&base), probe_digest(&changed));
    }

    #[test]
    fn test_order_is_significant() {
        let a = [0.1_f32, 0.2];
        let b = [0.2_f32, 0.1];
        assert_ne!(probe_digest(&a), probe_digest(&b));
    }
}
