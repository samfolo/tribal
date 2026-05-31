//! Conversions between `f32` vectors and pgvector's `halfvec` storage type.
//!
//! Embeddings are produced as `Vec<f32>` and stored as `halfvec` (2-byte
//! floats). These helpers are the single site for the `f32`/`f16` round-trip,
//! so the lossy narrowing on write and the widening on read live in one place.

use half::f16;
use pgvector::HalfVector;

/// Narrows an `f32` slice to the `halfvec` storage type for binding.
pub(crate) fn to_halfvec(values: &[f32]) -> HalfVector {
    HalfVector::from_f32_slice(values)
}

/// Widens a decoded `halfvec` back to `Vec<f32>`.
pub(crate) fn to_f32_vec(vector: &HalfVector) -> Vec<f32> {
    vector.as_slice().iter().copied().map(f16::to_f32).collect()
}
