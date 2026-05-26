//! Shared helpers for prompt assembly.

/// Narrows a configured `f64` sampling temperature to the `f32` the wire
/// layer carries. `None` (provider default) passes through unchanged.
// f32 precision is ample for a sampling temperature; the narrowing is intended.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn narrow_temperature(temperature: Option<f64>) -> Option<f32> {
    temperature.map(|t| t as f32)
}
