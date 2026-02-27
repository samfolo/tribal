//! Shared utilities used across the worker crate.

/// Clamps a `usize` to `u32`, saturating at [`u32::MAX`].
pub(crate) fn clamp_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clamp_to_u32_within_range() {
        assert_eq!(clamp_to_u32(42), 42);
        assert_eq!(clamp_to_u32(0), 0);
    }

    #[test]
    fn test_clamp_to_u32_saturates() {
        assert_eq!(clamp_to_u32(usize::MAX), u32::MAX);
    }
}
