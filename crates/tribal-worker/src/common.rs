//! Shared utilities used across the worker crate.

/// Expect message for triage task batch index unwrap.
pub(crate) const EXPECT_BATCH_INDEX: &str = "triage tasks always have a batch index";

/// Maximum number of characters to include in parse-failure log previews.
pub(crate) const PARSE_PREVIEW_LENGTH: usize = 500;

/// Clamps a `usize` to `u32`, saturating at [`u32::MAX`].
pub(crate) fn clamp_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Attempts to convert a value to `i32`, saturating at [`i32::MAX`]
/// if the conversion fails.
pub(crate) fn clamp_to_i32(value: impl TryInto<i32>) -> i32 {
    value.try_into().unwrap_or(i32::MAX)
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
}
