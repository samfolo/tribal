//! Text utilities shared across test infrastructure modules.

/// Truncates `s` to at most `max` characters, appending `"..."` if
/// truncated. Safe for multi-byte UTF-8.
pub(crate) fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((idx, _)) => format!("{}...", &s[..idx]),
        None => s.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_string_unchanged() {
        assert_eq!(truncate("hello", 80), "hello");
    }

    #[test]
    fn test_exact_length_unchanged() {
        let s = "a".repeat(80);
        assert_eq!(truncate(&s, 80), s);
    }

    #[test]
    fn test_long_string_truncated() {
        let s = "a".repeat(100);
        let expected = format!("{}...", "a".repeat(80));
        assert_eq!(truncate(&s, 80), expected);
    }

    #[test]
    fn test_multibyte_characters_truncate_by_character_not_byte() {
        // '£' is 2 bytes (U+00A3); truncating at 3 characters should
        // yield 6 bytes of content, not slice mid-codepoint at byte 3.
        let s = "££££";
        assert_eq!(truncate(s, 3), "£££...");
    }
}
