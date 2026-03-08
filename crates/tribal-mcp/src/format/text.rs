//! Text formatting utilities for MCP response summaries.

/// Truncates `s` to at most `max` characters, appending an ellipsis if
/// truncated. Safe for multi-byte UTF-8.
pub(crate) fn truncate_content(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((idx, _)) => format!("{}…", &s[..idx]),
        None => s.to_owned(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_short_string_unchanged() {
        assert_eq!(truncate_content("hello", 100), "hello");
    }

    #[test]
    fn test_exact_length_unchanged() {
        let s = "a".repeat(100);
        assert_eq!(truncate_content(&s, 100), s);
    }

    #[test]
    fn test_long_string_truncated() {
        let s = "a".repeat(150);
        let result = truncate_content(&s, 100);
        assert!(result.starts_with(&"a".repeat(100)));
        assert!(result.ends_with('…'));
    }

    #[test]
    fn test_multibyte_characters_truncate_by_character_not_byte() {
        let s = "££££";
        assert_eq!(truncate_content(s, 3), "£££…");
    }
}
