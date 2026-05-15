//! Word-aware text wrapping.
//!
//! Thin wrapper over [`textwrap`] so the rest of the CLI sees only
//! one wrap entry point. Default features (`smawk`,
//! `unicode-linebreak`, `unicode-width`) give optimal-fit line breaks
//! with grapheme-correct width measurement.

use std::borrow::Cow;

/// Wrap `text` so no line exceeds `width` display columns. Returns
/// one [`String`] per line in the wrapped output. Empty input yields
/// an empty vector.
#[must_use]
pub fn wrap(text: &str, width: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    textwrap::wrap(text, width)
        .into_iter()
        .map(Cow::into_owned)
        .collect()
}
