//! Platform-aware directory resolution.
//!
//! Provides a cascading fallback strategy for choosing a directory path
//! using the `dirs` crate: preferred → fallback → `std::env::temp_dir`.

use std::path::PathBuf;

/// Resolves a directory path using a cascading strategy.
///
/// Tries `preferred_fn` first, then `fallback_fn`, and finally
/// `std::env::temp_dir`.  The `subdirectory` suffix is appended to
/// whichever base directory is chosen.
///
/// Returns the resolved path as a `String` and a flag indicating
/// whether `std::env::temp_dir` was used (so callers can emit a
/// warning).
pub(crate) fn resolve_directory(
    preferred_fn: fn() -> Option<PathBuf>,
    fallback_fn: fn() -> Option<PathBuf>,
    subdirectory: &str,
) -> (String, bool) {
    if let Some(dir) = preferred_fn() {
        return (dir.join(subdirectory).to_string_lossy().into_owned(), false);
    }

    if let Some(dir) = fallback_fn() {
        return (dir.join(subdirectory).to_string_lossy().into_owned(), false);
    }

    let dir = std::env::temp_dir();
    (dir.join(subdirectory).to_string_lossy().into_owned(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_uses_preferred_when_available() {
        let (path, used_temp) = resolve_directory(
            || Some(PathBuf::from("/preferred")),
            || Some(PathBuf::from("/fallback")),
            "tribal/logs",
        );
        assert_eq!(path, "/preferred/tribal/logs");
        assert!(!used_temp);
    }

    #[test]
    fn test_resolve_uses_fallback_when_preferred_absent() {
        let (path, used_temp) =
            resolve_directory(|| None, || Some(PathBuf::from("/fallback")), "tribal/logs");
        assert_eq!(path, "/fallback/tribal/logs");
        assert!(!used_temp);
    }

    #[test]
    fn test_resolve_uses_temp_dir_when_both_absent() {
        let (path, used_temp) = resolve_directory(|| None, || None, "tribal/logs");
        let expected = std::env::temp_dir().join("tribal/logs");
        assert_eq!(path, expected.to_string_lossy());
        assert!(used_temp);
    }
}
