//! Filesystem-path helpers for Tribal's user-config subdirectory.
//!
//! Every Tribal-owned file under the user's config / data / state /
//! cache base directories lives under [`TRIBAL_DIRECTORY_NAME`]; routing
//! all path construction through these helpers keeps the subdirectory
//! name a single source of truth.

use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Subdirectory name that contains every Tribal-owned file under a base
/// directory (e.g. `$XDG_CONFIG_HOME/tribal/`,
/// `$XDG_STATE_HOME/tribal/logs/`).
pub const TRIBAL_DIRECTORY_NAME: &str = "tribal";

const CONFIG_FILENAME: &str = "tribal.yaml";
const PROMPTS_DIRECTORY_NAME: &str = "prompts";

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Resolves a Tribal-owned subdirectory under one of the supplied base
/// directories, cascading `preferred_fn` → `fallback_fn` →
/// [`std::env::temp_dir`]. The [`TRIBAL_DIRECTORY_NAME`] prefix is
/// prepended to `leaf` so callers pass only the inner leaf (e.g.
/// `"logs"`, `"traces"`).
///
/// Returns the resolved path as a string plus a flag indicating whether
/// the [`std::env::temp_dir`] fallback was used.
pub(crate) fn resolve_directory(
    preferred_fn: fn() -> Option<PathBuf>,
    fallback_fn: fn() -> Option<PathBuf>,
    leaf: &str,
) -> (String, bool) {
    let subdirectory = PathBuf::from(TRIBAL_DIRECTORY_NAME).join(leaf);

    if let Some(dir) = preferred_fn() {
        return (
            dir.join(&subdirectory).to_string_lossy().into_owned(),
            false,
        );
    }

    if let Some(dir) = fallback_fn() {
        return (
            dir.join(&subdirectory).to_string_lossy().into_owned(),
            false,
        );
    }

    let dir = std::env::temp_dir();
    (dir.join(&subdirectory).to_string_lossy().into_owned(), true)
}

/// Default path of the user's Tribal config file, expressed with a
/// leading tilde for later expansion by `shellexpand`.
#[must_use]
pub fn default_config_file_path() -> String {
    format!("~/.config/{TRIBAL_DIRECTORY_NAME}/{CONFIG_FILENAME}")
}

/// Default path of the user's on-disk prompts directory, expressed with
/// a leading tilde for later expansion by `shellexpand`.
#[must_use]
pub fn default_prompts_directory() -> String {
    format!("~/.config/{TRIBAL_DIRECTORY_NAME}/{PROMPTS_DIRECTORY_NAME}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- resolve_directory --------------------------------------------------

    #[test]
    fn test_resolve_uses_preferred_when_available() {
        let (path, used_temp) = resolve_directory(
            || Some(PathBuf::from("/preferred")),
            || Some(PathBuf::from("/fallback")),
            "logs",
        );
        assert_eq!(path, "/preferred/tribal/logs");
        assert!(!used_temp);
    }

    #[test]
    fn test_resolve_uses_fallback_when_preferred_absent() {
        let (path, used_temp) =
            resolve_directory(|| None, || Some(PathBuf::from("/fallback")), "logs");
        assert_eq!(path, "/fallback/tribal/logs");
        assert!(!used_temp);
    }

    #[test]
    fn test_resolve_uses_temp_dir_when_both_absent() {
        let (path, used_temp) = resolve_directory(|| None, || None, "logs");
        let expected = std::env::temp_dir().join("tribal").join("logs");
        assert_eq!(path, expected.to_string_lossy());
        assert!(used_temp);
    }

    // -- default path helpers ----------------------------------------------

    #[test]
    fn test_default_config_file_path_uses_tribal_directory() {
        assert_eq!(
            default_config_file_path(),
            "~/.config/tribal/tribal.yaml".to_string(),
        );
    }

    #[test]
    fn test_default_prompts_directory_uses_tribal_directory() {
        assert_eq!(
            default_prompts_directory(),
            "~/.config/tribal/prompts".to_string(),
        );
    }
}
