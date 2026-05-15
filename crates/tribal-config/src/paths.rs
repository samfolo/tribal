//! Filesystem-path helpers for Tribal's user-config subdirectory.
//!
//! Every Tribal-owned file under the user's config / data / state /
//! cache base directories lives under [`TRIBAL_DIRECTORY_NAME`]; routing
//! all path construction through these helpers keeps the subdirectory
//! name a single source of truth.

use std::path::PathBuf;

use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Subdirectory name that contains every Tribal-owned file under a base
/// directory (e.g. `$XDG_CONFIG_HOME/tribal/`,
/// `$XDG_STATE_HOME/tribal/logs/`).
pub const TRIBAL_DIRECTORY_NAME: &str = "tribal";

const CREDENTIALS_FILENAME: &str = "credentials.json";
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

/// Resolves the absolute path of the credentials file under
/// `$XDG_CONFIG_HOME/tribal/credentials.json` (POSIX fallback:
/// `$HOME/.config/tribal/credentials.json`).
///
/// # Errors
///
/// Returns [`ConfigDirError::Unavailable`] when neither `XDG_CONFIG_HOME`
/// nor `HOME` is set in the environment.
pub fn credentials_file_path() -> Result<PathBuf, ConfigDirError> {
    Ok(user_config_directory()?
        .join(TRIBAL_DIRECTORY_NAME)
        .join(CREDENTIALS_FILENAME))
}

fn user_config_directory() -> Result<PathBuf, ConfigDirError> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .ok_or(ConfigDirError::Unavailable)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failure resolving a user-config-base directory.
#[derive(Debug, Error)]
pub enum ConfigDirError {
    /// Neither `XDG_CONFIG_HOME` nor `HOME` is set.
    #[error("could not resolve $XDG_CONFIG_HOME or $HOME")]
    Unavailable,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
// `Jail::expect_with` closures return `Result<(), figment::Error>` (208 bytes),
// which we cannot reduce without wrapping an upstream type.
#[allow(clippy::result_large_err)]
mod tests {
    use figment::Jail;

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

    // -- credentials_file_path ---------------------------------------------

    #[test]
    fn test_credentials_path_honours_xdg_config_home() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env("XDG_CONFIG_HOME", "/custom/xdg");

            let path = credentials_file_path().expect("path resolves");
            assert_eq!(path, PathBuf::from("/custom/xdg/tribal/credentials.json"));
            Ok(())
        });
    }

    #[test]
    fn test_credentials_path_falls_back_to_home_dotconfig() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env("HOME", "/home/sam");

            let path = credentials_file_path().expect("path resolves");
            assert_eq!(
                path,
                PathBuf::from("/home/sam/.config/tribal/credentials.json")
            );
            Ok(())
        });
    }

    #[test]
    fn test_credentials_path_errors_when_neither_env_set() {
        Jail::expect_with(|jail| {
            jail.clear_env();

            assert!(matches!(
                credentials_file_path(),
                Err(ConfigDirError::Unavailable),
            ));
            Ok(())
        });
    }
}
