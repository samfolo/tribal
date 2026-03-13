//! Prompt file configuration.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default prompt template directory.
pub const DEFAULT_DIRECTORY: &str = "~/.config/tribal/prompts";

fn default_directory() -> String {
    String::from(DEFAULT_DIRECTORY)
}

// ---------------------------------------------------------------------------
// PromptsConfig
// ---------------------------------------------------------------------------

/// Prompt file location and hot-reload settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptsConfig {
    /// Directory containing prompt template files.
    ///
    /// Supports tilde expansion (`~`).
    #[serde(default = "default_directory")]
    pub directory: String,

    /// Whether to watch for file changes and reload automatically.
    ///
    /// The hot-reload feature itself is deferred; this field is defined
    /// per the configuration schema.
    #[serde(default)]
    pub hot_reload: bool,
}

impl Default for PromptsConfig {
    fn default() -> Self {
        Self {
            directory: default_directory(),
            hot_reload: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = PromptsConfig::default();
        assert_eq!(config.directory, DEFAULT_DIRECTORY);
        assert!(!config.hot_reload);
    }
}
