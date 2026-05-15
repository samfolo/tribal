//! Prompt source configuration.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default prompt template directory used by [`PromptSource::Disk`].
pub const DEFAULT_DIRECTORY: &str = "~/.config/tribal/prompts";

fn default_directory() -> String {
    String::from(DEFAULT_DIRECTORY)
}

// ---------------------------------------------------------------------------
// PromptSource
// ---------------------------------------------------------------------------

/// Where the active prompts are loaded from.
///
/// `Embedded` reads the six prompts compiled into the binary via
/// `include_str!`. `Disk` reads them from `directory`, writing the
/// embedded defaults on first run if files are missing; setting
/// `hot_reload: true` spawns a watcher that reloads on file change.
///
/// The shape makes invalid states unrepresentable — `hot_reload` is only
/// reachable when an on-disk source actually exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PromptSource {
    /// Prompts compiled into the binary.
    ///
    /// Empty struct variant rather than a unit variant: serde's
    /// `deny_unknown_fields` is only honoured for struct-variant arms of
    /// internally-tagged enums, so the empty braces are what make
    /// `{kind: embedded, hot_reload: true}` fail to deserialise.
    Embedded {},

    /// Prompts read from a directory on disk.
    Disk {
        /// Directory containing prompt template files. Supports tilde (`~`).
        #[serde(default = "default_directory")]
        directory: String,

        /// Whether to watch for file changes and reload automatically.
        #[serde(default)]
        hot_reload: bool,
    },
}

impl Default for PromptSource {
    fn default() -> Self {
        Self::Embedded {}
    }
}

// ---------------------------------------------------------------------------
// PromptsConfig
// ---------------------------------------------------------------------------

/// Wrapper around [`PromptSource`].
///
/// A struct rather than an inline enum so future prompt-wide knobs (e.g.
/// global cache settings) can land without breaking the YAML schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PromptsConfig {
    /// Where the active prompts are loaded from.
    #[serde(default)]
    pub source: PromptSource,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prompts_config_default_is_embedded() {
        let config = PromptsConfig::default();
        assert_eq!(config.source, PromptSource::Embedded {});
    }

    #[test]
    fn test_prompt_source_serde_roundtrip_embedded() {
        let yaml = "kind: embedded\n";
        let parsed: PromptSource = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed, PromptSource::Embedded {});

        let serialised = serde_yaml::to_string(&PromptSource::Embedded {}).unwrap();
        let reparsed: PromptSource = serde_yaml::from_str(&serialised).unwrap();
        assert_eq!(reparsed, PromptSource::Embedded {});
    }

    #[test]
    fn test_prompt_source_serde_roundtrip_disk_default_hot_reload() {
        let yaml = "kind: disk\ndirectory: /tmp/prompts\n";
        let parsed: PromptSource = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            parsed,
            PromptSource::Disk {
                directory: "/tmp/prompts".into(),
                hot_reload: false,
            },
        );
    }

    #[test]
    fn test_prompt_source_serde_roundtrip_disk_hot_reload_true() {
        let yaml = "kind: disk\ndirectory: /tmp/prompts\nhot_reload: true\n";
        let parsed: PromptSource = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            parsed,
            PromptSource::Disk {
                directory: "/tmp/prompts".into(),
                hot_reload: true,
            },
        );
    }

    #[test]
    fn test_prompt_source_disk_omits_directory_uses_default() {
        let yaml = "kind: disk\n";
        let parsed: PromptSource = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            parsed,
            PromptSource::Disk {
                directory: DEFAULT_DIRECTORY.into(),
                hot_reload: false,
            },
        );
    }

    #[test]
    fn test_prompt_source_rejects_hot_reload_on_embedded() {
        let yaml = "kind: embedded\nhot_reload: true\n";
        let result: Result<PromptSource, _> = serde_yaml::from_str(yaml);
        assert!(
            result.is_err(),
            "embedded variant must reject hot_reload field",
        );
    }
}
