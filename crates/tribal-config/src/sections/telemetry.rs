//! OpenTelemetry and trace export configuration.

use serde::{Deserialize, Serialize};

use crate::paths::resolve_directory;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default OTLP protocol.
pub const DEFAULT_OTLP_PROTOCOL: &str = "grpc";

/// Default service name for telemetry spans.
pub const DEFAULT_SERVICE_NAME: &str = "tribal";

// ---------------------------------------------------------------------------
// FileRotation
// ---------------------------------------------------------------------------

/// Trace file rotation policy.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileRotation {
    /// Rotate daily (e.g. `traces-2026-03-13.jsonl`).
    #[default]
    Daily,

    /// Rotate hourly.
    Hourly,

    /// No rotation — single file.
    Never,
}

// ---------------------------------------------------------------------------
// TelemetryConfig
// ---------------------------------------------------------------------------

/// OpenTelemetry and trace export configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// Master switch for telemetry.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// OTLP exporter endpoint (e.g. `http://localhost:4317`).
    #[serde(default)]
    pub otlp_endpoint: Option<String>,

    /// OTLP protocol (`grpc` or `http`).
    #[serde(default = "default_otlp_protocol")]
    pub otlp_protocol: String,

    /// Service name reported in spans.
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Print spans to stderr (development).
    #[serde(default = "default_console_export")]
    pub console_export: bool,

    /// Write spans to file.
    #[serde(default)]
    pub file_export: bool,

    /// Directory for trace file output.
    ///
    /// Resolved at startup via platform-aware defaults:
    /// `dirs::data_local_dir` → `std::env::temp_dir`,
    /// each joined with `tribal/traces`.
    #[serde(default = "default_file_directory")]
    pub file_directory: String,

    /// Trace file rotation policy.
    #[serde(default)]
    pub file_rotation: FileRotation,

    /// Whether `std::env::temp_dir` was used as a last-resort fallback
    /// for `file_directory`.
    ///
    /// Set automatically during default construction; not serialised.
    #[serde(skip)]
    pub used_temp_dir_fallback: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        let (file_directory, used_temp_dir_fallback) =
            resolve_directory(dirs::data_local_dir, || None, "traces");
        Self {
            enabled: default_enabled(),
            otlp_endpoint: None,
            otlp_protocol: default_otlp_protocol(),
            service_name: default_service_name(),
            console_export: default_console_export(),
            file_export: false,
            file_directory,
            used_temp_dir_fallback,
            file_rotation: FileRotation::default(),
        }
    }
}

const fn default_enabled() -> bool {
    true
}

fn default_otlp_protocol() -> String {
    String::from(DEFAULT_OTLP_PROTOCOL)
}

fn default_service_name() -> String {
    String::from(DEFAULT_SERVICE_NAME)
}

const fn default_console_export() -> bool {
    true
}

fn default_file_directory() -> String {
    let (dir, _) = resolve_directory(dirs::data_local_dir, dirs::data_local_dir, "traces");
    dir
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_file_rotation_serde_roundtrip, FileRotation {
        FileRotation::Daily => "daily",
        FileRotation::Hourly => "hourly",
        FileRotation::Never => "never",
    });

    #[test]
    fn test_file_rotation_default_is_daily() {
        assert_eq!(FileRotation::default(), FileRotation::Daily);
    }

    #[test]
    fn test_default_values() {
        let config = TelemetryConfig::default();
        assert!(config.enabled);
        assert_eq!(config.otlp_endpoint, None);
        assert_eq!(config.otlp_protocol, DEFAULT_OTLP_PROTOCOL);
        assert_eq!(config.service_name, DEFAULT_SERVICE_NAME);
        assert!(config.console_export);
        assert!(!config.file_export);
        assert!(!config.file_directory.is_empty());
        assert!(config.file_directory.ends_with("traces"));
        assert_eq!(config.file_rotation, FileRotation::Daily);
    }

    #[test]
    fn test_used_temp_dir_fallback_not_serialised() {
        let config = TelemetryConfig {
            used_temp_dir_fallback: true,
            ..TelemetryConfig::default()
        };
        let yaml = serde_yaml::to_string(&config).unwrap();
        assert!(
            !yaml.contains("used_temp_dir_fallback"),
            "used_temp_dir_fallback should be skipped during serialisation"
        );
    }
}
