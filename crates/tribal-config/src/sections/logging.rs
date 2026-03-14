//! Logging configuration types.
//!
//! [`LoggingConfig`] is loaded from the application configuration file and
//! consumed by the telemetry subscriber to build the tracing subscriber stack.

use serde::{Deserialize, Serialize};

use super::telemetry::FileRotation;
use crate::paths::resolve_directory;

// ---------------------------------------------------------------------------
// LoggingConfig
// ---------------------------------------------------------------------------

/// Configuration for the tracing subscriber.
///
/// Loaded from the application configuration file.  All fields have
/// sensible defaults for local development (info level, JSON format,
/// stderr output).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    /// Tracing filter directive string.
    ///
    /// Supports per-module granularity, e.g. `"info,tribal_db=debug"`.
    /// Defaults to `"info"`.  The `TRIBAL_LOG` environment variable,
    /// mapped by the configuration loader, takes precedence over a
    /// YAML-supplied value for this field.
    #[serde(default = "default_level")]
    pub level: String,

    /// Output format for log lines.
    ///
    /// Defaults to [`LogFormat::Json`] for structured output suitable
    /// for log aggregation.
    #[serde(default)]
    pub format: LogFormat,

    /// Output destination for log lines.
    ///
    /// Defaults to [`LogOutput::Stderr`].
    #[serde(default)]
    pub output: LogOutput,

    /// Directory for log file output when [`output`](LoggingConfig::output)
    /// is [`LogOutput::File`].
    ///
    /// Resolved at startup via platform-aware defaults:
    /// `dirs::state_dir` → `dirs::data_local_dir` → `std::env::temp_dir`,
    /// each joined with `tribal/logs`.
    #[serde(default = "default_file_directory")]
    pub file_directory: String,

    /// Log file rotation policy.
    ///
    /// Defaults to [`FileRotation::Daily`].
    #[serde(default)]
    pub file_rotation: FileRotation,

    /// Whether `std::env::temp_dir` was used as a last-resort fallback
    /// for `file_directory`.
    ///
    /// Set automatically during default construction; not serialised.
    #[serde(skip)]
    pub used_temp_dir_fallback: bool,

    /// Whether to include raw LLM request/response content in log output.
    ///
    /// Defaults to `false`.  When `true`, inference-related spans and
    /// events may include prompt text and model responses, which can be
    /// large and contain sensitive information.
    #[serde(default)]
    pub include_llm_content: bool,
}

fn default_level() -> String {
    String::from("info")
}

fn default_file_directory() -> String {
    let (dir, _) = resolve_directory(dirs::state_dir, dirs::data_local_dir, "tribal/logs");
    dir
}

impl Default for LoggingConfig {
    fn default() -> Self {
        let (file_directory, used_temp_dir_fallback) =
            resolve_directory(dirs::state_dir, dirs::data_local_dir, "tribal/logs");
        Self {
            level: default_level(),
            format: LogFormat::default(),
            output: LogOutput::default(),
            file_directory,
            file_rotation: FileRotation::default(),
            used_temp_dir_fallback,
            include_llm_content: false,
        }
    }
}

// ---------------------------------------------------------------------------
// LogFormat
// ---------------------------------------------------------------------------

/// Output format for log lines.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Structured JSON output, one object per line (JSONL).
    ///
    /// Includes level, target, span context, timestamp, and message.
    /// Suitable for log aggregation and machine parsing.
    #[default]
    Json,

    /// Human-readable coloured output for terminal use.
    ///
    /// Uses `tracing-subscriber`'s pretty formatter with ANSI colours
    /// when writing to a TTY.
    Pretty,
}

// ---------------------------------------------------------------------------
// LogOutput
// ---------------------------------------------------------------------------

/// Output destination for log lines.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogOutput {
    /// Write log output to standard error.
    #[default]
    Stderr,

    /// Write log output to a rolling file in
    /// [`LoggingConfig::file_directory`].
    File,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_log_format_serde_roundtrip, LogFormat {
        LogFormat::Json => "json",
        LogFormat::Pretty => "pretty",
    });

    enum_serde_tests!(test_log_output_serde_roundtrip, LogOutput {
        LogOutput::Stderr => "stderr",
        LogOutput::File => "file",
    });

    #[test]
    fn test_logging_config_default_values() {
        let config = LoggingConfig::default();
        assert_eq!(config.level, "info");
        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.output, LogOutput::Stderr);
        assert!(!config.file_directory.is_empty());
        assert!(config.file_directory.ends_with("tribal/logs"));
        assert_eq!(config.file_rotation, FileRotation::Daily);
        assert!(!config.include_llm_content);
    }

    #[test]
    fn test_log_format_default_is_json() {
        assert_eq!(LogFormat::default(), LogFormat::Json);
    }

    #[test]
    fn test_log_output_default_is_stderr() {
        assert_eq!(LogOutput::default(), LogOutput::Stderr);
    }

    #[test]
    fn test_deserialise_empty_object_applies_defaults() {
        let yaml = "---";
        let config: LoggingConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.level, "info");
        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.output, LogOutput::Stderr);
        assert!(config.file_directory.ends_with("tribal/logs"));
    }

    #[test]
    fn test_deserialise_all_fields() {
        let yaml = r#"
level: "debug,tribal_db=trace"
format: pretty
output: file
file_directory: /var/log/tribal
file_rotation: hourly
include_llm_content: true
"#;
        let config: LoggingConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.level, "debug,tribal_db=trace");
        assert_eq!(config.format, LogFormat::Pretty);
        assert_eq!(config.output, LogOutput::File);
        assert_eq!(config.file_directory, "/var/log/tribal");
        assert_eq!(config.file_rotation, FileRotation::Hourly);
        assert!(config.include_llm_content);
    }

    #[test]
    fn test_used_temp_dir_fallback_not_serialised() {
        let config = LoggingConfig {
            used_temp_dir_fallback: true,
            ..LoggingConfig::default()
        };
        let yaml = serde_yaml::to_string(&config).unwrap();
        assert!(
            !yaml.contains("used_temp_dir_fallback"),
            "used_temp_dir_fallback should be skipped during serialisation"
        );
    }
}
