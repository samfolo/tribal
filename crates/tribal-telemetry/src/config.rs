//! Telemetry configuration types.
//!
//! [`LoggingConfig`] is loaded from the application configuration file and
//! consumed by [`init_subscriber`](crate::init_subscriber) to build the
//! tracing subscriber stack.

use serde::Deserialize;

/// Configuration for the tracing subscriber.
///
/// Loaded from the application configuration file.  All fields have
/// sensible defaults for local development (info level, JSON format,
/// stderr output).
///
/// The `TRIBAL_LOG` environment variable, when set, overrides the
/// [`level`](LoggingConfig::level) field entirely.  This allows runtime
/// log level changes without modifying the configuration file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LoggingConfig {
    /// Tracing filter directive string.
    ///
    /// Supports per-module granularity, e.g. `"info,tribal_db=debug"`.
    /// Defaults to `"info"`.
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

    /// Path to the log file when [`output`](LoggingConfig::output) is
    /// [`LogOutput::File`].
    ///
    /// Ignored when output is `Stderr`.  Required when output is `File`;
    /// omitting it produces
    /// [`TelemetryError::FileOutputMissingPath`](crate::TelemetryError::FileOutputMissingPath).
    #[serde(default)]
    pub file_path: Option<String>,

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

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_level(),
            format: LogFormat::default(),
            output: LogOutput::default(),
            file_path: None,
            include_llm_content: false,
        }
    }
}

/// Output format for log lines.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
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

/// Output destination for log lines.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogOutput {
    /// Write log output to standard error.
    #[default]
    Stderr,

    /// Write log output to a file specified by
    /// [`LoggingConfig::file_path`].
    File,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_config_default_values() {
        let config = LoggingConfig::default();
        assert_eq!(config.level, "info");
        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.output, LogOutput::Stderr);
        assert_eq!(config.file_path, None);
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
        let config: LoggingConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config, LoggingConfig::default());
    }

    #[test]
    fn test_deserialise_all_fields() {
        let json = r#"{
            "level": "debug,tribal_db=trace",
            "format": "pretty",
            "output": "file",
            "file_path": "/var/log/tribal.jsonl",
            "include_llm_content": true
        }"#;
        let config: LoggingConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.level, "debug,tribal_db=trace");
        assert_eq!(config.format, LogFormat::Pretty);
        assert_eq!(config.output, LogOutput::File);
        assert_eq!(config.file_path.as_deref(), Some("/var/log/tribal.jsonl"));
        assert!(config.include_llm_content);
    }

    /// Roundtrip-tests every `LogFormat` variant through serde.
    ///
    /// The `match` is a compile-time exhaustiveness guard: adding a
    /// variant without listing it here fails the build.
    #[test]
    fn test_deserialise_log_format_variants() {
        // Exhaustiveness guard — adding a variant without a test case
        // produces a compile error here.
        #[allow(dead_code)]
        fn check_exhaustiveness(v: LogFormat) {
            match v {
                LogFormat::Json | LogFormat::Pretty => {}
            }
        }

        let cases: &[(LogFormat, &str)] = &[
            (LogFormat::Json, r#""json""#),
            (LogFormat::Pretty, r#""pretty""#),
        ];
        for &(expected, json) in cases {
            let parsed: LogFormat = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, expected, "deserialising {json}");
        }
    }

    /// Roundtrip-tests every `LogOutput` variant through serde.
    ///
    /// The `match` is a compile-time exhaustiveness guard: adding a
    /// variant without listing it here fails the build.
    #[test]
    fn test_deserialise_log_output_variants() {
        #[allow(dead_code)]
        fn check_exhaustiveness(v: LogOutput) {
            match v {
                LogOutput::Stderr | LogOutput::File => {}
            }
        }

        let cases: &[(LogOutput, &str)] = &[
            (LogOutput::Stderr, r#""stderr""#),
            (LogOutput::File, r#""file""#),
        ];
        for &(expected, json) in cases {
            let parsed: LogOutput = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, expected, "deserialising {json}");
        }
    }
}
