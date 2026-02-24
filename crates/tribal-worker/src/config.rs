//! Worker configuration with validation and duration helpers.

use std::time::Duration;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ConfigError
// ---------------------------------------------------------------------------

/// Errors produced when validating a [`WorkerConfig`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A configuration field has an invalid value.
    #[error("invalid config: {field}: {reason}")]
    InvalidField {
        /// The field that failed validation.
        field: &'static str,
        /// Why the value is invalid.
        reason: &'static str,
    },
}

// ---------------------------------------------------------------------------
// WorkerConfig
// ---------------------------------------------------------------------------

/// Configuration for the worker loop.
///
/// All duration fields are expressed as integer seconds and converted to
/// [`Duration`] via convenience methods.  Defaults are applied via
/// `serde(default)` so that an empty JSON object deserialises to a valid
/// configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerConfig {
    /// Maximum number of concurrently executing tasks.
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,

    /// Seconds between poll cycles.
    #[serde(default = "default_poll_interval_seconds")]
    pub poll_interval_seconds: u64,

    /// Per-task timeout in seconds.
    #[serde(default = "default_task_timeout_seconds")]
    pub task_timeout_seconds: u64,

    /// Maximum retries before a task is dead-lettered.
    #[serde(default = "default_task_max_retries")]
    pub task_max_retries: u32,

    /// Seconds between heartbeat updates for claimed tasks.
    #[serde(default = "default_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: u64,

    /// Seconds between stale-task reclaim sweeps.
    #[serde(default = "default_reclaim_interval_seconds")]
    pub reclaim_interval_seconds: u64,

    /// Maximum candidate count per job (cap applied during extraction).
    #[serde(default = "default_max_candidates_per_job")]
    pub max_candidates_per_job: u32,

    /// Number of similar items returned during triage search.
    #[serde(default = "default_triage_search_limit")]
    pub triage_search_limit: u32,

    /// Whether to include raw LLM content in debug log output.
    #[serde(default)]
    pub include_llm_content: bool,
}

impl WorkerConfig {
    /// Returns the poll interval as a [`Duration`].
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_seconds)
    }

    /// Returns the task timeout as a [`Duration`].
    #[must_use]
    pub fn task_timeout(&self) -> Duration {
        Duration::from_secs(self.task_timeout_seconds)
    }

    /// Returns the heartbeat interval as a [`Duration`].
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.heartbeat_interval_seconds)
    }

    /// Returns the reclaim interval as a [`Duration`].
    #[must_use]
    pub fn reclaim_interval(&self) -> Duration {
        Duration::from_secs(self.reclaim_interval_seconds)
    }

    /// Validates the configuration, returning an error on the first
    /// invalid field.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidField`] if any field violates its
    /// constraint.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_concurrent_tasks == 0 {
            return Err(ConfigError::InvalidField {
                field: "max_concurrent_tasks",
                reason: "must be greater than zero",
            });
        }
        if self.task_timeout_seconds == 0 {
            return Err(ConfigError::InvalidField {
                field: "task_timeout_seconds",
                reason: "must be greater than zero",
            });
        }
        if self.heartbeat_interval_seconds >= self.task_timeout_seconds {
            return Err(ConfigError::InvalidField {
                field: "heartbeat_interval_seconds",
                reason: "must be less than task_timeout_seconds",
            });
        }
        if self.reclaim_interval_seconds < self.poll_interval_seconds {
            return Err(ConfigError::InvalidField {
                field: "reclaim_interval_seconds",
                reason: "must be greater than or equal to poll_interval_seconds",
            });
        }
        if self.triage_search_limit == 0 {
            return Err(ConfigError::InvalidField {
                field: "triage_search_limit",
                reason: "must be greater than zero",
            });
        }
        if self.max_candidates_per_job == 0 {
            return Err(ConfigError::InvalidField {
                field: "max_candidates_per_job",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: default_max_concurrent_tasks(),
            poll_interval_seconds: default_poll_interval_seconds(),
            task_timeout_seconds: default_task_timeout_seconds(),
            task_max_retries: default_task_max_retries(),
            heartbeat_interval_seconds: default_heartbeat_interval_seconds(),
            reclaim_interval_seconds: default_reclaim_interval_seconds(),
            max_candidates_per_job: default_max_candidates_per_job(),
            triage_search_limit: default_triage_search_limit(),
            include_llm_content: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const fn default_max_concurrent_tasks() -> usize {
    4
}
const fn default_poll_interval_seconds() -> u64 {
    2
}
const fn default_task_timeout_seconds() -> u64 {
    300
}
const fn default_task_max_retries() -> u32 {
    3
}
const fn default_heartbeat_interval_seconds() -> u64 {
    100
}
const fn default_reclaim_interval_seconds() -> u64 {
    10
}
const fn default_max_candidates_per_job() -> u32 {
    20
}
const fn default_triage_search_limit() -> u32 {
    10
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config: WorkerConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(config.max_concurrent_tasks, 4);
        assert_eq!(config.poll_interval_seconds, 2);
        assert_eq!(config.task_timeout_seconds, 300);
        assert_eq!(config.task_max_retries, 3);
        assert_eq!(config.heartbeat_interval_seconds, 100);
        assert_eq!(config.reclaim_interval_seconds, 10);
        assert_eq!(config.max_candidates_per_job, 20);
        assert_eq!(config.triage_search_limit, 10);
        assert!(!config.include_llm_content);
    }

    #[test]
    fn test_duration_conversions() {
        let config = WorkerConfig::default();
        assert_eq!(config.poll_interval(), Duration::from_secs(2));
        assert_eq!(config.task_timeout(), Duration::from_secs(300));
        assert_eq!(config.heartbeat_interval(), Duration::from_secs(100));
        assert_eq!(config.reclaim_interval(), Duration::from_secs(10));
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = WorkerConfig {
            max_concurrent_tasks: 8,
            poll_interval_seconds: 5,
            task_timeout_seconds: 600,
            task_max_retries: 5,
            heartbeat_interval_seconds: 200,
            reclaim_interval_seconds: 20,
            max_candidates_per_job: 50,
            triage_search_limit: 25,
            include_llm_content: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: WorkerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_validate_rejects_zero_max_concurrent_tasks() {
        let config = WorkerConfig {
            max_concurrent_tasks: 0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_concurrent_tasks"));
    }

    #[test]
    fn test_validate_rejects_zero_task_timeout() {
        let config = WorkerConfig {
            task_timeout_seconds: 0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("task_timeout_seconds"));
    }

    #[test]
    fn test_validate_rejects_heartbeat_ge_timeout() {
        let config = WorkerConfig {
            heartbeat_interval_seconds: 300,
            task_timeout_seconds: 300,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("heartbeat_interval_seconds"));
    }

    #[test]
    fn test_validate_rejects_reclaim_lt_poll() {
        let config = WorkerConfig {
            reclaim_interval_seconds: 1,
            poll_interval_seconds: 2,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("reclaim_interval_seconds"));
    }

    #[test]
    fn test_validate_rejects_zero_triage_search_limit() {
        let config = WorkerConfig {
            triage_search_limit: 0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("triage_search_limit"));
    }

    #[test]
    fn test_validate_rejects_zero_max_candidates() {
        let config = WorkerConfig {
            max_candidates_per_job: 0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("max_candidates_per_job"));
    }

    #[test]
    fn test_validate_accepts_valid_config() {
        let config = WorkerConfig::default();
        assert!(config.validate().is_ok());
    }
}
