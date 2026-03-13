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
/// All duration fields are expressed as integer milliseconds and converted
/// to [`Duration`] via convenience methods.  Defaults are applied via
/// `serde(default)` so that an empty JSON object deserialises to a valid
/// configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    /// Maximum number of concurrently executing tasks.
    #[serde(default = "default_max_concurrent_tasks")]
    pub max_concurrent_tasks: usize,

    /// Milliseconds between poll cycles.
    #[serde(default = "default_poll_interval_millis")]
    pub poll_interval_millis: u64,

    /// Per-task timeout in milliseconds.
    #[serde(default = "default_task_timeout_millis")]
    pub task_timeout_millis: u64,

    /// Maximum retries before a task is dead-lettered.
    #[serde(default = "default_task_max_retries")]
    pub task_max_retries: u32,

    /// Milliseconds between heartbeat updates for claimed tasks.
    #[serde(default = "default_heartbeat_interval_millis")]
    pub heartbeat_interval_millis: u64,

    /// Milliseconds between stale-task reclaim sweeps.
    #[serde(default = "default_reclaim_interval_millis")]
    pub reclaim_interval_millis: u64,

    /// Maximum candidate count per job (cap applied during extraction).
    #[serde(default = "default_max_candidates_per_job")]
    pub max_candidates_per_job: u32,

    /// Number of similar items returned during triage search.
    #[serde(default = "default_triage_search_limit")]
    pub triage_search_limit: u32,

    /// Minimum cosine similarity for semantic tag matching (0.0, 1.0].
    #[serde(default = "default_tag_similarity_threshold")]
    pub tag_similarity_threshold: f64,

    /// Whether to include raw LLM content in debug log output.
    #[serde(default)]
    pub include_llm_content: bool,
}

impl WorkerConfig {
    /// Returns the poll interval as a [`Duration`].
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_millis)
    }

    /// Returns the task timeout as a [`Duration`].
    #[must_use]
    pub fn task_timeout(&self) -> Duration {
        Duration::from_millis(self.task_timeout_millis)
    }

    /// Returns the heartbeat interval as a [`Duration`].
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_millis)
    }

    /// Returns the reclaim interval as a [`Duration`].
    #[must_use]
    pub fn reclaim_interval(&self) -> Duration {
        Duration::from_millis(self.reclaim_interval_millis)
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
        if self.poll_interval_millis == 0 {
            return Err(ConfigError::InvalidField {
                field: "poll_interval_millis",
                reason: "must be greater than zero",
            });
        }
        if self.task_timeout_millis == 0 {
            return Err(ConfigError::InvalidField {
                field: "task_timeout_millis",
                reason: "must be greater than zero",
            });
        }
        if self.heartbeat_interval_millis == 0 {
            return Err(ConfigError::InvalidField {
                field: "heartbeat_interval_millis",
                reason: "must be greater than zero",
            });
        }
        if self.heartbeat_interval_millis >= self.task_timeout_millis {
            return Err(ConfigError::InvalidField {
                field: "heartbeat_interval_millis",
                reason: "must be less than task_timeout_millis",
            });
        }
        if self.reclaim_interval_millis < self.poll_interval_millis {
            return Err(ConfigError::InvalidField {
                field: "reclaim_interval_millis",
                reason: "must be greater than or equal to poll_interval_millis",
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
        if self.tag_similarity_threshold <= 0.0 || self.tag_similarity_threshold > 1.0 {
            return Err(ConfigError::InvalidField {
                field: "tag_similarity_threshold",
                reason: "must be in the range (0.0, 1.0]",
            });
        }
        Ok(())
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: default_max_concurrent_tasks(),
            poll_interval_millis: default_poll_interval_millis(),
            task_timeout_millis: default_task_timeout_millis(),
            task_max_retries: default_task_max_retries(),
            heartbeat_interval_millis: default_heartbeat_interval_millis(),
            reclaim_interval_millis: default_reclaim_interval_millis(),
            max_candidates_per_job: default_max_candidates_per_job(),
            triage_search_limit: default_triage_search_limit(),
            tag_similarity_threshold: default_tag_similarity_threshold(),
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
const fn default_poll_interval_millis() -> u64 {
    2_000
}
const fn default_task_timeout_millis() -> u64 {
    300_000
}
const fn default_task_max_retries() -> u32 {
    3
}
const fn default_heartbeat_interval_millis() -> u64 {
    100_000
}
const fn default_reclaim_interval_millis() -> u64 {
    10_000
}
const fn default_max_candidates_per_job() -> u32 {
    20
}
const fn default_triage_search_limit() -> u32 {
    10
}
fn default_tag_similarity_threshold() -> f64 {
    0.85
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
        assert_eq!(config.poll_interval_millis, 2_000);
        assert_eq!(config.task_timeout_millis, 300_000);
        assert_eq!(config.task_max_retries, 3);
        assert_eq!(config.heartbeat_interval_millis, 100_000);
        assert_eq!(config.reclaim_interval_millis, 10_000);
        assert_eq!(config.max_candidates_per_job, 20);
        assert_eq!(config.triage_search_limit, 10);
        assert!((config.tag_similarity_threshold - 0.85).abs() < f64::EPSILON);
        assert!(!config.include_llm_content);
    }

    #[test]
    fn test_duration_conversions() {
        let config = WorkerConfig::default();
        assert_eq!(config.poll_interval(), Duration::from_millis(2_000));
        assert_eq!(config.task_timeout(), Duration::from_millis(300_000));
        assert_eq!(config.heartbeat_interval(), Duration::from_millis(100_000));
        assert_eq!(config.reclaim_interval(), Duration::from_millis(10_000));
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = WorkerConfig {
            max_concurrent_tasks: 8,
            poll_interval_millis: 5_000,
            task_timeout_millis: 600_000,
            task_max_retries: 5,
            heartbeat_interval_millis: 200_000,
            reclaim_interval_millis: 20_000,
            max_candidates_per_job: 50,
            triage_search_limit: 25,
            tag_similarity_threshold: 0.9,
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
    fn test_validate_rejects_zero_poll_interval() {
        let config = WorkerConfig {
            poll_interval_millis: 0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("poll_interval_millis"));
    }

    #[test]
    fn test_validate_rejects_zero_task_timeout() {
        let config = WorkerConfig {
            task_timeout_millis: 0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("task_timeout_millis"));
    }

    #[test]
    fn test_validate_rejects_zero_heartbeat_interval() {
        let config = WorkerConfig {
            heartbeat_interval_millis: 0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("heartbeat_interval_millis"));
    }

    #[test]
    fn test_validate_rejects_heartbeat_ge_timeout() {
        let config = WorkerConfig {
            heartbeat_interval_millis: 300_000,
            task_timeout_millis: 300_000,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("heartbeat_interval_millis"));
    }

    #[test]
    fn test_validate_rejects_reclaim_lt_poll() {
        let config = WorkerConfig {
            reclaim_interval_millis: 1_000,
            poll_interval_millis: 2_000,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("reclaim_interval_millis"));
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
    fn test_validate_rejects_zero_tag_similarity_threshold() {
        let config = WorkerConfig {
            tag_similarity_threshold: 0.0,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("tag_similarity_threshold"));
    }

    #[test]
    fn test_validate_rejects_negative_tag_similarity_threshold() {
        let config = WorkerConfig {
            tag_similarity_threshold: -0.1,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("tag_similarity_threshold"));
    }

    #[test]
    fn test_validate_rejects_tag_similarity_threshold_above_one() {
        let config = WorkerConfig {
            tag_similarity_threshold: 1.01,
            ..WorkerConfig::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("tag_similarity_threshold"));
    }

    #[test]
    fn test_validate_accepts_tag_similarity_threshold_of_one() {
        let config = WorkerConfig {
            tag_similarity_threshold: 1.0,
            ..WorkerConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_accepts_valid_config() {
        let config = WorkerConfig::default();
        assert!(config.validate().is_ok());
    }
}
