//! Worker configuration with validation and duration helpers.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    config_section,
    validation::{
        ConfigPath, Diagnostics, FieldValue, OrderRelation, SIMILARITY_RANGE, ValidationError,
    },
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default maximum number of concurrently executing tasks.
pub const DEFAULT_MAX_CONCURRENT_TASKS: usize = 4;

/// Default milliseconds between poll cycles.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 2_000;

/// Default per-task timeout in milliseconds.
pub const DEFAULT_TASK_TIMEOUT_MS: u64 = 300_000;

/// Default maximum retries before a task is dead-lettered.
pub const DEFAULT_TASK_MAX_RETRIES: u32 = 3;

/// Default milliseconds between heartbeat updates for claimed tasks.
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 100_000;

/// Default milliseconds between stale-task reclaim sweeps.
pub const DEFAULT_RECLAIM_INTERVAL_MS: u64 = 10_000;

/// Default maximum candidate count per job.
pub const DEFAULT_MAX_CANDIDATES_PER_JOB: u32 = 20;

/// Default number of similar items returned during triage search.
pub const DEFAULT_TRIAGE_SEARCH_LIMIT: u32 = 10;

/// Default minimum cosine similarity for semantic tag matching.
pub const DEFAULT_TAG_SIMILARITY_THRESHOLD: f64 = 0.85;

// ---------------------------------------------------------------------------
// WorkerConfig
// ---------------------------------------------------------------------------

config_section! {
    /// Configuration for the worker loop.
    ///
    /// All duration fields are expressed as integer milliseconds and converted
    /// to [`Duration`] via convenience methods.  Defaults are applied via
    /// `serde(default)` so that an empty YAML object deserialises to a valid
    /// configuration.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct WorkerConfig {
        /// Maximum number of concurrently executing tasks.
        #[serde(default = "default_max_concurrent_tasks")]
        pub max_concurrent_tasks: usize,

        /// Milliseconds between poll cycles.
        #[serde(default = "default_poll_interval_ms")]
        pub poll_interval_ms: u64,

        /// Per-task timeout in milliseconds.
        #[serde(default = "default_task_timeout_ms")]
        pub task_timeout_ms: u64,

        /// Maximum retries before a task is dead-lettered.
        #[serde(default = "default_task_max_retries")]
        pub task_max_retries: u32,

        /// Milliseconds between heartbeat updates for claimed tasks.
        #[serde(default = "default_heartbeat_interval_ms")]
        pub heartbeat_interval_ms: u64,

        /// Milliseconds between stale-task reclaim sweeps.
        #[serde(default = "default_reclaim_interval_ms")]
        pub reclaim_interval_ms: u64,

        /// Maximum candidate count per job (cap applied during extraction).
        #[serde(default = "default_max_candidates_per_job")]
        pub max_candidates_per_job: u32,

        /// Number of similar items returned during triage search.
        #[serde(default = "default_triage_search_limit")]
        pub triage_search_limit: u32,

        /// Minimum cosine similarity for semantic tag matching (0.0, 1.0].
        #[serde(default = "default_tag_similarity_threshold")]
        pub tag_similarity_threshold: f64,
    }
}

impl WorkerConfig {
    /// Returns the poll interval as a [`Duration`].
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    /// Returns the task timeout as a [`Duration`].
    #[must_use]
    pub fn task_timeout(&self) -> Duration {
        Duration::from_millis(self.task_timeout_ms)
    }

    /// Returns the heartbeat interval as a [`Duration`].
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms)
    }

    /// Returns the reclaim interval as a [`Duration`].
    #[must_use]
    pub fn reclaim_interval(&self) -> Duration {
        Duration::from_millis(self.reclaim_interval_ms)
    }

    /// Validates the configuration, pushing one [`ValidationError`] per
    /// invariant violation.  Same exhaustive-collection pattern as the
    /// section validators in `validation.rs`.
    pub(crate) fn validate(&self, diags: &mut Diagnostics) {
        let max_concurrent_tasks = u64::try_from(self.max_concurrent_tasks).unwrap_or(u64::MAX);

        if max_concurrent_tasks == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                "worker.max_concurrent_tasks",
            )));
        }
        if self.poll_interval_ms == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                "worker.poll_interval_ms",
            )));
        }
        if self.task_timeout_ms == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                "worker.task_timeout_ms",
            )));
        }
        if self.heartbeat_interval_ms == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                "worker.heartbeat_interval_ms",
            )));
        }
        if self.heartbeat_interval_ms >= self.task_timeout_ms {
            diags.push(ValidationError::FieldOrdering {
                subject: FieldValue {
                    field: ConfigPath::from_static("worker.heartbeat_interval_ms"),
                    value: self.heartbeat_interval_ms,
                },
                bound: FieldValue {
                    field: ConfigPath::from_static("worker.task_timeout_ms"),
                    value: self.task_timeout_ms,
                },
                relation: OrderRelation::LessThan,
            });
        }
        if self.reclaim_interval_ms < self.poll_interval_ms {
            diags.push(ValidationError::FieldOrdering {
                subject: FieldValue {
                    field: ConfigPath::from_static("worker.reclaim_interval_ms"),
                    value: self.reclaim_interval_ms,
                },
                bound: FieldValue {
                    field: ConfigPath::from_static("worker.poll_interval_ms"),
                    value: self.poll_interval_ms,
                },
                relation: OrderRelation::AtLeast,
            });
        }
        if self.triage_search_limit == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                "worker.triage_search_limit",
            )));
        }
        if self.max_candidates_per_job == 0 {
            diags.push(ValidationError::must_be_positive(ConfigPath::from_static(
                "worker.max_candidates_per_job",
            )));
        }
        if self.tag_similarity_threshold <= 0.0 || self.tag_similarity_threshold > 1.0 {
            diags.push(ValidationError::OutOfRange {
                field: ConfigPath::from_static("worker.tag_similarity_threshold"),
                value: self.tag_similarity_threshold,
                range: SIMILARITY_RANGE,
            });
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: DEFAULT_MAX_CONCURRENT_TASKS,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            task_timeout_ms: DEFAULT_TASK_TIMEOUT_MS,
            task_max_retries: DEFAULT_TASK_MAX_RETRIES,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            reclaim_interval_ms: DEFAULT_RECLAIM_INTERVAL_MS,
            max_candidates_per_job: DEFAULT_MAX_CANDIDATES_PER_JOB,
            triage_search_limit: DEFAULT_TRIAGE_SEARCH_LIMIT,
            tag_similarity_threshold: DEFAULT_TAG_SIMILARITY_THRESHOLD,
        }
    }
}

// ---------------------------------------------------------------------------
// Defaults (serde)
// ---------------------------------------------------------------------------

const fn default_max_concurrent_tasks() -> usize {
    DEFAULT_MAX_CONCURRENT_TASKS
}
const fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}
const fn default_task_timeout_ms() -> u64 {
    DEFAULT_TASK_TIMEOUT_MS
}
const fn default_task_max_retries() -> u32 {
    DEFAULT_TASK_MAX_RETRIES
}
const fn default_heartbeat_interval_ms() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_MS
}
const fn default_reclaim_interval_ms() -> u64 {
    DEFAULT_RECLAIM_INTERVAL_MS
}
const fn default_max_candidates_per_job() -> u32 {
    DEFAULT_MAX_CANDIDATES_PER_JOB
}
const fn default_triage_search_limit() -> u32 {
    DEFAULT_TRIAGE_SEARCH_LIMIT
}
// f64 operations are not const-stable.
fn default_tag_similarity_threshold() -> f64 {
    DEFAULT_TAG_SIMILARITY_THRESHOLD
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let yaml = "---";
        let config: WorkerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.max_concurrent_tasks, DEFAULT_MAX_CONCURRENT_TASKS);
        assert_eq!(config.poll_interval_ms, DEFAULT_POLL_INTERVAL_MS);
        assert_eq!(config.task_timeout_ms, DEFAULT_TASK_TIMEOUT_MS);
        assert_eq!(config.task_max_retries, DEFAULT_TASK_MAX_RETRIES);
        assert_eq!(config.heartbeat_interval_ms, DEFAULT_HEARTBEAT_INTERVAL_MS);
        assert_eq!(config.reclaim_interval_ms, DEFAULT_RECLAIM_INTERVAL_MS);
        assert_eq!(
            config.max_candidates_per_job,
            DEFAULT_MAX_CANDIDATES_PER_JOB
        );
        assert_eq!(config.triage_search_limit, DEFAULT_TRIAGE_SEARCH_LIMIT);
        assert!(
            (config.tag_similarity_threshold - DEFAULT_TAG_SIMILARITY_THRESHOLD).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn test_duration_conversions() {
        let config = WorkerConfig::default();
        assert_eq!(
            config.poll_interval(),
            Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)
        );
        assert_eq!(
            config.task_timeout(),
            Duration::from_millis(DEFAULT_TASK_TIMEOUT_MS)
        );
        assert_eq!(
            config.heartbeat_interval(),
            Duration::from_millis(DEFAULT_HEARTBEAT_INTERVAL_MS)
        );
        assert_eq!(
            config.reclaim_interval(),
            Duration::from_millis(DEFAULT_RECLAIM_INTERVAL_MS)
        );
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = WorkerConfig {
            max_concurrent_tasks: 8,
            poll_interval_ms: 5_000,
            task_timeout_ms: 600_000,
            task_max_retries: 5,
            heartbeat_interval_ms: 200_000,
            reclaim_interval_ms: 20_000,
            max_candidates_per_job: 50,
            triage_search_limit: 25,
            tag_similarity_threshold: 0.9,
        };
        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: WorkerConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(config, parsed);
    }

    fn validate_diagnostics(config: &WorkerConfig) -> Diagnostics {
        let mut diags = Diagnostics::default();
        config.validate(&mut diags);
        diags
    }

    /// Returns true if `diags` contains a [`ValidationError`] matching
    /// `pred`.
    fn any<P: Fn(&ValidationError) -> bool>(diags: &Diagnostics, pred: P) -> bool {
        diags.iter().any(pred)
    }

    #[test]
    fn test_validate_rejects_zero_max_concurrent_tasks() {
        let config = WorkerConfig {
            max_concurrent_tasks: 0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "worker.max_concurrent_tasks",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_poll_interval() {
        let config = WorkerConfig {
            poll_interval_ms: 0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "worker.poll_interval_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_task_timeout() {
        let config = WorkerConfig {
            task_timeout_ms: 0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "worker.task_timeout_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_heartbeat_interval() {
        let config = WorkerConfig {
            heartbeat_interval_ms: 0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "worker.heartbeat_interval_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_heartbeat_ge_timeout() {
        let config = WorkerConfig {
            heartbeat_interval_ms: 300_000,
            task_timeout_ms: 300_000,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::LessThan,
            } if subject.field.as_str() == "worker.heartbeat_interval_ms"
                && bound.field.as_str() == "worker.task_timeout_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_reclaim_lt_poll() {
        let config = WorkerConfig {
            reclaim_interval_ms: 1_000,
            poll_interval_ms: 2_000,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::FieldOrdering {
                subject,
                bound,
                relation: OrderRelation::AtLeast,
            } if subject.field.as_str() == "worker.reclaim_interval_ms"
                && bound.field.as_str() == "worker.poll_interval_ms",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_triage_search_limit() {
        let config = WorkerConfig {
            triage_search_limit: 0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "worker.triage_search_limit",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_max_candidates() {
        let config = WorkerConfig {
            max_candidates_per_job: 0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::BelowMin { field, min: 1, .. }
                if field.as_str() == "worker.max_candidates_per_job",
        )));
    }

    #[test]
    fn test_validate_rejects_zero_tag_similarity_threshold() {
        let config = WorkerConfig {
            tag_similarity_threshold: 0.0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "worker.tag_similarity_threshold",
        )));
    }

    #[test]
    fn test_validate_rejects_negative_tag_similarity_threshold() {
        let config = WorkerConfig {
            tag_similarity_threshold: -0.1,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "worker.tag_similarity_threshold",
        )));
    }

    #[test]
    fn test_validate_rejects_tag_similarity_threshold_above_one() {
        let config = WorkerConfig {
            tag_similarity_threshold: 1.01,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(any(&diags, |d| matches!(
            d,
            ValidationError::OutOfRange { field, .. }
                if field.as_str() == "worker.tag_similarity_threshold",
        )));
    }

    #[test]
    fn test_validate_accepts_tag_similarity_threshold_of_one() {
        let config = WorkerConfig {
            tag_similarity_threshold: 1.0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(
            !any(&diags, |d| matches!(
                d,
                ValidationError::OutOfRange { field, .. }
                    if field.as_str() == "worker.tag_similarity_threshold",
            )),
            "threshold of 1.0 should be accepted",
        );
    }

    #[test]
    fn test_validate_accepts_valid_config() {
        let diags = validate_diagnostics(&WorkerConfig::default());
        assert!(
            diags.is_empty(),
            "default config should be valid: {diags:?}",
        );
    }

    #[test]
    fn test_validate_collects_multiple_errors() {
        let config = WorkerConfig {
            max_concurrent_tasks: 0,
            poll_interval_ms: 0,
            task_timeout_ms: 0,
            ..WorkerConfig::default()
        };
        let diags = validate_diagnostics(&config);
        assert!(
            diags.len() >= 3,
            "expected at least 3 diagnostics, got {}: {diags:?}",
            diags.len(),
        );
    }
}
