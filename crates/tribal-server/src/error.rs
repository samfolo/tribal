//! Crate-level error type for `tribal-server`.
//!
//! [`AppError`] is the single error enum for the server binary.
//! All variants use named fields where applicable; wrapped errors carry
//! `#[source]` for error chain propagation.

use std::io;

use thiserror::Error;
use tribal_config::{ConfigError, TransportKind};
use tribal_db::DbError;
use tribal_inference::ProviderRegistryError;
use tribal_telemetry::TelemetryError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Exit code for transient migration lock failure (`EX_TEMPFAIL`).
const EXIT_CODE_MIGRATION_LOCK: i32 = 75;

/// Exit code for worker runtime failure or unexpected death (`EX_SOFTWARE`).
const EXIT_CODE_WORKER_DEATH: i32 = 70;

// ---------------------------------------------------------------------------
// AppError
// ---------------------------------------------------------------------------

/// Errors that can occur during application startup or subcommand execution.
///
/// All variants use named fields where applicable. `#[source]` preserves
/// the error chain for debugging.
#[derive(Debug, Error)]
pub enum AppError {
    /// A CLI argument combination was invalid.
    #[error("{source}")]
    Cli {
        /// The underlying clap validation error.
        #[source]
        source: clap::Error,
    },

    /// Configuration loading or validation failed.
    #[error("{source}")]
    Config {
        /// The underlying configuration error.
        #[source]
        source: ConfigError,
    },

    /// Failed to write help text to stdout.
    #[error("failed to write help output")]
    HelpOutput {
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Tracing subscriber initialisation failed.
    #[error("{source}")]
    Telemetry {
        /// The underlying telemetry error.
        #[source]
        source: TelemetryError,
    },

    /// Database pool connection failed after retries.
    #[error("failed to connect to database pool '{pool_name}' after {attempts} attempts")]
    PoolConnection {
        /// Name of the pool that failed to connect.
        pool_name: &'static str,
        /// Number of connection attempts made.
        attempts: u32,
        /// The error from the final attempt.
        #[source]
        source: DbError,
    },

    /// Database has no migrations table — `tribal setup` required.
    #[error("database is uninitialised; run `tribal setup` first")]
    FirstRunRequired,

    /// Migration advisory lock could not be acquired.
    #[error("could not acquire migration lock after {attempts} attempts")]
    MigrationLockFailed {
        /// Number of lock attempts made.
        attempts: u32,
    },

    /// Migration execution failed.
    #[error("migration failed")]
    MigrationFailed {
        /// The underlying migration error.
        #[source]
        source: sqlx::migrate::MigrateError,
    },

    /// Provider registry construction failed.
    #[error("{source}")]
    ProviderRegistry {
        /// The underlying registry construction error.
        #[source]
        source: ProviderRegistryError,
    },

    /// Prompt file I/O failed.
    #[error("prompt I/O failed: {context}")]
    PromptIo {
        /// Description of the failed operation.
        context: String,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Prompt file watcher initialisation failed.
    #[error("prompt watcher failed: {context}")]
    PromptWatcher {
        /// Description of the failed operation.
        context: String,
        /// The underlying notify error.
        #[source]
        source: notify::Error,
    },

    /// Prompt loading or upsert failed.
    #[error("prompt loading failed: {context}")]
    PromptLoading {
        /// Description of the failed operation.
        context: String,
        /// The underlying database error.
        #[source]
        source: DbError,
    },

    /// Provider setup failed during startup.
    #[error("provider setup failed: {context}")]
    ProviderSetup {
        /// Description of the setup failure.
        context: String,
    },

    /// Project resolution failed.
    #[error("project resolution failed: {context}")]
    ProjectResolution {
        /// Description of the resolution failure.
        context: String,
    },

    /// Tokio runtime creation failed.
    #[error("failed to create async runtime")]
    Runtime {
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Worker startup failed.
    #[error("worker startup failed")]
    WorkerStartup {
        /// The underlying worker error.
        #[source]
        source: tribal_worker::WorkerError,
    },

    /// OS signal handler registration failed.
    #[error("failed to register OS signal handler")]
    SignalHandler {
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Failed to create the worker runtime.
    #[error("failed to create worker runtime")]
    WorkerRuntime {
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// The worker exited or panicked unexpectedly during operation.
    #[error("worker died unexpectedly")]
    WorkerDeath,

    /// The worker did not finish within the configured shutdown deadline.
    #[error("shutdown deadline exceeded ({deadline_ms}ms)")]
    ShutdownDeadlineExceeded {
        /// The configured deadline in milliseconds.
        deadline_ms: u128,
    },

    /// Transport failed to bind the TCP listener.
    #[error("failed to bind {transport} transport to {address}")]
    TransportBind {
        /// The transport that failed to bind.
        transport: TransportKind,
        /// The socket address that could not be bound.
        address: std::net::SocketAddr,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Transport encountered a fatal serving error.
    #[error("{transport} transport serving error")]
    TransportServe {
        /// The transport that failed.
        transport: TransportKind,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Stdio transport failed during operation.
    #[error("stdio transport failed")]
    TransportStdio {
        /// The underlying error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Setup I/O operation failed (directory creation, config file write).
    #[error("setup I/O failed: {context}")]
    SetupIo {
        /// Description of the failed operation.
        context: String,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },

    /// Git remote detection failed.
    #[error("git remote detection failed: {reason}")]
    GitDetection {
        /// Description of why detection failed.
        reason: String,
    },

    /// A token management operation failed.
    #[error("token operation failed: {reason}")]
    TokenOperation {
        /// Description of why the operation failed.
        reason: String,
    },

    /// General database query error.
    #[error("{source}")]
    Database {
        /// The underlying database error.
        #[source]
        source: DbError,
    },
}

impl AppError {
    /// Returns the process exit code for this error.
    ///
    /// Migration lock failures use `EX_TEMPFAIL` (75); worker errors use
    /// `EX_SOFTWARE` (70); all other errors use exit code 1.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::MigrationLockFailed { .. } => EXIT_CODE_MIGRATION_LOCK,
            Self::WorkerStartup { .. }
            | Self::WorkerRuntime { .. }
            | Self::WorkerDeath
            | Self::ShutdownDeadlineExceeded { .. } => EXIT_CODE_WORKER_DEATH,
            _ => 1,
        }
    }

    /// Wraps a pool-acquire failure as the appropriate `AppError` variant.
    ///
    /// `PoolTimedOut` is mapped to [`DbError::PoolExhausted`] (preserving
    /// the pool name); all other errors become [`DbError::QueryFailed`].
    pub(crate) fn pool_acquire(
        pool_name: &'static str,
        context: &str,
        source: sqlx::Error,
    ) -> Self {
        if matches!(source, sqlx::Error::PoolTimedOut) {
            return Self::Database {
                source: DbError::PoolExhausted { pool_name },
            };
        }
        Self::Database {
            source: DbError::QueryFailed {
                context: context.into(),
                source,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// From impls
// ---------------------------------------------------------------------------

impl From<clap::Error> for AppError {
    fn from(source: clap::Error) -> Self {
        Self::Cli { source }
    }
}

impl From<ConfigError> for AppError {
    fn from(source: ConfigError) -> Self {
        Self::Config { source }
    }
}

impl From<TelemetryError> for AppError {
    fn from(source: TelemetryError) -> Self {
        Self::Telemetry { source }
    }
}

impl From<ProviderRegistryError> for AppError {
    fn from(source: ProviderRegistryError) -> Self {
        Self::ProviderRegistry { source }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;
    use tribal_config::DEFAULT_BIND_ADDRESS;

    use super::*;

    #[test]
    fn test_display_cli_error() {
        let source = clap::Error::raw(
            ErrorKind::ArgumentConflict,
            "--bind cannot be used with --transport stdio",
        );
        let err = AppError::Cli { source };
        assert!(
            err.to_string()
                .contains("--bind cannot be used with --transport stdio"),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_display_config_error() {
        let err = AppError::Config {
            source: ConfigError::ValidationFailed {
                errors: vec!["database.url must not be empty".into()],
            },
        };
        assert!(
            err.to_string().contains("database.url"),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_display_help_output() {
        let err = AppError::HelpOutput {
            source: io::Error::new(io::ErrorKind::BrokenPipe, "pipe closed"),
        };
        assert_eq!(err.to_string(), "failed to write help output");
    }

    #[test]
    fn test_display_first_run_required() {
        let err = AppError::FirstRunRequired;
        assert!(err.to_string().contains("tribal setup"));
    }

    #[test]
    fn test_display_migration_lock_failed() {
        let err = AppError::MigrationLockFailed { attempts: 3 };
        assert!(err.to_string().contains("3 attempts"));
    }

    #[test]
    fn test_display_project_resolution() {
        let err = AppError::ProjectResolution {
            context: "project not found in database".into(),
        };
        assert!(err.to_string().contains("project not found"));
    }

    #[test]
    fn test_display_worker_startup() {
        let err = AppError::WorkerStartup {
            source: tribal_worker::WorkerError::Cancelled,
        };
        assert_eq!(err.to_string(), "worker startup failed");
    }

    #[test]
    fn test_display_worker_runtime() {
        let err = AppError::WorkerRuntime {
            source: io::Error::other("thread pool exhausted"),
        };
        assert_eq!(err.to_string(), "failed to create worker runtime");
    }

    #[test]
    fn test_display_signal_handler() {
        let err = AppError::SignalHandler {
            source: io::Error::other("permission denied"),
        };
        assert_eq!(err.to_string(), "failed to register OS signal handler");
    }

    #[test]
    fn test_display_worker_death() {
        let err = AppError::WorkerDeath;
        assert_eq!(err.to_string(), "worker died unexpectedly");
    }

    #[test]
    fn test_display_git_detection() {
        let err = AppError::GitDetection {
            reason: "not inside a git repository".into(),
        };
        assert!(
            err.to_string().contains("not inside a git repository"),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_display_token_operation() {
        let err = AppError::TokenOperation {
            reason: "no token matches prefix: 'abc12345'".into(),
        };
        assert!(
            err.to_string().contains("no token matches prefix"),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_display_setup_io() {
        let err = AppError::SetupIo {
            context: "create config directory /tmp/tribal".into(),
            source: io::Error::new(io::ErrorKind::PermissionDenied, "permission denied"),
        };
        assert!(
            err.to_string().contains("create config directory"),
            "unexpected display: {err}",
        );
    }

    #[test]
    fn test_exit_code_migration_lock() {
        let err = AppError::MigrationLockFailed { attempts: 3 };
        assert_eq!(err.exit_code(), EXIT_CODE_MIGRATION_LOCK);
    }

    #[test]
    fn test_exit_code_worker_startup() {
        let err = AppError::WorkerStartup {
            source: tribal_worker::WorkerError::Cancelled,
        };
        assert_eq!(err.exit_code(), EXIT_CODE_WORKER_DEATH);
    }

    #[test]
    fn test_exit_code_worker_runtime() {
        let err = AppError::WorkerRuntime {
            source: io::Error::other("thread pool exhausted"),
        };
        assert_eq!(err.exit_code(), EXIT_CODE_WORKER_DEATH);
    }

    #[test]
    fn test_exit_code_worker_death() {
        let err = AppError::WorkerDeath;
        assert_eq!(err.exit_code(), EXIT_CODE_WORKER_DEATH);
    }

    #[test]
    fn test_exit_code_shutdown_deadline_exceeded() {
        let err = AppError::ShutdownDeadlineExceeded { deadline_ms: 5000 };
        assert_eq!(err.exit_code(), EXIT_CODE_WORKER_DEATH);
    }

    #[test]
    fn test_display_shutdown_deadline_exceeded() {
        let err = AppError::ShutdownDeadlineExceeded { deadline_ms: 5000 };
        assert_eq!(err.to_string(), "shutdown deadline exceeded (5000ms)");
    }

    #[test]
    fn test_exit_code_default() {
        let err = AppError::FirstRunRequired;
        assert_eq!(err.exit_code(), 1);
    }

    // -- transport variants -------------------------------------------------

    #[test]
    fn test_display_transport_bind() {
        let addr = DEFAULT_BIND_ADDRESS.parse().unwrap();
        let err = AppError::TransportBind {
            transport: TransportKind::Http,
            address: addr,
            source: io::Error::other("test"),
        };
        assert_eq!(
            err.to_string(),
            "failed to bind http transport to 127.0.0.1:8725",
        );
    }

    #[test]
    fn test_display_transport_serve() {
        let err = AppError::TransportServe {
            transport: TransportKind::Http,
            source: io::Error::other("test"),
        };
        assert_eq!(err.to_string(), "http transport serving error");
    }

    #[test]
    fn test_display_transport_stdio() {
        let err = AppError::TransportStdio {
            source: Box::new(io::Error::other("test")),
        };
        assert_eq!(err.to_string(), "stdio transport failed");
    }

    #[test]
    fn test_exit_code_transport_defaults_to_1() {
        let addr = DEFAULT_BIND_ADDRESS.parse().unwrap();
        let variants: Vec<AppError> = vec![
            AppError::TransportBind {
                transport: TransportKind::Http,
                address: addr,
                source: io::Error::other("test"),
            },
            AppError::TransportServe {
                transport: TransportKind::Http,
                source: io::Error::other("test"),
            },
            AppError::TransportStdio {
                source: Box::new(io::Error::other("test")),
            },
        ];
        for err in &variants {
            assert_eq!(err.exit_code(), 1, "unexpected exit code for: {err}");
        }
    }
}
