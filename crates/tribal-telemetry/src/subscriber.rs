//! Tracing subscriber initialisation.
//!
//! [`init_subscriber`] builds a layered subscriber from a
//! [`LoggingConfig`](tribal_config::LoggingConfig) and sets it as the global
//! default.  It should be called exactly once, early in program startup.

use std::sync::atomic::{AtomicBool, Ordering};

use tracing::subscriber::set_global_default;
use tracing_appender::rolling::Rotation;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};
use tribal_config::{FileRotation, LogFormat, LogOutput, LoggingConfig};

use crate::{error::TelemetryError, guard::TelemetryGuard};

/// Whether [`init_subscriber`] has already been called.
static INITIALISED: AtomicBool = AtomicBool::new(false);

/// Initialises the global tracing subscriber.
///
/// Builds a layered subscriber stack based on the given configuration:
///
/// 1. **Filter layer** — `EnvFilter` parsed from `config.level`.
/// 2. **Format layer** — JSON or pretty, depending on `config.format`.
/// 3. **Output layer** — stderr or rolling file, depending on `config.output`.
///
/// Both stderr and file output use non-blocking writers for consistent
/// behaviour.  The returned [`TelemetryGuard`] must be held for the
/// program lifetime to ensure pending writes are flushed on shutdown.
///
/// # Errors
///
/// - [`TelemetryError::SubscriberAlreadyInitialised`] if this function
///   has already been called successfully.
/// - [`TelemetryError::InvalidFilterDirective`] if the filter string
///   cannot be parsed.
/// - [`TelemetryError::DirectoryCreation`] if the log directory cannot
///   be created.
/// - [`TelemetryError::SetGlobalDefault`] if another library already
///   registered a global subscriber.
///
/// # Panics
///
/// Does not panic.  All failure modes return `Err`.
pub fn init_subscriber(config: &LoggingConfig) -> Result<TelemetryGuard, TelemetryError> {
    if INITIALISED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err(TelemetryError::SubscriberAlreadyInitialised);
    }

    match try_init_subscriber(config) {
        Ok(guard) => Ok(guard),
        Err(err) => {
            INITIALISED.store(false, Ordering::SeqCst);
            Err(err)
        }
    }
}

/// Inner implementation that builds and installs the subscriber.
///
/// Separated from [`init_subscriber`] so that the `INITIALISED` flag can
/// be reset cleanly on failure without duplicating the guard logic.
fn try_init_subscriber(config: &LoggingConfig) -> Result<TelemetryGuard, TelemetryError> {
    let env_filter = EnvFilter::try_new(&config.level).map_err(|source| {
        TelemetryError::InvalidFilterDirective {
            directive: config.level.clone(),
            source,
        }
    })?;

    // Build writer and guard based on output destination.
    let (writer, guard) = match config.output {
        LogOutput::Stderr => {
            let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());
            (non_blocking, guard)
        }
        LogOutput::File => {
            std::fs::create_dir_all(&config.file_directory).map_err(|source| {
                TelemetryError::DirectoryCreation {
                    path: config.file_directory.clone(),
                    source,
                }
            })?;

            let rotation = match config.file_rotation {
                FileRotation::Daily => Rotation::DAILY,
                FileRotation::Hourly => Rotation::HOURLY,
                FileRotation::Never => Rotation::NEVER,
            };

            let appender = tracing_appender::rolling::RollingFileAppender::new(
                rotation,
                &config.file_directory,
                "tribal.log",
            );

            let (non_blocking, guard) = tracing_appender::non_blocking(appender);
            (non_blocking, guard)
        }
    };

    // Build subscriber with the appropriate format layer.
    //
    // JSON and Pretty produce different concrete types, so the
    // `set_global_default` call is duplicated in each branch.
    match config.format {
        LogFormat::Json => {
            let subscriber = Registry::default().with(env_filter).with(
                fmt::layer()
                    .json()
                    .with_writer(writer)
                    .with_target(true)
                    .with_current_span(true)
                    .with_span_list(true),
            );
            set_global_default(subscriber)
                .map_err(|source| TelemetryError::SetGlobalDefault { source })?;
        }
        LogFormat::Pretty => {
            let subscriber = Registry::default()
                .with(env_filter)
                .with(fmt::layer().pretty().with_writer(writer).with_target(true));
            set_global_default(subscriber)
                .map_err(|source| TelemetryError::SetGlobalDefault { source })?;
        }
    }

    if config.used_temp_dir_fallback {
        tracing::warn!(
            directory = %config.file_directory,
            "no standard state/data directory found; using temporary directory for log files",
        );
    }

    Ok(TelemetryGuard::new(guard))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Serialises tests that manipulate the process-global `INITIALISED`
    /// flag.  Without this, parallel test threads race on the `AtomicBool`.
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    // Note: each test calls `INITIALISED.store(false, ...)` at the start
    // to reset state from whichever test ran previously.  Error-path tests
    // do not need end-of-test cleanup because `init_subscriber` resets the
    // flag automatically when it fails.  Tests where the second call may
    // *succeed* still need explicit cleanup at the end.

    #[test]
    fn test_invalid_filter_directive_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        let config = LoggingConfig {
            level: "not valid [[".to_owned(),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(&config);

        assert!(
            matches!(result, Err(TelemetryError::InvalidFilterDirective { .. })),
            "expected InvalidFilterDirective, got {result:?}",
        );
        assert!(
            !INITIALISED.load(Ordering::SeqCst),
            "flag should be reset after failure"
        );
    }

    #[test]
    fn test_file_output_with_nonexistent_directory_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        let config = LoggingConfig {
            output: LogOutput::File,
            file_directory: "/nonexistent/dir/tribal/logs".to_owned(),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(&config);

        assert!(
            matches!(result, Err(TelemetryError::DirectoryCreation { .. })),
            "expected DirectoryCreation, got {result:?}",
        );
        assert!(
            !INITIALISED.load(Ordering::SeqCst),
            "flag should be reset after failure"
        );
    }

    #[test]
    fn test_failed_init_allows_retry() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        // First call with an invalid directive fails.
        let bad_config = LoggingConfig {
            level: "not valid [[".to_owned(),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(&bad_config);
        assert!(result.is_err());

        // Flag was reset — a subsequent call with valid config is not
        // rejected as `SubscriberAlreadyInitialised`.
        let good_config = LoggingConfig::default();
        let result = init_subscriber(&good_config);

        // We expect `SetGlobalDefault` (the unit test process may already
        // have a subscriber) rather than `SubscriberAlreadyInitialised`.
        // The key assertion is that the flag did not block retry.
        assert!(
            !matches!(result, Err(TelemetryError::SubscriberAlreadyInitialised)),
            "flag should have been reset after failed init, got {result:?}",
        );

        // Clean up: the second call may have succeeded, leaving the flag true.
        INITIALISED.store(false, Ordering::SeqCst);
    }
}
