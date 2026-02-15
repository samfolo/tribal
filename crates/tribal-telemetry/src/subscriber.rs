//! Tracing subscriber initialisation.
//!
//! [`init_subscriber`] builds a layered subscriber from a
//! [`LoggingConfig`](crate::LoggingConfig) and sets it as the global
//! default.  It should be called exactly once, early in program startup.

use std::sync::atomic::{AtomicBool, Ordering};

use tracing::subscriber::set_global_default;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

use crate::{
    config::{LogFormat, LogOutput, LoggingConfig},
    error::TelemetryError,
    guard::TelemetryGuard,
};

/// Whether [`init_subscriber`] has already been called.
static INITIALISED: AtomicBool = AtomicBool::new(false);

/// Initialises the global tracing subscriber.
///
/// Builds a layered subscriber stack based on the given configuration:
///
/// 1. **Filter layer** — `EnvFilter` parsed from the `TRIBAL_LOG`
///    environment variable (if set) or `config.level`.
/// 2. **Format layer** — JSON or pretty, depending on `config.format`.
/// 3. **Output layer** — stderr or file, depending on `config.output`.
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
/// - [`TelemetryError::FileOutputMissingPath`] if output is `File` but
///   `file_path` is `None`.
/// - [`TelemetryError::FileCreation`] if the log file cannot be opened.
/// - [`TelemetryError::SetGlobalDefault`] if another library already
///   registered a global subscriber.
///
/// # Panics
///
/// Does not panic.  All failure modes return `Err`.
pub fn init_subscriber(config: LoggingConfig) -> Result<TelemetryGuard, TelemetryError> {
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
fn try_init_subscriber(config: LoggingConfig) -> Result<TelemetryGuard, TelemetryError> {
    // 1. Determine filter directive: TRIBAL_LOG env var overrides config.
    let directive = std::env::var("TRIBAL_LOG").unwrap_or(config.level);

    // 2. Build EnvFilter from the directive string.
    let env_filter = EnvFilter::try_new(&directive).map_err(|source| {
        TelemetryError::InvalidFilterDirective {
            directive: directive.clone(),
            source,
        }
    })?;

    // 3. Build writer and guard based on output destination.
    let (writer, guard) = match config.output {
        LogOutput::Stderr => {
            let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());
            (non_blocking, guard)
        }
        LogOutput::File => {
            let path = config
                .file_path
                .as_deref()
                .ok_or(TelemetryError::FileOutputMissingPath)?;

            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|source| TelemetryError::FileCreation {
                    path: path.to_owned(),
                    source,
                })?;

            let (non_blocking, guard) = tracing_appender::non_blocking(file);
            (non_blocking, guard)
        }
    };

    // 4. Build subscriber with the appropriate format layer.
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

    Ok(TelemetryGuard::new(guard))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Serialises tests that manipulate the process-global `INITIALISED`
    /// flag.  Without this, parallel test threads race on the `AtomicBool`.
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_invalid_filter_directive_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        let config = LoggingConfig {
            level: "not valid [[".to_owned(),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(config);

        INITIALISED.store(false, Ordering::SeqCst);

        assert!(
            matches!(result, Err(TelemetryError::InvalidFilterDirective { .. })),
            "expected InvalidFilterDirective, got {result:?}",
        );
    }

    #[test]
    fn test_file_output_without_path_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        let config = LoggingConfig {
            output: LogOutput::File,
            file_path: None,
            ..LoggingConfig::default()
        };
        let result = init_subscriber(config);

        INITIALISED.store(false, Ordering::SeqCst);

        assert!(
            matches!(result, Err(TelemetryError::FileOutputMissingPath)),
            "expected FileOutputMissingPath, got {result:?}",
        );
    }

    #[test]
    fn test_file_output_with_nonexistent_directory_returns_error() {
        let _lock = TEST_MUTEX.lock().unwrap();
        INITIALISED.store(false, Ordering::SeqCst);

        let config = LoggingConfig {
            output: LogOutput::File,
            file_path: Some("/nonexistent/dir/tribal.log".to_owned()),
            ..LoggingConfig::default()
        };
        let result = init_subscriber(config);

        INITIALISED.store(false, Ordering::SeqCst);

        assert!(
            matches!(result, Err(TelemetryError::FileCreation { .. })),
            "expected FileCreation, got {result:?}",
        );
    }
}
