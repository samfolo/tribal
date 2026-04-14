//! Implementation of `tribal config show`.

use tribal_config::load_config;

use super::output;
use crate::{cli::ConfigShowArgs, error::AppError};

/// Runs the `tribal config show` flow.
///
/// Loads the fully resolved configuration (all layers merged) and
/// prints it as YAML to stdout. Sensitive fields are redacted unless
/// `--show-secrets` is set.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading fails.
pub(crate) fn run(config_path: &str, args: ConfigShowArgs) -> Result<(), AppError> {
    let config = load_config(config_path, None, None)?;
    let yaml = serde_yaml::to_string(&config).map_err(|e| AppError::Config {
        source: tribal_config::ConfigError::ValidationFailed {
            errors: vec![format!("failed to serialise resolved config: {e}")],
        },
    })?;

    if args.show_secrets {
        output::resolved_config(&yaml);
    } else {
        output::resolved_config(&output::redact_secrets(&yaml));
    }

    Ok(())
}
