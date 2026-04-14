//! Implementation of `tribal config show`.

use tribal_config::{load_config, redact_secrets};

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
/// Returns an [`AppError`] if config loading or serialisation fails.
pub(crate) fn run(config_path: &str, args: ConfigShowArgs) -> Result<(), AppError> {
    let config = load_config(config_path, None, None)?;

    if args.show_secrets {
        output::resolved_config(&config.to_yaml()?);
    } else {
        output::resolved_config(&redact_secrets(&config.to_yaml()?));
    }

    Ok(())
}
