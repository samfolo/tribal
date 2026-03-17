//! Terminal output for the `tribal setup` command.
//!
//! All user-facing presentation lives here, separated from business logic.

use super::config_file::ConfigFileOutcome;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Reports that the config directory was created or already existed.
pub(super) fn config_directory(path: &str) {
    eprintln!("  config directory: {path}");
}

/// Reports that prompt files were written.
pub(super) fn prompt_files(path: &str) {
    eprintln!("  prompt files: {path}");
}

/// Reports a successful database connection.
pub(super) fn database_connected() {
    eprintln!("  database: connected");
}

/// Prints a hint when the database connection fails.
pub(super) fn database_unreachable() {
    eprintln!("  hint: verify that PostgreSQL is running and the target database exists");
}

/// Reports that migrations completed.
pub(super) fn migrations_complete() {
    eprintln!("  migrations: complete");
}

/// Reports the principal that was found or created.
pub(super) fn principal(key: &str) {
    eprintln!("  principal: {key}");
}

/// Reports that a token was created with its expiry date.
pub(super) fn token_created(expires_date: &str) {
    eprintln!("  token: created (expires {expires_date})");
}

/// Reports the config file outcome (written, skipped, warnings).
pub(super) fn config_file(outcome: &ConfigFileOutcome) {
    match outcome {
        ConfigFileOutcome::Written { path } => {
            eprintln!("  config file: written to {path}");
        }
        ConfigFileOutcome::AlreadyExists { warnings } => {
            eprintln!("  config file: already exists, skipping");
            for warning in warnings {
                eprintln!("  warning: {warning}");
            }
        }
    }
}

/// Displays the bearer token and next-step instructions.
pub(super) fn instructions(raw_token: &str) {
    eprintln!();
    eprintln!("Setup complete. Your bearer token:");
    eprintln!();
    eprintln!("  {raw_token}");
    eprintln!();
    eprintln!("Save this token — it will not be shown again.");
    eprintln!();
    eprintln!("Next steps:");
    eprintln!("  1. Export the token:  export TRIBAL_AUTH_TOKEN=\"{raw_token}\"");
    eprintln!("  2. Register a project:  tribal project register");
    eprintln!();
}
