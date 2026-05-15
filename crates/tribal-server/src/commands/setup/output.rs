//! Terminal output for the `tribal setup` command.
//!
//! All user-facing presentation lives here, separated from business logic.
//! Each function takes the destination writer as a parameter so callers
//! can pipe output to stderr (production) or a `Vec<u8>` (tests).

use std::io::{self, Write};

use super::config_file::ConfigFileOutcome;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Reports that the config directory was created or already existed.
pub(super) fn config_directory(out: &mut dyn Write, path: &str) {
    write_line(out, &format!("  config directory: {path}"));
}

/// Reports that prompt files were written.
pub(super) fn prompt_files(out: &mut dyn Write, path: &str) {
    write_line(out, &format!("  prompt files: {path}"));
}

/// Reports a successful database connection.
pub(super) fn database_connected(out: &mut dyn Write) {
    write_line(out, "  database: connected");
}

/// Prints a hint when the database connection fails.
pub(super) fn database_unreachable(out: &mut dyn Write) {
    write_line(
        out,
        "  hint: verify that PostgreSQL is running and the target database exists",
    );
}

/// Reports that migrations completed.
pub(super) fn migrations_complete(out: &mut dyn Write) {
    write_line(out, "  migrations: complete");
}

/// Reports the principal that was found or created.
pub(super) fn principal(out: &mut dyn Write, key: &str) {
    write_line(out, &format!("  principal: {key}"));
}

/// Reports that a token was created with its expiry date.
pub(super) fn token_created(out: &mut dyn Write, expires_date: &str) {
    write_line(out, &format!("  token: created (expires {expires_date})"));
}

/// Reports the config file outcome (written, skipped, warnings).
pub(super) fn config_file(out: &mut dyn Write, outcome: &ConfigFileOutcome) {
    match outcome {
        ConfigFileOutcome::Written { path } => {
            write_line(out, &format!("  config file: written to {path}"));
        }
        ConfigFileOutcome::AlreadyExists { warnings } => {
            write_line(out, "  config file: already exists, skipping");
            for warning in warnings {
                write_line(out, &format!("  warning: {warning}"));
            }
        }
    }
}

/// Displays the bearer token and next-step instructions.
pub(super) fn instructions(out: &mut dyn Write, raw_token: &str) {
    write_line(out, "");
    write_line(out, "Setup complete. Your bearer token:");
    write_line(out, "");
    write_line(out, &format!("  {raw_token}"));
    write_line(out, "");
    write_line(out, "Save this token — it will not be shown again.");
    write_line(out, "");
    write_line(out, "Next steps:");
    write_line(
        out,
        &format!("  1. Export the token:  export TRIBAL_AUTH_TOKEN=\"{raw_token}\""),
    );
    write_line(out, "  2. Register a project:  tribal project register");
    write_line(out, "");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Writes `line` followed by a newline. IO errors are ignored — output is a
/// best-effort signal, not a correctness mechanism.
fn write_line(out: &mut dyn Write, line: &str) {
    let _ = writeln!(out, "{line}");
    // Flush is a best-effort hint for unbuffered stderr; ignore the result.
    let _: io::Result<()> = out.flush();
}
