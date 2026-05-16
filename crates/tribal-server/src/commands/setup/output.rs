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
            write_line(
                out,
                &format!("  config file: written to {}", path.display()),
            );
        }
        ConfigFileOutcome::AlreadyExists { warnings, .. } => {
            write_line(out, "  config file: already exists, skipping");
            for warning in warnings {
                write_line(out, &format!("  warning: {warning}"));
            }
        }
    }
}

/// Displays the bearer token and next-step instructions.
///
/// Unlike the other `output::*` helpers, this one propagates IO failures
/// rather than swallowing them: the raw bearer token is shown only here,
/// the database stores just the hash, and the message itself promises
/// the token "will not be shown again". A silent write failure would
/// leave the user without recoverable credentials.
pub(super) fn instructions(out: &mut dyn Write, raw_token: &str) -> io::Result<()> {
    try_write_line(out, "")?;
    try_write_line(out, "Setup complete. Your bearer token:")?;
    try_write_line(out, "")?;
    try_write_line(out, &format!("  {raw_token}"))?;
    try_write_line(out, "")?;
    try_write_line(out, "Save this token — it will not be shown again.")?;
    try_write_line(out, "")?;
    try_write_line(out, "Next steps:")?;
    try_write_line(
        out,
        &format!("  1. Export the token:  export TRIBAL_AUTH_TOKEN=\"{raw_token}\""),
    )?;
    try_write_line(out, "  2. Register a project:  tribal project register")?;
    try_write_line(out, "")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Writes `line` followed by a newline, swallowing IO errors.
///
/// Suitable for progress lines where loss of output is not a correctness
/// issue. For output the caller cannot afford to lose (e.g. credentials),
/// use [`try_write_line`].
fn write_line(out: &mut dyn Write, line: &str) {
    let _ = try_write_line(out, line);
}

/// Writes `line` followed by a newline, returning any IO failure.
///
/// Both flavours route through the same code path so that the eventual
/// move to a CLI design system has a single point of substitution.
fn try_write_line(out: &mut dyn Write, line: &str) -> io::Result<()> {
    writeln!(out, "{line}")?;
    out.flush()
}
