//! Terminal output for `tribal token` subcommands.
//!
//! All user-facing presentation lives here, separated from business logic.
//! Status messages go to stderr; structured data (raw tokens, tables) to
//! stdout.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use tribal_domain::{AuthToken, Principal, PrincipalId};

use crate::commands::common::TIMESTAMP_FORMAT;

// ---------------------------------------------------------------------------
// Constants — status messages
// ---------------------------------------------------------------------------

/// Confirmation after a token is created.
pub(super) const TOKEN_CREATED: &str = "token created";

/// Confirmation after a token is revoked.
pub(super) const TOKEN_REVOKED: &str = "token revoked";

/// Informational message when a revoke targets an already-revoked token.
pub(super) const TOKEN_ALREADY_REVOKED: &str = "token already revoked";

/// Confirmation after a bulk revocation.
pub(super) const TOKENS_REVOKED: &str = "tokens revoked";

/// Message when no tokens exist in the database.
pub(super) const NO_TOKENS: &str = "no tokens found";

// ---------------------------------------------------------------------------
// Constants — error messages
// ---------------------------------------------------------------------------

/// Error when a prefix matches no token.
pub(super) const NO_MATCHING_TOKEN: &str = "no token matches prefix";

/// Error when a prefix matches more than one token.
pub(super) const AMBIGUOUS_PREFIX: &str = "multiple tokens match prefix";

/// Error when the prefix contains non-hex characters.
pub(super) const INVALID_PREFIX: &str = "prefix must be lowercase hexadecimal";

/// Error when a principal key does not exist.
pub(super) const PRINCIPAL_NOT_FOUND: &str = "principal not found";

/// Error when a token references a principal that no longer exists.
pub(super) const ORPHANED_TOKEN: &str = "token references unknown principal";

// ---------------------------------------------------------------------------
// Constants — status labels
// ---------------------------------------------------------------------------

/// Label for an active (non-revoked, non-expired) token.
const STATUS_ACTIVE: &str = "active";

/// Label for a revoked token.
const STATUS_REVOKED: &str = "revoked";

/// Label for an expired token.
const STATUS_EXPIRED: &str = "expired";

// ---------------------------------------------------------------------------
// Constants — display
// ---------------------------------------------------------------------------

/// Number of leading hex characters of the token hash shown as a display
/// prefix. Used consistently in the list table and in revoke confirmations.
pub(super) const HASH_PREFIX_LENGTH: usize = 8;

/// Minimum width for the Prefix column.
const MIN_COL_WIDTH_PREFIX: usize = 6;

/// Minimum width for the Principal column.
const MIN_COL_WIDTH_PRINCIPAL: usize = 9;

/// Minimum width for the Created column.
const MIN_COL_WIDTH_CREATED: usize = 7;

/// Minimum width for the Expires column.
const MIN_COL_WIDTH_EXPIRES: usize = 7;

/// Minimum width for the Status column.
const MIN_COL_WIDTH_STATUS: usize = 6;

/// Spacing between table columns.
const COL_SEPARATOR: &str = "  ";

// ---------------------------------------------------------------------------
// Create output
// ---------------------------------------------------------------------------

/// Prints the raw token to stdout as a bare value (no label, no prefix).
///
/// Suitable for piping, e.g. `tribal token create | pbcopy`.
pub(super) fn raw_token(token: &str) {
    println!("{token}");
}

/// Reports the resolved principal to stderr.
pub(super) fn principal_resolved(key: &str) {
    eprintln!("  principal: {key}");
}

/// Reports a successful token creation to stderr.
pub(super) fn token_created(expires: &str) {
    eprintln!("  {TOKEN_CREATED} (expires {expires})");
}

// ---------------------------------------------------------------------------
// Revoke output
// ---------------------------------------------------------------------------

/// Confirms a successful single-token revocation to stderr.
pub(super) fn token_revoked(prefix: &str) {
    eprintln!("  {TOKEN_REVOKED}: {prefix}");
}

/// Reports that the target token was already revoked.
pub(super) fn token_already_revoked(prefix: &str) {
    eprintln!("  {TOKEN_ALREADY_REVOKED}: {prefix}");
}

/// Reports the number of tokens revoked in a bulk operation.
pub(super) fn tokens_revoked(count: u64) {
    eprintln!("  {count} {TOKENS_REVOKED}");
}

// ---------------------------------------------------------------------------
// List output
// ---------------------------------------------------------------------------

/// Reports that no tokens exist.
pub(super) fn no_tokens() {
    eprintln!("{NO_TOKENS}");
}

/// Prints a table of tokens to stdout with dynamic column widths.
///
/// Columns: Prefix, Principal, Created, Expires, Status. When two tokens
/// share the same default prefix, the displayed prefix is extended until
/// each row is uniquely identifiable from the CLI.
pub(super) fn token_table(tokens: &[AuthToken], principals: &HashMap<PrincipalId, Principal>) {
    let hashes: Vec<&str> = tokens.iter().map(AuthToken::token_hash).collect();
    let prefixes = unique_prefixes(&hashes);

    let rows: Vec<_> = tokens
        .iter()
        .zip(prefixes)
        .map(|(t, prefix)| {
            let principal_key = principals[&t.principal_id()].principal_key();
            let created = format_timestamp(t.created_at());
            let expires = format_timestamp(t.expires_at());
            let status = token_status(t);
            (prefix, principal_key.to_owned(), created, expires, status)
        })
        .collect();

    let prefix_w = rows
        .iter()
        .map(|(p, ..)| p.len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_PREFIX);
    let principal_w = rows
        .iter()
        .map(|(_, p, ..)| p.len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_PRINCIPAL);
    let created_w = rows
        .iter()
        .map(|(_, _, c, ..)| c.len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_CREATED);
    let expires_w = rows
        .iter()
        .map(|(.., e, _)| e.len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_EXPIRES);
    let status_w = rows
        .iter()
        .map(|(.., s)| s.len())
        .max()
        .unwrap_or(0)
        .max(MIN_COL_WIDTH_STATUS);

    let sep = COL_SEPARATOR;

    println!(
        "{:<prefix_w$}{sep}{:<principal_w$}{sep}{:<created_w$}{sep}{:<expires_w$}{sep}{:<status_w$}",
        "Prefix", "Principal", "Created", "Expires", "Status",
    );

    let total_width = prefix_w + principal_w + created_w + expires_w + status_w + (sep.len() * 4);
    println!("{}", "-".repeat(total_width));

    for (prefix, principal_key, created, expires, status) in &rows {
        println!(
            "{prefix:<prefix_w$}{sep}{principal_key:<principal_w$}{sep}{created:<created_w$}{sep}{expires:<expires_w$}{sep}{status:<status_w$}",
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Computes the shortest unique prefix length for a set of hashes,
/// starting from [`HASH_PREFIX_LENGTH`] and extending until every hash
/// is distinguishable. Returns the truncated prefix for each hash.
fn unique_prefixes(hashes: &[&str]) -> Vec<String> {
    // Pass 1: find the global prefix length where all hashes are unique.
    let mut len = HASH_PREFIX_LENGTH;
    let max_len = hashes.iter().map(|h| h.len()).min().unwrap_or(len);
    while len < max_len {
        let mut seen = HashSet::with_capacity(hashes.len());
        if hashes.iter().all(|h| seen.insert(&h[..len])) {
            break;
        }
        len += 1;
    }

    // Pass 2: truncate each hash to the resolved length.
    hashes.iter().map(|h| h[..len].to_owned()).collect()
}

/// Returns the display status for a token.
fn token_status(token: &AuthToken) -> &'static str {
    if token.revoked_at().is_some() {
        STATUS_REVOKED
    } else if token.expires_at() < Utc::now() {
        STATUS_EXPIRED
    } else {
        STATUS_ACTIVE
    }
}

/// Formats a timestamp for display in the table.
fn format_timestamp(dt: DateTime<Utc>) -> String {
    dt.format(TIMESTAMP_FORMAT).to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use tribal_test_utils::an_auth_token;

    use super::*;

    #[test]
    fn test_token_status_active() {
        let token = an_auth_token()
            .expires_at(Utc::now() + TimeDelta::try_hours(1).unwrap())
            .revoked_at(None)
            .build();
        assert_eq!(token_status(&token), STATUS_ACTIVE);
    }

    #[test]
    fn test_token_status_revoked() {
        let token = an_auth_token()
            .expires_at(Utc::now() + TimeDelta::try_hours(1).unwrap())
            .revoked_at(Some(Utc::now()))
            .build();
        assert_eq!(token_status(&token), STATUS_REVOKED);
    }

    #[test]
    fn test_token_status_expired() {
        let token = an_auth_token()
            .expires_at(Utc::now() - TimeDelta::try_hours(1).unwrap())
            .revoked_at(None)
            .build();
        assert_eq!(token_status(&token), STATUS_EXPIRED);
    }

    #[test]
    fn test_token_status_revoked_takes_precedence_over_expired() {
        let token = an_auth_token()
            .expires_at(Utc::now() - TimeDelta::try_hours(1).unwrap())
            .revoked_at(Some(Utc::now() - TimeDelta::try_hours(2).unwrap()))
            .build();
        assert_eq!(token_status(&token), STATUS_REVOKED);
    }
}
