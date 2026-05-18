//! Terminal output for `tribal token` subcommands.
//!
//! All user-facing presentation lives here, separated from business logic.
//! Status messages go to stderr; structured data (raw tokens, tables) to
//! stdout.

use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
};

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

/// Writes the raw token as a bare value — suitable for piping, e.g.
/// `tribal token create | pbcopy`. Flushes before returning so a
/// downstream pipe reader sees the bytes even on abrupt process exit.
pub(super) fn raw_token(out: &mut dyn Write, token: &str) -> io::Result<()> {
    writeln!(out, "{token}")?;
    out.flush()
}

pub(super) fn principal_resolved(out: &mut dyn Write, key: &str) {
    let _ = writeln!(out, "  principal: {key}");
}

pub(super) fn token_created(out: &mut dyn Write, expires: &str) {
    let _ = writeln!(out, "  {TOKEN_CREATED} (expires {expires})");
}

// ---------------------------------------------------------------------------
// Revoke output
// ---------------------------------------------------------------------------

/// Confirms a token revocation to stderr.
///
/// When `already_revoked` is true, reports that another process revoked
/// the token before this one did.
pub(super) fn token_revoked(prefix: &str, already_revoked: bool) {
    let msg = if already_revoked {
        TOKEN_ALREADY_REVOKED
    } else {
        TOKEN_REVOKED
    };
    eprintln!("  {msg}: {prefix}");
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
    let now = Utc::now();

    let rows: Vec<_> = tokens
        .iter()
        .zip(prefixes)
        .map(|(t, prefix)| {
            let principal_key = principals[&t.principal_id()].principal_key();
            let created = format_timestamp(t.created_at());
            let expires = format_timestamp(t.expires_at());
            let status = token_status(t, now);
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

/// Returns the display status for a token evaluated against a fixed point
/// in time, ensuring consistent results across an entire table render.
fn token_status(token: &AuthToken, now: DateTime<Utc>) -> &'static str {
    if token.revoked_at().is_some() {
        STATUS_REVOKED
    } else if token.expires_at() < now {
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
        let now = Utc::now();
        let token = an_auth_token()
            .expires_at(now + TimeDelta::try_hours(1).unwrap())
            .revoked_at(None)
            .build();
        assert_eq!(token_status(&token, now), STATUS_ACTIVE);
    }

    #[test]
    fn test_token_status_revoked() {
        let now = Utc::now();
        let token = an_auth_token()
            .expires_at(now + TimeDelta::try_hours(1).unwrap())
            .revoked_at(Some(now))
            .build();
        assert_eq!(token_status(&token, now), STATUS_REVOKED);
    }

    #[test]
    fn test_token_status_expired() {
        let now = Utc::now();
        let token = an_auth_token()
            .expires_at(now - TimeDelta::try_hours(1).unwrap())
            .revoked_at(None)
            .build();
        assert_eq!(token_status(&token, now), STATUS_EXPIRED);
    }

    #[test]
    fn test_token_status_revoked_takes_precedence_over_expired() {
        let now = Utc::now();
        let token = an_auth_token()
            .expires_at(now - TimeDelta::try_hours(1).unwrap())
            .revoked_at(Some(now - TimeDelta::try_hours(2).unwrap()))
            .build();
        assert_eq!(token_status(&token, now), STATUS_REVOKED);
    }

    // -- unique_prefixes ----------------------------------------------------

    #[test]
    fn test_unique_prefixes_no_collisions() {
        let hashes = vec![
            "aaaa0000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "bbbb0000cccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        ];
        let prefixes = unique_prefixes(&hashes);
        assert_eq!(prefixes[0].len(), HASH_PREFIX_LENGTH);
        assert_eq!(prefixes[1].len(), HASH_PREFIX_LENGTH);
    }

    #[test]
    fn test_unique_prefixes_extends_on_collision() {
        let hashes = vec![
            "abcdef00aaaaaaaabbbbbbbbccccccccddddddddeeeeeeeeffffffff00000000",
            "abcdef00bbbbbbbbccccccccddddddddeeeeeeeeffffffff0000000011111111",
        ];
        let prefixes = unique_prefixes(&hashes);
        // First 8 chars are identical; position 9 diverges ('a' vs 'b').
        assert_eq!(prefixes[0].len(), 9);
        assert_eq!(prefixes[1].len(), 9);
        assert_ne!(prefixes[0], prefixes[1]);
    }

    #[test]
    fn test_unique_prefixes_single_hash() {
        let hashes = vec!["deadbeefcafebabe0123456789abcdef0123456789abcdef0123456789abcdef"];
        let prefixes = unique_prefixes(&hashes);
        assert_eq!(prefixes[0].len(), HASH_PREFIX_LENGTH);
    }
}
