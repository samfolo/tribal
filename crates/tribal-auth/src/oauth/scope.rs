//! Scope catalogue and request-scope validation.
//!
//! The catalogue is the single wire vocabulary of permission scopes the
//! authorisation server advertises (RFC 8414/9728 metadata) and accepts.
//! Every scope that reaches a stored grant — requested at `/authorize`,
//! declared at `/register`, or defaulted at `/token` — resolves against
//! this one definition, so what we advertise, accept, and mint cannot
//! drift apart.

use tribal_domain::{Scope, is_authorised};

use crate::oauth::error::OAuthError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Catalogue of permission scopes advertised in the metadata documents
/// and accepted on requests.
pub const SCOPES_CATALOGUE: &[&str] = &[
    "tribal:read",
    "tribal:write",
    "tribal.knowledge:read",
    "tribal.knowledge:write",
    "tribal.jobs:read",
    "tribal.jobs:write",
];

/// Scope granted when an authorisation code carries no explicit scope.
pub(crate) const DEFAULT_GRANT_SCOPE: &str = "tribal:read";

// ---------------------------------------------------------------------------
// Effective grant resolution
// ---------------------------------------------------------------------------

/// A scope string with surrounding whitespace removed, or `None` if it is
/// empty or whitespace-only. Empty and whitespace-only scopes are treated as
/// absent everywhere a grant is resolved, so a client cannot register or
/// request a blank scope that displays as nothing while a token still mints.
pub(crate) fn present_scope(raw: Option<&str>) -> Option<&str> {
    raw.map(str::trim).filter(|trimmed| !trimmed.is_empty())
}

/// Resolves the effective grant a code carries at issue time: the requested
/// scope if present, else the client's registered scope, else
/// [`DEFAULT_GRANT_SCOPE`]. This is the single definition of the grant the
/// consent page displays and the code stores, so the two cannot disagree.
pub(crate) fn effective_scope(requested: Option<&str>, registered: Option<&str>) -> String {
    present_scope(requested)
        .or_else(|| present_scope(registered))
        .map_or_else(|| DEFAULT_GRANT_SCOPE.to_owned(), str::to_owned)
}

/// Resolves the grant a stored code mints into a token: the code's scope if
/// present, else [`DEFAULT_GRANT_SCOPE`]. Shares the emptiness rule with
/// [`effective_scope`] so a code issued by `/authorize` and the token minted
/// at `/token` carry the same grant the consent page showed.
pub(crate) fn grant_or_default(stored: Option<&str>) -> String {
    present_scope(stored).map_or_else(|| DEFAULT_GRANT_SCOPE.to_owned(), str::to_owned)
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Parses a space-separated scope string into validated [`Scope`] values.
///
/// Each token must parse as a `Scope`; an unparseable token yields
/// [`OAuthError::InvalidScope`]. This is the syntax gate applied when a
/// stored grant's scope is minted into a token; catalogue membership is
/// enforced earlier, at request time, by [`first_uncatalogued_scope`].
pub(crate) fn parse_scope_list(raw: &str) -> Result<Vec<Scope>, OAuthError> {
    raw.split_ascii_whitespace()
        .map(|token| {
            Scope::parse(token).map_err(|_| OAuthError::InvalidScope {
                unknown_token: token.to_owned(),
            })
        })
        .collect()
}

/// Returns the first requested scope token absent from the catalogue, or
/// `None` when every token is catalogued.
///
/// An empty or whitespace-only request yields `None`: the absence of a
/// requested scope is not an uncatalogued scope, and the grant later
/// falls back to [`DEFAULT_GRANT_SCOPE`]. The caller maps the rejected
/// token onto its endpoint's wire error (`invalid_scope` at `/authorize`,
/// `invalid_client_metadata` at `/register`).
pub(crate) fn first_uncatalogued_scope(raw: &str) -> Option<&str> {
    raw.split_ascii_whitespace()
        .find(|token| !SCOPES_CATALOGUE.contains(token))
}

/// Returns the first requested scope token not granted by the client's
/// registered scope, or `None` when every requested token is within it.
///
/// A requested token is within the registration when some registered
/// scope satisfies it (the same hierarchy [`is_authorised`] enforces), so
/// a client registered for the broad `tribal:read` may request the
/// narrower `tribal.knowledge:read`. Catalogue membership is a separate,
/// outer bound enforced by [`first_uncatalogued_scope`]; this is the
/// per-client upper bound DCR registration establishes.
pub(crate) fn scope_exceeding_registration<'a>(
    requested: &'a str,
    registered: &str,
) -> Option<&'a str> {
    let registered: Vec<Scope> = registered
        .split_ascii_whitespace()
        .filter_map(|token| Scope::parse(token).ok())
        .collect();
    requested
        .split_ascii_whitespace()
        .find(|token| match Scope::parse(token) {
            Ok(requested_scope) => !is_authorised(&registered, &requested_scope),
            Err(_) => true,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scopes_catalogue_entries_are_valid_scopes() {
        // The catalogue is the wire vocabulary advertised in both metadata
        // documents. Every entry must parse as a `Scope`, so a typo or a
        // scope that no longer exists cannot drift into what we advertise.
        for raw in SCOPES_CATALOGUE {
            assert!(
                Scope::parse(raw).is_ok(),
                "advertised scope {raw:?} is not a valid Scope",
            );
        }
    }

    #[test]
    fn test_default_grant_scope_is_catalogued() {
        assert!(SCOPES_CATALOGUE.contains(&DEFAULT_GRANT_SCOPE));
    }

    #[test]
    fn test_effective_scope_prefers_requested_then_registered() {
        assert_eq!(
            effective_scope(Some("tribal:read"), Some("tribal:write")),
            "tribal:read",
        );
        assert_eq!(
            effective_scope(None, Some("tribal.knowledge:read")),
            "tribal.knowledge:read",
        );
    }

    #[test]
    fn test_effective_scope_treats_empty_and_whitespace_as_absent() {
        // A blank or whitespace-only requested or registered scope must not
        // display as nothing while a token still mints the default grant.
        assert_eq!(effective_scope(Some(""), Some("")), DEFAULT_GRANT_SCOPE);
        assert_eq!(effective_scope(Some("   "), None), DEFAULT_GRANT_SCOPE);
        assert_eq!(effective_scope(None, Some("  ")), DEFAULT_GRANT_SCOPE);
        assert_eq!(effective_scope(None, None), DEFAULT_GRANT_SCOPE);
    }

    #[test]
    fn test_grant_or_default_shares_the_emptiness_rule() {
        assert_eq!(grant_or_default(Some("tribal:write")), "tribal:write");
        assert_eq!(grant_or_default(Some("")), DEFAULT_GRANT_SCOPE);
        assert_eq!(grant_or_default(Some("   ")), DEFAULT_GRANT_SCOPE);
        assert_eq!(grant_or_default(None), DEFAULT_GRANT_SCOPE);
    }

    #[test]
    fn test_parse_scope_list_accepts_valid_tokens() {
        let scopes = parse_scope_list("tribal:read tribal.knowledge:write").unwrap();
        assert_eq!(scopes.len(), 2);
    }

    #[test]
    fn test_parse_scope_list_rejects_unknown() {
        let err = parse_scope_list("tribal:admin").unwrap_err();
        assert!(matches!(err, OAuthError::InvalidScope { .. }));
    }

    #[test]
    fn test_first_uncatalogued_scope_accepts_catalogued() {
        assert_eq!(
            first_uncatalogued_scope("tribal:read tribal.knowledge:write"),
            None,
        );
    }

    #[test]
    fn test_first_uncatalogued_scope_flags_uncatalogued() {
        // `tribal.knowledge.facts:read` is a valid Scope but is not an
        // advertised catalogue entry, so a request for it is rejected.
        assert_eq!(
            first_uncatalogued_scope("tribal:read tribal.knowledge.facts:read"),
            Some("tribal.knowledge.facts:read"),
        );
    }

    #[test]
    fn test_first_uncatalogued_scope_treats_empty_as_catalogued() {
        assert_eq!(first_uncatalogued_scope(""), None);
        assert_eq!(first_uncatalogued_scope("   "), None);
    }
}
