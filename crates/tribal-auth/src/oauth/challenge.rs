//! Builders for the `WWW-Authenticate: Bearer ...` header.
//!
//! The MCP 2025-11-25 spec extends RFC 6750 with the
//! `resource_metadata` parameter (RFC 9728 §5.1) on 401 and 403
//! responses. This module produces the exact byte string the spec
//! mandates from a typed input.

use url::Url;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Bearer scheme prefix per RFC 6750 §3.
const BEARER_SCHEME: &str = "Bearer";

/// Parameter name for the resource-metadata URL.
const PARAM_RESOURCE_METADATA: &str = "resource_metadata";

/// Parameter name for the scope hint.
const PARAM_SCOPE: &str = "scope";

/// Parameter name for the OAuth error code on step-up responses.
const PARAM_ERROR: &str = "error";

/// Error code returned on insufficient-scope 403 responses.
pub const ERROR_INSUFFICIENT_SCOPE: &str = "insufficient_scope";

/// Error code returned on invalid-token 401 responses (per RFC 6750 §3.1).
pub const ERROR_INVALID_TOKEN: &str = "invalid_token";

// ---------------------------------------------------------------------------
// BearerChallenge
// ---------------------------------------------------------------------------

/// Typed input to [`build_bearer_challenge_header`].
///
/// Constructed once per request at the bearer-middleware boundary;
/// `resource_metadata_url` is shared across all responses for a given
/// resource.
#[derive(Debug, Clone)]
pub struct BearerChallenge {
    /// The URL clients use to fetch the protected-resource metadata.
    pub resource_metadata_url: Url,
    /// Optional scope hint (space-separated tokens).
    pub scope: Option<String>,
    /// Optional OAuth error code.
    pub error: Option<&'static str>,
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builds the `WWW-Authenticate` header value for a Bearer challenge.
///
/// Produces a single byte string starting with the `Bearer` auth-scheme
/// and a comma-separated parameter list. Quoted values escape inner
/// double quotes and backslashes per RFC 7235 §2.2.
#[must_use]
pub fn build_bearer_challenge_header(challenge: &BearerChallenge) -> String {
    let mut buf = String::from(BEARER_SCHEME);
    let mut first = true;
    let mut append = |key: &str, value: &str, quoted: bool| {
        if first {
            buf.push(' ');
            first = false;
        } else {
            buf.push_str(", ");
        }
        buf.push_str(key);
        buf.push('=');
        if quoted {
            buf.push('"');
            for c in value.chars() {
                if c == '"' || c == '\\' {
                    buf.push('\\');
                }
                buf.push(c);
            }
            buf.push('"');
        } else {
            buf.push_str(value);
        }
    };

    if let Some(error) = challenge.error {
        append(PARAM_ERROR, error, true);
    }
    if let Some(scope) = &challenge.scope {
        append(PARAM_SCOPE, scope, true);
    }
    append(
        PARAM_RESOURCE_METADATA,
        challenge.resource_metadata_url.as_str(),
        true,
    );

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).expect("test url parses")
    }

    #[test]
    fn test_bare_challenge_includes_resource_metadata_only() {
        let header = build_bearer_challenge_header(&BearerChallenge {
            resource_metadata_url: url(
                "http://127.0.0.1:8080/.well-known/oauth-protected-resource",
            ),
            scope: None,
            error: None,
        });
        assert_eq!(
            header,
            r#"Bearer resource_metadata="http://127.0.0.1:8080/.well-known/oauth-protected-resource""#,
        );
    }

    #[test]
    fn test_scope_and_resource_metadata_emitted_with_quoted_values() {
        let header = build_bearer_challenge_header(&BearerChallenge {
            resource_metadata_url: url("https://example.com/.well-known/oauth-protected-resource"),
            scope: Some("tribal:read tribal:write".to_owned()),
            error: None,
        });
        assert_eq!(
            header,
            r#"Bearer scope="tribal:read tribal:write", resource_metadata="https://example.com/.well-known/oauth-protected-resource""#,
        );
    }

    #[test]
    fn test_insufficient_scope_step_up_emits_error_scope_and_resource_metadata() {
        let header = build_bearer_challenge_header(&BearerChallenge {
            resource_metadata_url: url("https://example.com/.well-known/oauth-protected-resource"),
            scope: Some("tribal:write".to_owned()),
            error: Some(ERROR_INSUFFICIENT_SCOPE),
        });
        assert!(header.starts_with(r#"Bearer error="insufficient_scope", scope="tribal:write""#));
        assert!(header.contains(r#"resource_metadata="https://example.com/"#));
    }

    #[test]
    fn test_escapes_double_quotes_in_values() {
        let header = build_bearer_challenge_header(&BearerChallenge {
            resource_metadata_url: url("https://example.com/path"),
            scope: Some(r#"value"with"quotes"#.to_owned()),
            error: None,
        });
        // The escaped quotes survive in the header.
        assert!(header.contains(r#"value\"with\"quotes"#));
    }
}
