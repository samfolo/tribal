//! Sealed bearer-token type.

use crate::redacted_secret::{RedactedSecret, RedactedSecretParseError, SecretKind};

/// Compile-time marker for [`BearerToken`].
pub enum BearerTokenKind {}

impl SecretKind for BearerTokenKind {
    const DEBUG_NAME: &'static str = "BearerToken";
    const NOUN: &'static str = "bearer token";
}

/// MCP bearer token issued by `tribal token create` and persisted in
/// the namespaced stable credential envelope for later wire-up.
///
/// See [`RedactedSecret`] for the construction, validation, redaction,
/// and serde round-trip discipline shared with all secret newtypes.
pub type BearerToken = RedactedSecret<BearerTokenKind>;

/// Failure parsing a [`BearerToken`].
pub type BearerTokenParseError = RedactedSecretParseError<BearerTokenKind>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_label_is_bearer_token() {
        let token: BearerToken = "Iz5pXq".parse().unwrap();
        let debug = format!("{token:?}");
        assert!(
            debug.starts_with("BearerToken("),
            "unexpected debug: {debug}",
        );
    }

    #[test]
    fn test_parse_error_message_uses_bearer_token_noun() {
        let err: BearerTokenParseError = "".parse::<BearerToken>().unwrap_err();
        assert_eq!(err.to_string(), "bearer token must not be empty");
    }
}
