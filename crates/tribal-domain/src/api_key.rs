//! Sealed API-key type.

use crate::redacted_secret::{RedactedSecret, RedactedSecretParseError, SecretKind};

/// Compile-time marker for [`ApiKey`].
pub enum ApiKeyKind {}

impl SecretKind for ApiKeyKind {
    const DEBUG_NAME: &'static str = "ApiKey";
    const NOUN: &'static str = "api key";
}

/// Provider API key.
///
/// See [`RedactedSecret`] for the construction, validation, redaction,
/// and serde round-trip discipline shared with all secret newtypes.
pub type ApiKey = RedactedSecret<ApiKeyKind>;

/// Failure parsing an [`ApiKey`].
pub type ApiKeyParseError = RedactedSecretParseError<ApiKeyKind>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_label_is_api_key() {
        let key: ApiKey = "sk-secret".parse().unwrap();
        let debug = format!("{key:?}");
        assert!(debug.starts_with("ApiKey("), "unexpected debug: {debug}");
    }

    #[test]
    fn test_parse_error_message_uses_api_key_noun() {
        let err: ApiKeyParseError = "".parse::<ApiKey>().unwrap_err();
        assert_eq!(err.to_string(), "api key must not be empty");
    }
}
