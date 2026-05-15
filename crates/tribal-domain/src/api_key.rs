//! Sealed API-key type.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::REDACTED;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Failure parsing an [`ApiKey`].
///
/// Variants are intentionally fieldless: parse failures may originate
/// from a value that is *almost* a real key (a sk-… string with a
/// trailing newline, say), so carrying the raw input here would defeat
/// the redaction discipline enforced on [`ApiKey`] itself.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ApiKeyParseError {
    /// The input was empty.
    #[error("api key must not be empty")]
    Empty,

    /// The input contained one or more whitespace characters.
    ///
    /// No production provider issues keys that include whitespace; an
    /// input that does is almost certainly the result of a trailing
    /// newline, a wrapped value, or a misconfigured shell substitution.
    /// Rejecting strictly surfaces the malformed value at the boundary
    /// instead of silently emitting a broken `Authorization` header.
    #[error("api key must not contain whitespace")]
    ContainsWhitespace,
}

// ---------------------------------------------------------------------------
// ApiKey
// ---------------------------------------------------------------------------

/// Provider API key, sealed at construction.
///
/// An `ApiKey` value is guaranteed non-empty and free of whitespace.
/// Every entry point — [`FromStr`], [`TryFrom<String>`], and the serde
/// `try_from = "String"` deserialise path — funnels through the same
/// strict validation, so downstream code can rely on the type without
/// re-checking.
///
/// **No `Display`**: deliberately omitted so that accidental
/// `format!("{key}")` calls fail to compile, preventing the secret from
/// leaking into log lines or error messages. Use [`Self::as_str`] when
/// the raw value is genuinely required (e.g. constructing an
/// `Authorization` header). Serde serialisation goes through
/// [`From<ApiKey> for String`] rather than `Display`.
///
/// **Redacting `Debug`**: implemented by hand rather than derived, so
/// that `tracing::debug!("{key:?}")` and similar paths cannot leak the
/// raw value either.
///
/// Opportunistic callers — where an empty or malformed input should be
/// treated as "not set" rather than an error — should use
/// `value.parse::<ApiKey>().ok()` to convert the [`Result`] into an
/// `Option`, matching the standard idiom.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ApiKey(String);

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ApiKey").field(&REDACTED).finish()
    }
}

impl ApiKey {
    /// Returns the raw key string.
    ///
    /// Callers are responsible for not logging or persisting the result;
    /// redaction lives at the config presentation layer, not on this
    /// accessor.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ApiKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl FromStr for ApiKey {
    type Err = ApiKeyParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.is_empty() {
            return Err(ApiKeyParseError::Empty);
        }
        if raw.chars().any(char::is_whitespace) {
            return Err(ApiKeyParseError::ContainsWhitespace);
        }
        Ok(Self(raw.to_owned()))
    }
}

impl TryFrom<String> for ApiKey {
    type Error = ApiKeyParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ApiKey> for String {
    fn from(key: ApiKey) -> Self {
        key.0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_accepts_non_empty_no_whitespace() {
        assert_eq!("sk-abc".parse::<ApiKey>().unwrap().as_str(), "sk-abc");
    }

    #[test]
    fn test_from_str_rejects_empty() {
        assert_eq!("".parse::<ApiKey>(), Err(ApiKeyParseError::Empty));
    }

    #[test]
    fn test_from_str_rejects_whitespace_only() {
        let err = "   ".parse::<ApiKey>().unwrap_err();
        assert_eq!(err, ApiKeyParseError::ContainsWhitespace);
    }

    #[test]
    fn test_from_str_rejects_leading_whitespace() {
        let err = "  sk-abc".parse::<ApiKey>().unwrap_err();
        assert_eq!(err, ApiKeyParseError::ContainsWhitespace);
    }

    #[test]
    fn test_from_str_rejects_trailing_whitespace() {
        let err = "sk-abc\n".parse::<ApiKey>().unwrap_err();
        assert_eq!(err, ApiKeyParseError::ContainsWhitespace);
    }

    #[test]
    fn test_from_str_rejects_internal_whitespace() {
        let err = "sk abc".parse::<ApiKey>().unwrap_err();
        assert_eq!(err, ApiKeyParseError::ContainsWhitespace);
    }

    #[test]
    fn test_opportunistic_ok_returns_none_on_malformed() {
        assert!("".parse::<ApiKey>().ok().is_none());
        assert!("   ".parse::<ApiKey>().ok().is_none());
        assert!("sk-abc\n".parse::<ApiKey>().ok().is_none());
    }

    #[test]
    fn test_try_from_string_delegates_to_from_str() {
        let key = ApiKey::try_from("sk-abc".to_owned()).unwrap();
        assert_eq!(key.as_str(), "sk-abc");
        assert!(ApiKey::try_from(String::new()).is_err());
    }

    #[test]
    fn test_into_string_returns_inner() {
        let key: ApiKey = "sk-abc".parse().unwrap();
        assert_eq!(String::from(key), "sk-abc");
    }

    #[test]
    fn test_deserialize_accepts_non_empty() {
        let key: ApiKey = serde_json::from_str("\"sk-abc\"").unwrap();
        assert_eq!(key.as_str(), "sk-abc");
    }

    #[test]
    fn test_deserialize_rejects_empty() {
        let err = serde_json::from_str::<ApiKey>("\"\"").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_deserialize_rejects_whitespace() {
        let err = serde_json::from_str::<ApiKey>("\"  sk-abc\"").unwrap_err();
        assert!(err.to_string().contains("whitespace"));
    }

    #[test]
    fn test_serialize_emits_inner_string() {
        let key: ApiKey = "sk-abc".parse().unwrap();
        assert_eq!(serde_json::to_string(&key).unwrap(), "\"sk-abc\"");
    }

    #[test]
    fn test_debug_does_not_leak_inner_value() {
        let key: ApiKey = "sk-secret-abc".parse().unwrap();
        let debug = format!("{key:?}");
        assert!(
            !debug.contains("sk-secret-abc"),
            "debug leaked key: {debug}"
        );
        assert!(
            debug.contains(REDACTED),
            "debug missing placeholder: {debug}"
        );
    }

    #[test]
    fn test_parse_error_debug_does_not_leak_malformed_input() {
        // A real key with a trailing newline is the realistic malformed
        // case; the parse error must not carry the value through to any
        // log line, tracing call, or test diagnostic.
        let err = "sk-secret-with-newline\n".parse::<ApiKey>().unwrap_err();
        let debug = format!("{err:?}");
        let display = format!("{err}");
        assert!(
            !debug.contains("sk-secret-with-newline"),
            "debug leaked malformed input: {debug}",
        );
        assert!(
            !display.contains("sk-secret-with-newline"),
            "display leaked malformed input: {display}",
        );
    }
}
