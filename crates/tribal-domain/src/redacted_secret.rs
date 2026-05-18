//! Generic redacting newtype for secret string values.
//!
//! Concrete secret types ([`ApiKey`](crate::ApiKey),
//! [`BearerToken`](crate::BearerToken)) are type aliases over
//! [`RedactedSecret<K>`] keyed by an uninhabited marker that implements
//! [`SecretKind`]. The marker carries the type's display name and noun;
//! the generic owns construction, validation, redaction, and serde
//! round-trip.

use std::{fmt, marker::PhantomData, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::REDACTED;

// ---------------------------------------------------------------------------
// Marker trait
// ---------------------------------------------------------------------------

/// Compile-time marker identifying a concrete secret kind.
///
/// Implementors are uninhabited (`enum K {}`) — purely a type-level
/// discriminator that lets [`RedactedSecret<K>`] tag distinct secrets
/// without runtime cost. [`DEBUG_NAME`](Self::DEBUG_NAME) controls the
/// tuple label in the redacting [`Debug`] impl; [`NOUN`](Self::NOUN)
/// supplies the human-readable noun for parse-error messages.
pub trait SecretKind: 'static {
    /// Tuple label rendered by [`Debug`] (e.g. `"ApiKey"`).
    const DEBUG_NAME: &'static str;
    /// Noun used in parse-error messages (e.g. `"api key"`).
    const NOUN: &'static str;
}

// ---------------------------------------------------------------------------
// RedactedSecret
// ---------------------------------------------------------------------------

/// Secret string value, sealed at construction.
///
/// A `RedactedSecret<K>` is guaranteed non-empty and free of whitespace.
/// Every entry point — [`FromStr`], [`TryFrom<String>`], and the serde
/// `try_from = "String"` deserialise path — funnels through the same
/// strict validation, so downstream code can rely on the type without
/// re-checking.
///
/// **No `Display`**: deliberately omitted so that accidental
/// `format!("{secret}")` calls fail to compile, preventing the value
/// from leaking into log lines or error messages. Use [`Self::as_str`]
/// when the raw value is genuinely required (e.g. constructing an
/// `Authorization` header). Serde serialisation goes through
/// [`From<RedactedSecret<K>> for String`] rather than `Display`.
///
/// **Redacting `Debug`**: implemented by hand rather than derived, so
/// that `tracing::debug!("{secret:?}")` and similar paths cannot leak
/// the raw value either.
#[derive(Serialize, Deserialize)]
#[serde(try_from = "String", into = "String", bound = "")]
pub struct RedactedSecret<K: SecretKind>(String, PhantomData<K>);

impl<K: SecretKind> fmt::Debug for RedactedSecret<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(K::DEBUG_NAME).field(&REDACTED).finish()
    }
}

// Manual `Clone` / `PartialEq` / `Eq` / `Hash` rather than `derive` so
// the generated bounds do not require the marker to implement these
// traits — `PhantomData<K>` supplies each unconditionally.

impl<K: SecretKind> Clone for RedactedSecret<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), PhantomData)
    }
}

impl<K: SecretKind> PartialEq for RedactedSecret<K> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<K: SecretKind> Eq for RedactedSecret<K> {}

impl<K: SecretKind> std::hash::Hash for RedactedSecret<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<K: SecretKind> RedactedSecret<K> {
    /// Returns the raw secret string.
    ///
    /// Callers are responsible for not logging or persisting the result;
    /// redaction lives at the presentation layer, not on this accessor.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<K: SecretKind> AsRef<str> for RedactedSecret<K> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<K: SecretKind> FromStr for RedactedSecret<K> {
    type Err = RedactedSecretParseError<K>;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.is_empty() {
            return Err(RedactedSecretParseError::empty());
        }
        if raw.chars().any(char::is_whitespace) {
            return Err(RedactedSecretParseError::contains_whitespace());
        }
        Ok(Self(raw.to_owned(), PhantomData))
    }
}

impl<K: SecretKind> TryFrom<String> for RedactedSecret<K> {
    type Error = RedactedSecretParseError<K>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl<K: SecretKind> From<RedactedSecret<K>> for String {
    fn from(secret: RedactedSecret<K>) -> Self {
        secret.0
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Failure parsing a [`RedactedSecret<K>`].
///
/// Variants are intentionally fieldless: a parse failure may originate
/// from a value that is *almost* a real secret (a string with a trailing
/// newline, say), so carrying the raw input here would defeat the
/// redaction discipline enforced on [`RedactedSecret<K>`] itself.
pub enum RedactedSecretParseError<K: SecretKind> {
    /// The input was empty.
    Empty(PhantomData<K>),
    /// The input contained one or more whitespace characters.
    ContainsWhitespace(PhantomData<K>),
}

impl<K: SecretKind> RedactedSecretParseError<K> {
    fn empty() -> Self {
        Self::Empty(PhantomData)
    }

    fn contains_whitespace() -> Self {
        Self::ContainsWhitespace(PhantomData)
    }
}

impl<K: SecretKind> fmt::Debug for RedactedSecretParseError<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match self {
            Self::Empty(_) => "Empty",
            Self::ContainsWhitespace(_) => "ContainsWhitespace",
        };
        write!(f, "{}::{variant}", K::DEBUG_NAME)
    }
}

impl<K: SecretKind> fmt::Display for RedactedSecretParseError<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(_) => write!(f, "{} must not be empty", K::NOUN),
            Self::ContainsWhitespace(_) => {
                write!(f, "{} must not contain whitespace", K::NOUN)
            }
        }
    }
}

impl<K: SecretKind> std::error::Error for RedactedSecretParseError<K> {}

impl<K: SecretKind> Clone for RedactedSecretParseError<K> {
    fn clone(&self) -> Self {
        match self {
            Self::Empty(_) => Self::Empty(PhantomData),
            Self::ContainsWhitespace(_) => Self::ContainsWhitespace(PhantomData),
        }
    }
}

impl<K: SecretKind> PartialEq for RedactedSecretParseError<K> {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Empty(_), Self::Empty(_))
                | (Self::ContainsWhitespace(_), Self::ContainsWhitespace(_)),
        )
    }
}

impl<K: SecretKind> Eq for RedactedSecretParseError<K> {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    enum TestKind {}

    impl SecretKind for TestKind {
        const DEBUG_NAME: &'static str = "TestSecret";
        const NOUN: &'static str = "test secret";
    }

    type TestSecret = RedactedSecret<TestKind>;
    type TestSecretParseError = RedactedSecretParseError<TestKind>;

    #[test]
    fn test_from_str_accepts_non_empty_no_whitespace() {
        assert_eq!("sk-abc".parse::<TestSecret>().unwrap().as_str(), "sk-abc");
    }

    #[test]
    fn test_from_str_rejects_empty() {
        assert_eq!("".parse::<TestSecret>(), Err(TestSecretParseError::empty()));
    }

    #[test]
    fn test_from_str_rejects_whitespace_only() {
        assert_eq!(
            "   ".parse::<TestSecret>(),
            Err(TestSecretParseError::contains_whitespace()),
        );
    }

    #[test]
    fn test_from_str_rejects_leading_whitespace() {
        assert_eq!(
            "  sk-abc".parse::<TestSecret>(),
            Err(TestSecretParseError::contains_whitespace()),
        );
    }

    #[test]
    fn test_from_str_rejects_trailing_whitespace() {
        assert_eq!(
            "sk-abc\n".parse::<TestSecret>(),
            Err(TestSecretParseError::contains_whitespace()),
        );
    }

    #[test]
    fn test_from_str_rejects_internal_whitespace() {
        assert_eq!(
            "sk abc".parse::<TestSecret>(),
            Err(TestSecretParseError::contains_whitespace()),
        );
    }

    #[test]
    fn test_opportunistic_ok_returns_none_on_malformed() {
        assert!("".parse::<TestSecret>().ok().is_none());
        assert!("   ".parse::<TestSecret>().ok().is_none());
        assert!("sk-abc\n".parse::<TestSecret>().ok().is_none());
    }

    #[test]
    fn test_try_from_string_delegates_to_from_str() {
        let secret = TestSecret::try_from("sk-abc".to_owned()).unwrap();
        assert_eq!(secret.as_str(), "sk-abc");
        assert!(TestSecret::try_from(String::new()).is_err());
    }

    #[test]
    fn test_into_string_returns_inner() {
        let secret: TestSecret = "sk-abc".parse().unwrap();
        assert_eq!(String::from(secret), "sk-abc");
    }

    #[test]
    fn test_deserialize_accepts_non_empty() {
        let secret: TestSecret = serde_json::from_str("\"sk-abc\"").unwrap();
        assert_eq!(secret.as_str(), "sk-abc");
    }

    #[test]
    fn test_deserialize_rejects_empty() {
        let err = serde_json::from_str::<TestSecret>("\"\"").unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_deserialize_rejects_whitespace() {
        let err = serde_json::from_str::<TestSecret>("\"  sk-abc\"").unwrap_err();
        assert!(err.to_string().contains("whitespace"));
    }

    #[test]
    fn test_serialize_emits_inner_string() {
        let secret: TestSecret = "sk-abc".parse().unwrap();
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"sk-abc\"");
    }

    #[test]
    fn test_debug_uses_kind_name_and_redacts_value() {
        let secret: TestSecret = "sk-secret-value".parse().unwrap();
        let debug = format!("{secret:?}");
        assert!(
            !debug.contains("sk-secret-value"),
            "debug leaked secret: {debug}",
        );
        assert!(
            debug.contains(REDACTED),
            "debug missing placeholder: {debug}"
        );
        assert!(
            debug.contains(TestKind::DEBUG_NAME),
            "debug missing kind name: {debug}",
        );
    }

    #[test]
    fn test_parse_error_messages_use_noun() {
        let empty: TestSecretParseError = "".parse::<TestSecret>().unwrap_err();
        assert!(empty.to_string().contains(TestKind::NOUN));

        let ws: TestSecretParseError = "x y".parse::<TestSecret>().unwrap_err();
        assert!(ws.to_string().contains(TestKind::NOUN));
    }

    #[test]
    fn test_parse_error_debug_does_not_leak_malformed_input() {
        let err: TestSecretParseError = "sk-secret-with-newline\n"
            .parse::<TestSecret>()
            .unwrap_err();
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
