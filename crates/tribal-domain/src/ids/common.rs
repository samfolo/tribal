/// Error returned when parsing a prefixed ID string fails.
#[derive(Debug, thiserror::Error)]
pub enum IdParseError {
    /// The input string did not start with the expected prefix.
    #[error("expected prefix '{expected}', input was: {input}")]
    MissingPrefix {
        /// The prefix that was expected (e.g. `"proj"`).
        expected: &'static str,
        /// The full input string that failed to parse.
        input: String,
    },

    /// The UUID portion of the input string was not a valid UUID.
    #[error("invalid UUID in id '{input}'")]
    InvalidUuid {
        /// The full input string that failed to parse.
        input: String,
        /// The underlying UUID parse error.
        #[source]
        source: uuid::Error,
    },
}

/// Defines a newtype wrapper around [`uuid::Uuid`] with a required string prefix.
///
/// The generated type enforces the prefix format at parse time, provides
/// [`Display`](std::fmt::Display)/[`FromStr`](std::str::FromStr) implementations
/// that include the prefix, and prevents cross-entity misuse at compile time.
///
/// # Generated API
///
/// - `Type::new()` — creates a new random v4 ID
/// - `Type::inner()` — returns a reference to the inner `Uuid`
/// - `Type::PREFIX` — the prefix string constant
/// - `Display` — formats as `{prefix}_{uuid}`
/// - `FromStr` — parses and validates the prefix
/// - `TryFrom<String>` — delegates to `FromStr`
/// - `From<Type> for String` — delegates to `Display`
/// - `Copy`, `Clone`, `Eq`, `Hash`, `Serialize`, `Deserialize`, `Default`
macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash,
            serde::Serialize, serde::Deserialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(uuid::Uuid);

        impl $name {
            /// Creates a new random ID with a v4 UUID.
            #[must_use]
            pub fn new() -> Self {
                Self(uuid::Uuid::new_v4())
            }

            /// Returns a reference to the inner UUID.
            #[must_use]
            pub fn inner(&self) -> &uuid::Uuid {
                &self.0
            }

            /// The string prefix used when displaying and parsing this ID type.
            pub const PREFIX: &'static str = $prefix;
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}_{}", $prefix, self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = $crate::IdParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let raw = s.strip_prefix(concat!($prefix, "_"))
                    .ok_or_else(|| $crate::IdParseError::MissingPrefix {
                        expected: $prefix,
                        input: s.to_string(),
                    })?;
                let uuid = uuid::Uuid::parse_str(raw)
                    .map_err(|e| $crate::IdParseError::InvalidUuid {
                        input: s.to_string(),
                        source: e,
                    })?;
                Ok(Self(uuid))
            }
        }

        impl TryFrom<String> for $name {
            type Error = $crate::IdParseError;

            fn try_from(s: String) -> Result<Self, Self::Error> {
                s.parse()
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> String {
                id.to_string()
            }
        }
    };
}

pub(crate) use define_id;

/// Generates the standard test suite for an ID newtype produced by [`define_id!`].
///
/// Covers: parse success, wrong prefix, invalid UUID, `Display`/`FromStr`
/// roundtrip, serde roundtrip, and `Copy` semantics.
#[cfg(test)]
macro_rules! id_tests {
    ($type:ty, $prefix:literal) => {
        #[test]
        fn test_parse_valid() {
            let input = format!("{}_{}", $prefix, "550e8400-e29b-41d4-a716-446655440000");
            let id: $type = input.parse().expect("should parse valid prefixed UUID");
            assert_eq!(
                id.inner().to_string(),
                "550e8400-e29b-41d4-a716-446655440000"
            );
        }

        #[test]
        fn test_parse_wrong_prefix() {
            let input = format!("{}_{}", "wrong", "550e8400-e29b-41d4-a716-446655440000");
            let err = input.parse::<$type>().unwrap_err();
            assert!(
                matches!(err, $crate::IdParseError::MissingPrefix { .. }),
                "expected MissingPrefix, got: {err:?}"
            );
        }

        #[test]
        fn test_parse_invalid_uuid() {
            let input = format!("{}_{}", $prefix, "not-a-uuid");
            let err = input.parse::<$type>().unwrap_err();
            assert!(
                matches!(err, $crate::IdParseError::InvalidUuid { .. }),
                "expected InvalidUuid, got: {err:?}"
            );
        }

        #[test]
        fn test_display_fromstr_roundtrip() {
            let id = <$type>::new();
            let s = id.to_string();
            let parsed: $type = s.parse().expect("should roundtrip through Display/FromStr");
            assert_eq!(id, parsed);
        }

        #[test]
        fn test_serde_roundtrip() {
            let id = <$type>::new();
            let json = serde_json::to_string(&id).expect("should serialise");
            let parsed: $type = serde_json::from_str(&json).expect("should deserialise");
            assert_eq!(id, parsed);
        }

        #[test]
        fn test_copy_semantics() {
            let id = <$type>::new();
            let copied = id;
            // Both bindings remain usable — `id` was not moved.
            assert_eq!(id, copied);
        }
    };
}

#[cfg(test)]
pub(crate) use id_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_prefix_error_message() {
        let err = IdParseError::MissingPrefix {
            expected: "proj",
            input: "ki_550e8400-e29b-41d4-a716-446655440000".to_string(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("proj"),
            "message should contain expected prefix"
        );
        assert!(msg.contains("ki_"), "message should contain the input");
    }

    #[test]
    fn test_invalid_uuid_error_message() {
        let uuid_err = uuid::Uuid::parse_str("not-a-uuid").unwrap_err();
        let err = IdParseError::InvalidUuid {
            input: "proj_not-a-uuid".to_string(),
            source: uuid_err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("proj_not-a-uuid"),
            "message should contain the input"
        );
    }
}
