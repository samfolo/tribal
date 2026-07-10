//! Validated identity for a field in the configuration schema.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// The JSON Schema pattern for a dotted configuration-field path.
pub const CONFIG_FIELD_PATH_PATTERN: &str = r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$";

/// A dotted path made only from non-empty snake-case schema identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConfigFieldPath(String);

impl ConfigFieldPath {
    /// Parses a configuration-field path.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigFieldPathError::Empty`] for an empty path, or
    /// [`ConfigFieldPathError::InvalidSegment`] for the first malformed segment.
    pub fn parse(raw: &str) -> Result<Self, ConfigFieldPathError> {
        if raw.is_empty() {
            return Err(ConfigFieldPathError::Empty);
        }

        for (index, segment) in raw.split('.').enumerate() {
            if !is_valid_segment(segment) {
                return Err(ConfigFieldPathError::InvalidSegment {
                    index,
                    segment: segment.to_owned(),
                });
            }
        }

        Ok(Self(raw.to_owned()))
    }

    /// Returns the canonical dotted path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConfigFieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ConfigFieldPath {
    type Err = ConfigFieldPathError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl TryFrom<&str> for ConfigFieldPath {
    type Error = ConfigFieldPathError;

    fn try_from(raw: &str) -> Result<Self, Self::Error> {
        Self::parse(raw)
    }
}

impl TryFrom<String> for ConfigFieldPath {
    type Error = ConfigFieldPathError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<ConfigFieldPath> for String {
    fn from(path: ConfigFieldPath) -> Self {
        path.0
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for ConfigFieldPath {
    fn schema_name() -> String {
        "ConfigFieldPath".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let validation = schemars::schema::StringValidation {
            pattern: Some(CONFIG_FIELD_PATH_PATTERN.to_owned()),
            ..Default::default()
        };
        let mut schema = schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(validation)),
            ..Default::default()
        };
        schema.extensions.insert(
            "x-cortex-swift-type".to_owned(),
            serde_json::Value::String("validated-string".to_owned()),
        );
        schema.into()
    }
}

/// Failure parsing a [`ConfigFieldPath`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigFieldPathError {
    /// The path had no segments.
    #[error("config field path is empty")]
    Empty,
    /// A segment was empty or was not a snake-case identifier.
    #[error("config field path segment {index} is invalid: {segment}")]
    InvalidSegment {
        /// Zero-based segment position.
        index: usize,
        /// The malformed segment.
        segment: String,
    },
}

fn is_valid_segment(segment: &str) -> bool {
    let mut bytes = segment.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_field_paths_round_trip_through_serde() {
        for raw in ["logging", "logging.level", "server.sse.idle_timeout_ms"] {
            let path = ConfigFieldPath::parse(raw).expect("fixture is valid");
            let encoded = serde_json::to_string(&path).expect("field path serialises");
            let decoded: ConfigFieldPath =
                serde_json::from_str(&encoded).expect("field path deserialises");
            assert_eq!(decoded, path);
        }
    }

    #[test]
    fn test_empty_field_path_is_rejected() {
        assert_eq!(
            ConfigFieldPath::parse("").unwrap_err(),
            ConfigFieldPathError::Empty,
        );
    }

    #[test]
    fn test_malformed_field_path_reports_its_first_invalid_segment() {
        for (raw, index, segment) in [
            ("Logging.level", 0, "Logging"),
            ("logging..level", 1, ""),
            ("logging.log-level", 1, "log-level"),
            ("logging.2level", 1, "2level"),
        ] {
            assert!(matches!(
                ConfigFieldPath::parse(raw),
                Err(ConfigFieldPathError::InvalidSegment {
                    index: actual_index,
                    segment: actual_segment,
                }) if actual_index == index && actual_segment == segment
            ));
        }
    }

    #[test]
    fn test_deserialisation_cannot_bypass_field_path_validation() {
        assert!(serde_json::from_str::<ConfigFieldPath>(r#""credentials.bad-key""#).is_err());
    }
}
