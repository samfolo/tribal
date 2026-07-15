//! Validated name for a reusable provider connection.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// The JSON Schema pattern for a provider-connection name.
pub const PROVIDER_CONNECTION_NAME_PATTERN: &str = r"^[a-z][a-z0-9_]*$";

/// A stable lowercase name for one reusable provider connection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProviderConnectionName(String);

impl ProviderConnectionName {
    /// Parses a provider-connection name.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderConnectionNameError`] when `raw` is not a lowercase
    /// snake-case identifier beginning with a letter.
    pub fn parse(raw: &str) -> Result<Self, ProviderConnectionNameError> {
        is_valid_provider_connection_name(raw)
            .then(|| Self(raw.to_owned()))
            .ok_or_else(|| ProviderConnectionNameError {
                value: raw.to_owned(),
            })
    }

    /// Returns the canonical connection name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderConnectionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ProviderConnectionName {
    type Err = ProviderConnectionNameError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl TryFrom<&str> for ProviderConnectionName {
    type Error = ProviderConnectionNameError;

    fn try_from(raw: &str) -> Result<Self, Self::Error> {
        Self::parse(raw)
    }
}

impl TryFrom<String> for ProviderConnectionName {
    type Error = ProviderConnectionNameError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<ProviderConnectionName> for String {
    fn from(name: ProviderConnectionName) -> Self {
        name.0
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for ProviderConnectionName {
    fn schema_name() -> String {
        "ProviderConnectionName".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let validation = schemars::schema::StringValidation {
            pattern: Some(PROVIDER_CONNECTION_NAME_PATTERN.to_owned()),
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

/// Failure parsing a [`ProviderConnectionName`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid provider connection name '{value}'")]
pub struct ProviderConnectionNameError {
    value: String,
}

/// Returns whether `name` satisfies the provider-connection grammar.
#[must_use]
pub fn is_valid_provider_connection_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_connection_names_round_trip_through_serde() {
        for raw in ["ollama_default", "openai_2", "a"] {
            let name = ProviderConnectionName::parse(raw).expect("fixture is valid");
            let encoded = serde_json::to_string(&name).expect("name serialises");
            let decoded: ProviderConnectionName =
                serde_json::from_str(&encoded).expect("name deserialises");
            assert_eq!(decoded, name);
        }
    }

    #[test]
    fn test_invalid_connection_names_are_rejected() {
        for raw in ["", "OpenAI", "2openai", "open-ai", "open.ai", "åpenai"] {
            assert!(
                ProviderConnectionName::parse(raw).is_err(),
                "accepted {raw:?}"
            );
            let encoded = serde_json::to_string(raw).expect("fixture serialises");
            assert!(serde_json::from_str::<ProviderConnectionName>(&encoded).is_err());
        }
    }
}
