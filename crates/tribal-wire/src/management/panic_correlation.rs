//! Opaque correlation identity for a contained panic.

use std::{fmt, str::FromStr};

use base64::Engine as _;
use serde::{Deserialize, Serialize};

const DECODED_LENGTH: usize = 32;

/// Canonical opaque identity for correlating a contained panic report.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PanicCorrelationId(String);

impl PanicCorrelationId {
    /// Prefix separating panic correlations from other opaque identifiers.
    pub const PREFIX: &'static str = "pcorr_";
    /// Whole-string grammar pinned into the public schema.
    pub const PATTERN: &'static str = r"^pcorr_[A-Za-z0-9_-]{43}$";

    /// Parses a canonical panic-correlation identifier.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the prefix, encoding, decoded length, or
    /// canonical spelling is invalid.
    pub fn parse(raw: &str) -> Result<Self, PanicCorrelationIdParseError> {
        let payload = raw
            .strip_prefix(Self::PREFIX)
            .ok_or(PanicCorrelationIdParseError::InvalidPrefix)?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|source| PanicCorrelationIdParseError::InvalidEncoding { source })?;
        if bytes.len() != DECODED_LENGTH {
            return Err(PanicCorrelationIdParseError::InvalidLength {
                actual: bytes.len(),
            });
        }
        if base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes) != payload {
            return Err(PanicCorrelationIdParseError::NonCanonical);
        }
        Ok(Self(raw.to_owned()))
    }

    /// Returns the canonical wire spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PanicCorrelationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for PanicCorrelationId {
    type Err = PanicCorrelationIdParseError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl TryFrom<&str> for PanicCorrelationId {
    type Error = PanicCorrelationIdParseError;

    fn try_from(raw: &str) -> Result<Self, Self::Error> {
        Self::parse(raw)
    }
}

impl TryFrom<String> for PanicCorrelationId {
    type Error = PanicCorrelationIdParseError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<PanicCorrelationId> for String {
    fn from(id: PanicCorrelationId) -> Self {
        id.0
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for PanicCorrelationId {
    fn schema_name() -> String {
        "PanicCorrelationId".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let validation = schemars::schema::StringValidation {
            pattern: Some(Self::PATTERN.to_owned()),
            ..Default::default()
        };
        let mut schema = schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            string: Some(Box::new(validation)),
            ..Default::default()
        };
        schema.extensions.insert(
            "x-cortex-swift-type".to_owned(),
            serde_json::Value::String("canonical-base64url-32".to_owned()),
        );
        schema.extensions.insert(
            "x-cortex-base64url-prefix".to_owned(),
            serde_json::Value::String(Self::PREFIX.to_owned()),
        );
        schema.into()
    }
}

/// Failure parsing a [`PanicCorrelationId`].
#[derive(Debug, thiserror::Error)]
pub enum PanicCorrelationIdParseError {
    /// The identifier did not carry the panic-correlation prefix.
    #[error("panic correlation id has an invalid prefix")]
    InvalidPrefix,
    /// The payload was not unpadded URL-safe base64.
    #[error("panic correlation id has invalid base64url: {source}")]
    InvalidEncoding {
        /// The decoder's structural cause.
        #[source]
        source: base64::DecodeError,
    },
    /// The decoded payload was not a SHA-256-sized value.
    #[error("panic correlation id decodes to {actual} bytes, expected {DECODED_LENGTH}")]
    InvalidLength {
        /// Decoded byte count.
        actual: usize,
    },
    /// The payload had an alternate spelling for the same bytes.
    #[error("panic correlation id is not canonically encoded")]
    NonCanonical,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn an_id() -> String {
        format!(
            "{}{}",
            PanicCorrelationId::PREFIX,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7_u8; DECODED_LENGTH]),
        )
    }

    #[test]
    fn test_a_canonical_correlation_round_trips_through_serde() {
        let id = PanicCorrelationId::parse(&an_id()).expect("fixture is canonical");
        let encoded = serde_json::to_string(&id).expect("correlation serialises");
        let decoded: PanicCorrelationId =
            serde_json::from_str(&encoded).expect("correlation deserialises");
        assert_eq!(decoded, id);
    }

    #[test]
    fn test_a_wrong_prefix_is_rejected() {
        let raw = an_id().replacen(PanicCorrelationId::PREFIX, "other_", 1);
        assert!(matches!(
            PanicCorrelationId::parse(&raw),
            Err(PanicCorrelationIdParseError::InvalidPrefix)
        ));
    }

    #[test]
    fn test_a_wrong_decoded_length_is_rejected() {
        let raw = format!(
            "{}{}",
            PanicCorrelationId::PREFIX,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7_u8; DECODED_LENGTH - 1]),
        );
        assert!(matches!(
            PanicCorrelationId::parse(&raw),
            Err(PanicCorrelationIdParseError::InvalidLength { .. })
        ));
    }

    #[test]
    fn test_malformed_base64url_is_rejected() {
        let raw = format!("{}{}", PanicCorrelationId::PREFIX, "!".repeat(43));
        assert!(matches!(
            PanicCorrelationId::parse(&raw),
            Err(PanicCorrelationIdParseError::InvalidEncoding { .. })
        ));
    }
}
