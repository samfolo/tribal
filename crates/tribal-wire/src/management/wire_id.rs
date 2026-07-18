//! Validated opaque identities carried by the management contract.

use std::{fmt, str::FromStr};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

const DECODED_LENGTH: usize = 32;

/// Failure parsing a management-contract identifier.
#[derive(Debug, thiserror::Error)]
pub enum WireIdError {
    /// The required type prefix was absent.
    #[error("wire id must begin with {expected}")]
    InvalidPrefix { expected: &'static str },
    /// The payload was not valid unpadded base64url.
    #[error("decoding wire id: {source}")]
    Decode {
        /// The base64url decoder's structural cause.
        #[source]
        source: base64::DecodeError,
    },
    /// The decoded payload had the wrong byte length.
    #[error("wire id decoded to {actual} bytes; expected {expected}")]
    InvalidLength { expected: usize, actual: usize },
    /// The payload was an alternate spelling for the same bytes.
    #[error("wire id is not in canonical unpadded base64url form")]
    NonCanonical,
    /// A model id did not satisfy the catalogue grammar.
    #[error("model id does not match the catalogue-id grammar")]
    InvalidModelId,
}

macro_rules! define_id_debug {
    (visible, $name:ident) => {
        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }
    };
    (redacted, $name:ident) => {
        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("<redacted credential source id>")
            }
        }
    };
}

macro_rules! define_canonical_id {
    ($debug:ident, $name:ident, $prefix:literal, $pattern:literal, $swift_type:literal) => {
        #[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Required prefix for this identifier kind.
            pub const PREFIX: &'static str = $prefix;
            /// Whole-string grammar pinned into JSON Schema.
            pub const PATTERN: &'static str = $pattern;

            /// Parses the canonical wire spelling.
            ///
            /// # Errors
            ///
            /// Returns [`WireIdError`] when the prefix, payload, length, or
            /// canonical spelling is invalid.
            pub fn parse(raw: &str) -> Result<Self, WireIdError> {
                parse_canonical(raw, Self::PREFIX).map(Self)
            }

            /// Returns the canonical wire spelling.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = WireIdError;

            fn from_str(raw: &str) -> Result<Self, Self::Err> {
                Self::parse(raw)
            }
        }

        impl TryFrom<String> for $name {
            type Error = WireIdError;

            fn try_from(raw: String) -> Result<Self, Self::Error> {
                Self::parse(&raw)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = WireIdError;

            fn try_from(raw: &str) -> Result<Self, Self::Error> {
                Self::parse(raw)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        define_id_debug!($debug, $name);

        #[cfg(feature = "schema")]
        impl schemars::JsonSchema for $name {
            fn schema_name() -> String {
                stringify!($name).to_owned()
            }

            fn json_schema(
                _generator: &mut schemars::r#gen::SchemaGenerator,
            ) -> schemars::schema::Schema {
                marked_string_schema(Some(Self::PATTERN), $swift_type, Some(Self::PREFIX))
            }
        }
    };
}

define_canonical_id!(
    visible,
    ConfigDigest,
    "cdig_",
    r"^cdig_[A-Za-z0-9_-]{43}$",
    "canonical-base64url-32"
);
define_canonical_id!(
    visible,
    ConfigRevision,
    "cfg_",
    r"^cfg_[A-Za-z0-9_-]{43}$",
    "canonical-base64url-32"
);
define_canonical_id!(
    visible,
    EmbeddingProfileRevision,
    "epr_",
    r"^epr_[A-Za-z0-9_-]{43}$",
    "canonical-base64url-32"
);
define_canonical_id!(
    visible,
    PanicCorrelationId,
    "pcorr_",
    r"^pcorr_[A-Za-z0-9_-]{43}$",
    "canonical-base64url-32"
);

/// Compatibility name for the shared canonical wire-id parser failure.
pub type PanicCorrelationIdParseError = WireIdError;

impl ConfigDigest {
    /// Derives the digest identity for exact configuration bytes.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let digest = sha2::Sha256::digest(bytes);
        Self(format!(
            "{}{}",
            Self::PREFIX,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest),
        ))
    }
}

impl ConfigRevision {
    /// Projects a proven digest onto its compare-token namespace.
    #[must_use]
    pub fn from_digest(digest: &ConfigDigest) -> Self {
        let payload = digest
            .as_str()
            .chars()
            .skip(ConfigDigest::PREFIX.len())
            .collect::<String>();
        Self(format!("{}{}", Self::PREFIX, payload))
    }
}

/// Opaque identity of a curated model-catalogue row.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct KnownModelId(String);

impl KnownModelId {
    /// Whole-string grammar pinned into JSON Schema.
    pub const PATTERN: &'static str = r"^[a-z][a-z0-9_.-]{0,127}$";

    /// Parses a catalogue identity.
    ///
    /// # Errors
    ///
    /// Returns [`WireIdError::InvalidModelId`] when the value is outside the
    /// catalogue grammar.
    pub fn parse(raw: &str) -> Result<Self, WireIdError> {
        let mut bytes = raw.bytes();
        let valid = matches!(bytes.next(), Some(b'a'..=b'z'))
            && raw.len() <= 128
            && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-'));
        valid
            .then(|| Self(raw.to_owned()))
            .ok_or(WireIdError::InvalidModelId)
    }

    /// Returns the catalogue identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for KnownModelId {
    type Err = WireIdError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl TryFrom<String> for KnownModelId {
    type Error = WireIdError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<KnownModelId> for String {
    fn from(id: KnownModelId) -> Self {
        id.0
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for KnownModelId {
    fn schema_name() -> String {
        "KnownModelId".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        marked_string_schema(Some(Self::PATTERN), "validated-string", None)
    }
}

fn parse_canonical(raw: &str, prefix: &'static str) -> Result<String, WireIdError> {
    let payload = raw
        .strip_prefix(prefix)
        .ok_or(WireIdError::InvalidPrefix { expected: prefix })?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|source| WireIdError::Decode { source })?;
    if bytes.len() != DECODED_LENGTH {
        return Err(WireIdError::InvalidLength {
            expected: DECODED_LENGTH,
            actual: bytes.len(),
        });
    }
    if base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes) != payload {
        return Err(WireIdError::NonCanonical);
    }
    Ok(raw.to_owned())
}

#[cfg(feature = "schema")]
pub(super) fn marked_string_schema(
    pattern: Option<&str>,
    swift_type: &'static str,
    base64url_prefix: Option<&str>,
) -> schemars::schema::Schema {
    let mut schema = schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::String.into()),
        ..Default::default()
    };
    schema.string = pattern.map(|pattern| {
        Box::new(schemars::schema::StringValidation {
            pattern: Some(pattern.to_owned()),
            ..Default::default()
        })
    });
    schema.extensions.insert(
        "x-tribal-swift-type".to_owned(),
        serde_json::Value::String(swift_type.to_owned()),
    );
    if let Some(prefix) = base64url_prefix {
        schema.extensions.insert(
            "x-tribal-base64url-prefix".to_owned(),
            serde_json::Value::String(prefix.to_owned()),
        );
    }
    schema.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digest_and_revision_share_bytes_under_distinct_prefixes() {
        let digest = ConfigDigest::from_bytes(b"config");
        let revision = ConfigRevision::from_digest(&digest);
        assert_eq!(
            digest.as_str().strip_prefix(ConfigDigest::PREFIX),
            revision.as_str().strip_prefix(ConfigRevision::PREFIX),
        );
    }

    #[test]
    fn test_noncanonical_base64url_is_rejected() {
        let canonical = ConfigDigest::from_bytes(&[7_u8; DECODED_LENGTH]);
        let mut altered = canonical.as_str().to_owned();
        altered.replace_range(altered.len() - 1.., "d");
        assert!(matches!(
            ConfigDigest::parse(&altered),
            Err(WireIdError::NonCanonical | WireIdError::Decode { .. })
        ));
    }

    #[test]
    fn test_model_id_grammar_is_enforced() {
        assert!(KnownModelId::parse("openai.gpt-4_1").is_ok());
        assert!(KnownModelId::parse("OpenAI.gpt").is_err());
        assert!(KnownModelId::parse("").is_err());
    }

    #[test]
    fn test_panic_correlation_uses_the_shared_canonical_parser() {
        let raw = format!(
            "{}{}",
            PanicCorrelationId::PREFIX,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([7_u8; DECODED_LENGTH]),
        );
        let id = PanicCorrelationId::parse(&raw).expect("correlation is canonical");
        let encoded = serde_json::to_string(&id).expect("correlation serialises");
        let decoded: PanicCorrelationId =
            serde_json::from_str(&encoded).expect("correlation deserialises");
        assert_eq!(decoded, id);
    }
}
