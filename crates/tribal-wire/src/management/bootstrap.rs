//! Atomic bootstrap request and hand-off DTOs.

use serde::{Deserialize, Serialize};
use tribal_domain::{ProviderConnectionName, Scope};
use url::Url;

use super::{
    ConfigRevision, DatabaseInitialiseOutcome, IssuedBearerToken, McpTargetSelection,
    ProcessingProfile, ProjectRegisterInput, ProjectRegisterOutcome, ProjectSummary,
    ProviderConnectionInput, PublicMcpConfigDocument, Revisioned, SecretLiteral,
    SensitiveMcpConfigDocument, TokenSummary,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "storage", content = "data", rename_all = "snake_case")]
pub enum BootstrapStorage {
    Configured,
    External { database_url: SecretLiteral },
}

/// Absolute OTLP HTTP endpoint accepted at the contract boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct OtlpEndpoint(String);

#[cfg(feature = "schema")]
impl schemars::JsonSchema for OtlpEndpoint {
    fn schema_name() -> String {
        "OtlpEndpoint".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        super::wire_id::marked_string_schema(
            Some(r"^https?://[^@\s/?#]+(?::[0-9]+)?(?:[/?#][^\s]*)?$"),
            "validated-string",
            None,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OtlpEndpointError {
    #[error("OTLP endpoint is not an absolute URL")]
    NotAbsolute,
    #[error("OTLP endpoint must use http or https")]
    UnsupportedScheme,
    #[error("OTLP endpoint must not contain user information")]
    UserInformation,
}

impl TryFrom<String> for OtlpEndpoint {
    type Error = OtlpEndpointError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parsed = Url::parse(&value).map_err(|_| OtlpEndpointError::NotAbsolute)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(OtlpEndpointError::UnsupportedScheme);
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(OtlpEndpointError::UserInformation);
        }
        if parsed.host().is_none() {
            return Err(OtlpEndpointError::NotAbsolute);
        }
        Ok(Self(value))
    }
}

impl From<OtlpEndpoint> for String {
    fn from(value: OtlpEndpoint) -> Self {
        value.0
    }
}

impl OtlpEndpoint {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BootstrapTelemetryInput {
    pub otlp_endpoint: OtlpEndpoint,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BootstrapGenesisInput {
    pub connection: ProviderConnectionName,
    pub model: String,
    pub dimensions: Option<u32>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BootstrapProviderConnectionInput {
    pub name: ProviderConnectionName,
    pub connection: ProviderConnectionInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "policy", content = "data", rename_all = "snake_case")]
pub enum BootstrapTokenPolicy {
    EnsureLocalCredential {
        principal: Option<String>,
        ttl_hours: Option<u64>,
        scopes: Vec<Scope>,
    },
    Create {
        principal: Option<String>,
        ttl_hours: Option<u64>,
        scopes: Vec<Scope>,
    },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BootstrapRequest {
    pub expected_revision: ConfigRevision,
    pub storage: BootstrapStorage,
    pub provider_connections: Vec<BootstrapProviderConnectionInput>,
    pub processing: Option<ProcessingProfile>,
    pub genesis: Option<BootstrapGenesisInput>,
    pub telemetry: Option<BootstrapTelemetryInput>,
    pub additional_project: Option<ProjectRegisterInput>,
    pub token: BootstrapTokenPolicy,
    pub integration: McpTargetSelection,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum BootstrapPublicCredential {
    Existing {
        summary: TokenSummary,
    },
    Issued {
        token: IssuedBearerToken,
        summary: TokenSummary,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CredentialOrigin {
    Existing,
    Issued,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "exposure", content = "data", rename_all = "snake_case")]
pub enum BootstrapHandoff {
    Public {
        credential: BootstrapPublicCredential,
        integration: PublicMcpConfigDocument,
    },
    PersistedBearer {
        credential: TokenSummary,
        origin: CredentialOrigin,
        integration: SensitiveMcpConfigDocument,
    },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct BootstrapOutcome {
    pub database: DatabaseInitialiseOutcome,
    pub system_project: ProjectSummary,
    pub additional_project: Option<ProjectRegisterOutcome>,
    pub handoff: BootstrapHandoff,
}

pub type BootstrapResult = Revisioned<BootstrapOutcome>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_endpoint_refuses_unsafe_forms_during_deserialisation() {
        for endpoint in [
            r#""collector:4318""#,
            r#""grpc://collector.example/v1/traces""#,
            r#""https://user:password@collector.example/v1/traces""#,
        ] {
            assert!(serde_json::from_str::<OtlpEndpoint>(endpoint).is_err());
        }
        assert!(
            serde_json::from_str::<OtlpEndpoint>(r#""https://collector.example/v1/traces""#)
                .is_ok()
        );
    }
}
