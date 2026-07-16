//! Revisioned configuration and product-action contract.

use std::fmt;

use serde::{Deserialize, Serialize};
use tribal_domain::{
    AuthTokenId, ConfigFieldPath, ProjectId, ProviderConnectionName, ProviderKind,
};

use super::{ConfigDigest, ConfigRevision, EmbeddingProfileRevision, KnownModelId};

/// Observable effect of a successful configuration write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "effect", content = "data", rename_all = "snake_case")]
pub enum ConfigWriteEffect {
    Unchanged,
    CommittedNoRuntimeEffect,
    AppliedLive,
    OnNextStart,
    AwaitingRestart,
    Shadowed { by: String },
}

/// Durable revision and runtime effect of one field write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigWriteOutcome {
    pub effect: ConfigWriteEffect,
    pub revision: ConfigRevision,
}

/// Durability state of the complete configuration document.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ConfigDocument {
    DurableValid {
        values: ConfigLiteral,
        revision: ConfigRevision,
    },
    DurableInvalid {
        revision: ConfigRevision,
    },
    UncertainValid {
        values: ConfigLiteral,
        observed_digest: ConfigDigest,
        phase: ConfigPersistencePhase,
    },
    UncertainInvalid {
        observed_digest: ConfigDigest,
        phase: ConfigPersistencePhase,
    },
    Unreadable {
        phase: ConfigPersistencePhase,
    },
}

/// Request for a single configuration value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigGetRequest {
    pub key: ConfigFieldPath,
}

/// Arbitrary configuration JSON with constant-redacted formatting.
#[derive(PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConfigLiteral(serde_json::Value);

impl ConfigLiteral {
    #[must_use]
    pub fn new(value: serde_json::Value) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn expose_sensitive(&self) -> &serde_json::Value {
        &self.0
    }
}

impl fmt::Debug for ConfigLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted config literal>")
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for ConfigLiteral {
    fn schema_name() -> String {
        "ConfigLiteral".to_owned()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = generator.subschema_for::<serde_json::Value>().into_object();
        schema.extensions.insert(
            "x-cortex-swift-type".to_owned(),
            serde_json::Value::String("redacted-json".to_owned()),
        );
        schema.into()
    }
}

/// Secret supplied inline to a managed configuration operation.
#[derive(PartialEq, zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
pub struct SecretLiteral(String);

/// Failure validating an inline secret.
#[derive(Debug, thiserror::Error)]
pub enum SecretLiteralError {
    #[error("secret literal is empty")]
    Empty,
    #[error("secret literal contains whitespace")]
    ContainsWhitespace,
}

impl TryFrom<String> for SecretLiteral {
    type Error = SecretLiteralError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(SecretLiteralError::Empty);
        }
        if value.chars().any(char::is_whitespace) {
            return Err(SecretLiteralError::ContainsWhitespace);
        }
        Ok(Self(value))
    }
}

impl Serialize for SecretLiteral {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SecretLiteral {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

impl SecretLiteral {
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted secret literal>")
    }
}

#[cfg(feature = "schema")]
impl schemars::JsonSchema for SecretLiteral {
    fn schema_name() -> String {
        "SecretLiteral".to_owned()
    }

    fn json_schema(_generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        super::wire_id::marked_string_schema(Some(r"^\S+$"), "redacted-validated-string", None)
    }
}

/// Durability state of a single configuration value.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ConfigValue {
    DurableValid {
        key: ConfigFieldPath,
        value: ConfigLiteral,
        revision: ConfigRevision,
    },
    DurableInvalid {
        key: ConfigFieldPath,
        revision: ConfigRevision,
    },
    UncertainValid {
        key: ConfigFieldPath,
        value: ConfigLiteral,
        observed_digest: ConfigDigest,
        phase: ConfigPersistencePhase,
    },
    UncertainInvalid {
        key: ConfigFieldPath,
        observed_digest: ConfigDigest,
        phase: ConfigPersistencePhase,
    },
    Unreadable {
        phase: ConfigPersistencePhase,
    },
}

/// Revision-checked single-field write.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigSetRequest {
    pub key: ConfigFieldPath,
    pub value: ConfigLiteral,
    pub expected_revision: ConfigRevision,
}

impl fmt::Debug for ConfigSetRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigSetRequest")
            .field("key", &self.key)
            .field("value", &"<redacted>")
            .field("expected_revision", &self.expected_revision)
            .finish()
    }
}

/// Revision-checked atomic multi-field write.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigPatchRequest {
    pub changes: Vec<ConfigPatchChange>,
    pub expected_revision: ConfigRevision,
}

/// One field in an atomic configuration patch.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigPatchChange {
    pub key: ConfigFieldPath,
    pub value: ConfigLiteral,
}

impl fmt::Debug for ConfigPatchChange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigPatchChange")
            .field("key", &self.key)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Field-by-field effects from one atomic patch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigPatchOutcome {
    pub fields: Vec<ConfigFieldOutcome>,
    pub revision: ConfigRevision,
}

/// Runtime effect assigned to one patched field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigFieldOutcome {
    pub key: ConfigFieldPath,
    pub effect: ConfigWriteEffect,
}

/// Inference configuration stage.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum InferenceStage {
    Extraction,
    Triage,
    Relation,
}

/// One curated model choice and its current capability classification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct KnownModelEntry {
    pub id: KnownModelId,
    pub provider: ProviderKind,
    pub model: String,
    pub display_name: String,
    pub access: ModelAccess,
    pub availability: ModelAvailability,
    pub settings: ModelSettingsCapability,
    /// Compatible configured connections, in canonical name order.
    pub connections: Vec<ProviderConnectionName>,
}

/// Curated model choices under one configuration revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ModelsCatalogue {
    pub models: Vec<KnownModelEntry>,
    pub revision: ConfigRevision,
}

/// How a model is authenticated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ModelAccess {
    Platform,
    BringYourOwn,
}

/// Whether a catalogue row is currently selectable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ModelAvailability {
    Available,
    Unavailable { reason: ModelUnavailableReason },
}

/// Stable reason a catalogue row is unavailable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ModelUnavailableReason {
    PlatformEndpointUnavailable,
}

/// Settings inputs supported by one model row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ModelSettingsCapability {
    pub credential: CredentialRequirement,
    pub endpoint: EndpointRequirement,
}

/// Credential input required by a product action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CredentialRequirement {
    None,
    ApiKey,
}

/// Endpoint input supported by a product action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum EndpointRequirement {
    PrimaryWithDefault { value: String },
    AdvancedWithDefault { value: String },
    Unavailable,
}

/// Atomic request to establish graph genesis configuration.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GenesisConfigurationRequest {
    pub connection: ProviderConnectionName,
    pub model: String,
    pub dimensions: Option<u32>,
    pub expected_revision: ConfigRevision,
}

/// Connection-backed embedding identity for graph genesis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GenesisEmbeddingInput {
    pub connection: ProviderConnectionName,
    pub provider: ProviderKind,
    pub model: String,
    pub dimensions: Option<u32>,
}

/// Revision-checked request to converge config on the active profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GenesisConvergenceRequest {
    pub expected_revision: ConfigRevision,
    pub expected_profile_revision: EmbeddingProfileRevision,
}

/// Available genesis choices under one configuration revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GenesisOptions {
    pub recommended: GenesisEmbeddingInput,
    pub connections: Vec<GenesisConnectionOption>,
    pub revision: ConfigRevision,
}

/// Genesis capability for one configured connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GenesisConnectionOption {
    pub connection: ProviderConnectionName,
    pub provider: ProviderKind,
    pub availability: GenesisProviderAvailability,
}

/// Whether a provider can establish graph genesis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum GenesisProviderAvailability {
    Available { models: GenesisModelOptions },
    Unavailable { reason: GenesisUnavailableReason },
}

/// Reason a provider cannot establish graph genesis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GenesisUnavailableReason {
    NoEmbeddingApi,
    ManagedGatewayTransport,
    CredentialMissing,
}

/// Models served for one configured embedding connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum GenesisModelOptions {
    Catalogue {
        models: Vec<EmbeddingModelOption>,
        allow_other: bool,
        constraint: GenesisModelConstraint,
    },
    Freeform {
        constraint: GenesisModelConstraint,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct EmbeddingModelOption {
    pub model: String,
    pub display_name: String,
    pub dimensions: EmbeddingDimensions,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GenesisModelConstraint {
    NonEmptyNoWhitespace,
}

/// Dimension choices served for one embedding model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum EmbeddingDimensions {
    Fixed {
        value: u32,
    },
    Native {
        resolved: Option<u32>,
    },
    Selectable {
        default: Option<u32>,
        min: u32,
        max: u32,
    },
}

/// Stable summary of a graph's active embedding profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct EmbeddingProfileSummary {
    pub provider: ProviderKind,
    pub base_url: String,
    pub model: String,
    pub dimensions: u32,
}

/// Active graph embedding identity and drift classification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum GraphEmbeddingProfile {
    NoProfile,
    Active {
        profile: EmbeddingProfileSummary,
        profile_revision: EmbeddingProfileRevision,
        connection: Option<ProviderConnectionName>,
        configured: GenesisEmbeddingInput,
        genesis_drift: Option<String>,
    },
    Unknown {
        detail: String,
    },
}

/// Durable configuration delta notification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigChangeEvent {
    pub revision: ConfigRevision,
    pub source: ConfigChangeSource,
    pub changed: Vec<ConfigFieldPath>,
}

/// Origin of a durable configuration change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ConfigChangeSource {
    Managed,
    RawFile,
}

/// Public error envelope for configuration and product actions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagementResponseError {
    pub message: String,
    pub error: ManagementError,
}

/// Stable failure classification for configuration and product actions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "code", content = "data", rename_all = "snake_case")]
pub enum ManagementError {
    /// Manager code reached an invariant it could not uphold.
    InternalInvariant,
    /// An explicitly requested external probe could not produce evidence.
    ProbeUnavailable,
    ConfigurationInvalid {
        fields: Vec<ConfigFieldPath>,
    },
    ConfigConflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
    ProfileConflict {
        expected: EmbeddingProfileRevision,
        actual: EmbeddingProfileRevision,
    },
    ConfigPatchRefused {
        reason: ConfigPatchRefusal,
    },
    ConfigPersistenceUnavailable {
        phase: ConfigPersistencePhase,
        observation: ConfigPersistenceObservation,
    },
    ModelUnavailable {
        reason: ModelUnavailableReason,
    },
    ProviderConnectionNotFound {
        name: ProviderConnectionName,
    },
    ProviderConnectionRemovalRefused {
        reason: super::ProviderConnectionRemovalRefusal,
    },
    ProviderCredentialMutationRefused {
        reason: super::CredentialMutationRefusal,
    },
    GenesisPolicyRefused {
        reason: GenesisPolicyRefusal,
    },
    Administration {
        failure: AdministrationFailure,
    },
}

/// Inventory row that could not fit within the management response budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum InventoryItemRef {
    Project(ProjectId),
    Token(AuthTokenId),
}

/// Stable client-actionable administration failure classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "code", content = "data", rename_all = "snake_case")]
pub enum AdministrationFailure {
    OperationTimedOut,
    ManagerShuttingDown,
    DatabaseUnavailable,
    DatabaseMigrationFailed,
    ProjectSourceInvalid,
    ProjectNotFound { id: ProjectId },
    TokenIssuanceRefused,
    PersistedCredentialUnavailable,
    PersistedCredentialRecoveryFailed,
    InventoryItemTooLarge { item: InventoryItemRef },
    IntegrationTargetIncompatible,
    IntegrationUnavailable,
    ReindexUnavailable,
    ThreadRetentionRefused,
}

/// Structural reason an atomic patch is refused.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "reason", content = "data", rename_all = "snake_case")]
pub enum ConfigPatchRefusal {
    Empty,
    Duplicate {
        key: ConfigFieldPath,
    },
    Overlapping {
        parent: ConfigFieldPath,
        child: ConfigFieldPath,
    },
    MixedHotAndNonHot,
    MultipleHotFields,
}

/// Point at which durable configuration persistence became uncertain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ConfigPersistencePhase {
    NotCommitted,
    DurabilityUncertain,
}

/// Filesystem observation retained after persistence uncertainty.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ConfigPersistenceObservation {
    Observed { digest: ConfigDigest },
    Unreadable,
}

/// Reason a graph-genesis action violates active-profile policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GenesisPolicyRefusal {
    ProfileUnavailable,
    ProfileAuthorityBusy,
    ActiveProfileForbidsDrift,
    ProviderUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn revision() -> ConfigRevision {
        ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"config"))
    }

    #[test]
    fn test_secret_literal_rejects_empty_and_whitespace() {
        assert!(matches!(
            SecretLiteral::try_from(String::new()),
            Err(SecretLiteralError::Empty)
        ));
        assert!(matches!(
            SecretLiteral::try_from("not secret".to_owned()),
            Err(SecretLiteralError::ContainsWhitespace)
        ));
    }

    #[test]
    fn test_direct_and_nested_secret_debug_are_redacted() {
        let sentinel = "sentinel-secret-value";
        let secret = SecretLiteral::try_from(sentinel.to_owned()).expect("test secret is valid");
        assert!(!format!("{secret:?}").contains(sentinel));

        let request = GenesisConfigurationRequest {
            connection: ProviderConnectionName::parse("openai_default")
                .expect("fixture connection name is valid"),
            model: "text-embedding-3-small".to_owned(),
            dimensions: None,
            expected_revision: revision(),
        };
        assert!(!format!("{secret:?}").contains(sentinel));
        assert!(format!("{request:?}").contains("openai_default"));
    }

    #[test]
    fn test_config_literal_debug_is_constant_redacted() {
        let literal = ConfigLiteral::new(serde_json::json!({"token": "sentinel"}));
        assert_eq!(format!("{literal:?}"), "<redacted config literal>");
    }

    #[test]
    fn test_administration_failure_retains_both_public_classifications() {
        let error = ManagementError::Administration {
            failure: AdministrationFailure::DatabaseUnavailable,
        };
        assert_eq!(
            serde_json::to_value(error).unwrap(),
            serde_json::json!({
                "code": "administration",
                "data": {
                    "failure": {
                        "code": "database_unavailable"
                    }
                }
            })
        );
    }
}
