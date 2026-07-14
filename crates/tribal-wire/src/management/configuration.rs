//! Revisioned configuration and product-action contract.

use std::fmt;

use serde::{Deserialize, Serialize};
use tribal_domain::{AuthTokenId, ConfigFieldPath, ProjectId, ProviderKind};

use super::{
    ConfigDigest, ConfigRevision, CredentialSourceId, EmbeddingProfileRevision, KnownModelId,
};

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

/// Proposed configuration value checked without persistence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigValidateRequest {
    /// The dotted field path being checked.
    pub key: ConfigFieldPath,
    /// The candidate JSON value.
    pub value: serde_json::Value,
}

/// One configuration rule violated by a candidate value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigViolation {
    /// The dotted field path that violates the rule.
    pub key: String,
    /// The operator-facing violation detail.
    pub message: String,
}

/// Validation result for a proposed configuration value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigValidation {
    /// Whether the candidate satisfies every configuration rule.
    pub valid: bool,
    /// Every violated rule, empty for a valid candidate.
    pub violations: Vec<ConfigViolation>,
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

/// Atomic model-selection request.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ModelSelectionRequest {
    pub model: KnownModelId,
    pub stages: Vec<InferenceStage>,
    pub endpoint: EndpointSelection,
    pub credential: Option<CredentialInput>,
    pub reuse_api_key_for_embedding: bool,
    pub expected_revision: ConfigRevision,
}

/// Requested endpoint transition for model selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum EndpointSelection {
    Preserve,
    ProviderDefault,
    Custom { value: String },
}

/// Inline credential or manager-issued credential capability.
#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum CredentialInput {
    Literal { value: SecretLiteral },
    Source { source: CredentialSourceId },
}

impl fmt::Debug for CredentialInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal { .. } => formatter.write_str("Literal { value: <redacted> }"),
            Self::Source { source } => formatter.debug_tuple("Source").field(source).finish(),
        }
    }
}

/// Request for use-bound credential capabilities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CredentialSourcesRequest {
    pub use_case: CredentialUse,
    pub expected_revision: ConfigRevision,
}

/// Operation to which a credential capability is bound.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum CredentialUse {
    ModelSelection {
        model: KnownModelId,
        stages: Vec<InferenceStage>,
        endpoint: EndpointSelection,
    },
    Genesis {
        embedding: GenesisEmbeddingInput,
    },
}

/// One opaque credential capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CredentialSource {
    pub id: CredentialSourceId,
    pub kind: CredentialSourceKind,
}

/// Durable configuration origin referenced by a capability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum CredentialSourceKind {
    InferenceStage { stage: InferenceStage },
    EmbeddingConnection { name: String },
}

/// Available credential capabilities for one exact request context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CredentialSources {
    pub sources: Vec<CredentialSource>,
    pub capabilities: CredentialUseCapabilities,
    pub revision: ConfigRevision,
}

/// Additional actions permitted by a credential capability set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum CredentialUseCapabilities {
    ModelSelection {
        embedding_reuse: EmbeddingReuseAvailability,
    },
    Genesis,
}

/// Whether an inference key can also back an embedding connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum EmbeddingReuseAvailability {
    AvailableExisting {
        connection: String,
    },
    AvailableCreate {
        connection: String,
    },
    Unavailable {
        reason: EmbeddingReuseUnavailableReason,
    },
}

/// Reason embedding-key reuse is unavailable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "reason", content = "data", rename_all = "snake_case")]
pub enum EmbeddingReuseUnavailableReason {
    ProviderUnsupported,
    EndpointMismatch,
    ConnectionNameConflict { connection: String },
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
    pub api_key_embedding_reuse: bool,
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
    pub embedding: GenesisEmbeddingInput,
    pub credential: Option<CredentialInput>,
    pub expected_revision: ConfigRevision,
}

/// Candidate embedding identity for graph genesis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GenesisEmbeddingInput {
    pub provider: ProviderKind,
    pub model: String,
    pub dimensions: Option<u32>,
    pub base_url: Option<String>,
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
    pub providers: Vec<GenesisProviderOption>,
    pub revision: ConfigRevision,
}

/// Genesis capability for one provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct GenesisProviderOption {
    pub provider: ProviderKind,
    pub availability: GenesisProviderAvailability,
}

/// Whether a provider can establish graph genesis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum GenesisProviderAvailability {
    Available {
        credential: CredentialRequirement,
        endpoint: EndpointRequirement,
        model: GenesisModelConstraint,
        dimensions: GenesisDimensionsConstraint,
    },
    Unavailable {
        reason: GenesisUnavailableReason,
    },
}

/// Reason a provider cannot establish graph genesis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GenesisUnavailableReason {
    NoEmbeddingApi,
    ManagedGatewayTransport,
}

/// Validation rule for a genesis model name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum GenesisModelConstraint {
    NonEmptyNoWhitespace,
}

/// Validation rule for genesis embedding dimensions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum GenesisDimensionsConstraint {
    OptionalRange { min: u32, max: u32 },
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
    CredentialCapabilityInvalid {
        reason: CredentialCapabilityInvalidReason,
    },
    CredentialSourceUnavailable,
    CredentialConnectionConflict {
        connection: String,
    },
    EndpointTransitionRefused {
        reason: EndpointTransitionRefusal,
    },
    ConfigPatchRefused {
        reason: ConfigPatchRefusal,
    },
    UnknownModel {
        id: KnownModelId,
    },
    InvalidStageSet {
        reason: InvalidStageSetReason,
    },
    EmbeddingReuseRefused {
        reason: EmbeddingReuseUnavailableReason,
    },
    ConfigPersistenceUnavailable {
        phase: ConfigPersistencePhase,
        observation: ConfigPersistenceObservation,
    },
    ModelUnavailable {
        reason: ModelUnavailableReason,
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

/// Reason a credential capability cannot be consumed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CredentialCapabilityInvalidReason {
    WrongConnection,
    ManagerReplaced,
    RevisionChanged,
    Reissued,
    Consumed,
    UseMismatch,
    Unknown,
}

/// Reason an endpoint transition is unsafe or incomplete.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum EndpointTransitionRefusal {
    ProviderChangeRequiresEndpoint,
    ProviderHasNoDefault,
    CredentialFanoutHasMultipleEndpoints,
    CredentialRequired,
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

/// Structural reason an inference-stage set is invalid.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "reason", content = "data", rename_all = "snake_case")]
pub enum InvalidStageSetReason {
    Empty,
    Duplicate { stage: InferenceStage },
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
            embedding: GenesisEmbeddingInput {
                provider: ProviderKind::OpenAi,
                model: "text-embedding-3-small".to_owned(),
                dimensions: None,
                base_url: None,
            },
            credential: Some(CredentialInput::Literal { value: secret }),
            expected_revision: revision(),
        };
        assert!(!format!("{request:?}").contains(sentinel));
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
