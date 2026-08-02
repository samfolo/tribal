//! Product-level Settings projections and revisioned actions.

use serde::{Deserialize, Serialize};
use tribal_domain::{GraphId, ProviderConnectionName, ProviderKind};

use super::{
    CheckSubject, ConfigFieldPath, ConfigPatchChange, ConfigPatchOutcome, ConfigRevision,
    CredentialRequirement, EndpointRequirement, LifecycleSnapshot, ProbeReceiptFreshness,
    SecretLiteral,
};
use crate::operator_check::CheckName;

/// Revision-coherent lifecycle and first-run Settings projection.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ManagerSnapshot {
    pub lifecycle: LifecycleSnapshot,
    pub settings: SettingsSetupSnapshot,
    /// Identity of the configured target, when it is ready and proved at
    /// this snapshot's revision; `None` for an unconfigured, unavailable,
    /// or not-ready target.
    pub graph_id: Option<GraphId>,
}

/// First-run progress derived by the manager from one configuration revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SettingsSetupSnapshot {
    pub revision: ConfigRevision,
    pub focus: Option<SettingsSetupFocus>,
    pub steps: Vec<SettingsSetupStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "focus", content = "data", rename_all = "snake_case")]
pub enum SettingsSetupFocus {
    Connection,
    ProviderConnection {
        name: ProviderConnectionName,
    },
    ProcessingProfile,
    GraphGenesis,
    Readiness {
        check: CheckName,
        subject: Option<CheckSubject>,
        destination: SettingsFocusDestination,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "destination", content = "data", rename_all = "snake_case")]
pub enum SettingsFocusDestination {
    Pane { pane: SettingsPane },
    Diagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SettingsPane {
    Connection,
    Providers,
    Pipeline,
    Graph,
    Authentication,
    Retrieval,
    Prompts,
    Logging,
    Telemetry,
    Server,
    Worker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SettingsSetupStep {
    pub step: SettingsSetupStepKind,
    pub state: SettingsSetupStepState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SettingsSetupStepKind {
    Connection,
    ProviderConnections,
    ProcessingProfile,
    GraphGenesis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum SettingsSetupStepState {
    Complete,
    NeedsAction { reason: SettingsSetupReason },
    Blocked { by: SettingsSetupStepKind },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "reason", content = "data", rename_all = "snake_case")]
pub enum SettingsSetupReason {
    DatabaseUnconfigured,
    DatabaseInvalid,
    DatabaseUnavailable,
    MigrationsNotCurrent,
    ProviderConnectionMissing,
    ProviderCredentialMissing,
    ProcessingProfileInvalid,
    GraphGenesisInvalid,
    GraphProfileDrift,
    ReadinessBlocked { check: CheckName },
}

/// Safe structural projection of the configured database URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseConnectionSummary {
    pub endpoint: DatabaseEndpointSummary,
    pub revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum DatabaseEndpointSummary {
    Unconfigured,
    Parsed {
        scheme: String,
        username: Option<String>,
        host: String,
        port: Option<u16>,
        database: Option<String>,
        password: SecretPresence,
        safe_option_keys: Vec<String>,
        has_additional_options: bool,
    },
    /// A Unix-socket route. Deliberately carries no host or path field: the
    /// socket path is private filesystem detail no summary may project.
    LocalSocket {
        scheme: String,
        username: Option<String>,
        database: Option<String>,
        password: SecretPresence,
        safe_option_keys: Vec<String>,
        has_additional_options: bool,
    },
    OpaqueConfigured,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SecretPresence {
    Absent,
    Configured,
}

/// Health check of the stored configured database.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseProbeRequest {
    pub expected_revision: ConfigRevision,
}

/// Provider connections and capabilities in canonical display order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionsCatalogue {
    pub connections: Vec<ProviderConnectionSummary>,
    pub supported: Vec<ProviderConnectionCapability>,
    pub revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionCapability {
    pub provider: ProviderKind,
    pub credential: CredentialRequirement,
    pub endpoint: EndpointRequirement,
    pub availability: ProviderConnectionAvailability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ProviderConnectionAvailability {
    Available,
    Unavailable {
        reason: ProviderConnectionUnavailableReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ProviderConnectionUnavailableReason {
    PlatformSignInUnavailable,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionSummary {
    pub name: ProviderConnectionName,
    pub provider: ProviderKind,
    pub endpoint: ProviderEndpointSummary,
    pub credential: ProviderCredentialSummary,
    pub referenced_by: Vec<ProviderConnectionUse>,
    pub health: Option<ProviderConnectionProbeReceipt>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionProbeReceipt {
    pub connection: ProviderConnectionName,
    pub provider: ProviderKind,
    pub observed_at_unix_ms: u64,
    pub revision: ConfigRevision,
    pub result: ProviderConnectionProbeResult,
    pub freshness: ProbeReceiptFreshness,
}

/// Result of a non-billable provider connection probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ProviderConnectionProbeResult {
    /// The endpoint accepted its model-discovery request.
    Reachable,
    /// The endpoint or credential rejected the discovery request.
    Failed { failure: ProviderConnectionFailure },
    /// The connection has no direct endpoint to probe.
    Skipped {
        reason: ProviderConnectionProbeSkipReason,
    },
}

/// Why a provider connection cannot be probed directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ProviderConnectionProbeSkipReason {
    /// Tribal Platform availability is established by its authenticated gateway.
    ManagedConnection,
}

/// Stable classification for a failed provider connection probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ProviderConnectionFailureKind {
    Authentication,
    Permission,
    QuotaExhausted,
    RateLimited,
    Overloaded,
    UpstreamUnavailable,
    EndpointUnreachable,
    TimedOut,
    InvalidRequest,
    Unknown,
}

/// Safe diagnostic details from a failed provider connection probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionFailure {
    pub kind: ProviderConnectionFailureKind,
    pub provider_code: Option<String>,
    pub http_status: Option<u16>,
    pub message: Option<String>,
    pub request_id: Option<String>,
    pub retry_after_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum ProviderEndpointSummary {
    Url { value: String },
    Managed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ProviderCredentialSummary {
    NotRequired,
    Managed,
    Missing,
    Configured { suffix: String },
    EnvironmentSupplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "use", content = "data", rename_all = "snake_case")]
pub enum ProviderConnectionUse {
    Inference { stage: InferenceStage },
    ActiveEmbedding,
    GenesisEmbedding,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionUpsertRequest {
    pub name: ProviderConnectionName,
    pub connection: ProviderConnectionInput,
    pub expected_revision: ConfigRevision,
}

#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum ProviderConnectionInput {
    Ollama {
        base_url: String,
    },
    Anthropic {
        base_url: String,
        credential: CredentialMutation,
    },
    OpenAi {
        base_url: String,
        credential: CredentialMutation,
    },
    Platform {},
}

impl std::fmt::Debug for ProviderConnectionInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ollama { base_url } => formatter
                .debug_struct("Ollama")
                .field("base_url", base_url)
                .finish(),
            Self::Anthropic { base_url, .. } => formatter
                .debug_struct("Anthropic")
                .field("base_url", base_url)
                .field("credential", &"<redacted>")
                .finish(),
            Self::OpenAi { base_url, .. } => formatter
                .debug_struct("OpenAi")
                .field("base_url", base_url)
                .field("credential", &"<redacted>")
                .finish(),
            Self::Platform {} => formatter.write_str("Platform"),
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "action", content = "data", rename_all = "snake_case")]
pub enum CredentialMutation {
    Preserve,
    Replace { value: SecretLiteral },
    Clear,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CredentialMutationRefusal {
    PreserveRequiresExistingConnection,
    PreserveRequiresMatchingProvider,
}

impl std::fmt::Debug for CredentialMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Preserve => formatter.write_str("Preserve"),
            Self::Replace { .. } => formatter.write_str("Replace { value: <redacted> }"),
            Self::Clear => formatter.write_str("Clear"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionRemoveRequest {
    pub name: ProviderConnectionName,
    pub expected_revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderConnectionMutationReceipt {
    pub name: ProviderConnectionName,
    pub provider: ProviderKind,
    pub outcome: ConfigPatchOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "reason", content = "data", rename_all = "snake_case")]
pub enum ProviderConnectionRemovalRefusal {
    InUse { uses: Vec<ProviderConnectionUse> },
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProviderProbeRequest {
    pub target: ProviderProbeTarget,
    pub expected_revision: ConfigRevision,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "target", content = "data", rename_all = "snake_case")]
pub enum ProviderProbeTarget {
    Stored {
        name: ProviderConnectionName,
    },
    Candidate {
        existing: Option<ProviderConnectionName>,
        connection: ProviderConnectionInput,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "target", content = "data", rename_all = "snake_case")]
pub enum ProviderProbeResponse {
    Stored {
        receipt: ProviderConnectionProbeReceipt,
    },
    Candidate {
        observation: CandidateProviderProbeObservation,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CandidateProviderProbeObservation {
    pub observed_at_unix_ms: u64,
    pub validated_against_revision: ConfigRevision,
    pub result: ProviderConnectionProbeResult,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "profile", rename_all = "snake_case")]
pub enum ProcessingProfile {
    Efficient {
        model: PresetModelSettings,
    },
    HigherQuality {
        model: PresetModelSettings,
    },
    Custom {
        settings: Box<CustomProcessingSettings>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PresetModelSettings {
    pub connection: ProviderConnectionName,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct StageModelSettings {
    pub connection: ProviderConnectionName,
    pub model: String,
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CustomProcessingSettings {
    pub extraction: ExtractionStageSettings,
    pub triage: VerifiedStageSettings,
    pub relation: VerifiedStageSettings,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ExtractionStageSettings {
    pub model: StageModelSettings,
    pub execution: StageExecutionSettings,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct VerifiedStageSettings {
    pub model: StageModelSettings,
    pub execution: VerifiedStageExecutionSettings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum StageExecutionSettings {
    Efficient {
        max_total_tokens: Option<u64>,
    },
    HigherQuality {
        max_total_tokens: Option<u64>,
        max_turns: Option<u32>,
        execution_deadline_seconds: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum VerifiedStageExecutionSettings {
    Efficient {
        max_total_tokens: Option<u64>,
    },
    HigherQuality {
        verifier: Option<bool>,
        max_total_tokens: Option<u64>,
        max_turns: Option<u32>,
        execution_deadline_seconds: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProcessingProfileSnapshot {
    pub effective: CustomProcessingSettings,
    pub profile: ProcessingProfile,
    pub revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ProcessingProfileSetRequest {
    pub profile: ProcessingProfile,
    pub expected_revision: ConfigRevision,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigValidatePatchRequest {
    pub changes: Vec<ConfigPatchChange>,
    pub expected_revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ConfigPatchValidation {
    pub revision: ConfigRevision,
    pub violations: Vec<PatchConfigViolation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct PatchConfigViolation {
    pub fields: Vec<ConfigFieldPath>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "scope", content = "data", rename_all = "snake_case")]
pub enum SettingsResetScope {
    Processing,
    ProcessingStage { stage: InferenceStage },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SettingsResetPreviewRequest {
    pub scope: SettingsResetScope,
    pub expected_revision: ConfigRevision,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct SettingsResetPreview {
    pub changes: Vec<ConfigPatchChange>,
    pub effective: CustomProcessingSettings,
    pub profile: ProcessingProfile,
    pub revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct AuthenticationSettingsSnapshot {
    pub personal_token_lifetime_hours: u64,
    pub oauth_session_lifetime_hours: u64,
    pub issuer_url: Option<String>,
    pub resource_url: Option<String>,
    pub client_registration: ClientRegistrationAvailability,
    pub revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "state", content = "data", rename_all = "snake_case")]
pub enum ClientRegistrationAvailability {
    Automatic,
    Unavailable {
        reason: ClientRegistrationUnavailableReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ClientRegistrationUnavailableReason {
    NoNetworkTransport,
    RoutableOauthSurface,
}

// Re-exported here so Settings DTOs remain one discoverable vocabulary.
pub use super::configuration::InferenceStage;
