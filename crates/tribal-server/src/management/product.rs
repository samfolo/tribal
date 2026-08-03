//! Product-level Settings projections and atomic configuration actions.

use base64::Engine as _;
use sha2::Digest as _;
use tribal_config::{
    ClientRegistrationMode, ProcessingProfile as ConfigProcessingProfile, ProviderConnectionConfig,
    TribalConfig,
};
use tribal_db::{
    AdvisoryLockRepository, EmbeddingProfileRepository, PgAdvisoryLockRepository,
    PgEmbeddingProfileRepository, advisory_locks,
};
use tribal_domain::{
    ApiKey, ConfigFieldPath, ProviderConnectionName, ProviderKind, normalise_endpoint_url,
};
use tribal_inference::{KnownModel, known_models};
use tribal_wire::management::{
    AuthenticationSettingsSnapshot, CheckName, CheckObservation, CheckResult, CheckSubject,
    ClientRegistrationAvailability, ClientRegistrationUnavailableReason, ConfigDiagnosticLocation,
    ConfigDocument, ConfigLiteral, ConfigPatchChange, ConfigPatchOutcome, ConfigPatchRequest,
    ConfigPersistenceObservation, ConfigRevision, CredentialMutation, CredentialMutationRefusal,
    CredentialRequirement, DatabaseConnectionSummary, DatabaseEndpointSummary, DegradedReason,
    EmbeddingDimensions, EmbeddingModelOption, EmbeddingProfileRevision, EmbeddingProfileSummary,
    EndpointRequirement, GenesisConfigurationRequest, GenesisConnectionOption,
    GenesisConvergenceRequest, GenesisEmbeddingInput, GenesisModelConstraint, GenesisModelOptions,
    GenesisOptions, GenesisProviderAvailability, GenesisUnavailableReason, GraphEmbeddingProfile,
    InferenceStage, KnownModelEntry, KnownModelId, LifecyclePhase, LifecycleSnapshot,
    ManagementError, ManagementResponseError, ModelAccess, ModelAvailability,
    ModelSettingsCapability, ModelUnavailableReason, ModelsCatalogue, ProcessingProfile,
    ProcessingProfileSetRequest, ProcessingProfileSnapshot, ProviderConnectionAvailability,
    ProviderConnectionCapability, ProviderConnectionInput, ProviderConnectionMutationReceipt,
    ProviderConnectionProbeReceipt, ProviderConnectionRemovalRefusal,
    ProviderConnectionRemoveRequest, ProviderConnectionSummary,
    ProviderConnectionUnavailableReason, ProviderConnectionUpsertRequest, ProviderConnectionUse,
    ProviderConnectionsCatalogue, ProviderCredentialSummary, ProviderEndpointSummary, Revisioned,
    SecretPresence, SettingsFocusDestination, SettingsPane, SettingsResetPreview,
    SettingsResetPreviewRequest, SettingsResetScope, SettingsSetupFocus, SettingsSetupReason,
    SettingsSetupSnapshot, SettingsSetupStep, SettingsSetupStepKind, SettingsSetupStepState,
    StartBlockedVerdict, StartVerdict, StoppedState,
};

use super::{
    application::{
        DatabaseSession,
        operation::{self, OperationContext},
    },
    worker::{ConfigWorkerClient, ConfigWorkerRequestError},
};

/// Factory for product sessions bound to individual management connections.
#[derive(Debug, Clone)]
pub(crate) struct ProductService {
    config: ConfigWorkerClient,
}

/// Product capabilities scoped to one compatible management connection.
#[derive(Debug, Clone)]
pub(crate) struct ProductSession {
    config: ConfigWorkerClient,
}

impl ProductService {
    pub(crate) fn new(config: ConfigWorkerClient) -> Self {
        Self { config }
    }

    pub(crate) fn session(&self) -> ProductSession {
        ProductSession {
            config: self.config.clone(),
        }
    }
}

impl ProductSession {
    pub(in crate::management) async fn setup_snapshot(
        &self,
        operation: &OperationContext,
        lifecycle: &LifecycleSnapshot,
    ) -> Result<SettingsSetupSnapshot, ManagementResponseError> {
        let document = self
            .config
            .for_operation(operation)
            .document()
            .await
            .map_err(ConfigWorkerRequestError::into_public_error)?;
        match document {
            ConfigDocument::DurableValid { .. } => {
                let snapshot = self.snapshot(operation).await?;
                Ok(setup_snapshot(
                    &snapshot.config,
                    snapshot.revision,
                    lifecycle,
                ))
            }
            ConfigDocument::DurableInvalid { revision } => {
                Ok(invalid_setup_snapshot(revision, lifecycle))
            }
            ConfigDocument::UncertainValid {
                observed_digest,
                phase,
                ..
            }
            | ConfigDocument::UncertainInvalid {
                observed_digest,
                phase,
            } => Err(public_error(
                "configuration durability is unavailable",
                ManagementError::ConfigPersistenceUnavailable {
                    phase,
                    observation: ConfigPersistenceObservation::Observed {
                        digest: observed_digest,
                    },
                },
            )),
            ConfigDocument::Unreadable { phase } => Err(public_error(
                "configuration durability is unavailable",
                ManagementError::ConfigPersistenceUnavailable {
                    phase,
                    observation: ConfigPersistenceObservation::Unreadable,
                },
            )),
        }
    }

    pub(in crate::management) async fn database_connection(
        &self,
        operation: &OperationContext,
    ) -> Result<DatabaseConnectionSummary, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        Ok(DatabaseConnectionSummary {
            endpoint: database_endpoint(&snapshot.config.database.url),
            revision: snapshot.revision,
        })
    }

    pub(in crate::management) async fn authentication_settings(
        &self,
        operation: &OperationContext,
    ) -> Result<AuthenticationSettingsSnapshot, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        let config = snapshot.config.as_ref();
        Ok(AuthenticationSettingsSnapshot {
            personal_token_lifetime_hours: config.auth.token_ttl_hours,
            oauth_session_lifetime_hours: config.oauth.access_token_ttl_hours,
            issuer_url: config.oauth.issuer_url.clone(),
            resource_url: config.oauth.resource_url.clone(),
            client_registration: match tribal_config::client_registration_mode(config) {
                ClientRegistrationMode::Automatic => ClientRegistrationAvailability::Automatic,
                ClientRegistrationMode::NoNetworkTransport => {
                    ClientRegistrationAvailability::Unavailable {
                        reason: ClientRegistrationUnavailableReason::NoNetworkTransport,
                    }
                }
                ClientRegistrationMode::RoutableOauthSurface => {
                    ClientRegistrationAvailability::Unavailable {
                        reason: ClientRegistrationUnavailableReason::RoutableOauthSurface,
                    }
                }
            },
            revision: snapshot.revision,
        })
    }

    pub(in crate::management) async fn models_catalogue(
        &self,
        operation: &OperationContext,
    ) -> Result<ModelsCatalogue, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        let models = known_models()
            .iter()
            .copied()
            .map(|model| model_entry(model, &snapshot.config))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ModelsCatalogue {
            models,
            revision: snapshot.revision,
        })
    }

    pub(in crate::management) async fn provider_connections(
        &self,
        operation: &OperationContext,
        receipts: &[ProviderConnectionProbeReceipt],
    ) -> Result<ProviderConnectionsCatalogue, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        let mut connections = snapshot
            .config
            .provider_connections
            .iter()
            .map(|(name, connection)| {
                provider_summary(&snapshot.config, name, connection, receipts)
            })
            .collect::<Vec<_>>();
        connections.sort_by(|left, right| {
            provider_order(left.provider)
                .cmp(&provider_order(right.provider))
                .then_with(|| left.name.as_str().cmp(right.name.as_str()))
        });
        Ok(ProviderConnectionsCatalogue {
            connections,
            supported: ProviderKind::ALL
                .into_iter()
                .map(provider_capability)
                .collect(),
            revision: snapshot.revision,
        })
    }

    pub(in crate::management) async fn upsert_provider_connection(
        &self,
        operation: &OperationContext,
        request: ProviderConnectionUpsertRequest,
    ) -> Result<ProviderConnectionMutationReceipt, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        ensure_revision(&request.expected_revision, &snapshot.revision)?;
        let provider = input_provider(&request.connection);
        let connection = provider_input(
            request.connection,
            snapshot
                .config
                .provider_connections
                .get(request.name.as_str()),
        )?;
        let mut candidate = snapshot.config.as_ref().clone();
        candidate
            .provider_connections
            .insert(request.name.clone(), connection);
        validate_candidate(&candidate)?;
        let outcome = self
            .patch(
                operation,
                snapshot.revision,
                vec![serialised_change(
                    "provider_connections",
                    &candidate.provider_connections,
                )?],
            )
            .await?;
        Ok(ProviderConnectionMutationReceipt {
            name: request.name,
            provider,
            outcome,
        })
    }

    pub(in crate::management) async fn remove_provider_connection(
        &self,
        session: &DatabaseSession,
        request: ProviderConnectionRemoveRequest,
    ) -> Result<ProviderConnectionMutationReceipt, ManagementResponseError> {
        ensure_revision(&request.expected_revision, &session.revision)?;
        let (transaction, profile) = locked_optional_active_profile(session).await?;
        let Some(connection) = session
            .config
            .provider_connections
            .get(request.name.as_str())
        else {
            let _ = transaction.rollback().await;
            return Err(public_error(
                "provider connection does not exist",
                ManagementError::ProviderConnectionNotFound { name: request.name },
            ));
        };
        let provider = connection.provider();
        let active_connection = profile.as_ref().and_then(|profile| {
            let summary = profile_summary(profile);
            session
                .config
                .provider_connections
                .resolve(summary.provider, &summary.base_url)
                .map(|(name, _)| name)
        });
        let uses = provider_uses(&session.config, &request.name, active_connection);
        if !uses.is_empty() {
            let _ = transaction.rollback().await;
            return Err(public_error(
                "provider connection is in use",
                ManagementError::ProviderConnectionRemovalRefused {
                    reason: ProviderConnectionRemovalRefusal::InUse { uses },
                },
            ));
        }
        let mut candidate = session.config.as_ref().clone();
        candidate.provider_connections.remove(&request.name);
        if let Err(error) = validate_candidate(&candidate) {
            let _ = transaction.rollback().await;
            return Err(error);
        }
        let outcome = match self
            .patch(
                session.operation(),
                session.revision.clone(),
                vec![serialised_change(
                    "provider_connections",
                    &candidate.provider_connections,
                )?],
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };
        transaction
            .commit()
            .await
            .map_err(|error| profile_sql_error(&error))?;
        Ok(ProviderConnectionMutationReceipt {
            name: request.name,
            provider,
            outcome,
        })
    }

    pub(in crate::management) async fn processing_profile(
        &self,
        operation: &OperationContext,
    ) -> Result<ProcessingProfileSnapshot, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        Ok(ProcessingProfileSnapshot {
            effective: wire_custom_processing(snapshot.config.custom_processing_settings())?,
            profile: wire_processing(snapshot.config.processing_profile())?,
            revision: snapshot.revision,
        })
    }

    pub(in crate::management) async fn set_processing_profile(
        &self,
        operation: &OperationContext,
        request: ProcessingProfileSetRequest,
    ) -> Result<ConfigPatchOutcome, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        ensure_revision(&request.expected_revision, &snapshot.revision)?;
        let mut candidate = snapshot.config.as_ref().clone();
        candidate.apply_processing_profile(config_processing(request.profile)?);
        validate_candidate(&candidate)?;
        self.patch(
            operation,
            snapshot.revision,
            vec![
                serialised_change("inference", &candidate.inference)?,
                serialised_change("agents", &candidate.agents)?,
            ],
        )
        .await
    }

    pub(in crate::management) async fn reset_preview(
        &self,
        operation: &OperationContext,
        request: SettingsResetPreviewRequest,
    ) -> Result<SettingsResetPreview, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        ensure_revision(&request.expected_revision, &snapshot.revision)?;
        let defaults = TribalConfig::default();
        let mut candidate = snapshot.config.as_ref().clone();
        let changes = match request.scope {
            SettingsResetScope::Processing => {
                candidate.inference = defaults.inference;
                candidate.agents = defaults.agents;
                vec![
                    serialised_change("inference", &candidate.inference)?,
                    serialised_change("agents", &candidate.agents)?,
                ]
            }
            SettingsResetScope::ProcessingStage { stage } => {
                let name = match stage {
                    InferenceStage::Extraction => {
                        candidate.inference.extraction = defaults.inference.extraction;
                        candidate.agents.extraction = defaults.agents.extraction;
                        "extraction"
                    }
                    InferenceStage::Triage => {
                        candidate.inference.triage = defaults.inference.triage;
                        candidate.agents.triage = defaults.agents.triage;
                        "triage"
                    }
                    InferenceStage::Relation => {
                        candidate.inference.relation = defaults.inference.relation;
                        candidate.agents.relation = defaults.agents.relation;
                        "relation"
                    }
                };
                let (inference, agent) = match stage {
                    InferenceStage::Extraction => (
                        &candidate.inference.extraction,
                        &candidate.agents.extraction,
                    ),
                    InferenceStage::Triage => {
                        (&candidate.inference.triage, &candidate.agents.triage)
                    }
                    InferenceStage::Relation => {
                        (&candidate.inference.relation, &candidate.agents.relation)
                    }
                };
                vec![
                    serialised_change(&format!("inference.{name}"), inference)?,
                    serialised_change(&format!("agents.{name}"), agent)?,
                ]
            }
        };
        Ok(SettingsResetPreview {
            changes,
            effective: wire_custom_processing(candidate.custom_processing_settings())?,
            profile: wire_processing(candidate.processing_profile())?,
            revision: snapshot.revision,
        })
    }

    pub(in crate::management) async fn genesis_options(
        &self,
        operation: &OperationContext,
    ) -> Result<GenesisOptions, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        let config = snapshot.config.as_ref();
        Ok(GenesisOptions {
            recommended: configured_embedding(config)?,
            connections: config
                .provider_connections
                .iter()
                .map(|(name, connection)| genesis_connection(name, connection))
                .collect(),
            revision: snapshot.revision,
        })
    }

    pub(in crate::management) async fn configure_genesis(
        &self,
        operation: &OperationContext,
        request: GenesisConfigurationRequest,
    ) -> Result<ConfigPatchOutcome, ManagementResponseError> {
        let snapshot = self.snapshot(operation).await?;
        ensure_revision(&request.expected_revision, &snapshot.revision)?;
        let mut candidate = snapshot.config.as_ref().clone();
        candidate.init.embedding.connection = request.connection;
        candidate.init.embedding.model = request.model;
        candidate.init.embedding.dimensions = request.dimensions;
        validate_candidate(&candidate)?;
        self.patch(
            operation,
            snapshot.revision,
            vec![serialised_change(
                "init.embedding",
                &candidate.init.embedding,
            )?],
        )
        .await
    }

    pub(crate) async fn embedding_profile(
        &self,
        session: &DatabaseSession,
    ) -> Result<Revisioned<GraphEmbeddingProfile>, ManagementResponseError> {
        let profile = active_profile(session).await?;
        let Some(profile) = profile else {
            return Ok(session.revisioned(GraphEmbeddingProfile::NoProfile));
        };
        let summary = profile_summary(&profile);
        let connection = session
            .config
            .provider_connections
            .resolve(summary.provider, &summary.base_url)
            .map(|(name, _)| name.clone());
        let configured = configured_embedding(&session.config)?;
        let genesis_drift = genesis_differs(&session.config, &summary)
            .then(|| "configuration differs from the active embedding profile".to_owned());
        Ok(session.revisioned(GraphEmbeddingProfile::Active {
            profile: summary,
            profile_revision: profile_revision(&profile)?,
            connection,
            configured,
            genesis_drift,
        }))
    }

    pub(crate) async fn converge_genesis(
        &self,
        session: &DatabaseSession,
        request: GenesisConvergenceRequest,
    ) -> Result<ConfigPatchOutcome, ManagementResponseError> {
        let (transaction, profile) = locked_active_profile(session).await?;
        let actual_profile_revision = profile_revision(&profile)?;
        if actual_profile_revision != request.expected_profile_revision {
            let _ = transaction.rollback().await;
            return Err(public_error(
                "embedding profile revision changed",
                ManagementError::ProfileConflict {
                    expected: request.expected_profile_revision,
                    actual: actual_profile_revision,
                },
            ));
        }
        let summary = profile_summary(&profile);
        let Some((connection, _)) = session
            .config
            .provider_connections
            .resolve(summary.provider, &summary.base_url)
        else {
            let _ = transaction.rollback().await;
            return Err(public_error(
                "active embedding profile has no matching provider connection",
                ManagementError::GenesisPolicyRefused {
                    reason: tribal_wire::management::GenesisPolicyRefusal::ProfileUnavailable,
                },
            ));
        };
        let changes = vec![
            serialised_change("init.embedding.connection", connection)?,
            serialised_change("init.embedding.model", &summary.model)?,
            serialised_change("init.embedding.dimensions", &Some(summary.dimensions))?,
        ];
        session
            .operation()
            .checkpoint()
            .map_err(operation::public_error)?;
        let mut outcome = match self
            .config
            .for_operation(session.operation())
            .patch(ConfigPatchRequest {
                changes,
                expected_revision: request.expected_revision,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error.into_public_error());
            }
        };
        transaction
            .commit()
            .await
            .map_err(|error| profile_sql_error(&error))?;
        for field in &mut outcome.fields {
            if !matches!(
                field.effect,
                tribal_wire::management::ConfigWriteEffect::Unchanged
            ) {
                field.effect = tribal_wire::management::ConfigWriteEffect::CommittedNoRuntimeEffect;
            }
        }
        Ok(outcome)
    }

    async fn snapshot(
        &self,
        operation: &OperationContext,
    ) -> Result<super::configuration::ResolvedConfigSnapshot, ManagementResponseError> {
        self.config
            .for_operation(operation)
            .resolved_snapshot()
            .await
            .map_err(ConfigWorkerRequestError::into_public_error)
    }

    async fn patch(
        &self,
        operation: &OperationContext,
        expected_revision: ConfigRevision,
        changes: Vec<ConfigPatchChange>,
    ) -> Result<ConfigPatchOutcome, ManagementResponseError> {
        self.config
            .for_operation(operation)
            .patch(ConfigPatchRequest {
                changes,
                expected_revision,
            })
            .await
            .map_err(ConfigWorkerRequestError::into_public_error)
    }
}

pub(in crate::management) fn database_endpoint(raw: &str) -> DatabaseEndpointSummary {
    if raw.is_empty() {
        return DatabaseEndpointSummary::Unconfigured;
    }
    let Ok(url) = url::Url::parse(raw) else {
        return DatabaseEndpointSummary::OpaqueConfigured;
    };
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return DatabaseEndpointSummary::OpaqueConfigured;
    }
    // Socket classification precedes the absent-host guard: the query-only
    // socket form carries no authority host at all. Query parameters can
    // override every authority component, so a mixed route has no single
    // safe summary to project.
    let authority_host = url.host_str().filter(|host| !host.is_empty());
    let query_pairs = url.query_pairs().collect::<Vec<_>>();
    let host_selectors = query_pairs
        .iter()
        .filter(|(key, _)| key == "host")
        .map(|(_, value)| value.as_ref())
        .collect::<Vec<_>>();
    let route_override_keys = ["hostaddr", "port", "dbname", "user", "password"];
    if query_pairs
        .iter()
        .any(|(key, _)| route_override_keys.contains(&key.as_ref()))
    {
        return DatabaseEndpointSummary::OpaqueConfigured;
    }
    let socket_query_selector = match host_selectors.as_slice() {
        [host] if host.starts_with('/') => true,
        [] => false,
        _ => return DatabaseEndpointSummary::OpaqueConfigured,
    };
    if authority_host.is_some() && !host_selectors.is_empty() {
        return DatabaseEndpointSummary::OpaqueConfigured;
    }
    // The authority host, when this is not a socket route: carrying it
    // here keeps the parsed arm from re-deriving what was already proved.
    let parsed_host = match authority_host {
        Some(host) if is_percent_encoded_socket_host(host) => None,
        Some(host) => Some(host.to_owned()),
        None if socket_query_selector => None,
        None => return DatabaseEndpointSummary::OpaqueConfigured,
    };
    let socket = parsed_host.is_none();
    let safe = [
        "application_name",
        "connect_timeout",
        "sslcert",
        "sslmode",
        "sslrootcert",
        "target_session_attrs",
    ];
    let mut safe_option_keys = Vec::new();
    let mut has_additional_options = false;
    for (key, value) in query_pairs {
        // The query-only socket form's `host` key is the route itself, not an
        // option; it is neither projected nor counted.
        if socket && key == "host" && value.starts_with('/') {
            continue;
        }
        if safe.contains(&key.as_ref()) {
            if !safe_option_keys
                .iter()
                .any(|existing| existing == key.as_ref())
            {
                safe_option_keys.push(key.into_owned());
            }
        } else {
            has_additional_options = true;
        }
    }
    safe_option_keys.sort();
    let scheme = url.scheme().to_owned();
    let username = (!url.username().is_empty()).then(|| url.username().to_owned());
    let database = url
        .path_segments()
        .and_then(|mut segments| segments.next())
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let password = if url.password().is_some() {
        SecretPresence::Configured
    } else {
        SecretPresence::Absent
    };
    if socket {
        return DatabaseEndpointSummary::LocalSocket {
            scheme,
            username,
            database,
            password,
            safe_option_keys,
            has_additional_options,
        };
    }
    let Some(host) = parsed_host else {
        return DatabaseEndpointSummary::OpaqueConfigured;
    };
    DatabaseEndpointSummary::Parsed {
        scheme,
        username,
        host,
        port: url.port(),
        database,
        password,
        safe_option_keys,
        has_additional_options,
    }
}

/// True when a non-special-scheme authority host percent-decodes to a leading
/// `/` — the percent-encoded Unix-socket host form, which `url` deliberately
/// leaves un-decoded.
fn is_percent_encoded_socket_host(host: &str) -> bool {
    host.len() >= 3 && host[..3].eq_ignore_ascii_case("%2f")
}

fn provider_summary(
    config: &TribalConfig,
    name: &ProviderConnectionName,
    connection: &ProviderConnectionConfig,
    receipts: &[ProviderConnectionProbeReceipt],
) -> ProviderConnectionSummary {
    let credential = match connection {
        ProviderConnectionConfig::Ollama { .. } => ProviderCredentialSummary::NotRequired,
        ProviderConnectionConfig::Platform {} => ProviderCredentialSummary::Managed,
        ProviderConnectionConfig::Anthropic { api_key, .. }
        | ProviderConnectionConfig::OpenAi { api_key, .. } => {
            api_key
                .as_ref()
                .map_or(ProviderCredentialSummary::Missing, |key| {
                    ProviderCredentialSummary::Configured {
                        suffix: key
                            .as_str()
                            .chars()
                            .rev()
                            .take(4)
                            .collect::<String>()
                            .chars()
                            .rev()
                            .collect(),
                    }
                })
        }
    };
    ProviderConnectionSummary {
        name: name.clone(),
        provider: connection.provider(),
        endpoint: connection
            .base_url()
            .map_or(ProviderEndpointSummary::Managed, |value| {
                ProviderEndpointSummary::Url {
                    value: value.to_owned(),
                }
            }),
        credential,
        referenced_by: configured_uses(config, name),
        health: receipts
            .iter()
            .find(|receipt| {
                receipt.connection == *name && receipt.provider == connection.provider()
            })
            .cloned(),
    }
}

fn configured_uses(
    config: &TribalConfig,
    name: &ProviderConnectionName,
) -> Vec<ProviderConnectionUse> {
    let mut uses = Vec::new();
    for (stage, connection) in [
        (
            InferenceStage::Extraction,
            &config.inference.extraction.connection,
        ),
        (InferenceStage::Triage, &config.inference.triage.connection),
        (
            InferenceStage::Relation,
            &config.inference.relation.connection,
        ),
    ] {
        if connection == name {
            uses.push(ProviderConnectionUse::Inference { stage });
        }
    }
    if config.init.embedding.connection == *name {
        uses.push(ProviderConnectionUse::GenesisEmbedding);
    }
    uses
}

fn provider_capability(provider: ProviderKind) -> ProviderConnectionCapability {
    let endpoint = provider
        .default_base_url()
        .map_or(EndpointRequirement::Unavailable, |value| {
            EndpointRequirement::PrimaryWithDefault {
                value: value.to_owned(),
            }
        });
    ProviderConnectionCapability {
        provider,
        credential: if provider.requires_api_key() {
            CredentialRequirement::ApiKey
        } else {
            CredentialRequirement::None
        },
        endpoint,
        availability: if provider == ProviderKind::Platform {
            ProviderConnectionAvailability::Unavailable {
                reason: ProviderConnectionUnavailableReason::PlatformSignInUnavailable,
            }
        } else {
            ProviderConnectionAvailability::Available
        },
    }
}

const fn provider_order(provider: ProviderKind) -> u8 {
    match provider {
        ProviderKind::Ollama => 0,
        ProviderKind::Anthropic => 1,
        ProviderKind::OpenAi => 2,
        ProviderKind::Platform => 3,
    }
}

fn input_provider(input: &ProviderConnectionInput) -> ProviderKind {
    match input {
        ProviderConnectionInput::Ollama { .. } => ProviderKind::Ollama,
        ProviderConnectionInput::Anthropic { .. } => ProviderKind::Anthropic,
        ProviderConnectionInput::OpenAi { .. } => ProviderKind::OpenAi,
        ProviderConnectionInput::Platform {} => ProviderKind::Platform,
    }
}

pub(super) fn provider_input(
    input: ProviderConnectionInput,
    existing: Option<&ProviderConnectionConfig>,
) -> Result<ProviderConnectionConfig, ManagementResponseError> {
    match input {
        ProviderConnectionInput::Ollama { base_url } => {
            Ok(ProviderConnectionConfig::Ollama { base_url })
        }
        ProviderConnectionInput::Anthropic {
            base_url,
            credential,
        } => Ok(ProviderConnectionConfig::Anthropic {
            base_url,
            api_key: credential_value(credential, existing, ProviderKind::Anthropic)?,
        }),
        ProviderConnectionInput::OpenAi {
            base_url,
            credential,
        } => Ok(ProviderConnectionConfig::OpenAi {
            base_url,
            api_key: credential_value(credential, existing, ProviderKind::OpenAi)?,
        }),
        ProviderConnectionInput::Platform {} => Ok(ProviderConnectionConfig::Platform {}),
    }
}

fn credential_value(
    mutation: CredentialMutation,
    existing: Option<&ProviderConnectionConfig>,
    provider: ProviderKind,
) -> Result<Option<ApiKey>, ManagementResponseError> {
    match mutation {
        CredentialMutation::Replace { value } => value
            .expose_secret()
            .parse()
            .map(Some)
            .map_err(|_| invalid_contract()),
        CredentialMutation::Clear => Ok(None),
        CredentialMutation::Preserve => {
            let Some(existing) = existing else {
                return Err(public_error(
                    "preserving a credential requires an existing connection",
                    ManagementError::ProviderCredentialMutationRefused {
                        reason: CredentialMutationRefusal::PreserveRequiresExistingConnection,
                    },
                ));
            };
            if existing.provider() != provider {
                return Err(public_error(
                    "preserving a credential requires the same provider",
                    ManagementError::ProviderCredentialMutationRefused {
                        reason: CredentialMutationRefusal::PreserveRequiresMatchingProvider,
                    },
                ));
            }
            Ok(existing.api_key().cloned())
        }
    }
}

fn model_entry(
    descriptor: KnownModel,
    config: &TribalConfig,
) -> Result<KnownModelEntry, ManagementResponseError> {
    let endpoint = descriptor.provider.default_base_url();
    Ok(KnownModelEntry {
        id: KnownModelId::parse(descriptor.id).map_err(|_| invalid_contract())?,
        provider: descriptor.provider,
        model: descriptor.model.to_owned(),
        display_name: descriptor.display_name.to_owned(),
        access: if descriptor.provider == ProviderKind::Platform {
            ModelAccess::Platform
        } else {
            ModelAccess::BringYourOwn
        },
        availability: if descriptor.provider == ProviderKind::Platform {
            ModelAvailability::Unavailable {
                reason: ModelUnavailableReason::PlatformEndpointUnavailable,
            }
        } else {
            ModelAvailability::Available
        },
        settings: ModelSettingsCapability {
            credential: if descriptor.provider.requires_api_key() {
                CredentialRequirement::ApiKey
            } else {
                CredentialRequirement::None
            },
            endpoint: endpoint.map_or(EndpointRequirement::Unavailable, |value| {
                EndpointRequirement::AdvancedWithDefault {
                    value: value.to_owned(),
                }
            }),
        },
        connections: config
            .provider_connections
            .iter()
            .filter(|(_, connection)| connection.provider() == descriptor.provider)
            .map(|(name, _)| name.clone())
            .collect(),
    })
}

fn genesis_connection(
    name: &ProviderConnectionName,
    connection: &ProviderConnectionConfig,
) -> GenesisConnectionOption {
    let availability = match connection {
        ProviderConnectionConfig::Anthropic { .. } => GenesisProviderAvailability::Unavailable {
            reason: GenesisUnavailableReason::NoEmbeddingApi,
        },
        ProviderConnectionConfig::Platform {} => GenesisProviderAvailability::Unavailable {
            reason: GenesisUnavailableReason::ManagedGatewayTransport,
        },
        ProviderConnectionConfig::OpenAi { api_key: None, .. } => {
            GenesisProviderAvailability::Unavailable {
                reason: GenesisUnavailableReason::CredentialMissing,
            }
        }
        ProviderConnectionConfig::OpenAi { .. } => GenesisProviderAvailability::Available {
            models: GenesisModelOptions::Catalogue {
                models: vec![
                    embedding_model("text-embedding-3-small", "Text Embedding 3 Small", 1536),
                    embedding_model("text-embedding-3-large", "Text Embedding 3 Large", 3072),
                    EmbeddingModelOption {
                        model: "text-embedding-ada-002".to_owned(),
                        display_name: "Text Embedding Ada 002".to_owned(),
                        dimensions: EmbeddingDimensions::Fixed { value: 1536 },
                    },
                ],
                allow_other: false,
                constraint: GenesisModelConstraint::NonEmptyNoWhitespace,
            },
        },
        ProviderConnectionConfig::Ollama { .. } => GenesisProviderAvailability::Available {
            models: GenesisModelOptions::Catalogue {
                models: vec![EmbeddingModelOption {
                    model: tribal_config::DEFAULT_EMBEDDING_MODEL.to_owned(),
                    display_name: "Nomic Embed Text".to_owned(),
                    dimensions: EmbeddingDimensions::Native {
                        resolved: Some(tribal_config::DEFAULT_EMBEDDING_DIMENSIONS),
                    },
                }],
                allow_other: true,
                constraint: GenesisModelConstraint::NonEmptyNoWhitespace,
            },
        },
    };
    GenesisConnectionOption {
        connection: name.clone(),
        provider: connection.provider(),
        availability,
    }
}

fn embedding_model(model: &str, display_name: &str, maximum: u32) -> EmbeddingModelOption {
    EmbeddingModelOption {
        model: model.to_owned(),
        display_name: display_name.to_owned(),
        dimensions: EmbeddingDimensions::Selectable {
            default: Some(maximum),
            min: 1,
            max: maximum,
        },
    }
}

fn setup_snapshot(
    config: &TribalConfig,
    revision: ConfigRevision,
    lifecycle: &LifecycleSnapshot,
) -> SettingsSetupSnapshot {
    let (blockers, observations) = setup_readiness(lifecycle);
    let database_blocker = blockers.iter().copied().find(|check| {
        matches!(
            check,
            CheckName::DatabaseReachable | CheckName::MigrationsCurrent
        )
    });
    let database_complete = !config.database.url.is_empty() && database_blocker.is_none();

    let referenced = [
        &config.inference.extraction.connection,
        &config.inference.triage.connection,
        &config.inference.relation.connection,
        &config.init.embedding.connection,
    ];
    let missing_connection = referenced
        .iter()
        .copied()
        .find(|name| config.provider_connections.get(name.as_str()).is_none());
    let missing_credential = referenced.iter().copied().find(|name| {
        config
            .provider_connections
            .get(name.as_str())
            .is_some_and(|connection| {
                connection.provider().requires_api_key()
                    && connection
                        .api_key()
                        .is_none_or(|key| key.as_str().is_empty())
            })
    });
    let providers_complete = missing_connection.is_none() && missing_credential.is_none();

    let graph_drift = blockers.contains(&CheckName::EmbeddingProfile);
    let graph_complete = config
        .provider_connections
        .get(config.init.embedding.connection.as_str())
        .is_some()
        && !graph_drift;
    let database_reason = match database_blocker {
        Some(CheckName::MigrationsCurrent) => SettingsSetupReason::MigrationsNotCurrent,
        Some(_) => SettingsSetupReason::DatabaseUnavailable,
        None if config.database.url.is_empty() => SettingsSetupReason::DatabaseUnconfigured,
        None => SettingsSetupReason::DatabaseInvalid,
    };
    let provider_reason = if missing_connection.is_some() {
        SettingsSetupReason::ProviderConnectionMissing
    } else if missing_credential.is_some() {
        SettingsSetupReason::ProviderCredentialMissing
    } else {
        SettingsSetupReason::ProviderConnectionMissing
    };
    let steps = vec![
        setup_step(
            SettingsSetupStepKind::Connection,
            database_complete,
            database_reason,
            None,
        ),
        setup_step(
            SettingsSetupStepKind::ProviderConnections,
            providers_complete,
            provider_reason,
            (!database_complete).then_some(SettingsSetupStepKind::Connection),
        ),
        setup_step(
            SettingsSetupStepKind::ProcessingProfile,
            true,
            SettingsSetupReason::ProcessingProfileInvalid,
            (!providers_complete).then_some(SettingsSetupStepKind::ProviderConnections),
        ),
        setup_step(
            SettingsSetupStepKind::GraphGenesis,
            graph_complete,
            if graph_drift {
                SettingsSetupReason::GraphProfileDrift
            } else {
                SettingsSetupReason::GraphGenesisInvalid
            },
            (!providers_complete).then_some(SettingsSetupStepKind::ProviderConnections),
        ),
    ];
    let focus = blockers
        .first()
        .copied()
        .map(|check| readiness_focus(config, check, observations))
        .or_else(|| {
            steps.iter().find_map(|step| match &step.state {
                SettingsSetupStepState::NeedsAction { .. } => Some(match step.step {
                    SettingsSetupStepKind::Connection => SettingsSetupFocus::Connection,
                    SettingsSetupStepKind::ProviderConnections => {
                        SettingsSetupFocus::ProviderConnection {
                            name: config.init.embedding.connection.clone(),
                        }
                    }
                    SettingsSetupStepKind::ProcessingProfile => {
                        SettingsSetupFocus::ProcessingProfile
                    }
                    SettingsSetupStepKind::GraphGenesis => SettingsSetupFocus::GraphGenesis,
                }),
                SettingsSetupStepState::Complete | SettingsSetupStepState::Blocked { .. } => None,
            })
        });
    SettingsSetupSnapshot {
        revision,
        focus,
        steps,
    }
}

fn setup_readiness(lifecycle: &LifecycleSnapshot) -> (Vec<CheckName>, &[CheckObservation]) {
    match &lifecycle.phase {
        LifecyclePhase::Unconfigured { readiness, .. } => {
            let StartBlockedVerdict::Blocked { first, rest } = &readiness.start;
            let mut blockers = vec![*first];
            blockers.extend(rest.iter().copied());
            (blockers, &readiness.checks)
        }
        LifecyclePhase::Stopped {
            state: StoppedState::Ready { readiness, .. },
        } => (Vec::new(), &readiness.checks),
        LifecyclePhase::Stopped {
            state: StoppedState::ReadinessUnavailable { last_report, .. },
        } => last_report
            .as_ref()
            .map_or((Vec::new(), &[]), readiness_evidence),
        LifecyclePhase::Degraded { reason, .. } => match reason {
            DegradedReason::Readiness { report } => {
                let blockers = start_blockers(&report.start);
                (blockers, &report.checks)
            }
            DegradedReason::RuntimeControlLost { report, .. } => readiness_evidence(report),
        },
        LifecyclePhase::Starting
        | LifecyclePhase::Healthy { .. }
        | LifecyclePhase::VersionMismatch { .. }
        | LifecyclePhase::Stopping { .. }
        | LifecyclePhase::CancellingEarlyChild { .. }
        | LifecyclePhase::RuntimeUnresponsive { .. }
        | LifecyclePhase::ManagerTerminating { .. } => (Vec::new(), &[]),
    }
}

fn readiness_evidence(
    report: &tribal_wire::management::ReadinessReport,
) -> (Vec<CheckName>, &[CheckObservation]) {
    (start_blockers(&report.start), &report.checks)
}

fn start_blockers(verdict: &StartVerdict) -> Vec<CheckName> {
    match verdict {
        StartVerdict::Clear => Vec::new(),
        StartVerdict::Blocked { first, rest } => {
            let mut blockers = vec![*first];
            blockers.extend(rest.iter().copied());
            blockers
        }
    }
}

fn readiness_focus(
    config: &TribalConfig,
    check: CheckName,
    observations: &[CheckObservation],
) -> SettingsSetupFocus {
    match check {
        CheckName::DatabaseReachable | CheckName::MigrationsCurrent => {
            SettingsSetupFocus::Connection
        }
        CheckName::ProviderEmbedding => SettingsSetupFocus::ProviderConnection {
            name: config.init.embedding.connection.clone(),
        },
        CheckName::ProviderExtraction => SettingsSetupFocus::ProviderConnection {
            name: config.inference.extraction.connection.clone(),
        },
        CheckName::ProviderTriage => SettingsSetupFocus::ProviderConnection {
            name: config.inference.triage.connection.clone(),
        },
        CheckName::ProviderRelation => SettingsSetupFocus::ProviderConnection {
            name: config.inference.relation.connection.clone(),
        },
        CheckName::EmbeddingProfile => SettingsSetupFocus::GraphGenesis,
        CheckName::ConfigParse | CheckName::ConfigValidate => {
            let subject = first_subject(check, observations);
            specialised_config_focus(config, check, subject)
        }
        CheckName::ProjectResolution
        | CheckName::ValidTokenExists
        | CheckName::AdvertisedUrlReachable
        | CheckName::BinaryUniqueness => SettingsSetupFocus::Readiness {
            check,
            subject: first_subject(check, observations),
            destination: SettingsFocusDestination::Diagnostics,
        },
    }
}

fn specialised_config_focus(
    config: &TribalConfig,
    check: CheckName,
    subject: Option<CheckSubject>,
) -> SettingsSetupFocus {
    match subject.as_ref().and_then(subject_pane) {
        Some(SettingsPane::Connection) => SettingsSetupFocus::Connection,
        Some(SettingsPane::Providers) => SettingsSetupFocus::ProviderConnection {
            name: subject
                .as_ref()
                .and_then(subject_connection_name)
                .unwrap_or_else(|| config.init.embedding.connection.clone()),
        },
        Some(SettingsPane::Pipeline) => SettingsSetupFocus::ProcessingProfile,
        Some(SettingsPane::Graph) => SettingsSetupFocus::GraphGenesis,
        Some(pane) => SettingsSetupFocus::Readiness {
            check,
            subject,
            destination: SettingsFocusDestination::Pane { pane },
        },
        None => SettingsSetupFocus::Readiness {
            check,
            subject,
            destination: SettingsFocusDestination::Diagnostics,
        },
    }
}

fn subject_connection_name(subject: &CheckSubject) -> Option<ProviderConnectionName> {
    let raw = match subject {
        CheckSubject::Configuration {
            location: ConfigDiagnosticLocation::Field { path },
        } => path
            .as_str()
            .strip_prefix("provider_connections.")?
            .split('.')
            .next()?,
        CheckSubject::Credentials | CheckSubject::Project | CheckSubject::Runtime => return None,
    };
    ProviderConnectionName::parse(raw).ok()
}

fn first_subject(check: CheckName, observations: &[CheckObservation]) -> Option<CheckSubject> {
    observations
        .iter()
        .find(|observation| check_name(&observation.result) == check)
        .and_then(|observation| observation.subjects.first().cloned())
}

fn check_name(result: &CheckResult) -> CheckName {
    match result {
        CheckResult::Pass { name, .. }
        | CheckResult::Warn { name, .. }
        | CheckResult::Fail { name, .. }
        | CheckResult::Skip { name, .. } => *name,
    }
}

fn subject_pane(subject: &CheckSubject) -> Option<SettingsPane> {
    match subject {
        CheckSubject::Configuration {
            location: ConfigDiagnosticLocation::Field { path },
        } => pane_for_path(path.as_str()),
        CheckSubject::Credentials => Some(SettingsPane::Providers),
        CheckSubject::Project | CheckSubject::Runtime => None,
    }
}

fn pane_for_path(path: &str) -> Option<SettingsPane> {
    if path == "database" || path.starts_with("database.") {
        Some(SettingsPane::Connection)
    } else if path == "provider_connections" || path.starts_with("provider_connections.") {
        Some(SettingsPane::Providers)
    } else if path == "inference"
        || path.starts_with("inference.")
        || path == "agents"
        || path.starts_with("agents.")
    {
        Some(SettingsPane::Pipeline)
    } else if path == "init.embedding" || path.starts_with("init.embedding.") {
        Some(SettingsPane::Graph)
    } else if path == "auth"
        || path.starts_with("auth.")
        || path == "oauth"
        || path.starts_with("oauth.")
    {
        Some(SettingsPane::Authentication)
    } else if path == "retrieval" || path.starts_with("retrieval.") {
        Some(SettingsPane::Retrieval)
    } else if path == "prompts" || path.starts_with("prompts.") {
        Some(SettingsPane::Prompts)
    } else if path == "logging" || path.starts_with("logging.") {
        Some(SettingsPane::Logging)
    } else if path == "telemetry" || path.starts_with("telemetry.") {
        Some(SettingsPane::Telemetry)
    } else if path == "server" || path.starts_with("server.") {
        Some(SettingsPane::Server)
    } else if path == "worker" || path.starts_with("worker.") {
        Some(SettingsPane::Worker)
    } else {
        None
    }
}

fn invalid_setup_snapshot(
    revision: ConfigRevision,
    lifecycle: &LifecycleSnapshot,
) -> SettingsSetupSnapshot {
    let (blockers, observations) = setup_readiness(lifecycle);
    let blocker = blockers.first().copied();
    let focus = blocker.map(|check| {
        let subject = first_subject(check, observations);
        match check {
            CheckName::ConfigParse | CheckName::ConfigValidate => {
                invalid_config_focus(check, subject)
            }
            _ => SettingsSetupFocus::Readiness {
                check,
                subject,
                destination: SettingsFocusDestination::Diagnostics,
            },
        }
    });
    let affected = focus.as_ref().and_then(setup_step_for_focus);
    SettingsSetupSnapshot {
        revision,
        focus,
        steps: setup_steps_for_invalid(affected, blocker),
    }
}

fn invalid_config_focus(check: CheckName, subject: Option<CheckSubject>) -> SettingsSetupFocus {
    match subject.as_ref().and_then(subject_pane) {
        Some(SettingsPane::Connection) => SettingsSetupFocus::Connection,
        Some(SettingsPane::Providers) => subject
            .as_ref()
            .and_then(subject_connection_name)
            .map_or_else(
                || SettingsSetupFocus::Readiness {
                    check,
                    subject,
                    destination: SettingsFocusDestination::Pane {
                        pane: SettingsPane::Providers,
                    },
                },
                |name| SettingsSetupFocus::ProviderConnection { name },
            ),
        Some(SettingsPane::Pipeline) => SettingsSetupFocus::ProcessingProfile,
        Some(SettingsPane::Graph) => SettingsSetupFocus::GraphGenesis,
        Some(pane) => SettingsSetupFocus::Readiness {
            check,
            subject,
            destination: SettingsFocusDestination::Pane { pane },
        },
        None => SettingsSetupFocus::Readiness {
            check,
            subject,
            destination: SettingsFocusDestination::Diagnostics,
        },
    }
}

fn setup_step_for_focus(focus: &SettingsSetupFocus) -> Option<SettingsSetupStepKind> {
    match focus {
        SettingsSetupFocus::Connection => Some(SettingsSetupStepKind::Connection),
        SettingsSetupFocus::ProviderConnection { .. } => {
            Some(SettingsSetupStepKind::ProviderConnections)
        }
        SettingsSetupFocus::ProcessingProfile => Some(SettingsSetupStepKind::ProcessingProfile),
        SettingsSetupFocus::GraphGenesis => Some(SettingsSetupStepKind::GraphGenesis),
        SettingsSetupFocus::Readiness {
            destination: SettingsFocusDestination::Pane { pane },
            ..
        } => match pane {
            SettingsPane::Connection => Some(SettingsSetupStepKind::Connection),
            SettingsPane::Providers => Some(SettingsSetupStepKind::ProviderConnections),
            SettingsPane::Pipeline => Some(SettingsSetupStepKind::ProcessingProfile),
            SettingsPane::Graph => Some(SettingsSetupStepKind::GraphGenesis),
            SettingsPane::Authentication
            | SettingsPane::Retrieval
            | SettingsPane::Prompts
            | SettingsPane::Logging
            | SettingsPane::Telemetry
            | SettingsPane::Server
            | SettingsPane::Worker => None,
        },
        SettingsSetupFocus::Readiness {
            destination: SettingsFocusDestination::Diagnostics,
            ..
        } => None,
    }
}

fn setup_steps_for_invalid(
    affected: Option<SettingsSetupStepKind>,
    blocker: Option<CheckName>,
) -> Vec<SettingsSetupStep> {
    let order = [
        SettingsSetupStepKind::Connection,
        SettingsSetupStepKind::ProviderConnections,
        SettingsSetupStepKind::ProcessingProfile,
        SettingsSetupStepKind::GraphGenesis,
    ];
    let affected_index = affected.and_then(|target| order.iter().position(|step| *step == target));
    order
        .into_iter()
        .enumerate()
        .map(|(index, step)| {
            let state = match affected_index {
                None => SettingsSetupStepState::Complete,
                Some(target) if index < target => SettingsSetupStepState::Complete,
                Some(target) if index > target => {
                    SettingsSetupStepState::Blocked { by: order[target] }
                }
                Some(_) => SettingsSetupStepState::NeedsAction {
                    reason: match step {
                        SettingsSetupStepKind::Connection => SettingsSetupReason::DatabaseInvalid,
                        SettingsSetupStepKind::ProviderConnections => {
                            SettingsSetupReason::ReadinessBlocked {
                                check: blocker.unwrap_or(CheckName::ConfigValidate),
                            }
                        }
                        SettingsSetupStepKind::ProcessingProfile => {
                            SettingsSetupReason::ProcessingProfileInvalid
                        }
                        SettingsSetupStepKind::GraphGenesis => {
                            SettingsSetupReason::GraphGenesisInvalid
                        }
                    },
                },
            };
            SettingsSetupStep { step, state }
        })
        .collect()
}

fn setup_step(
    step: SettingsSetupStepKind,
    complete: bool,
    reason: SettingsSetupReason,
    blocked: Option<SettingsSetupStepKind>,
) -> SettingsSetupStep {
    let state = if let Some(by) = blocked {
        SettingsSetupStepState::Blocked { by }
    } else if complete {
        SettingsSetupStepState::Complete
    } else {
        SettingsSetupStepState::NeedsAction { reason }
    };
    SettingsSetupStep { step, state }
}

fn provider_uses(
    config: &TribalConfig,
    name: &ProviderConnectionName,
    active_connection: Option<&ProviderConnectionName>,
) -> Vec<ProviderConnectionUse> {
    let mut uses = configured_uses(config, name);
    if active_connection == Some(name) {
        uses.push(ProviderConnectionUse::ActiveEmbedding);
    }
    uses
}

fn wire_processing(
    profile: ConfigProcessingProfile,
) -> Result<ProcessingProfile, ManagementResponseError> {
    serde_json::from_value(serde_json::to_value(profile).map_err(|_| invalid_contract())?)
        .map_err(|_| invalid_contract())
}

fn wire_custom_processing(
    settings: tribal_config::CustomProcessingSettings,
) -> Result<tribal_wire::management::CustomProcessingSettings, ManagementResponseError> {
    serde_json::from_value(serde_json::to_value(settings).map_err(|_| invalid_contract())?)
        .map_err(|_| invalid_contract())
}

pub(super) fn config_processing(
    profile: ProcessingProfile,
) -> Result<ConfigProcessingProfile, ManagementResponseError> {
    serde_json::from_value(serde_json::to_value(profile).map_err(|_| invalid_contract())?)
        .map_err(|_| invalid_contract())
}

fn validate_candidate(config: &TribalConfig) -> Result<(), ManagementResponseError> {
    tribal_config::validate(config).map_err(|error| {
        let fields = match error {
            tribal_config::ConfigError::ValidationFailed { diagnostics } => diagnostics
                .iter()
                .flat_map(tribal_config::ValidationError::fields)
                .filter_map(|field| ConfigFieldPath::parse(&field.to_string()).ok())
                .collect(),
            tribal_config::ConfigError::Load { .. }
            | tribal_config::ConfigError::Render { .. }
            | tribal_config::ConfigError::RemovedProviderShape { .. } => Vec::new(),
        };
        public_error(
            "configuration candidate is invalid",
            ManagementError::ConfigurationInvalid { fields },
        )
    })
}

fn serialised_change(
    path: &str,
    value: &impl serde::Serialize,
) -> Result<ConfigPatchChange, ManagementResponseError> {
    Ok(ConfigPatchChange {
        key: ConfigFieldPath::parse(path).map_err(|_| invalid_contract())?,
        value: ConfigLiteral::new(serde_json::to_value(value).map_err(|_| invalid_contract())?),
    })
}

fn ensure_revision(
    expected: &ConfigRevision,
    actual: &ConfigRevision,
) -> Result<(), ManagementResponseError> {
    if expected == actual {
        return Ok(());
    }
    Err(public_error(
        "configuration revision changed",
        ManagementError::ConfigConflict {
            expected: expected.clone(),
            actual: actual.clone(),
        },
    ))
}

async fn active_profile(
    session: &DatabaseSession,
) -> Result<Option<tribal_domain::EmbeddingProfile>, ManagementResponseError> {
    session
        .operation()
        .cancel_safe(async {
            let mut transaction = session
                .pool
                .begin()
                .await
                .map_err(|error| profile_sql_error(&error))?;
            set_profile_lock_timeout(&mut transaction).await?;
            PgAdvisoryLockRepository
                .acquire_shared_xact(
                    &mut transaction,
                    advisory_locks::EMBEDDING_PROFILE_AUTHORITY,
                )
                .await
                .map_err(|error| profile_db_error(&error))?;
            let profile = PgEmbeddingProfileRepository
                .find_active(&mut transaction)
                .await
                .map_err(|error| profile_db_error(&error))?;
            transaction
                .commit()
                .await
                .map_err(|error| profile_sql_error(&error))?;
            Ok::<_, ManagementResponseError>(profile)
        })
        .await
        .map_err(operation::public_error)?
}

async fn locked_active_profile(
    session: &DatabaseSession,
) -> Result<
    (
        sqlx::Transaction<'_, sqlx::Postgres>,
        tribal_domain::EmbeddingProfile,
    ),
    ManagementResponseError,
> {
    session
        .operation()
        .cancel_safe(async {
            let mut transaction = session
                .pool
                .begin()
                .await
                .map_err(|error| profile_sql_error(&error))?;
            set_profile_lock_timeout(&mut transaction).await?;
            PgAdvisoryLockRepository
                .acquire_shared_xact(
                    &mut transaction,
                    advisory_locks::EMBEDDING_PROFILE_AUTHORITY,
                )
                .await
                .map_err(|error| profile_db_error(&error))?;
            let profile = PgEmbeddingProfileRepository
                .find_active(&mut transaction)
                .await
                .map_err(|error| profile_db_error(&error))?
                .ok_or_else(|| {
                    public_error(
                        "no active embedding profile is available",
                        ManagementError::GenesisPolicyRefused {
                            reason:
                                tribal_wire::management::GenesisPolicyRefusal::ProfileUnavailable,
                        },
                    )
                })?;
            Ok::<_, ManagementResponseError>((transaction, profile))
        })
        .await
        .map_err(operation::public_error)?
}

async fn locked_optional_active_profile(
    session: &DatabaseSession,
) -> Result<
    (
        sqlx::Transaction<'_, sqlx::Postgres>,
        Option<tribal_domain::EmbeddingProfile>,
    ),
    ManagementResponseError,
> {
    session
        .operation()
        .cancel_safe(async {
            let mut transaction = session
                .pool
                .begin()
                .await
                .map_err(|error| profile_sql_error(&error))?;
            set_profile_lock_timeout(&mut transaction).await?;
            PgAdvisoryLockRepository
                .acquire_shared_xact(
                    &mut transaction,
                    advisory_locks::EMBEDDING_PROFILE_AUTHORITY,
                )
                .await
                .map_err(|error| profile_db_error(&error))?;
            let profile = PgEmbeddingProfileRepository
                .find_active(&mut transaction)
                .await
                .map_err(|error| profile_db_error(&error))?;
            Ok::<_, ManagementResponseError>((transaction, profile))
        })
        .await
        .map_err(operation::public_error)?
}

async fn set_profile_lock_timeout(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), ManagementResponseError> {
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(|error| profile_sql_error(&error))
}

fn profile_summary(profile: &tribal_domain::EmbeddingProfile) -> EmbeddingProfileSummary {
    EmbeddingProfileSummary {
        provider: profile.provider_kind(),
        base_url: profile.normalised_base_url().to_owned(),
        model: profile.model().to_owned(),
        dimensions: profile.dimensions(),
    }
}

fn configured_embedding(
    config: &TribalConfig,
) -> Result<GenesisEmbeddingInput, ManagementResponseError> {
    let embedding = &config.init.embedding;
    let connection = config
        .provider_connections
        .require(&embedding.connection)
        .map_err(|_| invalid_contract())?;
    Ok(GenesisEmbeddingInput {
        connection: embedding.connection.clone(),
        provider: connection.provider(),
        model: embedding.model.clone(),
        dimensions: embedding.dimensions,
    })
}

fn profile_revision(
    profile: &tribal_domain::EmbeddingProfile,
) -> Result<EmbeddingProfileRevision, ManagementResponseError> {
    let identity = serde_json::to_vec(&(
        profile.id().to_string(),
        profile.epoch(),
        profile.provider_kind().as_str(),
        profile.normalised_base_url(),
        profile.model(),
        profile.dimensions(),
        profile.distance_metric().as_str(),
        profile.revision_token(),
        profile.probe_digest(),
    ))
    .map_err(|_| invalid_contract())?;
    let digest = sha2::Sha256::digest(identity);
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
    EmbeddingProfileRevision::parse(&format!("{}{}", EmbeddingProfileRevision::PREFIX, encoded))
        .map_err(|_| invalid_contract())
}

fn genesis_differs(config: &TribalConfig, profile: &EmbeddingProfileSummary) -> bool {
    let embedding = &config.init.embedding;
    let connection = config
        .provider_connections
        .get(embedding.connection.as_str());
    connection.is_none_or(|connection| {
        connection.provider() != profile.provider
            || connection
                .base_url()
                .and_then(|value| normalise_endpoint_url(value).ok())
                .as_deref()
                != Some(profile.base_url.as_str())
    }) || embedding.model != profile.model
        || embedding.dimensions != Some(profile.dimensions)
}

fn profile_db_error(error: &tribal_db::DbError) -> ManagementResponseError {
    let reason = if error.is_lock_not_available() {
        tribal_wire::management::GenesisPolicyRefusal::ProfileAuthorityBusy
    } else {
        tribal_wire::management::GenesisPolicyRefusal::ProfileUnavailable
    };
    public_error(
        "embedding profile is unavailable",
        ManagementError::GenesisPolicyRefused { reason },
    )
}

fn profile_sql_error(error: &sqlx::Error) -> ManagementResponseError {
    let busy = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "55P03");
    public_error(
        "embedding profile is unavailable",
        ManagementError::GenesisPolicyRefused {
            reason: if busy {
                tribal_wire::management::GenesisPolicyRefusal::ProfileAuthorityBusy
            } else {
                tribal_wire::management::GenesisPolicyRefusal::ProfileUnavailable
            },
        },
    )
}

fn invalid_contract() -> ManagementResponseError {
    public_error(
        "management contract invariant failed",
        ManagementError::InternalInvariant,
    )
}

fn public_error(message: &str, error: ManagementError) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_summary_never_projects_password_or_option_values() {
        let summary = database_endpoint(
            "postgres://user:secret@db.example/tribal?sslmode=require&application_name=private",
        );
        let encoded = serde_json::to_string(&summary).expect("summary serialises");
        assert!(!encoded.contains("secret"));
        assert!(!encoded.contains("private"));
        assert!(encoded.contains("sslmode"));
        assert!(encoded.contains("application_name"));
    }

    #[test]
    fn test_percent_encoded_socket_url_projects_local_socket_without_path() {
        let summary = database_endpoint(
            "postgresql://owner:pw@%2Fprivate%2Ftmp%2Ftribal.pg/tribal?application_name=x",
        );
        let DatabaseEndpointSummary::LocalSocket {
            scheme,
            username,
            database,
            password,
            safe_option_keys,
            has_additional_options,
        } = &summary
        else {
            panic!("expected LocalSocket, got {summary:?}");
        };
        assert_eq!(scheme, "postgresql");
        assert_eq!(username.as_deref(), Some("owner"));
        assert_eq!(database.as_deref(), Some("tribal"));
        assert_eq!(*password, SecretPresence::Configured);
        assert_eq!(safe_option_keys, &["application_name"]);
        assert!(!has_additional_options);

        let encoded = serde_json::to_string(&summary).expect("summary serialises");
        assert!(!encoded.contains("%2F") && !encoded.contains("%2f"));
        assert!(!encoded.to_lowercase().contains("tmp"));
        assert!(!encoded.contains("pw"));
    }

    #[test]
    fn test_query_only_socket_url_projects_local_socket_with_uncounted_selector() {
        let summary =
            database_endpoint("postgresql:///tribal?host=/private/tmp/tribal.pg&sslmode=disable");
        let DatabaseEndpointSummary::LocalSocket {
            username,
            database,
            password,
            safe_option_keys,
            has_additional_options,
            ..
        } = &summary
        else {
            panic!("expected LocalSocket, got {summary:?}");
        };
        assert_eq!(*username, None);
        assert_eq!(database.as_deref(), Some("tribal"));
        assert_eq!(*password, SecretPresence::Absent);
        assert_eq!(safe_option_keys, &["sslmode"]);
        assert!(
            !has_additional_options,
            "the intrinsic socket selector must not count as an additional option",
        );

        let encoded = serde_json::to_string(&summary).expect("summary serialises");
        assert!(!encoded.contains("/private"));
    }

    #[test]
    fn test_socket_url_with_unrecognised_option_flags_additional() {
        let summary = database_endpoint("postgresql:///tribal?host=/x/y&statement_timeout=5000");
        let DatabaseEndpointSummary::LocalSocket {
            has_additional_options,
            ..
        } = summary
        else {
            panic!("expected LocalSocket, got {summary:?}");
        };
        assert!(has_additional_options);
    }

    #[test]
    fn test_mixed_authority_and_socket_query_becomes_opaque() {
        assert!(matches!(
            database_endpoint("postgres://db.example/tribal?host=/var/run"),
            DatabaseEndpointSummary::OpaqueConfigured,
        ));
    }

    #[test]
    fn test_authority_overrides_become_opaque() {
        for query in [
            "host=other.example",
            "hostaddr=192.0.2.1",
            "port=6432",
            "dbname=other",
            "user=other",
            "password=secret",
        ] {
            assert_eq!(
                database_endpoint(&format!("postgres://db.example/tribal?{query}")),
                DatabaseEndpointSummary::OpaqueConfigured,
                "{query} changes the effective connection and cannot be projected from authority",
            );
        }
    }

    #[test]
    fn test_socket_route_with_query_credentials_becomes_opaque() {
        assert_eq!(
            database_endpoint("postgresql:///tribal?host=/private/tmp/tribal.pg&password=secret"),
            DatabaseEndpointSummary::OpaqueConfigured,
        );
    }

    #[test]
    fn test_provider_uses_are_derived_from_canonical_references() {
        let config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        let name = ProviderConnectionName::parse("ollama_default").unwrap();
        assert_eq!(configured_uses(&config, &name).len(), 4);
        assert_eq!(provider_uses(&config, &name, Some(&name)).len(), 5);
    }

    #[test]
    fn test_invalid_setup_focuses_connection_without_resolving_config() {
        let revision = ConfigRevision::from_digest(
            &tribal_wire::management::ConfigDigest::from_bytes(b"invalid"),
        );
        let setup = invalid_setup_snapshot(
            revision,
            &blocked_lifecycle(
                CheckName::ConfigValidate,
                vec![CheckSubject::Configuration {
                    location: ConfigDiagnosticLocation::Field {
                        path: ConfigFieldPath::parse("database.url").unwrap(),
                    },
                }],
            ),
        );
        assert_eq!(setup.focus, Some(SettingsSetupFocus::Connection));
        assert!(matches!(
            setup.steps[0].state,
            SettingsSetupStepState::NeedsAction {
                reason: SettingsSetupReason::DatabaseInvalid
            }
        ));
    }

    #[test]
    fn test_setup_projection_uses_readiness_for_database_state_and_focus() {
        let config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        let revision = ConfigRevision::from_digest(
            &tribal_wire::management::ConfigDigest::from_bytes(b"configured"),
        );
        let lifecycle = blocked_lifecycle(CheckName::MigrationsCurrent, Vec::new());

        let setup = setup_snapshot(&config, revision, &lifecycle);

        assert_eq!(setup.focus, Some(SettingsSetupFocus::Connection));
        assert!(matches!(
            setup.steps[0].state,
            SettingsSetupStepState::NeedsAction {
                reason: SettingsSetupReason::MigrationsNotCurrent
            }
        ));
        assert!(matches!(
            setup.steps[1].state,
            SettingsSetupStepState::Blocked {
                by: SettingsSetupStepKind::Connection
            }
        ));
    }

    #[test]
    fn test_setup_projection_preserves_unknown_readiness_subject() {
        let config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        let revision = ConfigRevision::from_digest(
            &tribal_wire::management::ConfigDigest::from_bytes(b"configured"),
        );
        let subject = CheckSubject::Project;
        let lifecycle = blocked_lifecycle(CheckName::ProjectResolution, vec![subject.clone()]);

        let setup = setup_snapshot(&config, revision, &lifecycle);

        assert_eq!(
            setup.focus,
            Some(SettingsSetupFocus::Readiness {
                check: CheckName::ProjectResolution,
                subject: Some(subject),
                destination: SettingsFocusDestination::Diagnostics,
            })
        );
    }

    #[test]
    fn test_provider_health_focus_does_not_make_configuration_incomplete() {
        let config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        let revision = ConfigRevision::from_digest(
            &tribal_wire::management::ConfigDigest::from_bytes(b"configured"),
        );
        let lifecycle = blocked_lifecycle(CheckName::ProviderExtraction, Vec::new());

        let setup = setup_snapshot(&config, revision, &lifecycle);

        assert_eq!(
            setup.focus,
            Some(SettingsSetupFocus::ProviderConnection {
                name: config.inference.extraction.connection.clone(),
            })
        );
        assert_eq!(setup.steps[1].state, SettingsSetupStepState::Complete);
    }

    #[test]
    fn test_invalid_provider_document_focuses_its_named_connection() {
        let revision = ConfigRevision::from_digest(
            &tribal_wire::management::ConfigDigest::from_bytes(b"invalid"),
        );
        let lifecycle = blocked_lifecycle(
            CheckName::ConfigValidate,
            vec![CheckSubject::Configuration {
                location: ConfigDiagnosticLocation::Field {
                    path: ConfigFieldPath::parse("provider_connections.ollama_default.base_url")
                        .unwrap(),
                },
            }],
        );

        let setup = invalid_setup_snapshot(revision, &lifecycle);

        assert_eq!(
            setup.focus,
            Some(SettingsSetupFocus::ProviderConnection {
                name: ProviderConnectionName::parse("ollama_default").unwrap(),
            })
        );
        assert!(matches!(
            setup.steps[1].state,
            SettingsSetupStepState::NeedsAction {
                reason: SettingsSetupReason::ReadinessBlocked {
                    check: CheckName::ConfigValidate
                }
            }
        ));
    }

    fn blocked_lifecycle(check: CheckName, subjects: Vec<CheckSubject>) -> LifecycleSnapshot {
        LifecycleSnapshot {
            header: tribal_wire::management::LifecycleSnapshotHeader {
                manager_instance_id: "manager".to_owned(),
                revision: 1,
                manager_version: "test".to_owned(),
            },
            phase: LifecyclePhase::Unconfigured {
                readiness: tribal_wire::management::StartBlockedReadinessReport {
                    start: StartBlockedVerdict::Blocked {
                        first: check,
                        rest: Vec::new(),
                    },
                    health: tribal_wire::management::HealthVerdict::NotApplicable,
                    checks: vec![CheckObservation {
                        result: CheckResult::Fail {
                            name: check,
                            detail: "blocked".to_owned(),
                            remediation: "repair the subject".to_owned(),
                        },
                        scope: tribal_wire::management::ReadinessScope::Start,
                        subjects,
                    }],
                },
                focus: None,
                failure: None,
            },
        }
    }

    #[test]
    fn test_processing_wire_round_trip_is_total() {
        let config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        let raw = config.processing_profile();
        let wire = wire_processing(raw.clone()).expect("wire projection succeeds");
        assert_eq!(
            config_processing(wire).expect("config projection succeeds"),
            raw
        );
    }

    #[test]
    fn test_effective_processing_wire_projection_is_exact() {
        let mut config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        config.inference.extraction.temperature = Some(0.2);
        let raw = config.custom_processing_settings();

        let wire = wire_custom_processing(raw.clone()).expect("wire projection succeeds");
        let rebuilt: tribal_config::CustomProcessingSettings =
            serde_json::from_value(serde_json::to_value(wire).expect("wire profile serialises"))
                .expect("config profile deserialises");

        assert_eq!(rebuilt, raw);
    }

    #[test]
    fn test_configured_embedding_projection_names_the_actual_genesis_connection() {
        let config = TribalConfig::minimum_valid("postgres://localhost/tribal");

        let configured = configured_embedding(&config).expect("valid config projects");

        assert_eq!(configured.connection, config.init.embedding.connection);
        assert_eq!(configured.model, config.init.embedding.model);
        assert_eq!(configured.dimensions, config.init.embedding.dimensions);
    }
}
