//! Product-level model and genesis operations over the config transaction.

use std::{collections::HashSet, sync::Mutex};

use base64::Engine as _;
use sha2::Digest as _;
use sqlx::Connection as _;
use tribal_db::{
    AdvisoryLockRepository, EmbeddingProfileRepository, PgAdvisoryLockRepository,
    PgEmbeddingProfileRepository, advisory_locks,
};
use tribal_domain::{ConfigFieldPath, ProviderKind, normalise_endpoint_url};
use tribal_wire::management::{
    ConfigDocument, ConfigLiteral, ConfigPatchChange, ConfigPatchOutcome, ConfigPatchRequest,
    ConfigRevision, ConfigWriteEffect, CredentialCapabilityInvalidReason, CredentialInput,
    CredentialRequirement, CredentialSource, CredentialSourceId, CredentialSourceKind,
    CredentialSources, CredentialSourcesRequest, CredentialUse, CredentialUseCapabilities,
    EmbeddingProfileRevision, EmbeddingProfileSummary, EmbeddingReuseAvailability,
    EmbeddingReuseUnavailableReason, EndpointRequirement, EndpointSelection,
    EndpointTransitionRefusal, GenesisConfigurationRequest, GenesisConvergenceRequest,
    GenesisDimensionsConstraint, GenesisEmbeddingInput, GenesisModelConstraint, GenesisOptions,
    GenesisProviderAvailability, GenesisProviderOption, GenesisUnavailableReason,
    GraphEmbeddingProfile, InferenceStage, InvalidStageSetReason, KnownModelEntry, KnownModelId,
    ManagementError, ManagementResponseError, ModelAccess, ModelAvailability,
    ModelSelectionRequest, ModelSettingsCapability, ModelUnavailableReason, ModelsCatalogue,
    Revisioned,
};

use super::{
    application::DatabaseSession,
    configuration::{CredentialMaterial, management_error},
    worker::ConfigWorkerClient,
};

struct ModelDescriptor {
    id: &'static str,
    provider: ProviderKind,
    model: &'static str,
    display_name: &'static str,
    access: ModelAccess,
}

fn model_descriptors() -> [ModelDescriptor; 4] {
    [
        ModelDescriptor {
            id: "ollama.llama3.2",
            provider: ProviderKind::Ollama,
            model: "llama3.2",
            display_name: "Llama 3.2 (local)",
            access: ModelAccess::BringYourOwn,
        },
        ModelDescriptor {
            id: "anthropic.claude-3-5-haiku",
            provider: ProviderKind::Anthropic,
            model: "claude-3-5-haiku-latest",
            display_name: "Claude 3.5 Haiku",
            access: ModelAccess::BringYourOwn,
        },
        ModelDescriptor {
            id: "openai.gpt-4o-mini",
            provider: ProviderKind::OpenAi,
            model: "gpt-4o-mini",
            display_name: "GPT-4o mini",
            access: ModelAccess::BringYourOwn,
        },
        ModelDescriptor {
            id: "platform.default",
            provider: ProviderKind::Platform,
            model: "default",
            display_name: "Tribal Platform",
            access: ModelAccess::Platform,
        },
    ]
}

/// Factory for product sessions bound to individual management connections.
#[derive(Debug, Clone)]
pub(crate) struct ProductService {
    config: ConfigWorkerClient,
}

/// Product capabilities whose secret sources live for one connection only.
pub(crate) struct ProductSession {
    config: ConfigWorkerClient,
    issuance: Mutex<Option<CredentialIssuance>>,
}

struct CredentialIssuance {
    revision: ConfigRevision,
    use_case: CredentialUse,
    grants: Vec<CredentialGrant>,
}

struct CredentialGrant {
    id: CredentialSourceId,
    material: CredentialMaterial,
}

struct ResolvedCredential {
    secret: zeroize::Zeroizing<String>,
    restore: Option<CredentialIssuance>,
}

impl std::fmt::Debug for ProductSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProductSession { credential_state: <redacted> }")
    }
}

impl ProductService {
    pub(crate) fn new(config: ConfigWorkerClient) -> Self {
        Self { config }
    }

    pub(crate) fn session(&self) -> ProductSession {
        ProductSession {
            config: self.config.clone(),
            issuance: Mutex::new(None),
        }
    }
}

impl ProductSession {
    pub(crate) async fn models_catalogue(
        &self,
    ) -> Result<ModelsCatalogue, ManagementResponseError> {
        let (_, revision) = document(self.config.document().await.map_err(management_error)?)?;
        let models = model_descriptors()
            .into_iter()
            .map(model_entry)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ModelsCatalogue { models, revision })
    }

    pub(crate) async fn credential_sources(
        &self,
        request: CredentialSourcesRequest,
    ) -> Result<CredentialSources, ManagementResponseError> {
        let (values, actual) = document(self.config.document().await.map_err(management_error)?)?;
        if actual != request.expected_revision {
            return Err(config_conflict(request.expected_revision, actual));
        }
        validate_credential_use(&request.use_case)?;
        let materials = self
            .config
            .credential_materials()
            .await
            .map_err(management_error)?;
        let mut grants = Vec::new();
        for material in materials {
            if material_matches(&material, &request.use_case)? {
                grants.push(CredentialGrant {
                    id: generate_source_id()?,
                    material,
                });
            }
        }
        let capabilities = capabilities(&request.use_case, &values)?;
        let sources = grants
            .iter()
            .map(|grant| CredentialSource {
                id: grant.id.clone(),
                kind: grant.material.kind.clone(),
            })
            .collect();
        self.replace_issuance(CredentialIssuance {
            revision: actual.clone(),
            use_case: request.use_case,
            grants,
        })?;
        Ok(CredentialSources {
            sources,
            capabilities,
            revision: actual,
        })
    }

    pub(crate) async fn select_model(
        &self,
        request: ModelSelectionRequest,
    ) -> Result<ConfigPatchOutcome, ManagementResponseError> {
        validate_stages(&request.stages)?;
        let descriptor = descriptor(&request.model)?;
        if descriptor.provider == ProviderKind::Platform {
            return Err(public_error(
                "platform model transport is unavailable",
                ManagementError::ModelUnavailable {
                    reason: ModelUnavailableReason::PlatformEndpointUnavailable,
                },
            ));
        }
        let (values, actual) = document(self.config.document().await.map_err(management_error)?)?;
        if actual != request.expected_revision {
            return Err(config_conflict(request.expected_revision, actual));
        }
        let use_case = CredentialUse::ModelSelection {
            model: request.model.clone(),
            stages: request.stages.clone(),
            endpoint: request.endpoint.clone(),
        };
        let endpoints = selected_endpoints(
            descriptor.provider,
            &request.stages,
            &request.endpoint,
            &values,
        )?;
        let transition = request
            .stages
            .iter()
            .zip(&endpoints)
            .any(|(stage, endpoint)| {
                stage_provider(&values, stage) != Some(descriptor.provider)
                    || stage_endpoint(&values, stage)
                        .or_else(|| descriptor.provider.default_base_url().map(str::to_owned))
                        .is_none_or(|current| endpoints_differ(&current, endpoint))
            });
        if transition && request.credential.is_none() && descriptor.provider.requires_api_key() {
            return Err(public_error(
                "provider or endpoint transition requires a credential",
                ManagementError::EndpointTransitionRefused {
                    reason: EndpointTransitionRefusal::CredentialRequired,
                },
            ));
        }
        let mut credential = self.resolve_credential(request.credential, &use_case, &actual)?;
        let mut changes = Vec::new();
        for (stage, endpoint) in request.stages.iter().zip(&endpoints) {
            let root = stage_root(stage);
            changes.push(change(
                &format!("{root}.provider"),
                serde_json::to_value(descriptor.provider).map_err(|_| invalid_contract())?,
            )?);
            changes.push(change(
                &format!("{root}.model"),
                serde_json::Value::String(descriptor.model.to_owned()),
            )?);
            if !matches!(request.endpoint, EndpointSelection::Preserve) {
                changes.push(change(
                    &format!("{root}.base_url"),
                    serde_json::Value::String(endpoint.clone()),
                )?);
            }
            if let Some(resolved) = &credential {
                changes.push(change(
                    &format!("{root}.api_key"),
                    serde_json::Value::String(resolved.secret.to_string()),
                )?);
            }
        }
        if request.reuse_api_key_for_embedding {
            if descriptor.provider != ProviderKind::OpenAi {
                return Err(public_error(
                    "provider credential cannot back graph embeddings",
                    ManagementError::EmbeddingReuseRefused {
                        reason: EmbeddingReuseUnavailableReason::ProviderUnsupported,
                    },
                ));
            }
            let resolved = credential.as_ref().ok_or_else(|| {
                public_error(
                    "embedding reuse requires an explicit credential",
                    ManagementError::EmbeddingReuseRefused {
                        reason: EmbeddingReuseUnavailableReason::EndpointMismatch,
                    },
                )
            })?;
            let endpoint = one_endpoint(&endpoints)?;
            let connection = reusable_connection(&values, descriptor.provider, &endpoint)?;
            changes.extend(credential_connection_changes(
                &connection,
                descriptor.provider,
                &endpoint,
                &resolved.secret,
            )?);
        }
        let result = self
            .config
            .patch(ConfigPatchRequest {
                changes,
                expected_revision: actual,
            })
            .await
            .map_err(management_error);
        if result.is_err() {
            self.restore(&mut credential)?;
        }
        result
    }

    pub(crate) async fn preflight_model_selection(
        &self,
        expected_revision: &ConfigRevision,
        model: &KnownModelId,
        stages: &[InferenceStage],
        endpoint: &EndpointSelection,
        credential: Option<&CredentialInput>,
        reuse_api_key_for_embedding: bool,
    ) -> Result<Vec<String>, ManagementResponseError> {
        validate_stages(stages)?;
        let descriptor = descriptor(model)?;
        if descriptor.provider == ProviderKind::Platform {
            return Err(public_error(
                "platform model transport is unavailable",
                ManagementError::ModelUnavailable {
                    reason: ModelUnavailableReason::PlatformEndpointUnavailable,
                },
            ));
        }
        let (values, actual) = document(self.config.document().await.map_err(management_error)?)?;
        if &actual != expected_revision {
            return Err(config_conflict(expected_revision.clone(), actual));
        }
        let use_case = CredentialUse::ModelSelection {
            model: model.clone(),
            stages: stages.to_vec(),
            endpoint: endpoint.clone(),
        };
        self.inspect_credential(credential, &use_case, expected_revision)?;
        let endpoints = selected_endpoints(descriptor.provider, stages, endpoint, &values)?;
        let transition = stages.iter().zip(&endpoints).any(|(stage, endpoint)| {
            stage_provider(&values, stage) != Some(descriptor.provider)
                || stage_endpoint(&values, stage)
                    .or_else(|| descriptor.provider.default_base_url().map(str::to_owned))
                    .is_none_or(|current| endpoints_differ(&current, endpoint))
        });
        if transition && credential.is_none() && descriptor.provider.requires_api_key() {
            return Err(public_error(
                "provider or endpoint transition requires a credential",
                ManagementError::EndpointTransitionRefused {
                    reason: EndpointTransitionRefusal::CredentialRequired,
                },
            ));
        }
        if reuse_api_key_for_embedding {
            if descriptor.provider != ProviderKind::OpenAi {
                return Err(public_error(
                    "provider credential cannot back graph embeddings",
                    ManagementError::EmbeddingReuseRefused {
                        reason: EmbeddingReuseUnavailableReason::ProviderUnsupported,
                    },
                ));
            }
            if credential.is_none() {
                return Err(public_error(
                    "embedding reuse requires an explicit credential",
                    ManagementError::EmbeddingReuseRefused {
                        reason: EmbeddingReuseUnavailableReason::EndpointMismatch,
                    },
                ));
            }
            let endpoint = one_endpoint(&endpoints)?;
            let _ = reusable_connection(&values, descriptor.provider, &endpoint)?;
        }
        Ok(endpoints)
    }

    pub(crate) async fn genesis_options(&self) -> Result<GenesisOptions, ManagementResponseError> {
        let (_, revision) = document(self.config.document().await.map_err(management_error)?)?;
        Ok(GenesisOptions {
            recommended: GenesisEmbeddingInput {
                provider: ProviderKind::Ollama,
                model: tribal_config::DEFAULT_EMBEDDING_MODEL.to_owned(),
                dimensions: None,
                base_url: Some(ProviderKind::DEFAULT_OLLAMA_BASE_URL.to_owned()),
            },
            providers: ProviderKind::ALL
                .into_iter()
                .map(genesis_provider)
                .collect(),
            revision,
        })
    }

    pub(crate) async fn configure_genesis(
        &self,
        request: GenesisConfigurationRequest,
    ) -> Result<ConfigPatchOutcome, ManagementResponseError> {
        validate_genesis(&request.embedding)?;
        let (values, actual) = document(self.config.document().await.map_err(management_error)?)?;
        if actual != request.expected_revision {
            return Err(config_conflict(request.expected_revision, actual));
        }
        let use_case = CredentialUse::Genesis {
            embedding: request.embedding.clone(),
        };
        let endpoint = request
            .embedding
            .base_url
            .clone()
            .or_else(|| {
                request
                    .embedding
                    .provider
                    .default_base_url()
                    .map(str::to_owned)
            })
            .ok_or_else(|| {
                public_error(
                    "provider requires an explicit endpoint",
                    ManagementError::EndpointTransitionRefused {
                        reason: EndpointTransitionRefusal::ProviderHasNoDefault,
                    },
                )
            })?;
        let mut credential = self.resolve_credential(request.credential, &use_case, &actual)?;
        if request.embedding.provider.requires_api_key()
            && credential.is_none()
            && !existing_embedding_credential(&values, request.embedding.provider, &endpoint)?
        {
            return Err(public_error(
                "genesis provider requires a credential",
                ManagementError::EndpointTransitionRefused {
                    reason: EndpointTransitionRefusal::CredentialRequired,
                },
            ));
        }
        let mut changes = vec![
            change(
                "init.embedding.provider",
                serde_json::to_value(request.embedding.provider).map_err(|_| invalid_contract())?,
            )?,
            change(
                "init.embedding.model",
                serde_json::Value::String(request.embedding.model),
            )?,
            change(
                "init.embedding.dimensions",
                request
                    .embedding
                    .dimensions
                    .map_or(serde_json::Value::Null, serde_json::Value::from),
            )?,
            change(
                "init.embedding.base_url",
                serde_json::Value::String(endpoint.clone()),
            )?,
        ];
        if let Some(resolved) = &credential {
            let connection = reusable_connection(&values, request.embedding.provider, &endpoint)?;
            changes.extend(credential_connection_changes(
                &connection,
                request.embedding.provider,
                &endpoint,
                &resolved.secret,
            )?);
        }
        let result = self
            .config
            .patch(ConfigPatchRequest {
                changes,
                expected_revision: actual,
            })
            .await
            .map_err(management_error);
        if result.is_err() {
            self.restore(&mut credential)?;
        }
        result
    }

    pub(crate) async fn preflight_genesis(
        &self,
        expected_revision: &ConfigRevision,
        embedding: &GenesisEmbeddingInput,
        credential: Option<&CredentialInput>,
        credential_will_exist: bool,
    ) -> Result<(), ManagementResponseError> {
        validate_genesis(embedding)?;
        let (values, actual) = document(self.config.document().await.map_err(management_error)?)?;
        if &actual != expected_revision {
            return Err(config_conflict(expected_revision.clone(), actual));
        }
        let use_case = CredentialUse::Genesis {
            embedding: embedding.clone(),
        };
        self.inspect_credential(credential, &use_case, expected_revision)?;
        let endpoint = embedding
            .base_url
            .clone()
            .or_else(|| embedding.provider.default_base_url().map(str::to_owned))
            .ok_or_else(|| {
                public_error(
                    "provider requires an explicit endpoint",
                    ManagementError::EndpointTransitionRefused {
                        reason: EndpointTransitionRefusal::ProviderHasNoDefault,
                    },
                )
            })?;
        if embedding.provider.requires_api_key()
            && credential.is_none()
            && !credential_will_exist
            && !existing_embedding_credential(&values, embedding.provider, &endpoint)?
        {
            return Err(public_error(
                "genesis provider requires a credential",
                ManagementError::EndpointTransitionRefused {
                    reason: EndpointTransitionRefusal::CredentialRequired,
                },
            ));
        }
        if credential.is_some() {
            let _ = reusable_connection(&values, embedding.provider, &endpoint)?;
        }
        Ok(())
    }

    pub(crate) async fn embedding_profile(
        &self,
        session: &DatabaseSession,
    ) -> Result<Revisioned<GraphEmbeddingProfile>, ManagementResponseError> {
        let mut connection = session
            .pool
            .acquire()
            .await
            .map_err(|error| profile_sql_error(&error))?;
        let mut transaction = connection
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
        let Some(profile) = profile else {
            return Ok(session.revisioned(GraphEmbeddingProfile::NoProfile));
        };
        let values =
            serde_json::to_value(session.config.as_ref()).map_err(|_| invalid_contract())?;
        let summary = profile_summary(&profile);
        let genesis_drift = genesis_differs(&values, &summary)
            .then(|| "configuration differs from the active embedding profile".to_owned());
        Ok(session.revisioned(GraphEmbeddingProfile::Active {
            profile: summary,
            profile_revision: profile_revision(&profile)?,
            genesis_drift,
        }))
    }

    pub(crate) async fn converge_genesis(
        &self,
        session: &DatabaseSession,
        request: GenesisConvergenceRequest,
    ) -> Result<ConfigPatchOutcome, ManagementResponseError> {
        let mut connection = session
            .pool
            .acquire()
            .await
            .map_err(|error| profile_sql_error(&error))?;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|error| profile_sql_error(&error))?;
        set_profile_lock_timeout(&mut transaction).await?;
        if let Err(error) = PgAdvisoryLockRepository
            .acquire_shared_xact(
                &mut transaction,
                advisory_locks::EMBEDDING_PROFILE_AUTHORITY,
            )
            .await
        {
            let _ = transaction.rollback().await;
            return Err(profile_db_error(&error));
        }
        let profile = match PgEmbeddingProfileRepository
            .find_active(&mut transaction)
            .await
        {
            Ok(Some(profile)) => profile,
            Ok(None) => {
                let _ = transaction.rollback().await;
                return Err(public_error(
                    "no active embedding profile is available",
                    ManagementError::GenesisPolicyRefused {
                        reason: tribal_wire::management::GenesisPolicyRefusal::ProfileUnavailable,
                    },
                ));
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(profile_db_error(&error));
            }
        };
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
        let changes = vec![
            change(
                "init.embedding.provider",
                serde_json::to_value(summary.provider).map_err(|_| invalid_contract())?,
            )?,
            change(
                "init.embedding.base_url",
                serde_json::Value::String(summary.base_url),
            )?,
            change(
                "init.embedding.model",
                serde_json::Value::String(summary.model),
            )?,
            change(
                "init.embedding.dimensions",
                serde_json::Value::from(summary.dimensions),
            )?,
        ];
        let mut outcome = match self
            .config
            .patch(ConfigPatchRequest {
                changes,
                expected_revision: request.expected_revision,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(management_error(error));
            }
        };
        transaction
            .commit()
            .await
            .map_err(|error| profile_sql_error(&error))?;
        for field in &mut outcome.fields {
            if !matches!(field.effect, ConfigWriteEffect::Unchanged) {
                field.effect = ConfigWriteEffect::CommittedNoRuntimeEffect;
            }
        }
        Ok(outcome)
    }

    fn resolve_credential(
        &self,
        credential: Option<CredentialInput>,
        use_case: &CredentialUse,
        revision: &ConfigRevision,
    ) -> Result<Option<ResolvedCredential>, ManagementResponseError> {
        match credential {
            None => Ok(None),
            Some(CredentialInput::Literal { value }) => Ok(Some(ResolvedCredential {
                secret: zeroize::Zeroizing::new(value.expose_secret().to_owned()),
                restore: None,
            })),
            Some(CredentialInput::Source { source }) => {
                let issuance = self.take_issuance()?;
                let Some(issuance) = issuance else {
                    return Err(capability_error(CredentialCapabilityInvalidReason::Unknown));
                };
                if &issuance.revision != revision {
                    self.replace_issuance(issuance)?;
                    return Err(capability_error(
                        CredentialCapabilityInvalidReason::RevisionChanged,
                    ));
                }
                if &issuance.use_case != use_case {
                    self.replace_issuance(issuance)?;
                    return Err(capability_error(
                        CredentialCapabilityInvalidReason::UseMismatch,
                    ));
                }
                let Some(grant) = issuance.grants.iter().find(|grant| grant.id == source) else {
                    self.replace_issuance(issuance)?;
                    return Err(capability_error(
                        CredentialCapabilityInvalidReason::Reissued,
                    ));
                };
                let secret = zeroize::Zeroizing::new(grant.material.secret.to_string());
                Ok(Some(ResolvedCredential {
                    secret,
                    restore: Some(issuance),
                }))
            }
        }
    }

    fn inspect_credential(
        &self,
        credential: Option<&CredentialInput>,
        use_case: &CredentialUse,
        revision: &ConfigRevision,
    ) -> Result<(), ManagementResponseError> {
        let Some(CredentialInput::Source { source }) = credential else {
            return Ok(());
        };
        let state = self.issuance.lock().map_err(|_| capability_state_error())?;
        let Some(issuance) = state.as_ref() else {
            return Err(capability_error(CredentialCapabilityInvalidReason::Unknown));
        };
        if &issuance.revision != revision {
            return Err(capability_error(
                CredentialCapabilityInvalidReason::RevisionChanged,
            ));
        }
        if &issuance.use_case != use_case {
            return Err(capability_error(
                CredentialCapabilityInvalidReason::UseMismatch,
            ));
        }
        if !issuance.grants.iter().any(|grant| &grant.id == source) {
            return Err(capability_error(
                CredentialCapabilityInvalidReason::Reissued,
            ));
        }
        Ok(())
    }

    fn restore(
        &self,
        credential: &mut Option<ResolvedCredential>,
    ) -> Result<(), ManagementResponseError> {
        if let Some(issuance) = credential
            .as_mut()
            .and_then(|resolved| resolved.restore.take())
        {
            self.replace_issuance(issuance)?;
        }
        Ok(())
    }

    fn replace_issuance(
        &self,
        issuance: CredentialIssuance,
    ) -> Result<(), ManagementResponseError> {
        let mut state = self.issuance.lock().map_err(|_| capability_state_error())?;
        *state = Some(issuance);
        Ok(())
    }

    fn take_issuance(&self) -> Result<Option<CredentialIssuance>, ManagementResponseError> {
        self.issuance
            .lock()
            .map_err(|_| capability_state_error())
            .map(|mut state| state.take())
    }
}

fn descriptor(id: &KnownModelId) -> Result<ModelDescriptor, ManagementResponseError> {
    model_descriptors()
        .into_iter()
        .find(|model| model.id == id.as_str())
        .ok_or_else(|| {
            public_error(
                "model is not in the current catalogue",
                ManagementError::UnknownModel { id: id.clone() },
            )
        })
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

fn genesis_differs(values: &serde_json::Value, profile: &EmbeddingProfileSummary) -> bool {
    let provider = values
        .pointer("/init/embedding/provider")
        .cloned()
        .and_then(|value| serde_json::from_value::<ProviderKind>(value).ok());
    let base_url = values
        .pointer("/init/embedding/base_url")
        .and_then(serde_json::Value::as_str);
    let model = values
        .pointer("/init/embedding/model")
        .and_then(serde_json::Value::as_str);
    let dimensions = values
        .pointer("/init/embedding/dimensions")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok());
    provider != Some(profile.provider)
        || base_url.is_none_or(|value| endpoints_differ(value, &profile.base_url))
        || model != Some(profile.model.as_str())
        || dimensions != Some(profile.dimensions)
}

fn profile_db_error(error: &tribal_db::DbError) -> ManagementResponseError {
    if error.is_lock_not_available() {
        public_error(
            "embedding profile authority is busy",
            ManagementError::GenesisPolicyRefused {
                reason: tribal_wire::management::GenesisPolicyRefusal::ProfileAuthorityBusy,
            },
        )
    } else {
        public_error(
            "embedding profile is unavailable",
            ManagementError::GenesisPolicyRefused {
                reason: tribal_wire::management::GenesisPolicyRefusal::ProfileUnavailable,
            },
        )
    }
}

fn profile_sql_error(error: &sqlx::Error) -> ManagementResponseError {
    let busy = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .is_some_and(|code| code == "55P03");
    public_error(
        if busy {
            "embedding profile authority is busy"
        } else {
            "embedding profile is unavailable"
        },
        ManagementError::GenesisPolicyRefused {
            reason: if busy {
                tribal_wire::management::GenesisPolicyRefusal::ProfileAuthorityBusy
            } else {
                tribal_wire::management::GenesisPolicyRefusal::ProfileUnavailable
            },
        },
    )
}

fn model_entry(descriptor: ModelDescriptor) -> Result<KnownModelEntry, ManagementResponseError> {
    let endpoint = descriptor.provider.default_base_url();
    Ok(KnownModelEntry {
        id: KnownModelId::parse(descriptor.id).map_err(|_| invalid_contract())?,
        provider: descriptor.provider,
        model: descriptor.model.to_owned(),
        display_name: descriptor.display_name.to_owned(),
        access: descriptor.access,
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
            api_key_embedding_reuse: descriptor.provider == ProviderKind::OpenAi,
        },
    })
}

fn document(
    document: ConfigDocument,
) -> Result<(serde_json::Value, ConfigRevision), ManagementResponseError> {
    match document {
        ConfigDocument::DurableValid { values, revision } => {
            Ok((values.expose_sensitive().clone(), revision))
        }
        ConfigDocument::DurableInvalid { .. } => Err(public_error(
            "configuration must be repaired before this operation",
            ManagementError::ConfigurationInvalid { fields: Vec::new() },
        )),
        ConfigDocument::UncertainValid { phase, .. }
        | ConfigDocument::UncertainInvalid { phase, .. }
        | ConfigDocument::Unreadable { phase } => Err(public_error(
            "configuration durability is uncertain",
            ManagementError::ConfigPersistenceUnavailable {
                phase,
                observation: tribal_wire::management::ConfigPersistenceObservation::Unreadable,
            },
        )),
    }
}

fn selected_endpoints(
    provider: ProviderKind,
    stages: &[InferenceStage],
    selection: &EndpointSelection,
    values: &serde_json::Value,
) -> Result<Vec<String>, ManagementResponseError> {
    match selection {
        EndpointSelection::Preserve => {
            let mut endpoints = Vec::new();
            for stage in stages {
                if stage_provider(values, stage) != Some(provider) {
                    return Err(public_error(
                        "provider change requires an explicit endpoint transition",
                        ManagementError::EndpointTransitionRefused {
                            reason: EndpointTransitionRefusal::ProviderChangeRequiresEndpoint,
                        },
                    ));
                }
                let endpoint = stage_endpoint(values, stage)
                    .or_else(|| provider.default_base_url().map(str::to_owned))
                    .ok_or_else(|| {
                        public_error(
                            "provider has no current or default endpoint",
                            ManagementError::EndpointTransitionRefused {
                                reason: EndpointTransitionRefusal::ProviderHasNoDefault,
                            },
                        )
                    })?;
                endpoints.push(endpoint);
            }
            Ok(endpoints)
        }
        EndpointSelection::ProviderDefault => provider
            .default_base_url()
            .map(|value| vec![value.to_owned(); stages.len()])
            .ok_or_else(|| {
                public_error(
                    "provider has no default endpoint",
                    ManagementError::EndpointTransitionRefused {
                        reason: EndpointTransitionRefusal::ProviderHasNoDefault,
                    },
                )
            }),
        EndpointSelection::Custom { value } => {
            let value = normalise_endpoint_url(value).map_err(|_| invalid_contract())?;
            Ok(vec![value; stages.len()])
        }
    }
}

fn one_endpoint(endpoints: &[String]) -> Result<String, ManagementResponseError> {
    let mut normalised = endpoints
        .iter()
        .map(|endpoint| normalise_endpoint_url(endpoint).map_err(|_| invalid_contract()));
    let first = normalised
        .next()
        .transpose()?
        .ok_or_else(invalid_contract)?;
    if normalised.any(|endpoint| endpoint.as_deref().ok() != Some(first.as_str())) {
        return Err(public_error(
            "credential fan-out spans multiple endpoints",
            ManagementError::EndpointTransitionRefused {
                reason: EndpointTransitionRefusal::CredentialFanoutHasMultipleEndpoints,
            },
        ));
    }
    Ok(first)
}

fn endpoints_differ(left: &str, right: &str) -> bool {
    match (normalise_endpoint_url(left), normalise_endpoint_url(right)) {
        (Ok(left), Ok(right)) => left != right,
        _ => true,
    }
}

fn material_matches(
    material: &CredentialMaterial,
    use_case: &CredentialUse,
) -> Result<bool, ManagementResponseError> {
    let (provider, endpoints, stages) = match use_case {
        CredentialUse::ModelSelection {
            model,
            stages,
            endpoint,
        } => {
            validate_stages(stages)?;
            let descriptor = descriptor(model)?;
            let endpoints = match endpoint {
                EndpointSelection::Preserve => Vec::new(),
                EndpointSelection::ProviderDefault => descriptor
                    .provider
                    .default_base_url()
                    .map(|value| vec![value.to_owned()])
                    .unwrap_or_default(),
                EndpointSelection::Custom { value } => vec![value.clone()],
            };
            (descriptor.provider, endpoints, Some(stages.as_slice()))
        }
        CredentialUse::Genesis { embedding } => {
            validate_genesis(embedding)?;
            let endpoint = embedding
                .base_url
                .clone()
                .or_else(|| embedding.provider.default_base_url().map(str::to_owned));
            (embedding.provider, endpoint.into_iter().collect(), None)
        }
    };
    if material.provider != provider {
        return Ok(false);
    }
    if let (CredentialSourceKind::InferenceStage { stage }, Some(stages)) = (&material.kind, stages)
        && !stages.contains(stage)
    {
        return Ok(false);
    }
    if endpoints.is_empty() {
        return Ok(true);
    }
    let Some(source_endpoint) = material.base_url.as_deref() else {
        return Ok(false);
    };
    let source = normalise_endpoint_url(source_endpoint).map_err(|_| invalid_contract())?;
    endpoints
        .iter()
        .map(|endpoint| normalise_endpoint_url(endpoint).map_err(|_| invalid_contract()))
        .collect::<Result<Vec<_>, _>>()
        .map(|destinations| destinations.contains(&source))
}

fn capabilities(
    use_case: &CredentialUse,
    values: &serde_json::Value,
) -> Result<CredentialUseCapabilities, ManagementResponseError> {
    match use_case {
        CredentialUse::ModelSelection {
            model, endpoint, ..
        } => {
            let descriptor = descriptor(model)?;
            let embedding_reuse = if descriptor.provider == ProviderKind::OpenAi {
                let endpoint = match endpoint {
                    EndpointSelection::Custom { value } => value.clone(),
                    EndpointSelection::ProviderDefault => descriptor
                        .provider
                        .default_base_url()
                        .map(str::to_owned)
                        .ok_or_else(invalid_contract)?,
                    EndpointSelection::Preserve => {
                        return Ok(CredentialUseCapabilities::ModelSelection {
                            embedding_reuse: EmbeddingReuseAvailability::Unavailable {
                                reason: EmbeddingReuseUnavailableReason::EndpointMismatch,
                            },
                        });
                    }
                };
                let connection = reusable_connection(values, descriptor.provider, &endpoint)?;
                if connection_exists(values, &connection) {
                    EmbeddingReuseAvailability::AvailableExisting { connection }
                } else {
                    EmbeddingReuseAvailability::AvailableCreate { connection }
                }
            } else {
                EmbeddingReuseAvailability::Unavailable {
                    reason: EmbeddingReuseUnavailableReason::ProviderUnsupported,
                }
            };
            Ok(CredentialUseCapabilities::ModelSelection { embedding_reuse })
        }
        CredentialUse::Genesis { .. } => Ok(CredentialUseCapabilities::Genesis),
    }
}

fn reusable_connection(
    values: &serde_json::Value,
    provider: ProviderKind,
    endpoint: &str,
) -> Result<String, ManagementResponseError> {
    let expected = normalise_endpoint_url(endpoint).map_err(|_| invalid_contract())?;
    if let Some(entries) = values
        .get("credentials")
        .and_then(serde_json::Value::as_object)
    {
        for (name, entry) in entries {
            let entry_provider = entry
                .get("provider_kind")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            let entry_endpoint = entry
                .get("base_url")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| normalise_endpoint_url(value).ok());
            if entry_provider == Some(provider) && entry_endpoint.as_deref() == Some(&expected) {
                return Ok(name.clone());
            }
        }
    }
    let default = format!("{}_default", provider.as_str());
    if connection_exists(values, &default) {
        return Err(public_error(
            "default credential connection names another endpoint",
            ManagementError::CredentialConnectionConflict {
                connection: default,
            },
        ));
    }
    Ok(default)
}

fn existing_embedding_credential(
    values: &serde_json::Value,
    provider: ProviderKind,
    endpoint: &str,
) -> Result<bool, ManagementResponseError> {
    let connection = reusable_connection(values, provider, endpoint)?;
    Ok(values
        .get("credentials")
        .and_then(serde_json::Value::as_object)
        .and_then(|entries| entries.get(&connection))
        .and_then(|entry| entry.get("api_key"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|secret| !secret.is_empty()))
}

fn connection_exists(values: &serde_json::Value, name: &str) -> bool {
    values
        .get("credentials")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|entries| entries.contains_key(name))
}

fn credential_connection_changes(
    connection: &str,
    provider: ProviderKind,
    endpoint: &str,
    secret: &str,
) -> Result<Vec<ConfigPatchChange>, ManagementResponseError> {
    Ok(vec![
        change(
            &format!("credentials.{connection}.provider_kind"),
            serde_json::to_value(provider).map_err(|_| invalid_contract())?,
        )?,
        change(
            &format!("credentials.{connection}.base_url"),
            serde_json::Value::String(endpoint.to_owned()),
        )?,
        change(
            &format!("credentials.{connection}.api_key"),
            serde_json::Value::String(secret.to_owned()),
        )?,
    ])
}

fn stage_provider(values: &serde_json::Value, stage: &InferenceStage) -> Option<ProviderKind> {
    values
        .pointer(&format!("/inference/{}/provider", stage_name(stage)))
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn stage_endpoint(values: &serde_json::Value, stage: &InferenceStage) -> Option<String> {
    values
        .pointer(&format!("/inference/{}/base_url", stage_name(stage)))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn stage_name(stage: &InferenceStage) -> &'static str {
    match stage {
        InferenceStage::Extraction => "extraction",
        InferenceStage::Triage => "triage",
        InferenceStage::Relation => "relation",
    }
}

fn stage_root(stage: &InferenceStage) -> &'static str {
    match stage {
        InferenceStage::Extraction => "inference.extraction",
        InferenceStage::Triage => "inference.triage",
        InferenceStage::Relation => "inference.relation",
    }
}

fn validate_credential_use(use_case: &CredentialUse) -> Result<(), ManagementResponseError> {
    match use_case {
        CredentialUse::ModelSelection { stages, model, .. } => {
            validate_stages(stages)?;
            let _ = descriptor(model)?;
            Ok(())
        }
        CredentialUse::Genesis { embedding } => validate_genesis(embedding),
    }
}

fn validate_stages(stages: &[InferenceStage]) -> Result<(), ManagementResponseError> {
    if stages.is_empty() {
        return Err(public_error(
            "at least one inference stage is required",
            ManagementError::InvalidStageSet {
                reason: InvalidStageSetReason::Empty,
            },
        ));
    }
    let mut seen = HashSet::new();
    for stage in stages {
        if !seen.insert(stage) {
            return Err(public_error(
                "inference stages must be unique",
                ManagementError::InvalidStageSet {
                    reason: InvalidStageSetReason::Duplicate {
                        stage: stage.clone(),
                    },
                },
            ));
        }
    }
    Ok(())
}

fn validate_genesis(embedding: &GenesisEmbeddingInput) -> Result<(), ManagementResponseError> {
    if !embedding.provider.supports_embedding() || embedding.provider == ProviderKind::Platform {
        return Err(public_error(
            "provider has no embedding API",
            ManagementError::GenesisPolicyRefused {
                reason: tribal_wire::management::GenesisPolicyRefusal::ProviderUnavailable,
            },
        ));
    }
    if embedding.model.is_empty() || embedding.model.chars().any(char::is_whitespace) {
        return Err(public_error(
            "embedding model must be non-empty and contain no whitespace",
            ManagementError::ConfigurationInvalid {
                fields: ConfigFieldPath::parse("init.embedding.model")
                    .ok()
                    .into_iter()
                    .collect(),
            },
        ));
    }
    if embedding.dimensions.is_some_and(|dimensions| {
        !(1..=tribal_domain::MAX_EMBEDDING_DIMENSIONS).contains(&dimensions)
    }) {
        return Err(public_error(
            "embedding dimensions are outside the supported range",
            ManagementError::ConfigurationInvalid {
                fields: ConfigFieldPath::parse("init.embedding.dimensions")
                    .ok()
                    .into_iter()
                    .collect(),
            },
        ));
    }
    Ok(())
}

fn genesis_provider(provider: ProviderKind) -> GenesisProviderOption {
    let availability = if !provider.supports_embedding() {
        GenesisProviderAvailability::Unavailable {
            reason: GenesisUnavailableReason::NoEmbeddingApi,
        }
    } else if provider == ProviderKind::Platform {
        GenesisProviderAvailability::Unavailable {
            reason: GenesisUnavailableReason::ManagedGatewayTransport,
        }
    } else {
        GenesisProviderAvailability::Available {
            credential: if provider.requires_api_key() {
                CredentialRequirement::ApiKey
            } else {
                CredentialRequirement::None
            },
            endpoint: provider.default_base_url().map_or(
                EndpointRequirement::Unavailable,
                |value| EndpointRequirement::AdvancedWithDefault {
                    value: value.to_owned(),
                },
            ),
            model: GenesisModelConstraint::NonEmptyNoWhitespace,
            dimensions: GenesisDimensionsConstraint::OptionalRange {
                min: 1,
                max: tribal_domain::MAX_EMBEDDING_DIMENSIONS,
            },
        }
    };
    GenesisProviderOption {
        provider,
        availability,
    }
}

fn generate_source_id() -> Result<CredentialSourceId, ManagementResponseError> {
    let mut bytes = zeroize::Zeroizing::new([0_u8; 32]);
    getrandom::fill(bytes.as_mut()).map_err(|_| {
        public_error(
            "credential capability entropy is unavailable",
            ManagementError::CredentialSourceUnavailable,
        )
    })?;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes.as_ref());
    CredentialSourceId::parse(&format!("{}{}", CredentialSourceId::PREFIX, encoded))
        .map_err(|_| invalid_contract())
}

fn change(
    path: &str,
    value: serde_json::Value,
) -> Result<ConfigPatchChange, ManagementResponseError> {
    Ok(ConfigPatchChange {
        key: ConfigFieldPath::parse(path).map_err(|_| invalid_contract())?,
        value: ConfigLiteral::new(value),
    })
}

fn config_conflict(expected: ConfigRevision, actual: ConfigRevision) -> ManagementResponseError {
    public_error(
        "configuration revision changed",
        ManagementError::ConfigConflict { expected, actual },
    )
}

fn capability_error(reason: CredentialCapabilityInvalidReason) -> ManagementResponseError {
    public_error(
        "credential capability is invalid",
        ManagementError::CredentialCapabilityInvalid { reason },
    )
}

fn capability_state_error() -> ManagementResponseError {
    capability_error(CredentialCapabilityInvalidReason::Unknown)
}

fn invalid_contract() -> ManagementResponseError {
    public_error(
        "management contract invariant failed",
        ManagementError::ConfigurationInvalid { fields: Vec::new() },
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
    use std::path::{Path, PathBuf};

    use tribal_wire::management::{CredentialUse, GenesisProviderAvailability};

    use super::*;

    fn write_config(path: &Path, config: &tribal_config::TribalConfig) {
        std::fs::write(
            path,
            serde_yaml::to_string(config).expect("config serialises"),
        )
        .expect("config writes");
    }

    fn openai_config() -> tribal_config::TribalConfig {
        let mut config = tribal_config::TribalConfig::minimum_valid(
            "postgres://user:pass@localhost:5432/tribal",
        );
        config.inference.extraction.provider = ProviderKind::OpenAi;
        config.inference.extraction.base_url =
            ProviderKind::OpenAi.default_base_url().map(str::to_owned);
        config.inference.extraction.api_key = Some(
            "sk-product-secret"
                .parse()
                .expect("fixture API key is valid"),
        );
        config
    }

    fn session(path: PathBuf) -> ProductSession {
        let (worker, _terminal) =
            super::super::worker::spawn(super::super::configuration::ConfigAuthority::new(path))
                .expect("config worker starts");
        ProductService::new(worker).session()
    }

    async fn revision(session: &ProductSession) -> ConfigRevision {
        let (_, revision) = document(session.config.document().await.expect("document reads"))
            .expect("document is durable and valid");
        revision
    }

    #[tokio::test]
    async fn test_catalogue_and_genesis_options_are_total_over_provider_kinds() {
        let temp = tempfile::tempdir().expect("temporary product root");
        let path = temp.path().join("tribal.yaml");
        write_config(
            &path,
            &tribal_config::TribalConfig::minimum_valid(
                "postgres://user:pass@localhost:5432/tribal",
            ),
        );
        let session = session(path);
        let catalogue = session.models_catalogue().await.expect("catalogue returns");
        assert_eq!(catalogue.models.len(), 4);
        assert!(catalogue.models.iter().any(|entry| {
            entry.provider == ProviderKind::Platform
                && matches!(entry.availability, ModelAvailability::Unavailable { .. })
        }));

        let options = session.genesis_options().await.expect("options return");
        assert_eq!(options.providers.len(), ProviderKind::ALL.len());
        assert!(options.providers.iter().any(|option| {
            option.provider == ProviderKind::Anthropic
                && matches!(
                    option.availability,
                    GenesisProviderAvailability::Unavailable {
                        reason: GenesisUnavailableReason::NoEmbeddingApi
                    }
                )
        }));
    }

    #[tokio::test]
    async fn test_credential_source_is_use_bound_and_consumed_by_atomic_selection() {
        let temp = tempfile::tempdir().expect("temporary product root");
        let path = temp.path().join("tribal.yaml");
        write_config(&path, &openai_config());
        let session = session(path.clone());
        let expected_revision = revision(&session).await;
        let model = KnownModelId::parse("openai.gpt-4o-mini").expect("model id parses");
        let use_case = CredentialUse::ModelSelection {
            model: model.clone(),
            stages: vec![InferenceStage::Extraction],
            endpoint: EndpointSelection::ProviderDefault,
        };
        let sources = session
            .credential_sources(CredentialSourcesRequest {
                use_case,
                expected_revision: expected_revision.clone(),
            })
            .await
            .expect("credential sources return");
        let source = sources
            .sources
            .first()
            .expect("configured stage source is offered")
            .id
            .clone();
        assert!(!format!("{source:?}").contains(source.as_str()));

        let mismatched_use = session
            .select_model(ModelSelectionRequest {
                model: model.clone(),
                stages: vec![InferenceStage::Triage],
                endpoint: EndpointSelection::ProviderDefault,
                credential: Some(CredentialInput::Source {
                    source: source.clone(),
                }),
                reuse_api_key_for_embedding: false,
                expected_revision: expected_revision.clone(),
            })
            .await
            .expect_err("source cannot cross its issued use");
        assert!(matches!(
            mismatched_use.error,
            ManagementError::CredentialCapabilityInvalid {
                reason: CredentialCapabilityInvalidReason::UseMismatch,
            }
        ));

        let outcome = session
            .select_model(ModelSelectionRequest {
                model: model.clone(),
                stages: vec![InferenceStage::Extraction],
                endpoint: EndpointSelection::ProviderDefault,
                credential: Some(CredentialInput::Source {
                    source: source.clone(),
                }),
                reuse_api_key_for_embedding: false,
                expected_revision,
            })
            .await
            .expect("source-backed selection commits");
        assert!(
            outcome
                .fields
                .windows(2)
                .all(|pair| pair[0].key.as_str() < pair[1].key.as_str())
        );
        let persisted = std::fs::read_to_string(&path).expect("config rereads");
        assert!(persisted.contains("sk-product-secret"));

        let replay = session
            .select_model(ModelSelectionRequest {
                model,
                stages: vec![InferenceStage::Extraction],
                endpoint: EndpointSelection::ProviderDefault,
                credential: Some(CredentialInput::Source { source }),
                reuse_api_key_for_embedding: false,
                expected_revision: outcome.revision,
            })
            .await
            .expect_err("consumed source is refused");
        assert!(matches!(
            replay.error,
            ManagementError::CredentialCapabilityInvalid { .. }
        ));
    }

    #[test]
    fn test_credential_connection_reuse_is_endpoint_exact_and_non_destructive() {
        let matching = serde_json::json!({
            "credentials": {
                "shared_openai": {
                    "provider_kind": "openai",
                    "base_url": "https://api.openai.com/v1/"
                }
            }
        });
        assert_eq!(
            reusable_connection(&matching, ProviderKind::OpenAi, "https://api.openai.com/v1",)
                .expect("normalised endpoint reuses its connection"),
            "shared_openai",
        );

        let occupied_default = serde_json::json!({
            "credentials": {
                "openai_default": {
                    "provider_kind": "openai",
                    "base_url": "https://gateway.example/v1"
                }
            }
        });
        let refusal = reusable_connection(
            &occupied_default,
            ProviderKind::OpenAi,
            "https://api.openai.com/v1",
        )
        .expect_err("an occupied default name is never overwritten");
        assert!(matches!(
            refusal.error,
            ManagementError::CredentialConnectionConflict { connection }
                if connection == "openai_default"
        ));
    }

    #[test]
    fn test_custom_model_endpoints_are_normalised_before_effects() {
        let endpoints = selected_endpoints(
            ProviderKind::OpenAi,
            &[InferenceStage::Extraction],
            &EndpointSelection::Custom {
                value: "https://api.openai.com/v1/".to_owned(),
            },
            &serde_json::json!({}),
        )
        .expect("custom endpoint is valid");
        assert_eq!(endpoints, ["https://api.openai.com:443/v1"]);

        let error = selected_endpoints(
            ProviderKind::OpenAi,
            &[InferenceStage::Extraction],
            &EndpointSelection::Custom {
                value: "not a URL".to_owned(),
            },
            &serde_json::json!({}),
        )
        .expect_err("invalid custom endpoint is refused");
        assert!(matches!(
            error.error,
            ManagementError::ConfigurationInvalid { .. }
        ));
    }
}
