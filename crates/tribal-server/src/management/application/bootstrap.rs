//! Ordered headless bootstrap over manager-owned capabilities.

use std::collections::{HashMap, HashSet};

use tribal_domain::{BearerToken, ProviderKind, normalise_endpoint_url};
use tribal_wire::management::{
    BootstrapGenesisCredential, BootstrapHandoff, BootstrapOutcome, BootstrapPublicCredential,
    BootstrapRequest, BootstrapResult, BootstrapStorage, ConfigFieldPath, ConfigLiteral,
    ConfigPatchChange, ConfigPatchOutcome, ConfigPatchRequest, ConfigRevision, CredentialOrigin,
    DatabaseInitialiseRequest, GenesisConfigurationRequest, InferenceStage, InvalidStageSetReason,
    IssuedBearerToken, ManagementError, ManagementResponseError, McpConfigEntry, McpConfigRequest,
    McpTarget, ModelAvailability, ModelSelectionRequest, ModelUnavailableReason,
    ProjectRegisterRequest, Revisioned,
};

use super::{
    database::DatabaseAccess,
    integration::IntegrationAdministration,
    operation::OperationContext,
    project::ProjectAdministration,
    token::{PreparedBootstrapToken, TokenAdministration, public_error as token_error},
};
use crate::management::{
    lifecycle::LifecycleController,
    product::ProductSession,
    worker::{ConfigWorkerClient, ConfigWorkerRequestError},
};

pub(super) struct BootstrapAdministration<'a> {
    config: ConfigWorkerClient,
    lifecycle: Option<&'a LifecycleController>,
    database: DatabaseAccess,
    projects: ProjectAdministration,
    tokens: TokenAdministration,
    integration: IntegrationAdministration,
    operation: OperationContext,
}

struct BootstrapPreflight {
    reuse_stage: Option<InferenceStage>,
    token: PreparedBootstrapToken,
}

impl<'a> BootstrapAdministration<'a> {
    pub(super) fn new(
        config: &'a ConfigWorkerClient,
        lifecycle: &'a LifecycleController,
        database: DatabaseAccess,
        projects: ProjectAdministration,
        tokens: TokenAdministration,
        integration: IntegrationAdministration,
        operation: OperationContext,
    ) -> Self {
        Self {
            config: config.clone(),
            lifecycle: Some(lifecycle),
            database,
            projects,
            tokens,
            integration,
            operation,
        }
    }

    #[cfg(test)]
    fn without_lifecycle(
        config: ConfigWorkerClient,
        database: DatabaseAccess,
        projects: ProjectAdministration,
        tokens: TokenAdministration,
        integration: IntegrationAdministration,
    ) -> Self {
        Self {
            config,
            lifecycle: None,
            database,
            projects,
            tokens,
            integration,
            operation: OperationContext::new(tokio_util::sync::CancellationToken::new()),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the ordered bootstrap effect chain is kept visible in one function"
    )]
    pub(super) async fn run(
        &self,
        product: &ProductSession,
        request: BootstrapRequest,
    ) -> Result<BootstrapResult, ManagementResponseError> {
        let preflight = self
            .operation
            .cancel_safe(self.preflight(product, &request))
            .await
            .map_err(super::operation::public_error)??;
        let BootstrapRequest {
            expected_revision,
            storage,
            model_selections,
            genesis,
            telemetry,
            project,
            token: _,
            integration,
        } = request;
        let mut revision = expected_revision;

        if let BootstrapStorage::External { database_url } = storage {
            self.checkpoint()?;
            revision = self
                .patch_config(
                    revision,
                    vec![change(
                        "database.url",
                        serde_json::Value::String(database_url.expose_secret().to_owned()),
                    )?],
                )
                .await?;
        }
        for selection in model_selections {
            self.checkpoint()?;
            let reuse = preflight
                .reuse_stage
                .as_ref()
                .is_some_and(|stage| selection.stages.contains(stage));
            let mut outcome = product
                .select_model(
                    &self.operation,
                    ModelSelectionRequest {
                        model: selection.model,
                        stages: selection.stages,
                        endpoint: selection.endpoint,
                        credential: selection.credential,
                        reuse_api_key_for_embedding: reuse,
                        expected_revision: revision,
                    },
                )
                .await?;
            self.project_patch(&mut outcome).await;
            revision = outcome.revision;
        }
        if let Some(genesis) = genesis {
            self.checkpoint()?;
            let credential = match genesis.credential {
                Some(BootstrapGenesisCredential::Explicit { credential }) => Some(credential),
                Some(BootstrapGenesisCredential::ReuseInferenceStage { .. }) | None => None,
            };
            let mut outcome = product
                .configure_genesis(
                    &self.operation,
                    GenesisConfigurationRequest {
                        embedding: genesis.embedding,
                        credential,
                        expected_revision: revision,
                    },
                )
                .await?;
            self.project_patch(&mut outcome).await;
            revision = outcome.revision;
        }
        if let Some(telemetry) = telemetry {
            self.checkpoint()?;
            revision = self
                .patch_config(
                    revision,
                    vec![
                        change("telemetry.enabled", serde_json::Value::Bool(true))?,
                        change(
                            "telemetry.otlp_endpoint",
                            serde_json::Value::String(String::from(telemetry.otlp_endpoint)),
                        )?,
                    ],
                )
                .await?;
        }

        self.checkpoint()?;
        let database = self
            .database
            .initialise(
                &self.operation,
                DatabaseInitialiseRequest {
                    expected_revision: revision,
                },
            )
            .await
            .map_err(super::database_initialise_error)?;
        let database_outcome = database.value;
        revision = database.config_revision.clone();
        let project_outcome = if let Some(project) = project {
            self.checkpoint()?;
            let result = self
                .projects
                .register(
                    &self.operation,
                    ProjectRegisterRequest {
                        expected_revision: revision,
                        project,
                    },
                )
                .await
                .map_err(super::project::public_error)?;
            revision = result.config_revision;
            Some(result.value)
        } else {
            None
        };

        self.checkpoint()?;
        let prepared = self
            .integration
            .prepare(
                &self.operation,
                McpConfigRequest {
                    expected_revision: revision.clone(),
                    target: integration,
                },
            )
            .await
            .map_err(super::integration_error)?;
        self.checkpoint()?;
        let credential = self
            .tokens
            .provision_bootstrap(&self.operation, revision, preflight.token)
            .await
            .map_err(token_error)?;
        if prepared.revision() != &credential.config_revision {
            return Err(config_conflict(
                prepared.revision().clone(),
                credential.config_revision,
            ));
        }
        let receipt = credential.value;
        let bearer = prepared
            .requires_bearer()
            .then(|| receipt.raw.parse::<BearerToken>())
            .transpose()
            .map_err(|_| token_error(super::token::TokenAdministrationError::Issuance))?;
        let integration = prepared
            .render(bearer.as_ref())
            .map_err(super::integration_error)?;
        let handoff = match integration.value {
            McpConfigEntry::Public { document } => BootstrapHandoff::Public {
                credential: match receipt.origin {
                    super::credential::PersistedIssuanceOrigin::Existing => {
                        BootstrapPublicCredential::Existing {
                            summary: receipt.summary,
                        }
                    }
                    super::credential::PersistedIssuanceOrigin::Issued => {
                        BootstrapPublicCredential::Issued {
                            token: IssuedBearerToken::new(receipt.raw),
                            summary: receipt.summary,
                        }
                    }
                },
                integration: document,
            },
            McpConfigEntry::PersistedBearer { document } => BootstrapHandoff::PersistedBearer {
                credential: receipt.summary,
                origin: match receipt.origin {
                    super::credential::PersistedIssuanceOrigin::Existing => {
                        CredentialOrigin::Existing
                    }
                    super::credential::PersistedIssuanceOrigin::Issued => CredentialOrigin::Issued,
                },
                integration: document,
            },
        };
        Ok(Revisioned {
            config_revision: integration.config_revision,
            value: BootstrapOutcome {
                database: database_outcome,
                project: project_outcome,
                handoff,
            },
        })
    }

    fn checkpoint(&self) -> Result<(), ManagementResponseError> {
        self.operation
            .checkpoint()
            .map_err(super::operation::public_error)
    }

    async fn preflight(
        &self,
        product: &ProductSession,
        request: &BootstrapRequest,
    ) -> Result<BootstrapPreflight, ManagementResponseError> {
        let catalogue = product.models_catalogue(&self.operation).await?;
        if catalogue.revision != request.expected_revision {
            return Err(config_conflict(
                request.expected_revision.clone(),
                catalogue.revision,
            ));
        }
        self.preflight_config(request).await?;
        let token = self
            .tokens
            .preflight_bootstrap(
                &self.operation,
                &request.expected_revision,
                request.token.clone(),
            )
            .await
            .map_err(token_error)?;
        if let Some(project) = &request.project {
            ProjectAdministration::preflight(project).map_err(super::project::public_error)?;
        }
        let target = self
            .integration
            .preflight_target(
                &self.operation,
                &request.expected_revision,
                &request.integration,
            )
            .await
            .map_err(super::integration_error)?;
        if request.project.is_some()
            && matches!(
                target,
                McpTarget::Stdio {
                    context: tribal_wire::management::StdioProjectContext::Unscoped
                }
            )
        {
            return Err(super::integration_error(
                super::integration::IntegrationAdministrationError::IncompatibleTarget,
            ));
        }

        let reuse_stage =
            request
                .genesis
                .as_ref()
                .and_then(|genesis| match genesis.credential.as_ref() {
                    Some(BootstrapGenesisCredential::ReuseInferenceStage { stage }) => {
                        Some(stage.clone())
                    }
                    Some(BootstrapGenesisCredential::Explicit { .. }) | None => None,
                });
        let mut seen = HashSet::new();
        let mut endpoints = HashMap::new();
        for selection in &request.model_selections {
            for stage in &selection.stages {
                if !seen.insert(stage.clone()) {
                    return Err(invalid_stage(stage.clone()));
                }
            }
            let entry = catalogue
                .models
                .iter()
                .find(|entry| entry.id == selection.model)
                .ok_or_else(|| {
                    public_error(ManagementError::UnknownModel {
                        id: selection.model.clone(),
                    })
                })?;
            if let ModelAvailability::Unavailable { reason } = &entry.availability {
                return Err(public_error(ManagementError::ModelUnavailable {
                    reason: reason.clone(),
                }));
            }
            let reuse = reuse_stage
                .as_ref()
                .is_some_and(|stage| selection.stages.contains(stage));
            let selected = product
                .preflight_model_selection(
                    &self.operation,
                    &request.expected_revision,
                    selection,
                    reuse,
                )
                .await?;
            for (stage, endpoint) in selection.stages.iter().zip(selected) {
                endpoints.insert(stage.clone(), endpoint);
            }
        }

        if let Some(genesis) = &request.genesis {
            let explicit = match genesis.credential.as_ref() {
                Some(BootstrapGenesisCredential::Explicit { credential }) => Some(credential),
                Some(BootstrapGenesisCredential::ReuseInferenceStage { .. }) | None => None,
            };
            if let Some(stage) = &reuse_stage {
                if genesis.embedding.provider != ProviderKind::OpenAi {
                    return Err(public_error(ManagementError::EmbeddingReuseRefused {
                        reason: tribal_wire::management::EmbeddingReuseUnavailableReason::ProviderUnsupported,
                    }));
                }
                let selected = endpoints.get(stage).ok_or_else(|| {
                    public_error(ManagementError::EmbeddingReuseRefused {
                        reason: tribal_wire::management::EmbeddingReuseUnavailableReason::EndpointMismatch,
                    })
                })?;
                let genesis_endpoint = genesis
                    .embedding
                    .base_url
                    .clone()
                    .or_else(|| genesis.embedding.provider.default_base_url().map(str::to_owned))
                    .ok_or_else(|| {
                        public_error(ManagementError::EmbeddingReuseRefused {
                            reason: tribal_wire::management::EmbeddingReuseUnavailableReason::EndpointMismatch,
                        })
                    })?;
                if normalise_endpoint_url(selected).ok()
                    != normalise_endpoint_url(&genesis_endpoint).ok()
                {
                    return Err(public_error(ManagementError::EmbeddingReuseRefused {
                        reason: tribal_wire::management::EmbeddingReuseUnavailableReason::EndpointMismatch,
                    }));
                }
            }
            product
                .preflight_genesis(
                    &self.operation,
                    &request.expected_revision,
                    &genesis.embedding,
                    explicit,
                    reuse_stage.is_some(),
                )
                .await?;
        }
        Ok(BootstrapPreflight { reuse_stage, token })
    }

    async fn preflight_config(
        &self,
        request: &BootstrapRequest,
    ) -> Result<(), ManagementResponseError> {
        if let BootstrapStorage::External { database_url } = &request.storage {
            self.validate_config(
                "database.url",
                serde_json::Value::String(database_url.expose_secret().to_owned()),
            )
            .await?;
        }
        if let Some(telemetry) = &request.telemetry {
            self.validate_config("telemetry.enabled", serde_json::Value::Bool(true))
                .await?;
            self.validate_config(
                "telemetry.otlp_endpoint",
                serde_json::Value::String(telemetry.otlp_endpoint.as_str().to_owned()),
            )
            .await?;
        }
        Ok(())
    }

    async fn validate_config(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), ManagementResponseError> {
        let violations = self
            .config
            .for_operation(&self.operation)
            .validate(key.to_owned(), value)
            .await
            .map_err(ConfigWorkerRequestError::into_public_error)?;
        if violations.is_empty() {
            Ok(())
        } else {
            Err(ManagementResponseError {
                message: "bootstrap configuration is invalid".to_owned(),
                error: ManagementError::ConfigurationInvalid {
                    fields: violations
                        .into_iter()
                        .filter_map(|violation| ConfigFieldPath::parse(&violation.key).ok())
                        .collect(),
                },
            })
        }
    }

    async fn patch_config(
        &self,
        expected_revision: ConfigRevision,
        changes: Vec<ConfigPatchChange>,
    ) -> Result<ConfigRevision, ManagementResponseError> {
        let mut outcome = self
            .config
            .for_operation(&self.operation)
            .patch(ConfigPatchRequest {
                changes,
                expected_revision,
            })
            .await
            .map_err(ConfigWorkerRequestError::into_public_error)?;
        self.project_patch(&mut outcome).await;
        Ok(outcome.revision)
    }

    async fn project_patch(&self, outcome: &mut ConfigPatchOutcome) {
        let Some(lifecycle) = self.lifecycle else {
            return;
        };
        super::project_patch_effects(lifecycle, outcome, Vec::new()).await;
        if super::patch_requires_lifecycle_update(outcome) {
            lifecycle.config_changed().await;
        }
    }
}

fn change(
    path: &str,
    value: serde_json::Value,
) -> Result<ConfigPatchChange, ManagementResponseError> {
    Ok(ConfigPatchChange {
        key: ConfigFieldPath::parse(path).map_err(|_| {
            public_error(ManagementError::ConfigurationInvalid { fields: Vec::new() })
        })?,
        value: ConfigLiteral::new(value),
    })
}

fn invalid_stage(stage: InferenceStage) -> ManagementResponseError {
    ManagementResponseError {
        message: "bootstrap inference stages must be unique".to_owned(),
        error: ManagementError::InvalidStageSet {
            reason: InvalidStageSetReason::Duplicate { stage },
        },
    }
}

fn config_conflict(expected: ConfigRevision, actual: ConfigRevision) -> ManagementResponseError {
    ManagementResponseError {
        message: "configuration changed before bootstrap".to_owned(),
        error: ManagementError::ConfigConflict { expected, actual },
    }
}

fn public_error(error: ManagementError) -> ManagementResponseError {
    ManagementResponseError {
        message: match &error {
            ManagementError::UnknownModel { .. } => "bootstrap model is not in the catalogue",
            ManagementError::ModelUnavailable {
                reason: ModelUnavailableReason::PlatformEndpointUnavailable,
            } => "bootstrap model is unavailable",
            ManagementError::EmbeddingReuseRefused { .. } => {
                "bootstrap embedding credential reuse is incompatible"
            }
            _ => "bootstrap request was refused",
        }
        .to_owned(),
        error,
    }
}

#[cfg(test)]
mod tests {
    use tribal_domain::full_access_scopes;
    use tribal_wire::management::{
        BootstrapGenesisInput, BootstrapTelemetryInput, BootstrapTokenPolicy, ConfiguredMcpTarget,
        CredentialInput, EndpointSelection, GenesisEmbeddingInput, McpTargetSelection,
        ModelSelectionInput, OtlpEndpoint, SecretLiteral, StdioProjectContext, TransportKind,
    };

    use super::*;
    use crate::management::{
        application::credential::{CredentialCoordinator, CredentialCoordinatorRuntime},
        authority::ConfigAuthorityNamespace,
        configuration::ConfigAuthority,
        product::ProductService,
        worker::{self, ConfigWorkerRuntime},
    };

    struct Harness {
        database: tribal_test_utils::TestDb,
        temp: tempfile::TempDir,
        worker: ConfigWorkerClient,
        worker_runtime: ConfigWorkerRuntime,
        credential_runtime: Option<CredentialCoordinatorRuntime>,
        product: ProductSession,
        administration: BootstrapAdministration<'static>,
        revision: ConfigRevision,
    }

    impl Harness {
        async fn new() -> Self {
            Self::with_transport(TransportKind::Stdio).await
        }

        async fn with_transport(transport: TransportKind) -> Self {
            let database = tribal_test_utils::TestDb::new().await;
            let temp = tempfile::tempdir().expect("temporary bootstrap root");
            let config_path = temp.path().join("tribal.yaml");
            let mut config = tribal_config::TribalConfig::minimum_valid(database.database_url());
            config.server.transport = transport;
            std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
            let (worker, worker_runtime) =
                worker::spawn(ConfigAuthority::new(config_path)).expect("config worker starts");
            let revision = worker.resolved_snapshot().await.unwrap().revision;
            let (credentials, credential_runtime) = CredentialCoordinator::spawn_with_root(
                ConfigAuthorityNamespace::from_test("abcdef0123456789abcdef01"),
                temp.path(),
                tokio_util::sync::CancellationToken::new(),
            );
            let access = DatabaseAccess::new(worker.clone());
            let administration = BootstrapAdministration::without_lifecycle(
                worker.clone(),
                access.clone(),
                ProjectAdministration::new(access.clone()),
                TokenAdministration::new(access.clone(), credentials.clone()),
                IntegrationAdministration::new(worker.clone(), access, credentials),
            );
            let product = ProductService::new(worker.clone()).session();
            Self {
                database,
                temp,
                worker,
                worker_runtime,
                credential_runtime: Some(credential_runtime),
                product,
                administration,
                revision,
            }
        }

        fn request(&self) -> BootstrapRequest {
            BootstrapRequest {
                expected_revision: self.revision.clone(),
                storage: BootstrapStorage::Configured,
                model_selections: Vec::new(),
                genesis: None,
                telemetry: None,
                project: None,
                token: BootstrapTokenPolicy::EnsureLocalCredential {
                    principal: None,
                    ttl_hours: None,
                    scopes: full_access_scopes(),
                },
                integration: McpTargetSelection::Explicit {
                    target: McpTarget::Stdio {
                        context: StdioProjectContext::Unscoped,
                    },
                },
            }
        }

        async fn token_count(&self) -> i64 {
            sqlx::query_scalar("SELECT COUNT(*) FROM auth_tokens")
                .fetch_one(self.database.pool())
                .await
                .unwrap()
        }

        async fn shutdown(self) {
            let Self {
                database,
                temp,
                worker,
                worker_runtime,
                credential_runtime,
                product,
                administration,
                revision: _,
            } = self;
            drop(administration);
            drop(product);
            drop(worker);
            if let Some(credential_runtime) = credential_runtime {
                credential_runtime.join().await.unwrap();
            }
            worker_runtime.join().unwrap();
            drop(temp);
            drop(database);
        }
    }

    fn model(
        id: &str,
        stage: InferenceStage,
        endpoint: EndpointSelection,
        credential: Option<CredentialInput>,
    ) -> ModelSelectionInput {
        ModelSelectionInput {
            model: tribal_wire::management::KnownModelId::parse(id).unwrap(),
            stages: vec![stage],
            endpoint,
            credential,
        }
    }

    #[tokio::test]
    async fn test_ensure_is_safely_rerunnable_and_hands_off_one_public_secret() {
        let harness = Harness::new().await;
        let first = harness
            .administration
            .run(&harness.product, harness.request())
            .await
            .unwrap();
        let first_id = match first.value.handoff {
            BootstrapHandoff::Public {
                credential: BootstrapPublicCredential::Issued { summary, .. },
                ..
            } => summary.id,
            other => panic!("first ensure issues a public credential: {other:?}"),
        };
        assert_eq!(harness.token_count().await, 1);

        let repeated = harness
            .administration
            .run(&harness.product, harness.request())
            .await
            .unwrap();
        match repeated.value.handoff {
            BootstrapHandoff::Public {
                credential: BootstrapPublicCredential::Existing { summary },
                ..
            } => assert_eq!(summary.id, first_id),
            other => panic!("repeated ensure reuses the public credential: {other:?}"),
        }
        assert_eq!(harness.token_count().await, 1);
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_distinct_models_genesis_reuse_and_telemetry_chain_revisions() {
        let harness = Harness::new().await;
        let mut request = harness.request();
        request.model_selections = vec![
            model(
                "ollama.llama3.1-8b",
                InferenceStage::Extraction,
                EndpointSelection::ProviderDefault,
                None,
            ),
            model(
                "openai.gpt-4o-mini",
                InferenceStage::Triage,
                EndpointSelection::Custom {
                    value: "https://api.openai.com/v1".to_owned(),
                },
                Some(CredentialInput::Literal {
                    value: SecretLiteral::try_from("bootstrap-openai-key".to_owned()).unwrap(),
                }),
            ),
        ];
        request.genesis = Some(BootstrapGenesisInput {
            embedding: GenesisEmbeddingInput {
                provider: ProviderKind::OpenAi,
                model: "text-embedding-3-small".to_owned(),
                dimensions: Some(1536),
                base_url: Some("https://api.openai.com/v1".to_owned()),
            },
            credential: Some(BootstrapGenesisCredential::ReuseInferenceStage {
                stage: InferenceStage::Triage,
            }),
        });
        request.telemetry = Some(BootstrapTelemetryInput {
            otlp_endpoint: OtlpEndpoint::try_from("https://collector.example/v1/traces".to_owned())
                .unwrap(),
        });

        let result = harness
            .administration
            .run(&harness.product, request)
            .await
            .unwrap();
        assert_ne!(result.config_revision, harness.revision);
        let snapshot = harness.worker.resolved_snapshot().await.unwrap();
        assert_eq!(snapshot.config.inference.extraction.model, "llama3.1:8b");
        assert_eq!(snapshot.config.inference.triage.model, "gpt-4o-mini");
        assert_eq!(
            snapshot.config.telemetry.otlp_endpoint.as_deref(),
            Some("https://collector.example/v1/traces")
        );
        assert_eq!(
            snapshot.config.init.embedding.model,
            "text-embedding-3-small"
        );
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_structural_refusals_leave_config_and_database_untouched() {
        let harness = Harness::new().await;
        let before_tokens = harness.token_count().await;
        let mut duplicate = harness.request();
        duplicate.model_selections = vec![
            model(
                "ollama.llama3.1-8b",
                InferenceStage::Extraction,
                EndpointSelection::ProviderDefault,
                None,
            ),
            model(
                "ollama.llama3.1-8b",
                InferenceStage::Extraction,
                EndpointSelection::ProviderDefault,
                None,
            ),
        ];
        let error = harness
            .administration
            .run(&harness.product, duplicate)
            .await
            .expect_err("duplicate stage is refused");
        assert!(matches!(
            error.error,
            ManagementError::InvalidStageSet { .. }
        ));

        let mut mismatch = harness.request();
        mismatch.model_selections = vec![model(
            "openai.gpt-4o-mini",
            InferenceStage::Triage,
            EndpointSelection::Custom {
                value: "https://api.openai.com/v1".to_owned(),
            },
            Some(CredentialInput::Literal {
                value: SecretLiteral::try_from("bootstrap-openai-key".to_owned()).unwrap(),
            }),
        )];
        mismatch.genesis = Some(BootstrapGenesisInput {
            embedding: GenesisEmbeddingInput {
                provider: ProviderKind::OpenAi,
                model: "text-embedding-3-small".to_owned(),
                dimensions: Some(1536),
                base_url: Some("https://example.invalid/v1".to_owned()),
            },
            credential: Some(BootstrapGenesisCredential::ReuseInferenceStage {
                stage: InferenceStage::Triage,
            }),
        });
        let error = harness
            .administration
            .run(&harness.product, mismatch)
            .await
            .expect_err("endpoint mismatch is refused");
        assert!(matches!(
            error.error,
            ManagementError::EmbeddingReuseRefused { .. }
        ));

        let mut invalid_token = harness.request();
        invalid_token.telemetry = Some(BootstrapTelemetryInput {
            otlp_endpoint: OtlpEndpoint::try_from("https://collector.example/v1/traces".to_owned())
                .unwrap(),
        });
        invalid_token.token = BootstrapTokenPolicy::Create {
            principal: None,
            ttl_hours: Some(0),
            scopes: full_access_scopes(),
        };
        let error = harness
            .administration
            .run(&harness.product, invalid_token)
            .await
            .expect_err("invalid token policy is refused before effects");
        assert!(matches!(
            error.error,
            ManagementError::Administration {
                failure: tribal_wire::management::AdministrationFailure::TokenIssuanceRefused
            }
        ));
        assert_eq!(harness.token_count().await, before_tokens);
        assert_eq!(
            harness.worker.resolved_snapshot().await.unwrap().revision,
            harness.revision
        );
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_persisted_bearer_handoff_contains_the_secret_once() {
        let harness = Harness::with_transport(TransportKind::Http).await;
        let mut request = harness.request();
        request.integration = McpTargetSelection::Configured {
            policy: ConfiguredMcpTarget::ExportPersistedBearer {
                stdio_context: StdioProjectContext::Unscoped,
            },
        };
        let result = harness
            .administration
            .run(&harness.product, request)
            .await
            .unwrap();
        let BootstrapHandoff::PersistedBearer {
            origin,
            integration,
            ..
        } = result.value.handoff
        else {
            panic!("configured bearer export returns a sensitive handoff")
        };
        assert_eq!(origin, CredentialOrigin::Issued);
        integration.with_document(|document| {
            assert_eq!(document.to_string().matches("Bearer ").count(), 1);
        });
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_coordinator_exit_is_a_typed_final_credential_refusal() {
        let mut harness = Harness::new().await;
        harness
            .credential_runtime
            .take()
            .expect("credential runtime is live")
            .abort()
            .await;
        let error = harness
            .administration
            .run(&harness.product, harness.request())
            .await
            .expect_err("closed credential coordinator refuses bootstrap");
        assert!(matches!(
            error.error,
            ManagementError::Administration {
                failure:
                    tribal_wire::management::AdministrationFailure::PersistedCredentialUnavailable
            }
        ));
        assert_eq!(harness.token_count().await, 0);
        assert_eq!(
            harness.worker.resolved_snapshot().await.unwrap().revision,
            harness.revision
        );
        harness.shutdown().await;
    }
}
