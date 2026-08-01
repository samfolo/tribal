//! Manager-private operator application façade.

use std::collections::BTreeMap;

mod bootstrap;
pub(crate) mod credential;
mod database;
mod integration;
pub(in crate::management) mod operation;
mod pagination;
mod project;
mod reindex;
pub(in crate::management) mod storage_transition;
mod thread;
mod token;

use bootstrap::BootstrapAdministration;
use credential::CredentialCoordinator;
pub(in crate::management) use database::inspect_target_state;
pub(crate) use database::{
    COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
    DatabaseSession,
};
use database::{DATABASE_URL_KEY, DatabaseAccess, DatabaseAccessError, DatabaseInitialiseError};
use integration::IntegrationAdministration;
use operation::OperationContext;
use project::ProjectAdministration;
use reindex::ReindexAdministration;
pub(in crate::management) use storage_transition::{LocalAdmissionGuard, StorageTransitionGate};
use thread::ThreadAdministration;
use token::TokenAdministration;
use tribal_domain::StorageTransitionId;
use tribal_wire::management::{
    AdministrationFailure, AuthenticationSettingsCall, BootstrapRunCall, CheckReportCall,
    ConfigGetAllCall, ConfigGetCall, ConfigPatchCall, ConfigPatchValidation, ConfigPathCall,
    ConfigSchemaCall, ConfigSetCall, ConfigValidatePatchCall, DatabaseConnectionCall,
    DatabaseInitialiseCall, DatabaseInitialiseOutcome, DatabaseInspectCall, DatabaseProbeCall,
    GraphConfigureGenesisCall, GraphConvergeGenesisCall, GraphEmbeddingProfileCall,
    GraphGenesisOptionsCall, IntegrationMcpConfigCall, LogsTailCall, ManagementCall,
    ManagementError, ManagementMethod, ManagementResponseError, ManagerShutdownCall,
    ManagerSnapshot, ManagerSnapshotCall, ModelsCatalogueCall, PatchConfigViolation,
    ProcessingProfileCall, ProcessingProfileSetCall, ProjectListCall, ProjectRegisterCall,
    ProviderConnectionRemoveCall, ProviderConnectionUpsertCall, ProviderConnectionsCall,
    ProviderProbeCall, ReindexCancelCall, ReindexPruneCall, ReindexRunCall,
    RestartOperationInProgress, Revisioned, RuntimeRestartCall, RuntimeRestartResult,
    RuntimeStartCall, RuntimeStartResult, RuntimeStopCall, RuntimeStopResult, ServerStatusCall,
    SettingsResetPreviewCall, StartOperationInProgress, StopOperationInProgress, StorageAbortCall,
    StorageAssessCall, StorageAssessResult, StorageContinueCall, StorageForceStopCall,
    StorageSwitchCall, ThreadsPruneCall, TokenCreateCall, TokenListCall, TokenRevokeAllCall,
    TokenRevokeCall,
};

use super::{
    config_schema,
    configuration::management_error,
    identity::GraphIdentityResolver,
    lifecycle::LifecycleController,
    probe::{ProbeError, ProbeService},
    product::ProductSession,
    readiness,
    worker::{ConfigWorkerClient, ConfigWorkerRequestError},
};

/// One connection's access to the manager-owned application services.
pub(crate) struct ManagementApplication<'a> {
    config: &'a ConfigWorkerClient,
    product: &'a ProductSession,
    probe: &'a ProbeService,
    lifecycle: &'a LifecycleController,
    identity: &'a GraphIdentityResolver,
    database: DatabaseAccess,
    projects: ProjectAdministration,
    tokens: TokenAdministration,
    integration: IntegrationAdministration,
    reindex: ReindexAdministration,
    threads: ThreadAdministration,
    storage: StorageTransitionGate,
    shutdown: tokio_util::sync::CancellationToken,
}

impl<'a> ManagementApplication<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "the connection façade names each shared manager service it borrows"
    )]
    pub(in crate::management) fn new(
        config: &'a ConfigWorkerClient,
        product: &'a ProductSession,
        probe: &'a ProbeService,
        lifecycle: &'a LifecycleController,
        identity: &'a GraphIdentityResolver,
        credentials: CredentialCoordinator,
        storage: StorageTransitionGate,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        let database = DatabaseAccess::new(config.clone());
        Self {
            config,
            product,
            probe,
            lifecycle,
            identity,
            projects: ProjectAdministration::new(database.clone()),
            tokens: TokenAdministration::new(database.clone(), credentials.clone()),
            integration: IntegrationAdministration::new(
                config.clone(),
                database.clone(),
                credentials,
            ),
            reindex: ReindexAdministration::new(database.clone()),
            threads: ThreadAdministration::new(database.clone()),
            storage,
            database,
            shutdown,
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the exhaustive registry dispatch remains explicit in one match"
    )]
    pub(crate) async fn dispatch(
        &self,
        method: ManagementMethod,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, ManagementResponseError> {
        let operation = OperationContext::new(self.shutdown.clone());
        operation.checkpoint().map_err(operation::public_error)?;
        match method {
            ManagementMethod::ManagerSnapshot => {
                let lifecycle = self
                    .lifecycle
                    .snapshot_for(&operation)
                    .await
                    .map_err(operation::public_error)?
                    .ok_or_else(|| internal_error("lifecycle owner is unavailable"))?;
                let settings = self.product.setup_snapshot(&operation, &lifecycle).await?;
                let graph_id = self.identity.published(&settings.revision);
                encode_call::<ManagerSnapshotCall>(Ok(ManagerSnapshot {
                    lifecycle,
                    settings,
                    graph_id,
                }))
            }
            ManagementMethod::RuntimeStart => match self.storage.admit_lifecycle() {
                Err(transition_id) => {
                    encode_lifecycle::<RuntimeStartCall>(Ok(Some(start_refused_by(transition_id))))
                }
                Ok(_admission) => {
                    encode_lifecycle::<RuntimeStartCall>(self.lifecycle.start_for(&operation).await)
                }
            },
            ManagementMethod::RuntimeStop => match self.storage.admit_lifecycle() {
                Err(transition_id) => {
                    encode_lifecycle::<RuntimeStopCall>(Ok(Some(stop_refused_by(transition_id))))
                }
                Ok(_admission) => {
                    encode_lifecycle::<RuntimeStopCall>(self.lifecycle.stop_for(&operation).await)
                }
            },
            ManagementMethod::RuntimeRestart => match self.storage.admit_lifecycle() {
                Err(transition_id) => encode_lifecycle::<RuntimeRestartCall>(Ok(Some(
                    restart_refused_by(transition_id),
                ))),
                Ok(_admission) => encode_lifecycle::<RuntimeRestartCall>(
                    self.lifecycle.restart_for(&operation).await,
                ),
            },
            ManagementMethod::ManagerShutdown => encode_lifecycle::<ManagerShutdownCall>(
                self.lifecycle.shutdown_for(&operation).await,
            ),
            ManagementMethod::ServerStatus => encode_lifecycle::<ServerStatusCall>(
                self.lifecycle.runtime_status_for(&operation).await,
            ),
            ManagementMethod::LogsTail => {
                let request = parse_call::<LogsTailCall>(params)?;
                encode_lifecycle::<LogsTailCall>(
                    self.lifecycle
                        .runtime_logs_tail_for(&operation, request.lines)
                        .await,
                )
            }
            ManagementMethod::TokenList => {
                let request = parse_call::<TokenListCall>(params)?;
                encode_call::<TokenListCall>(
                    self.tokens
                        .list(&operation, request)
                        .await
                        .map_err(token::public_error),
                )
            }
            ManagementMethod::TokenCreate => {
                let request = parse_call::<TokenCreateCall>(params)?;
                let result = match request.expected_runtime.clone() {
                    Some(expected) => {
                        self.tokens
                            .create_bound_to_runtime(self.lifecycle, &operation, request, expected)
                            .await
                    }
                    None => self
                        .tokens
                        .create(&operation, request)
                        .await
                        .map_err(token::public_error),
                };
                encode_call::<TokenCreateCall>(result)
            }
            ManagementMethod::TokenRevoke => {
                let request = parse_call::<TokenRevokeCall>(params)?;
                encode_call::<TokenRevokeCall>(
                    self.tokens
                        .revoke(&operation, request)
                        .await
                        .map_err(token::public_error),
                )
            }
            ManagementMethod::TokenRevokeAll => {
                let request = parse_call::<TokenRevokeAllCall>(params)?;
                encode_call::<TokenRevokeAllCall>(
                    self.tokens
                        .revoke_all(&operation, request)
                        .await
                        .map_err(token::public_error),
                )
            }
            ManagementMethod::CheckReport => {
                encode_call::<CheckReportCall>(self.readiness_report(&operation).await)
            }
            ManagementMethod::DatabaseInitialise => {
                let request = parse_call::<DatabaseInitialiseCall>(params)?;
                match self.storage.admit_migration() {
                    Err(transition_id) => encode_call::<DatabaseInitialiseCall>(Ok(Revisioned {
                        config_revision: request.expected_revision.clone(),
                        value: initialise_refused_by(transition_id),
                    }
                    .into())),
                    Ok(_admission) => {
                        let result = self
                            .database
                            .initialise(&operation, request)
                            .await
                            .map_err(database_initialise_error);
                        if result.is_ok() {
                            self.identity.reprove().await;
                        }
                        encode_call::<DatabaseInitialiseCall>(result)
                    }
                }
            }
            ManagementMethod::ProjectRegister => {
                let request = parse_call::<ProjectRegisterCall>(params)?;
                encode_call::<ProjectRegisterCall>(
                    self.projects
                        .register(&operation, request)
                        .await
                        .map_err(project::public_error),
                )
            }
            ManagementMethod::ProjectList => {
                let request = parse_call::<ProjectListCall>(params)?;
                encode_call::<ProjectListCall>(
                    self.projects
                        .list(&operation, request)
                        .await
                        .map_err(project::public_error),
                )
            }
            ManagementMethod::IntegrationMcpConfig => {
                let request = parse_call::<IntegrationMcpConfigCall>(params)?;
                encode_call::<IntegrationMcpConfigCall>(
                    self.integration
                        .mcp_config(&operation, request)
                        .await
                        .map_err(integration_error),
                )
            }
            ManagementMethod::ReindexRun => {
                let request = parse_call::<ReindexRunCall>(params)?;
                encode_call::<ReindexRunCall>(
                    self.reindex
                        .run(&operation, self.product, request)
                        .await
                        .map_err(reindex_error),
                )
            }
            ManagementMethod::ReindexCancel => {
                let request = parse_call::<ReindexCancelCall>(params)?;
                encode_call::<ReindexCancelCall>(
                    self.reindex
                        .cancel(&operation, request)
                        .await
                        .map_err(reindex_error),
                )
            }
            ManagementMethod::ReindexPrune => {
                let request = parse_call::<ReindexPruneCall>(params)?;
                encode_call::<ReindexPruneCall>(
                    self.reindex
                        .prune(&operation, request)
                        .await
                        .map_err(reindex_error),
                )
            }
            ManagementMethod::ThreadsPrune => {
                let request = parse_call::<ThreadsPruneCall>(params)?;
                encode_call::<ThreadsPruneCall>(
                    self.threads
                        .prune(&operation, request)
                        .await
                        .map_err(thread_error),
                )
            }
            ManagementMethod::BootstrapRun => {
                let request = parse_call::<BootstrapRunCall>(params)?;
                let bootstrap = BootstrapAdministration::new(
                    self.config,
                    self.lifecycle,
                    self.database.clone(),
                    self.projects.clone(),
                    self.tokens.clone(),
                    self.integration.clone(),
                    operation,
                );
                match self.storage.admit_migration() {
                    Err(_) => encode_call::<BootstrapRunCall>(Err(administration_error(
                        "a storage transition holds this graph",
                        AdministrationFailure::GraphTransitionInProgress,
                    ))),
                    Ok(_admission) => {
                        let result = bootstrap.run(self.product, request).await;
                        if result.is_ok() {
                            self.identity.reprove().await;
                        }
                        encode_call::<BootstrapRunCall>(result)
                    }
                }
            }
            ManagementMethod::DatabaseProbe => {
                let request = parse_call::<DatabaseProbeCall>(params)?;
                let receipt = operation
                    .cancel_safe(self.probe.database(request))
                    .await
                    .map_err(operation::public_error)?
                    .map_err(probe_error)?;
                self.refresh_readiness(&operation).await?;
                encode_call::<DatabaseProbeCall>(Ok(receipt))
            }
            ManagementMethod::DatabaseConnection => encode_call::<DatabaseConnectionCall>(
                self.product.database_connection(&operation).await,
            ),
            ManagementMethod::StorageAssess => {
                let request = parse_call::<StorageAssessCall>(params)?;
                encode_call::<StorageAssessCall>(
                    self.storage
                        .assess(&operation, &self.database, Some(&request.expected_revision))
                        .await
                        .map(StorageAssessResult::from)
                        .map_err(database_access_error),
                )
            }
            ManagementMethod::StorageSwitch => {
                let request = parse_call::<StorageSwitchCall>(params)?;
                encode_call::<StorageSwitchCall>(
                    self.storage
                        .switch(&operation, &self.database, self.lifecycle, request)
                        .await
                        .map_err(database_access_error),
                )
            }
            ManagementMethod::StorageContinue => {
                let request = parse_call::<StorageContinueCall>(params)?;
                encode_call::<StorageContinueCall>(
                    self.storage
                        .continue_switch(&operation, self.lifecycle, request)
                        .await
                        .map_err(database_access_error),
                )
            }
            ManagementMethod::StorageForceStop => {
                let request = parse_call::<StorageForceStopCall>(params)?;
                encode_call::<StorageForceStopCall>(
                    self.storage
                        .force_stop(&operation, self.lifecycle, request)
                        .await
                        .map_err(database_access_error),
                )
            }
            ManagementMethod::StorageAbort => {
                let request = parse_call::<StorageAbortCall>(params)?;
                encode_call::<StorageAbortCall>(
                    self.storage
                        .abort(&operation, self.lifecycle, request)
                        .await
                        .map_err(database_access_error),
                )
            }
            ManagementMethod::DatabaseInspect => {
                let request = parse_call::<DatabaseInspectCall>(params)?;
                encode_call::<DatabaseInspectCall>(
                    self.database
                        .inspect(&operation, request)
                        .await
                        .map_err(database_access_error),
                )
            }
            ManagementMethod::AuthenticationSettings => encode_call::<AuthenticationSettingsCall>(
                self.product.authentication_settings(&operation).await,
            ),
            ManagementMethod::ProviderConnections => {
                let receipts = self.probe.provider_receipts().await.map_err(probe_error)?;
                encode_call::<ProviderConnectionsCall>(
                    self.product
                        .provider_connections(&operation, &receipts)
                        .await,
                )
            }
            ManagementMethod::ProviderConnectionUpsert => {
                let mut receipt = self
                    .product
                    .upsert_provider_connection(
                        &operation,
                        parse_call::<ProviderConnectionUpsertCall>(params)?,
                    )
                    .await?;
                project_patch_effects(self.lifecycle, &mut receipt.outcome, Vec::new()).await;
                if patch_requires_lifecycle_update(&receipt.outcome) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<ProviderConnectionUpsertCall>(Ok(receipt))
            }
            ManagementMethod::ProviderConnectionRemove => {
                let request = parse_call::<ProviderConnectionRemoveCall>(params)?;
                let session = self
                    .database
                    .mutation_session(&operation, &request.expected_revision)
                    .await
                    .map_err(database_access_error)?;
                let mut receipt = self
                    .product
                    .remove_provider_connection(&session, request)
                    .await?;
                project_patch_effects(self.lifecycle, &mut receipt.outcome, Vec::new()).await;
                if patch_requires_lifecycle_update(&receipt.outcome) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<ProviderConnectionRemoveCall>(Ok(receipt))
            }
            ManagementMethod::ProviderProbe => {
                let response = operation
                    .cancel_safe(
                        self.probe
                            .provider(parse_call::<ProviderProbeCall>(params)?),
                    )
                    .await
                    .map_err(operation::public_error)?
                    .map_err(probe_error)?;
                self.refresh_readiness(&operation).await?;
                encode_call::<ProviderProbeCall>(Ok(response))
            }
            ManagementMethod::ConfigGetAll => encode_call::<ConfigGetAllCall>(
                self.config
                    .for_operation(&operation)
                    .document()
                    .await
                    .map_err(ConfigWorkerRequestError::into_public_error),
            ),
            ManagementMethod::ConfigPath => encode_call::<ConfigPathCall>(
                self.config
                    .for_operation(&operation)
                    .path()
                    .await
                    .map_err(ConfigWorkerRequestError::into_public_error),
            ),
            ManagementMethod::ConfigSchema => encode_call::<ConfigSchemaCall>(Ok(
                config_schema::project(tribal_config::config_schema())
                    .map_err(|_| internal_error("configuration schema projection failed"))?,
            )),
            ManagementMethod::ConfigGet => encode_call::<ConfigGetCall>(
                self.config
                    .for_operation(&operation)
                    .get(parse_call::<ConfigGetCall>(params)?)
                    .await
                    .map_err(ConfigWorkerRequestError::into_public_error),
            ),
            ManagementMethod::ConfigValidatePatch => {
                let request = parse_call::<ConfigValidatePatchCall>(params)?;
                let (revision, violations) = self
                    .config
                    .for_operation(&operation)
                    .validate_patch(request)
                    .await
                    .map_err(ConfigWorkerRequestError::into_public_error)?;
                let mut grouped = BTreeMap::<String, Vec<_>>::new();
                for violation in violations {
                    if let Ok(field) = tribal_domain::ConfigFieldPath::parse(&violation.key) {
                        let fields = grouped.entry(violation.message).or_default();
                        if !fields.contains(&field) {
                            fields.push(field);
                        }
                    }
                }
                encode_call::<ConfigValidatePatchCall>(Ok(ConfigPatchValidation {
                    revision,
                    violations: grouped
                        .into_iter()
                        .map(|(message, fields)| PatchConfigViolation { fields, message })
                        .collect(),
                }))
            }
            ManagementMethod::ConfigSet => {
                let request = parse_call::<ConfigSetCall>(params)?;
                let _admission = admit_config_write(&self.storage, &[&request.key])?;
                let runtime_changes = vec![tribal_wire::runtime_control::RuntimeConfigChange {
                    key: request.key.clone(),
                    value: tribal_wire::management::ConfigLiteral::new(
                        request.value.expose_sensitive().clone(),
                    ),
                }];
                let mut outcome = self
                    .config
                    .for_operation(&operation)
                    .set(request)
                    .await
                    .map_err(ConfigWorkerRequestError::into_public_error)?;
                project_runtime_effect(
                    self.lifecycle,
                    &mut outcome.effect,
                    outcome.revision.clone(),
                    runtime_changes,
                )
                .await;
                if !matches!(
                    outcome.effect,
                    tribal_wire::management::ConfigWriteEffect::Unchanged
                        | tribal_wire::management::ConfigWriteEffect::AppliedLive
                ) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<ConfigSetCall>(Ok(outcome))
            }
            ManagementMethod::ConfigPatch => {
                let request = parse_call::<ConfigPatchCall>(params)?;
                let keys: Vec<_> = request.changes.iter().map(|change| &change.key).collect();
                let _admission = admit_config_write(&self.storage, &keys)?;
                let runtime_changes = request
                    .changes
                    .iter()
                    .map(|change| tribal_wire::runtime_control::RuntimeConfigChange {
                        key: change.key.clone(),
                        value: tribal_wire::management::ConfigLiteral::new(
                            change.value.expose_sensitive().clone(),
                        ),
                    })
                    .collect();
                let mut outcome = self
                    .config
                    .for_operation(&operation)
                    .patch(request)
                    .await
                    .map_err(ConfigWorkerRequestError::into_public_error)?;
                project_patch_effects(self.lifecycle, &mut outcome, runtime_changes).await;
                if patch_requires_lifecycle_update(&outcome) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<ConfigPatchCall>(Ok(outcome))
            }
            ManagementMethod::ModelsCatalogue => {
                encode_call::<ModelsCatalogueCall>(self.product.models_catalogue(&operation).await)
            }
            ManagementMethod::ProcessingProfile => encode_call::<ProcessingProfileCall>(
                self.product.processing_profile(&operation).await,
            ),
            ManagementMethod::ProcessingProfileSet => {
                let mut outcome = self
                    .product
                    .set_processing_profile(
                        &operation,
                        parse_call::<ProcessingProfileSetCall>(params)?,
                    )
                    .await?;
                project_patch_effects(self.lifecycle, &mut outcome, Vec::new()).await;
                if patch_requires_lifecycle_update(&outcome) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<ProcessingProfileSetCall>(Ok(outcome))
            }
            ManagementMethod::SettingsResetPreview => encode_call::<SettingsResetPreviewCall>(
                self.product
                    .reset_preview(&operation, parse_call::<SettingsResetPreviewCall>(params)?)
                    .await,
            ),
            ManagementMethod::GraphGenesisOptions => encode_call::<GraphGenesisOptionsCall>(
                self.product.genesis_options(&operation).await,
            ),
            ManagementMethod::GraphEmbeddingProfile => {
                let session = self
                    .database
                    .read_session(&operation)
                    .await
                    .map_err(database_access_error)?;
                encode_call::<GraphEmbeddingProfileCall>(
                    self.product.embedding_profile(&session).await,
                )
            }
            ManagementMethod::GraphConfigureGenesis => {
                let mut outcome = self
                    .product
                    .configure_genesis(&operation, parse_call::<GraphConfigureGenesisCall>(params)?)
                    .await?;
                project_patch_effects(self.lifecycle, &mut outcome, Vec::new()).await;
                if patch_requires_lifecycle_update(&outcome) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<GraphConfigureGenesisCall>(Ok(outcome))
            }
            ManagementMethod::GraphConvergeGenesis => {
                let request = parse_call::<GraphConvergeGenesisCall>(params)?;
                let session = self
                    .database
                    .mutation_session(&operation, &request.expected_revision)
                    .await
                    .map_err(database_access_error)?;
                encode_call::<GraphConvergeGenesisCall>(
                    self.product.converge_genesis(&session, request).await,
                )
            }
        }
    }

    async fn refresh_readiness(
        &self,
        operation: &OperationContext,
    ) -> Result<(), ManagementResponseError> {
        let report = self.readiness_report(operation).await?;
        self.lifecycle.update_readiness(report).await;
        Ok(())
    }

    async fn readiness_report(
        &self,
        operation: &OperationContext,
    ) -> Result<tribal_wire::management::ReadinessReport, ManagementResponseError> {
        let runtime_present = self
            .lifecycle
            .snapshot_for(operation)
            .await
            .map_err(operation::public_error)?
            .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
        operation
            .cancel_safe(readiness::automatic(
                self.config,
                self.probe,
                runtime_present,
            ))
            .await
            .map_err(operation::public_error)?
            .map_err(|_| internal_error("readiness observation failed"))
    }
}

fn parse_call<C: ManagementCall>(
    params: Option<serde_json::Value>,
) -> Result<C::Request, ManagementResponseError>
where
    C::Request: serde::de::DeserializeOwned,
{
    serde_json::from_value(params.unwrap_or(serde_json::Value::Null))
        .map_err(|_| invalid_request("management request parameters are invalid"))
}

fn encode_lifecycle<C: ManagementCall>(
    result: Result<Option<C::Response>, operation::OperationError>,
) -> Result<serde_json::Value, ManagementResponseError>
where
    C::Response: serde::Serialize,
{
    let value = result
        .map_err(operation::public_error)?
        .ok_or_else(|| internal_error("lifecycle owner is unavailable"))?;
    serde_json::to_value(value).map_err(|_| internal_error("lifecycle response encoding failed"))
}

fn encode_call<C: ManagementCall>(
    result: Result<C::Response, ManagementResponseError>,
) -> Result<serde_json::Value, ManagementResponseError>
where
    C::Response: serde::Serialize,
{
    let value = result?;
    serde_json::to_value(value).map_err(|_| internal_error("management response encoding failed"))
}

async fn project_runtime_effect(
    lifecycle: &LifecycleController,
    effect: &mut tribal_wire::management::ConfigWriteEffect,
    revision: tribal_wire::management::ConfigRevision,
    changes: Vec<tribal_wire::runtime_control::RuntimeConfigChange>,
) {
    if matches!(
        effect,
        tribal_wire::management::ConfigWriteEffect::OnNextStart
    ) {
        let running = lifecycle
            .snapshot()
            .await
            .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
        if !running {
            return;
        }
        let hot = !changes.is_empty()
            && changes.iter().all(|change| {
                tribal_config::reload_class(change.key.as_str()) == tribal_config::ReloadClass::Hot
            });
        let applied = if hot {
            lifecycle.apply_config(revision, changes).await
        } else {
            None
        };
        *effect = if matches!(
            applied,
            Some(tribal_wire::runtime_control::RuntimeConfigApplyOutcome::Applied)
        ) {
            tribal_wire::management::ConfigWriteEffect::AppliedLive
        } else {
            tribal_wire::management::ConfigWriteEffect::AwaitingRestart
        };
    }
}

async fn project_patch_effects(
    lifecycle: &LifecycleController,
    outcome: &mut tribal_wire::management::ConfigPatchOutcome,
    changes: Vec<tribal_wire::runtime_control::RuntimeConfigChange>,
) {
    let running = lifecycle
        .snapshot()
        .await
        .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
    if !running {
        return;
    }
    let hot = !changes.is_empty()
        && changes.iter().all(|change| {
            tribal_config::reload_class(change.key.as_str()) == tribal_config::ReloadClass::Hot
        });
    let applied = if hot {
        lifecycle
            .apply_config(outcome.revision.clone(), changes)
            .await
    } else {
        None
    };
    for field in &mut outcome.fields {
        if matches!(
            field.effect,
            tribal_wire::management::ConfigWriteEffect::OnNextStart
        ) {
            field.effect = if matches!(
                applied,
                Some(tribal_wire::runtime_control::RuntimeConfigApplyOutcome::Applied)
            ) {
                tribal_wire::management::ConfigWriteEffect::AppliedLive
            } else {
                tribal_wire::management::ConfigWriteEffect::AwaitingRestart
            };
        }
    }
}

fn lifecycle_has_runtime(phase: &tribal_wire::management::LifecyclePhase) -> bool {
    matches!(
        phase,
        tribal_wire::management::LifecyclePhase::Healthy { .. }
            | tribal_wire::management::LifecyclePhase::Degraded { .. }
            | tribal_wire::management::LifecyclePhase::VersionMismatch { .. }
            | tribal_wire::management::LifecyclePhase::Stopping { .. }
            | tribal_wire::management::LifecyclePhase::RuntimeUnresponsive { .. }
    )
}

fn patch_requires_lifecycle_update(outcome: &tribal_wire::management::ConfigPatchOutcome) -> bool {
    outcome.fields.iter().any(|field| {
        !matches!(
            field.effect,
            tribal_wire::management::ConfigWriteEffect::Unchanged
                | tribal_wire::management::ConfigWriteEffect::AppliedLive
        )
    })
}

fn invalid_request(message: &str) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::ConfigurationInvalid { fields: Vec::new() },
    }
}

fn internal_error(message: &str) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::InternalInvariant,
    }
}

fn probe_error(error: ProbeError) -> ManagementResponseError {
    match error {
        ProbeError::RevisionConflict { expected, actual } => ManagementResponseError {
            message: "configuration changed during the probe".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        other => private_failure(
            &other,
            ManagementResponseError {
                message: "external probe is unavailable".to_owned(),
                error: ManagementError::ProbeUnavailable,
            },
        ),
    }
}

fn database_initialise_error(error: DatabaseInitialiseError) -> ManagementResponseError {
    match error {
        DatabaseInitialiseError::Session(DatabaseAccessError::Operation(failure)) => {
            operation::public_error(failure)
        }
        DatabaseInitialiseError::Session(DatabaseAccessError::Configuration(error)) => {
            management_error(error)
        }
        DatabaseInitialiseError::Session(DatabaseAccessError::RevisionConflict {
            expected,
            actual,
        }) => ManagementResponseError {
            message: "configuration changed before database initialisation".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        error @ DatabaseInitialiseError::Migration { .. } => private_administration_error(
            &error,
            "database migration failed",
            AdministrationFailure::DatabaseMigrationFailed,
        ),
        error @ (DatabaseInitialiseError::Session(DatabaseAccessError::Connection { .. })
        | DatabaseInitialiseError::MigrationConnection { .. }
        | DatabaseInitialiseError::Principal { .. }
        | DatabaseInitialiseError::Project { .. }) => private_administration_error(
            &error,
            "database is unavailable",
            AdministrationFailure::DatabaseUnavailable,
        ),
    }
}

fn database_access_error(error: DatabaseAccessError) -> ManagementResponseError {
    match error {
        DatabaseAccessError::Operation(failure) => operation::public_error(failure),
        DatabaseAccessError::Configuration(error) => management_error(error),
        DatabaseAccessError::RevisionConflict { expected, actual } => ManagementResponseError {
            message: "configuration changed before graph administration".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        error @ DatabaseAccessError::Connection { .. } => private_administration_error(
            &error,
            "graph database is unavailable",
            AdministrationFailure::DatabaseUnavailable,
        ),
    }
}

/// The typed answers each barrier-admitted surface gives while a transition
/// owns the graph. The dispatch arms and their tests share these, so a test
/// cannot assert a shape the dispatch does not actually send.
fn start_refused_by(transition_id: StorageTransitionId) -> RuntimeStartResult {
    RuntimeStartResult::OperationInProgress {
        state: StartOperationInProgress::GraphTransition { transition_id },
    }
}

fn stop_refused_by(transition_id: StorageTransitionId) -> RuntimeStopResult {
    RuntimeStopResult::OperationInProgress {
        state: StopOperationInProgress::GraphTransition { transition_id },
    }
}

fn restart_refused_by(transition_id: StorageTransitionId) -> RuntimeRestartResult {
    RuntimeRestartResult::OperationInProgress {
        state: RestartOperationInProgress::GraphTransition { transition_id },
    }
}

fn initialise_refused_by(transition_id: StorageTransitionId) -> DatabaseInitialiseOutcome {
    DatabaseInitialiseOutcome::GraphTransitionInProgress {
        transition_id: Some(transition_id),
    }
}

/// A transition owns the graph and its recovery is bound to the target the
/// configuration named when it admitted, so a write that moves that target is
/// refused until the transition settles.
fn admit_config_write(
    storage: &StorageTransitionGate,
    keys: &[&tribal_domain::ConfigFieldPath],
) -> Result<Option<LocalAdmissionGuard>, ManagementResponseError> {
    if !keys.iter().any(|key| key.as_str() == DATABASE_URL_KEY) {
        return Ok(None);
    }
    storage.admit_migration().map(Some).map_err(|_| {
        administration_error(
            "a storage transition holds this graph",
            AdministrationFailure::GraphTransitionInProgress,
        )
    })
}

pub(super) fn administration_error(
    message: &str,
    failure: AdministrationFailure,
) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::Administration { failure },
    }
}

pub(super) fn private_failure(
    error: &(impl std::error::Error + ?Sized),
    response: ManagementResponseError,
) -> ManagementResponseError {
    tracing::error!(error = %error, "management application request failed");
    response
}

pub(super) fn private_administration_error(
    error: &(impl std::error::Error + ?Sized),
    message: &str,
    failure: AdministrationFailure,
) -> ManagementResponseError {
    private_failure(error, administration_error(message, failure))
}

fn integration_error(
    error: integration::IntegrationAdministrationError,
) -> ManagementResponseError {
    match error {
        integration::IntegrationAdministrationError::Session(DatabaseAccessError::Operation(
            failure,
        ))
        | integration::IntegrationAdministrationError::Credential {
            source: credential::CredentialCoordinatorError::Operation(failure),
        }
        | integration::IntegrationAdministrationError::Configuration(
            ConfigWorkerRequestError::Operation(failure),
        ) => operation::public_error(failure),
        integration::IntegrationAdministrationError::Session(
            DatabaseAccessError::RevisionConflict { expected, actual },
        ) => ManagementResponseError {
            message: "configuration changed before integration rendering".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        integration::IntegrationAdministrationError::Session(
            DatabaseAccessError::Configuration(error),
        )
        | integration::IntegrationAdministrationError::Configuration(
            ConfigWorkerRequestError::Authority(error),
        ) => management_error(error),
        error @ (integration::IntegrationAdministrationError::IncompatibleTarget
        | integration::IntegrationAdministrationError::ProjectSource
        | integration::IntegrationAdministrationError::ProjectNotFound { .. }) => {
            let failure = integration::public_failure(&error);
            administration_error("integration configuration could not be rendered", failure)
        }
        error => {
            let failure = integration::public_failure(&error);
            private_administration_error(
                &error,
                "integration configuration could not be rendered",
                failure,
            )
        }
    }
}

fn reindex_error(error: reindex::ReindexAdministrationError) -> ManagementResponseError {
    match error {
        reindex::ReindexAdministrationError::Public(error) => error,
        reindex::ReindexAdministrationError::Interrupted(failure)
        | reindex::ReindexAdministrationError::Session(DatabaseAccessError::Operation(failure)) => {
            operation::public_error(failure)
        }
        reindex::ReindexAdministrationError::Session(DatabaseAccessError::RevisionConflict {
            expected,
            actual,
        }) => ManagementResponseError {
            message: "configuration changed before reindex administration".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        reindex::ReindexAdministrationError::Session(DatabaseAccessError::Configuration(error)) => {
            management_error(error)
        }
        error @ (reindex::ReindexAdministrationError::Session(
            DatabaseAccessError::Connection { .. },
        )
        | reindex::ReindexAdministrationError::Database { .. }
        | reindex::ReindexAdministrationError::Operation {
            source: tribal_worker::ReindexOpError::Db(_),
        }) => private_administration_error(
            &error,
            "reindex database is unavailable",
            AdministrationFailure::DatabaseUnavailable,
        ),
        reindex::ReindexAdministrationError::Target => administration_error(
            "reindex operation is unavailable",
            AdministrationFailure::ReindexUnavailable,
        ),
        error @ (reindex::ReindexAdministrationError::Gateway { .. }
        | reindex::ReindexAdministrationError::Operation { .. }) => private_administration_error(
            &error,
            "reindex operation is unavailable",
            AdministrationFailure::ReindexUnavailable,
        ),
    }
}

fn thread_error(error: thread::ThreadAdministrationError) -> ManagementResponseError {
    match error {
        thread::ThreadAdministrationError::Session(DatabaseAccessError::Operation(failure)) => {
            operation::public_error(failure)
        }
        thread::ThreadAdministrationError::Session(DatabaseAccessError::RevisionConflict {
            expected,
            actual,
        }) => ManagementResponseError {
            message: "configuration changed before thread retention".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        thread::ThreadAdministrationError::Session(DatabaseAccessError::Configuration(error)) => {
            management_error(error)
        }
        error @ (thread::ThreadAdministrationError::Session(DatabaseAccessError::Connection {
            ..
        })
        | thread::ThreadAdministrationError::Database { .. }) => private_administration_error(
            &error,
            "thread retention database is unavailable",
            AdministrationFailure::DatabaseUnavailable,
        ),
        thread::ThreadAdministrationError::Retention => administration_error(
            "thread retention request was refused",
            AdministrationFailure::ThreadRetentionRefused,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusal each barrier-admitted surface delivers, asserted against
    /// the very mapping its dispatch arm calls.
    #[test]
    fn test_a_pending_transition_refuses_every_admitted_surface() {
        let gate = StorageTransitionGate::new();
        let pending = StorageTransitionId::new();
        gate.occupy_for_test(pending);

        let refused_lifecycle = gate.admit_lifecycle().expect_err("lifecycle is refused");
        assert_eq!(refused_lifecycle, pending);
        assert!(matches!(
            start_refused_by(refused_lifecycle),
            RuntimeStartResult::OperationInProgress {
                state: StartOperationInProgress::GraphTransition { transition_id }
            } if transition_id == pending
        ));
        assert!(matches!(
            stop_refused_by(refused_lifecycle),
            RuntimeStopResult::OperationInProgress {
                state: StopOperationInProgress::GraphTransition { transition_id }
            } if transition_id == pending
        ));
        assert!(matches!(
            restart_refused_by(refused_lifecycle),
            RuntimeRestartResult::OperationInProgress {
                state: RestartOperationInProgress::GraphTransition { transition_id }
            } if transition_id == pending
        ));

        let refused_migration = gate.admit_migration().expect_err("migration is refused");
        assert_eq!(refused_migration, pending);
        assert!(matches!(
            initialise_refused_by(refused_migration),
            DatabaseInitialiseOutcome::GraphTransitionInProgress {
                transition_id: Some(transition_id)
            } if transition_id == pending
        ));

        // Bootstrap shares the migration slot and answers the typed
        // administration failure.
        let error = administration_error(
            "a storage transition holds this graph",
            AdministrationFailure::GraphTransitionInProgress,
        );
        assert!(matches!(
            error.error,
            ManagementError::Administration {
                failure: AdministrationFailure::GraphTransitionInProgress
            }
        ));

        gate.release_for_test();
        drop(
            gate.admit_lifecycle()
                .expect("admission resumes on release"),
        );
        drop(
            gate.admit_migration()
                .expect("admission resumes on release"),
        );
    }

    #[test]
    fn test_a_pending_transition_refuses_a_database_target_write() {
        let gate = StorageTransitionGate::new();
        let database_url =
            tribal_domain::ConfigFieldPath::parse(DATABASE_URL_KEY).expect("a valid config key");
        let unrelated =
            tribal_domain::ConfigFieldPath::parse("server.host").expect("a valid config key");

        let admission =
            admit_config_write(&gate, &[&database_url]).expect("no transition, no refusal");
        assert!(
            admission.is_some(),
            "a database-target write holds its admission for the write's duration",
        );
        drop(admission);

        gate.occupy_for_test(StorageTransitionId::new());
        let refused = admit_config_write(&gate, &[&database_url])
            .expect_err("a pending transition refuses the write");
        assert!(matches!(
            refused.error,
            ManagementError::Administration {
                failure: AdministrationFailure::GraphTransitionInProgress
            }
        ));
        assert!(
            admit_config_write(&gate, &[&unrelated])
                .expect("an unrelated key is untouched by the transition")
                .is_none(),
            "only a database-target write takes the barrier",
        );

        gate.release_for_test();
        drop(
            admit_config_write(&gate, &[&database_url])
                .expect("the write resumes once the transition settles"),
        );
    }

    #[test]
    fn test_malformed_call_parameters_are_refused() {
        let error = parse_call::<ConfigGetCall>(None).expect_err("missing parameters are refused");

        assert!(matches!(
            error.error,
            ManagementError::ConfigurationInvalid { .. }
        ));
    }

    #[test]
    fn test_private_database_failure_is_logged_but_not_exposed() {
        const PRIVATE_CONTEXT: &str = "private database diagnostic";
        let (capture, _capture) = tribal_test_utils::TracingCapture::install();
        let response = database_access_error(DatabaseAccessError::Connection {
            source: tribal_db::DbError::QueryFailed {
                context: PRIVATE_CONTEXT.to_owned(),
                source: sqlx::Error::RowNotFound,
            },
        });

        let encoded = serde_json::to_string(&response).expect("public error encodes");
        assert!(!encoded.contains(PRIVATE_CONTEXT));
        let event = capture
            .event("management application request failed")
            .expect("private failure emits one diagnostic event");
        assert!(
            event
                .field("error")
                .is_some_and(|error| error.contains(PRIVATE_CONTEXT))
        );
    }
}
