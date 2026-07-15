//! Manager-private operator application façade.

mod bootstrap;
pub(crate) mod credential;
mod database;
mod integration;
mod pagination;
mod project;
mod reindex;
mod thread;
mod token;

use bootstrap::BootstrapAdministration;
use credential::CredentialCoordinator;
pub(crate) use database::{
    COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
    DatabaseSession,
};
use database::{DatabaseAccess, DatabaseAccessError, DatabaseInitialiseError};
use integration::IntegrationAdministration;
use project::ProjectAdministration;
use reindex::ReindexAdministration;
use thread::ThreadAdministration;
use token::TokenAdministration;
use tribal_wire::management::{
    BootstrapRunCall, CheckReportCall, ConfigGetAllCall, ConfigGetCall, ConfigPatchCall,
    ConfigPathCall, ConfigSchemaCall, ConfigSetCall, ConfigValidateCall, ConfigValidation,
    ConfigViolation, CredentialProbeCall, CredentialSourcesCall, DatabaseInitialiseCall,
    DatabaseProbeCall, GraphConfigureGenesisCall, GraphConvergeGenesisCall,
    GraphEmbeddingProfileCall, GraphGenesisOptionsCall, IntegrationMcpConfigCall, LogsTailCall,
    ManagementCall, ManagementError, ManagementMethod, ManagementResponseError,
    ManagerShutdownCall, ManagerSnapshotCall, ModelsCatalogueCall, ModelsSelectCall,
    ProjectListCall, ProjectRegisterCall, ReindexCancelCall, ReindexPruneCall, ReindexRunCall,
    RuntimeRestartCall, RuntimeStartCall, RuntimeStopCall, ServerStatusCall, ThreadsPruneCall,
    TokenCreateCall, TokenListCall, TokenRevokeAllCall, TokenRevokeCall,
};

use super::{
    config_schema,
    configuration::{ConfigAuthorityError, management_error},
    lifecycle::LifecycleController,
    probe::{ProbeError, ProbeService},
    product::ProductSession,
    readiness,
    worker::ConfigWorkerClient,
};

/// One connection's access to the manager-owned application services.
pub(crate) struct ManagementApplication<'a> {
    config: &'a ConfigWorkerClient,
    product: &'a ProductSession,
    probe: &'a ProbeService,
    lifecycle: &'a LifecycleController,
    database: DatabaseAccess,
    projects: ProjectAdministration,
    tokens: TokenAdministration,
    integration: IntegrationAdministration,
    reindex: ReindexAdministration,
    threads: ThreadAdministration,
}

impl<'a> ManagementApplication<'a> {
    pub(crate) fn new(
        config: &'a ConfigWorkerClient,
        product: &'a ProductSession,
        probe: &'a ProbeService,
        lifecycle: &'a LifecycleController,
        credentials: CredentialCoordinator,
    ) -> Self {
        let database = DatabaseAccess::new(config.clone());
        Self {
            config,
            product,
            probe,
            lifecycle,
            projects: ProjectAdministration::new(database.clone()),
            tokens: TokenAdministration::new(database.clone(), credentials.clone()),
            integration: IntegrationAdministration::new(
                config.clone(),
                database.clone(),
                credentials,
            ),
            reindex: ReindexAdministration::new(database.clone()),
            threads: ThreadAdministration::new(database.clone()),
            database,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive registry dispatch remains explicit in one match"
    )]
    pub(crate) async fn dispatch(
        &self,
        method: ManagementMethod,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, ManagementResponseError> {
        match method {
            ManagementMethod::ManagerSnapshot => {
                encode_lifecycle::<ManagerSnapshotCall>(self.lifecycle.snapshot().await)
            }
            ManagementMethod::RuntimeStart => {
                encode_lifecycle::<RuntimeStartCall>(self.lifecycle.start().await)
            }
            ManagementMethod::RuntimeStop => {
                encode_lifecycle::<RuntimeStopCall>(self.lifecycle.stop().await)
            }
            ManagementMethod::RuntimeRestart => {
                encode_lifecycle::<RuntimeRestartCall>(self.lifecycle.restart().await)
            }
            ManagementMethod::ManagerShutdown => {
                encode_lifecycle::<ManagerShutdownCall>(self.lifecycle.shutdown().await)
            }
            ManagementMethod::ServerStatus => {
                encode_lifecycle::<ServerStatusCall>(self.lifecycle.runtime_status().await)
            }
            ManagementMethod::LogsTail => {
                let request = parse_call::<LogsTailCall>(params)?;
                encode_lifecycle::<LogsTailCall>(
                    self.lifecycle.runtime_logs_tail(request.lines).await,
                )
            }
            ManagementMethod::TokenList => {
                let request = parse_call::<TokenListCall>(params)?;
                encode_call::<TokenListCall>(
                    self.tokens.list(request).await.map_err(token::public_error),
                )
            }
            ManagementMethod::TokenCreate => {
                let request = parse_call::<TokenCreateCall>(params)?;
                encode_call::<TokenCreateCall>(
                    self.tokens
                        .create(request)
                        .await
                        .map_err(token::public_error),
                )
            }
            ManagementMethod::TokenRevoke => {
                let request = parse_call::<TokenRevokeCall>(params)?;
                encode_call::<TokenRevokeCall>(
                    self.tokens
                        .revoke(request)
                        .await
                        .map_err(token::public_error),
                )
            }
            ManagementMethod::TokenRevokeAll => {
                let request = parse_call::<TokenRevokeAllCall>(params)?;
                encode_call::<TokenRevokeAllCall>(
                    self.tokens
                        .revoke_all(request)
                        .await
                        .map_err(token::public_error),
                )
            }
            ManagementMethod::CheckReport => {
                encode_call::<CheckReportCall>(self.readiness_report().await)
            }
            ManagementMethod::DatabaseInitialise => {
                let request = parse_call::<DatabaseInitialiseCall>(params)?;
                encode_call::<DatabaseInitialiseCall>(
                    self.database
                        .initialise(request)
                        .await
                        .map_err(database_initialise_error),
                )
            }
            ManagementMethod::ProjectRegister => {
                let request = parse_call::<ProjectRegisterCall>(params)?;
                encode_call::<ProjectRegisterCall>(
                    self.projects
                        .register(request)
                        .await
                        .map_err(project::public_error),
                )
            }
            ManagementMethod::ProjectList => {
                let request = parse_call::<ProjectListCall>(params)?;
                encode_call::<ProjectListCall>(
                    self.projects
                        .list(request)
                        .await
                        .map_err(project::public_error),
                )
            }
            ManagementMethod::IntegrationMcpConfig => {
                let request = parse_call::<IntegrationMcpConfigCall>(params)?;
                encode_call::<IntegrationMcpConfigCall>(
                    self.integration
                        .mcp_config(request)
                        .await
                        .map_err(integration_error),
                )
            }
            ManagementMethod::ReindexRun => {
                let request = parse_call::<ReindexRunCall>(params)?;
                encode_call::<ReindexRunCall>(
                    self.reindex
                        .run(self.product, request)
                        .await
                        .map_err(reindex_error),
                )
            }
            ManagementMethod::ReindexCancel => {
                let request = parse_call::<ReindexCancelCall>(params)?;
                encode_call::<ReindexCancelCall>(
                    self.reindex.cancel(request).await.map_err(reindex_error),
                )
            }
            ManagementMethod::ReindexPrune => {
                let request = parse_call::<ReindexPruneCall>(params)?;
                encode_call::<ReindexPruneCall>(
                    self.reindex.prune(request).await.map_err(reindex_error),
                )
            }
            ManagementMethod::ThreadsPrune => {
                let request = parse_call::<ThreadsPruneCall>(params)?;
                encode_call::<ThreadsPruneCall>(
                    self.threads.prune(request).await.map_err(thread_error),
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
                );
                encode_call::<BootstrapRunCall>(bootstrap.run(self.product, request).await)
            }
            ManagementMethod::DatabaseProbe => {
                let receipt = self.probe.database().await.map_err(probe_error)?;
                self.refresh_readiness().await?;
                encode_call::<DatabaseProbeCall>(Ok(receipt))
            }
            ManagementMethod::CredentialProbe => {
                let receipts = self.probe.credentials().await.map_err(probe_error)?;
                self.refresh_readiness().await?;
                encode_call::<CredentialProbeCall>(Ok(receipts))
            }
            ManagementMethod::ConfigGetAll => {
                encode_config::<ConfigGetAllCall>(self.config.document().await)
            }
            ManagementMethod::ConfigPath => {
                encode_config::<ConfigPathCall>(self.config.path().await)
            }
            ManagementMethod::ConfigSchema => encode_call::<ConfigSchemaCall>(Ok(
                config_schema::project(tribal_config::config_schema())
                    .map_err(|_| internal_error("configuration schema projection failed"))?,
            )),
            ManagementMethod::ConfigGet => encode_config::<ConfigGetCall>(
                self.config.get(parse_call::<ConfigGetCall>(params)?).await,
            ),
            ManagementMethod::ConfigValidate => {
                let request = parse_call::<ConfigValidateCall>(params)?;
                let violations = self
                    .config
                    .validate(request.key.as_str().to_owned(), request.value)
                    .await
                    .map_err(management_error)?;
                encode_call::<ConfigValidateCall>(Ok(ConfigValidation {
                    valid: violations.is_empty(),
                    violations: violations
                        .into_iter()
                        .map(|violation| ConfigViolation {
                            key: violation.key,
                            message: violation.message,
                        })
                        .collect(),
                }))
            }
            ManagementMethod::ConfigSet => {
                let request = parse_call::<ConfigSetCall>(params)?;
                let runtime_changes = vec![tribal_wire::runtime_control::RuntimeConfigChange {
                    key: request.key.clone(),
                    value: tribal_wire::management::ConfigLiteral::new(
                        request.value.expose_sensitive().clone(),
                    ),
                }];
                let mut outcome = self.config.set(request).await.map_err(management_error)?;
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
                let mut outcome = self.config.patch(request).await.map_err(management_error)?;
                project_patch_effects(self.lifecycle, &mut outcome, runtime_changes).await;
                if patch_requires_lifecycle_update(&outcome) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<ConfigPatchCall>(Ok(outcome))
            }
            ManagementMethod::ModelsCatalogue => {
                encode_call::<ModelsCatalogueCall>(self.product.models_catalogue().await)
            }
            ManagementMethod::ModelsSelect => {
                let mut outcome = self
                    .product
                    .select_model(parse_call::<ModelsSelectCall>(params)?)
                    .await?;
                project_patch_effects(self.lifecycle, &mut outcome, Vec::new()).await;
                if patch_requires_lifecycle_update(&outcome) {
                    self.lifecycle.config_changed().await;
                }
                encode_call::<ModelsSelectCall>(Ok(outcome))
            }
            ManagementMethod::CredentialSources => encode_call::<CredentialSourcesCall>(
                self.product
                    .credential_sources(parse_call::<CredentialSourcesCall>(params)?)
                    .await,
            ),
            ManagementMethod::GraphGenesisOptions => {
                encode_call::<GraphGenesisOptionsCall>(self.product.genesis_options().await)
            }
            ManagementMethod::GraphEmbeddingProfile => {
                let session = self
                    .database
                    .read_session()
                    .await
                    .map_err(database_access_error)?;
                encode_call::<GraphEmbeddingProfileCall>(
                    self.product.embedding_profile(&session).await,
                )
            }
            ManagementMethod::GraphConfigureGenesis => {
                let mut outcome = self
                    .product
                    .configure_genesis(parse_call::<GraphConfigureGenesisCall>(params)?)
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
                    .mutation_session(&request.expected_revision)
                    .await
                    .map_err(database_access_error)?;
                encode_call::<GraphConvergeGenesisCall>(
                    self.product.converge_genesis(&session, request).await,
                )
            }
        }
    }

    async fn refresh_readiness(&self) -> Result<(), ManagementResponseError> {
        let report = self.readiness_report().await?;
        self.lifecycle.update_readiness(report).await;
        Ok(())
    }

    async fn readiness_report(
        &self,
    ) -> Result<tribal_wire::management::ReadinessReport, ManagementResponseError> {
        let runtime_present = self
            .lifecycle
            .snapshot()
            .await
            .is_some_and(|snapshot| lifecycle_has_runtime(&snapshot.phase));
        readiness::automatic(self.config, self.probe, runtime_present)
            .await
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
    result: Option<C::Response>,
) -> Result<serde_json::Value, ManagementResponseError>
where
    C::Response: serde::Serialize,
{
    let value = result.ok_or_else(|| internal_error("lifecycle owner is unavailable"))?;
    serde_json::to_value(value).map_err(|_| internal_error("lifecycle response encoding failed"))
}

fn encode_config<C: ManagementCall>(
    result: Result<C::Response, ConfigAuthorityError>,
) -> Result<serde_json::Value, ManagementResponseError>
where
    C::Response: serde::Serialize,
{
    encode_call::<C>(result.map_err(management_error))
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

fn probe_error(_error: ProbeError) -> ManagementResponseError {
    ManagementResponseError {
        message: "external probe is unavailable".to_owned(),
        error: ManagementError::ProbeUnavailable,
    }
}

fn database_initialise_error(error: DatabaseInitialiseError) -> ManagementResponseError {
    match error {
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
        DatabaseInitialiseError::Migration { .. } => administration_error(
            "database migration failed",
            tribal_wire::management::AdministrationFailure::DatabaseMigrationFailed,
        ),
        DatabaseInitialiseError::EmptyMigrationCatalogue => {
            internal_error("compiled database migration catalogue is empty")
        }
        DatabaseInitialiseError::Session(DatabaseAccessError::Connection { .. })
        | DatabaseInitialiseError::MigrationState { .. }
        | DatabaseInitialiseError::MigrationConnection { .. }
        | DatabaseInitialiseError::Principal { .. } => administration_error(
            "database is unavailable",
            tribal_wire::management::AdministrationFailure::DatabaseUnavailable,
        ),
    }
}

fn database_access_error(error: DatabaseAccessError) -> ManagementResponseError {
    match error {
        DatabaseAccessError::Configuration(error) => management_error(error),
        DatabaseAccessError::RevisionConflict { expected, actual } => ManagementResponseError {
            message: "configuration changed before graph administration".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        DatabaseAccessError::Connection { .. } => administration_error(
            "graph database is unavailable",
            tribal_wire::management::AdministrationFailure::DatabaseUnavailable,
        ),
    }
}

fn administration_error(
    message: &str,
    failure: tribal_wire::management::AdministrationFailure,
) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::Administration { failure },
    }
}

fn integration_error(
    error: integration::IntegrationAdministrationError,
) -> ManagementResponseError {
    match error {
        integration::IntegrationAdministrationError::Session(
            DatabaseAccessError::RevisionConflict { expected, actual },
        ) => ManagementResponseError {
            message: "configuration changed before integration rendering".to_owned(),
            error: ManagementError::ConfigConflict { expected, actual },
        },
        integration::IntegrationAdministrationError::Session(
            DatabaseAccessError::Configuration(error),
        )
        | integration::IntegrationAdministrationError::Configuration(error) => {
            management_error(error)
        }
        error => {
            let failure = integration::public_failure(&error);
            administration_error("integration configuration could not be rendered", failure)
        }
    }
}

fn reindex_error(error: reindex::ReindexAdministrationError) -> ManagementResponseError {
    match error {
        reindex::ReindexAdministrationError::Public(error) => error,
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
        reindex::ReindexAdministrationError::Session(DatabaseAccessError::Connection {
            ..
        })
        | reindex::ReindexAdministrationError::Database { .. }
        | reindex::ReindexAdministrationError::Operation {
            source: tribal_worker::ReindexOpError::Db(_),
        } => administration_error(
            "reindex database is unavailable",
            tribal_wire::management::AdministrationFailure::DatabaseUnavailable,
        ),
        reindex::ReindexAdministrationError::Target
        | reindex::ReindexAdministrationError::Gateway { .. }
        | reindex::ReindexAdministrationError::Operation { .. } => administration_error(
            "reindex operation is unavailable",
            tribal_wire::management::AdministrationFailure::ReindexUnavailable,
        ),
    }
}

fn thread_error(error: thread::ThreadAdministrationError) -> ManagementResponseError {
    match error {
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
        thread::ThreadAdministrationError::Session(DatabaseAccessError::Connection { .. })
        | thread::ThreadAdministrationError::Database { .. } => administration_error(
            "thread retention database is unavailable",
            tribal_wire::management::AdministrationFailure::DatabaseUnavailable,
        ),
        thread::ThreadAdministrationError::Retention => administration_error(
            "thread retention request was refused",
            tribal_wire::management::AdministrationFailure::ThreadRetentionRefused,
        ),
    }
}

#[cfg(test)]
mod dispatch {
    use super::*;

    #[test]
    fn test_malformed_call_parameters_are_refused() {
        let error = parse_call::<ConfigGetCall>(None).expect_err("missing parameters are refused");

        assert!(matches!(
            error.error,
            ManagementError::ConfigurationInvalid { .. }
        ));
    }
}
