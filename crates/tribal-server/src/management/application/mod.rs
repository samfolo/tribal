//! Manager-private operator application façade.

mod credential;
mod database;
mod pagination;
mod project;

use database::{DatabaseAccess, DatabaseAccessError, DatabaseInitialiseError};
use project::ProjectAdministration;
use tribal_wire::management::{
    ConfigGetCall, ConfigPatchCall, ConfigSetCall, ConfigValidateCall, ConfigValidation,
    ConfigViolation, CredentialSourcesCall, DatabaseInitialiseCall, GraphConfigureGenesisCall,
    GraphConvergeGenesisCall, LogsTailCall, ManagementCall, ManagementError, ManagementMethod,
    ManagementResponseError, ModelsSelectCall, ProjectListCall, ProjectRegisterCall,
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
}

impl<'a> ManagementApplication<'a> {
    pub(crate) fn new(
        config: &'a ConfigWorkerClient,
        product: &'a ProductSession,
        probe: &'a ProbeService,
        lifecycle: &'a LifecycleController,
    ) -> Self {
        let database = DatabaseAccess::new(config.clone());
        Self {
            config,
            product,
            probe,
            lifecycle,
            projects: ProjectAdministration::new(database.clone()),
            database,
        }
    }

    pub(crate) async fn dispatch(
        &self,
        method: ManagementMethod,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, ManagementResponseError> {
        match method {
            ManagementMethod::ManagerSnapshot => lifecycle_value(self.lifecycle.snapshot().await),
            ManagementMethod::RuntimeStart => lifecycle_value(self.lifecycle.start().await),
            ManagementMethod::RuntimeStop => lifecycle_value(self.lifecycle.stop().await),
            ManagementMethod::RuntimeRestart => lifecycle_value(self.lifecycle.restart().await),
            ManagementMethod::ManagerShutdown => lifecycle_value(self.lifecycle.shutdown().await),
            ManagementMethod::ServerStatus => {
                lifecycle_value(self.lifecycle.runtime_status().await)
            }
            ManagementMethod::LogsTail => {
                let request = parse_call::<LogsTailCall>(params)?;
                lifecycle_value(self.lifecycle.runtime_logs_tail(request.lines).await)
            }
            ManagementMethod::TokenList => {
                lifecycle_value(self.lifecycle.runtime_token_list().await)
            }
            ManagementMethod::CheckReport => self.readiness_value().await,
            ManagementMethod::DatabaseInitialise => self.initialise_database_value(params).await,
            ManagementMethod::ProjectRegister => self.register_project_value(params).await,
            ManagementMethod::ProjectList => self.list_projects_value(params).await,
            ManagementMethod::DatabaseProbe => {
                let receipt = self.probe.database().await.map_err(probe_error)?;
                self.refresh_readiness().await?;
                response_value(Ok(receipt))
            }
            ManagementMethod::CredentialProbe => {
                let receipts = self.probe.credentials().await.map_err(probe_error)?;
                self.refresh_readiness().await?;
                response_value(Ok(receipts))
            }
            ManagementMethod::ConfigGetAll => config_value(self.config.document().await),
            ManagementMethod::ConfigPath => config_value(self.config.path().await),
            ManagementMethod::ConfigSchema => serde_json::to_value(
                config_schema::project(tribal_config::config_schema())
                    .map_err(|_| invalid_request("configuration schema projection failed"))?,
            )
            .map_err(|_| invalid_request("configuration schema encoding failed")),
            ManagementMethod::ConfigGet => {
                config_value(self.config.get(parse_call::<ConfigGetCall>(params)?).await)
            }
            ManagementMethod::ConfigValidate => {
                let request = parse_call::<ConfigValidateCall>(params)?;
                let violations = self
                    .config
                    .validate(request.key.as_str().to_owned(), request.value)
                    .await
                    .map_err(management_error)?;
                response_value(Ok(ConfigValidation {
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
                response_value(Ok(outcome))
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
                response_value(Ok(outcome))
            }
            ManagementMethod::ModelsCatalogue => {
                response_value(self.product.models_catalogue().await)
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
                response_value(Ok(outcome))
            }
            ManagementMethod::CredentialSources => response_value(
                self.product
                    .credential_sources(parse_call::<CredentialSourcesCall>(params)?)
                    .await,
            ),
            ManagementMethod::GraphGenesisOptions => {
                response_value(self.product.genesis_options().await)
            }
            ManagementMethod::GraphEmbeddingProfile => {
                response_value(self.product.embedding_profile().await)
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
                response_value(Ok(outcome))
            }
            ManagementMethod::GraphConvergeGenesis => response_value(
                self.product
                    .converge_genesis(parse_call::<GraphConvergeGenesisCall>(params)?)
                    .await,
            ),
        }
    }

    async fn readiness_value(&self) -> Result<serde_json::Value, ManagementResponseError> {
        let report = self.readiness_report().await?;
        serde_json::to_value(report)
            .map_err(|_| invalid_request("readiness response encoding failed"))
    }

    async fn initialise_database_value(
        &self,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, ManagementResponseError> {
        let request = parse_call::<DatabaseInitialiseCall>(params)?;
        response_value(
            self.database
                .initialise(request)
                .await
                .map_err(database_initialise_error),
        )
    }

    async fn register_project_value(
        &self,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, ManagementResponseError> {
        let request = parse_call::<ProjectRegisterCall>(params)?;
        response_value(
            self.projects
                .register(request)
                .await
                .map_err(project::public_error),
        )
    }

    async fn list_projects_value(
        &self,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, ManagementResponseError> {
        let request = parse_call::<ProjectListCall>(params)?;
        response_value(
            self.projects
                .list(request)
                .await
                .map_err(project::public_error),
        )
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
            .map_err(|_| invalid_request("readiness observation failed"))
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

fn lifecycle_value<T: serde::Serialize>(
    result: Option<T>,
) -> Result<serde_json::Value, ManagementResponseError> {
    let value = result.ok_or_else(|| invalid_request("lifecycle owner is unavailable"))?;
    serde_json::to_value(value).map_err(|_| invalid_request("lifecycle response encoding failed"))
}

fn config_value<T: serde::Serialize>(
    result: Result<T, ConfigAuthorityError>,
) -> Result<serde_json::Value, ManagementResponseError> {
    let value = result.map_err(management_error)?;
    serde_json::to_value(value).map_err(|_| invalid_request("management response encoding failed"))
}

fn response_value<T: serde::Serialize>(
    result: Result<T, ManagementResponseError>,
) -> Result<serde_json::Value, ManagementResponseError> {
    let value = result?;
    serde_json::to_value(value).map_err(|_| invalid_request("management response encoding failed"))
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
        DatabaseInitialiseError::Session(DatabaseAccessError::Connection { .. })
        | DatabaseInitialiseError::MigrationState { .. }
        | DatabaseInitialiseError::MigrationConnection { .. }
        | DatabaseInitialiseError::Principal { .. } => administration_error(
            "database is unavailable",
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
