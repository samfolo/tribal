//! Atomic headless bootstrap over manager-owned capabilities.

use tribal_config::TribalConfig;
use tribal_domain::{BearerToken, ConfigFieldPath};
use tribal_wire::management::{
    AdministrationFailure, BootstrapHandoff, BootstrapOutcome, BootstrapPublicCredential,
    BootstrapRequest, BootstrapResult, BootstrapStorage, ConfigLiteral, ConfigPatchChange,
    ConfigPatchOutcome, ConfigPatchRequest, ConfigRevision, CredentialOrigin,
    DatabaseAdministrationTarget, DatabaseInitialiseOutcome, DatabaseInitialiseRequest,
    IssuedBearerToken, ManagementError, ManagementResponseError, McpConfigEntry, McpConfigRequest,
    McpTarget, ProjectRegisterRequest, Revisioned,
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
    product::{ProductSession, config_processing, provider_input},
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
    changes: Vec<ConfigPatchChange>,
    token: PreparedBootstrapToken,
}

/// A transition owns the graph this bootstrap would configure: initialisation
/// ran none of its postconditions, so none of the later effects may run.
fn refuse_under_transition(
    outcome: &DatabaseInitialiseOutcome,
) -> Result<(), ManagementResponseError> {
    if matches!(
        outcome,
        DatabaseInitialiseOutcome::GraphTransitionInProgress { .. }
    ) {
        return Err(super::administration_error(
            "a storage transition holds this graph",
            AdministrationFailure::GraphTransitionInProgress,
        ));
    }
    Ok(())
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

    pub(super) async fn run(
        &self,
        _product: &ProductSession,
        request: BootstrapRequest,
    ) -> Result<BootstrapResult, ManagementResponseError> {
        let BootstrapRequest {
            expected_revision,
            storage,
            provider_connections,
            processing,
            genesis,
            telemetry,
            additional_project,
            token,
            integration,
        } = request;
        let preflight = self
            .operation
            .cancel_safe(self.preflight(
                expected_revision.clone(),
                storage,
                provider_connections,
                processing,
                genesis,
                telemetry,
                additional_project.as_ref(),
                token,
                &integration,
            ))
            .await
            .map_err(super::operation::public_error)??;

        self.checkpoint()?;
        let revision = if preflight.changes.is_empty() {
            expected_revision
        } else {
            self.patch_config(expected_revision, preflight.changes)
                .await?
        };

        self.checkpoint()?;
        let database = self
            .database
            .initialise(
                &self.operation,
                DatabaseInitialiseRequest {
                    expected_revision: revision,
                    target: DatabaseAdministrationTarget::Configured,
                },
            )
            .await
            .map_err(super::database_initialise_error)?;
        let database_outcome = database.value;
        refuse_under_transition(&database_outcome)?;
        let mut revision = database.config_revision.clone();
        self.checkpoint()?;
        let system_project = self
            .projects
            .system(&self.operation)
            .await
            .map_err(super::project::public_error)?;
        if system_project.config_revision != revision {
            return Err(config_conflict(revision, system_project.config_revision));
        }
        let additional_project_outcome = if let Some(project) = additional_project {
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
                system_project: system_project.value,
                additional_project: additional_project_outcome,
                handoff,
            },
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "bootstrap preflight receives each destructive request domain explicitly"
    )]
    async fn preflight(
        &self,
        expected_revision: ConfigRevision,
        storage: BootstrapStorage,
        provider_connections: Vec<tribal_wire::management::BootstrapProviderConnectionInput>,
        processing: Option<tribal_wire::management::ProcessingProfile>,
        genesis: Option<tribal_wire::management::BootstrapGenesisInput>,
        telemetry: Option<tribal_wire::management::BootstrapTelemetryInput>,
        project: Option<&tribal_wire::management::ProjectRegisterInput>,
        token: tribal_wire::management::BootstrapTokenPolicy,
        integration: &tribal_wire::management::McpTargetSelection,
    ) -> Result<BootstrapPreflight, ManagementResponseError> {
        // The base a bootstrap overlays its inputs on may be unconfigured — an
        // empty or partially-valid document on a fresh system — so it is read
        // leniently. `validate_candidate` below then holds the overlaid result
        // to the full contract, and `patch_config` applies it through the same
        // repair path the incremental `config.set`/`config.patch` writes use,
        // so CLI bootstrap and the client's step-by-step setup onboard one way.
        let snapshot = self
            .config
            .for_operation(&self.operation)
            .base_snapshot()
            .await
            .map_err(ConfigWorkerRequestError::into_public_error)?;
        if snapshot.revision != expected_revision {
            return Err(config_conflict(expected_revision, snapshot.revision));
        }
        let mut candidate = snapshot.config.as_ref().clone();
        let mut changed_database = false;
        if let BootstrapStorage::External { database_url } = storage {
            database_url
                .expose_secret()
                .clone_into(&mut candidate.database.url);
            changed_database = true;
        }
        let changed_providers = !provider_connections.is_empty();
        for input in provider_connections {
            let existing = candidate.provider_connections.get(input.name.as_str());
            let connection = provider_input(input.connection, existing)?;
            candidate
                .provider_connections
                .insert(input.name, connection);
        }
        let changed_processing = processing.is_some();
        if let Some(processing) = processing {
            candidate.apply_processing_profile(config_processing(processing)?);
        }
        let changed_genesis = genesis.is_some();
        if let Some(genesis) = genesis {
            candidate.init.embedding.connection = genesis.connection;
            candidate.init.embedding.model = genesis.model;
            candidate.init.embedding.dimensions = genesis.dimensions;
        }
        let changed_telemetry = telemetry.is_some();
        if let Some(telemetry) = telemetry {
            candidate.telemetry.enabled = true;
            candidate.telemetry.otlp_endpoint = Some(String::from(telemetry.otlp_endpoint));
        }
        validate_candidate(&candidate)?;

        // Both preflights read the candidate — the configuration this bootstrap
        // is applying — so onboarding a fresh system computes the token
        // audience and integration target from the config it is about to
        // commit, never from the unconfigured durable base.
        let token =
            TokenAdministration::preflight_bootstrap(token, &candidate).map_err(token_error)?;
        if let Some(project) = project {
            ProjectAdministration::preflight(project).map_err(super::project::public_error)?;
        }
        let target = IntegrationAdministration::preflight_target(integration, &candidate)
            .map_err(super::integration_error)?;
        if project.is_some()
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

        let mut changes = Vec::new();
        if changed_database {
            changes.push(change("database", &candidate.database)?);
        }
        if changed_providers {
            changes.push(change(
                "provider_connections",
                &candidate.provider_connections,
            )?);
        }
        if changed_processing {
            changes.push(change("inference", &candidate.inference)?);
            changes.push(change("agents", &candidate.agents)?);
        }
        if changed_genesis {
            changes.push(change("init.embedding", &candidate.init.embedding)?);
        }
        if changed_telemetry {
            changes.push(change("telemetry", &candidate.telemetry)?);
        }
        Ok(BootstrapPreflight { changes, token })
    }

    fn checkpoint(&self) -> Result<(), ManagementResponseError> {
        self.operation
            .checkpoint()
            .map_err(super::operation::public_error)
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

fn validate_candidate(candidate: &TribalConfig) -> Result<(), ManagementResponseError> {
    tribal_config::validate(candidate).map_err(|error| {
        let fields = match error {
            tribal_config::ConfigError::ValidationFailed { diagnostics } => diagnostics
                .iter()
                .flat_map(tribal_config::ValidationError::fields)
                .filter_map(|path| ConfigFieldPath::parse(path.as_str()).ok())
                .collect(),
            tribal_config::ConfigError::Load { .. }
            | tribal_config::ConfigError::Render { .. }
            | tribal_config::ConfigError::RemovedProviderShape { .. } => Vec::new(),
        };
        ManagementResponseError {
            message: "bootstrap configuration is invalid".to_owned(),
            error: ManagementError::ConfigurationInvalid { fields },
        }
    })
}

fn change(
    path: &str,
    value: &impl serde::Serialize,
) -> Result<ConfigPatchChange, ManagementResponseError> {
    Ok(ConfigPatchChange {
        key: ConfigFieldPath::parse(path).map_err(|_| internal_error())?,
        value: ConfigLiteral::new(serde_json::to_value(value).map_err(|_| internal_error())?),
    })
}

fn config_conflict(expected: ConfigRevision, actual: ConfigRevision) -> ManagementResponseError {
    ManagementResponseError {
        message: "configuration changed before bootstrap".to_owned(),
        error: ManagementError::ConfigConflict { expected, actual },
    }
}

fn internal_error() -> ManagementResponseError {
    ManagementResponseError {
        message: "bootstrap contract invariant failed".to_owned(),
        error: ManagementError::InternalInvariant,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_candidate_validation_reports_all_participating_fields() {
        let mut config = TribalConfig::minimum_valid("postgres://localhost/tribal");
        config.inference.extraction.connection =
            tribal_domain::ProviderConnectionName::parse("missing").unwrap();
        let error = validate_candidate(&config).expect_err("missing connection is invalid");
        assert!(matches!(
            error.error,
            ManagementError::ConfigurationInvalid { ref fields }
                if fields.iter().any(|field| field.as_str() == "inference.extraction.connection")
        ));
    }
}
