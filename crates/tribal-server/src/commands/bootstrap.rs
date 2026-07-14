//! Typed projection of the manager-owned bootstrap composition.

use std::{collections::BTreeMap, io::Read as _};

use tribal_wire::management::{
    AbsoluteDirectoryPath, BootstrapGenesisCredential, BootstrapGenesisInput, BootstrapRequest,
    BootstrapRunCall, BootstrapStorage, BootstrapTelemetryInput, BootstrapTokenPolicy,
    ConfigGetAllCall, ConfiguredMcpTarget, CredentialInput, EndpointSelection,
    GenesisEmbeddingInput, InferenceStage, KnownModelId, McpTarget, McpTargetSelection,
    ModelSelectionInput, NetworkIntegrationAuth, OtlpEndpoint, ProjectRegisterInput,
    ProjectRegistrationSource, ProjectSelector, SecretLiteral, StdioProjectContext, TransportKind,
};

use super::{config, presentation};
use crate::{
    cli::{
        BootstrapArgs, InferenceStageArg, IntegrationAuthArg, StageCredentialSourceArg,
        StageEndpointArg,
    },
    error::AppError,
};

/// Failure validating bootstrap command arguments.
#[derive(Debug, thiserror::Error)]
enum BootstrapCommandError {
    #[error("invalid model selection '{value}'; expected stage=model-id")]
    ModelSelection { value: String },
    #[error("invalid model id in selection '{value}': {source}")]
    ModelId {
        value: String,
        #[source]
        source: tribal_wire::management::WireIdError,
    },
    #[error("invalid external database URL: {source}")]
    DatabaseSecret {
        #[source]
        source: tribal_wire::management::SecretLiteralError,
    },
    #[error("invalid OTLP endpoint: {source}")]
    Otlp {
        #[source]
        source: tribal_wire::management::OtlpEndpointError,
    },
    #[error("resolving the bootstrap project directory: {source}")]
    ProjectDirectory {
        #[source]
        source: std::io::Error,
    },
    #[error("bootstrap project directory must be absolute")]
    RelativeProjectDirectory,
    #[error("genesis provider and model must be supplied together")]
    IncompleteGenesis,
    #[error("persisted bearer authentication is unavailable for stdio integration")]
    PersistedBearerStdio,
    #[error("bootstrap project source cannot select an stdio context")]
    ProjectSelectorUnsupported,
    #[error("invalid model credential mapping '{value}'; expected stage=environment-variable")]
    ModelCredential { value: String },
    #[error("model credential for stage '{stage}' was supplied more than once")]
    DuplicateModelCredential { stage: String },
    #[error("model endpoint for stage '{stage}' was supplied more than once")]
    DuplicateModelEndpoint { stage: String },
    #[error("model endpoint for stage '{stage}' has no matching model selection")]
    OrphanModelEndpoint { stage: String },
    #[error("reading credential environment variable '{variable}': {source}")]
    CredentialEnvironment {
        variable: String,
        #[source]
        source: std::env::VarError,
    },
    #[error("reading credential from stdin: {source}")]
    CredentialStdin {
        #[source]
        source: std::io::Error,
    },
    #[error("invalid credential secret: {source}")]
    CredentialSecret {
        #[source]
        source: tribal_wire::management::SecretLiteralError,
    },
}

pub(crate) async fn run(config_path: &str, args: BootstrapArgs) -> Result<(), AppError> {
    let request_parts = request_parts(args)?;
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let result = connection
        .call::<BootstrapRunCall>(&BootstrapRequest {
            expected_revision,
            storage: request_parts.storage,
            model_selections: request_parts.model_selections,
            genesis: request_parts.genesis,
            telemetry: request_parts.telemetry,
            project: request_parts.project,
            token: request_parts.token,
            integration: request_parts.integration,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        request_parts.json,
        "Bootstrap",
        &result,
        "writing bootstrap result",
    )
}

struct BootstrapRequestParts {
    storage: BootstrapStorage,
    model_selections: Vec<ModelSelectionInput>,
    genesis: Option<BootstrapGenesisInput>,
    telemetry: Option<BootstrapTelemetryInput>,
    project: Option<ProjectRegisterInput>,
    token: BootstrapTokenPolicy,
    integration: McpTargetSelection,
    json: bool,
}

fn request_parts(args: BootstrapArgs) -> Result<BootstrapRequestParts, AppError> {
    let storage = match args.database_url {
        Some(database_url) => BootstrapStorage::External {
            database_url: SecretLiteral::try_from(database_url).map_err(|source| {
                command_error(BootstrapCommandError::DatabaseSecret { source })
            })?,
        },
        None => BootstrapStorage::Configured,
    };
    let model_credentials = model_credentials(
        args.model_credential_env,
        args.model_credential_sources,
        args.model_credential_stdin,
    )?;
    let model_endpoints = model_endpoints(args.model_endpoints)?;
    let model_selections =
        model_selections(args.model_selections, model_endpoints, model_credentials)?;
    let genesis_credential = genesis_credential(
        args.genesis_credential_env,
        args.genesis_credential_source,
        args.genesis_credential_stdin,
        args.genesis_reuse_stage,
    )?;
    let genesis = match (args.genesis_provider, args.genesis_model) {
        (Some(provider), Some(model)) => Some(BootstrapGenesisInput {
            embedding: GenesisEmbeddingInput {
                provider,
                model,
                dimensions: args.genesis_dimensions,
                base_url: args.genesis_base_url,
            },
            credential: genesis_credential,
        }),
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            return Err(command_error(BootstrapCommandError::IncompleteGenesis));
        }
    };
    let telemetry = args
        .otlp_endpoint
        .map(OtlpEndpoint::try_from)
        .transpose()
        .map_err(|source| command_error(BootstrapCommandError::Otlp { source }))?
        .map(|otlp_endpoint| BootstrapTelemetryInput { otlp_endpoint });
    let project = args
        .project_path
        .map(|path| project_input(path, args.project_name, args.project_branch))
        .transpose()?;
    let token = if args.create_token {
        BootstrapTokenPolicy::Create {
            principal: args.principal,
            ttl_hours: args.ttl,
            scopes: args.scope,
        }
    } else {
        BootstrapTokenPolicy::EnsureLocalCredential {
            principal: args.principal,
            ttl_hours: args.ttl,
            scopes: args.scope,
        }
    };
    let stdio_context = match project.as_ref().map(|project| &project.source) {
        None => StdioProjectContext::Unscoped,
        Some(ProjectRegistrationSource::WorkingTree { directory }) => {
            StdioProjectContext::Project {
                selector: ProjectSelector::WorkingTree {
                    directory: directory.clone(),
                },
            }
        }
        Some(ProjectRegistrationSource::GitRemote { .. }) => {
            return Err(command_error(
                BootstrapCommandError::ProjectSelectorUnsupported,
            ));
        }
    };
    let integration = match args.transport {
        None => McpTargetSelection::Configured {
            policy: match args.auth {
                IntegrationAuthArg::OAuth => ConfiguredMcpTarget::Public { stdio_context },
                IntegrationAuthArg::PersistedBearer => {
                    ConfiguredMcpTarget::ExportPersistedBearer { stdio_context }
                }
            },
        },
        Some(TransportKind::Stdio) if args.auth == IntegrationAuthArg::PersistedBearer => {
            return Err(command_error(BootstrapCommandError::PersistedBearerStdio));
        }
        Some(TransportKind::Stdio) => McpTargetSelection::Explicit {
            target: McpTarget::Stdio {
                context: stdio_context,
            },
        },
        Some(TransportKind::Http) => McpTargetSelection::Explicit {
            target: McpTarget::Http {
                auth: network_auth(args.auth),
            },
        },
        Some(TransportKind::Sse) => McpTargetSelection::Explicit {
            target: McpTarget::Sse {
                auth: network_auth(args.auth),
            },
        },
    };
    Ok(BootstrapRequestParts {
        storage,
        model_selections,
        genesis,
        telemetry,
        project,
        token,
        integration,
        json: args.json,
    })
}

fn model_selections(
    values: Vec<String>,
    mut endpoints: BTreeMap<String, EndpointSelection>,
    mut credentials: BTreeMap<String, CredentialInput>,
) -> Result<Vec<ModelSelectionInput>, AppError> {
    let mut selections = Vec::new();
    for value in values {
        let Some((stage, model)) = value.split_once('=') else {
            return Err(command_error(BootstrapCommandError::ModelSelection {
                value,
            }));
        };
        let parsed_stage = parse_stage(stage).ok_or_else(|| {
            command_error(BootstrapCommandError::ModelSelection {
                value: value.clone(),
            })
        })?;
        let id = KnownModelId::parse(model).map_err(|source| {
            command_error(BootstrapCommandError::ModelId {
                value: model.to_owned(),
                source,
            })
        })?;
        selections.push(ModelSelectionInput {
            model: id,
            stages: vec![parsed_stage],
            endpoint: endpoints
                .remove(stage)
                .unwrap_or(EndpointSelection::Preserve),
            credential: credentials.remove(stage),
        });
    }
    if let Some((stage, _)) = endpoints.into_iter().next() {
        return Err(command_error(BootstrapCommandError::OrphanModelEndpoint {
            stage,
        }));
    }
    if let Some((stage, _)) = credentials.into_iter().next() {
        return Err(command_error(BootstrapCommandError::ModelCredential {
            value: stage,
        }));
    }
    Ok(selections)
}

fn model_endpoints(
    values: Vec<StageEndpointArg>,
) -> Result<BTreeMap<String, EndpointSelection>, AppError> {
    let mut endpoints = BTreeMap::new();
    for value in values {
        let stage = stage_name(value.stage).to_owned();
        if endpoints.insert(stage.clone(), value.endpoint).is_some() {
            return Err(command_error(
                BootstrapCommandError::DuplicateModelEndpoint { stage },
            ));
        }
    }
    Ok(endpoints)
}

fn model_credentials(
    environment: Vec<String>,
    sources: Vec<StageCredentialSourceArg>,
    stdin_stage: Option<InferenceStageArg>,
) -> Result<BTreeMap<String, CredentialInput>, AppError> {
    let mut credentials = BTreeMap::new();
    for value in environment {
        let Some((stage, variable)) = value.split_once('=') else {
            return Err(command_error(BootstrapCommandError::ModelCredential {
                value,
            }));
        };
        if parse_stage(stage).is_none() || variable.is_empty() {
            return Err(command_error(BootstrapCommandError::ModelCredential {
                value,
            }));
        }
        insert_model_credential(
            &mut credentials,
            stage,
            credential_from_environment(variable)?,
        )?;
    }
    for value in sources {
        insert_model_credential(
            &mut credentials,
            stage_name(value.stage),
            CredentialInput::Source {
                source: value.source,
            },
        )?;
    }
    if let Some(stage) = stdin_stage {
        let stage = stage_name(stage);
        insert_model_credential(&mut credentials, stage, credential_from_stdin()?)?;
    }
    Ok(credentials)
}

fn insert_model_credential(
    credentials: &mut BTreeMap<String, CredentialInput>,
    stage: &str,
    credential: CredentialInput,
) -> Result<(), AppError> {
    if credentials.insert(stage.to_owned(), credential).is_some() {
        return Err(command_error(
            BootstrapCommandError::DuplicateModelCredential {
                stage: stage.to_owned(),
            },
        ));
    }
    Ok(())
}

fn genesis_credential(
    environment: Option<String>,
    source: Option<tribal_wire::management::CredentialSourceId>,
    stdin: bool,
    reuse: Option<InferenceStageArg>,
) -> Result<Option<BootstrapGenesisCredential>, AppError> {
    if let Some(variable) = environment {
        return credential_from_environment(&variable)
            .map(|credential| Some(BootstrapGenesisCredential::Explicit { credential }));
    }
    if let Some(source) = source {
        return Ok(Some(BootstrapGenesisCredential::Explicit {
            credential: CredentialInput::Source { source },
        }));
    }
    if stdin {
        return credential_from_stdin()
            .map(|credential| Some(BootstrapGenesisCredential::Explicit { credential }));
    }
    Ok(
        reuse.map(|stage| BootstrapGenesisCredential::ReuseInferenceStage {
            stage: stage_value(stage),
        }),
    )
}

fn credential_from_environment(variable: &str) -> Result<CredentialInput, AppError> {
    let value = std::env::var(variable).map_err(|source| {
        command_error(BootstrapCommandError::CredentialEnvironment {
            variable: variable.to_owned(),
            source,
        })
    })?;
    credential_literal(value)
}

fn credential_from_stdin() -> Result<CredentialInput, AppError> {
    let mut value = String::new();
    std::io::stdin()
        .read_to_string(&mut value)
        .map_err(|source| command_error(BootstrapCommandError::CredentialStdin { source }))?;
    credential_literal(value.trim_end_matches(['\r', '\n']).to_owned())
}

fn credential_literal(value: String) -> Result<CredentialInput, AppError> {
    SecretLiteral::try_from(value)
        .map(|value| CredentialInput::Literal { value })
        .map_err(|source| command_error(BootstrapCommandError::CredentialSecret { source }))
}

fn parse_stage(stage: &str) -> Option<InferenceStage> {
    match stage {
        "extraction" => Some(InferenceStage::Extraction),
        "triage" => Some(InferenceStage::Triage),
        "relation" => Some(InferenceStage::Relation),
        _ => None,
    }
}

fn stage_name(stage: InferenceStageArg) -> &'static str {
    match stage {
        InferenceStageArg::Extraction => "extraction",
        InferenceStageArg::Triage => "triage",
        InferenceStageArg::Relation => "relation",
    }
}

fn stage_value(stage: InferenceStageArg) -> InferenceStage {
    match stage {
        InferenceStageArg::Extraction => InferenceStage::Extraction,
        InferenceStageArg::Triage => InferenceStage::Triage,
        InferenceStageArg::Relation => InferenceStage::Relation,
    }
}

fn project_input(
    raw: String,
    name: Option<String>,
    default_branch: Option<String>,
) -> Result<ProjectRegisterInput, AppError> {
    let path = std::path::PathBuf::from(raw);
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|source| command_error(BootstrapCommandError::ProjectDirectory { source }))?
            .join(path)
    };
    let directory = AbsoluteDirectoryPath::try_from(absolute.to_string_lossy().into_owned())
        .map_err(|_| command_error(BootstrapCommandError::RelativeProjectDirectory))?;
    Ok(ProjectRegisterInput {
        source: ProjectRegistrationSource::WorkingTree { directory },
        name,
        default_branch,
    })
}

fn network_auth(auth: IntegrationAuthArg) -> NetworkIntegrationAuth {
    match auth {
        IntegrationAuthArg::OAuth => NetworkIntegrationAuth::OAuth,
        IntegrationAuthArg::PersistedBearer => NetworkIntegrationAuth::ExportPersistedBearer,
    }
}

fn command_error(source: BootstrapCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}

#[cfg(test)]
mod tests {
    use tribal_domain::ProviderKind;

    use super::*;

    #[test]
    fn test_explicit_project_scopes_the_stdio_handoff_to_the_same_directory() {
        let project = tempfile::tempdir().expect("project directory");
        let parts = request_parts(BootstrapArgs {
            project_path: Some(project.path().to_string_lossy().into_owned()),
            transport: Some(TransportKind::Stdio),
            ..BootstrapArgs::default()
        })
        .expect("bootstrap request projects");

        let project_directory = match parts.project.expect("project registration").source {
            ProjectRegistrationSource::WorkingTree { directory } => directory,
            ProjectRegistrationSource::GitRemote { .. } => panic!("working-tree project"),
        };
        let McpTargetSelection::Explicit {
            target:
                McpTarget::Stdio {
                    context:
                        StdioProjectContext::Project {
                            selector: ProjectSelector::WorkingTree { directory },
                        },
                },
        } = parts.integration
        else {
            panic!("explicit stdio project context");
        };
        assert_eq!(directory, project_directory);
    }

    #[test]
    fn test_explicit_stdio_rejects_persisted_bearer_export() {
        let result = request_parts(BootstrapArgs {
            transport: Some(TransportKind::Stdio),
            auth: IntegrationAuthArg::PersistedBearer,
            ..BootstrapArgs::default()
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_environment_model_credential_and_genesis_reuse_reach_the_wire_request() {
        let parts = request_parts(BootstrapArgs {
            model_selections: vec!["extraction=ollama.llama3.2".to_owned()],
            model_credential_env: vec!["extraction=PATH".to_owned()],
            genesis_provider: Some(ProviderKind::Ollama),
            genesis_model: Some("nomic-embed-text".to_owned()),
            genesis_reuse_stage: Some(InferenceStageArg::Extraction),
            ..BootstrapArgs::default()
        })
        .expect("credential-bearing bootstrap projects");

        assert!(matches!(
            parts.model_selections.as_slice(),
            [ModelSelectionInput {
                credential: Some(CredentialInput::Literal { .. }),
                ..
            }]
        ));
        assert!(matches!(
            parts.genesis.and_then(|genesis| genesis.credential),
            Some(BootstrapGenesisCredential::ReuseInferenceStage {
                stage: InferenceStage::Extraction
            })
        ));
    }

    #[test]
    fn test_endpoint_transitions_and_stored_credentials_reach_the_wire_request() {
        let model_source: tribal_wire::management::CredentialSourceId =
            format!("credsrc_{}", "A".repeat(43))
                .parse()
                .expect("model source parses");
        let genesis_source: tribal_wire::management::CredentialSourceId =
            format!("credsrc_{}Q", "A".repeat(42))
                .parse()
                .expect("genesis source parses");
        let parts = request_parts(BootstrapArgs {
            model_selections: vec!["extraction=openai.gpt-4.1".to_owned()],
            model_endpoints: vec![StageEndpointArg {
                stage: InferenceStageArg::Extraction,
                endpoint: EndpointSelection::ProviderDefault,
            }],
            model_credential_sources: vec![StageCredentialSourceArg {
                stage: InferenceStageArg::Extraction,
                source: model_source.clone(),
            }],
            genesis_provider: Some(ProviderKind::OpenAi),
            genesis_model: Some("text-embedding-3-small".to_owned()),
            genesis_credential_source: Some(genesis_source.clone()),
            ..BootstrapArgs::default()
        })
        .expect("stored credentials project");

        assert!(matches!(
            parts.model_selections.as_slice(),
            [ModelSelectionInput {
                endpoint: EndpointSelection::ProviderDefault,
                credential: Some(CredentialInput::Source { source }),
                ..
            }] if source == &model_source
        ));
        assert!(matches!(
            parts.genesis.and_then(|genesis| genesis.credential),
            Some(BootstrapGenesisCredential::Explicit {
                credential: CredentialInput::Source { source }
            }) if source == genesis_source
        ));
    }
}
