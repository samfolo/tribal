//! Typed projection of the manager-owned bootstrap composition.

use std::collections::BTreeMap;

use tribal_wire::management::{
    AbsoluteDirectoryPath, BootstrapGenesisInput, BootstrapRequest, BootstrapRunCall,
    BootstrapStorage, BootstrapTelemetryInput, BootstrapTokenPolicy, ConfigGetAllCall,
    ConfiguredMcpTarget, EndpointSelection, GenesisEmbeddingInput, InferenceStage, KnownModelId,
    McpTarget, McpTargetSelection, ModelSelectionInput, NetworkIntegrationAuth, OtlpEndpoint,
    ProjectRegisterInput, ProjectRegistrationSource, SecretLiteral, StdioProjectContext,
    TransportKind,
};

use super::{config, presentation};
use crate::{
    cli::{BootstrapArgs, IntegrationAuthArg},
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
    let model_selections = model_selections(args.model_selections)?;
    let genesis = match (args.genesis_provider, args.genesis_model) {
        (Some(provider), Some(model)) => Some(BootstrapGenesisInput {
            embedding: GenesisEmbeddingInput {
                provider,
                model,
                dimensions: args.genesis_dimensions,
                base_url: args.genesis_base_url,
            },
            credential: None,
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
    let stdio_context = StdioProjectContext::Unscoped;
    let integration = match args.transport {
        None => McpTargetSelection::Configured {
            policy: match args.auth {
                IntegrationAuthArg::OAuth => ConfiguredMcpTarget::Public { stdio_context },
                IntegrationAuthArg::PersistedBearer => {
                    ConfiguredMcpTarget::ExportPersistedBearer { stdio_context }
                }
            },
        },
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

fn model_selections(values: Vec<String>) -> Result<Vec<ModelSelectionInput>, AppError> {
    let mut grouped = BTreeMap::<String, Vec<InferenceStage>>::new();
    for value in values {
        let Some((stage, model)) = value.split_once('=') else {
            return Err(command_error(BootstrapCommandError::ModelSelection {
                value,
            }));
        };
        let stage = match stage {
            "extraction" => InferenceStage::Extraction,
            "triage" => InferenceStage::Triage,
            "relation" => InferenceStage::Relation,
            _ => {
                return Err(command_error(BootstrapCommandError::ModelSelection {
                    value,
                }));
            }
        };
        grouped.entry(model.to_owned()).or_default().push(stage);
    }
    grouped
        .into_iter()
        .map(|(model, stages)| {
            let id = KnownModelId::parse(&model).map_err(|source| {
                command_error(BootstrapCommandError::ModelId {
                    value: model.clone(),
                    source,
                })
            })?;
            Ok(ModelSelectionInput {
                model: id,
                stages,
                endpoint: EndpointSelection::Preserve,
                credential: None,
            })
        })
        .collect()
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
