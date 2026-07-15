//! Typed projection of the manager-owned bootstrap composition.

use std::io::Read as _;

use tribal_domain::ProviderConnectionName;
use tribal_wire::management::{
    AbsoluteDirectoryPath, BootstrapGenesisInput, BootstrapProviderConnectionInput,
    BootstrapRequest, BootstrapRunCall, BootstrapStorage, BootstrapTelemetryInput,
    BootstrapTokenPolicy, ConfigGetAllCall, ConfiguredMcpTarget, CredentialMutation, McpTarget,
    McpTargetSelection, NetworkIntegrationAuth, OtlpEndpoint, ProjectRegisterInput,
    ProjectRegistrationSource, ProjectSelector, ProviderConnectionInput, SecretLiteral,
    StdioProjectContext, TransportKind,
};

use super::{config, presentation};
use crate::{
    cli::{BootstrapArgs, IntegrationAuthArg},
    error::AppError,
};

#[derive(Debug, thiserror::Error)]
enum BootstrapCommandError {
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
    #[error("provider '{provider}' has no configurable endpoint")]
    EndpointUnavailable {
        provider: tribal_domain::ProviderKind,
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
            provider_connections: request_parts.provider_connections,
            processing: None,
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
    provider_connections: Vec<BootstrapProviderConnectionInput>,
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
    let credential = match (args.genesis_credential_env, args.genesis_credential_stdin) {
        (Some(variable), false) => Some(credential_from_environment(&variable)?),
        (None, true) => Some(credential_from_stdin()?),
        (None, false) => None,
        (Some(_), true) => unreachable!("clap rejects competing genesis credential flags"),
    };
    let (provider_connections, genesis) = match (args.genesis_provider, args.genesis_model) {
        (Some(provider), Some(model)) => {
            let name = ProviderConnectionName::parse(&format!("{}_default", provider.as_str()))
                .map_err(|_| command_error(BootstrapCommandError::IncompleteGenesis))?;
            let base_url = args
                .genesis_base_url
                .or_else(|| provider.default_base_url().map(str::to_owned));
            let connection = match provider {
                tribal_domain::ProviderKind::Ollama => ProviderConnectionInput::Ollama {
                    base_url: required_endpoint(provider, base_url)?,
                },
                tribal_domain::ProviderKind::Anthropic => ProviderConnectionInput::Anthropic {
                    base_url: required_endpoint(provider, base_url)?,
                    credential: credential.unwrap_or(CredentialMutation::Clear),
                },
                tribal_domain::ProviderKind::OpenAi => ProviderConnectionInput::OpenAi {
                    base_url: required_endpoint(provider, base_url)?,
                    credential: credential.unwrap_or(CredentialMutation::Clear),
                },
                tribal_domain::ProviderKind::Platform => ProviderConnectionInput::Platform {},
            };
            (
                vec![BootstrapProviderConnectionInput {
                    name: name.clone(),
                    connection,
                }],
                Some(BootstrapGenesisInput {
                    connection: name,
                    model,
                    dimensions: args.genesis_dimensions,
                }),
            )
        }
        (None, None) if credential.is_none() => (Vec::new(), None),
        (Some(_), None) | (None, _) => {
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
        provider_connections,
        genesis,
        telemetry,
        project,
        token,
        integration,
        json: args.json,
    })
}

fn required_endpoint(
    provider: tribal_domain::ProviderKind,
    base_url: Option<String>,
) -> Result<String, AppError> {
    base_url.ok_or_else(|| command_error(BootstrapCommandError::EndpointUnavailable { provider }))
}

fn credential_from_environment(variable: &str) -> Result<CredentialMutation, AppError> {
    let value = std::env::var(variable).map_err(|source| {
        command_error(BootstrapCommandError::CredentialEnvironment {
            variable: variable.to_owned(),
            source,
        })
    })?;
    credential_literal(value)
}

fn credential_from_stdin() -> Result<CredentialMutation, AppError> {
    let mut value = String::new();
    std::io::stdin()
        .read_to_string(&mut value)
        .map_err(|source| command_error(BootstrapCommandError::CredentialStdin { source }))?;
    credential_literal(value.trim_end_matches(['\r', '\n']).to_owned())
}

fn credential_literal(value: String) -> Result<CredentialMutation, AppError> {
    SecretLiteral::try_from(value)
        .map(|value| CredentialMutation::Replace { value })
        .map_err(|source| command_error(BootstrapCommandError::CredentialSecret { source }))
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
