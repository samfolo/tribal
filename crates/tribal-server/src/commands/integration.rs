//! Typed projection of integration rendering.

use tribal_wire::management::{
    AbsoluteDirectoryPath, ConfigGetAllCall, ConfiguredMcpTarget, IntegrationMcpConfigCall,
    McpConfigRequest, McpTarget, McpTargetSelection, NetworkIntegrationAuth, ProjectSelector,
    StdioProjectContext, TransportKind,
};

use super::{config, presentation};
use crate::{
    cli::{IntegrationAuthArg, IntegrationMcpConfigArgs},
    error::AppError,
};

/// Failure constructing a manager-owned integration request.
#[derive(Debug, thiserror::Error)]
enum IntegrationCommandError {
    #[error("resolving the current directory: {source}")]
    CurrentDirectory {
        #[source]
        source: std::io::Error,
    },
    #[error("the current directory is not an absolute path")]
    RelativeCurrentDirectory,
}

pub(crate) async fn mcp_config(
    config_path: &str,
    args: IntegrationMcpConfigArgs,
) -> Result<(), AppError> {
    let stdio_context = stdio_context(args.unscoped, args.project)?;
    let target = match args.transport {
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
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let result = connection
        .call::<IntegrationMcpConfigCall>(&McpConfigRequest {
            expected_revision,
            target,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "MCP configuration",
        &result,
        "writing MCP configuration",
    )
}

fn stdio_context(
    unscoped: bool,
    project: Option<tribal_domain::ProjectId>,
) -> Result<StdioProjectContext, AppError> {
    if unscoped {
        return Ok(StdioProjectContext::Unscoped);
    }
    if let Some(id) = project {
        return Ok(StdioProjectContext::Project {
            selector: ProjectSelector::Id { id },
        });
    }
    let directory = std::env::current_dir()
        .map_err(|source| command_error(IntegrationCommandError::CurrentDirectory { source }))?;
    let directory = AbsoluteDirectoryPath::try_from(directory.to_string_lossy().into_owned())
        .map_err(|_| command_error(IntegrationCommandError::RelativeCurrentDirectory))?;
    Ok(StdioProjectContext::Project {
        selector: ProjectSelector::WorkingTree { directory },
    })
}

fn network_auth(auth: IntegrationAuthArg) -> NetworkIntegrationAuth {
    match auth {
        IntegrationAuthArg::OAuth => NetworkIntegrationAuth::OAuth,
        IntegrationAuthArg::PersistedBearer => NetworkIntegrationAuth::ExportPersistedBearer,
    }
}

fn command_error(source: IntegrationCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
