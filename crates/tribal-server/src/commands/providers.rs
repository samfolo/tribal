//! Typed provider-connection command projections.

use std::io::Read as _;

use tribal_domain::{ProviderConnectionName, ProviderKind};
use tribal_wire::management::{
    ConfigGetAllCall, CredentialMutation, ProviderConnectionInput, ProviderConnectionRemoveCall,
    ProviderConnectionRemoveRequest, ProviderConnectionUpsertCall, ProviderConnectionUpsertRequest,
    ProviderConnectionsCall, ProviderProbeCall, ProviderProbeRequest, ProviderProbeTarget,
    SecretLiteral,
};

use super::{config, presentation};
use crate::{
    cli::{OutputArgs, ProviderUpsertArgs},
    error::AppError,
};

#[derive(Debug, thiserror::Error)]
enum ProviderCommandError {
    #[error("provider '{provider}' has no configurable endpoint")]
    EndpointUnavailable { provider: ProviderKind },
    #[error("provider '{provider}' does not accept an API key")]
    CredentialUnavailable { provider: ProviderKind },
    #[error("reading API key environment variable '{variable}': {source}")]
    CredentialEnvironment {
        variable: String,
        #[source]
        source: std::env::VarError,
    },
    #[error("reading API key from stdin: {source}")]
    CredentialStdin {
        #[source]
        source: std::io::Error,
    },
    #[error("invalid API key: {source}")]
    CredentialSecret {
        #[source]
        source: tribal_wire::management::SecretLiteralError,
    },
}

pub(crate) async fn list(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let catalogue = connection
        .call::<ProviderConnectionsCall>(&())
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Provider connections",
        &catalogue,
        "writing provider connections",
    )
}

pub(crate) async fn upsert(config_path: &str, args: ProviderUpsertArgs) -> Result<(), AppError> {
    let input = provider_input(&args)?;
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let receipt = connection
        .call::<ProviderConnectionUpsertCall>(&ProviderConnectionUpsertRequest {
            name: args.name,
            connection: input,
            expected_revision,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Provider connection updated",
        &receipt,
        "writing provider update",
    )
}

pub(crate) async fn remove(
    config_path: &str,
    name: ProviderConnectionName,
    output: OutputArgs,
) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let receipt = connection
        .call::<ProviderConnectionRemoveCall>(&ProviderConnectionRemoveRequest {
            name,
            expected_revision,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Provider connection removed",
        &receipt,
        "writing provider removal",
    )
}

pub(crate) async fn probe(
    config_path: &str,
    name: ProviderConnectionName,
    output: OutputArgs,
) -> Result<(), AppError> {
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let response = connection
        .call::<ProviderProbeCall>(&ProviderProbeRequest {
            target: ProviderProbeTarget::Stored { name },
            expected_revision,
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        output.json,
        "Provider connection probe",
        &response,
        "writing provider probe",
    )
}

fn provider_input(args: &ProviderUpsertArgs) -> Result<ProviderConnectionInput, AppError> {
    let credential = credential_mutation(args)?;
    let endpoint = || {
        args.base_url.clone().ok_or_else(|| {
            command_error(ProviderCommandError::EndpointUnavailable {
                provider: args.provider,
            })
        })
    };
    match args.provider {
        ProviderKind::Ollama | ProviderKind::Platform
            if !matches!(&credential, CredentialMutation::Preserve) =>
        {
            Err(command_error(ProviderCommandError::CredentialUnavailable {
                provider: args.provider,
            }))
        }
        ProviderKind::Ollama => Ok(ProviderConnectionInput::Ollama {
            base_url: endpoint()?,
        }),
        ProviderKind::Anthropic => Ok(ProviderConnectionInput::Anthropic {
            base_url: endpoint()?,
            credential,
        }),
        ProviderKind::OpenAi => Ok(ProviderConnectionInput::OpenAi {
            base_url: endpoint()?,
            credential,
        }),
        ProviderKind::Platform => Ok(ProviderConnectionInput::Platform {}),
    }
}

fn credential_mutation(args: &ProviderUpsertArgs) -> Result<CredentialMutation, AppError> {
    if args.clear_api_key {
        return Ok(CredentialMutation::Clear);
    }
    let value = match (&args.api_key_env, args.api_key_stdin) {
        (Some(variable), false) => Some(std::env::var(variable).map_err(|source| {
            command_error(ProviderCommandError::CredentialEnvironment {
                variable: variable.clone(),
                source,
            })
        })?),
        (None, true) => {
            let mut value = String::new();
            std::io::stdin()
                .read_to_string(&mut value)
                .map_err(|source| {
                    command_error(ProviderCommandError::CredentialStdin { source })
                })?;
            Some(value.trim_end_matches(['\r', '\n']).to_owned())
        }
        (None, false) => None,
        (Some(_), true) => unreachable!("clap rejects competing API-key sources"),
    };
    value.map_or(Ok(CredentialMutation::Preserve), |value| {
        SecretLiteral::try_from(value)
            .map(|value| CredentialMutation::Replace { value })
            .map_err(|source| command_error(ProviderCommandError::CredentialSecret { source }))
    })
}

fn command_error(source: ProviderCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
