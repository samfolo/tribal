//! Typed projections of revisioned configuration calls.

use tribal_domain::ConfigFieldPathError;
use tribal_wire::management::{
    ConfigDocument, ConfigFieldPath, ConfigGetAllCall, ConfigGetCall, ConfigGetRequest,
    ConfigPathCall, ConfigRevision, ConfigSetCall, ConfigSetRequest, ConfigValidateCall,
    ConfigValidateRequest,
};

use super::presentation;
use crate::{
    cli::{ConfigGetArgs, ConfigSetArgs, ConfigShowArgs, ConfigValidateArgs, OutputArgs},
    error::AppError,
    management::{
        client::ManagementClientError,
        connector::{ManagerConnection, ManagerConnector, ManagerConnectorError},
    },
};

/// Failure projecting configuration operations through the manager.
#[derive(Debug, thiserror::Error)]
enum ConfigCommandError {
    #[error("invalid configuration field: {source}")]
    Field {
        #[source]
        source: ConfigFieldPathError,
    },
    #[error("establishing the management authority: {source}")]
    Connector {
        #[source]
        source: ManagerConnectorError,
    },
    #[error("calling the management authority: {source}")]
    Client {
        #[source]
        source: ManagementClientError,
    },
    #[error("the managed configuration has no stable revision")]
    UnstableRevision,
}

pub(crate) async fn show(config_path: &str, args: ConfigShowArgs) -> Result<(), AppError> {
    let mut connection = connect(config_path).await?;
    let document = connection
        .call::<ConfigGetAllCall>(&())
        .await
        .map_err(client_error)?;
    presentation::write(
        args.output.json,
        "Configuration",
        &document,
        "writing configuration",
    )
}

pub(crate) async fn get(config_path: &str, args: &ConfigGetArgs) -> Result<(), AppError> {
    let key = ConfigFieldPath::parse(&args.key).map_err(field_error)?;
    let mut connection = connect(config_path).await?;
    let value = connection
        .call::<ConfigGetCall>(&ConfigGetRequest { key })
        .await
        .map_err(client_error)?;
    presentation::write(
        args.output.json,
        "Configuration value",
        &value,
        "writing configuration value",
    )
}

pub(crate) async fn set(config_path: &str, args: &ConfigSetArgs) -> Result<(), AppError> {
    let key = ConfigFieldPath::parse(&args.key).map_err(field_error)?;
    let mut connection = connect(config_path).await?;
    let expected_revision = stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(client_error)?,
    )?;
    let outcome = connection
        .call::<ConfigSetCall>(&ConfigSetRequest {
            key,
            value: tribal_wire::management::ConfigLiteral::new(parse_value(&args.value)),
            expected_revision,
        })
        .await
        .map_err(client_error)?;
    presentation::write(
        args.output.json,
        "Configuration updated",
        &outcome,
        "writing configuration update",
    )
}

pub(crate) async fn validate(config_path: &str, args: &ConfigValidateArgs) -> Result<(), AppError> {
    let key = ConfigFieldPath::parse(&args.key).map_err(field_error)?;
    let mut connection = connect(config_path).await?;
    let validation = connection
        .call::<ConfigValidateCall>(&ConfigValidateRequest {
            key,
            value: parse_value(&args.value),
        })
        .await
        .map_err(client_error)?;
    presentation::write(
        args.output.json,
        "Configuration validation",
        &validation,
        "writing configuration validation",
    )
}

pub(crate) async fn path(config_path: &str, output: OutputArgs) -> Result<(), AppError> {
    let mut connection = connect(config_path).await?;
    let path = connection
        .call::<ConfigPathCall>(&())
        .await
        .map_err(client_error)?;
    presentation::write(
        output.json,
        "Configuration path",
        &path,
        "writing configuration path",
    )
}

pub(crate) async fn connect(config_path: &str) -> Result<ManagerConnection, AppError> {
    ManagerConnector::new(config_path)
        .map_err(connector_error)?
        .connect()
        .await
        .map_err(connector_error)
}

pub(crate) fn stable_revision(document: ConfigDocument) -> Result<ConfigRevision, AppError> {
    match document {
        ConfigDocument::DurableValid { revision, .. }
        | ConfigDocument::DurableInvalid { revision } => Ok(revision),
        ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => {
            Err(command_error(ConfigCommandError::UnstableRevision))
        }
    }
}

fn parse_value(raw: &str) -> serde_json::Value {
    serde_json::from_str(raw).unwrap_or_else(|_| serde_json::Value::String(raw.to_owned()))
}

fn field_error(source: ConfigFieldPathError) -> AppError {
    command_error(ConfigCommandError::Field { source })
}

fn connector_error(source: ManagerConnectorError) -> AppError {
    command_error(ConfigCommandError::Connector { source })
}

pub(crate) fn client_error(source: ManagementClientError) -> AppError {
    command_error(ConfigCommandError::Client { source })
}

fn command_error(source: ConfigCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
