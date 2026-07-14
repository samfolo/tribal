//! Manager-backed readiness projection and private readiness evaluation.

use tribal_wire::management::{CheckReportCall, CredentialProbeCall};

use super::presentation;
use crate::{
    cli::CheckArgs,
    error::AppError,
    management::{
        client::ManagementClientError,
        connector::{ManagerConnector, ManagerConnectorError},
    },
};

/// Failure projecting readiness through the manager.
#[derive(Debug, thiserror::Error)]
enum CheckCommandError {
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
}

pub(crate) async fn run(config_path: &str, args: CheckArgs) -> Result<(), AppError> {
    let mut connection = ManagerConnector::new(config_path)
        .map_err(connector_error)?
        .connect()
        .await
        .map_err(connector_error)?;
    if args.providers {
        connection
            .call::<CredentialProbeCall>(&())
            .await
            .map_err(client_error)?;
    }
    let report = connection
        .call::<CheckReportCall>(&())
        .await
        .map_err(client_error)?;
    presentation::write(args.json, "Readiness", &report, "writing readiness report")
}

fn connector_error(source: ManagerConnectorError) -> AppError {
    command_error(CheckCommandError::Connector { source })
}

fn client_error(source: ManagementClientError) -> AppError {
    command_error(CheckCommandError::Client { source })
}

fn command_error(source: CheckCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
