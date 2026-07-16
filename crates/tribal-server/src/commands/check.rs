//! Manager-backed readiness projection and private readiness evaluation.

use tribal_wire::{
    management::{
        CheckReportCall, ProviderConnectionsCall, ProviderProbeCall, ProviderProbeRequest,
        ProviderProbeTarget,
    },
    operator_check::CheckResult,
};

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
        let catalogue = connection
            .call::<ProviderConnectionsCall>(&())
            .await
            .map_err(client_error)?;
        for provider in catalogue.connections {
            connection
                .call::<ProviderProbeCall>(&ProviderProbeRequest {
                    target: ProviderProbeTarget::Stored {
                        name: provider.name,
                    },
                    expected_revision: catalogue.revision.clone(),
                })
                .await
                .map_err(client_error)?;
        }
    }
    let report = connection
        .call::<CheckReportCall>(&())
        .await
        .map_err(client_error)?;
    presentation::write(args.json, "Readiness", &report, "writing readiness report")?;
    if report
        .checks
        .iter()
        .any(|observation| matches!(observation.result, CheckResult::Fail { .. }))
    {
        Err(AppError::CheckFailed)
    } else {
        Ok(())
    }
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
