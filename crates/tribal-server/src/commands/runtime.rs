//! Typed projection of manager-owned runtime lifecycle calls.

use tribal_wire::management::{
    ManagerSnapshotCall, RuntimeRestartCall, RuntimeStartCall, RuntimeStopCall,
};

use super::presentation;
use crate::{
    cli::RuntimeCommand,
    error::AppError,
    management::{
        client::ManagementClientError,
        connector::{ManagerConnector, ManagerConnectorError},
    },
};

/// Failure projecting a runtime operation through the manager.
#[derive(Debug, thiserror::Error)]
enum RuntimeCommandError {
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

pub(crate) async fn run(config_path: &str, command: &RuntimeCommand) -> Result<(), AppError> {
    let mut connection = ManagerConnector::new(config_path)
        .map_err(connector_error)?
        .connect()
        .await
        .map_err(connector_error)?;

    match command {
        RuntimeCommand::Start { output } => {
            let result = connection
                .call::<RuntimeStartCall>(&())
                .await
                .map_err(client_error)?;
            presentation::write(
                output.json,
                "Runtime start",
                &result,
                "writing runtime start result",
            )
        }
        RuntimeCommand::Stop { output } => {
            let result = connection
                .call::<RuntimeStopCall>(&())
                .await
                .map_err(client_error)?;
            presentation::write(
                output.json,
                "Runtime stop",
                &result,
                "writing runtime stop result",
            )
        }
        RuntimeCommand::Restart { output } => {
            let result = connection
                .call::<RuntimeRestartCall>(&())
                .await
                .map_err(client_error)?;
            presentation::write(
                output.json,
                "Runtime restart",
                &result,
                "writing runtime restart result",
            )
        }
        RuntimeCommand::Status { output } => {
            let result = connection
                .call::<ManagerSnapshotCall>(&())
                .await
                .map_err(client_error)?;
            presentation::write(
                output.json,
                "Runtime status",
                &result,
                "writing runtime status",
            )
        }
    }
}

fn connector_error(source: ManagerConnectorError) -> AppError {
    AppError::Management {
        source: Box::new(RuntimeCommandError::Connector { source }),
    }
}

fn client_error(source: ManagementClientError) -> AppError {
    AppError::Management {
        source: Box::new(RuntimeCommandError::Client { source }),
    }
}
