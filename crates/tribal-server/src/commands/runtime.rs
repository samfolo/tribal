//! Headless projection of the manager-owned runtime lifecycle.

use std::{io::Write as _, path::Path};

use tribal_wire::management::{
    LifecycleSnapshot, RuntimeRestartResult, RuntimeStartResult, RuntimeStopResult,
};

use crate::{
    error::AppError,
    management::{
        authority::{AuthorityAcquire, AuthorityConflict, AuthorityError, AuthorityLease},
        client::{ManagementClient, ManagementClientError},
    },
};

/// Failure locating the manager required for a runtime command.
#[derive(Debug, thiserror::Error)]
enum RuntimeCommandError {
    #[error("no manager owns the requested configuration path")]
    ManagerUnavailable,
    #[error("configuration authority failed: {source}")]
    Authority {
        #[source]
        source: AuthorityError,
    },
    #[error("management client failed: {source}")]
    Client {
        #[source]
        source: ManagementClientError,
    },
    #[error("creating runtime command executor: {source}")]
    Runtime {
        #[source]
        source: std::io::Error,
    },
}

pub(crate) fn run(config_path: &str, method: &str) -> Result<(), AppError> {
    let descriptor = match AuthorityLease::acquire(Path::new(config_path))
        .map_err(|source| command_error(RuntimeCommandError::Authority { source }))?
    {
        AuthorityAcquire::Occupied(AuthorityConflict::Manager(descriptor)) => descriptor,
        AuthorityAcquire::Acquired(lease) => {
            drop(lease);
            return Err(command_error(RuntimeCommandError::ManagerUnavailable));
        }
        AuthorityAcquire::Occupied(
            AuthorityConflict::StandaloneRuntime(_)
            | AuthorityConflict::OneShot(_)
            | AuthorityConflict::Recovering { .. },
        ) => return Err(command_error(RuntimeCommandError::ManagerUnavailable)),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| command_error(RuntimeCommandError::Runtime { source }))?;
    let value = runtime
        .block_on(async {
            let mut client = ManagementClient::connect(&descriptor)
                .await
                .map_err(|source| RuntimeCommandError::Client { source })?;
            match method {
                "runtime.start" => call_value::<RuntimeStartResult>(&mut client, method).await,
                "runtime.stop" => call_value::<RuntimeStopResult>(&mut client, method).await,
                "runtime.restart" => call_value::<RuntimeRestartResult>(&mut client, method).await,
                "manager.snapshot" => call_value::<LifecycleSnapshot>(&mut client, method).await,
                _ => Err(RuntimeCommandError::ManagerUnavailable),
            }
        })
        .map_err(command_error)?;
    write_json(&value)
}

async fn call_value<T: serde::de::DeserializeOwned + serde::Serialize>(
    client: &mut ManagementClient,
    method: &str,
) -> Result<serde_json::Value, RuntimeCommandError> {
    let result: T = client
        .call(method, None)
        .await
        .map_err(|source| RuntimeCommandError::Client { source })?;
    serde_json::to_value(result).map_err(|source| RuntimeCommandError::Client {
        source: ManagementClientError::Frame {
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
        },
    })
}

fn write_json(value: &serde_json::Value) -> Result<(), AppError> {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    serde_json::to_writer(&mut writer, value).map_err(|source| AppError::Io {
        context: "writing runtime command result".to_owned(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    writer.write_all(b"\n").map_err(|source| AppError::Io {
        context: "writing runtime command result".to_owned(),
        source,
    })
}

fn command_error(source: RuntimeCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
