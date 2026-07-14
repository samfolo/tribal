//! Headless projection of manager-owned database administration.

use std::{io::Write as _, path::Path};

use tribal_wire::management::{
    ConfigDocument, ConfigGetAllCall, ConfigRevision, DatabaseInitialiseCall,
    DatabaseInitialiseRequest,
};

use crate::{
    error::AppError,
    management::{
        authority::{AuthorityAcquire, AuthorityConflict, AuthorityError, AuthorityLease},
        client::{ManagementClient, ManagementClientError},
    },
};

/// Failure projecting a database operation through the manager.
#[derive(Debug, thiserror::Error)]
enum DatabaseCommandError {
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
    #[error("the managed configuration has no stable valid revision")]
    ConfigurationUnavailable,
    #[error("creating database command executor: {source}")]
    Runtime {
        #[source]
        source: std::io::Error,
    },
}

pub(crate) fn initialise(config_path: &str) -> Result<(), AppError> {
    let descriptor = match AuthorityLease::acquire(Path::new(config_path))
        .map_err(|source| command_error(DatabaseCommandError::Authority { source }))?
    {
        AuthorityAcquire::Occupied(AuthorityConflict::Manager(descriptor)) => descriptor,
        AuthorityAcquire::Acquired(lease) => {
            drop(lease);
            return Err(command_error(DatabaseCommandError::ManagerUnavailable));
        }
        AuthorityAcquire::Occupied(
            AuthorityConflict::StandaloneRuntime(_)
            | AuthorityConflict::OneShot(_)
            | AuthorityConflict::Recovering { .. },
        ) => return Err(command_error(DatabaseCommandError::ManagerUnavailable)),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| command_error(DatabaseCommandError::Runtime { source }))?;
    let result = runtime
        .block_on(async {
            let mut client = ManagementClient::connect(&descriptor)
                .await
                .map_err(|source| DatabaseCommandError::Client { source })?;
            let document = client
                .call::<ConfigGetAllCall>(&())
                .await
                .map_err(|source| DatabaseCommandError::Client { source })?;
            let expected_revision = stable_revision(document)?;
            client
                .call::<DatabaseInitialiseCall>(&DatabaseInitialiseRequest { expected_revision })
                .await
                .map_err(|source| DatabaseCommandError::Client { source })
        })
        .map_err(command_error)?;
    write_json(&result)
}

fn stable_revision(document: ConfigDocument) -> Result<ConfigRevision, DatabaseCommandError> {
    match document {
        ConfigDocument::DurableValid { revision, .. } => Ok(revision),
        ConfigDocument::DurableInvalid { .. }
        | ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => Err(DatabaseCommandError::ConfigurationUnavailable),
    }
}

fn write_json(value: &impl serde::Serialize) -> Result<(), AppError> {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    serde_json::to_writer(&mut writer, value).map_err(|source| AppError::Io {
        context: "writing database command result".to_owned(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    writer.write_all(b"\n").map_err(|source| AppError::Io {
        context: "writing database command result".to_owned(),
        source,
    })
}

fn command_error(source: DatabaseCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
