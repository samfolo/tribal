//! Typed projections of project administration.

use std::path::PathBuf;

use tribal_wire::management::{
    AbsoluteDirectoryPath, AbsoluteDirectoryPathError, ConfigGetAllCall, PageCursor,
    PageCursorError, PageRequest, PageSize, PageSizeError, ProjectListCall, ProjectListRequest,
    ProjectRegisterCall, ProjectRegisterInput, ProjectRegisterRequest, ProjectRegistrationSource,
};

use super::{config, presentation};
use crate::{
    cli::{ProjectListArgs, ProjectRegisterArgs},
    error::AppError,
};

/// Failure validating a project projection request.
#[derive(Debug, thiserror::Error)]
enum ProjectCommandError {
    #[error("resolving the current directory: {source}")]
    CurrentDirectory {
        #[source]
        source: std::io::Error,
    },
    #[error("invalid project directory: {source}")]
    Directory {
        #[source]
        source: AbsoluteDirectoryPathError,
    },
    #[error("invalid page size: {source}")]
    PageSize {
        #[source]
        source: PageSizeError,
    },
    #[error("invalid page cursor: {source}")]
    Cursor {
        #[source]
        source: PageCursorError,
    },
}

pub(crate) async fn register(config_path: &str, args: ProjectRegisterArgs) -> Result<(), AppError> {
    let directory = absolute_directory(args.path)?;
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let result = connection
        .call::<ProjectRegisterCall>(&ProjectRegisterRequest {
            expected_revision,
            project: ProjectRegisterInput {
                source: ProjectRegistrationSource::WorkingTree { directory },
                name: args.name,
                default_branch: args.branch,
            },
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Project registration",
        &result,
        "writing project registration",
    )
}

pub(crate) async fn list(config_path: &str, args: ProjectListArgs) -> Result<(), AppError> {
    let page = PageRequest {
        size: PageSize::try_from(args.page_size)
            .map_err(|source| command_error(ProjectCommandError::PageSize { source }))?,
        after: args
            .after
            .map(PageCursor::try_from)
            .transpose()
            .map_err(|source| command_error(ProjectCommandError::Cursor { source }))?,
    };
    let mut connection = config::connect(config_path).await?;
    let result = connection
        .call::<ProjectListCall>(&ProjectListRequest { page })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Projects",
        &result,
        "writing project inventory",
    )
}

fn absolute_directory(raw: Option<String>) -> Result<AbsoluteDirectoryPath, AppError> {
    let path = match raw {
        Some(raw) => {
            let path = PathBuf::from(raw);
            if path.is_absolute() {
                path
            } else {
                std::env::current_dir()
                    .map_err(|source| {
                        command_error(ProjectCommandError::CurrentDirectory { source })
                    })?
                    .join(path)
            }
        }
        None => std::env::current_dir()
            .map_err(|source| command_error(ProjectCommandError::CurrentDirectory { source }))?,
    };
    AbsoluteDirectoryPath::try_from(path.to_string_lossy().into_owned())
        .map_err(|source| command_error(ProjectCommandError::Directory { source }))
}

fn command_error(source: ProjectCommandError) -> AppError {
    AppError::Management {
        source: Box::new(source),
    }
}
