//! Typed projection of thread-retention administration.

use tribal_wire::management::{
    ConfigGetAllCall, MutationMode, RetentionDays, RetentionDaysError, ThreadPruneRequest,
    ThreadsPruneCall,
};

use super::{config, presentation};
use crate::{cli::ThreadsPruneArgs, error::AppError};

#[derive(Debug, thiserror::Error)]
#[error("invalid thread retention: {source}")]
struct ThreadCommandError {
    #[source]
    source: RetentionDaysError,
}

pub(crate) async fn prune(config_path: &str, args: ThreadsPruneArgs) -> Result<(), AppError> {
    let older_than =
        RetentionDays::try_from(args.older_than_days).map_err(|source| AppError::Management {
            source: Box::new(ThreadCommandError { source }),
        })?;
    let mut connection = config::connect(config_path).await?;
    let expected_revision = config::stable_revision(
        connection
            .call::<ConfigGetAllCall>(&())
            .await
            .map_err(config::client_error)?,
    )?;
    let result = connection
        .call::<ThreadsPruneCall>(&ThreadPruneRequest {
            expected_revision,
            older_than,
            stage: args.stage,
            cascade: args.cascade,
            mode: if args.apply {
                MutationMode::Apply
            } else {
                MutationMode::Preview
            },
        })
        .await
        .map_err(config::client_error)?;
    presentation::write(
        args.output.json,
        "Thread retention",
        &result,
        "writing thread retention",
    )
}
