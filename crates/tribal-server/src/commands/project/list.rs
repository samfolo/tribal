//! Core list flow: entry point and async orchestration.

use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{PgProjectRepository, ProjectRepository};

use super::output;
use crate::{
    cli::ProjectListArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
    },
    error::AppError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the list connection.
const POOL_NAME_LIST: &str = "list";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal project list` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection,
/// or the query fails.
pub(crate) fn run(config_path: &str, args: ProjectListArgs) -> Result<(), AppError> {
    let cli_overrides = args.database.into_cli_overrides();
    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config.database))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Fetches all projects and prints the table.
async fn run_async(db_config: &DatabaseConfig) -> Result<(), AppError> {
    let pool = tribal_db::create_pool(
        db_config,
        POOL_NAME_LIST,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| AppError::pool_acquire(POOL_NAME_LIST, "acquiring list connection", err))?;

    let projects = PgProjectRepository
        .list(&mut conn)
        .await
        .map_err(|source| AppError::Database { source })?;

    output::project_table(&projects);

    Ok(())
}
