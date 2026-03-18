//! Core list flow: entry point and async orchestration.

use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{PgProjectRepository, ProjectRepository};
use tribal_domain::Project;

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
    let cli_overrides = args.into_cli_overrides();
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

/// Fetches all projects from the database and prints the table.
async fn run_async(db_config: &DatabaseConfig) -> Result<(), AppError> {
    let projects = fetch_projects(db_config).await?;
    output::project_table(&projects);
    Ok(())
}

/// Fetches all projects from the database.
async fn fetch_projects(db_config: &DatabaseConfig) -> Result<Vec<Project>, AppError> {
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

    PgProjectRepository
        .list(&mut conn)
        .await
        .map_err(|source| AppError::Database { source })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::GitRemote;
    use tribal_test_utils::{a_new_project, serial_lock, test_context, truncate_all_tables};

    use super::*;
    use crate::commands::common::test_db_config;

    async fn teardown(ctx: &tribal_test_utils::TestContext) {
        let mut conn = ctx.raw_connection().await.expect("raw_connection");
        truncate_all_tables(&mut conn).await;
    }

    #[tokio::test]
    async fn test_fetch_projects_empty() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;

        let db_config = test_db_config(ctx.database_url());
        let projects = fetch_projects(&db_config).await.expect("fetch_projects");

        assert!(projects.is_empty());

        teardown(ctx).await;
    }

    #[tokio::test]
    async fn test_fetch_projects_returns_inserted() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;

        {
            let mut conn = ctx.raw_connection().await.expect("raw_connection");
            PgProjectRepository
                .insert(
                    &mut conn,
                    &a_new_project()
                        .git_remote(GitRemote::from_parts("github.com", "integ/list-a", None))
                        .name("list-a".to_owned())
                        .build(),
                )
                .await
                .expect("insert first");
            PgProjectRepository
                .insert(
                    &mut conn,
                    &a_new_project()
                        .git_remote(GitRemote::from_parts("github.com", "integ/list-b", None))
                        .name("list-b".to_owned())
                        .build(),
                )
                .await
                .expect("insert second");
        }

        let db_config = test_db_config(ctx.database_url());
        let projects = fetch_projects(&db_config).await.expect("fetch_projects");

        assert_eq!(projects.len(), 2);
        let names: Vec<&str> = projects.iter().map(Project::name).collect();
        assert!(names.contains(&"list-a"));
        assert!(names.contains(&"list-b"));

        teardown(ctx).await;
    }
}
