//! Core register flow: entry point and async orchestration.

use std::str::FromStr;

use tribal_config::{DatabaseConfig, load_config};
use tribal_db::{DbError, NewProject, PgProjectRepository, ProjectRepository};
use tribal_domain::GitRemote;

use super::output;
use crate::{
    cli::ProjectRegisterArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
        PROJECT_SCHEMA_VERSION,
    },
    error::AppError,
    git::detect_git_remote,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default branch name used when `--branch` is not provided.
const DEFAULT_BRANCH: &str = "main";

/// Pool name for the register connection.
const POOL_NAME_REGISTER: &str = "register";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal project register` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if git detection, config loading, database
/// connection, or insertion fails.
pub(crate) fn run(config_path: &str, args: ProjectRegisterArgs) -> Result<(), AppError> {
    let ProjectRegisterArgs {
        remote,
        name,
        branch,
        database,
    } = args;

    let cli_overrides = database.into_cli_overrides();
    let git_remote = resolve_git_remote(remote.as_deref())?;
    output::git_remote_resolved(git_remote.as_str());

    let name = name.unwrap_or_else(|| git_remote.path().to_owned());
    let branch = branch.unwrap_or_else(|| DEFAULT_BRANCH.to_owned());

    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    rt.block_on(run_async(&config.database, &git_remote, &name, &branch))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Connects to the database, inserts (or finds) the project, and prints
/// the result.
async fn run_async(
    db_config: &DatabaseConfig,
    git_remote: &GitRemote,
    name: &str,
    branch: &str,
) -> Result<(), AppError> {
    let pool = tribal_db::create_pool(
        db_config,
        POOL_NAME_REGISTER,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    let mut conn = pool.acquire().await.map_err(|err| {
        AppError::pool_acquire(POOL_NAME_REGISTER, "acquiring register connection", err)
    })?;

    let new_project = NewProject::builder()
        .git_remote(git_remote.clone())
        .name(name.to_owned())
        .default_branch(branch.to_owned())
        .schema_version(PROJECT_SCHEMA_VERSION)
        .settings(serde_json::json!({}))
        .build();

    let (project, already_existed) = match PgProjectRepository.insert(&mut conn, &new_project).await
    {
        Ok(project) => (project, false),
        Err(DbError::UniqueViolation { .. }) => {
            let existing = PgProjectRepository
                .find_by_git_remote(&mut conn, git_remote)
                .await
                .map_err(|source| AppError::Database { source })?
                .ok_or_else(|| AppError::Database {
                    source: DbError::NotFound {
                        entity: "project",
                        id: git_remote.to_string(),
                    },
                })?;
            (existing, true)
        }
        Err(source) => return Err(AppError::Database { source }),
    };

    output::registered(&project, already_existed);
    output::project_id(&project);
    output::mcp_snippet(&project);

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the git remote from an explicit `--remote` flag or by
/// detecting from the current working directory.
///
/// Both branches produce consistent [`AppError::GitDetection`] errors.
fn resolve_git_remote(explicit: Option<&str>) -> Result<GitRemote, AppError> {
    match explicit {
        Some(url) => GitRemote::from_str(url).map_err(|e| AppError::GitDetection {
            reason: e.to_string(),
        }),
        None => detect_git_remote(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_test_utils::{serial_lock, test_context, truncate_all_tables};

    use super::*;

    // -- resolve_git_remote --------------------------------------------------

    #[test]
    fn test_resolve_git_remote_explicit_valid() {
        let remote = resolve_git_remote(Some("git@github.com:user/repo.git")).unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
        assert_eq!(remote.path(), "user/repo");
    }

    #[test]
    fn test_resolve_git_remote_explicit_invalid() {
        let err = resolve_git_remote(Some("")).unwrap_err();
        assert!(
            err.to_string().contains("git remote detection failed"),
            "unexpected error: {err}",
        );
    }

    // -- run_async -----------------------------------------------------------

    fn test_db_config(url: &str) -> DatabaseConfig {
        serde_json::from_value(serde_json::json!({ "url": url })).expect("valid DatabaseConfig")
    }

    async fn teardown(ctx: &tribal_test_utils::TestContext) {
        let mut conn = ctx.raw_connection().await.expect("raw_connection");
        truncate_all_tables(&mut conn).await;
    }

    #[tokio::test]
    async fn test_register_inserts_project() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;

        let db_config = test_db_config(ctx.database_url());
        let remote = GitRemote::from_parts("github.com", "integ/register-happy", None);

        run_async(&db_config, &remote, "register-happy", "main")
            .await
            .expect("run_async");

        let mut conn = ctx.raw_connection().await.expect("raw_connection");
        let project = PgProjectRepository
            .find_by_git_remote(&mut conn, &remote)
            .await
            .expect("find_by_git_remote")
            .expect("project should exist after registration");

        assert_eq!(project.name(), "register-happy");
        assert_eq!(project.default_branch(), "main");
        assert_eq!(
            project.git_remote().as_str(),
            "github.com/integ/register-happy",
        );
        assert_eq!(project.schema_version(), PROJECT_SCHEMA_VERSION);
        assert_eq!(project.settings(), &serde_json::json!({}));

        teardown(ctx).await;
    }

    #[tokio::test]
    async fn test_register_duplicate_returns_existing() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;

        let db_config = test_db_config(ctx.database_url());
        let remote = GitRemote::from_parts("github.com", "integ/register-dup", None);

        run_async(&db_config, &remote, "first-name", "main")
            .await
            .expect("first registration");

        let original_id = {
            let mut conn = ctx.raw_connection().await.expect("raw_connection");
            PgProjectRepository
                .find_by_git_remote(&mut conn, &remote)
                .await
                .expect("find_by_git_remote")
                .expect("project should exist")
                .id()
        };

        // Second registration with the same remote succeeds without error.
        run_async(&db_config, &remote, "different-name", "develop")
            .await
            .expect("duplicate registration");

        // The project in the database is unchanged — the first registration wins.
        let mut conn = ctx.raw_connection().await.expect("raw_connection");
        let after = PgProjectRepository
            .find_by_git_remote(&mut conn, &remote)
            .await
            .expect("find_by_git_remote")
            .expect("project should still exist");

        assert_eq!(after.id(), original_id);
        assert_eq!(after.name(), "first-name");
        assert_eq!(after.default_branch(), "main");

        teardown(ctx).await;
    }
}
