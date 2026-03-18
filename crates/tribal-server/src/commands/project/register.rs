//! Core register flow: entry point and async orchestration.

use std::str::FromStr;

use tribal_config::load_config;
use tribal_db::{DbError, NewProject, PgProjectRepository, ProjectRepository};
use tribal_domain::GitRemote;

use super::output;
use crate::{
    cli::ProjectRegisterArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DEFAULT_DATABASE_URL,
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

/// Error when looking up an existing project after a duplicate insert.
const ERR_DUPLICATE_LOOKUP_FAILED: &str =
    "project insert reported duplicate but lookup returned nothing";

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

    let command_defaults = [("database.url", DEFAULT_DATABASE_URL)];
    let config = load_config(config_path, Some(cli_overrides), Some(&command_defaults))?;

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
    db_config: &tribal_config::DatabaseConfig,
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
        .settings(serde_json::Value::Object(serde_json::Map::new()))
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
    use super::*;

    #[test]
    fn test_resolve_git_remote_explicit_valid() {
        let result = resolve_git_remote(Some("git@github.com:user/repo.git"));
        assert!(result.is_ok());
        let remote = result.unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
    }

    #[test]
    fn test_resolve_git_remote_explicit_invalid() {
        let result = resolve_git_remote(Some(""));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("git remote detection failed"),
            "unexpected error: {err}",
        );
    }
}
