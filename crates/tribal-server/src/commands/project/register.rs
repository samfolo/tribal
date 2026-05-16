//! Core register flow: entry point and async orchestration.

use std::{path::Path, str::FromStr, sync::Arc};

use tribal_config::{Auth, ENV_AUTH_TOKEN, TransportKind, TribalConfig, load_config};
use tribal_db::{
    DbError, NewProject, PgAuthTokenRepository, PgPrincipalRepository, PgProjectRepository,
    ProjectRepository,
};
use tribal_domain::{BearerToken, GitRemote};
use tribal_mcp::Authenticator;

use super::output;
use crate::{
    cli::ProjectRegisterArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
        PROJECT_SCHEMA_VERSION, resolve_absolute_config_path,
    },
    error::AppError,
    git::detect_git_remote,
    output::resolved_advertised_url,
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
/// connection, token validation, or insertion fails.
pub(crate) fn run(config_path: &str, args: ProjectRegisterArgs) -> Result<(), AppError> {
    let ProjectRegisterArgs {
        remote,
        name,
        branch,
        json,
        transport,
        token,
        skip_validation,
        database,
    } = args;

    let cli_overrides = database.into_cli_overrides();
    let git_remote = resolve_git_remote(remote.as_deref())?;

    if !json {
        output::git_remote_resolved(git_remote.as_str());
    }

    let name = name.unwrap_or_else(|| git_remote.path().to_owned());
    let branch = branch.unwrap_or_else(|| DEFAULT_BRANCH.to_owned());
    let transport = transport.unwrap_or_default();

    // Auth is only relevant for HTTP/SSE snippets — stdio never
    // embeds bearer auth, so stale env vars cannot break the default
    // workflow.
    let auth = if matches!(transport, TransportKind::Http | TransportKind::Sse) {
        let resolved = resolve_auth(token)?;
        if resolved.is_none() && !skip_validation {
            return Err(AppError::TokenOperation {
                reason: output::TOKEN_REQUIRED.to_owned(),
            });
        }
        resolved
    } else {
        None
    };

    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;
    let absolute_config_path = resolve_absolute_config_path(config_path)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let opts = OutputOptions {
        json,
        transport,
        auth: auth.as_ref(),
        skip_validation,
        config_path: &absolute_config_path,
    };

    rt.block_on(run_async(&config, &git_remote, &name, &branch, &opts))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Output options resolved from CLI flags before entering the async
/// flow.
struct OutputOptions<'a> {
    json: bool,
    transport: TransportKind,
    auth: Option<&'a Auth>,
    skip_validation: bool,
    config_path: &'a Path,
}

/// Connects to the database, inserts (or finds) the project, optionally
/// validates the token, and prints the result.
async fn run_async(
    config: &TribalConfig,
    git_remote: &GitRemote,
    name: &str,
    branch: &str,
    opts: &OutputOptions<'_>,
) -> Result<(), AppError> {
    let pool = tribal_db::create_pool(
        &config.database,
        POOL_NAME_REGISTER,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    let mut conn = pool.acquire().await.map_err(|err| {
        AppError::pool_acquire(POOL_NAME_REGISTER, "acquiring register connection", err)
    })?;

    // -- Validate auth if provided and validation not skipped -----------------

    if let Some(auth) = opts.auth
        && !opts.skip_validation
    {
        let authenticator = Authenticator::new(
            Arc::new(PgAuthTokenRepository),
            Arc::new(PgPrincipalRepository),
        );
        match auth {
            Auth::Bearer { token } => {
                authenticator
                    .verify_token(&mut conn, token.as_str())
                    .await
                    .map_err(|err| AppError::TokenVerification {
                        reason: output::TOKEN_INVALID.to_owned(),
                        source: Box::new(err),
                    })?;
            }
        }
    }

    // -- Register project ---------------------------------------------------

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

    // -- Output -------------------------------------------------------------

    let advertised_url = resolved_advertised_url(config);

    if opts.json {
        output::json_snippet(
            &project,
            opts.transport,
            opts.auth,
            opts.config_path,
            &advertised_url,
        );
    } else {
        output::registered(&project, already_existed);
        output::project_id(&project);
        output::mcp_snippet(
            &project,
            opts.transport,
            opts.auth,
            opts.config_path,
            &advertised_url,
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the bearer auth value from an explicit `--token` flag or
/// the `TRIBAL_AUTH_TOKEN` environment variable.
///
/// # Errors
///
/// Returns [`AppError::TokenVerification`] when the resolved string is
/// not a well-formed [`BearerToken`] (empty or contains whitespace).
fn resolve_auth(explicit: Option<String>) -> Result<Option<Auth>, AppError> {
    let Some(raw) = explicit.or_else(|| std::env::var(ENV_AUTH_TOKEN).ok()) else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let token: BearerToken = trimmed
        .parse()
        .map_err(|source| AppError::TokenVerification {
            reason: "bearer token from --token or TRIBAL_AUTH_TOKEN is invalid".into(),
            source: Box::new(source),
        })?;
    Ok(Some(Auth::Bearer { token }))
}

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
        let remote = resolve_git_remote(Some("git@github.com:user/repo.git")).unwrap();
        assert_eq!(remote.as_str(), "github.com/user/repo");
        assert_eq!(remote.path(), "user/repo");
    }

    #[test]
    fn test_resolve_git_remote_explicit_invalid() {
        let err = resolve_git_remote(Some("")).unwrap_err();
        assert!(
            matches!(err, AppError::GitDetection { .. }),
            "expected GitDetection, got: {err}",
        );
    }
}
