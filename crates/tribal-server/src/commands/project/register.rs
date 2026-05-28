//! Core register flow: entry point and async orchestration.

use std::{
    io::{self, Write},
    path::Path,
    str::FromStr,
    sync::Arc,
};

use tribal_auth::Authenticator;
use tribal_config::{Auth, ENV_AUTH_TOKEN, TransportKind, TribalConfig, load_config};
use tribal_db::{
    DbError, NewProject, PgAuthTokenRepository, PgPrincipalRepository, PgProjectRepository,
    ProjectRepository,
};
use tribal_domain::{BearerToken, GitRemote};

use super::{
    outcome::{RegisterOutcome, RegisteredProject},
    output,
};
use crate::{
    cli::ProjectRegisterArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
        PROJECT_SCHEMA_VERSION, resolve_absolute_config_path,
    },
    error::AppError,
    git::detect_git_remote,
    output::{build_snippet_entry, resolved_advertised_url},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default branch name used when `--branch` is not provided.
pub(crate) const DEFAULT_BRANCH: &str = "main";

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

    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();

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

    let _outcome = rt.block_on(run_async(
        &config,
        &git_remote,
        &name,
        &branch,
        &opts,
        &mut stdout,
        &mut stderr,
    ))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Output options resolved from CLI flags before entering the async
/// flow.
pub(crate) struct OutputOptions<'a> {
    pub(crate) json: bool,
    pub(crate) transport: TransportKind,
    pub(crate) auth: Option<&'a Auth>,
    pub(crate) skip_validation: bool,
    pub(crate) config_path: &'a Path,
}

/// Connects to the database, inserts (or finds) the project, optionally
/// validates the token, and returns the [`RegisterOutcome`].
///
/// Pure with respect to user-facing output: bootstrap reuses this entry
/// point to assemble its own polished hand-off without paying for
/// register-side print work that would only get discarded.
pub(crate) async fn compute(
    config: &TribalConfig,
    git_remote: &GitRemote,
    name: &str,
    branch: &str,
    opts: &OutputOptions<'_>,
) -> Result<RegisterOutcome, AppError> {
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

    let advertised_url = resolved_advertised_url(config);
    let mcp_config = build_snippet_entry(
        project.id(),
        opts.transport,
        opts.auth,
        opts.config_path,
        &advertised_url,
    );

    let registered = RegisteredProject {
        project_id: project.id(),
        project_name: project.name().to_owned(),
        git_remote: project.git_remote().clone(),
        mcp_config,
    };

    Ok(if already_existed {
        RegisterOutcome::AlreadyExists(registered)
    } else {
        RegisterOutcome::Registered(registered)
    })
}

/// Computes the registration and writes the user-facing output. Used
/// by the standalone `tribal project register` subcommand; bootstrap
/// calls [`compute`] directly to skip the print pass.
pub(crate) async fn run_async(
    config: &TribalConfig,
    git_remote: &GitRemote,
    name: &str,
    branch: &str,
    opts: &OutputOptions<'_>,
    out_stdout: &mut dyn Write,
    out_stderr: &mut dyn Write,
) -> Result<RegisterOutcome, AppError> {
    if !opts.json {
        output::git_remote_resolved(out_stderr, git_remote.as_str());
    }
    let outcome = compute(config, git_remote, name, branch, opts).await?;
    let project = outcome.project();

    if opts.json {
        output::json_snippet(out_stdout, &project.mcp_config).map_err(|source| AppError::Io {
            context: "writing project register --json snippet".into(),
            source,
        })?;
    } else {
        match &outcome {
            RegisterOutcome::Registered(p) => {
                output::registered(out_stderr, p.project_name.as_str(), p.project_id);
            }
            RegisterOutcome::AlreadyExists(p) => {
                output::already_exists(out_stderr, p.project_name.as_str(), p.project_id);
            }
        }
        output::project_id(out_stdout, project.project_id).map_err(|source| AppError::Io {
            context: "writing project id".into(),
            source,
        })?;
        output::mcp_snippet(out_stdout, &project.git_remote, &project.mcp_config).map_err(
            |source| AppError::Io {
                context: "writing project register mcp snippet".into(),
                source,
            },
        )?;
    }

    Ok(outcome)
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
    use std::path::PathBuf;

    use tribal_test_utils::{
        assert_json_snapshot, assert_text_snapshot, serial_lock, test_context, truncate_all_tables,
    };

    use super::*;

    /// A representative absolute config path for fixtures.
    fn fixture_config_path() -> PathBuf {
        PathBuf::from("/etc/tribal/tribal.yaml")
    }

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

    // -- Snapshot locks -----------------------------------------------------

    #[tokio::test]
    async fn test_register_stderr_stdio_matches_snapshot() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("create pool");
        let mut conn = pool.acquire().await.expect("acquire");
        truncate_all_tables(&mut conn).await;
        drop(conn);

        let mut config = TribalConfig::minimum_valid(ctx.database_url());
        config.database.max_connect_attempts = 1;
        let git_remote = GitRemote::from_parts("github.com", "acme/widgets", None);
        let config_path = fixture_config_path();
        let opts = OutputOptions {
            json: false,
            transport: TransportKind::Stdio,
            auth: None,
            skip_validation: false,
            config_path: &config_path,
        };

        let mut stdout: Vec<u8> = Vec::new();
        let mut stderr: Vec<u8> = Vec::new();
        let outcome = run_async(
            &config,
            &git_remote,
            "widgets",
            "main",
            &opts,
            &mut stdout,
            &mut stderr,
        )
        .await
        .expect("register succeeds");

        let captured = String::from_utf8(stderr).expect("utf8");
        let normalised =
            captured.replace(&outcome.project().project_id.to_string(), "<PROJECT_ID>");
        assert_text_snapshot!(
            &normalised,
            "src/commands/project/snapshots/stderr-stdio.txt"
        );
    }

    #[tokio::test]
    async fn test_register_json_http_matches_snapshot() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("create pool");
        let mut conn = pool.acquire().await.expect("acquire");
        truncate_all_tables(&mut conn).await;
        drop(conn);

        let mut config = TribalConfig::minimum_valid(ctx.database_url());
        config.database.max_connect_attempts = 1;
        let git_remote = GitRemote::from_parts("github.com", "acme/widgets", None);
        let config_path = fixture_config_path();
        let auth = Auth::Bearer {
            token: "test-bearer-token"
                .parse()
                .expect("test token parses as BearerToken"),
        };
        let opts = OutputOptions {
            json: true,
            transport: TransportKind::Http,
            auth: Some(&auth),
            skip_validation: true,
            config_path: &config_path,
        };

        let mut stdout: Vec<u8> = Vec::new();
        let mut stderr: Vec<u8> = Vec::new();
        run_async(
            &config,
            &git_remote,
            "widgets",
            "main",
            &opts,
            &mut stdout,
            &mut stderr,
        )
        .await
        .expect("register succeeds");

        let captured = String::from_utf8(stdout).expect("utf8");
        let entry: serde_json::Value =
            serde_json::from_str(&captured).expect("stdout is valid JSON");
        assert_json_snapshot!(&entry, "src/commands/project/snapshots/json-http.json");
    }
}
