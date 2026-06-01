//! Core create flow: entry point and async orchestration.

use std::io::{self, Write};

use chrono::{DateTime, Utc};
use tribal_auth::issue_token;
use tribal_config::DatabaseConfig;
use tribal_db::PgAuthTokenRepository;
use tribal_domain::{BearerToken, LOCAL_PRINCIPAL_KEY, Scope, full_access_scopes};

use super::output;
use crate::{
    cli::TokenCreateArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, CredentialsPersistOutcome,
        DATABASE_COMMAND_DEFAULTS, TIMESTAMP_FORMAT, TtlInput, compute_expires_at,
        find_or_create_principal, persist_credentials, prepare_config,
    },
    error::AppError,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Pool name for the create connection.
const POOL_NAME: &str = "token-create";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal token create` flow.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection, principal
/// resolution, or token insertion fails.
pub(crate) fn run(config_path: &str, mut args: TokenCreateArgs) -> Result<(), AppError> {
    let principal = args.principal.take();
    let ttl = args.ttl;
    let scopes = resolve_token_scopes(std::mem::take(&mut args.scope));
    let cli_overrides = args.into_cli_overrides();
    let config = prepare_config(config_path, cli_overrides, &DATABASE_COMMAND_DEFAULTS)?;

    let expires_at = compute_expires_at(TtlInput::from_pair(ttl, config.auth.token_ttl_hours))?;
    let principal_key = principal.unwrap_or_else(|| LOCAL_PRINCIPAL_KEY.to_owned());
    // The audience must be byte-identical to the value the running
    // server compares against, so it is derived from the same resolver
    // (server bind address plus any oauth.resource_url override) the
    // serve path uses, not from oauth.resource_url alone.
    let audience = crate::startup::resolve_oauth_runtime(&config)?.canonical_resource;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    rt.block_on(run_async(
        &config.database,
        &principal_key,
        scopes,
        &audience,
        expires_at,
        &mut stdout,
        &mut stderr,
    ))?;
    Ok(())
}

/// Resolves the scopes to grant: the explicitly-requested set, or full
/// read and write access when none were given. Clap has already rejected
/// any scope the CLI may not mint, so the requested set is used verbatim.
fn resolve_token_scopes(requested: Vec<Scope>) -> Vec<Scope> {
    if requested.is_empty() {
        full_access_scopes()
    } else {
        requested
    }
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Creates a new auth token for the resolved principal, persists the
/// credentials.json artefact (warn-and-success on failure), and returns
/// the freshly-minted bearer token.
///
/// # Errors
///
/// Returns an [`AppError`] if the database connection, principal
/// lookup, or token insertion fails.
pub async fn run_async(
    db_config: &DatabaseConfig,
    principal_key: &str,
    scopes: Vec<Scope>,
    audience: &str,
    expires_at: DateTime<Utc>,
    out_stdout: &mut dyn Write,
    out_stderr: &mut dyn Write,
) -> Result<BearerToken, AppError> {
    let pool = tribal_db::create_pool(
        db_config,
        POOL_NAME,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| AppError::pool_acquire(POOL_NAME, "acquiring create connection", err))?;

    let principal = find_or_create_principal(&mut conn, principal_key).await?;
    output::principal_resolved(out_stderr, principal.principal_key());

    let raw_token = issue_token(
        &mut conn,
        &PgAuthTokenRepository,
        principal.id(),
        scopes,
        audience.to_owned(),
        expires_at,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    drop(conn);

    // Persist credentials.json before any post-insert output so the
    // file remains a recoverable artefact if the stdout write fails.
    let bearer_token: BearerToken =
        raw_token
            .parse()
            .map_err(|source| AppError::TokenVerification {
                reason: "generated bearer token failed parse validation".into(),
                source: Box::new(source),
            })?;
    let credentials = persist_credentials(&bearer_token);

    let raw_token_result = output::raw_token(out_stdout, bearer_token.as_str());
    output::token_created(out_stderr, &expires_at.format(TIMESTAMP_FORMAT).to_string());

    if let CredentialsPersistOutcome::Failed { warning } = &credentials {
        let _ = writeln!(out_stderr, "{warning}");
    }

    raw_token_result.map_err(|source| AppError::Io {
        context: "writing raw bearer token".into(),
        source,
    })?;

    Ok(bearer_token)
}
