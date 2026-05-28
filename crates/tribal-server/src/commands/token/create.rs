//! Core create flow: entry point and async orchestration.

use std::io::{self, Write};

use chrono::{DateTime, Utc};
use tribal_auth::oauth::canonicalise_resource_url;
use tribal_common::sha256_hex;
use tribal_config::{DatabaseConfig, OAuthConfig};
use tribal_db::{AuthTokenRepository, NewAuthToken, PgAuthTokenRepository};
use tribal_domain::{BearerToken, LOCAL_PRINCIPAL_KEY, full_access_scopes};
use url::Url;

use super::output;
use crate::{
    cli::TokenCreateArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, CredentialsPersistOutcome,
        DATABASE_COMMAND_DEFAULTS, TIMESTAMP_FORMAT, TtlInput, compute_expires_at,
        find_or_create_principal, generate_raw_token, persist_credentials, prepare_config,
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
    let cli_overrides = args.into_cli_overrides();
    let config = prepare_config(config_path, cli_overrides, &DATABASE_COMMAND_DEFAULTS)?;

    let expires_at = compute_expires_at(TtlInput::from_pair(ttl, config.auth.token_ttl_hours))?;
    let principal_key = principal.unwrap_or_else(|| LOCAL_PRINCIPAL_KEY.to_owned());
    let audience = audience_from_oauth_config(&config.oauth);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    rt.block_on(run_async(
        &config.database,
        &principal_key,
        &audience,
        expires_at,
        &mut stdout,
        &mut stderr,
    ))?;
    Ok(())
}

/// Derives the audience claim for a CLI-minted token from the OAuth
/// config's resource URL, or the empty string if unset (legacy default).
fn audience_from_oauth_config(oauth: &OAuthConfig) -> String {
    oauth
        .resource_url
        .as_deref()
        .and_then(|raw| Url::parse(raw).ok())
        .map(|url| canonicalise_resource_url(&url))
        .unwrap_or_default()
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

    let raw_token = generate_raw_token();
    let token_hash = sha256_hex(&raw_token);

    let new_token = NewAuthToken::builder()
        .token_hash(token_hash)
        .principal_id(principal.id())
        .scopes(full_access_scopes())
        .audience(audience.to_owned())
        .expires_at(expires_at)
        .build();

    PgAuthTokenRepository
        .insert(&mut conn, &new_token)
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
