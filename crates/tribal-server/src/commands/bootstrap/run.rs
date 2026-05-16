//! Core bootstrap flow: entry point and async orchestration.

use std::{
    io::{self, Write},
    path::Path,
    str::FromStr,
};

use chrono::{DateTime, Utc};
use tribal_config::{Auth, TransportKind, TribalConfig, load_config, validate};
use tribal_domain::GitRemote;

use super::output::{Handoff, write_human, write_json};
use crate::{
    cli::BootstrapArgs,
    commands::{
        common::{
            DATABASE_COMMAND_DEFAULTS, persist_credentials, resolve_absolute_config_path,
            resolve_ttl,
        },
        project::register::{self, DEFAULT_BRANCH, OutputOptions},
        setup,
    },
    error::AppError,
    git::detect_git_remote,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal bootstrap` flow.
///
/// Composes `tribal setup` and `tribal project register` into a single
/// invocation. Setup mints a bearer token and seeds the database; the
/// token is then forwarded into register so the resulting MCP snippet
/// embeds it for HTTP/SSE transports.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, validation, git detection,
/// or any composed step fails.
pub(crate) fn run(config_path: &str, mut args: BootstrapArgs) -> Result<(), AppError> {
    // Drain the bootstrap-specific fields before consuming `args` so
    // `into_cli_overrides` can take ownership of the per-section args
    // unencumbered. Mirrors the pattern at `setup/run.rs`.
    let transport = args.transport;
    let remote = args.remote.take();
    let name = args.name.take();
    let principal = args.principal.take();
    let ttl = args.ttl;
    let json = args.json;

    let cli_overrides = args.into_cli_overrides();

    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;
    validate(&config)?;

    let expires_at = Utc::now() + resolve_ttl(ttl, config.auth.token_ttl_hours)?;
    let absolute_config_path = resolve_absolute_config_path(config_path)?;
    let git_remote = resolve_git_remote(remote.as_deref())?;
    let project_name = name.unwrap_or_else(|| git_remote.path().to_owned());
    let transport = transport.unwrap_or(config.server.transport);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();

    rt.block_on(run_async(
        &config,
        &absolute_config_path,
        principal.as_deref(),
        expires_at,
        &git_remote,
        &project_name,
        transport,
        json,
        &mut stdout,
        &mut stderr,
    ))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Drives setup → register → output, discarding the intermediate stderr
/// of each composed step.
///
/// `out_stdout` receives the `--json` payload (when requested);
/// `out_stderr` receives the human hand-off and any persistence
/// warnings.
//
// Bootstrap orchestrates two composed commands and their session args,
// so the parameter set is wide by nature. Bundling would create a
// single-use struct heavier than the function it serves.
#[allow(clippy::too_many_arguments)]
async fn run_async(
    config: &TribalConfig,
    config_path: &Path,
    principal_key: Option<&str>,
    expires_at: DateTime<Utc>,
    git_remote: &GitRemote,
    project_name: &str,
    transport: TransportKind,
    json: bool,
    out_stdout: &mut dyn Write,
    out_stderr: &mut dyn Write,
) -> Result<(), AppError> {
    // -- Setup --------------------------------------------------------------

    let setup_outcome = setup::run_async(
        config,
        config_path,
        principal_key,
        expires_at,
        &mut io::sink(),
    )
    .await?;

    // Persist credentials before the hand-off so any warning surfaces
    // ahead of the polished output. Setup left the in-memory token
    // intact regardless of persistence success.
    persist_credentials(out_stderr, &setup_outcome.bearer_token);

    // -- Register -----------------------------------------------------------

    let auth = Auth::Bearer {
        token: setup_outcome.bearer_token.clone(),
    };
    let opts = OutputOptions {
        json: false,
        transport,
        auth: Some(&auth),
        // The token was just minted by setup against this same
        // database — re-verifying would only add a round trip.
        skip_validation: true,
        config_path,
    };
    let register_outcome = register::run_async(
        config,
        git_remote,
        project_name,
        DEFAULT_BRANCH,
        &opts,
        &mut io::sink(),
        &mut io::sink(),
    )
    .await?;

    // -- Hand-off -----------------------------------------------------------

    let config_path_display = config_path.display().to_string();
    let handoff = Handoff {
        bearer_token: &setup_outcome.bearer_token,
        principal_key: &setup_outcome.principal_key,
        principal_id: setup_outcome.principal_id,
        project_id: register_outcome.project_id,
        project_name: &register_outcome.project_name,
        git_remote: &register_outcome.git_remote,
        transport,
        mcp_config: &register_outcome.mcp_config,
        config_path: &config_path_display,
    };

    if json {
        write_json(out_stdout, &handoff).map_err(|source| AppError::SetupIo {
            context: "writing bootstrap --json output".into(),
            source,
        })?;
    } else {
        write_human(out_stderr, &handoff).map_err(|source| AppError::SetupIo {
            context: "writing bootstrap stderr output".into(),
            source,
        })?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the git remote from an explicit `--remote` flag or by
/// detecting from the current working directory.
fn resolve_git_remote(explicit: Option<&str>) -> Result<GitRemote, AppError> {
    match explicit {
        Some(url) => GitRemote::from_str(url).map_err(|e| AppError::GitDetection {
            reason: e.to_string(),
        }),
        None => detect_git_remote(),
    }
}
