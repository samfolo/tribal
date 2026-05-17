//! Core bootstrap flow: entry point and async orchestration.

use std::{
    io::{self, IsTerminal, Write},
    path::Path,
    str::FromStr,
};

use anstream::AutoStream;
use chrono::{DateTime, Utc};
use tribal_config::{
    Auth, CliOverrides, ConfigPersistence, TransportKind, TribalConfig, load_config,
};
use tribal_domain::GitRemote;
use tribal_ui::{Mode, StreamThemeContext, Theme, resolve_mode};

use super::output::{Handoff, write_human, write_json};
use crate::{
    cli::BootstrapArgs,
    commands::{
        common::{
            CredentialsPersistOutcome, DATABASE_COMMAND_DEFAULTS, resolve_absolute_config_path,
            resolve_ttl,
        },
        project::register::{self, DEFAULT_BRANCH, OutputOptions},
        setup,
    },
    error::AppError,
    git::detect_git_remote,
    output::resolved_advertised_url,
};

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Bundle of inputs threaded into [`run_async`]. Re-exported at the
/// crate root under the `test-helpers` feature for integration-test
/// consumers.
pub struct BootstrapOptions<'a> {
    /// Fully merged + validated configuration.
    pub config: &'a TribalConfig,
    /// Absolute path the harness-spawned `tribal serve` should read.
    pub config_path: &'a Path,
    /// Principal key override from `--principal`, if supplied.
    pub principal_key: Option<&'a str>,
    /// Absolute expiry for the freshly minted bearer token.
    pub expires_at: DateTime<Utc>,
    /// CLI overrides used by the persisted-config renderer.
    pub persisted_overrides: &'a CliOverrides,
    /// Resolved git remote (`--remote` flag or repository detection).
    pub git_remote: &'a GitRemote,
    /// Human-friendly project name.
    pub project_name: &'a str,
    /// Resolved transport for the rendered snippet.
    pub transport: TransportKind,
    /// Whether to emit a single JSON object on stdout (`--json`).
    pub json: bool,
    /// Theme the human-output renderer applies. Production probes
    /// stderr at the sync wrapper to pick light/dark + capability;
    /// tests pass [`Theme::default_dark`] for deterministic snapshots.
    pub theme: &'a Theme,
}

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
    let transport = args.transport;
    let remote = args.remote.take();
    let name = args.name.take();
    let principal = args.principal.take();
    let ttl = args.ttl;
    let json = args.json;

    // `cli_overrides` is consumed by `load_config`; the persisted-config
    // renderer needs the same shape later.
    let cli_overrides = args.into_cli_overrides();
    let persisted_overrides = cli_overrides.clone();

    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    let expires_at = Utc::now() + resolve_ttl(ttl, config.auth.token_ttl_hours)?;
    let absolute_config_path = resolve_absolute_config_path(config_path)?;
    let git_remote = resolve_git_remote(remote.as_deref())?;
    let project_name = name.unwrap_or_else(|| git_remote.path().to_owned());
    let transport = transport.unwrap_or(config.server.transport);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let stderr_lock = io::stderr().lock();
    let is_tty = stderr_lock.is_terminal();
    let stream_ctx = StreamThemeContext::probe_stderr(is_tty, resolve_mode(Mode::Auto));
    let mut wrapped_stderr = AutoStream::new(stderr_lock, stream_ctx.color_choice);
    let mut stdout = io::stdout().lock();

    rt.block_on(run_async(
        BootstrapOptions {
            config: &config,
            config_path: &absolute_config_path,
            principal_key: principal.as_deref(),
            expires_at,
            persisted_overrides: &persisted_overrides,
            git_remote: &git_remote,
            project_name: &project_name,
            transport,
            json,
            theme: &stream_ctx.theme,
        },
        &mut stdout,
        &mut wrapped_stderr,
    ))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Drives setup → register → output. `out_stdout` carries the `--json`
/// payload; `out_stderr` carries the hand-off. Setup attempts a
/// best-effort write of credentials.json; the warn-and-success literal
/// records the outcome on failure.
///
/// # Errors
///
/// Returns an [`AppError`] if setup, project registration, or the
/// hand-off write fails.
pub async fn run_async(
    opts: BootstrapOptions<'_>,
    out_stdout: &mut dyn Write,
    out_stderr: &mut dyn Write,
) -> Result<(), AppError> {
    // -- Setup --------------------------------------------------------------

    let mut setup_buf: Vec<u8> = Vec::new();
    let setup_outcome = setup::run_async(
        opts.config,
        opts.config_path,
        opts.principal_key,
        opts.expires_at,
        ConfigPersistence::Persisted(opts.persisted_overrides),
        &mut setup_buf,
    )
    .await
    .inspect_err(|_| {
        // Replay any progress captured before setup failed.
        let _ = out_stderr.write_all(&setup_buf);
    })?;

    // Carry the credentials-write warning inside the recovery payload so
    // a later replay (register failure, hand-off failure) surfaces it
    // alongside setup's captured output rather than dropping it on the
    // floor. The happy-path emit further down still runs in addition,
    // landing the warning right under the polished hand-off.
    if let CredentialsPersistOutcome::Failed { warning } = &setup_outcome.credentials {
        writeln!(setup_buf, "{warning}").expect("writeln to Vec<u8> is infallible");
    }

    // From here on, any error path replays the captured setup output
    // via the guard's `Drop` rather than peppering each bail-out with
    // a manual replay.
    let mut recovery = RecoveryGuard::arm(out_stderr, &setup_buf);

    // -- Register -----------------------------------------------------------

    let auth = Auth::Bearer {
        token: setup_outcome.bearer_token.clone(),
    };
    let project = register::compute(
        opts.config,
        opts.git_remote,
        opts.project_name,
        DEFAULT_BRANCH,
        &OutputOptions {
            json: false,
            transport: opts.transport,
            auth: Some(&auth),
            // The token was just minted by setup against this same
            // database — re-verifying would only add a round trip.
            skip_validation: true,
            config_path: opts.config_path,
        },
    )
    .await?
    .into_project();

    // -- Hand-off -----------------------------------------------------------

    let advertised_url = resolved_advertised_url(opts.config);
    let handoff = Handoff {
        bearer_token: &setup_outcome.bearer_token,
        principal_key: &setup_outcome.principal_key,
        principal_id: setup_outcome.principal_id,
        project_id: project.project_id,
        project_name: &project.project_name,
        git_remote: &project.git_remote,
        transport: opts.transport,
        mcp_entry: &project.mcp_config,
        config_file: &setup_outcome.config_file,
        credentials: &setup_outcome.credentials,
        persistence: ConfigPersistence::Persisted(opts.persisted_overrides),
        advertised_url: &advertised_url,
    };

    if opts.json {
        write_json(out_stdout, &handoff).map_err(|source| AppError::SetupIo {
            context: "writing bootstrap --json output".into(),
            source,
        })?;
    } else {
        write_human(recovery.dest(), opts.theme, &handoff).map_err(|source| AppError::SetupIo {
            context: "writing bootstrap stderr output".into(),
            source,
        })?;
    }

    // The hand-off rendered the token block in its better form. Anything
    // after this point is best-effort decoration, not a recovery
    // surface, so disarm the guard.
    recovery.disarm();

    // Credentials persistence is governed by the warn-and-success rule:
    // a write failure must never escalate to command failure. The
    // warning emit therefore mirrors `setup::run` and `token::create::run`
    // — `let _ = writeln!(...)` — rather than `?`-propagating.
    if let CredentialsPersistOutcome::Failed { warning } = &setup_outcome.credentials {
        let _ = writeln!(recovery.dest(), "{warning}");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery guard
// ---------------------------------------------------------------------------

/// Replays a captured stderr buffer on drop when armed.
///
/// Borrows `dest` mutably for the guarded scope; consumers reach the
/// underlying writer through [`dest`](Self::dest) so a single early
/// return through `?` triggers the replay via [`Drop`]. The replay is
/// best-effort: the caller is already returning a more informative
/// [`AppError`], and a broken stderr should not mask it.
struct RecoveryGuard<'a> {
    dest: &'a mut dyn Write,
    buf: &'a [u8],
    armed: bool,
}

impl<'a> RecoveryGuard<'a> {
    fn arm(dest: &'a mut dyn Write, buf: &'a [u8]) -> Self {
        Self {
            dest,
            buf,
            armed: true,
        }
    }

    fn dest(&mut self) -> &mut dyn Write {
        self.dest
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RecoveryGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.dest.write_all(self.buf);
        }
    }
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
