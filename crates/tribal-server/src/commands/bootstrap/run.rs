//! Core bootstrap flow: entry point and async orchestration.

use std::{
    io::{self, IsTerminal, Write},
    path::Path,
    str::FromStr,
};

use anstream::AutoStream;
use chrono::{DateTime, Utc};
use tribal_config::{
    Auth, CREDENTIALS_FILENAME, CliOverrides, ConfigPersistence, TRIBAL_DIRECTORY_NAME,
    TransportKind, TribalConfig, oauth_surface_is_routable,
};
use tribal_domain::GitRemote;
use tribal_ui::{Mode, StreamThemeContext, Theme, resolve_mode};

use super::output::{Handoff, write_human, write_json};
use crate::{
    cli::BootstrapArgs,
    commands::{
        common::{
            CredentialsPersistOutcome, DATABASE_COMMAND_DEFAULTS, TtlInput, compute_expires_at,
            prepare_config, resolve_absolute_config_path,
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

    let config = prepare_config(config_path, cli_overrides, &DATABASE_COMMAND_DEFAULTS)?;

    let expires_at = compute_expires_at(TtlInput::from_pair(ttl, config.auth.token_ttl_hours))?;
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
    .inspect_err(|_| replay_setup_on_failure(opts.json, out_stderr, &setup_buf))?;

    // From here on, any error path replays the recovery buffer via the
    // guard's `Drop` rather than peppering each bail-out with a manual
    // replay. `recovery_buf_for` strips the bearer-token-bearing setup
    // output when `--json` is set (machine consumers must never see the
    // token on stderr, most prominently container logs in the Docker
    // install path).
    let recovery_buf = recovery_buf_for(opts.json, setup_buf);
    let mut recovery = RecoveryGuard::arm(out_stderr, &recovery_buf);

    // -- Register -----------------------------------------------------------

    let oauth_surface_routable = oauth_surface_is_routable(opts.config);
    let auth = Auth::Bearer {
        token: setup_outcome.bearer_token.clone(),
    };
    // The embedded snippet follows the same topology rule as
    // `tribal mcp-config`: a loopback surface advertises a URL-only
    // (OAuth) snippet with nothing to copy, a routable surface embeds the
    // bearer token for harnesses that cannot perform the flow. The token
    // is persisted to credentials.json regardless, so the static-token
    // escape hatch stays available.
    let snippet_auth = oauth_surface_routable.then_some(&auth);
    let project = register::compute(
        opts.config,
        opts.git_remote,
        opts.project_name,
        DEFAULT_BRANCH,
        &OutputOptions {
            json: false,
            transport: opts.transport,
            auth: snippet_auth,
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
        oauth_surface_routable,
    };

    if opts.json {
        write_json(out_stdout, &handoff).map_err(|source| AppError::Io {
            context: "writing bootstrap --json output".into(),
            source,
        })?;
    } else {
        write_human(recovery.dest(), opts.theme, &handoff).map_err(|source| AppError::Io {
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
// Recovery
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

/// Composes the non-secret message replayed to stderr on `--json`-mode
/// bootstrap failures. The path segments are sourced from
/// [`tribal_config::TRIBAL_DIRECTORY_NAME`] and
/// [`tribal_config::CREDENTIALS_FILENAME`] so the message tracks the
/// canonical credentials path without a second source of truth.
fn json_recovery_message() -> Vec<u8> {
    format!(
        "bootstrap failed after setup; the bearer token (if persisted) lives in $XDG_CONFIG_HOME/{TRIBAL_DIRECTORY_NAME}/{CREDENTIALS_FILENAME}\n"
    )
    .into_bytes()
}

/// Returns the buffer the `RecoveryGuard` should replay on a
/// post-setup failure. In `--json` mode, the captured `setup_buf` is
/// discarded wholesale and a non-secret message is substituted —
/// there is no string inspection, no extraction, no parsing of
/// `setup_buf`. In human mode the original buffer is returned
/// unchanged, since the terminal is the intended audience for the
/// human-readable token block.
fn recovery_buf_for(json_mode: bool, setup_buf: Vec<u8>) -> Vec<u8> {
    if json_mode {
        json_recovery_message()
    } else {
        setup_buf
    }
}

/// Replays the captured setup buffer to `dest` when setup itself
/// fails. Gated symmetrically with [`recovery_buf_for`]: in `--json`
/// mode the buffer carries the raw bearer token, so stderr stays
/// silent and the `AppError` returned via `?` is the only signal a
/// machine consumer needs.
fn replay_setup_on_failure(json_mode: bool, dest: &mut dyn Write, setup_buf: &[u8]) {
    if !json_mode {
        let _ = dest.write_all(setup_buf);
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET_TOKEN: &str = "tk_super_secret_bearer_value";

    fn fixture_setup_buf() -> Vec<u8> {
        format!(
            "Setup complete. Your bearer token:\n  {SECRET_TOKEN}\nNext steps:\n  1. Export the token:  export TRIBAL_AUTH_TOKEN=\"{SECRET_TOKEN}\"\n"
        )
        .into_bytes()
    }

    #[test]
    fn test_json_recovery_message_references_canonical_path_segments() {
        let bytes = json_recovery_message();
        let message = std::str::from_utf8(&bytes).expect("recovery message is valid UTF-8");
        assert!(
            message.contains(TRIBAL_DIRECTORY_NAME),
            "recovery message must reference TRIBAL_DIRECTORY_NAME ({TRIBAL_DIRECTORY_NAME}); got: {message}",
        );
        assert!(
            message.contains(CREDENTIALS_FILENAME),
            "recovery message must reference CREDENTIALS_FILENAME ({CREDENTIALS_FILENAME}); got: {message}",
        );
    }

    #[test]
    fn test_recovery_buf_for_json_mode_discards_setup_buffer() {
        let setup_buf = fixture_setup_buf();
        let result = recovery_buf_for(true, setup_buf);
        let result_str = std::str::from_utf8(&result).expect("recovery buffer is valid UTF-8");
        assert!(
            !result_str.contains(SECRET_TOKEN),
            "json-mode recovery buffer must not echo the captured setup output's token; got: {result_str}",
        );
        assert_eq!(result, json_recovery_message());
    }

    #[test]
    fn test_recovery_buf_for_human_mode_preserves_setup_buffer() {
        let setup_buf = fixture_setup_buf();
        let expected = setup_buf.clone();
        let result = recovery_buf_for(false, setup_buf);
        assert_eq!(
            result, expected,
            "human-mode recovery buffer must preserve the captured setup output verbatim for terminal replay",
        );
    }

    #[test]
    fn test_recovery_guard_replays_buffer_on_drop_when_armed() {
        let buf = b"replay-payload".to_vec();
        let mut dest: Vec<u8> = Vec::new();
        {
            let _guard = RecoveryGuard::arm(&mut dest, &buf);
        }
        assert_eq!(dest, buf);
    }

    #[test]
    fn test_recovery_guard_suppresses_replay_after_disarm() {
        let buf = b"replay-payload".to_vec();
        let mut dest: Vec<u8> = Vec::new();
        {
            let mut guard = RecoveryGuard::arm(&mut dest, &buf);
            guard.disarm();
        }
        assert!(
            dest.is_empty(),
            "disarmed guard must not replay on drop; got: {dest:?}",
        );
    }

    #[test]
    fn test_replay_setup_on_failure_writes_buffer_in_human_mode() {
        let setup_buf = fixture_setup_buf();
        let mut dest: Vec<u8> = Vec::new();
        replay_setup_on_failure(false, &mut dest, &setup_buf);
        assert_eq!(
            dest, setup_buf,
            "human-mode setup-failure replay must write the captured buffer to stderr",
        );
    }

    #[test]
    fn test_replay_setup_on_failure_is_silent_in_json_mode() {
        let setup_buf = fixture_setup_buf();
        let mut dest: Vec<u8> = Vec::new();
        replay_setup_on_failure(true, &mut dest, &setup_buf);
        assert!(
            dest.is_empty(),
            "json-mode setup-failure replay must not write the token-bearing setup buffer; got: {dest:?}",
        );
    }
}
