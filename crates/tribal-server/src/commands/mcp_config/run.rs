//! Core mcp-config flow: entry point and async orchestration.

use std::{
    io::{self, Write},
    path::Path,
};

use tribal_config::{
    Auth, CREDENTIALS_PERMISSIONS_PERMISSIVE_PREFIX, CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX,
    CredentialsPermissions, LoadedCredentials, TransportKind, TribalConfig, load_config,
    read_credentials,
};
use tribal_domain::BearerToken;

use crate::{
    cli::McpConfigArgs,
    commands::common::{
        COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS, DATABASE_COMMAND_DEFAULTS,
        resolve_absolute_config_path,
    },
    error::AppError,
    output::{build_snippet_entry, resolved_advertised_url},
    startup::resolve_project,
};

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Bundle of inputs threaded into [`run_async`].
///
/// Constructed by the synchronous [`run`] wrapper from the parsed
/// [`McpConfigArgs`] plus the resolved config and absolute config path.
pub struct McpConfigOptions<'a> {
    /// Fully merged + validated configuration.
    pub config: &'a TribalConfig,
    /// Absolute path the rendered stdio snippet should embed.
    pub config_path: &'a Path,
    /// Project override from `--project` or `TRIBAL_PROJECT_ID`.
    pub project_override: Option<String>,
    /// Resolved transport for the rendered snippet.
    pub transport: TransportKind,
    /// Bearer token override from `--token`, if supplied.
    pub explicit_token: Option<String>,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Context message for [`AppError::ProjectResolution`] when no cascade
/// step yielded a project. Composes with the variant's `#[error]` prefix
/// `"project resolution failed: "` to form the full literal users see.
const PROJECT_RESOLUTION_FAILED_CONTEXT: &str = "no project resolved by --project / TRIBAL_PROJECT_ID / git remote. Pass --project explicitly or set TRIBAL_PROJECT_ID.";

/// Warning emitted when `--token` is passed under the stdio transport,
/// which authenticates as `principal:local` at runtime and cannot embed
/// a token in the snippet.
const STDIO_TOKEN_IGNORED: &str = "--token has no effect when transport is stdio; stdio authenticates as principal:local at runtime";

/// Pool name for the short-lived mcp-config connection.
const POOL_NAME_MCP_CONFIG: &str = "mcp-config";

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal mcp-config` flow.
///
/// Resolves the active project against the database and renders the same
/// snippet `tribal bootstrap` emits. Bearer tokens for http/sse transports
/// come from the persisted credentials file unless overridden by
/// `--token`.
///
/// # Errors
///
/// Returns an [`AppError`] if config loading, database connection,
/// project resolution, or credentials read fails.
///
/// mcp-config is a pure renderer — it does **not** call `validate`. A
/// successful bootstrap should be enough to make this command work even
/// after the user rotates the cloud-provider API key out of their shell:
/// rendering an existing project's snippet needs no inference keys.
pub(crate) fn run(config_path: &str, mut args: McpConfigArgs) -> Result<(), AppError> {
    let transport = args.transport;
    let project = args.project.take();
    let token = args.token.take();

    let cli_overrides = args.into_cli_overrides();
    let config = load_config(
        config_path,
        Some(cli_overrides),
        Some(&DATABASE_COMMAND_DEFAULTS),
    )?;

    let absolute_config_path = resolve_absolute_config_path(config_path)?;
    let transport = transport.unwrap_or(config.server.transport);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?;

    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();

    rt.block_on(run_async(
        McpConfigOptions {
            config: &config,
            config_path: &absolute_config_path,
            project_override: project,
            transport,
            explicit_token: token,
        },
        &mut stdout,
        &mut stderr,
    ))
}

// ---------------------------------------------------------------------------
// Async flow
// ---------------------------------------------------------------------------

/// Drives database connection, project resolution, auth resolution, and
/// snippet rendering.
///
/// `out_stdout` receives the rendered JSON; `out_stderr` carries warnings
/// (stdio `--token` ignored, permissions drift).
///
/// # Errors
///
/// Returns an [`AppError`] if the database connection, project
/// resolution, credentials read, or snippet write fails.
pub async fn run_async(
    opts: McpConfigOptions<'_>,
    out_stdout: &mut dyn Write,
    out_stderr: &mut dyn Write,
) -> Result<(), AppError> {
    let pool = tribal_db::create_pool(
        &opts.config.database,
        POOL_NAME_MCP_CONFIG,
        COMMAND_POOL_MAX_CONNECTIONS,
        COMMAND_STATEMENT_TIMEOUT_MS,
    )
    .await
    .map_err(|source| AppError::Database { source })?;

    let resolved = resolve_project(&pool, opts.project_override)
        .await?
        .ok_or_else(|| AppError::ProjectResolution {
            context: PROJECT_RESOLUTION_FAILED_CONTEXT.into(),
        })?;

    let auth = resolve_auth(opts.transport, opts.explicit_token, out_stderr)?;
    let advertised_url = resolved_advertised_url(opts.config);
    let entry = build_snippet_entry(
        resolved.id(),
        opts.transport,
        auth.as_ref(),
        opts.config_path,
        &advertised_url,
    );

    write_snippet(out_stdout, &entry)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves the [`Auth`] value to embed in the snippet.
///
/// stdio short-circuits to `None` — harness-spawned servers authenticate
/// as `principal:local` at runtime. Network transports prefer the
/// explicit `--token` override, falling back to the persisted credentials
/// file via [`read_credentials`]. The `--token` value is trimmed once at
/// this boundary; empty-after-trim falls through to the credentials
/// cascade (matches `register::resolve_auth`).
fn resolve_auth(
    transport: TransportKind,
    explicit_token: Option<String>,
    out_stderr: &mut dyn Write,
) -> Result<Option<Auth>, AppError> {
    let trimmed_token = explicit_token
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());

    match transport {
        TransportKind::Stdio => {
            if trimmed_token.is_some() {
                let _ = writeln!(out_stderr, "{STDIO_TOKEN_IGNORED}");
            }
            Ok(None)
        }
        TransportKind::Http | TransportKind::Sse => {
            let auth = resolve_network_auth(trimmed_token, out_stderr)?;
            Ok(Some(auth))
        }
    }
}

/// Network-transport auth resolution: explicit `--token` first (already
/// trimmed by [`resolve_auth`]), then persisted credentials. Permissions
/// drift warns on stderr but does not block.
fn resolve_network_auth(
    explicit_token: Option<String>,
    out_stderr: &mut dyn Write,
) -> Result<Auth, AppError> {
    if let Some(raw) = explicit_token {
        let token: BearerToken = raw.parse().map_err(|source| AppError::TokenVerification {
            reason: "bearer token from --token is invalid".into(),
            source: Box::new(source),
        })?;
        return Ok(Auth::Bearer { token });
    }

    let loaded = read_credentials().map_err(|source| AppError::Credentials { source })?;
    let LoadedCredentials {
        credentials,
        path,
        permissions,
    } = loaded;

    match permissions {
        CredentialsPermissions::Permissive => {
            let _ = writeln!(
                out_stderr,
                "{CREDENTIALS_PERMISSIONS_PERMISSIVE_PREFIX}{}{CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX}",
                path.display(),
            );
        }
        CredentialsPermissions::Locked | CredentialsPermissions::Unknown => {}
    }

    Ok(credentials.auth)
}

/// Writes the rendered snippet to stdout. Propagates IO failures —
/// stdout is the structured channel callers pipe into wire-up tooling.
fn write_snippet(out: &mut dyn Write, entry: &serde_json::Value) -> Result<(), AppError> {
    let rendered = serde_json::to_string_pretty(entry).expect("JSON serialisation cannot fail");
    writeln!(out, "{rendered}").map_err(|source| AppError::SetupIo {
        context: "writing mcp-config snippet".into(),
        source,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tribal_config::DEFAULT_BIND_ADDRESS;
    use tribal_domain::ProjectId;
    use tribal_test_utils::assert_json_snapshot;
    use uuid::Uuid;

    use super::*;

    /// A representative absolute config path for fixtures.
    fn fixture_config_path() -> PathBuf {
        PathBuf::from("/etc/tribal/tribal.yaml")
    }

    /// A deterministic project ID for snapshot fixtures.
    fn fixture_project_id() -> ProjectId {
        ProjectId::from(Uuid::from_u128(1))
    }

    /// A representative advertised URL for fixtures.
    fn fixture_advertised_url() -> String {
        format!("http://{DEFAULT_BIND_ADDRESS}/mcp")
    }

    /// Drives the production render path against fixture inputs and
    /// returns the captured stdout parsed as a JSON value.
    fn render_snippet(transport: TransportKind, auth: Option<&Auth>) -> serde_json::Value {
        let entry = build_snippet_entry(
            fixture_project_id(),
            transport,
            auth,
            &fixture_config_path(),
            &fixture_advertised_url(),
        );
        let mut buf: Vec<u8> = Vec::new();
        write_snippet(&mut buf, &entry).expect("write_snippet succeeds");
        let captured = String::from_utf8(buf).expect("utf8");
        serde_json::from_str(&captured).expect("output is valid JSON")
    }

    fn fixture_bearer_auth() -> Auth {
        Auth::Bearer {
            token: "test-bearer-token"
                .parse()
                .expect("fixture token parses as BearerToken"),
        }
    }

    #[test]
    fn test_snippet_stdio_matches_snapshot() {
        let payload = render_snippet(TransportKind::Stdio, None);
        assert_json_snapshot!(
            &payload,
            "src/commands/mcp_config/snapshots/snippet-stdio.json"
        );
    }

    #[test]
    fn test_snippet_http_matches_snapshot() {
        let auth = fixture_bearer_auth();
        let payload = render_snippet(TransportKind::Http, Some(&auth));
        assert_json_snapshot!(
            &payload,
            "src/commands/mcp_config/snapshots/snippet-http.json"
        );
    }
}
