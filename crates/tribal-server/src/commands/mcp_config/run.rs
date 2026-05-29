//! Core mcp-config flow: entry point and async orchestration.

use std::{
    io::{self, Write},
    path::Path,
};

use tribal_config::{
    Auth, CREDENTIALS_PERMISSIONS_PERMISSIVE_PREFIX, CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX,
    CredentialsPermissions, LoadedCredentials, TransportKind, TribalConfig, load_config,
    oauth_surface_is_routable, public_mcp_url_override, read_credentials,
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

/// How an http/sse snippet's authentication is chosen.
///
/// `--token` and `--static-token` are mutually exclusive at the CLI, so
/// the two flags resolve to a single value here and the invalid
/// "both at once" combination cannot be represented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenStrategy {
    /// `--token <value>`: embed this caller-supplied bearer token.
    Explicit(String),
    /// `--static-token`: embed the persisted static token from the
    /// credentials file, whatever the deployment topology.
    Static,
    /// No override: embed the static token only on a routable surface;
    /// a loopback surface advertises a URL-only snippet for the OAuth
    /// flow, leaving nothing to copy.
    Auto,
}

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
    /// How the http/sse snippet's authentication is chosen.
    pub token_strategy: TokenStrategy,
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
const STDIO_TOKEN_IGNORED: &str = "--token / --static-token have no effect when transport is stdio; stdio authenticates as principal:local at runtime";

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
    let token_strategy = token_strategy_from_args(args.token.take(), args.static_token);

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
            token_strategy,
        },
        &mut stdout,
        &mut stderr,
    ))
}

/// Resolves the parsed `--token` / `--static-token` flags into a single
/// [`TokenStrategy`]. The `--token` value is trimmed here, so an
/// empty-after-trim value is treated as absent rather than reaching
/// [`TokenStrategy::Explicit`].
fn token_strategy_from_args(token: Option<String>, static_token: bool) -> TokenStrategy {
    match token.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()) {
        Some(token) => TokenStrategy::Explicit(token),
        None if static_token => TokenStrategy::Static,
        None => TokenStrategy::Auto,
    }
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

    let oauth_surface_routable =
        oauth_surface_is_routable(opts.config, public_mcp_url_override().as_deref());
    let auth = resolve_auth(
        opts.transport,
        opts.token_strategy,
        oauth_surface_routable,
        out_stderr,
    )?;
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

/// Resolves the [`Auth`] to embed in the snippet from the chosen
/// [`TokenStrategy`] and the deployment topology.
///
/// stdio carries no `Authorization` header, so it always resolves to
/// `None` and warns when a token strategy was nonetheless requested.
/// `None` is the URL-only snippet: an OAuth-capable harness authenticates
/// via the flow with nothing to copy. mcp-config does not consult
/// `TRIBAL_AUTH_TOKEN`.
fn resolve_auth(
    transport: TransportKind,
    token_strategy: TokenStrategy,
    oauth_surface_routable: bool,
    out_stderr: &mut dyn Write,
) -> Result<Option<Auth>, AppError> {
    match transport {
        TransportKind::Stdio => {
            if !matches!(token_strategy, TokenStrategy::Auto) {
                let _ = writeln!(out_stderr, "{STDIO_TOKEN_IGNORED}");
            }
            Ok(None)
        }
        TransportKind::Http | TransportKind::Sse => match token_strategy {
            TokenStrategy::Explicit(token) => Ok(Some(bearer_from_explicit(&token)?)),
            TokenStrategy::Static => Ok(Some(read_persisted_auth(out_stderr)?)),
            TokenStrategy::Auto if oauth_surface_routable => {
                Ok(Some(read_persisted_auth(out_stderr)?))
            }
            TokenStrategy::Auto => Ok(None),
        },
    }
}

/// Builds bearer auth from a caller-supplied `--token` value.
fn bearer_from_explicit(raw: &str) -> Result<Auth, AppError> {
    let token: BearerToken = raw.parse().map_err(|source| AppError::TokenVerification {
        reason: "bearer token from --token is invalid".into(),
        source: Box::new(source),
    })?;
    Ok(Auth::Bearer { token })
}

/// Reads the persisted static token from the credentials file. Permissions
/// drift warns on stderr but does not block.
fn read_persisted_auth(out_stderr: &mut dyn Write) -> Result<Auth, AppError> {
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
    writeln!(out, "{rendered}").map_err(|source| AppError::Io {
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

    // -- token_strategy_from_args ---------------------------------------------

    #[test]
    fn test_token_strategy_from_args_maps_each_flag_combination() {
        assert_eq!(
            token_strategy_from_args(Some("abc".to_owned()), false),
            TokenStrategy::Explicit("abc".to_owned()),
        );
        assert_eq!(token_strategy_from_args(None, true), TokenStrategy::Static);
        assert_eq!(token_strategy_from_args(None, false), TokenStrategy::Auto);
    }

    #[test]
    fn test_token_strategy_from_args_treats_blank_token_as_absent() {
        assert_eq!(
            token_strategy_from_args(Some("   ".to_owned()), false),
            TokenStrategy::Auto,
        );
        assert_eq!(
            token_strategy_from_args(Some("  abc  ".to_owned()), false),
            TokenStrategy::Explicit("abc".to_owned()),
        );
    }

    // -- resolve_auth ---------------------------------------------------------

    #[test]
    fn test_resolve_auth_stdio_warns_when_a_token_is_requested() {
        let mut stderr = Vec::<u8>::new();
        let auth = resolve_auth(
            TransportKind::Stdio,
            TokenStrategy::Static,
            false,
            &mut stderr,
        )
        .expect("stdio auth resolves");
        assert!(auth.is_none(), "stdio embeds no token in the snippet");
        let warning = String::from_utf8(stderr).expect("utf8 stderr");
        assert_eq!(warning.trim_end(), STDIO_TOKEN_IGNORED);
    }

    #[test]
    fn test_resolve_auth_stdio_auto_is_silent() {
        let mut stderr = Vec::<u8>::new();
        let auth = resolve_auth(
            TransportKind::Stdio,
            TokenStrategy::Auto,
            false,
            &mut stderr,
        )
        .expect("stdio auth resolves");
        assert!(auth.is_none());
        assert!(
            stderr.is_empty(),
            "stdio without a token strategy is silent"
        );
    }

    #[test]
    fn test_resolve_auth_loopback_auto_is_url_only() {
        let mut stderr = Vec::<u8>::new();
        let auth = resolve_auth(TransportKind::Http, TokenStrategy::Auto, false, &mut stderr)
            .expect("loopback auth resolves");
        assert!(auth.is_none(), "loopback default is URL-only");
        assert!(stderr.is_empty());
    }

    #[test]
    fn test_resolve_auth_explicit_token_embeds_on_loopback() {
        let mut stderr = Vec::<u8>::new();
        let auth = resolve_auth(
            TransportKind::Http,
            TokenStrategy::Explicit("test-bearer-token".to_owned()),
            false,
            &mut stderr,
        )
        .expect("explicit token resolves");
        assert!(matches!(auth, Some(Auth::Bearer { .. })));
    }
}
