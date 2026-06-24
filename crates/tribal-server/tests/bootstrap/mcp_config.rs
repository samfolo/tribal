//! End-to-end tests for `tribal mcp-config` across stdio / http and the
//! credentials-resolution variants the ticket spells out.
//!
//! Shape-level assertions (the JSON layout of the rendered entry) live
//! in the unit-level snapshot tests; the assertions here cover
//! end-to-end behaviour: exit / error semantics, the user-facing
//! literals on stderr, and credentials-resolution side effects.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tribal::TokenStrategy;
use tribal_config::{
    CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX, CliOverrides, Credentials, ENV_PUBLIC_MCP_URL,
    TransportKind,
};
use tribal_test_utils::{TestDb, env_lock};

use super::common::{
    CwdGuard, EnvGuard, TestEnv, fresh_db, parse_json, run_bootstrap, run_mcp_config,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Bootstraps a stdio project under the given test env and returns
/// the registered project ID. Most mcp-config behaviour tests need
/// a registered project plus a populated credentials.json — bootstrap
/// is the single call that produces both.
async fn seed_project(ctx: &TestDb, env: &TestEnv) -> String {
    let (stdout, _) = run_bootstrap(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        None,
        TransportKind::Stdio,
        true,
    )
    .await
    .expect("seed project via bootstrap");
    parse_json(&stdout)["project_id"]
        .as_str()
        .expect("bootstrap json carries project_id")
        .to_owned()
}

// ---------------------------------------------------------------------------
// stdio
// ---------------------------------------------------------------------------

/// stdio renders without consulting credentials.json — missing file
/// is not an error.
#[tokio::test]
async fn test_mcp_config_stdio_succeeds_without_credentials() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(&ctx, &env).await;
    std::fs::remove_file(env.credentials_path()).expect("remove credentials");

    run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Stdio,
        TokenStrategy::Auto,
    )
    .await
    .expect("mcp-config stdio succeeds without credentials");
}

/// stdio + `--token` must warn the user that the override is ignored
/// (stdio authenticates as `principal:local` at runtime) but still
/// emit the snippet and exit 0.
#[tokio::test]
async fn test_mcp_config_stdio_token_emits_warning_and_succeeds() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(&ctx, &env).await;

    let (_stdout, stderr) = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Stdio,
        TokenStrategy::Explicit("ignored-token".to_owned()),
    )
    .await
    .expect("mcp-config stdio with token still succeeds");

    let stderr = String::from_utf8(stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("have no effect when transport is stdio"),
        "expected stdio-token warning in: {stderr}",
    );
}

// ---------------------------------------------------------------------------
// http: topology-aware default
// ---------------------------------------------------------------------------

/// The loopback http default is URL-only: with no `--token` or
/// `--static-token`, the snippet carries no `Authorization` header and an
/// OAuth-capable harness authenticates via the flow.
#[tokio::test]
async fn test_mcp_config_http_loopback_default_is_url_only() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    // The bind is loopback, so only a routable advertised override would
    // flip the topology; clear it so the default is deterministic.
    let _public_url_guard = EnvGuard::remove(ENV_PUBLIC_MCP_URL);
    let project_id = seed_project(&ctx, &env).await;

    let (stdout, _stderr) = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Auto,
    )
    .await
    .expect("mcp-config http loopback default succeeds");

    let entry = parse_json(&stdout);
    assert!(
        entry.get("headers").is_none(),
        "loopback default omits the Authorization header: {entry}",
    );
}

// ---------------------------------------------------------------------------
// http: credentials resolution
// ---------------------------------------------------------------------------
//
// The loopback http default is URL-only and never reads credentials, so
// these cases force the credentials path with `--static-token`.

/// http `--static-token` with no credentials.json must surface the
/// canonical "no saved credentials" literal and exit non-zero.
#[tokio::test]
async fn test_mcp_config_http_missing_credentials_errors_with_literal() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(&ctx, &env).await;
    std::fs::remove_file(env.credentials_path()).expect("remove credentials");

    let err = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Static,
    )
    .await
    .expect_err("http without credentials must fail");

    let display = err.to_string();
    assert!(
        display.contains(
            "no saved credentials; run `tribal setup` or `tribal bootstrap`, or pass `--token` explicitly.",
        ),
        "missing literal in: {display}",
    );
}

/// A routable advertised surface flips the `Auto` default from URL-only
/// to embedding the persisted bearer: open registration is refused
/// beyond loopback, so a bearer-only harness needs the token in the
/// snippet. credentials.json (seeded by bootstrap) supplies it.
#[tokio::test]
async fn test_mcp_config_http_routable_auto_embeds_persisted_bearer() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let _public_url_guard = EnvGuard::set(ENV_PUBLIC_MCP_URL, "https://tribal.example.com/mcp");
    let project_id = seed_project(&ctx, &env).await;

    let (stdout, _stderr) = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Auto,
    )
    .await
    .expect("mcp-config http routable Auto succeeds");

    let entry = parse_json(&stdout);
    assert!(
        entry["headers"]["Authorization"]
            .as_str()
            .is_some_and(|header| header.starts_with("Bearer ")),
        "routable Auto embeds the persisted bearer: {entry}",
    );
}

/// A loopback surface with DCR disabled is not URL-only onboarding: with no
/// automatic registration path, a static token is the only way a fresh
/// client authenticates, so `Auto` embeds the persisted bearer rather than
/// advertising the OAuth flow. The decision turns on the onboarding mode,
/// not routability alone.
#[tokio::test]
async fn test_mcp_config_http_loopback_dcr_disabled_auto_embeds_persisted_bearer() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    // Loopback bind, no advertised override, DCR off. The nested-env name
    // follows the `TRIBAL_<SECTION>__<FIELD>` mapping the loader honours.
    let _public_url_guard = EnvGuard::remove(ENV_PUBLIC_MCP_URL);
    let _dcr_guard = EnvGuard::set("TRIBAL_OAUTH__DCR_ENABLED", "false");
    let project_id = seed_project(&ctx, &env).await;

    let (stdout, _stderr) = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Auto,
    )
    .await
    .expect("mcp-config loopback dcr-disabled Auto succeeds");

    let entry = parse_json(&stdout);
    assert!(
        entry["headers"]["Authorization"]
            .as_str()
            .is_some_and(|header| header.starts_with("Bearer ")),
        "loopback + DCR off embeds the persisted bearer: {entry}",
    );
}

/// http with garbage credentials.json must report a malformed-file
/// error citing the resolved path and a recovery hint. `contains` on
/// each invariant fragment (path-bearing prefix, recovery suffix)
/// stays robust to future stylisation that wraps the line in ANSI
/// codes.
#[tokio::test]
async fn test_mcp_config_http_malformed_credentials_errors_with_literal() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(&ctx, &env).await;
    std::fs::write(env.credentials_path(), b"{ not json").expect("write garbage");

    let err = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Static,
    )
    .await
    .expect_err("malformed credentials must fail");

    let display = err.to_string();
    let path = env.credentials_path();
    let prefix = format!("credentials.json at {} is malformed: ", path.display());
    assert!(
        display.contains(&prefix),
        "expected canonical prefix `{prefix}` in: {display}",
    );
    assert!(
        display.contains("; re-mint with `tribal token create`"),
        "expected recovery-hint suffix in: {display}",
    );
}

/// http with a credentials.json whose `schema_version` doesn't match
/// the binary's [`Credentials::SCHEMA_VERSION`] must reject and quote
/// the offending version.
#[tokio::test]
async fn test_mcp_config_http_schema_mismatch_errors_with_literal() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(&ctx, &env).await;

    let future_version = Credentials::SCHEMA_VERSION + 1;
    let payload = format!(
        r#"{{"schema_version": {future_version}, "auth": {{"type": "bearer", "token": "x"}}}}"#,
    );
    std::fs::write(env.credentials_path(), payload).expect("write schema-mismatch creds");

    let err = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Static,
    )
    .await
    .expect_err("schema-mismatch credentials must fail");

    let expected = format!(
        "credentials.json schema_version {future_version} is not supported by this binary; re-mint with `tribal token create`",
    );
    let display = err.to_string();
    assert!(
        display.contains(&expected),
        "expected schema-mismatch literal in: {display}",
    );
}

/// A permissive credentials file (mode wider than 0600) must warn on
/// stderr but still render the snippet successfully. The warning suffix
/// is asserted against the imported constant so a re-wording in
/// production cascades to the test.
#[cfg(unix)]
#[tokio::test]
async fn test_mcp_config_http_permissive_credentials_warn_and_succeed() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(&ctx, &env).await;

    std::fs::set_permissions(
        env.credentials_path(),
        std::fs::Permissions::from_mode(0o644),
    )
    .expect("chmod to permissive mode");

    let (_stdout, stderr) = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Static,
    )
    .await
    .expect("mcp-config still succeeds with permissive credentials");

    let stderr = String::from_utf8(stderr).expect("utf8 stderr");
    assert!(
        stderr.contains(CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX),
        "expected permissive-permissions suffix in: {stderr}",
    );
}

/// `--token T` overrides whatever credentials.json contains — the
/// emitted snippet must carry `Bearer T` regardless.
#[tokio::test]
async fn test_mcp_config_http_explicit_token_overrides_credentials() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(&ctx, &env).await;

    let override_token = "explicit-override-token";
    let (stdout, _stderr) = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Explicit(override_token.to_owned()),
    )
    .await
    .expect("mcp-config http with --token succeeds");

    let entry = parse_json(&stdout);
    assert_eq!(
        entry["headers"]["Authorization"],
        format!("Bearer {override_token}"),
        "header must carry the explicit override token",
    );
}

// ---------------------------------------------------------------------------
// Project resolution: required for stdio, irrelevant for the network
// transports
// ---------------------------------------------------------------------------

/// The network snippet binds its project server-side via `serve
/// --project`, so http renders even when the resolution cascade would
/// find nothing: no `--project`, no `TRIBAL_PROJECT_ID`, and a cwd with
/// no `.git`. The emitted entry carries a url and no project.
#[tokio::test]
async fn test_mcp_config_http_renders_without_a_resolvable_project() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    // Loopback default keeps this URL-only, so credentials are never
    // read and the snippet is fully determined by the cascade outcome.
    let _public_url_guard = EnvGuard::remove(ENV_PUBLIC_MCP_URL);

    // Cwd into a tempdir with no `.git` so the git-remote fallback
    // returns None and the cascade would otherwise exhaust.
    let cwd_dir = tempfile::tempdir().expect("cwd tempdir");
    let _cwd_guard = CwdGuard::set(cwd_dir.path());

    let (stdout, _stderr) = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        None,
        TransportKind::Http,
        TokenStrategy::Auto,
    )
    .await
    .expect("http renders without a resolvable project");

    let entry = parse_json(&stdout);
    assert_eq!(
        entry["type"], "http",
        "network snippet carries the transport type: {entry}",
    );
    assert!(
        entry.get("url").is_some(),
        "network snippet carries a url: {entry}",
    );
}

/// With no `--project`, no `TRIBAL_PROJECT_ID`, and a cwd that has
/// no `.git`, the stdio resolution cascade exhausts and surfaces the
/// canonical literal. stdio embeds `serve --project <id>`, so a project
/// is required here.
#[tokio::test]
async fn test_mcp_config_project_resolution_failure_errors_with_literal() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();

    // Cwd into a tempdir with no `.git` so the git-remote fallback
    // returns None.
    let cwd_dir = tempfile::tempdir().expect("cwd tempdir");
    let _cwd_guard = CwdGuard::set(cwd_dir.path());

    let err = run_mcp_config(
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        None,
        TransportKind::Stdio,
        TokenStrategy::Auto,
    )
    .await
    .expect_err("resolution must fail without inputs");

    let display = err.to_string();
    assert!(
        display.contains(
            "project resolution failed: no project resolved by --project / TRIBAL_PROJECT_ID / git remote. Pass --project explicitly or set TRIBAL_PROJECT_ID.",
        ),
        "missing canonical literal in: {display}",
    );
}
