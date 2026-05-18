//! End-to-end tests for `tribal mcp-config` across stdio / http and the
//! credentials-resolution variants the ticket spells out.
//!
//! Shape-level assertions (the JSON layout of the rendered entry) live
//! in the unit-level snapshot tests; the assertions here cover
//! end-to-end behaviour: exit / error semantics, the user-facing
//! literals on stderr, and credentials-resolution side effects.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tribal_config::{
    CREDENTIALS_PERMISSIONS_PERMISSIVE_SUFFIX, CliOverrides, Credentials, TransportKind,
};
use tribal_test_utils::{TestContext, serial_lock, test_context};

use super::common::{CwdGuard, TestEnv, fresh_db, parse_json, run_bootstrap, run_mcp_config};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Bootstraps a stdio project under the given test env and returns
/// the registered project ID. Most mcp-config behaviour tests need
/// a registered project plus a populated credentials.json — bootstrap
/// is the single call that produces both.
async fn seed_project(ctx: &TestContext, env: &TestEnv) -> String {
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
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(ctx, &env).await;
    std::fs::remove_file(env.credentials_path()).expect("remove credentials");

    run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Stdio,
        None,
    )
    .await
    .expect("mcp-config stdio succeeds without credentials");
}

/// stdio + `--token` must warn the user that the override is ignored
/// (stdio authenticates as `principal:local` at runtime) but still
/// emit the snippet and exit 0.
#[tokio::test]
async fn test_mcp_config_stdio_token_emits_warning_and_succeeds() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(ctx, &env).await;

    let (_stdout, stderr) = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Stdio,
        Some("ignored-token".to_owned()),
    )
    .await
    .expect("mcp-config stdio with token still succeeds");

    let stderr = String::from_utf8(stderr).expect("utf8 stderr");
    assert!(
        stderr.contains("--token has no effect when transport is stdio"),
        "expected stdio-token warning in: {stderr}",
    );
}

// ---------------------------------------------------------------------------
// http: credentials resolution
// ---------------------------------------------------------------------------

/// http with no credentials.json and no `--token` override must
/// surface the canonical "no saved credentials" literal and exit
/// non-zero.
#[tokio::test]
async fn test_mcp_config_http_missing_credentials_errors_with_literal() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(ctx, &env).await;
    std::fs::remove_file(env.credentials_path()).expect("remove credentials");

    let err = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        None,
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

/// http with garbage credentials.json must report a malformed-file
/// error citing the resolved path and a recovery hint. `contains` on
/// each invariant fragment (path-bearing prefix, recovery suffix)
/// stays robust to future stylisation that wraps the line in ANSI
/// codes.
#[tokio::test]
async fn test_mcp_config_http_malformed_credentials_errors_with_literal() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(ctx, &env).await;
    std::fs::write(env.credentials_path(), b"{ not json").expect("write garbage");

    let err = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        None,
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
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(ctx, &env).await;

    let future_version = Credentials::SCHEMA_VERSION + 1;
    let payload = format!(
        r#"{{"schema_version": {future_version}, "auth": {{"type": "bearer", "token": "x"}}}}"#,
    );
    std::fs::write(env.credentials_path(), payload).expect("write schema-mismatch creds");

    let err = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        None,
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
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(ctx, &env).await;

    std::fs::set_permissions(
        env.credentials_path(),
        std::fs::Permissions::from_mode(0o644),
    )
    .expect("chmod to permissive mode");

    let (_stdout, stderr) = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        None,
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
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let project_id = seed_project(ctx, &env).await;

    let override_token = "explicit-override-token";
    let (stdout, _stderr) = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        Some(override_token.to_owned()),
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
// Project resolution failure
// ---------------------------------------------------------------------------

/// With no `--project`, no `TRIBAL_PROJECT_ID`, and a cwd that has
/// no `.git`, the resolution cascade exhausts and surfaces the
/// canonical literal.
#[tokio::test]
async fn test_mcp_config_project_resolution_failure_errors_with_literal() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    // Cwd into a tempdir with no `.git` so the git-remote fallback
    // returns None.
    let cwd_dir = tempfile::tempdir().expect("cwd tempdir");
    let _cwd_guard = CwdGuard::set(cwd_dir.path());

    let err = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        None,
        TransportKind::Stdio,
        None,
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
