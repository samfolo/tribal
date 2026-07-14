//! End-to-end semantics for individual check rows.  The check-internal
//! unit tests exercise every detail variant; these tests verify that
//! the orchestrator threads state correctly and the wire row reads as
//! expected for a few representative cases.

use tribal_config::{ENV_PUBLIC_MCP_URL, TribalConfig};
use tribal_domain::TransportKind;
use tribal_test_utils::{TestDb, env_lock};

use super::common::{
    CheckRun, EnvGuard, TestEnv, fresh_db, parse_json, row_status, run_check, write_config,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_happy_path_all_phases_green_against_fresh_db() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    assert_eq!(row_status(&output, "config_parse"), Some("pass"));
    assert_eq!(row_status(&output, "config_validate"), Some("pass"));
    assert_eq!(row_status(&output, "database_reachable"), Some("pass"));
    assert_eq!(row_status(&output, "migrations_current"), Some("pass"));
    // binary_uniqueness depends on whether `tribal` is on the runner's
    // PATH; CI invocations via `cargo test` may not have it installed,
    // so pass-or-warn is the contract — never fail, never skip.
    let binary = row_status(&output, "binary_uniqueness");
    assert!(
        matches!(binary, Some("pass" | "warn")),
        "binary_uniqueness should run (pass or warn), got {binary:?}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_project_cascade_missing_is_warn_without_override() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    assert_eq!(row_status(&output, "project_resolution"), Some("warn"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_token_is_skip_under_stdio_without_token_override() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    assert_eq!(row_status(&output, "valid_token_exists"), Some("skip"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_token_skips_on_loopback_http_without_token() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.server.transport = TransportKind::Http;
    config.server.bind_address = Some("127.0.0.1:8725".into());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    // Loopback OAuth surface, no static token: clients authenticate via
    // OAuth, so the absent token is not a finding.
    let output = parse_json(&stdout);
    assert_eq!(row_status(&output, "valid_token_exists"), Some("skip"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_token_fails_on_routable_http_without_token() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.server.transport = TransportKind::Http;
    config.server.bind_address = Some("127.0.0.1:8725".into());
    // A routable advertised surface with open registration off: the
    // static token is the only auth path, so its absence leaves clients
    // unable to authenticate. That is a readiness failure, not advice.
    config.oauth.resource_url = Some("https://tribal.example.com/mcp".into());
    config.oauth.dcr_enabled = false;
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    assert_eq!(row_status(&output, "valid_token_exists"), Some("fail"));
    // A routable deployment with no auth path is not ready: the failed
    // row must drive the aggregate `ok` to false.
    assert_eq!(output["ok"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_token_skips_on_docker_wildcard_loopback_shape() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    // The exact Docker compose shape: bound to the container wildcard
    // address, reached on a loopback host-port mapping advertised via
    // TRIBAL_PUBLIC_MCP_URL, DCR left enabled. The explicit loopback
    // advertised URL keeps the surface URL-only, so an absent static token
    // is not a finding and the deployment stays ready.
    let _public_url_guard = EnvGuard::set(ENV_PUBLIC_MCP_URL, "http://127.0.0.1:8725/mcp");
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.server.transport = TransportKind::Http;
    config.server.bind_address = Some("0.0.0.0:8725".into());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    // The token row is the regression guard: on the Docker shape it must
    // skip, never fail. The aggregate `ok` is not asserted because the
    // `advertised_url_reachable` probe targets a server this harness does
    // not stand up; in the live container that endpoint is bound and the
    // healthcheck reads green.
    let parsed = parse_json(&stdout);
    assert_eq!(row_status(&parsed, "valid_token_exists"), Some("skip"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_valid_token_fails_on_loopback_http_with_dcr_disabled() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.server.transport = TransportKind::Http;
    config.server.bind_address = Some("127.0.0.1:8725".into());
    // Loopback, but DCR off: a fresh client cannot register, so the static
    // token is the only auth path. Its absence is a readiness failure even
    // on loopback, proving the decision turns on the onboarding mode rather
    // than routability alone.
    config.oauth.dcr_enabled = false;
    write_config(&env.config_path, &config);

    let (stdout, _stderr, output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let parsed = parse_json(&stdout);
    assert_eq!(row_status(&parsed, "valid_token_exists"), Some("fail"));
    assert!(!output.ok, "loopback + DCR off + no token is not ready");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_advertised_url_is_skip_under_stdio_transport() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    assert_eq!(
        row_status(&output, "advertised_url_reachable"),
        Some("skip")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_advertised_url_attempts_probe_under_http_transport() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.server.transport = TransportKind::Http;
    config.server.bind_address = Some("127.0.0.1:0".into());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    // No server is bound, so the probe should fail; the contract here
    // is that it ran (i.e. the row is fail, not skip-under-stdio).
    let output = parse_json(&stdout);
    assert_eq!(
        row_status(&output, "advertised_url_reachable"),
        Some("fail")
    );
}
