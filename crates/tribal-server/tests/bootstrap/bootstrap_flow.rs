//! End-to-end tests for `tribal bootstrap`.

use tribal_config::{
    CliOverrides, ENV_OPENAI_API_KEY, EmbeddingCliOverrides, ProviderKind, TransportKind,
};
use tribal_db::{PgPrincipalRepository, PrincipalRepository};
use tribal_domain::LOCAL_PRINCIPAL_KEY;
use tribal_test_utils::{serial_lock, test_context};

use super::common::{EnvGuard, TestEnv, fresh_db, parse_json, run_bootstrap, run_mcp_config};

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

/// Bootstrap → mcp-config must produce a byte-identical `mcp_config`
/// shape — the shared snippet builder is the single source of truth.
#[tokio::test]
async fn bootstrap_then_mcp_config_round_trip_stdio() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let (boot_stdout, _) = run_bootstrap(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        None,
        TransportKind::Stdio,
        true,
    )
    .await
    .expect("bootstrap succeeds");

    let boot_json = parse_json(&boot_stdout);
    let project_id = boot_json["project_id"]
        .as_str()
        .expect("bootstrap json carries project_id")
        .to_owned();
    let boot_mcp = boot_json["mcp_config"].clone();

    let (mc_stdout, _) = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Stdio,
        None,
    )
    .await
    .expect("mcp-config succeeds");

    let mc_json = parse_json(&mc_stdout);
    assert_eq!(boot_mcp, mc_json, "stdio mcp_config round-trip");
}

/// Same round-trip property for the http transport — bearer token
/// embedded by bootstrap must match the one mcp-config reads back
/// from the persisted credentials.
#[tokio::test]
async fn bootstrap_then_mcp_config_round_trip_http() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let (boot_stdout, _) = run_bootstrap(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        None,
        TransportKind::Http,
        true,
    )
    .await
    .expect("bootstrap succeeds");

    let boot_json = parse_json(&boot_stdout);
    let project_id = boot_json["project_id"]
        .as_str()
        .expect("project_id")
        .to_owned();
    let boot_mcp = boot_json["mcp_config"].clone();

    let (mc_stdout, _) = run_mcp_config(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        None,
    )
    .await
    .expect("mcp-config succeeds");

    let mc_json = parse_json(&mc_stdout);
    assert_eq!(boot_mcp, mc_json, "http mcp_config round-trip");
}

// ---------------------------------------------------------------------------
// --principal provisioning
// ---------------------------------------------------------------------------

/// `--principal user:alice` must provision both `principal:local`
/// (so stdio auth keeps working) and `user:alice` (the explicit
/// token holder).
#[tokio::test]
async fn bootstrap_with_explicit_principal_provisions_both() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let pool = fresh_db(ctx).await;
    let env = TestEnv::new();

    let (_stdout, _stderr) = run_bootstrap(
        ctx,
        &env.config_path,
        CliOverrides::default(),
        Some("user:alice"),
        TransportKind::Stdio,
        true,
    )
    .await
    .expect("bootstrap succeeds");

    let mut conn = pool.acquire().await.expect("acquire");
    let local = PgPrincipalRepository
        .find_by_key(&mut conn, LOCAL_PRINCIPAL_KEY)
        .await
        .expect("query local principal");
    let alice = PgPrincipalRepository
        .find_by_key(&mut conn, "user:alice")
        .await
        .expect("query alice principal");

    assert!(local.is_some(), "principal:local must be provisioned");
    assert!(alice.is_some(), "user:alice must be provisioned");
}

// ---------------------------------------------------------------------------
// Validation: OpenAI embedding provider without an API key
// ---------------------------------------------------------------------------

/// Selecting the OpenAI embedding provider with no API key must
/// surface the canonical validation literal verbatim. Drives the
/// full cascade so a regression in the figment overlay or the
/// validate rule would surface here.
#[tokio::test]
async fn bootstrap_validation_rejects_openai_without_key() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    let _api_key_guard = EnvGuard::remove(ENV_OPENAI_API_KEY);

    let overrides = CliOverrides {
        embedding: Some(EmbeddingCliOverrides {
            provider: Some(ProviderKind::OpenAi),
            model: None,
        }),
        ..CliOverrides::default()
    };

    let err = run_bootstrap(
        ctx,
        &env.config_path,
        overrides,
        None,
        TransportKind::Stdio,
        true,
    )
    .await
    .expect_err("openai without key must fail");

    let display = err.to_string();
    assert!(
        display.contains("embedding.api_key is required when embedding.provider is openai"),
        "missing canonical literal in: {display}",
    );
}

// ---------------------------------------------------------------------------
// First-run persistence symmetry
// ---------------------------------------------------------------------------

/// Bootstrap with persistable flags writes a `tribal.yaml`; a second
/// invocation with the same flags must leave the file byte-identical
/// (file-exists path, no silent rewrites).
#[tokio::test]
async fn bootstrap_persists_then_leaves_file_unchanged_on_second_run() {
    let _lock = serial_lock().await;
    let ctx = test_context().await;
    let _pool = fresh_db(ctx).await;
    let env = TestEnv::new();
    // The OpenAI provider requires an api_key at validation time; the
    // cascade picks it up from this env var.
    let _api_key_guard = EnvGuard::set(ENV_OPENAI_API_KEY, "test-key");

    let overrides = CliOverrides {
        embedding: Some(EmbeddingCliOverrides {
            provider: Some(ProviderKind::OpenAi),
            model: Some("text-embedding-3-small".into()),
        }),
        ..CliOverrides::default()
    };

    // -- First run writes the file ------------------------------------------
    let _ = run_bootstrap(
        ctx,
        &env.config_path,
        overrides.clone(),
        None,
        TransportKind::Stdio,
        true,
    )
    .await
    .expect("first run succeeds");

    assert!(env.config_path.exists(), "first run wrote config file");
    let first_content = tokio::fs::read_to_string(&env.config_path)
        .await
        .expect("read first config");
    assert!(
        first_content.contains("openai"),
        "persisted YAML mentions openai: {first_content}",
    );

    // -- Second run finds the file, leaves it byte-identical ----------------
    let _ = run_bootstrap(
        ctx,
        &env.config_path,
        overrides,
        None,
        TransportKind::Stdio,
        true,
    )
    .await
    .expect("second run succeeds");

    let second_content = tokio::fs::read_to_string(&env.config_path)
        .await
        .expect("read second config");
    assert_eq!(first_content, second_content, "config unchanged on re-run");
}
