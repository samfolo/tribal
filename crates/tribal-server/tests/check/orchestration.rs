//! Three-phase orchestration: parse cascade, validate-targeted skip,
//! database-unreachable cascade.

use tribal::{App, AppError};
use tribal_config::TribalConfig;
use tribal_domain::{ProviderKind, normalise_endpoint_url};
use tribal_test_utils::{TestDb, ensure_genesis_profile_with_endpoint, env_lock};

use super::common::{
    CheckRun, TestEnv, fresh_db, names, parse_json, row_status, run_check, statuses, write_config,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_parse_failure_cascades_skip_to_every_other_check() {
    let _env_lock = env_lock().await;
    let env = TestEnv::new();
    // Malformed YAML — the loader fails before any field is read.
    std::fs::write(&env.config_path, "not: : valid: yaml: :").expect("write");

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
    assert_eq!(output["ok"], serde_json::Value::Bool(false));

    let statuses = statuses(&output);
    assert_eq!(statuses.len(), 9, "expected 9 rows, got {statuses:?}");
    assert_eq!(statuses[0], "fail", "config_parse must be the failed row");
    for (i, s) in statuses.iter().enumerate().skip(1) {
        assert_eq!(s, "skip", "row {i} should be skip, got {s:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_validate_failure_targeted_skip_for_advertised_url() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    // stdio + bind_address conflict trips a `server.bind_address`
    // validation error; the orchestrator must skip the advertised-URL
    // probe for that reason.
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.server.bind_address = Some("127.0.0.1:8080".into());
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
    assert_eq!(row_status(&output, "config_validate"), Some("fail"));
    assert_eq!(
        row_status(&output, "advertised_url_reachable"),
        Some("skip")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_validate_failure_targeted_skip_for_provider_under_providers_flag() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    // An inference stage missing its api key trips a config-validate failure
    // and a targeted skip of that stage's provider probe.
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.inference.triage.provider = ProviderKind::OpenAi;
    config.inference.triage.api_key = None;
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: true,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    assert_eq!(row_status(&output, "config_validate"), Some("fail"));
    assert_eq!(row_status(&output, "provider_triage"), Some("skip"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_keyless_embedding_genesis_fails_the_provider_probe() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    // An OpenAI embedding genesis resolves its credential through the
    // catalogue; with no matching entry the re-homed probe fails closed
    // rather than being skipped, and the config itself stays valid.
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.init.embedding.provider = ProviderKind::OpenAi;
    config.init.embedding.model = "text-embedding-3-small".to_owned();
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: true,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    assert_eq!(row_status(&output, "config_validate"), Some("pass"));
    assert_eq!(row_status(&output, "provider_embedding"), Some("fail"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_database_unreachable_cascades_skip_to_db_dependent_checks() {
    let _env_lock = env_lock().await;
    let env = TestEnv::new();
    // Valid config; the database URL points at a host that won't
    // resolve, so phase 3's `database_reachable` fails and the
    // pool-dependent checks cascade to skip.
    let config = TribalConfig::minimum_valid("postgres://no-such-host.invalid:5432/db");
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
    assert_eq!(row_status(&output, "database_reachable"), Some("fail"));
    for name in [
        "migrations_current",
        "project_resolution",
        "valid_token_exists",
    ] {
        assert_eq!(
            row_status(&output, name),
            Some("skip"),
            "{name} should skip when database is unreachable",
        );
    }
    // advertised_url and binary_uniqueness are orthogonal to the pool;
    // they run regardless of database availability.  The binary check's
    // specific outcome depends on whether `tribal` is on PATH (CI may
    // not have it installed), so pass-or-warn proves the row ran.
    assert_eq!(
        row_status(&output, "advertised_url_reachable"),
        Some("skip")
    );
    let binary = row_status(&output, "binary_uniqueness");
    assert!(
        matches!(binary, Some("pass") | Some("warn")),
        "binary_uniqueness should run (pass or warn), got {binary:?}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_providers_flag_off_omits_provider_rows() {
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
    let names = names(&output);
    for provider_name in [
        "provider_embedding",
        "provider_extraction",
        "provider_triage",
        "provider_relation",
    ] {
        assert!(
            !names.iter().any(|n| n == provider_name),
            "{provider_name} must be omitted without --providers; got {names:?}",
        );
    }
    assert!(
        names.iter().any(|n| n == "embedding_profile"),
        "embedding_profile must appear; got {names:?}",
    );
    assert_eq!(names.len(), 9, "expected 9 rows without --providers");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_providers_flag_on_emits_four_provider_rows() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let _pool = fresh_db(&ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, _stderr, _output) = run_check(CheckRun {
        config_path: &env.config_path,
        json: true,
        providers: true,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    let output = parse_json(&stdout);
    let names = names(&output);
    for provider_name in [
        "provider_embedding",
        "provider_extraction",
        "provider_triage",
        "provider_relation",
    ] {
        assert!(
            names.iter().any(|n| n == provider_name),
            "{provider_name} must appear under --providers; got {names:?}",
        );
    }
    assert_eq!(names.len(), 13, "expected 13 rows with --providers");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_embedding_profile_reports_the_active_seed() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let pool = fresh_db(&ctx).await;
    let mut conn = pool.acquire().await.expect("acquire connection");
    let base_url = normalise_endpoint_url(ProviderKind::Ollama.default_base_url()).unwrap();
    // A local Ollama seed needs no credential, so the active profile resolves.
    ensure_genesis_profile_with_endpoint(
        &mut conn,
        ProviderKind::Ollama,
        "nomic-embed-text:v1.5",
        768,
        &base_url,
    )
    .await;
    drop(conn);

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
    assert_eq!(row_status(&output, "embedding_profile"), Some("pass"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_embedding_profile_fails_on_an_unresolvable_active_credential() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let env = TestEnv::new();
    let pool = fresh_db(&ctx).await;
    let mut conn = pool.acquire().await.expect("acquire connection");
    let base_url = normalise_endpoint_url(ProviderKind::OpenAi.default_base_url()).unwrap();
    // An OpenAI active profile with no matching catalogue entry is the
    // fail-closed condition the next boot would trip.
    ensure_genesis_profile_with_endpoint(
        &mut conn,
        ProviderKind::OpenAi,
        "text-embedding-3-small",
        1536,
        &base_url,
    )
    .await;
    drop(conn);

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
    assert_eq!(row_status(&output, "embedding_profile"), Some("fail"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_app_run_returns_check_failed_when_any_row_fails() {
    // app.run() reads process-global env (PATH, XDG, figment layers), so hold
    // env_lock to serialise against the env-mutating sibling tests in this
    // binary under `cargo test`. app.run() builds its own runtime, so invoke it
    // via spawn_blocking to avoid nesting runtimes.
    let _env = env_lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let config_path = dir.path().join("tribal.yaml");
    std::fs::write(&config_path, "not: : valid: yaml: :").expect("write malformed yaml");

    let app = App::try_from_args([
        "tribal",
        "--config",
        config_path.to_str().expect("utf8 path"),
        "check",
        "--json",
    ])
    .expect("parse args");

    let result = tokio::task::spawn_blocking(move || app.run())
        .await
        .expect("run task");
    assert!(
        matches!(result, Err(AppError::CheckFailed)),
        "expected CheckFailed, got: {result:?}",
    );
}
