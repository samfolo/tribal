//! Three-phase orchestration: parse cascade, validate-targeted skip,
//! database-unreachable cascade.

use tribal_config::{ProviderKind, TribalConfig};
use tribal_test_utils::{serial_lock, test_context};

use super::common::{
    CheckRun, TestEnv, fresh_db, names, parse_json, row_status, run_check, statuses, write_config,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_parse_failure_cascades_skip_to_every_other_check() {
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    // Malformed YAML — the loader fails before any field is read.
    std::fs::write(&env.config_path, "not: : valid: yaml: :").expect("write");

    let (stdout, _stderr) = run_check(CheckRun {
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
    assert_eq!(statuses.len(), 8, "expected 8 rows, got {statuses:?}");
    assert_eq!(statuses[0], "fail", "config_parse must be the failed row");
    for (i, s) in statuses.iter().enumerate().skip(1) {
        assert_eq!(s, "skip", "row {i} should be skip, got {s:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_validate_failure_targeted_skip_for_advertised_url() {
    let ctx = test_context().await;
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    let _pool = fresh_db(ctx).await;
    // stdio + bind_address conflict trips a `server.bind_address`
    // validation error; the orchestrator must skip the advertised-URL
    // probe for that reason.
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.server.bind_address = Some("127.0.0.1:8080".into());
    write_config(&env.config_path, &config);

    let (stdout, _stderr) = run_check(CheckRun {
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
    let ctx = test_context().await;
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    let _pool = fresh_db(ctx).await;
    // OpenAI embedding requires `api_key`; omitting it trips a
    // targeted skip for the embedding provider probe.
    let mut config = TribalConfig::minimum_valid(ctx.database_url());
    config.embedding.provider = ProviderKind::OpenAi;
    config.embedding.api_key = None;
    write_config(&env.config_path, &config);

    let (stdout, _stderr) = run_check(CheckRun {
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
    assert_eq!(row_status(&output, "provider_embedding"), Some("skip"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_database_unreachable_cascades_skip_to_db_dependent_checks() {
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    // Valid config; the database URL points at a host that won't
    // resolve, so phase 3's `database_reachable` fails and the
    // pool-dependent checks cascade to skip.
    let config = TribalConfig::minimum_valid("postgres://no-such-host.invalid:5432/db");
    write_config(&env.config_path, &config);

    let (stdout, _stderr) = run_check(CheckRun {
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
    // they run regardless of database availability.
    assert_eq!(
        row_status(&output, "advertised_url_reachable"),
        Some("skip")
    );
    assert_eq!(row_status(&output, "binary_uniqueness"), Some("pass"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_providers_flag_off_omits_provider_rows() {
    let ctx = test_context().await;
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    let _pool = fresh_db(ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, _stderr) = run_check(CheckRun {
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
    assert_eq!(names.len(), 8, "expected 8 rows without --providers");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_providers_flag_on_emits_four_provider_rows() {
    let ctx = test_context().await;
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    let _pool = fresh_db(ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, _stderr) = run_check(CheckRun {
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
    assert_eq!(names.len(), 12, "expected 12 rows with --providers");
}
