//! Wire-format stability: the JSON shape downstream tooling parses.

use tribal_config::TribalConfig;
use tribal_test_utils::{serial_lock, test_context};

use super::common::{CheckRun, TestEnv, fresh_db, parse_json, run_check, write_config};

#[tokio::test(flavor = "multi_thread")]
async fn test_root_envelope_has_ok_and_checks_fields() {
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
    assert!(
        output.get("ok").is_some(),
        "envelope must expose `ok`: {output}"
    );
    assert!(
        output.get("checks").is_some(),
        "envelope must expose `checks`: {output}"
    );
    assert!(output["ok"].is_boolean(), "`ok` must be a boolean");
    assert!(output["checks"].is_array(), "`checks` must be an array");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_every_row_has_status_name_detail() {
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
    for row in output["checks"].as_array().expect("checks array") {
        assert!(row["status"].is_string(), "row missing `status`: {row}");
        assert!(row["name"].is_string(), "row missing `name`: {row}");
        assert!(row["detail"].is_string(), "row missing `detail`: {row}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_status_strings_are_lowercase_pass_warn_fail_skip() {
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
    for row in output["checks"].as_array().expect("checks array") {
        let status = row["status"].as_str().expect("status string");
        assert!(
            matches!(status, "pass" | "warn" | "fail" | "skip"),
            "unexpected status {status:?} in row {row}",
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pass_and_skip_rows_omit_remediation() {
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
    for row in output["checks"].as_array().expect("checks array") {
        let status = row["status"].as_str().expect("status string");
        match status {
            "pass" | "skip" => assert!(
                row.get("remediation").is_none(),
                "{status} row must omit remediation: {row}",
            ),
            "warn" | "fail" => assert!(
                row["remediation"].is_string(),
                "{status} row must carry remediation: {row}",
            ),
            other => panic!("unexpected status {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ok_is_true_when_no_row_is_fail() {
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
    let any_fail = output["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .any(|r| r["status"].as_str() == Some("fail"));
    assert_eq!(
        output["ok"].as_bool(),
        Some(!any_fail),
        "ok must equal !any_fail; got {output}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ok_is_false_when_any_row_is_fail() {
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    // Unreachable database guarantees a fail row.
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
    assert_eq!(output["ok"].as_bool(), Some(false));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_human_output_goes_to_stderr_and_stdout_is_empty() {
    let ctx = test_context().await;
    let _lock = serial_lock().await;
    let env = TestEnv::new();
    let _pool = fresh_db(ctx).await;
    let config = TribalConfig::minimum_valid(ctx.database_url());
    write_config(&env.config_path, &config);

    let (stdout, stderr) = run_check(CheckRun {
        config_path: &env.config_path,
        json: false,
        providers: false,
        project: None,
        token: None,
    })
    .await
    .expect("check runs");

    assert!(stdout.is_empty(), "stdout must be empty in human mode");
    assert!(
        !stderr.is_empty(),
        "stderr must carry the human-readable output"
    );
}
