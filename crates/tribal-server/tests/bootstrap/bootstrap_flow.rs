//! End-to-end tests for `tribal bootstrap`.

use tribal::TokenStrategy;
use tribal_common::sha256_hex;
use tribal_config::{
    CliOverrides, ENV_OPENAI_API_KEY, EmbeddingCliOverrides, InitCliOverrides,
    TelemetryCliOverrides, TransportKind,
};
use tribal_db::{
    AuthTokenRepository, PgAuthTokenRepository, PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, ProviderKind};
use tribal_test_utils::{TestDb, env_lock};

use super::common::{
    EnvGuard, TestEnv, fresh_db, parse_json, run_bootstrap, run_mcp_config, run_setup,
};

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

/// Bootstrap → mcp-config must produce a byte-identical `mcp_config`
/// shape — the shared snippet builder is the single source of truth.
#[tokio::test]
async fn test_bootstrap_then_mcp_config_round_trip_stdio() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();

    let (boot_stdout, _) = run_bootstrap(
        &ctx,
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
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Stdio,
        TokenStrategy::Auto,
    )
    .await
    .expect("mcp-config succeeds");

    let mc_json = parse_json(&mc_stdout);
    assert_eq!(boot_mcp, mc_json, "stdio mcp_config round-trip");
}

/// The round-trip property for the loopback http default: both commands
/// advertise the URL-only OAuth snippet (no `Authorization` header), so the
/// byte-identical shape is the OAuth default with nothing to copy. The
/// embedded-token round-trip is covered by the DCR-disabled case below.
#[tokio::test]
async fn test_bootstrap_then_mcp_config_round_trip_http() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();

    let (boot_stdout, _) = run_bootstrap(
        &ctx,
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
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Auto,
    )
    .await
    .expect("mcp-config succeeds");

    let mc_json = parse_json(&mc_stdout);
    assert_eq!(boot_mcp, mc_json, "http mcp_config round-trip");
}

/// The byte-identical round-trip with an actually-embedded token. A
/// loopback surface with DCR disabled is not URL-only onboarding, so
/// bootstrap embeds the minted bearer and mcp-config `Auto` reads the same
/// token back from credentials.json. Both snippets must carry the identical
/// `Authorization` header, guarding the cross-command embedded-token
/// property that the URL-only default no longer exercises.
#[tokio::test]
async fn test_bootstrap_then_mcp_config_round_trip_http_embeds_token() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    // DCR disabled on a loopback surface is not URL-only onboarding, so
    // both commands embed the static token. The nested-env name follows the
    // `TRIBAL_<SECTION>__<FIELD>` mapping the figment loader honours.
    let _dcr_guard = EnvGuard::set("TRIBAL_OAUTH__DCR_ENABLED", "false");

    let (boot_stdout, _) = run_bootstrap(
        &ctx,
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
        &ctx,
        &env.config_path,
        CliOverrides::default(),
        Some(project_id),
        TransportKind::Http,
        TokenStrategy::Auto,
    )
    .await
    .expect("mcp-config succeeds");

    let mc_json = parse_json(&mc_stdout);
    assert_eq!(boot_mcp, mc_json, "http embedded-token round-trip");
    assert!(
        mc_json["headers"]["Authorization"]
            .as_str()
            .is_some_and(|header| header.starts_with("Bearer ")),
        "a non-URL-only surface must embed the bearer on both sides: {mc_json}",
    );
}

// ---------------------------------------------------------------------------
// --principal provisioning
// ---------------------------------------------------------------------------

/// `--principal user:alice` must provision both `principal:local`
/// (so stdio auth keeps working) and `user:alice` (the explicit
/// token holder).
#[tokio::test]
async fn test_bootstrap_with_explicit_principal_provisions_both() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let pool = fresh_db(&ctx).await;
    let env = TestEnv::new();

    let (stdout, _stderr) = run_bootstrap(
        &ctx,
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
        .expect("query alice principal")
        .expect("user:alice must be provisioned");

    assert!(local.is_some(), "principal:local must be provisioned");

    // The minted token must be issued against `user:alice`, not the
    // ambient `principal:local`. Verified both via the JSON payload's
    // declared principal_key and by looking the token row up by hash
    // and asserting its principal_id matches alice's.
    let payload = parse_json(&stdout);
    assert_eq!(
        payload["principal_key"].as_str(),
        Some("user:alice"),
        "bootstrap --json must report the token's principal as user:alice: {payload}",
    );
    let bearer = payload["bearer_token"]
        .as_str()
        .expect("bootstrap --json carries bearer_token");
    let token_hash = sha256_hex(bearer);
    let token_row = PgAuthTokenRepository
        .find_by_hash(&mut conn, &token_hash)
        .await
        .expect("query minted token")
        .expect("minted token row must exist");
    assert_eq!(
        token_row.principal_id(),
        alice.id(),
        "minted token must be owned by user:alice, not principal:local",
    );
}

// ---------------------------------------------------------------------------
// Genesis seed: OpenAI embedding provider without an API key
// ---------------------------------------------------------------------------

/// Builds genesis-seed overrides pinning the OpenAI embedding provider.
fn openai_embedding_overrides(model: Option<&str>) -> CliOverrides {
    CliOverrides {
        init: Some(InitCliOverrides {
            embedding: Some(EmbeddingCliOverrides {
                provider: Some(ProviderKind::OpenAi),
                model: model.map(str::to_owned),
            }),
        }),
        ..CliOverrides::default()
    }
}

/// Seeding the OpenAI embedding provider with no API key now bootstraps
/// successfully: the genesis identity is persisted alongside a keyless
/// `openai_default` catalogue skeleton, and the credential is resolved
/// fail-closed at boot, not at bootstrap.
#[tokio::test]
async fn test_bootstrap_openai_without_key_persists_a_keyless_skeleton() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let _api_key_guard = EnvGuard::remove(ENV_OPENAI_API_KEY);

    run_bootstrap(
        &ctx,
        &env.config_path,
        openai_embedding_overrides(None),
        None,
        TransportKind::Stdio,
        true,
    )
    .await
    .expect("openai embedding bootstraps; the credential resolves at boot");

    let written = tokio::fs::read_to_string(&env.config_path)
        .await
        .expect("read config");
    assert!(
        written.contains("provider: openai"),
        "genesis seed persisted: {written}",
    );
    assert!(
        written.contains("openai_default"),
        "catalogue skeleton persisted: {written}",
    );
    assert!(
        !written.contains("api_key"),
        "the key is never persisted: {written}",
    );
}

/// The same through standalone `tribal setup`: with no embedding api-key
/// validation gate left, a keyless OpenAI seed no longer fails the setup
/// path.
#[tokio::test]
async fn test_setup_openai_without_key_succeeds() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    let _api_key_guard = EnvGuard::remove(ENV_OPENAI_API_KEY);

    run_setup(
        &ctx,
        &env.config_path,
        openai_embedding_overrides(None),
        None,
    )
    .await
    .expect("openai embedding sets up; the credential resolves at boot");
}

// ---------------------------------------------------------------------------
// First-run persistence symmetry
// ---------------------------------------------------------------------------

/// Bootstrap with persistable flags writes a `tribal.yaml`; a second
/// invocation with the same flags must leave the file byte-identical
/// (file-exists path, no silent rewrites).
#[tokio::test]
async fn test_bootstrap_persists_then_leaves_file_unchanged_on_second_run() {
    let _env = env_lock().await;
    let ctx = TestDb::new().await;
    let _pool = fresh_db(&ctx).await;
    let env = TestEnv::new();
    // The OpenAI provider requires an api_key at validation time; the
    // cascade picks it up from this env var.
    let _api_key_guard = EnvGuard::set(ENV_OPENAI_API_KEY, "test-key");

    let overrides = CliOverrides {
        init: Some(InitCliOverrides {
            embedding: Some(EmbeddingCliOverrides {
                provider: Some(ProviderKind::OpenAi),
                model: Some("text-embedding-3-small".into()),
            }),
        }),
        telemetry: Some(TelemetryCliOverrides {
            otlp_endpoint: Some("http://localhost:4317".into()),
        }),
        ..CliOverrides::default()
    };

    // -- First run writes the file ------------------------------------------
    let _ = run_bootstrap(
        &ctx,
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

    // The persisted YAML must contain the database URL plus the
    // explicitly-passed persistable fields, and must omit unpassed
    // families (inference, server). Substring on top-level keys —
    // sufficient against the renderer's deterministic output.
    assert!(
        first_content.contains("database:"),
        "database section is present: {first_content}",
    );
    assert!(
        first_content.contains("embedding:"),
        "embedding section is present: {first_content}",
    );
    assert!(
        first_content.contains("telemetry:"),
        "telemetry section is present: {first_content}",
    );
    assert!(
        !first_content.contains("inference:"),
        "inference section must be absent: {first_content}",
    );
    assert!(
        !first_content.contains("server:"),
        "server section must be absent: {first_content}",
    );

    // -- Second run finds the file, leaves it byte-identical ----------------
    let _ = run_bootstrap(
        &ctx,
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
