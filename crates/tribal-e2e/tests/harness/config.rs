use sqlx::PgPool;
use tribal_config::{CredentialEntry, PromptSource, TribalConfig};
use tribal_domain::normalise_endpoint_url;
use tribal_inference::resolve_dimensions;
use tribal_test_utils::ensure_genesis_profile_with_endpoint;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Read-path pool size for MCP handlers.
const POOL_MCP_MAX_CONNECTIONS: u32 = 4;

/// Write-path pool size for the worker.
///
/// Must satisfy `max_concurrent_tasks + POOL_CONNECTION_OVERHEAD` from
/// `tribal-config` validation (default 4 + 4 = 8).
const POOL_WORKER_MAX_CONNECTIONS: u32 = 8;

/// Milliseconds between worker poll cycles.
const POLL_INTERVAL_MS: u64 = 100;

/// Milliseconds between heartbeat updates for claimed tasks.
const HEARTBEAT_INTERVAL_MS: u64 = 200;

/// Per-task timeout in milliseconds.
const TASK_TIMEOUT_MS: u64 = 5_000;

/// Milliseconds between stale-task reclaim sweeps.
const RECLAIM_INTERVAL_MS: u64 = 500;

/// Per-request timeout for provider calls.
///
/// Must be less than `TASK_TIMEOUT_MS` to satisfy validation.
const REQUEST_TIMEOUT_MS: u64 = 4_000;

// ---------------------------------------------------------------------------
// Config builder
// ---------------------------------------------------------------------------

/// Constructs a [`TribalConfig`] for E2E testing.
///
/// Starts from compiled defaults and applies overrides for fast,
/// deterministic test execution. Validation is deferred to
/// [`TestHarness::init`] so that per-test config overrides are
/// applied before the check runs.
pub fn test_config(
    database_url: &str,
    embedding_url: &str,
    extraction_url: &str,
    triage_url: &str,
    relation_url: &str,
    prompts_dir: &str,
) -> TribalConfig {
    let mut config = TribalConfig::default();

    // -- Database ------------------------------------------------------------
    config.database.url = database_url.to_owned();
    config.database.max_connect_attempts = 1;
    config.database.pool_mcp_max_connections = POOL_MCP_MAX_CONNECTIONS;
    config.database.pool_worker_max_connections = POOL_WORKER_MAX_CONNECTIONS;

    // -- Providers -----------------------------------------------------------
    config.embedding.base_url = Some(embedding_url.to_owned());
    config.inference.extraction.base_url = Some(extraction_url.to_owned());
    config.inference.triage.base_url = Some(triage_url.to_owned());
    config.inference.relation.base_url = Some(relation_url.to_owned());

    // -- Prompts -------------------------------------------------------------
    config.prompts.source = PromptSource::Disk {
        directory: prompts_dir.to_owned(),
        hot_reload: false,
    };

    // -- Worker timings ------------------------------------------------------
    config.worker.poll_interval_ms = POLL_INTERVAL_MS;
    config.worker.heartbeat_interval_ms = HEARTBEAT_INTERVAL_MS;
    config.worker.task_timeout_ms = TASK_TIMEOUT_MS;
    config.worker.reclaim_interval_ms = RECLAIM_INTERVAL_MS;

    // -- Provider limits -----------------------------------------------------
    for limits in config.limits.providers.values_mut() {
        limits.request_timeout_ms = REQUEST_TIMEOUT_MS;
    }

    // -- Discovery -----------------------------------------------------------
    // E2E embeddings are synthetic (deterministic but not semantically
    // meaningful), so the similarity threshold is set to the minimum
    // valid value to avoid discarding results during overfetch filtering.
    config.discovery.similarity_threshold = f64::MIN_POSITIVE;

    // -- Logging -------------------------------------------------------------
    config.logging.include_llm_content = true;

    config
}

/// Mirrors the resolved `config.embedding` identity into `init.embedding` (the
/// genesis seed the runtime provisions from) and, for a key-requiring provider,
/// a `<provider>_default` catalogue entry (the credential the runtime resolves
/// the active profile against).
///
/// E2E tests configure embedding through `config.embedding`, but the runtime
/// resolves the live embedding identity from the active profile and its
/// credential from the catalogue. Calling this after the per-test override keeps
/// the genesis profile, the registry, and the mounted embedding mock in
/// agreement.
pub fn mirror_embedding_into_init_and_catalogue(config: &mut TribalConfig) {
    config.init.embedding.provider = config.embedding.provider;
    config.init.embedding.model = config.embedding.model.clone();
    config.init.embedding.dimensions = Some(config.embedding.dimensions);
    config.init.embedding.base_url = config.embedding.base_url.clone();

    if config.embedding.provider.requires_api_key() {
        let base_url = config
            .embedding
            .base_url
            .clone()
            .unwrap_or_else(|| config.embedding.provider.default_base_url().to_owned());
        config.credentials.insert(
            format!("{}_default", config.embedding.provider.as_str()),
            CredentialEntry {
                provider_kind: config.embedding.provider,
                base_url,
                api_key: config.embedding.api_key.clone(),
            },
        );
    }
}

/// Seeds the genesis embedding profile from `init.embedding` so the seed graph
/// and the server's first-boot provisioning both reuse it.
///
/// Called after the config is resolved (so `init.embedding` reflects the test's
/// final embedding identity) and before the seed runs, the resulting active
/// profile carries the test's wiremock endpoint, which the server's provider
/// builder constructs against.
pub async fn seed_genesis_from_init(pool: &PgPool, config: &TribalConfig) {
    let init = &config.init.embedding;
    let base_url = init
        .base_url
        .clone()
        .unwrap_or_else(|| init.provider.default_base_url().to_owned());
    let normalised =
        normalise_endpoint_url(&base_url).expect("init.embedding.base_url must normalise");
    let dimensions = resolve_dimensions(init.provider, &init.model, init.dimensions)
        .expect("init.embedding dimensions must resolve");

    let mut conn = pool
        .acquire()
        .await
        .expect("acquire connection for genesis");
    ensure_genesis_profile_with_endpoint(
        &mut conn,
        init.provider,
        &init.model,
        dimensions,
        &normalised,
    )
    .await;
}
