use sqlx::PgPool;
use tribal_config::{
    DEFAULT_EMBEDDING_DIMENSIONS, InferenceStage, PromptSource, ProviderConnectionConfig,
    TribalConfig,
};
use tribal_domain::{ProviderConnectionName, ProviderKind, normalise_endpoint_url};
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

const EMBEDDING_CONNECTION: &str = "e2e_embedding";
const EXTRACTION_CONNECTION: &str = "e2e_extraction";
const TRIAGE_CONNECTION: &str = "e2e_triage";
const RELATION_CONNECTION: &str = "e2e_relation";

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
    database_url.clone_into(&mut config.database.url);
    config.database.max_connect_attempts = 1;
    config.database.pool_mcp_max_connections = POOL_MCP_MAX_CONNECTIONS;
    config.database.pool_worker_max_connections = POOL_WORKER_MAX_CONNECTIONS;

    // -- Providers -----------------------------------------------------------
    // The genesis seed points the embedding identity at the wiremock; a
    // concrete dimension keeps the synthetic mock vectors deterministic.
    config.init.embedding.connection =
        insert_ollama_connection(&mut config, EMBEDDING_CONNECTION, embedding_url);
    config.init.embedding.dimensions = Some(DEFAULT_EMBEDDING_DIMENSIONS);
    config.inference.extraction.connection =
        insert_ollama_connection(&mut config, EXTRACTION_CONNECTION, extraction_url);
    config.inference.triage.connection =
        insert_ollama_connection(&mut config, TRIAGE_CONNECTION, triage_url);
    config.inference.relation.connection =
        insert_ollama_connection(&mut config, RELATION_CONNECTION, relation_url);

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

/// Switches the genesis embedding identity to `OpenAI` for an E2E test and
/// replaces its named connection with the matching endpoint and credential.
///
/// The runtime resolves the live embedding identity from the active profile
/// (seeded from `init.embedding`) and its credential from the connection, so a
/// test exercising the `OpenAI` path sets both here, in its config override,
/// against the embedding wiremock the harness already mounted.
pub fn use_openai_embedding(config: &mut TribalConfig, api_key: &str) {
    let connection = config.init.embedding.connection.clone();
    replace_connection_provider(config, connection, ProviderKind::OpenAi, Some(api_key));
}

/// Changes one inference stage's named connection while preserving its mock endpoint.
pub fn use_inference_provider(
    config: &mut TribalConfig,
    stage: InferenceStage,
    provider: ProviderKind,
    api_key: Option<&str>,
) {
    let connection = match stage {
        InferenceStage::Extraction => config.inference.extraction.connection.clone(),
        InferenceStage::Triage => config.inference.triage.connection.clone(),
        InferenceStage::Relation => config.inference.relation.connection.clone(),
    };
    replace_connection_provider(config, connection, provider, api_key);
}

/// Resolves the provider owned by one inference stage's named connection.
pub fn inference_provider(config: &TribalConfig, stage: InferenceStage) -> ProviderKind {
    let connection = match stage {
        InferenceStage::Extraction => &config.inference.extraction.connection,
        InferenceStage::Triage => &config.inference.triage.connection,
        InferenceStage::Relation => &config.inference.relation.connection,
    };
    config
        .provider_connections
        .require(connection)
        .expect("E2E stage connection must exist")
        .provider()
}

/// Resolves the provider owned by the genesis embedding connection.
pub fn embedding_provider(config: &TribalConfig) -> ProviderKind {
    config
        .provider_connections
        .require(&config.init.embedding.connection)
        .expect("E2E embedding connection must exist")
        .provider()
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
    let connection = config
        .provider_connections
        .require(&init.connection)
        .expect("E2E embedding connection must exist");
    let provider = connection.provider();
    let base_url = connection
        .base_url()
        .or_else(|| provider.default_base_url())
        .expect("E2E embedding connection must expose an endpoint");
    let normalised = normalise_endpoint_url(base_url).expect("embedding endpoint must normalise");
    let dimensions = resolve_dimensions(provider, &init.model, init.dimensions)
        .expect("init.embedding dimensions must resolve");

    let mut conn = pool
        .acquire()
        .await
        .expect("acquire connection for genesis");
    ensure_genesis_profile_with_endpoint(&mut conn, provider, &init.model, dimensions, &normalised)
        .await;
}

fn insert_ollama_connection(
    config: &mut TribalConfig,
    name: &str,
    base_url: &str,
) -> ProviderConnectionName {
    let name = ProviderConnectionName::parse(name).expect("E2E connection name is valid");
    config.provider_connections.insert(
        name.clone(),
        ProviderConnectionConfig::Ollama {
            base_url: base_url.to_owned(),
        },
    );
    name
}

fn replace_connection_provider(
    config: &mut TribalConfig,
    name: ProviderConnectionName,
    provider: ProviderKind,
    api_key: Option<&str>,
) {
    let base_url = config
        .provider_connections
        .require(&name)
        .expect("E2E connection must exist")
        .base_url()
        .map(ToOwned::to_owned)
        .or_else(|| provider.default_base_url().map(ToOwned::to_owned))
        .expect("E2E provider must expose an endpoint");
    let api_key = api_key.map(|value| value.parse().expect("test fixture api key is valid"));
    let connection = match provider {
        ProviderKind::Ollama => ProviderConnectionConfig::Ollama { base_url },
        ProviderKind::Anthropic => ProviderConnectionConfig::Anthropic { base_url, api_key },
        ProviderKind::OpenAi => ProviderConnectionConfig::OpenAi { base_url, api_key },
        ProviderKind::Platform => ProviderConnectionConfig::Platform {},
    };
    config.provider_connections.insert(name, connection);
}
