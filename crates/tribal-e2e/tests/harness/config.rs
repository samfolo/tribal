use tribal_config::{PromptSource, TribalConfig};

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
