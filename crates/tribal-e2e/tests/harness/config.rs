use tribal_config::TribalConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const POLL_INTERVAL_MS: u64 = 100;
const HEARTBEAT_INTERVAL_MS: u64 = 200;
const TASK_TIMEOUT_MS: u64 = 5_000;
const RECLAIM_INTERVAL_MS: u64 = 500;

// ---------------------------------------------------------------------------
// Config builder
// ---------------------------------------------------------------------------

/// Constructs a [`TribalConfig`] for E2E testing.
///
/// Starts from compiled defaults and overrides database URL, provider
/// base URLs, prompts directory, and worker timings for fast test
/// execution.
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

    // -- Providers -----------------------------------------------------------
    config.embedding.base_url = Some(embedding_url.to_owned());
    config.inference.extraction.base_url = Some(extraction_url.to_owned());
    config.inference.triage.base_url = Some(triage_url.to_owned());
    config.inference.relation.base_url = Some(relation_url.to_owned());

    // -- Prompts -------------------------------------------------------------
    config.prompts.directory = prompts_dir.to_owned();

    // -- Worker timings ------------------------------------------------------
    config.worker.poll_interval_ms = POLL_INTERVAL_MS;
    config.worker.heartbeat_interval_ms = HEARTBEAT_INTERVAL_MS;
    config.worker.task_timeout_ms = TASK_TIMEOUT_MS;
    config.worker.reclaim_interval_ms = RECLAIM_INTERVAL_MS;

    config
}
