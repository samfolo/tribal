//! Implementation of the `tribal serve` subcommand.
//!
//! Runs the full infrastructure bootstrap (database, migrations, providers,
//! prompts, project resolution) and assembles [`AppState`].

use std::{path::PathBuf, sync::Arc};

use tokio::{runtime::Runtime, sync::RwLock};
use tribal_config::{TribalConfig, load_config, validate};
use tribal_mcp::{AppState, HandlerConfig};

use crate::{
    cli::ServeArgs,
    error::AppError,
    startup::{
        POOL_NAME_MCP, POOL_NAME_WORKER, build_embedding_provider, build_inference_provider,
        build_provider_registry, check_first_run, create_pool_with_retry, ensure_prompt_files,
        generate_instance_id, load_prompts, resolve_project, run_migrations,
    },
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Runs the `tribal serve` startup sequence.
///
/// # Errors
///
/// Returns an [`AppError`] if any startup phase fails.
pub(crate) fn run(config_path: &str, args: ServeArgs) -> Result<(), AppError> {
    let (cli_overrides, cli_project) = args.into_cli_overrides();

    let config = load_config(config_path, Some(cli_overrides))?;
    validate(&config)?;

    let _handler_config = HandlerConfig::from(&config).with_pool_name(POOL_NAME_MCP);

    // Telemetry must be initialised before the async runtime so the guard
    // outlives `block_on` and flushes pending writes on shutdown.
    let _telemetry_guard = tribal_telemetry::init_subscriber(&config.logging)?;

    let rt = Runtime::new().map_err(|source| AppError::Runtime { source })?;

    rt.block_on(async { bootstrap(&config, cli_project).await })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/// Asynchronous startup sequence: pools, migrations, providers, prompts,
/// project resolution, and `AppState` assembly.
async fn bootstrap(config: &TribalConfig, cli_project: Option<String>) -> Result<(), AppError> {
    // -- Database pools ------------------------------------------------------

    let pool_mcp = create_pool_with_retry(
        &config.database,
        POOL_NAME_MCP,
        config.database.pool_mcp_max_connections,
        config.database.statement_timeout_mcp_ms,
        config.database.max_connect_attempts,
    )
    .await?;

    let pool_worker = create_pool_with_retry(
        &config.database,
        POOL_NAME_WORKER,
        config.database.pool_worker_max_connections,
        config.database.statement_timeout_worker_ms,
        config.database.max_connect_attempts,
    )
    .await?;

    // -- Migrations ----------------------------------------------------------

    check_first_run(&pool_mcp).await?;
    run_migrations(&pool_mcp).await?;

    // -- Instance identity ---------------------------------------------------

    let instance_id = generate_instance_id();

    // -- Prompts -------------------------------------------------------------

    let prompts_dir = expand_prompts_dir(&config.prompts.directory);
    ensure_prompt_files(&prompts_dir).await?;
    let active_prompt_versions = load_prompts(&pool_mcp, &prompts_dir).await?;

    // -- Providers -----------------------------------------------------------

    let registry = build_provider_registry(config)?;

    let (embedding_provider, embedding_key) =
        build_embedding_provider(&registry, &config.embedding).await?;

    let (extraction_provider, extraction_key) =
        build_inference_provider(&registry, &config.inference.extraction)?;

    let (triage_provider, triage_key) =
        build_inference_provider(&registry, &config.inference.triage)?;

    let (relation_provider, relation_key) =
        build_inference_provider(&registry, &config.inference.relation)?;

    // -- Project resolution --------------------------------------------------

    let resolved_project = resolve_project(&pool_mcp, cli_project).await?;

    // -- AppState assembly ---------------------------------------------------

    let base = AppState::builder()
        .pool_mcp(pool_mcp)
        .pool_worker(pool_worker)
        .instance_id(instance_id)
        .active_prompt_versions(Arc::new(RwLock::new(active_prompt_versions)))
        .provider_registry(Arc::new(registry))
        .embedding_provider(embedding_provider)
        .extraction_provider(extraction_provider)
        .triage_provider(triage_provider)
        .relation_provider(relation_provider)
        .embedding_key(embedding_key)
        .extraction_key(extraction_key)
        .triage_key(triage_key)
        .relation_key(relation_key)
        .worker_config(config.worker.clone())
        .server_config(Arc::new(config.server.clone()));

    let _state = Arc::new(match resolved_project {
        Some(project) => base.resolved_project(project).build(),
        None => base.build(),
    });

    tracing::info!("startup sequence complete");

    // Transport launch deferred to 6.4.

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Expands tilde (`~`) in the prompts directory path.
fn expand_prompts_dir(raw: &str) -> PathBuf {
    PathBuf::from(shellexpand::tilde(raw).as_ref())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_prompts_dir_no_tilde() {
        let result = expand_prompts_dir("/absolute/path/prompts");
        assert_eq!(result, PathBuf::from("/absolute/path/prompts"));
    }

    #[test]
    fn test_expand_prompts_dir_with_tilde() {
        let result = expand_prompts_dir("~/prompts");
        assert!(!result.to_str().unwrap().starts_with('~'));
        assert!(result.to_str().unwrap().ends_with("/prompts"));
    }
}
