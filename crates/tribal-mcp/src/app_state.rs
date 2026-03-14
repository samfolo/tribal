//! Process-level shared state for the Tribal server.
//!
//! [`AppState`] is constructed once during startup and shared across all
//! MCP connections and the worker runtime.  Individual connection handlers
//! wrap an `Arc<AppState>` alongside per-connection state (session,
//! repositories).

use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::RwLock;
use tribal_config::{ServerConfig, WorkerConfig};
use tribal_domain::ProjectId;
use tribal_inference::{EmbeddingProvider, InferenceProvider, ProviderKey, ProviderRegistry};

use crate::server_handler::ActivePromptVersions;

// ---------------------------------------------------------------------------
// ResolvedProject
// ---------------------------------------------------------------------------

/// Project context resolved during startup.
///
/// Populated when the startup cascade (CLI flag, env var, or git remote
/// heuristic) successfully identifies a registered project.
#[derive(Debug, Clone)]
pub struct ResolvedProject {
    /// Database identifier for the project.
    pub id: ProjectId,

    /// Human-friendly project name.
    pub name: String,

    /// Normalised git remote URL used as the project's stable identity.
    pub git_remote: String,
}

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// Process-level state shared across all MCP connections and the worker.
///
/// Constructed once during startup and wrapped in `Arc` for sharing.
pub struct AppState {
    // -- Pools ---------------------------------------------------------------
    /// MCP read-path connection pool.
    pub pool_mcp: PgPool,

    /// Worker write-path connection pool.
    pub pool_worker: PgPool,

    // -- Identity ------------------------------------------------------------
    /// Unique instance identifier: `{hostname}-{pid}-{boot_id}`.
    ///
    /// Written to `tasks.claimed_by` by the worker on every task claim.
    pub instance_id: Arc<str>,

    // -- Prompts -------------------------------------------------------------
    /// Active prompt version IDs, wrapped in `RwLock` for hot-reload.
    pub active_prompt_versions: Arc<RwLock<ActivePromptVersions>>,

    // -- Providers -----------------------------------------------------------
    /// Provider registry (semaphores and HTTP clients).
    pub provider_registry: Arc<ProviderRegistry>,

    /// Embedding provider instance.
    pub embedding_provider: Arc<dyn EmbeddingProvider>,

    /// Extraction stage inference provider.
    pub extraction_provider: Arc<dyn InferenceProvider>,

    /// Triage stage inference provider.
    pub triage_provider: Arc<dyn InferenceProvider>,

    /// Relation stage inference provider.
    pub relation_provider: Arc<dyn InferenceProvider>,

    // -- Provider keys (1 per config section) --------------------------------
    /// Registry key for the embedding provider.
    pub embedding_key: ProviderKey,

    /// Registry key for the extraction inference provider.
    pub extraction_key: ProviderKey,

    /// Registry key for the triage inference provider.
    pub triage_key: ProviderKey,

    /// Registry key for the relation inference provider.
    pub relation_key: ProviderKey,

    // -- Config --------------------------------------------------------------
    /// Worker configuration (concurrency, timeouts, thresholds).
    pub worker_config: WorkerConfig,

    /// Server configuration (transport, bind address, shutdown deadline).
    pub server_config: Arc<ServerConfig>,

    // -- Session -------------------------------------------------------------
    /// Resolved project context from the startup cascade, if any.
    pub resolved_project: Option<ResolvedProject>,
}
