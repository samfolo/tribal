//! Process-level shared state for the Tribal server.
//!
//! [`AppState`] is constructed once during startup and shared across all
//! MCP connections and the worker runtime.  Individual connection handlers
//! wrap an `Arc<AppState>` alongside per-connection state (session,
//! repositories).

use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tribal_common::JobStateTxs;
use tribal_config::{AgentsConfig, ServerConfig, WorkerConfig};
use tribal_domain::{PipelineParameters, ProjectId, ProjectOrigin};
use tribal_inference::{CompletionStageSpecs, InferenceGateway, ProviderIdentity};
use tribal_telemetry::MetricsRecorder;
use typed_builder::TypedBuilder;

use crate::{server_handler::ActivePromptVersions, session::SessionProject};

// ---------------------------------------------------------------------------
// ResolvedProject
// ---------------------------------------------------------------------------

/// Project context resolved during startup.
#[derive(Debug, Clone, TypedBuilder)]
pub struct ResolvedProject {
    /// Database identifier for the project.
    #[builder(setter(into))]
    pub(crate) id: ProjectId,

    /// Human-friendly project name.
    #[builder(setter(into))]
    pub(crate) name: String,

    /// Persisted source of project identity.
    pub(crate) origin: ProjectOrigin,
}

impl ResolvedProject {
    /// Returns the project's database identifier.
    #[must_use]
    pub fn id(&self) -> ProjectId {
        self.id
    }

    /// Returns the project's human-friendly name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the persisted source of project identity.
    #[must_use]
    pub fn origin(&self) -> &ProjectOrigin {
        &self.origin
    }
}

impl From<&ResolvedProject> for SessionProject {
    fn from(rp: &ResolvedProject) -> Self {
        Self {
            id: rp.id,
            name: rp.name.clone(),
            origin: rp.origin.clone(),
        }
    }
}

/// Immutable project defaults installed before the server accepts traffic.
#[derive(Debug, Clone, TypedBuilder)]
pub struct ProjectDefaults {
    /// The graph-owned System project.
    pub(crate) system: ResolvedProject,
    /// Project selected for this process, when startup resolved one.
    #[builder(default, setter(strip_option))]
    pub(crate) process: Option<ResolvedProject>,
}

impl ProjectDefaults {
    /// Returns the graph-owned System project.
    #[must_use]
    pub fn system(&self) -> &ResolvedProject {
        &self.system
    }

    /// Returns the process project, when startup selected one.
    #[must_use]
    pub fn process(&self) -> Option<&ResolvedProject> {
        self.process.as_ref()
    }
}

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// Process-level state shared across all MCP connections and the worker.
///
/// Constructed once during startup and wrapped in `Arc` for sharing.
// Several fields are wired at startup but consumed by transport code in
// later milestones.
#[allow(dead_code)]
#[derive(TypedBuilder)]
pub struct AppState {
    // -- Pools ---------------------------------------------------------------
    /// MCP read-path connection pool.
    pub(crate) pool_mcp: PgPool,

    /// Worker write-path connection pool.
    pub(crate) pool_worker: PgPool,

    // -- Identity ------------------------------------------------------------
    /// Unique instance identifier: `{hostname}~{pid}~{boot_id}`.
    ///
    /// Written to `tasks.claimed_by` by the worker on every task claim.
    pub(crate) instance_id: Arc<str>,

    // -- Prompts -------------------------------------------------------------
    /// Active prompt version IDs, wrapped in `RwLock` for hot-reload.
    pub(crate) active_prompt_versions: Arc<RwLock<ActivePromptVersions>>,

    // -- Providers -----------------------------------------------------------
    /// The inference gateway: the one port every completion and embedding
    /// call routes through. The discover read path embeds queries through
    /// it, and the reindex tools resolve target providers through it.
    pub(crate) gateway: Arc<InferenceGateway>,

    /// The active embedding identity snapshotted at boot, recorded as
    /// fingerprint provenance alongside the stage binding hashes.
    pub(crate) embedding_identity: ProviderIdentity,

    // -- Fingerprint ----------------------------------------------------------
    /// Git-describe version of the build, used for fingerprint computation.
    pub(crate) build_version: Arc<str>,

    /// The boot-time stage endpoint specs, whose post-reconcile sampling
    /// parameters feed the stage binding hashes the fingerprint records.
    pub(crate) stage_specs: CompletionStageSpecs,

    /// The active embedding dimensionality snapshotted at boot.
    pub(crate) embedding_dimensions: u32,

    /// Job-level pipeline parameters for fingerprint computation.
    pub(crate) pipeline_parameters: PipelineParameters,

    /// The agentic execution configuration the stage binding hashes
    /// derive from.
    pub(crate) agents_config: AgentsConfig,

    // -- Config --------------------------------------------------------------
    /// Worker configuration (concurrency, timeouts, thresholds).
    pub(crate) worker_config: WorkerConfig,

    /// Server configuration (transport, bind address, shutdown deadline).
    pub(crate) server_config: Arc<ServerConfig>,

    // -- Coordination --------------------------------------------------------
    /// Cancellation token shared across the MCP and worker runtimes.
    ///
    /// Triggered to initiate graceful shutdown of all subsystems.
    pub(crate) cancellation_token: CancellationToken,

    /// Per-job watch channels shared between worker and MCP handlers.
    ///
    /// The worker sends typed [`tribal_domain::JobState`] transitions;
    /// MCP handlers subscribe to observe progress. A background sweep
    /// task evicts stale entries based on TTL thresholds.
    pub(crate) job_state_txs: JobStateTxs,

    // -- Observability -------------------------------------------------------
    /// Telemetry metric instruments.
    pub(crate) metrics: Arc<dyn MetricsRecorder>,

    // -- Projects ------------------------------------------------------------
    /// Immutable graph and process project defaults.
    pub(crate) project_defaults: ProjectDefaults,
}

impl AppState {
    /// Returns a reference to the MCP read-path connection pool.
    #[must_use]
    pub fn mcp_pool(&self) -> &PgPool {
        &self.pool_mcp
    }

    /// Returns a reference to the worker write-path connection pool.
    #[must_use]
    pub fn worker_pool(&self) -> &PgPool {
        &self.pool_worker
    }

    /// Returns a reference to the telemetry metric instruments.
    #[must_use]
    pub fn metrics(&self) -> &dyn MetricsRecorder {
        &self.metrics
    }

    /// Returns the immutable startup project defaults.
    #[must_use]
    pub fn project_defaults(&self) -> &ProjectDefaults {
        &self.project_defaults
    }

    /// Returns a reference to the shared active prompt versions lock.
    #[must_use]
    pub fn active_prompt_versions(&self) -> &Arc<RwLock<ActivePromptVersions>> {
        &self.active_prompt_versions
    }

    /// Returns the per-serve instance identity (`{hostname}~{pid}~{boot}`).
    #[must_use]
    pub fn instance_id(&self) -> &Arc<str> {
        &self.instance_id
    }

    /// Returns the binary's build version (git-describe).
    #[must_use]
    pub fn build_version(&self) -> &Arc<str> {
        &self.build_version
    }
}
