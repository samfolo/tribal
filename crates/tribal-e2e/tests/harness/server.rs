use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use rmcp::{
    ServiceExt,
    model::CallToolResult,
    service::{RoleClient, RunningService},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tribal::{ServerHandle, start_server};
use tribal_auth::{AuthContext, AuthenticatedPrincipal, TransportAuthStrategy};
use tribal_config::{
    DEFAULT_EMBEDDING_DIMENSIONS, DEFAULT_EMBEDDING_MODEL, TribalConfig, validate,
};
use tribal_db::{
    NewProject, PgPrincipalRepository, PgProjectRepository, PrincipalRepository, ProjectRepository,
};
use tribal_domain::{JobOutcome, PrincipalId, Project, ProviderKind};
use tribal_inference::RequestClass;
use tribal_mcp::{
    AppState, ConnectionRepositories, HandlerConfig, SessionContext, SessionProject,
    TribalServerHandler,
};
use tribal_telemetry::noop_recorder;
use tribal_test_utils::{Seed, TestDb, a_new_principal, a_new_project};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, method, path},
};

use super::{
    config::{seed_genesis_from_init, test_config},
    diagnostics::{DiagnosticContext, ServerDiagnostic},
    mocks::{
        envelope::{
            chat_path, embed_path, fixed_embedding_vector, tags_path, wrap_completion,
            wrap_embedding,
        },
        mounting::StageMountBuilder,
    },
    polling, tool_call,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Buffer size for the in-process duplex transport between server and client.
const DUPLEX_BUFFER_SIZE: usize = 65_536;

/// Principal key used when no override is provided.
pub const DEFAULT_PRINCIPAL_KEY: &str = "e2e-principal";

// ---------------------------------------------------------------------------
// Seed closure type
// ---------------------------------------------------------------------------

/// The stored form of an async seed closure.
///
/// The higher-ranked lifetime is necessary because the returned future
/// borrows `SeedContext` mutably — its lifetime is tied to the borrow.
type AsyncSeedFn = Box<
    dyn for<'a> FnOnce(&'a mut SeedContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> + Send,
>;

/// Boxed per-test config mutation closure.
type ConfigOverrideFn = Box<dyn FnOnce(&mut TribalConfig)>;

/// Synchronous closure that configures a [`Seed`] knowledge graph.
type GraphFn = Box<dyn FnOnce(Seed) -> Seed>;

// ---------------------------------------------------------------------------
// HarnessSetup
// ---------------------------------------------------------------------------

/// Configuration for [`TestHarness::init`]. Tests override only what they
/// care about — everything else uses sensible defaults.
pub struct HarnessSetup {
    principal_key: String,
    project: Option<NewProject>,
    no_project: bool,
    config_override: Option<ConfigOverrideFn>,
    seed: Option<AsyncSeedFn>,
    graph_fn: Option<GraphFn>,
}

impl HarnessSetup {
    fn new() -> Self {
        Self {
            principal_key: DEFAULT_PRINCIPAL_KEY.to_owned(),
            project: None,
            no_project: false,
            config_override: None,
            seed: None,
            graph_fn: None,
        }
    }

    /// Sets the principal key for this test.
    pub fn principal_key(&mut self, key: &str) {
        self.principal_key = key.to_owned();
    }

    /// Overrides the default project.
    pub fn project(&mut self, project: NewProject) {
        self.project = Some(project);
    }

    /// Suppresses default project creation and startup resolution.
    ///
    /// When set, `cli_project` is `None` and the server starts without a
    /// resolved project. `SeedContext::project_id()` and
    /// `SeedContext::project_git_remote()` will panic — manage projects
    /// manually via the seed closure.
    /// # Panics
    ///
    /// Panics if `graph()` has already been called.
    pub fn no_project(&mut self) {
        assert!(
            self.graph_fn.is_none(),
            "no_project() and graph() are mutually exclusive",
        );
        self.no_project = true;
    }

    /// Applies a per-test config override after defaults are set.
    pub fn config(&mut self, f: impl FnOnce(&mut TribalConfig) + 'static) {
        self.config_override = Some(Box::new(f));
    }

    /// Registers an async seed closure that runs after project and
    /// principal insertion. See the `seed!` macro for ergonomic usage.
    ///
    /// Mutually exclusive with [`graph()`](Self::graph).
    ///
    /// # Panics
    ///
    /// Panics if `graph()` has already been called.
    pub fn seed(&mut self, f: AsyncSeedFn) {
        assert!(
            self.graph_fn.is_none(),
            "seed() and graph() are mutually exclusive",
        );
        self.seed = Some(f);
    }

    /// Registers a declarative knowledge graph to seed before server
    /// startup.
    ///
    /// The closure receives a [`Seed`] pre-configured with a default
    /// project and the harness principal. Add items, relations, and
    /// other graph structure; the harness executes the seed, transfers
    /// item labels, and starts the server with the seeded project.
    ///
    /// Mutually exclusive with [`seed()`](Self::seed) and
    /// [`no_project()`](Self::no_project).
    ///
    /// # Panics
    ///
    /// Panics if `seed()` or `no_project()` has already been called.
    pub fn graph(&mut self, f: impl FnOnce(Seed) -> Seed + 'static) {
        assert!(
            self.seed.is_none(),
            "graph() and seed() are mutually exclusive",
        );
        assert!(
            !self.no_project,
            "graph() and no_project() are mutually exclusive",
        );
        self.graph_fn = Some(Box::new(f));
    }
}

// ---------------------------------------------------------------------------
// SeedContext
// ---------------------------------------------------------------------------

/// Provides database access and label storage during the seed phase.
pub struct SeedContext {
    conn: sqlx::PgConnection,
    project: Option<Project>,
    principal_id: PrincipalId,
    principal_key: String,
    labels: HashMap<String, String>,
}

impl SeedContext {
    /// Raw database connection for repository calls.
    pub fn conn(&mut self) -> &mut sqlx::PgConnection {
        &mut self.conn
    }

    /// ID of the auto-created project.
    ///
    /// # Panics
    ///
    /// Panics if `no_project()` was set on `HarnessSetup`.
    pub fn project_id(&self) -> tribal_domain::ProjectId {
        self.project
            .as_ref()
            .expect("project_id unavailable — no_project() was set")
            .id()
    }

    /// ID of the auto-created principal.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Git remote of the auto-created project.
    ///
    /// # Panics
    ///
    /// Panics if `no_project()` was set on `HarnessSetup`.
    pub fn project_git_remote(&self) -> &tribal_domain::GitRemote {
        self.project
            .as_ref()
            .expect("project_git_remote unavailable — no_project() was set")
            .git_remote()
    }

    /// Principal key string.
    pub fn principal_key(&self) -> &str {
        &self.principal_key
    }

    /// Stores a labelled ID for retrieval after init via `harness.label()`.
    pub fn label(&mut self, name: &str, id: impl ToString) {
        self.labels.insert(name.to_owned(), id.to_string());
    }
}

// ---------------------------------------------------------------------------
// seed! macro
// ---------------------------------------------------------------------------

/// Wraps an async seed closure, hiding the HRTB boxing ceremony.
///
/// The body can `.await` repository calls and use `seed.conn()`,
/// `seed.label()`, etc.
///
/// ```ignore
/// seed!(setup, |seed| {
///     let item = PgKnowledgeItemRepository
///         .insert(seed.conn(), &a_new_knowledge_item()...build())
///         .await
///         .expect("insert");
///     seed.label("item", item.id());
/// });
/// ```
///
/// Expands to `setup.seed(Box::new(|seed| Box::pin(async move { ... })))`.
macro_rules! seed {
    ($setup:expr, |$seed:ident| $body:block) => {
        $setup.seed(::std::boxed::Box::new(
            |$seed| ::std::boxed::Box::pin(async move $body),
        ))
    };
}

pub(crate) use seed;

// ---------------------------------------------------------------------------
// TestHarness
// ---------------------------------------------------------------------------

/// A fully bootstrapped E2E test environment with an MCP client.
pub struct TestHarness {
    server_handle: Option<ServerHandle>,
    cancellation_token: CancellationToken,
    config: TribalConfig,
    client: RunningService<RoleClient, ()>,
    state: Arc<AppState>,

    /// Connection pool for assertions and teardown (independent of server
    /// pools).
    pub pool: PgPool,

    embedding_server: MockServer,
    extraction_server: MockServer,
    triage_server: MockServer,
    relation_server: MockServer,

    labels: HashMap<String, String>,
    epoch: Arc<AtomicU64>,
    principal_key: String,
    cli_project: Option<String>,

    _prompts_dir: TempDir,

    /// The owned, isolated test database. Declared last so it drops AFTER
    /// `server_handle` and `pool` (Rust drops struct fields in declaration
    /// order), keeping the cloned database alive for the whole harness
    /// lifetime. Held for its lifetime, not read.
    _db: TestDb,
}

impl TestHarness {
    /// Constructs a fully bootstrapped E2E test environment.
    ///
    /// The `setup` closure receives a [`HarnessSetup`] with sensible
    /// defaults. Override only what the test cares about.
    ///
    /// # Panics
    ///
    /// Panics if any infrastructure step fails (database, wiremock, server
    /// startup, MCP handshake).
    pub async fn init(setup_fn: impl FnOnce(&mut HarnessSetup)) -> Self {
        // 1-2. Owned, isolated test database + assertion pool. The cloned
        // database is already empty, so no clean-slate truncation is needed.
        let db = TestDb::new().await;
        let pool = db.create_pool().await.expect("create assertion pool");

        // 3. Wiremock servers
        let embedding_server = MockServer::start().await;
        let extraction_server = MockServer::start().await;
        let triage_server = MockServer::start().await;
        let relation_server = MockServer::start().await;

        // 4. Apply setup
        let mut setup = HarnessSetup::new();
        setup_fn(&mut setup);

        // 5. Build config (before seeding so the genesis profile the seed
        // attaches embeddings to matches the live embedding identity the
        // server's provider builder constructs from).
        let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");
        let mut config = test_config(
            db.database_url(),
            &embedding_server.uri(),
            &extraction_server.uri(),
            &triage_server.uri(),
            &relation_server.uri(),
            prompts_dir.path().to_str().expect("prompts dir to str"),
        );

        if let Some(config_override) = setup.config_override.take() {
            config_override(&mut config);
        }

        validate(&config).expect("E2E test config must pass validation");

        // Seed the genesis profile from init.embedding so both the seed below
        // and the server's first-boot provisioning reuse it.
        seed_genesis_from_init(&pool, &config).await;

        // 6-7. Seed data (graph path or manual path)
        let mut raw_conn = db.raw_connection().await.expect("raw connection for seed");

        let (cli_project, labels) = if let Some(graph_fn) = setup.graph_fn {
            // Graph path: Seed manages project, principal, items, and
            // relations. The harness transfers item labels and uses the
            // Seed's project for server startup.
            let new_project = setup.project.unwrap_or_else(|| a_new_project().build());
            let pre_configured = Seed::new()
                .define_project(&new_project.name, new_project.git_remote.to_string())
                .define_principal("default", &setup.principal_key)
                .set_embedding_model(
                    DEFAULT_EMBEDDING_MODEL,
                    DEFAULT_EMBEDDING_DIMENSIONS as usize,
                );

            let configured = graph_fn(pre_configured);
            let result = configured.execute(&mut raw_conn).await;

            let cli_project = Some(result.project_id(&new_project.name).to_string());
            let mut labels = HashMap::new();
            for label in result.item_labels() {
                labels.insert(label.clone(), result.item_id(&label).to_string());
            }

            (cli_project, labels)
        } else {
            // Manual path: harness manages project and principal directly.
            let project = if setup.no_project {
                None
            } else {
                let new_project = setup.project.unwrap_or_else(|| a_new_project().build());
                let project = PgProjectRepository
                    .insert(&mut raw_conn, &new_project)
                    .await
                    .expect("insert project");
                Some(project)
            };

            let cli_project = project.as_ref().map(|p| p.id().to_string());

            let principal = PgPrincipalRepository
                .insert(
                    &mut raw_conn,
                    &a_new_principal()
                        .principal_key(setup.principal_key.clone())
                        .build(),
                )
                .await
                .expect("insert principal");

            let mut labels = HashMap::new();
            if let Some(seed_fn) = setup.seed {
                let mut seed_ctx = SeedContext {
                    conn: raw_conn,
                    project,
                    principal_id: principal.id(),
                    principal_key: setup.principal_key.clone(),
                    labels: HashMap::new(),
                };
                seed_fn(&mut seed_ctx).await;
                labels = seed_ctx.labels;
            }

            (cli_project, labels)
        };

        // 8. Mount infrastructure mocks
        mount_infrastructure_mocks(
            &config,
            &embedding_server,
            &extraction_server,
            &triage_server,
            &relation_server,
        )
        .await;

        // 10. Start server and connect client
        let token = CancellationToken::new();
        let (handle, state, client) = start_and_connect(
            &config,
            cli_project.clone(),
            &setup.principal_key,
            token.clone(),
        )
        .await;

        TestHarness {
            server_handle: Some(handle),
            cancellation_token: token,
            config,
            client,
            state,
            pool,
            embedding_server,
            extraction_server,
            triage_server,
            relation_server,
            labels,
            epoch: Arc::new(AtomicU64::new(0)),
            principal_key: setup.principal_key,
            cli_project,
            _prompts_dir: prompts_dir,
            _db: db,
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Initiates graceful shutdown of the server.
    ///
    /// # Panics
    ///
    /// Panics if shutdown has already been called.
    pub async fn shutdown(&mut self) {
        self.cancellation_token.cancel();
        let handle = self
            .server_handle
            .take()
            .expect("shutdown called more than once");
        tokio::task::spawn_blocking(move || {
            handle.shutdown().expect("server shutdown failed");
        })
        .await
        .expect("shutdown task panicked");
    }

    /// Restarts the server after a prior [`shutdown()`](Self::shutdown).
    ///
    /// Preserves the database pool, wiremock servers, config, and labels.
    /// Connects a fresh MCP client with a new session.
    ///
    /// # Panics
    ///
    /// Panics if the server has not been shut down first.
    pub async fn restart(&mut self) {
        assert!(
            self.server_handle.is_none(),
            "restart() requires a prior shutdown()",
        );

        self.invalidate_client_handles();

        let token = CancellationToken::new();
        let (handle, state, client) = start_and_connect(
            &self.config,
            self.cli_project.clone(),
            &self.principal_key,
            token.clone(),
        )
        .await;

        self.server_handle = Some(handle);
        self.cancellation_token = token;
        self.state = state;
        self.client = client;
    }

    /// Advances the epoch, invalidating all outstanding `ClientHandle`s.
    ///
    /// Any subsequent `call_tool` on a stale handle will panic rather than
    /// silently using a session that no longer exists.
    fn invalidate_client_handles(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    // -----------------------------------------------------------------------
    // Tool calls
    // -----------------------------------------------------------------------

    /// Calls a tool via JSON-RPC and returns the result.
    ///
    /// # Panics
    ///
    /// Panics on protocol-level transport errors or if `arguments` is not a
    /// JSON object or null.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> CallToolResult {
        tool_call::call_tool(&self.client, name, arguments).await
    }

    // -----------------------------------------------------------------------
    // Polling
    // -----------------------------------------------------------------------

    /// Polls `tribal_job_status` until the job reaches a terminal
    /// state, then asserts that the outcome matches `expected`.
    ///
    /// # Panics
    ///
    /// Panics with rich diagnostic context if the outcome does not
    /// match within the poll limit.
    pub async fn expect_outcome(&self, job_id: &str, expected: JobOutcome) {
        polling::expect_outcome(self, job_id, expected).await;
    }

    /// Polls `tribal_job_status` until the job completes with the
    /// `success` outcome. Shorthand for
    /// `expect_outcome(job_id, JobOutcome::Success)`.
    ///
    /// # Panics
    ///
    /// Panics with rich diagnostic context if the job does not
    /// complete successfully.
    pub async fn expect_completion(&self, job_id: &str) {
        self.expect_outcome(job_id, JobOutcome::Success).await;
    }

    // -----------------------------------------------------------------------
    // Labels
    // -----------------------------------------------------------------------

    /// Retrieves a labelled ID stored during the seed phase.
    ///
    /// # Panics
    ///
    /// Panics if no label with the given name exists.
    #[must_use]
    pub fn label(&self, name: &str) -> &str {
        self.labels
            .get(name)
            .unwrap_or_else(|| panic!("no label named '{name}'"))
    }

    // -----------------------------------------------------------------------
    // Mock mounting
    // -----------------------------------------------------------------------

    /// Mounts extraction stage mocks via a closure-based builder.
    ///
    /// # Panics
    ///
    /// Panics if the closure does not configure at least one mock response.
    pub async fn mount_extraction(&self, f: impl FnOnce(&mut StageMountBuilder<'_>)) {
        let provider = self.config.inference.extraction.provider;
        self.mount_stage(&self.extraction_server, "extraction", provider, f)
            .await;
    }

    /// Mounts triage stage mocks via a closure-based builder.
    ///
    /// # Panics
    ///
    /// Panics if the closure does not configure at least one mock response.
    pub async fn mount_triage(&self, f: impl FnOnce(&mut StageMountBuilder<'_>)) {
        let provider = self.config.inference.triage.provider;
        self.mount_stage(&self.triage_server, "triage", provider, f)
            .await;
    }

    /// Mounts relation stage mocks via a closure-based builder.
    ///
    /// # Panics
    ///
    /// Panics if the closure does not configure at least one mock response.
    pub async fn mount_relation(&self, f: impl FnOnce(&mut StageMountBuilder<'_>)) {
        let provider = self.config.inference.relation.provider;
        self.mount_stage(&self.relation_server, "relation", provider, f)
            .await;
    }

    async fn mount_stage(
        &self,
        server: &MockServer,
        stage: &'static str,
        provider: ProviderKind,
        f: impl FnOnce(&mut StageMountBuilder<'_>),
    ) {
        let mut builder = StageMountBuilder::new(server, stage, provider);
        f(&mut builder);
        builder.mount().await;
    }

    // -----------------------------------------------------------------------
    // Escape hatches
    //
    // Direct access to the underlying wiremock servers for mock patterns
    // that the `StageMountBuilder` API does not cover (e.g. custom body
    // matchers, partial JSON matching, or provider-specific assertions).
    // -----------------------------------------------------------------------

    #[must_use]
    pub fn embedding_server(&self) -> &MockServer {
        &self.embedding_server
    }

    #[must_use]
    pub fn extraction_server(&self) -> &MockServer {
        &self.extraction_server
    }

    #[must_use]
    pub fn triage_server(&self) -> &MockServer {
        &self.triage_server
    }

    #[must_use]
    pub fn relation_server(&self) -> &MockServer {
        &self.relation_server
    }

    // -----------------------------------------------------------------------
    // Diagnostics
    // -----------------------------------------------------------------------

    /// Builds a [`DiagnosticContext`] with provider metadata for each
    /// wiremock server, enabling provider-aware request formatting.
    pub fn diagnostic_context(&self) -> DiagnosticContext<'_> {
        DiagnosticContext {
            pool: &self.pool,
            servers: vec![
                ServerDiagnostic {
                    name: "embedding",
                    server: &self.embedding_server,
                    provider: self.config.init.embedding.provider,
                    request_class: RequestClass::Embedding,
                },
                ServerDiagnostic {
                    name: "extraction",
                    server: &self.extraction_server,
                    provider: self.config.inference.extraction.provider,
                    request_class: RequestClass::Inference,
                },
                ServerDiagnostic {
                    name: "triage",
                    server: &self.triage_server,
                    provider: self.config.inference.triage.provider,
                    request_class: RequestClass::Inference,
                },
                ServerDiagnostic {
                    name: "relation",
                    server: &self.relation_server,
                    provider: self.config.inference.relation.provider,
                    request_class: RequestClass::Inference,
                },
            ],
        }
    }

    // -----------------------------------------------------------------------
    // Multi-session support
    // -----------------------------------------------------------------------

    /// Connects a new MCP client with an independent session.
    ///
    /// The new client shares the same server and wiremock infrastructure
    /// but has its own `SessionContext` and `AuthContext` for the given
    /// principal.
    ///
    /// # Panics
    ///
    /// Panics if the MCP handshake fails on either side of the duplex
    /// transport.
    pub async fn connect_client(&self, principal_key: &str) -> ClientHandle {
        let auth = resolve_e2e_auth(&self.state, principal_key).await;
        let session = SessionContext::new(None);
        let repositories = ConnectionRepositories::new();
        let handler_config = HandlerConfig::from(&self.config);
        let handler = TribalServerHandler::new(
            Arc::clone(&self.state),
            TransportAuthStrategy::AtCreation(auth),
            repositories,
            session,
            handler_config,
            "stdio",
        );

        let (server_transport, client_transport) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

        tokio::spawn(async move {
            let server = handler
                .serve(server_transport)
                .await
                .expect("server MCP handshake failed");
            server.waiting().await.expect("server MCP session error");
        });

        let client = ().serve(client_transport).await.expect("client MCP handshake failed");

        ClientHandle {
            client,
            birth_epoch: self.epoch.load(Ordering::SeqCst),
            current_epoch: Arc::clone(&self.epoch),
        }
    }
}

// ---------------------------------------------------------------------------
// ClientHandle
// ---------------------------------------------------------------------------

/// An additional MCP client connected to the same server.
///
/// Tracks the harness epoch at creation time. Calling `call_tool` after a
/// `restart()` panics with a clear message — the handle is stale because
/// sessions are per-connection and do not survive restarts.
pub struct ClientHandle {
    client: RunningService<RoleClient, ()>,
    birth_epoch: u64,
    current_epoch: Arc<AtomicU64>,
}

impl ClientHandle {
    /// Calls a tool via JSON-RPC and returns the result.
    ///
    /// # Panics
    ///
    /// Panics if the harness has been restarted since this handle was
    /// created — the underlying session is no longer valid.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> CallToolResult {
        self.assert_valid();
        tool_call::call_tool(&self.client, name, arguments).await
    }

    /// Panics if the harness epoch has advanced past this handle's birth.
    fn assert_valid(&self) {
        let current = self.current_epoch.load(Ordering::SeqCst);
        assert_eq!(
            self.birth_epoch, current,
            "stale ClientHandle from epoch {}, current epoch is {current} — \
             sessions do not survive restart()",
            self.birth_epoch,
        );
    }
}

// ---------------------------------------------------------------------------
// Server startup
// ---------------------------------------------------------------------------

/// Starts the server and connects an MCP client over a duplex transport.
async fn start_and_connect(
    config: &TribalConfig,
    cli_project: Option<String>,
    principal_key: &str,
    token: CancellationToken,
) -> (ServerHandle, Arc<AppState>, RunningService<RoleClient, ()>) {
    let spawn_config = config.clone();
    let spawn_token = token.clone();

    let handle = tokio::task::spawn_blocking(move || {
        start_server(
            &spawn_config,
            cli_project,
            spawn_token,
            None,
            noop_recorder(),
        )
        .expect("server startup failed")
    })
    .await
    .expect("start_server task panicked");

    let state = Arc::clone(handle.state());

    let auth = resolve_e2e_auth(&state, principal_key).await;
    let session_project = state.resolved_project().map(SessionProject::from);
    let session = SessionContext::new(session_project);
    let repositories = ConnectionRepositories::new();
    let handler_config = HandlerConfig::from(config);
    let handler = TribalServerHandler::new(
        Arc::clone(&state),
        TransportAuthStrategy::AtCreation(auth),
        repositories,
        session,
        handler_config,
        "stdio",
    );

    let (server_transport, client_transport) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

    tokio::spawn(async move {
        let server = handler
            .serve(server_transport)
            .await
            .expect("server MCP handshake failed");
        server.waiting().await.expect("server MCP session error");
    });

    let client = ().serve(client_transport).await.expect("client MCP handshake failed");

    (handle, state, client)
}

// ---------------------------------------------------------------------------
// Auth helper
// ---------------------------------------------------------------------------

/// Resolves the E2E principal from the database and wraps it in an
/// [`AuthContext`] for handler construction.
///
/// # Panics
///
/// Panics if the principal cannot be found — the harness setup must
/// have seeded it beforehand.
async fn resolve_e2e_auth(state: &AppState, principal_key: &str) -> AuthContext {
    let mut conn = state
        .mcp_pool()
        .acquire()
        .await
        .expect("acquire connection for auth");
    let principal = PgPrincipalRepository
        .find_by_key(&mut conn, principal_key)
        .await
        .expect("find principal by key")
        .unwrap_or_else(|| {
            panic!("principal '{principal_key}' not found — run harness setup first")
        });
    AuthContext::new(AuthenticatedPrincipal::for_test(
        principal.id(),
        principal_key,
        tribal_domain::full_access_scopes(),
    ))
}

// ---------------------------------------------------------------------------
// Infrastructure mock mounting
// ---------------------------------------------------------------------------

/// Mounts provider probes and embedding mocks based on the finalised config.
///
/// Two categories:
/// - **Embedding:** tags (if `Ollama`) + embed endpoint with fixed vector
/// - **Inference probes:** tags (if `Ollama`) + probe absorber on each
///   inference server using `body_string_contains(INFERENCE_PROBE_INPUT)`
async fn mount_infrastructure_mocks(
    config: &TribalConfig,
    embedding_server: &MockServer,
    extraction_server: &MockServer,
    triage_server: &MockServer,
    relation_server: &MockServer,
) {
    let embedding_provider = config.init.embedding.provider;

    // -- Embedding server ----------------------------------------------------
    if let Some(tags_endpoint) = tags_path(embedding_provider) {
        mount_tags(
            embedding_server,
            tags_endpoint,
            &config.init.embedding.model,
        )
        .await;
    }

    let dimensions = config
        .init
        .embedding
        .dimensions
        .expect("E2E test config sets a concrete embedding dimension");
    let vector = fixed_embedding_vector(dimensions);
    let embed_response = wrap_embedding(&vector, embedding_provider);
    let embedding_endpoint = embed_path(embedding_provider);

    Mock::given(method("POST"))
        .and(path(embedding_endpoint))
        .respond_with(ResponseTemplate::new(200).set_body_json(embed_response))
        .mount(embedding_server)
        .await;

    // -- Inference servers (extraction, triage, relation) ---------------------
    let stages: &[(&MockServer, ProviderKind, &str)] = &[
        (
            extraction_server,
            config.inference.extraction.provider,
            &config.inference.extraction.model,
        ),
        (
            triage_server,
            config.inference.triage.provider,
            &config.inference.triage.model,
        ),
        (
            relation_server,
            config.inference.relation.provider,
            &config.inference.relation.model,
        ),
    ];

    for &(server, provider, model) in stages {
        if let Some(tags_endpoint) = tags_path(provider) {
            mount_tags(server, tags_endpoint, model).await;
        }
        mount_probe_absorber(server, provider).await;
    }
}

async fn mount_tags(server: &MockServer, endpoint: &str, model_name: &str) {
    let body = json!({ "models": [{ "name": model_name }] });
    Mock::given(method("GET"))
        .and(path(endpoint))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn mount_probe_absorber(server: &MockServer, provider: ProviderKind) {
    let endpoint = chat_path(provider);
    let response = wrap_completion(&json!("OK"), provider);
    Mock::given(method("POST"))
        .and(path(endpoint))
        .and(body_string_contains(
            tribal_inference::INFERENCE_PROBE_INPUT,
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .mount(server)
        .await;
}
