use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rmcp::{
    ServiceExt,
    model::CallToolResult,
    service::{RoleClient, RunningService},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tribal_config::{ProviderKind, TribalConfig, validate};
use tribal_db::{PgPrincipalRepository, PgProjectRepository, PrincipalRepository, ProjectRepository};
use tribal_domain::{GitRemote, PrincipalId, ProjectId};
use tribal_mcp::{
    AppState, ConnectionRepositories, HandlerConfig, SessionContext, SessionProject,
    TribalServerHandler,
};
use tribal_server::{ServerHandle, start_server};
use tribal_test_utils::{
    a_new_principal, a_new_project, serial_lock, test_context, truncate_all_tables,
};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{body_string_contains, method, path}};

use super::config::test_config;
use super::mocks::envelope::{
    chat_path, embed_path, fixed_embedding_vector, tags_path, wrap_completion, wrap_embedding,
};
use super::mocks::mounting::StageMountBuilder;
use super::tool_call;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Buffer size for the in-process duplex transport between server and client.
const DUPLEX_BUFFER_SIZE: usize = 65_536;

// ---------------------------------------------------------------------------
// Seed closure type
// ---------------------------------------------------------------------------

type AsyncSeedFn = Box<
    dyn for<'a> FnOnce(
            &'a mut SeedContext,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
        + Send,
>;

// ---------------------------------------------------------------------------
// seed! macro
// ---------------------------------------------------------------------------

/// Wraps an async seed closure, hiding the HRTB boxing ceremony.
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
macro_rules! seed {
    ($setup:expr, |$seed:ident| $body:block) => {
        $setup.seed(|$seed| ::std::boxed::Box::pin(async move $body))
    };
}

pub(crate) use seed;

// ---------------------------------------------------------------------------
// HarnessSetup
// ---------------------------------------------------------------------------

/// Configuration for [`TestHarness::init`]. Tests override only what they
/// care about — everything else uses sensible defaults.
pub struct HarnessSetup {
    principal_key: String,
    project: Option<tribal_domain::NewProject>,
    no_project: bool,
    config_override: Option<Box<dyn FnOnce(&mut TribalConfig)>>,
    seed: Option<AsyncSeedFn>,
}

impl HarnessSetup {
    fn new() -> Self {
        Self {
            principal_key: "e2e-principal".to_owned(),
            project: None,
            no_project: false,
            config_override: None,
            seed: None,
        }
    }

    /// Sets the principal key for this test.
    pub fn principal_key(&mut self, key: &str) {
        self.principal_key = key.to_owned();
    }

    /// Overrides the default project.
    pub fn project(&mut self, project: tribal_domain::NewProject) {
        self.project = Some(project);
    }

    /// Suppresses default project creation and startup resolution.
    ///
    /// When set, `cli_project` is `None` and the server starts without a
    /// resolved project. `SeedContext::project_id()` will panic — manage
    /// projects manually via the seed closure.
    pub fn no_project(&mut self) {
        self.no_project = true;
    }

    /// Applies a per-test config override after defaults are set.
    pub fn config(&mut self, f: impl FnOnce(&mut TribalConfig) + 'static) {
        self.config_override = Some(Box::new(f));
    }

    /// Registers an async seed closure that runs after project and
    /// principal insertion.
    pub fn seed(
        &mut self,
        f: impl for<'a> FnOnce(
                &'a mut SeedContext,
            ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>
            + Send
            + 'static,
    ) {
        self.seed = Some(Box::new(f));
    }
}

// ---------------------------------------------------------------------------
// SeedContext
// ---------------------------------------------------------------------------

/// Provides database access and label storage during the seed phase.
pub struct SeedContext {
    conn: sqlx::PgConnection,
    project_id: Option<ProjectId>,
    principal_id: PrincipalId,
    project_git_remote: Option<GitRemote>,
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
    pub fn project_id(&self) -> ProjectId {
        self.project_id
            .expect("project_id unavailable — no_project() was set")
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
    pub fn project_git_remote(&self) -> &GitRemote {
        self.project_git_remote
            .as_ref()
            .expect("project_git_remote unavailable — no_project() was set")
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
// TestHarness
// ---------------------------------------------------------------------------

/// A fully bootstrapped E2E test environment with an MCP client.
pub struct TestHarness {
    server_handle: Option<ServerHandle>,
    cancellation_token: CancellationToken,
    config: TribalConfig,
    client: RunningService<RoleClient, ()>,
    state: Arc<AppState>,

    /// Connection pool for assertions and teardown (independent of server pools).
    pub pool: PgPool,

    embedding_server: MockServer,
    extraction_server: MockServer,
    triage_server: MockServer,
    relation_server: MockServer,

    labels: HashMap<String, String>,
    principal_key: String,
    cli_project: Option<String>,

    _prompts_dir: TempDir,
    _serial_guard: tokio::sync::MutexGuard<'static, ()>,
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
        // 1. Serial lock + test context + per-test pool
        let guard = serial_lock().await;
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("create per-test pool");

        // 2. Clean slate
        let mut conn = pool.acquire().await.expect("acquire connection");
        truncate_all_tables(&mut conn).await;
        drop(conn);

        // 3. Wiremock servers
        let embedding_server = MockServer::start().await;
        let extraction_server = MockServer::start().await;
        let triage_server = MockServer::start().await;
        let relation_server = MockServer::start().await;

        // 4. Apply setup
        let mut setup = HarnessSetup::new();
        setup_fn(&mut setup);

        // 5. Insert project + principal
        let mut raw_conn = ctx
            .raw_connection()
            .await
            .expect("raw connection for seed");

        let mut project_id = None;
        let mut project_git_remote = None;
        let mut cli_project = None;

        if !setup.no_project {
            let new_project = setup.project.unwrap_or_else(|| a_new_project().build());
            let project = PgProjectRepository
                .insert(&mut raw_conn, &new_project)
                .await
                .expect("insert project");
            project_id = Some(project.id());
            project_git_remote = Some(project.git_remote().clone());
            cli_project = Some(project.id().to_string());
        }

        let principal = PgPrincipalRepository
            .insert(
                &mut raw_conn,
                &a_new_principal()
                    .principal_key(setup.principal_key.clone())
                    .build(),
            )
            .await
            .expect("insert principal");

        // 6. Execute seed closure
        let mut labels = HashMap::new();
        if let Some(seed_fn) = setup.seed {
            let mut seed_ctx = SeedContext {
                conn: raw_conn,
                project_id,
                principal_id: principal.id(),
                project_git_remote: project_git_remote.clone(),
                principal_key: setup.principal_key.clone(),
                labels: HashMap::new(),
            };
            seed_fn(&mut seed_ctx).await;
            labels = seed_ctx.labels;
            // Connection is consumed by SeedContext and dropped here.
        }

        // 7. Build config
        let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");
        let mut config = test_config(
            ctx.database_url(),
            &embedding_server.uri(),
            &extraction_server.uri(),
            &triage_server.uri(),
            &relation_server.uri(),
            prompts_dir.path().to_str().expect("prompts dir to str"),
        );

        if let Some(config_override) = setup.config_override {
            config_override(&mut config);
        }

        // 8. Validate
        validate(&config).expect("E2E test config must pass validation");

        // 9. Mount infrastructure mocks
        mount_infrastructure_mocks(
            &config,
            &embedding_server,
            &extraction_server,
            &triage_server,
            &relation_server,
        )
        .await;

        // 10. Start server
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
            principal_key: setup.principal_key,
            cli_project,
            _prompts_dir: prompts_dir,
            _serial_guard: guard,
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
            let _ = handle.shutdown();
        })
        .await
        .expect("shutdown task panicked");
    }

    /// Truncates all application tables for test isolation.
    pub async fn teardown(&self) {
        let mut conn = self
            .pool
            .acquire()
            .await
            .expect("acquire connection for teardown");
        truncate_all_tables(&mut conn).await;
    }

    /// Restarts the server after a prior `shutdown()`.
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

    // -----------------------------------------------------------------------
    // Tool calls
    // -----------------------------------------------------------------------

    /// Calls a tool via JSON-RPC and returns the result.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> CallToolResult {
        tool_call::call_tool(&self.client, name, arguments).await
    }

    // -----------------------------------------------------------------------
    // Polling
    // -----------------------------------------------------------------------

    /// Polls `tribal_job_status` until the job completes or fails.
    ///
    /// # Panics
    ///
    /// Panics with rich diagnostic context if the job does not complete.
    pub async fn expect_completion(&self, job_id: &str) {
        super::polling::expect_completion(self, job_id).await;
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
    pub async fn mount_extraction(&self, f: impl FnOnce(&mut StageMountBuilder<'_>)) {
        let provider = self.config.inference.extraction.provider;
        let mut builder =
            StageMountBuilder::new(&self.extraction_server, "extraction", provider);
        f(&mut builder);
        builder.mount().await;
    }

    /// Mounts triage stage mocks via a closure-based builder.
    pub async fn mount_triage(&self, f: impl FnOnce(&mut StageMountBuilder<'_>)) {
        let provider = self.config.inference.triage.provider;
        let mut builder = StageMountBuilder::new(&self.triage_server, "triage", provider);
        f(&mut builder);
        builder.mount().await;
    }

    /// Mounts relation stage mocks via a closure-based builder.
    pub async fn mount_relation(&self, f: impl FnOnce(&mut StageMountBuilder<'_>)) {
        let provider = self.config.inference.relation.provider;
        let mut builder =
            StageMountBuilder::new(&self.relation_server, "relation", provider);
        f(&mut builder);
        builder.mount().await;
    }

    // -----------------------------------------------------------------------
    // Escape hatches
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
    // Multi-session support
    // -----------------------------------------------------------------------

    /// Connects a new MCP client with an independent session.
    ///
    /// The new client shares the same server and wiremock infrastructure
    /// but has its own `SessionContext` bound to the given principal key.
    pub async fn connect_client(&self, principal_key: &str) -> ClientHandle {
        let session = SessionContext::new(None, principal_key.to_owned());
        let repositories = ConnectionRepositories::new();
        let handler_config = HandlerConfig::default();
        let handler = TribalServerHandler::new(
            Arc::clone(&self.state),
            repositories,
            session,
            handler_config,
        );

        let (server_transport, client_transport) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

        tokio::spawn(async move {
            let server = handler
                .serve(server_transport)
                .await
                .expect("server MCP handshake failed");
            server.waiting().await.expect("server MCP session error");
        });

        let client = ()
            .serve(client_transport)
            .await
            .expect("client MCP handshake failed");

        ClientHandle { client }
    }
}

// ---------------------------------------------------------------------------
// ClientHandle
// ---------------------------------------------------------------------------

/// An additional MCP client connected to the same server.
pub struct ClientHandle {
    client: RunningService<RoleClient, ()>,
}

impl ClientHandle {
    /// Calls a tool via JSON-RPC and returns the result.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> CallToolResult {
        tool_call::call_tool(&self.client, name, arguments).await
    }
}

// ---------------------------------------------------------------------------
// Server startup helper
// ---------------------------------------------------------------------------

async fn start_and_connect(
    config: &TribalConfig,
    cli_project: Option<String>,
    principal_key: &str,
    token: CancellationToken,
) -> (ServerHandle, Arc<AppState>, RunningService<RoleClient, ()>) {
    let spawn_config = config.clone();
    let spawn_token = token.clone();

    let handle = tokio::task::spawn_blocking(move || {
        start_server(&spawn_config, cli_project, spawn_token).expect("server startup failed")
    })
    .await
    .expect("start_server task panicked");

    let state = Arc::clone(handle.state());

    let session_project = state.resolved_project().map(SessionProject::from);
    let session = SessionContext::new(session_project, principal_key.to_owned());
    let repositories = ConnectionRepositories::new();
    let handler_config = HandlerConfig::default();
    let handler =
        TribalServerHandler::new(Arc::clone(&state), repositories, session, handler_config);

    let (server_transport, client_transport) = tokio::io::duplex(DUPLEX_BUFFER_SIZE);

    tokio::spawn(async move {
        let server = handler
            .serve(server_transport)
            .await
            .expect("server MCP handshake failed");
        server.waiting().await.expect("server MCP session error");
    });

    let client = ()
        .serve(client_transport)
        .await
        .expect("client MCP handshake failed");

    (handle, state, client)
}

// ---------------------------------------------------------------------------
// Infrastructure mock mounting
// ---------------------------------------------------------------------------

/// Mounts provider probes and embedding mocks based on the finalised config.
///
/// Two categories of mocks are mounted:
/// - **Embedding:** tags (if Ollama) + embed endpoint with fixed vector
/// - **Inference probes:** tags (if Ollama) + probe absorber on each
///   inference server using `body_string_contains(INFERENCE_PROBE_INPUT)`
async fn mount_infrastructure_mocks(
    config: &TribalConfig,
    embedding_server: &MockServer,
    extraction_server: &MockServer,
    triage_server: &MockServer,
    relation_server: &MockServer,
) {
    let embed_provider = config.embedding.provider;

    // -- Embedding server ----------------------------------------------------
    if let Some(tp) = tags_path(embed_provider) {
        mount_tags(embedding_server, tp, &config.embedding.model).await;
    }

    let vector = fixed_embedding_vector(config.embedding.dimensions);
    let embed_body = wrap_embedding(&vector, embed_provider);
    let ep = embed_path(embed_provider);
    Mock::given(method("POST"))
        .and(path(ep))
        .respond_with(ResponseTemplate::new(200).set_body_json(embed_body))
        .mount(embedding_server)
        .await;

    // -- Inference servers (extraction, triage, relation) ---------------------
    let stages: &[(&MockServer, ProviderKind, &str)] = &[
        (extraction_server, config.inference.extraction.provider, &config.inference.extraction.model),
        (triage_server, config.inference.triage.provider, &config.inference.triage.model),
        (relation_server, config.inference.relation.provider, &config.inference.relation.model),
    ];

    for &(server, provider, model) in stages {
        if let Some(tp) = tags_path(provider) {
            mount_tags(server, tp, model).await;
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
