use std::sync::Arc;

use dashmap::DashMap;
use rmcp::{
    model::{Extensions as RmcpExtensions, Meta, RequestId},
    service::{RequestContext, RoleServer, serve_directly_with_ct},
};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tribal_common::JobStateTxs;
use tribal_config::{DEFAULT_OLLAMA_BASE_URL, ServerConfig, WorkerConfig};
use tribal_domain::{PrincipalId, ProjectId, PromptVersionId};
use tribal_inference::{EmbeddingProvider, InferenceProvider, ProviderRegistry};
use tribal_test_utils::{
    MockEmbeddingProvider, MockInferenceProvider, MockJobRepository, MockKnowledgeItemRepository,
    MockPrincipalRepository, MockProjectRepository, MockReferenceRepository,
    MockRelationRepository, MockRetrievalFeedbackRepository, MockStandingRepository,
    MockTaskRepository, MockTriageResultRepository, TEST_PRINCIPAL_KEY, lazy_pool,
};
use typed_builder::TypedBuilder;

use crate::{
    app_state::AppState,
    auth::{AuthContext, AuthenticatedPrincipal, TransportAuthStrategy},
    config::HandlerConfig,
    server_handler::{ActivePromptVersions, ConnectionRepositories, TribalServerHandler},
    session::{SessionContext, SessionProject},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TEST_PROVIDER_KIND: &str = "ollama";
const TEST_INSTANCE_ID: &str = "test-host-1-00000000-0000-0000-0000-000000000000";

// ---------------------------------------------------------------------------
// test_repositories
// ---------------------------------------------------------------------------

/// Default set of mock repositories for handler tests.
///
/// All mocks are unconfigured — calls will panic unless responses are
/// enqueued. Replace individual fields when a test needs a specific mock
/// configuration.
pub(crate) fn test_repositories() -> ConnectionRepositories {
    ConnectionRepositories {
        knowledge_item: Arc::new(MockKnowledgeItemRepository::builder().build()),
        project: Arc::new(MockProjectRepository::builder().build()),
        job: Arc::new(MockJobRepository::builder().build()),
        task: Arc::new(MockTaskRepository::builder().build()),
        retrieval_feedback: Arc::new(MockRetrievalFeedbackRepository::builder().build()),
        standing: Arc::new(MockStandingRepository::builder().build()),
        reference: Arc::new(MockReferenceRepository::builder().build()),
        relation: Arc::new(MockRelationRepository::builder().build()),
        principal: Arc::new(MockPrincipalRepository::builder().build()),
        triage_result: Arc::new(MockTriageResultRepository::builder().build()),
    }
}

// ---------------------------------------------------------------------------
// TestHandler
// ---------------------------------------------------------------------------

/// Builder for constructing a [`TribalServerHandler`] in tests.
///
/// Every field defaults to a sensible test value. Override only the fields
/// relevant to the test under consideration.
#[derive(TypedBuilder)]
#[builder(build_method(into = TribalServerHandler))]
pub(crate) struct TestHandler {
    #[builder(default = lazy_pool())]
    pool: PgPool,

    #[builder(default = default_auth_context())]
    auth: AuthContext,

    #[builder(default = test_repositories())]
    repositories: ConnectionRepositories,

    #[builder(default = default_embedding_provider())]
    embedding_provider: Arc<dyn EmbeddingProvider>,

    #[builder(default = default_prompt_versions())]
    active_prompt_versions: Arc<RwLock<ActivePromptVersions>>,

    #[builder(default = SessionContext::new(None))]
    session: SessionContext,

    #[builder(default)]
    config: HandlerConfig,

    #[builder(default = Arc::new(DashMap::new()))]
    job_state_txs: JobStateTxs,

    #[builder(default = CancellationToken::new())]
    cancellation_token: CancellationToken,
}

impl From<TestHandler> for TribalServerHandler {
    fn from(th: TestHandler) -> Self {
        let state = Arc::new(
            AppState::builder()
                .pool_mcp(th.pool.clone())
                .pool_worker(th.pool)
                .instance_id(Arc::from(TEST_INSTANCE_ID))
                .active_prompt_versions(th.active_prompt_versions)
                .provider_registry(Arc::new(
                    ProviderRegistry::new(Vec::new())
                        .expect("empty registry construction must not fail"),
                ))
                .embedding_provider(th.embedding_provider)
                .extraction_provider(default_inference_provider())
                .triage_provider(default_inference_provider())
                .relation_provider(default_inference_provider())
                .embedding_key(test_embedding_key())
                .extraction_key(test_inference_key())
                .triage_key(test_inference_key())
                .relation_key(test_inference_key())
                .worker_config(WorkerConfig::default())
                .server_config(Arc::new(ServerConfig::default()))
                .cancellation_token(th.cancellation_token)
                .job_state_txs(th.job_state_txs)
                .build(),
        );
        Self::new(
            state,
            TransportAuthStrategy::AtCreation(th.auth),
            th.repositories,
            th.session,
            th.config,
        )
    }
}

/// Returns a [`SessionContext`] with a default project attached.
///
/// Use with `TestHandler::builder().session(session_with_project()).build()`
/// when a test needs a handler whose session already has a project set.
pub(crate) fn session_with_project() -> SessionContext {
    let project = SessionProject {
        id: ProjectId::new(),
        name: "tribal".into(),
        git_remote: "git@github.com:user/tribal.git"
            .parse()
            .expect("valid test git remote"),
    };
    SessionContext::new(Some(project))
}

// ---------------------------------------------------------------------------
// test_request_context
// ---------------------------------------------------------------------------

/// Builds a [`RequestContext<RoleServer>`] with the given extensions.
///
/// `Peer::new` is `pub(crate)` in rmcp, so external crates cannot
/// construct a `RequestContext` directly.  This helper works around that
/// by spinning up a disposable handler over an in-memory duplex transport
/// via [`serve_directly_with_ct`], cloning the `Peer` it produces, and
/// immediately cancelling the service.
///
/// # Panics
///
/// Panics if the in-memory transport setup fails (should not happen in
/// practice).
pub(crate) fn test_request_context(extensions: RmcpExtensions) -> RequestContext<RoleServer> {
    let dummy = TestHandler::builder().build();
    let (_, server) = tokio::io::duplex(1);
    let (read, write) = tokio::io::split(server);
    let ct = CancellationToken::new();
    let running = serve_directly_with_ct(dummy, (read, write), None, ct.clone());
    let peer = running.peer().clone();
    ct.cancel();

    RequestContext {
        ct: CancellationToken::new(),
        id: RequestId::Number(1),
        meta: Meta::default(),
        extensions,
        peer,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn default_auth_context() -> AuthContext {
    AuthContext::new(AuthenticatedPrincipal::for_test(
        PrincipalId::new(),
        TEST_PRINCIPAL_KEY,
        tribal_domain::full_access_scopes(),
    ))
}

fn default_embedding_provider() -> Arc<dyn EmbeddingProvider> {
    Arc::new(MockEmbeddingProvider::builder().build())
}

fn default_inference_provider() -> Arc<dyn InferenceProvider> {
    Arc::new(MockInferenceProvider::builder().build())
}

fn default_prompt_versions() -> Arc<RwLock<ActivePromptVersions>> {
    Arc::new(RwLock::new(ActivePromptVersions {
        extraction_system_prompt_version_id: PromptVersionId::new(),
        extraction_user_prompt_version_id: PromptVersionId::new(),
        triage_system_prompt_version_id: PromptVersionId::new(),
        triage_user_prompt_version_id: PromptVersionId::new(),
        relation_system_prompt_version_id: PromptVersionId::new(),
        relation_user_prompt_version_id: PromptVersionId::new(),
    }))
}

fn test_embedding_key() -> tribal_inference::ProviderKey {
    tribal_inference::ProviderKey::new(
        TEST_PROVIDER_KIND,
        DEFAULT_OLLAMA_BASE_URL,
        tribal_inference::RequestClass::Embedding,
    )
    .expect("test embedding key construction must not fail")
}

fn test_inference_key() -> tribal_inference::ProviderKey {
    tribal_inference::ProviderKey::new(
        TEST_PROVIDER_KIND,
        DEFAULT_OLLAMA_BASE_URL,
        tribal_inference::RequestClass::Inference,
    )
    .expect("test inference key construction must not fail")
}
