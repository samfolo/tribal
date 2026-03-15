use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::RwLock;
use tribal_config::{DEFAULT_OLLAMA_BASE_URL, ServerConfig, WorkerConfig};
use tribal_domain::{ProjectId, PromptVersionId};
use tribal_inference::{EmbeddingProvider, InferenceProvider, ProviderRegistry};
use tribal_test_utils::{
    MockEmbeddingProvider, MockInferenceProvider, MockJobRepository, MockKnowledgeItemRepository,
    MockPrincipalRepository, MockProjectRepository, MockReferenceRepository,
    MockRelationRepository, MockRetrievalFeedbackRepository, MockStandingRepository,
    MockTaskRepository, MockTriageResultRepository, lazy_pool,
};
use typed_builder::TypedBuilder;

use crate::{
    app_state::AppState,
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

    #[builder(default = test_repositories())]
    repositories: ConnectionRepositories,

    #[builder(default = default_embedding_provider())]
    embedding_provider: Arc<dyn EmbeddingProvider>,

    #[builder(default = default_prompt_versions())]
    active_prompt_versions: Arc<RwLock<ActivePromptVersions>>,

    #[builder(default = SessionContext::new(None, "user:test".into()))]
    session: SessionContext,

    #[builder(default)]
    config: HandlerConfig,
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
                .build(),
        );
        Self::new(state, th.repositories, th.session, th.config)
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
    SessionContext::new(Some(project), "user:test".into())
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
