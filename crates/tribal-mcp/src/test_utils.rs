use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::RwLock;
use tribal_domain::PromptVersionId;
use tribal_inference::EmbeddingProvider;
use tribal_test_utils::{
    MockEmbeddingProvider, MockJobRepository, MockKnowledgeItemRepository, MockPrincipalRepository,
    MockProjectRepository, MockReferenceRepository, MockRelationRepository,
    MockRetrievalFeedbackRepository, MockStandingRepository, MockTaskRepository,
    MockTriageResultRepository, lazy_pool,
};
use typed_builder::TypedBuilder;

use crate::{
    config::HandlerConfig,
    server_handler::{ActivePromptVersions, ConnectionRepositories, TribalServerHandler},
    session::SessionContext,
};

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
        Self::new(
            th.pool,
            th.repositories,
            th.embedding_provider,
            th.active_prompt_versions,
            th.session,
            th.config,
        )
    }
}

fn default_embedding_provider() -> Arc<dyn EmbeddingProvider> {
    Arc::new(MockEmbeddingProvider::builder().build())
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
