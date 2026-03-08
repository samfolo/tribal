use std::sync::Arc;

use tribal_test_utils::{
    MockJobRepository, MockKnowledgeItemRepository, MockProjectRepository,
    MockRetrievalFeedbackRepository,
};

use crate::server_handler::ConnectionRepositories;

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
        retrieval_feedback: Arc::new(MockRetrievalFeedbackRepository::builder().build()),
    }
}
