use chrono::Utc;
use tribal_db::NewRetrievalFeedback;
use tribal_domain::{
    FeedbackRating, KnowledgeItemId, PrincipalId, RetrievalFeedback, RetrievalFeedbackId,
};

define_factory! {
    /// Factory for [`RetrievalFeedback`] instances.
    pub struct RetrievalFeedbackFactory for RetrievalFeedback {
        id: RetrievalFeedbackId = RetrievalFeedbackId::new(),
        trace_id: String = "00000000000000000000000000000001".to_owned(),
        query_text: String = "test query".to_owned(),
        embedding_model: String = "nomic-embed-text:v1.5".to_owned(),
        returned_item_ids: Vec<KnowledgeItemId> = Vec::new(),
        explored_anchor_ids: Vec<KnowledgeItemId> = Vec::new(),
        policy_version: Option<String> = None,
        principal_id: PrincipalId = PrincipalId::new(),
        rating: FeedbackRating = FeedbackRating::Positive,
        notes: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`RetrievalFeedbackFactory`] with sensible defaults.
pub fn a_retrieval_feedback() -> RetrievalFeedbackFactory {
    RetrievalFeedbackFactory::new()
}

define_factory! {
    /// Factory for [`NewRetrievalFeedback`] instances used in repository
    /// insert operations.
    pub struct NewRetrievalFeedbackFactory for NewRetrievalFeedback {
        trace_id: String = "00000000000000000000000000000001".to_owned(),
        query_text: String = "test query".to_owned(),
        embedding_model: String = "nomic-embed-text:v1.5".to_owned(),
        returned_item_ids: Vec<KnowledgeItemId> = Vec::new(),
        explored_anchor_ids: Vec<KnowledgeItemId> = Vec::new(),
        policy_version: Option<String> = None,
        principal_id: PrincipalId = PrincipalId::new(),
        rating: FeedbackRating = FeedbackRating::Positive,
        notes: Option<String> = None,
    }
}

/// Returns a [`NewRetrievalFeedbackFactory`] with sensible defaults.
pub fn a_new_retrieval_feedback() -> NewRetrievalFeedbackFactory {
    NewRetrievalFeedbackFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let f = a_retrieval_feedback().build();
        assert_eq!(f.rating(), FeedbackRating::Positive);
        assert_eq!(f.query_text(), "test query");
    }

    #[test]
    fn test_new_builds_with_defaults() {
        let new = a_new_retrieval_feedback().build();
        assert_eq!(new.trace_id, "00000000000000000000000000000001");
        assert_eq!(new.query_text, "test query");
        assert_eq!(new.embedding_model, "nomic-embed-text:v1.5");
        assert!(new.returned_item_ids.is_empty());
        assert!(new.explored_anchor_ids.is_empty());
        assert!(new.policy_version.is_none());
        assert_eq!(new.rating, FeedbackRating::Positive);
        assert!(new.notes.is_none());
    }
}
