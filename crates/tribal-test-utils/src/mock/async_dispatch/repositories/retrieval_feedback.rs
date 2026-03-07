//! Mock implementation of [`RetrievalFeedbackRepository`].

use tribal_db::{NewRetrievalFeedback, RetrievalFeedbackRepository};
use tribal_domain::{RetrievalFeedback, RetrievalFeedbackId};

use super::mock_repository;

mock_repository! {
    MockRetrievalFeedbackRepository for RetrievalFeedbackRepository, tribal_db::DbError {
        insert(NewRetrievalFeedback => RetrievalFeedback)
            (new: &NewRetrievalFeedback) { new.clone() };
        find_by_id(RetrievalFeedbackId => RetrievalFeedback)
            (id: RetrievalFeedbackId) { id }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::RetrievalFeedbackRepository;

    use super::*;

    // -- Tests --------------------------------------------------------------

    #[test]
    fn test_send_sync_behind_arc() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockRetrievalFeedbackRepository>();

        let mock = MockRetrievalFeedbackRepository::builder().build();
        let _arc: Arc<dyn RetrievalFeedbackRepository + Send + Sync> = Arc::new(mock);
    }
}
