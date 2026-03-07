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
    use tribal_domain::RetrievalFeedbackId;

    use super::*;
    use crate::{a_retrieval_feedback, test_context};

    // -- Constants ----------------------------------------------------------

    const EXPECT_BEGIN: &str = "should begin transaction";

    // -- Tests --------------------------------------------------------------

    #[tokio::test]
    async fn test_sequential_find_by_id_returns_in_order() {
        let rf1 = a_retrieval_feedback().build();
        let rf2 = a_retrieval_feedback().build();

        let mock = MockRetrievalFeedbackRepository::builder()
            .on_find_by_id(rf1.clone(), None)
            .on_find_by_id(rf2.clone(), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);

        let r1 = mock
            .find_by_id(&mut tx, RetrievalFeedbackId::new())
            .await
            .unwrap();
        let r2 = mock
            .find_by_id(&mut tx, RetrievalFeedbackId::new())
            .await
            .unwrap();

        assert_eq!(r1.id(), rf1.id());
        assert_eq!(r2.id(), rf2.id());
    }

    #[tokio::test]
    async fn test_find_by_id_records_history() {
        let rf = a_retrieval_feedback().build();

        let mock = MockRetrievalFeedbackRepository::builder()
            .on_find_by_id(rf, None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);

        let id = RetrievalFeedbackId::new();
        let _ = mock.find_by_id(&mut tx, id).await.unwrap();

        let history = mock.find_by_id_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0], id);
    }

    #[test]
    fn test_send_sync_behind_arc() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockRetrievalFeedbackRepository>();

        let mock = MockRetrievalFeedbackRepository::builder().build();
        let _arc: Arc<dyn RetrievalFeedbackRepository + Send + Sync> = Arc::new(mock);
    }
}
