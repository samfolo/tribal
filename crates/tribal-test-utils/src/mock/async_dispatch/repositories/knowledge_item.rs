//! Mock implementation of [`KnowledgeItemRepository`].

use tribal_db::{
    KnowledgeItemRepository, NewKnowledgeItem, SemanticSearchParams, SemanticSearchResponse,
};
use tribal_domain::{KnowledgeItem, KnowledgeItemId};

use super::mock_repository;

mock_repository! {
    MockKnowledgeItemRepository for KnowledgeItemRepository, tribal_db::DbError {
        insert(NewKnowledgeItem => KnowledgeItem)
            (new: &NewKnowledgeItem) { new.clone() };
        find_by_id(KnowledgeItemId => KnowledgeItem)
            (id: KnowledgeItemId) { id };
        find_by_ids(Vec<KnowledgeItemId> => Vec<KnowledgeItem>)
            (ids: &[KnowledgeItemId]) { ids.to_vec() };
        find_existing_ids(Vec<KnowledgeItemId> => Vec<KnowledgeItemId>)
            (ids: &[KnowledgeItemId]) { ids.to_vec() };
        semantic_search(SemanticSearchParams => SemanticSearchResponse)
            (params: &SemanticSearchParams) { params.clone() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::KnowledgeItemRepository;

    use super::*;
    use crate::{a_knowledge_item, test_context};

    // -- Constants ----------------------------------------------------------

    const EXPECT_BEGIN: &str = "should begin transaction";

    // -- Tests --------------------------------------------------------------

    #[tokio::test]
    async fn test_slice_to_vec_find_by_ids_records_converted_history() {
        let ki1 = a_knowledge_item().build();
        let ki2 = a_knowledge_item().build();

        let mock = MockKnowledgeItemRepository::builder()
            .on_find_by_ids(vec![ki1.clone(), ki2.clone()], None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);

        let query_ids = vec![ki1.id(), ki2.id()];
        let result = mock.find_by_ids(&mut tx, &query_ids).await.unwrap();

        assert_eq!(result.len(), 2);

        let history = mock.find_by_ids_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0], query_ids);
    }

    #[tokio::test]
    async fn test_conditional_find_by_ids_matches_on_slice_contents() {
        let target_id = KnowledgeItemId::new();
        let ki = a_knowledge_item().build();

        let mock = MockKnowledgeItemRepository::builder()
            .when_find_by_ids(move |ids| ids.contains(&target_id))
            .respond_with(vec![ki.clone()], None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect(EXPECT_BEGIN);

        let result = mock.find_by_ids(&mut tx, &[target_id]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), ki.id());
    }

    #[test]
    fn test_send_sync_behind_arc() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockKnowledgeItemRepository>();

        let mock = MockKnowledgeItemRepository::builder().build();
        let _arc: Arc<dyn KnowledgeItemRepository + Send + Sync> = Arc::new(mock);
    }
}
