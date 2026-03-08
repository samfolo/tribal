//! Mock implementation of [`ReferenceRepository`].

use tribal_db::{NewReference, ReferenceRepository};
use tribal_domain::{KnowledgeItemId, Reference};

use super::mock_repository;

mock_repository! {
    MockReferenceRepository for ReferenceRepository, tribal_db::DbError {
        insert(NewReference => Reference)
            (new: &NewReference) { new.clone() };
        find_by_knowledge_item_id(KnowledgeItemId => Vec<Reference>)
            (knowledge_item_id: KnowledgeItemId) { knowledge_item_id };
        find_by_knowledge_item_ids(Vec<KnowledgeItemId> => Vec<Reference>)
            (knowledge_item_ids: &[KnowledgeItemId]) { knowledge_item_ids.to_vec() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::ReferenceRepository;

    use super::*;
    use crate::{a_reference, test_context};

    #[tokio::test]
    async fn test_find_by_knowledge_item_ids_returns_canned_references() {
        let reference = a_reference().build();
        let mock = MockReferenceRepository::builder()
            .on_find_by_knowledge_item_ids(vec![reference.clone()], None)
            .build();

        let ctx = test_context().await;
        let mut conn = ctx.conn().await;

        let ids = vec![KnowledgeItemId::new()];
        let result = mock
            .find_by_knowledge_item_ids(&mut conn, &ids)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), reference.id());
    }

    #[tokio::test]
    async fn test_find_by_knowledge_item_id_returns_canned_references() {
        let reference = a_reference().build();
        let mock = MockReferenceRepository::builder()
            .on_find_by_knowledge_item_id(vec![reference.clone()], None)
            .build();

        let ctx = test_context().await;
        let mut conn = ctx.conn().await;

        let id = KnowledgeItemId::new();
        let result = mock
            .find_by_knowledge_item_id(&mut conn, id)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id(), reference.id());
    }

    #[tokio::test]
    async fn test_find_by_knowledge_item_ids_records_history() {
        let mock = MockReferenceRepository::builder()
            .on_find_by_knowledge_item_ids(vec![], None)
            .build();

        let ctx = test_context().await;
        let mut conn = ctx.conn().await;

        let id = KnowledgeItemId::new();
        let _ = mock
            .find_by_knowledge_item_ids(&mut conn, &[id])
            .await;

        let history = mock.find_by_knowledge_item_ids_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0], vec![id]);
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockReferenceRepository>();

        let mock = MockReferenceRepository::builder().build();
        let _arc: Arc<dyn ReferenceRepository + Send + Sync> = Arc::new(mock);
    }
}
