//! Mock implementation of [`RelationRepository`].

use tribal_db::{NewKnowledgeItemRelation, RelationRepository, TraversalResponse};
use tribal_domain::{Direction, KnowledgeItemId, KnowledgeItemRelation, RelationKind};

use super::mock_repository;

mock_repository! {
    MockRelationRepository for RelationRepository, tribal_db::DbError {
        batch_insert(Vec<NewKnowledgeItemRelation> => Vec<KnowledgeItemRelation>)
            (batch: &[NewKnowledgeItemRelation]) { batch.to_vec() };
        find_inbound((KnowledgeItemId, Option<Vec<RelationKind>>) => Vec<KnowledgeItemRelation>)
            (anchor_id: KnowledgeItemId, relation_types: Option<&[RelationKind]>)
            { (anchor_id, relation_types.map(<[RelationKind]>::to_vec)) };
        find_outbound((KnowledgeItemId, Option<Vec<RelationKind>>) => Vec<KnowledgeItemRelation>)
            (anchor_id: KnowledgeItemId, relation_types: Option<&[RelationKind]>)
            { (anchor_id, relation_types.map(<[RelationKind]>::to_vec)) };
        traverse((KnowledgeItemId, Direction, u32, u32, Option<Vec<RelationKind>>) => TraversalResponse)
            (anchor_id: KnowledgeItemId, direction: Direction, max_depth: u32, limit: u32, relation_types: Option<&[RelationKind]>)
            { (anchor_id, direction, max_depth, limit, relation_types.map(<[RelationKind]>::to_vec)) }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::RelationRepository;

    use super::*;
    use crate::test_context;

    #[tokio::test]
    async fn test_traverse_returns_canned_response() {
        let response = TraversalResponse {
            nodes: vec![],
            exact: true,
        };
        let mock = MockRelationRepository::builder()
            .on_traverse(response, None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let result = mock
            .traverse(
                &mut tx,
                KnowledgeItemId::new(),
                Direction::Inbound,
                1,
                20,
                None,
            )
            .await
            .unwrap();

        assert!(result.nodes.is_empty());
        assert!(result.exact);
    }

    #[tokio::test]
    async fn test_traverse_records_history() {
        let response = TraversalResponse {
            nodes: vec![],
            exact: true,
        };
        let mock = MockRelationRepository::builder()
            .on_traverse(response, None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let anchor_id = KnowledgeItemId::new();
        let _ = mock
            .traverse(&mut tx, anchor_id, Direction::Both, 2, 50, None)
            .await;

        let history = mock.traverse_history();
        assert_eq!(history.len(), 1);
        let (id, dir, depth, limit, types) = &history[0];
        assert_eq!(*id, anchor_id);
        assert_eq!(*dir, Direction::Both);
        assert_eq!(*depth, 2);
        assert_eq!(*limit, 50);
        assert!(types.is_none());
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockRelationRepository>();

        let mock = MockRelationRepository::builder().build();
        let _arc: Arc<dyn RelationRepository + Send + Sync> = Arc::new(mock);
    }
}
