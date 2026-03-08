//! Mock implementation of [`StandingRepository`].

use tribal_db::StandingRepository;
use tribal_domain::{KnowledgeItemId, Standing};

use super::mock_repository;

mock_repository! {
    MockStandingRepository for StandingRepository, tribal_db::DbError {
        compute(Vec<KnowledgeItemId> => Vec<Standing>)
            (ids: &[KnowledgeItemId]) { ids.to_vec() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::StandingRepository;

    use super::*;
    use crate::{a_standing, test_context};

    #[tokio::test]
    async fn test_compute_returns_canned_standings() {
        let standing = a_standing().build();
        let mock = MockStandingRepository::builder()
            .on_compute(vec![standing.clone()], None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let ids = vec![KnowledgeItemId::new()];
        let result = mock.compute(&mut tx, &ids).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].supporting_count(), standing.supporting_count(),);
    }

    #[tokio::test]
    async fn test_compute_records_history() {
        let mock = MockStandingRepository::builder()
            .on_compute(vec![], None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let id = KnowledgeItemId::new();
        let _ = mock.compute(&mut tx, &[id]).await;

        let history = mock.compute_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0], vec![id]);
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockStandingRepository>();

        let mock = MockStandingRepository::builder().build();
        let _arc: Arc<dyn StandingRepository + Send + Sync> = Arc::new(mock);
    }
}
