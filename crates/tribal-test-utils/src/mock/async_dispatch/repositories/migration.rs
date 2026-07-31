//! Mock implementation of [`MigrationRepository`].

use tribal_db::{MigrationHeadStatus, MigrationRepository};

use super::mock_repository;

mock_repository! {
    MockMigrationRepository for MigrationRepository, tribal_db::DbError {
        has_migrations_table(() => bool)
            () { () };
        current_head_matches(i64 => MigrationHeadStatus)
            (expected: i64) { expected }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::{MigrationHeadStatus, MigrationRepository};

    use super::*;
    use crate::TestDb;

    #[tokio::test]
    async fn test_current_head_matches_returns_canned_status() {
        let mock = MockMigrationRepository::builder()
            .on_current_head_matches(MigrationHeadStatus::Matches, None)
            .build();

        let ctx = TestDb::new().await;
        let mut tx = ctx.begin().await.expect("begin");

        let status = mock
            .current_head_matches(&mut tx, 42)
            .await
            .expect("current_head_matches");
        assert_eq!(status, MigrationHeadStatus::Matches);
    }

    #[tokio::test]
    async fn test_current_head_matches_records_history() {
        let mock = MockMigrationRepository::builder()
            .on_current_head_matches(MigrationHeadStatus::Matches, None)
            .build();

        let ctx = TestDb::new().await;
        let mut tx = ctx.begin().await.expect("begin");

        let _ = mock.current_head_matches(&mut tx, 7).await;

        let history = mock.current_head_matches_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0], 7);
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockMigrationRepository>();

        let mock = MockMigrationRepository::builder().build();
        let _arc: Arc<dyn MigrationRepository + Send + Sync> = Arc::new(mock);
    }
}
