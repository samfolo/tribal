//! Mock implementation of [`PrincipalRepository`].

use tribal_db::{NewPrincipal, PrincipalRepository};
use tribal_domain::{Principal, PrincipalId};

use super::mock_repository;

mock_repository! {
    MockPrincipalRepository for PrincipalRepository, tribal_db::DbError {
        insert(NewPrincipal => Principal)
            (new_principal: &NewPrincipal) { new_principal.clone() };
        find_by_id(PrincipalId => Principal)
            (id: PrincipalId) { id };
        find_by_key(String => Option<Principal>)
            (principal_key: &str) { principal_key.to_owned() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::PrincipalRepository;

    use super::*;
    use crate::{a_principal, test_context};

    #[tokio::test]
    async fn test_find_by_id_returns_canned_principal() {
        let principal = a_principal().build();
        let mock = MockPrincipalRepository::builder()
            .on_find_by_id(principal.clone(), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let result = mock.find_by_id(&mut tx, principal.id()).await.unwrap();
        assert_eq!(result.principal_key(), principal.principal_key());
    }

    #[tokio::test]
    async fn test_find_by_key_returns_canned_principal() {
        let principal = a_principal().build();
        let mock = MockPrincipalRepository::builder()
            .on_find_by_key(Some(principal.clone()), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let result = mock
            .find_by_key(&mut tx, principal.principal_key())
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().principal_key(), principal.principal_key());
    }

    #[tokio::test]
    async fn test_find_by_id_records_history() {
        let principal = a_principal().build();
        let mock = MockPrincipalRepository::builder()
            .on_find_by_id(principal.clone(), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let id = principal.id();
        let _ = mock.find_by_id(&mut tx, id).await;

        let history = mock.find_by_id_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0], id);
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockPrincipalRepository>();

        let mock = MockPrincipalRepository::builder().build();
        let _arc: Arc<dyn PrincipalRepository + Send + Sync> = Arc::new(mock);
    }
}
