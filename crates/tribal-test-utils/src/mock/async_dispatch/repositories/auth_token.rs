//! Mock implementation of [`AuthTokenRepository`].

use chrono::{DateTime, Utc};
use tribal_db::{AuthTokenRepository, NewAuthToken};
use tribal_domain::{AuthToken, AuthTokenId, PrincipalId};

use super::mock_repository;

mock_repository! {
    MockAuthTokenRepository for AuthTokenRepository, tribal_db::DbError {
        insert(NewAuthToken => AuthToken)
            (new: &NewAuthToken) { new.clone() };
        find_by_hash(String => Option<AuthToken>)
            (token_hash: &str) { token_hash.to_owned() };
        find_by_principal_id(PrincipalId => Vec<AuthToken>)
            (principal_id: PrincipalId) { principal_id };
        revoke((AuthTokenId, DateTime<Utc>) => (AuthToken, bool))
            (id: AuthTokenId, revoked_at: DateTime<Utc>) { (id, revoked_at) };
        find_all(() => Vec<AuthToken>)
            () { () };
        find_by_hash_prefix(String => Vec<AuthToken>)
            (prefix: &str) { prefix.to_owned() };
        revoke_all((Option<PrincipalId>, DateTime<Utc>) => u64)
            (principal_id: Option<PrincipalId>, revoked_at: DateTime<Utc>) { (principal_id, revoked_at) };
        any_active(DateTime<Utc> => bool)
            (now: DateTime<Utc>) { now }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tribal_db::AuthTokenRepository;

    use super::*;
    use crate::{an_auth_token, test_context};

    #[tokio::test]
    async fn test_find_by_hash_returns_canned_token() {
        let token = an_auth_token().build();
        let mock = MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token.clone()), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let result = mock
            .find_by_hash(&mut tx, token.token_hash())
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().id(), token.id());
    }

    #[tokio::test]
    async fn test_find_by_hash_records_history() {
        let token = an_auth_token().build();
        let mock = MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token.clone()), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        let _ = mock.find_by_hash(&mut tx, "abc123").await;

        let history = mock.find_by_hash_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0], "abc123");
    }

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockAuthTokenRepository>();

        let mock = MockAuthTokenRepository::builder().build();
        let _arc: Arc<dyn AuthTokenRepository + Send + Sync> = Arc::new(mock);
    }
}
