//! Mock-based and integration tests for token verification and
//! stdio principal resolution.

use std::sync::Arc;

use chrono::{Duration, Utc};
use tribal_common::sha256_hex;
use tribal_db::{
    AuthTokenRepository, NewAuthToken, NewPrincipal, PgAuthTokenRepository, PgPrincipalRepository,
    PrincipalRepository,
};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, PrincipalId};
use tribal_mcp::{AuthError, Authenticator};
use tribal_test_utils::{
    MockAuthTokenRepository, MockPrincipalRepository, TEST_PRINCIPAL_KEY, a_not_found, a_principal,
    a_query_failed, an_auth_token, test_context,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_authenticator(
    auth_token: MockAuthTokenRepository,
    principal: MockPrincipalRepository,
) -> Authenticator {
    Authenticator::new(Arc::new(auth_token), Arc::new(principal))
}

// ---------------------------------------------------------------------------
// verify_token — mock-based tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_verify_token_valid() {
    let principal_id = PrincipalId::new();
    let principal = a_principal()
        .id(principal_id)
        .principal_key(TEST_PRINCIPAL_KEY.to_owned())
        .build();
    let token = an_auth_token()
        .principal_id(principal_id)
        .expires_at(Utc::now() + Duration::hours(1))
        .build();

    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token), None)
            .build(),
        MockPrincipalRepository::builder()
            .on_find_by_id(principal, None)
            .build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let result = authenticator
        .verify_token(&mut tx, "raw-token")
        .await
        .expect("verification should succeed");

    assert_eq!(result.principal_id(), principal_id);
    assert_eq!(result.principal_key(), TEST_PRINCIPAL_KEY);
}

#[tokio::test]
async fn test_verify_token_invalid_hash() {
    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash(None, None)
            .build(),
        MockPrincipalRepository::builder().build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .verify_token(&mut tx, "unknown-token")
        .await
        .expect_err("should fail for unknown hash");

    assert!(matches!(err, AuthError::InvalidToken { .. }));
}

#[tokio::test]
async fn test_verify_token_revoked() {
    let token = an_auth_token()
        .revoked_at(Some(Utc::now()))
        .expires_at(Utc::now() + Duration::hours(1))
        .build();

    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token), None)
            .build(),
        MockPrincipalRepository::builder().build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .verify_token(&mut tx, "revoked-token")
        .await
        .expect_err("should fail for revoked token");

    assert!(matches!(err, AuthError::TokenRevoked { .. }));
}

#[tokio::test]
async fn test_verify_token_expired() {
    let token = an_auth_token()
        .expires_at(Utc::now() - Duration::hours(1))
        .build();

    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token), None)
            .build(),
        MockPrincipalRepository::builder().build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .verify_token(&mut tx, "expired-token")
        .await
        .expect_err("should fail for expired token");

    assert!(matches!(err, AuthError::TokenExpired { .. }));
}

#[tokio::test]
async fn test_verify_token_revoked_and_expired() {
    let token = an_auth_token()
        .revoked_at(Some(Utc::now()))
        .expires_at(Utc::now() - Duration::hours(1))
        .build();

    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token), None)
            .build(),
        MockPrincipalRepository::builder().build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .verify_token(&mut tx, "revoked-and-expired-token")
        .await
        .expect_err("revocation should take precedence over expiry");

    assert!(matches!(err, AuthError::TokenRevoked { .. }));
}

#[tokio::test]
async fn test_verify_token_principal_not_found() {
    let principal_id = PrincipalId::new();
    let token = an_auth_token()
        .principal_id(principal_id)
        .expires_at(Utc::now() + Duration::hours(1))
        .build();

    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token), None)
            .build(),
        MockPrincipalRepository::builder()
            .on_find_by_id_error(a_not_found("principal", principal_id.to_string()), None)
            .build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .verify_token(&mut tx, "orphaned-token")
        .await
        .expect_err("should fail when principal is missing");

    assert!(matches!(
        err,
        AuthError::PrincipalNotFound { principal_id: id } if id == principal_id
    ));
}

#[tokio::test]
async fn test_verify_token_find_by_hash_database_error() {
    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash_error(a_query_failed("connection reset"), None)
            .build(),
        MockPrincipalRepository::builder().build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .verify_token(&mut tx, "any-token")
        .await
        .expect_err("should propagate database error from find_by_hash");

    assert!(matches!(err, AuthError::DatabaseUnavailable { .. }));
}

#[tokio::test]
async fn test_verify_token_find_by_id_database_error() {
    let token = an_auth_token()
        .expires_at(Utc::now() + Duration::hours(1))
        .build();

    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder()
            .on_find_by_hash(Some(token), None)
            .build(),
        MockPrincipalRepository::builder()
            .on_find_by_id_error(a_query_failed("connection reset"), None)
            .build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .verify_token(&mut tx, "valid-but-db-fails")
        .await
        .expect_err("should propagate database error from find_by_id");

    assert!(matches!(err, AuthError::DatabaseUnavailable { .. }));
}

// ---------------------------------------------------------------------------
// resolve_stdio_principal — mock-based tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_resolve_stdio_principal_success() {
    let principal_id = PrincipalId::new();
    let principal = a_principal()
        .id(principal_id)
        .principal_key(LOCAL_PRINCIPAL_KEY.to_owned())
        .build();

    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder().build(),
        MockPrincipalRepository::builder()
            .on_find_by_key(Some(principal), None)
            .build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let result = authenticator
        .resolve_stdio_principal(&mut tx)
        .await
        .expect("resolution should succeed");

    assert_eq!(result.principal_id(), principal_id);
    assert_eq!(result.principal_key(), LOCAL_PRINCIPAL_KEY);
}

#[tokio::test]
async fn test_resolve_stdio_principal_missing() {
    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder().build(),
        MockPrincipalRepository::builder()
            .on_find_by_key(None, None)
            .build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .resolve_stdio_principal(&mut tx)
        .await
        .expect_err("should fail when local principal is missing");

    assert!(matches!(err, AuthError::LocalPrincipalMissing { .. }));
    assert!(err.to_string().contains("tribal setup"));
}

#[tokio::test]
async fn test_resolve_stdio_principal_database_error() {
    let authenticator = test_authenticator(
        MockAuthTokenRepository::builder().build(),
        MockPrincipalRepository::builder()
            .on_find_by_key_error(a_query_failed("connection reset"), None)
            .build(),
    );

    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let err = authenticator
        .resolve_stdio_principal(&mut tx)
        .await
        .expect_err("should propagate database error from find_by_key");

    assert!(matches!(err, AuthError::DatabaseUnavailable { .. }));
}

// ---------------------------------------------------------------------------
// verify_token — integration test (real database)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_verify_token_integration() {
    let ctx = test_context().await;
    let mut tx = ctx.begin_test().await.expect("begin");

    let principal = PgPrincipalRepository
        .insert(
            &mut tx,
            &NewPrincipal::builder()
                .principal_key("user:integration-test".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");

    let raw_token = "integration-test-raw-token-value";
    let token_hash = sha256_hex(raw_token);

    PgAuthTokenRepository
        .insert(
            &mut tx,
            &NewAuthToken::builder()
                .token_hash(token_hash)
                .principal_id(principal.id())
                .expires_at(Utc::now() + Duration::hours(24))
                .build(),
        )
        .await
        .expect("insert auth token");

    let authenticator = Authenticator::new(
        Arc::new(PgAuthTokenRepository),
        Arc::new(PgPrincipalRepository),
    );

    let result = authenticator
        .verify_token(&mut tx, raw_token)
        .await
        .expect("token verification should succeed");

    assert_eq!(result.principal_id(), principal.id());
    assert_eq!(result.principal_key(), principal.principal_key());
}
