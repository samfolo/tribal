use chrono::{SubsecRound, Utc};
use tribal_db::{
    AuthTokenRepository, DbError, PgAuthTokenRepository, PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::{AuthTokenId, PrincipalId};
use tribal_test_utils::{a_new_auth_token, a_new_principal, shift_timestamp_by_id, test_context};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal and returns its ID.
async fn setup_principal(txn: &mut sqlx::PgConnection, suffix: &str) -> PrincipalId {
    PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:auth-token-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal")
        .id()
}

/// Generates a valid 64-character hex hash from a UUID.
fn make_token_hash() -> String {
    format!("{:064x}", uuid::Uuid::new_v4().as_u128())
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_auth_token() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "insert").await;
    let hash = make_token_hash();
    let expires_at = (Utc::now() + chrono::Duration::hours(24)).trunc_subsecs(6);

    let new = a_new_auth_token()
        .token_hash(hash.clone())
        .principal_id(principal_id)
        .expires_at(expires_at)
        .build();

    let token = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(token.id().to_string().starts_with("at_"));
    assert_eq!(token.token_hash(), hash);
    assert_eq!(token.principal_id(), principal_id);
    assert_eq!(token.expires_at(), expires_at);
    assert!(token.revoked_at().is_none());
}

#[tokio::test]
async fn test_insert_duplicate_hash_returns_unique_violation() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "dup-hash").await;
    let hash = make_token_hash();

    let new = a_new_auth_token()
        .token_hash(hash.clone())
        .principal_id(principal_id)
        .build();

    repo.insert(&mut txn, &new).await.expect("first insert");

    let result = repo.insert(&mut txn, &new).await;
    assert!(
        matches!(result, Err(DbError::UniqueViolation { .. })),
        "expected UniqueViolation, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// find_by_hash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_hash_returns_auth_token() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "find-hash").await;
    let hash = make_token_hash();

    let token = repo
        .insert(
            &mut txn,
            &a_new_auth_token()
                .token_hash(hash.clone())
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert");

    let found = repo.find_by_hash(&mut txn, &hash).await.expect("find");

    assert_eq!(found.unwrap().id(), token.id());
}

#[tokio::test]
async fn test_find_by_hash_returns_none_for_unknown() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let found = repo
        .find_by_hash(&mut txn, &make_token_hash())
        .await
        .expect("find");

    assert!(found.is_none());
}

// ---------------------------------------------------------------------------
// find_by_principal_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_principal_id_returns_tokens_ordered() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "find-principal").await;

    let first = repo
        .insert(
            &mut txn,
            &a_new_auth_token()
                .token_hash(make_token_hash())
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("first insert");

    // Backdate the first token so ordering is deterministic.
    shift_timestamp_by_id(
        &mut txn,
        "auth_tokens",
        "created_at",
        *first.id().inner(),
        chrono::Duration::hours(-1),
    )
    .await;

    let _second = repo
        .insert(
            &mut txn,
            &a_new_auth_token()
                .token_hash(make_token_hash())
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("second insert");

    let results = repo
        .find_by_principal_id(&mut txn, principal_id)
        .await
        .expect("find");

    assert_eq!(results.len(), 2);
    // Ordered by created_at DESC — newest first.
    assert!(results[0].created_at() >= results[1].created_at());
}

#[tokio::test]
async fn test_find_by_principal_id_returns_empty_for_unknown() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let results = repo
        .find_by_principal_id(&mut txn, PrincipalId::new())
        .await
        .expect("find");

    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// revoke
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_revoke_sets_revoked_at() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "revoke").await;

    let token = repo
        .insert(
            &mut txn,
            &a_new_auth_token()
                .token_hash(make_token_hash())
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert");

    let revoked_at = Utc::now().trunc_subsecs(6);
    let revoked = repo
        .revoke(&mut txn, token.id(), revoked_at)
        .await
        .expect("revoke");

    assert_eq!(revoked.revoked_at(), Some(revoked_at));
    assert_eq!(revoked.id(), token.id());
}

#[tokio::test]
async fn test_revoke_is_idempotent() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "idempotent").await;

    let token = repo
        .insert(
            &mut txn,
            &a_new_auth_token()
                .token_hash(make_token_hash())
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert");

    let first_revoke_at = Utc::now().trunc_subsecs(6);
    let first = repo
        .revoke(&mut txn, token.id(), first_revoke_at)
        .await
        .expect("first revoke");

    let second_revoke_at = Utc::now().trunc_subsecs(6);
    let second = repo
        .revoke(&mut txn, token.id(), second_revoke_at)
        .await
        .expect("second revoke");

    // Original revocation timestamp preserved.
    assert_eq!(first.revoked_at(), second.revoked_at());
    assert_eq!(second.revoked_at(), Some(first_revoke_at));
}

#[tokio::test]
async fn test_revoke_not_found() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let result = repo.revoke(&mut txn, AuthTokenId::new(), Utc::now()).await;

    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}
