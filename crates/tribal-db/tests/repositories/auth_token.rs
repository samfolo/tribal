use chrono::{SubsecRound, Utc};
use tribal_db::{
    AuthTokenRepository, DbError, PgAuthTokenRepository, PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::{AuthTokenId, PrincipalId, Scope, full_access_scopes};
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
    assert_eq!(token.scopes(), full_access_scopes());
    assert_eq!(token.expires_at(), expires_at);
    assert!(token.revoked_at().is_none());
}

#[tokio::test]
async fn test_insert_roundtrips_narrowed_scopes() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "narrowed-scopes").await;
    let narrowed = vec![Scope::parse("tribal.knowledge:read").unwrap()];

    let new = a_new_auth_token()
        .token_hash(make_token_hash())
        .principal_id(principal_id)
        .scopes(narrowed.clone())
        .build();

    let token = repo.insert(&mut txn, &new).await.expect("insert");
    assert_eq!(token.scopes(), narrowed);

    let found = repo
        .find_by_hash(&mut txn, token.token_hash())
        .await
        .expect("find")
        .expect("token should exist");
    assert_eq!(found.scopes(), narrowed);
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

// ---------------------------------------------------------------------------
// find_all
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_all_returns_tokens_ordered() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "find-all").await;

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

    shift_timestamp_by_id(
        &mut txn,
        "auth_tokens",
        "created_at",
        *first.id().inner(),
        chrono::Duration::hours(-1),
    )
    .await;

    let second = repo
        .insert(
            &mut txn,
            &a_new_auth_token()
                .token_hash(make_token_hash())
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("second insert");

    let results = repo.find_all(&mut txn).await.expect("find_all");

    assert_eq!(results.len(), 2);
    // Ordered by created_at DESC — the newer token (second) comes first.
    assert_eq!(results[0].id(), second.id());
    assert_eq!(results[1].id(), first.id());
}

#[tokio::test]
async fn test_find_all_returns_empty_when_no_tokens() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let results = repo.find_all(&mut txn).await.expect("find_all");

    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// find_by_hash_prefix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_hash_prefix_returns_matching_token() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "prefix-match").await;
    let hash = make_token_hash();
    let prefix = hash.get(..8).expect("hash is 64 chars");

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

    let results = repo
        .find_by_hash_prefix(&mut txn, prefix)
        .await
        .expect("find_by_hash_prefix");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), token.id());
}

#[tokio::test]
async fn test_find_by_hash_prefix_returns_empty_for_unknown() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let results = repo
        .find_by_hash_prefix(&mut txn, "00000000")
        .await
        .expect("find_by_hash_prefix");

    assert!(results.is_empty());
}

#[tokio::test]
async fn test_find_by_hash_prefix_returns_multiple_on_shared_prefix() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "prefix-multi").await;
    let shared_prefix = "abcdef00";

    // Insert three tokens whose hashes share the same 8-char prefix.
    // The query uses LIMIT 2, so at most two are returned — enough for
    // callers to distinguish "ambiguous" from "unique".
    let hash_a = format!("{shared_prefix}{}", &make_token_hash()[8..]);
    let hash_b = format!("{shared_prefix}{}", &make_token_hash()[8..]);
    let hash_c = format!("{shared_prefix}{}", &make_token_hash()[8..]);

    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(hash_a)
            .principal_id(principal_id)
            .build(),
    )
    .await
    .expect("insert a");

    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(hash_b)
            .principal_id(principal_id)
            .build(),
    )
    .await
    .expect("insert b");

    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(hash_c)
            .principal_id(principal_id)
            .build(),
    )
    .await
    .expect("insert c");

    let results = repo
        .find_by_hash_prefix(&mut txn, shared_prefix)
        .await
        .expect("find_by_hash_prefix");

    assert_eq!(
        results.len(),
        2,
        "LIMIT 2 caps results even with 3 matching tokens"
    );
}

// ---------------------------------------------------------------------------
// revoke_all
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_revoke_all_revokes_active_tokens() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "revoke-all").await;

    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(make_token_hash())
            .principal_id(principal_id)
            .build(),
    )
    .await
    .expect("insert first");

    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(make_token_hash())
            .principal_id(principal_id)
            .build(),
    )
    .await
    .expect("insert second");

    let count = repo
        .revoke_all(&mut txn, None, Utc::now().trunc_subsecs(6))
        .await
        .expect("revoke_all");

    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_revoke_all_with_principal_filter() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_a = setup_principal(&mut txn, "revoke-all-a").await;
    let principal_b = setup_principal(&mut txn, "revoke-all-b").await;

    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(make_token_hash())
            .principal_id(principal_a)
            .build(),
    )
    .await
    .expect("insert for a");

    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(make_token_hash())
            .principal_id(principal_b)
            .build(),
    )
    .await
    .expect("insert for b");

    let count = repo
        .revoke_all(&mut txn, Some(principal_a), Utc::now().trunc_subsecs(6))
        .await
        .expect("revoke_all");

    assert_eq!(count, 1, "only principal_a's token should be revoked");

    // Principal b's token should still be active.
    let b_tokens = repo
        .find_by_principal_id(&mut txn, principal_b)
        .await
        .expect("find");
    assert!(
        b_tokens[0].revoked_at().is_none(),
        "principal_b's token should remain active",
    );
}

#[tokio::test]
async fn test_revoke_all_skips_already_revoked() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "revoke-all-skip").await;

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

    // Pre-revoke this token.
    repo.revoke(&mut txn, token.id(), Utc::now().trunc_subsecs(6))
        .await
        .expect("revoke");

    let count = repo
        .revoke_all(&mut txn, None, Utc::now().trunc_subsecs(6))
        .await
        .expect("revoke_all");

    assert_eq!(count, 0, "already-revoked token should not be counted");
}

#[tokio::test]
async fn test_revoke_all_returns_zero_when_no_active_tokens() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let count = repo
        .revoke_all(&mut txn, None, Utc::now().trunc_subsecs(6))
        .await
        .expect("revoke_all");

    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_revoke_all_skips_expired_tokens() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "revoke-all-expired").await;

    // Insert a token that is already expired but not revoked.
    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(make_token_hash())
            .principal_id(principal_id)
            .expires_at((Utc::now() - chrono::Duration::hours(1)).trunc_subsecs(6))
            .build(),
    )
    .await
    .expect("insert expired");

    let count = repo
        .revoke_all(&mut txn, None, Utc::now().trunc_subsecs(6))
        .await
        .expect("revoke_all");

    assert_eq!(count, 0, "expired tokens should not be revoked");
}

#[tokio::test]
async fn test_revoke_all_includes_token_expiring_at_revocation_time() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgAuthTokenRepository;

    let principal_id = setup_principal(&mut txn, "revoke-all-boundary").await;
    let now = Utc::now().trunc_subsecs(6);

    // A token expiring exactly at the revocation time is still active
    // (token_status treats expires_at < now as expired, so == now is active).
    repo.insert(
        &mut txn,
        &a_new_auth_token()
            .token_hash(make_token_hash())
            .principal_id(principal_id)
            .expires_at(now)
            .build(),
    )
    .await
    .expect("insert boundary");

    let count = repo
        .revoke_all(&mut txn, None, now)
        .await
        .expect("revoke_all");

    assert_eq!(
        count, 1,
        "token expiring exactly at revocation time is still active"
    );
}
