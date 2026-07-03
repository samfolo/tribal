use tribal_db::{DbError, PgPrincipalRepository, PrincipalRepository};
use tribal_domain::PrincipalId;
use tribal_test_utils::{TestDb, a_new_principal};

// The platform-linkage checks exercise raw schema constraints there is no
// repository writer for yet, so their SQL lives in sql/ rather than inline.
const INSERT_PLATFORM_BOUND_PRINCIPAL: &str =
    include_str!("sql/insert_platform_bound_principal.sql");
const INSERT_PLATFORM_BOUND_PRINCIPAL_WITH_NAME: &str =
    include_str!("sql/insert_platform_bound_principal_with_name.sql");
const INSERT_HALF_BOUND_PRINCIPAL: &str = include_str!("sql/insert_half_bound_principal.sql");
const SELECT_PRINCIPAL_LINKAGE_BY_KEY: &str =
    include_str!("sql/select_principal_linkage_by_key.sql");

#[tokio::test]
async fn test_insert_with_none_optional_fields_returns_none() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let new = a_new_principal()
        .principal_key("user:null-fields".to_owned())
        .build();
    let principal = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(principal.display_name(), None);
}

#[tokio::test]
async fn test_insert_returns_populated_principal() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let new = a_new_principal()
        .principal_key("user:insert-test".to_owned())
        .display_name(Some("Test User".to_owned()))
        .build();

    let principal = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(principal.principal_key(), "user:insert-test");
    assert_eq!(principal.display_name(), Some("Test User"));
    assert!(principal.id().to_string().starts_with("prin_"));
}

#[tokio::test]
async fn test_insert_duplicate_principal_key_returns_unique_violation() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let new = a_new_principal()
        .principal_key("user:dup-key".to_owned())
        .build();
    repo.insert(&mut txn, &new).await.expect("first insert");

    let result = repo.insert(&mut txn, &new).await;
    assert!(
        matches!(result, Err(DbError::UniqueViolation { .. })),
        "expected UniqueViolation, got: {result:?}"
    );
}

#[tokio::test]
async fn test_find_by_id_returns_principal() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let new = a_new_principal()
        .principal_key("user:find-id".to_owned())
        .build();
    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.principal_key(), inserted.principal_key());
}

#[tokio::test]
async fn test_find_by_id_not_found_returns_error() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let result = repo.find_by_id(&mut txn, PrincipalId::new()).await;
    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}

#[tokio::test]
async fn test_find_by_key_returns_principal() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let new = a_new_principal()
        .principal_key("user:find-key".to_owned())
        .build();
    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_key(&mut txn, "user:find-key")
        .await
        .expect("find_by_key");

    assert!(found.is_some());
    assert_eq!(found.unwrap().id(), inserted.id());
}

#[tokio::test]
async fn test_find_by_key_not_found_returns_none() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let result = repo
        .find_by_key(&mut txn, "user:nonexistent")
        .await
        .expect("find_by_key");

    assert!(result.is_none());
}

// ---------------------------------------------------------------------------
// find_by_ids
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_ids_returns_matching_principals() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let mut ids = Vec::new();
    for i in 0..3 {
        let new = a_new_principal()
            .principal_key(format!("user:batch-{i}"))
            .build();
        let principal = repo.insert(&mut txn, &new).await.expect("insert");
        ids.push(principal.id());
    }

    let found = repo.find_by_ids(&mut txn, &ids).await.expect("find_by_ids");

    assert_eq!(found.len(), 3);
    let found_ids: Vec<PrincipalId> = found.iter().map(|p| p.id()).collect();
    for id in &ids {
        assert!(found_ids.contains(id), "missing id {id}");
    }
}

#[tokio::test]
async fn test_find_by_ids_omits_missing_ids() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let new = a_new_principal()
        .principal_key("user:batch-omit".to_owned())
        .build();
    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let ids = vec![inserted.id(), PrincipalId::new()];
    let found = repo.find_by_ids(&mut txn, &ids).await.expect("find_by_ids");

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id(), inserted.id());
}

#[tokio::test]
async fn test_find_by_ids_empty_input_returns_empty_vec() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let found = repo.find_by_ids(&mut txn, &[]).await.expect("find_by_ids");

    assert!(found.is_empty());
}

#[tokio::test]
async fn test_find_by_ids_all_missing_returns_empty_vec() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let ids = vec![PrincipalId::new(), PrincipalId::new()];
    let found = repo.find_by_ids(&mut txn, &ids).await.expect("find_by_ids");

    assert!(found.is_empty());
}

// ---------------------------------------------------------------------------
// Platform linkage — the account reference and opaque platform user id, and the
// rule that a platform-bound principal derives its display name and stores none.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_platform_bound_principal_stores_reference_and_key_only() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    sqlx::query(INSERT_PLATFORM_BOUND_PRINCIPAL)
        .execute(&mut *txn)
        .await
        .expect("a platform-bound principal without a display name inserts");

    let (account_reference, platform_user_id, display_name): (
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(SELECT_PRINCIPAL_LINKAGE_BY_KEY)
        .bind("user:platform-bound")
        .fetch_one(&mut *txn)
        .await
        .expect("read the platform-bound principal");

    assert_eq!(account_reference.as_deref(), Some("account_1"));
    assert_eq!(platform_user_id.as_deref(), Some("user_1"));
    assert_eq!(
        display_name, None,
        "a platform-bound principal stores no name"
    );
}

#[tokio::test]
async fn test_platform_bound_principal_rejects_a_stored_display_name() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let result = sqlx::query(INSERT_PLATFORM_BOUND_PRINCIPAL_WITH_NAME)
        .execute(&mut *txn)
        .await;

    assert!(
        result.is_err(),
        "a platform-bound principal must not store a display name"
    );
}

#[tokio::test]
async fn test_platform_binding_requires_both_columns() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let result = sqlx::query(INSERT_HALF_BOUND_PRINCIPAL)
        .execute(&mut *txn)
        .await;

    assert!(
        result.is_err(),
        "an account reference without the opaque user id must be refused"
    );
}

#[tokio::test]
async fn test_local_principal_has_no_platform_linkage() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPrincipalRepository;

    let new = a_new_principal()
        .principal_key("user:local-only".to_owned())
        .display_name(Some("Local User".to_owned()))
        .build();
    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let (account_reference, platform_user_id, display_name): (
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(SELECT_PRINCIPAL_LINKAGE_BY_KEY)
        .bind("user:local-only")
        .fetch_one(&mut *txn)
        .await
        .expect("read the local principal");

    assert_eq!(account_reference, None);
    assert_eq!(platform_user_id, None);
    assert_eq!(display_name.as_deref(), Some("Local User"));
    assert_eq!(inserted.display_name(), Some("Local User"));
}
