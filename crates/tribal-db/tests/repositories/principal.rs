use tribal_db::{DbError, PgPrincipalRepository, PrincipalRepository};
use tribal_domain::PrincipalId;
use tribal_test_utils::{a_new_principal, test_context};

#[tokio::test]
async fn test_insert_returns_populated_principal() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPrincipalRepository;

    let result = repo.find_by_id(&mut txn, PrincipalId::new()).await;
    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}

#[tokio::test]
async fn test_find_by_key_returns_principal() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
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
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPrincipalRepository;

    let result = repo
        .find_by_key(&mut txn, "user:nonexistent")
        .await
        .expect("find_by_key");

    assert!(result.is_none());
}
