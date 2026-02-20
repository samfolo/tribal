use tribal_db::{DbError, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{PromptStage, PromptVersionId};
use tribal_test_utils::{a_new_prompt_version, test_context};

// ---------------------------------------------------------------------------
// upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upsert_returns_new_prompt_version() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPromptVersionRepository;

    let new = a_new_prompt_version()
        .stage(PromptStage::Triage)
        .content_hash("c".repeat(64))
        .content("triage prompt".to_owned())
        .build();

    let pv = repo.upsert(&mut txn, &new).await.expect("upsert");

    assert!(pv.id().to_string().starts_with("pv_"));
    assert_eq!(pv.stage(), PromptStage::Triage);
    assert_eq!(pv.content_hash(), "c".repeat(64));
    assert_eq!(pv.content(), "triage prompt");
}

#[tokio::test]
async fn test_upsert_returns_existing_on_duplicate_stage_and_hash() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPromptVersionRepository;

    let new = a_new_prompt_version().build();

    let first = repo.upsert(&mut txn, &new).await.expect("first upsert");
    let second = repo.upsert(&mut txn, &new).await.expect("second upsert");

    assert_eq!(first.id(), second.id());
    assert_eq!(first.created_at(), second.created_at());
}

#[tokio::test]
async fn test_upsert_different_hash_creates_new_version() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPromptVersionRepository;

    let first = repo
        .upsert(
            &mut txn,
            &a_new_prompt_version()
                .content_hash("a".repeat(64))
                .content("v1".to_owned())
                .build(),
        )
        .await
        .expect("first upsert");

    let second = repo
        .upsert(
            &mut txn,
            &a_new_prompt_version()
                .content_hash("b".repeat(64))
                .content("v2".to_owned())
                .build(),
        )
        .await
        .expect("second upsert");

    assert_ne!(first.id(), second.id());
    assert_eq!(first.stage(), second.stage());
}

// ---------------------------------------------------------------------------
// find_by_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_id_returns_prompt_version() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPromptVersionRepository;

    let pv = repo
        .upsert(&mut txn, &a_new_prompt_version().build())
        .await
        .expect("upsert");

    let found = repo.find_by_id(&mut txn, pv.id()).await.expect("find");
    assert_eq!(found.id(), pv.id());
    assert_eq!(found.content(), pv.content());
}

#[tokio::test]
async fn test_find_by_id_not_found() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPromptVersionRepository;

    let result = repo.find_by_id(&mut txn, PromptVersionId::new()).await;
    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// find_by_stage_and_hash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_stage_and_hash_returns_prompt_version() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPromptVersionRepository;

    let new = a_new_prompt_version()
        .stage(PromptStage::Relation)
        .content_hash("d".repeat(64))
        .build();
    let pv = repo.upsert(&mut txn, &new).await.expect("upsert");

    let found = repo
        .find_by_stage_and_hash(&mut txn, PromptStage::Relation, &"d".repeat(64))
        .await
        .expect("find");

    assert_eq!(found.unwrap().id(), pv.id());
}

#[tokio::test]
async fn test_find_by_stage_and_hash_returns_none() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgPromptVersionRepository;

    let found = repo
        .find_by_stage_and_hash(&mut txn, PromptStage::Extraction, &"f".repeat(64))
        .await
        .expect("find");

    assert!(found.is_none());
}
