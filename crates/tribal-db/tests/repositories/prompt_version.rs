use tribal_db::{DbError, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{PromptClass, PromptRole, PromptStage, PromptVersionId};
use tribal_test_utils::{TestDb, a_new_prompt_version};

// ---------------------------------------------------------------------------
// upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upsert_returns_new_prompt_version() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPromptVersionRepository;

    let new = a_new_prompt_version()
        .stage(PromptStage::Triage)
        .content_hash("c".repeat(64))
        .content("triage prompt".to_owned())
        .build();

    let pv = repo.upsert(&mut txn, &new).await.expect("upsert");

    assert!(pv.id().to_string().starts_with("pv_"));
    assert_eq!(pv.stage(), PromptStage::Triage);
    assert_eq!(pv.role(), PromptRole::System);
    assert_eq!(pv.content_hash(), "c".repeat(64));
    assert_eq!(pv.content(), "triage prompt");
}

#[tokio::test]
async fn test_upsert_returns_existing_on_duplicate_stage_and_hash() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPromptVersionRepository;

    let new = a_new_prompt_version().build();

    let first = repo.upsert(&mut txn, &new).await.expect("first upsert");
    let second = repo.upsert(&mut txn, &new).await.expect("second upsert");

    assert_eq!(first.id(), second.id());
    assert_eq!(first.created_at(), second.created_at());
}

#[tokio::test]
async fn test_upsert_different_hash_creates_new_version() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
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
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPromptVersionRepository;

    let result = repo.find_by_id(&mut txn, PromptVersionId::new()).await;
    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// find_by_slot_and_hash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_slot_and_hash_returns_prompt_version() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPromptVersionRepository;

    let new = a_new_prompt_version()
        .stage(PromptStage::Relation)
        .role(PromptRole::System)
        .content_hash("d".repeat(64))
        .build();
    let pv = repo.upsert(&mut txn, &new).await.expect("upsert");

    let found = repo
        .find_by_slot_and_hash(
            &mut txn,
            PromptStage::Relation,
            PromptClass::OneShot,
            PromptRole::System,
            &"d".repeat(64),
        )
        .await
        .expect("find");

    assert_eq!(found.unwrap().id(), pv.id());
}

#[tokio::test]
async fn test_find_by_slot_and_hash_returns_none() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPromptVersionRepository;

    let found = repo
        .find_by_slot_and_hash(
            &mut txn,
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::System,
            &"f".repeat(64),
        )
        .await
        .expect("find");

    assert!(found.is_none());
}

#[tokio::test]
async fn test_upsert_same_stage_hash_different_role_creates_separate_record() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPromptVersionRepository;

    let hash = "a".repeat(64);

    let system = repo
        .upsert(
            &mut txn,
            &a_new_prompt_version()
                .role(PromptRole::System)
                .content_hash(hash.clone())
                .content("system content".to_owned())
                .build(),
        )
        .await
        .expect("system upsert");

    let user = repo
        .upsert(
            &mut txn,
            &a_new_prompt_version()
                .role(PromptRole::User)
                .content_hash(hash)
                .content("user content".to_owned())
                .build(),
        )
        .await
        .expect("user upsert");

    assert_ne!(system.id(), user.id());
    assert_eq!(system.stage(), user.stage());
    assert_eq!(system.role(), PromptRole::System);
    assert_eq!(user.role(), PromptRole::User);
}

#[tokio::test]
async fn test_find_by_slot_and_hash_distinguishes_roles() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgPromptVersionRepository;

    let hash = "e".repeat(64);

    let system_pv = repo
        .upsert(
            &mut txn,
            &a_new_prompt_version()
                .role(PromptRole::System)
                .content_hash(hash.clone())
                .content("system content".to_owned())
                .build(),
        )
        .await
        .expect("system upsert");

    let user_pv = repo
        .upsert(
            &mut txn,
            &a_new_prompt_version()
                .role(PromptRole::User)
                .content_hash(hash.clone())
                .content("user content".to_owned())
                .build(),
        )
        .await
        .expect("user upsert");

    let found_system = repo
        .find_by_slot_and_hash(
            &mut txn,
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::System,
            &hash,
        )
        .await
        .expect("find system")
        .expect("system should exist");

    let found_user = repo
        .find_by_slot_and_hash(
            &mut txn,
            PromptStage::Extraction,
            PromptClass::OneShot,
            PromptRole::User,
            &hash,
        )
        .await
        .expect("find user")
        .expect("user should exist");

    assert_eq!(found_system.id(), system_pv.id());
    assert_eq!(found_user.id(), user_pv.id());
    assert_ne!(found_system.id(), found_user.id());
}
