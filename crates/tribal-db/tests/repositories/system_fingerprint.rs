use tribal_db::{PgSystemFingerprintRepository, SystemFingerprintRepository};
use tribal_test_utils::{
    a_new_prompt_version, a_new_system_fingerprint, insert_prompt_version, test_context,
};

// ---------------------------------------------------------------------------
// upsert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upsert_returns_fingerprint() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgSystemFingerprintRepository;

    let pv_id = insert_prompt_version(&mut txn, &a_new_prompt_version().build()).await;

    let new = a_new_system_fingerprint()
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .build();

    let fp = repo.upsert(&mut txn, &new).await.expect("upsert");

    assert!(fp.id().to_string().starts_with("sfp_"));
    assert_eq!(fp.content_hash(), new.content_hash);
    assert_eq!(fp.build_version(), new.build_version);
}

#[tokio::test]
async fn test_upsert_idempotency() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgSystemFingerprintRepository;

    let pv_id = insert_prompt_version(&mut txn, &a_new_prompt_version().build()).await;

    let new = a_new_system_fingerprint()
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .build();

    let first = repo.upsert(&mut txn, &new).await.expect("first upsert");
    let second = repo.upsert(&mut txn, &new).await.expect("second upsert");

    assert_eq!(first.id(), second.id());
    assert_eq!(first.content_hash(), second.content_hash());
}

#[tokio::test]
async fn test_upsert_different_content_hash_produces_different_id() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgSystemFingerprintRepository;

    let pv_id = insert_prompt_version(&mut txn, &a_new_prompt_version().build()).await;

    let first = repo
        .upsert(
            &mut txn,
            &a_new_system_fingerprint()
                .content_hash("a".repeat(64))
                .build_version("v0.1.0".to_owned())
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .build(),
        )
        .await
        .expect("first upsert");

    let second = repo
        .upsert(
            &mut txn,
            &a_new_system_fingerprint()
                .content_hash("b".repeat(64))
                .build_version("v0.2.0".to_owned())
                .extraction_system_prompt_version_id(pv_id)
                .extraction_user_prompt_version_id(pv_id)
                .triage_system_prompt_version_id(pv_id)
                .triage_user_prompt_version_id(pv_id)
                .relation_system_prompt_version_id(pv_id)
                .relation_user_prompt_version_id(pv_id)
                .build(),
        )
        .await
        .expect("second upsert");

    assert_ne!(first.content_hash(), second.content_hash());
    assert_ne!(first.id(), second.id());
}

// ---------------------------------------------------------------------------
// find_by_hash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_hash_returns_stored_fingerprint() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgSystemFingerprintRepository;

    let pv_id = insert_prompt_version(&mut txn, &a_new_prompt_version().build()).await;

    let new = a_new_system_fingerprint()
        .extraction_system_prompt_version_id(pv_id)
        .extraction_user_prompt_version_id(pv_id)
        .triage_system_prompt_version_id(pv_id)
        .triage_user_prompt_version_id(pv_id)
        .relation_system_prompt_version_id(pv_id)
        .relation_user_prompt_version_id(pv_id)
        .build();

    let upserted = repo.upsert(&mut txn, &new).await.expect("upsert");

    let found = repo
        .find_by_hash(&mut txn, upserted.content_hash())
        .await
        .expect("find_by_hash")
        .expect("should find the fingerprint");

    assert_eq!(found.id(), upserted.id());
    assert_eq!(found.content_hash(), upserted.content_hash());
    assert_eq!(found.build_version(), upserted.build_version());
    assert_eq!(
        found.inference_parameters(),
        upserted.inference_parameters()
    );
}

#[tokio::test]
async fn test_find_by_hash_unknown_returns_none() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgSystemFingerprintRepository;

    let result = repo
        .find_by_hash(&mut txn, &"0".repeat(64))
        .await
        .expect("find_by_hash");

    assert!(result.is_none());
}
