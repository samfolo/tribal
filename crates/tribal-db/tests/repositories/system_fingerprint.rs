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

    assert!(fp.id().to_string().starts_with("sf_"));
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
async fn test_upsert_different_build_version_produces_different_hash() {
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
