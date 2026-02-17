use tribal_db::{
    KnowledgeItemRepository, PgKnowledgeItemRepository, PgPrincipalRepository,
    PgProjectRepository, PgReferenceRepository, PrincipalRepository, ProjectRepository,
    ReferenceRepository,
};
use tribal_domain::{KnowledgeItemId, PrincipalId, ProjectId, ReferenceKind};
use tribal_test_utils::{
    a_new_knowledge_item, a_new_principal, a_new_project, a_new_reference, test_context,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal, project, and knowledge item, returning their IDs.
///
/// The `suffix` disambiguates git remotes and principal keys within a
/// single test transaction.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId, KnowledgeItemId) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:test-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert(
            txn,
            &a_new_project()
                .git_remote(format!("git@github.com:test/{suffix}.git"))
                .build(),
        )
        .await
        .expect("insert project");

    let item = PgKnowledgeItemRepository
        .insert(
            txn,
            &a_new_knowledge_item()
                .project_id(project.id())
                .principal_id(principal.id())
                .build(),
        )
        .await
        .expect("insert knowledge item");

    (principal.id(), project.id(), item.id())
}

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_reference() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let (_, project_id, item_id) = setup_prerequisites(&mut txn, "ref-insert-pop").await;

    let new = a_new_reference()
        .knowledge_item_id(item_id)
        .project_id(project_id)
        .kind(ReferenceKind::Url)
        .value("https://example.com/docs".to_owned())
        .description(Some("API documentation page".to_owned()))
        .commit(Some("abc123".to_owned()))
        .branch(Some("main".to_owned()))
        .build();

    let r = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(r.knowledge_item_id(), item_id);
    assert_eq!(r.kind(), ReferenceKind::Url);
    assert_eq!(r.value(), "https://example.com/docs");
    assert_eq!(r.description(), Some("API documentation page"));
    assert_eq!(r.project_id(), project_id);
    assert_eq!(r.commit(), Some("abc123"));
    assert_eq!(r.branch(), Some("main"));
}

#[tokio::test]
async fn test_insert_with_none_optional_fields() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let (_, project_id, item_id) = setup_prerequisites(&mut txn, "ref-insert-none").await;

    let new = a_new_reference()
        .knowledge_item_id(item_id)
        .project_id(project_id)
        .build();

    let r = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(r.description().is_none());
    assert!(r.commit().is_none());
    assert!(r.branch().is_none());
}

#[tokio::test]
async fn test_insert_generates_prefixed_id() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let (_, project_id, item_id) = setup_prerequisites(&mut txn, "ref-insert-prefix").await;

    let new = a_new_reference()
        .knowledge_item_id(item_id)
        .project_id(project_id)
        .build();

    let r = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(
        r.id().to_string().starts_with("ref_"),
        "expected ref_ prefix, got: {}",
        r.id()
    );
}

// ---------------------------------------------------------------------------
// find_by_knowledge_item_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_knowledge_item_id_returns_references() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let (_, project_id, item_id) = setup_prerequisites(&mut txn, "ref-find-item").await;

    let ref_a = a_new_reference()
        .knowledge_item_id(item_id)
        .project_id(project_id)
        .kind(ReferenceKind::FilePath)
        .value("//src/lib.rs".to_owned())
        .build();

    let ref_b = a_new_reference()
        .knowledge_item_id(item_id)
        .project_id(project_id)
        .kind(ReferenceKind::Url)
        .value("https://example.com".to_owned())
        .build();

    repo.insert(&mut txn, &ref_a).await.expect("insert a");
    repo.insert(&mut txn, &ref_b).await.expect("insert b");

    let found = repo
        .find_by_knowledge_item_id(&mut txn, item_id)
        .await
        .expect("find");

    assert_eq!(found.len(), 2);
}

#[tokio::test]
async fn test_find_by_knowledge_item_id_returns_empty_vec_when_none_exist() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let found = repo
        .find_by_knowledge_item_id(&mut txn, KnowledgeItemId::new())
        .await
        .expect("find");

    assert!(found.is_empty());
}

// ---------------------------------------------------------------------------
// find_by_knowledge_item_ids
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_knowledge_item_ids_returns_references_for_multiple_items() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let (_, project_id, item_a) = setup_prerequisites(&mut txn, "ref-find-ids-a").await;

    let item_b = PgKnowledgeItemRepository
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id({
                    // Re-use the principal created by setup_prerequisites
                    // by querying via the same suffix pattern.  Instead,
                    // just create a second knowledge item under the same project.
                    PrincipalId::from(
                        *tribal_db::PgPrincipalRepository
                            .find_by_key(&mut txn, "user:test-ref-find-ids-a")
                            .await
                            .expect("find principal")
                            .expect("principal exists")
                            .id()
                            .inner(),
                    )
                })
                .content("second item".to_owned())
                .build(),
        )
        .await
        .expect("insert item b")
        .id();

    repo.insert(
        &mut txn,
        &a_new_reference()
            .knowledge_item_id(item_a)
            .project_id(project_id)
            .value("//src/a.rs".to_owned())
            .build(),
    )
    .await
    .expect("insert ref for a");

    repo.insert(
        &mut txn,
        &a_new_reference()
            .knowledge_item_id(item_b)
            .project_id(project_id)
            .value("//src/b.rs".to_owned())
            .build(),
    )
    .await
    .expect("insert ref for b");

    let found = repo
        .find_by_knowledge_item_ids(&mut txn, &[item_a, item_b])
        .await
        .expect("find");

    assert_eq!(found.len(), 2);
    let values: Vec<&str> = found.iter().map(|r| r.value()).collect();
    assert!(values.contains(&"//src/a.rs"));
    assert!(values.contains(&"//src/b.rs"));
}

#[tokio::test]
async fn test_find_by_knowledge_item_ids_empty_input_returns_empty_vec() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let found = repo
        .find_by_knowledge_item_ids(&mut txn, &[])
        .await
        .expect("find");

    assert!(found.is_empty());
}

#[tokio::test]
async fn test_find_by_knowledge_item_ids_omits_items_without_references() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgReferenceRepository;

    let (_, project_id, item_with_ref) =
        setup_prerequisites(&mut txn, "ref-find-ids-omit").await;

    // Create a second knowledge item without any references.
    let item_without_ref = PgKnowledgeItemRepository
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id({
                    PrincipalId::from(
                        *tribal_db::PgPrincipalRepository
                            .find_by_key(&mut txn, "user:test-ref-find-ids-omit")
                            .await
                            .expect("find principal")
                            .expect("principal exists")
                            .id()
                            .inner(),
                    )
                })
                .content("item without references".to_owned())
                .build(),
        )
        .await
        .expect("insert item without ref")
        .id();

    repo.insert(
        &mut txn,
        &a_new_reference()
            .knowledge_item_id(item_with_ref)
            .project_id(project_id)
            .build(),
    )
    .await
    .expect("insert ref");

    let found = repo
        .find_by_knowledge_item_ids(&mut txn, &[item_with_ref, item_without_ref])
        .await
        .expect("find");

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].knowledge_item_id(), item_with_ref);
}
