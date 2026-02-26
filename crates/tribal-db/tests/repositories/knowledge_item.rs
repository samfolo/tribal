use tribal_db::{
    DbError, JobStateOverride, KnowledgeItemRepository, PgJobRepository, PgKnowledgeItemRepository,
    PgPrincipalRepository, PgProjectRepository, PrincipalRepository, ProjectRepository,
    SemanticSearchParams,
};
use tribal_domain::{
    Confidence, EpisodeId, JobOutcome, JobStatus, KnowledgeItemId, KnowledgeKind, PrincipalId,
    ProjectId, RelationBatchId, RelationKind,
};
use tribal_test_utils::{
    a_new_job, a_new_knowledge_item, a_new_principal, a_new_project, a_new_prompt_version,
    insert_committed_relation, insert_embedding, insert_prompt_version, set_timestamp,
    test_context,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal and project, returning their IDs.
///
/// The `suffix` disambiguates git remotes and principal keys within a
/// single test transaction (needed when a test creates multiple projects).
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId) {
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

    (principal.id(), project.id())
}

/// Creates a committed supersedes relation from `source_id` to `target_id`.
///
/// Sets up the required prompt_version → job → relation chain so that
/// the superseded-item exclusion query recognises the relation as committed.
async fn insert_supersedes_relation(
    txn: &mut sqlx::PgConnection,
    source_id: KnowledgeItemId,
    target_id: KnowledgeItemId,
    principal_id: PrincipalId,
    project_id: ProjectId,
) {
    let pv_id = insert_prompt_version(txn, &a_new_prompt_version().build()).await;
    let batch_id = RelationBatchId::new();

    PgJobRepository
        .insert_for_test(
            txn,
            &a_new_job()
                .project_id(project_id)
                .principal_id(principal_id)
                .extraction_prompt_version_id(pv_id)
                .triage_prompt_version_id(pv_id)
                .relation_prompt_version_id(pv_id)
                .build(),
            &JobStateOverride::builder()
                .status(JobStatus::Completed)
                .outcome(Some(JobOutcome::Success))
                .committed_batch_id(Some(batch_id))
                .build(),
        )
        .await
        .expect("insert completed job");

    insert_committed_relation(
        txn,
        batch_id,
        source_id,
        target_id,
        RelationKind::Supersedes,
        principal_id,
    )
    .await;
}

/// Creates a 768-dimensional unit vector with 1.0 at the given index.
fn make_embedding(dominant_index: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 768];
    v[dominant_index] = 1.0;
    v
}

/// Returns a query embedding with known, distinct similarities to
/// basis vectors at indices 0, 1, and 2.
fn make_query_embedding() -> Vec<f32> {
    let mut v = vec![0.0f32; 768];
    v[0] = 0.9;
    v[1] = 0.5;
    v[2] = 0.1;
    v
}

const EMBEDDING_MODEL: &str = "text-embedding-test";

// ---------------------------------------------------------------------------
// insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_returns_populated_knowledge_item() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "insert-pop").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .kind(KnowledgeKind::Fact)
        .content("The earth orbits the sun.".to_owned())
        .tags(vec!["astronomy".to_owned()])
        .confidence(Confidence::Verified)
        .source_context(serde_json::json!({"source": "textbook"}))
        .build();

    let item = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(item.project_id(), project_id);
    assert_eq!(item.principal_id(), principal_id);
    assert_eq!(item.kind(), KnowledgeKind::Fact);
    assert_eq!(item.content(), "The earth orbits the sun.");
    assert_eq!(item.tags(), &["astronomy".to_owned()]);
    assert_eq!(item.confidence(), Confidence::Verified);
    assert_eq!(
        item.source_context(),
        &serde_json::json!({"source": "textbook"})
    );
}

#[tokio::test]
async fn test_insert_with_none_optional_fields_returns_none() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "insert-none").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .build();

    let item = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(item.claim_context().is_none());
    assert!(item.episode_id().is_none());
    assert!(item.capture_commit().is_none());
    assert!(item.capture_branch().is_none());
}

#[tokio::test]
async fn test_insert_generates_prefixed_id() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "insert-prefix").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .build();

    let item = repo.insert(&mut txn, &new).await.expect("insert");

    assert!(
        item.id().to_string().starts_with("ki_"),
        "expected ki_ prefix, got: {}",
        item.id()
    );
}

#[tokio::test]
async fn test_insert_with_tags_stores_and_returns_tags() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "insert-tags").await;

    let tags = vec!["rust".to_owned(), "testing".to_owned(), "tribal".to_owned()];
    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .tags(tags.clone())
        .build();

    let item = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(item.tags(), &tags);
}

#[tokio::test]
async fn test_insert_with_all_optional_fields_round_trips() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "insert-opts").await;

    let episode_id = EpisodeId::new();
    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .claim_context(Some(serde_json::json!({"runtime": "tokio-1.x"})))
        .episode_id(Some(episode_id))
        .capture_commit(Some("abc123def456".to_owned()))
        .capture_branch(Some("feat/test-branch".to_owned()))
        .build();

    let item = repo.insert(&mut txn, &new).await.expect("insert");

    assert_eq!(
        item.claim_context(),
        Some(&serde_json::json!({"runtime": "tokio-1.x"}))
    );
    assert_eq!(item.episode_id(), Some(episode_id));
    assert_eq!(item.capture_commit(), Some("abc123def456"));
    assert_eq!(item.capture_branch(), Some("feat/test-branch"));
}

// ---------------------------------------------------------------------------
// find_by_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_id_returns_knowledge_item() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "find-id").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .build();
    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.content(), inserted.content());
}

#[tokio::test]
async fn test_find_by_id_not_found_returns_error() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let result = repo.find_by_id(&mut txn, KnowledgeItemId::new()).await;

    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// find_by_ids
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_by_ids_returns_matching_items() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "find-ids").await;

    let mut ids = Vec::new();
    for i in 0..3 {
        let new = a_new_knowledge_item()
            .project_id(project_id)
            .principal_id(principal_id)
            .content(format!("item {i}"))
            .build();
        let item = repo.insert(&mut txn, &new).await.expect("insert");
        ids.push(item.id());
    }

    let found = repo.find_by_ids(&mut txn, &ids).await.expect("find_by_ids");

    assert_eq!(found.len(), 3);
    let found_ids: Vec<KnowledgeItemId> = found.iter().map(|i| i.id()).collect();
    for id in &ids {
        assert!(found_ids.contains(id), "missing id {id}");
    }
}

#[tokio::test]
async fn test_find_by_ids_omits_missing_ids() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "find-ids-omit").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .build();
    let inserted = repo.insert(&mut txn, &new).await.expect("insert");

    let ids = vec![inserted.id(), KnowledgeItemId::new()];
    let found = repo.find_by_ids(&mut txn, &ids).await.expect("find_by_ids");

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id(), inserted.id());
}

#[tokio::test]
async fn test_find_by_ids_empty_input_returns_empty_vec() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let found = repo.find_by_ids(&mut txn, &[]).await.expect("find_by_ids");

    assert!(found.is_empty());
}

#[tokio::test]
async fn test_find_by_ids_all_missing_returns_empty_vec() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let ids = vec![KnowledgeItemId::new(), KnowledgeItemId::new()];
    let found = repo.find_by_ids(&mut txn, &ids).await.expect("find_by_ids");

    assert!(found.is_empty());
}

// ---------------------------------------------------------------------------
// semantic_search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_semantic_search_returns_results_ordered_by_similarity() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-order").await;

    let mut item_ids = Vec::new();
    for i in 0..3 {
        let new = a_new_knowledge_item()
            .project_id(project_id)
            .principal_id(principal_id)
            .content(format!("item {i}"))
            .build();
        let item = repo.insert(&mut txn, &new).await.expect("insert");
        insert_embedding(&mut txn, item.id(), EMBEDDING_MODEL, make_embedding(i)).await;
        item_ids.push(item.id());
    }

    // Query embedding favours index 0 > 1 > 2.
    let params = SemanticSearchParams::builder()
        .query_embedding(make_query_embedding())
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 3);
    assert_eq!(response.results[0].item.id(), item_ids[0]);
    assert_eq!(response.results[1].item.id(), item_ids[1]);
    assert_eq!(response.results[2].item.id(), item_ids[2]);
    assert!(response.results[0].similarity > response.results[1].similarity);
    assert!(response.results[1].similarity > response.results[2].similarity);
}

#[tokio::test]
async fn test_semantic_search_filters_by_project_id() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_a) = setup_prerequisites(&mut txn, "ss-proj-a").await;
    let (_, project_b) = setup_prerequisites(&mut txn, "ss-proj-b").await;

    let item_a = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_a)
                .principal_id(principal_id)
                .content("project A item".to_owned())
                .build(),
        )
        .await
        .expect("insert a");
    insert_embedding(&mut txn, item_a.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let item_b = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_b)
                .principal_id(principal_id)
                .content("project B item".to_owned())
                .build(),
        )
        .await
        .expect("insert b");
    insert_embedding(&mut txn, item_b.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .project_id(Some(project_a))
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].item.id(), item_a.id());
}

#[tokio::test]
async fn test_semantic_search_filters_by_kinds() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-kinds").await;

    let fact_item = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .kind(KnowledgeKind::Fact)
                .content("a fact".to_owned())
                .build(),
        )
        .await
        .expect("insert fact");
    insert_embedding(&mut txn, fact_item.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let heuristic_item = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .kind(KnowledgeKind::Heuristic)
                .content("a heuristic".to_owned())
                .build(),
        )
        .await
        .expect("insert heuristic");
    insert_embedding(
        &mut txn,
        heuristic_item.id(),
        EMBEDDING_MODEL,
        make_embedding(0),
    )
    .await;

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .kinds(Some(vec![KnowledgeKind::Fact]))
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].item.id(), fact_item.id());
}

#[tokio::test]
async fn test_semantic_search_filters_by_tags_and_semantics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-tags").await;

    let both_tags = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .tags(vec!["rust".to_owned(), "testing".to_owned()])
                .content("has both tags".to_owned())
                .build(),
        )
        .await
        .expect("insert both");
    insert_embedding(&mut txn, both_tags.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let one_tag = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .tags(vec!["rust".to_owned()])
                .content("has one tag".to_owned())
                .build(),
        )
        .await
        .expect("insert one");
    insert_embedding(&mut txn, one_tag.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    // Filter requires BOTH "rust" AND "testing" (AND semantics).
    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .tags(Some(vec!["rust".to_owned(), "testing".to_owned()]))
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].item.id(), both_tags.id());
}

#[tokio::test]
async fn test_semantic_search_filters_by_time_range_from() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-time-from").await;

    let early = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("early item".to_owned())
                .build(),
        )
        .await
        .expect("insert early");
    insert_embedding(&mut txn, early.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let late = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("late item".to_owned())
                .build(),
        )
        .await
        .expect("insert late");
    insert_embedding(&mut txn, late.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    // Shift "early" 10 seconds into the past so we can filter by time.
    let cutoff = early.created_at() - chrono::Duration::seconds(5);
    let backdated = early.created_at() - chrono::Duration::seconds(10);
    set_timestamp(
        &mut txn,
        "knowledge_items",
        "created_at",
        *early.id().inner(),
        backdated,
    )
    .await;

    // Only items after cutoff (excludes the backdated early item).
    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .time_range_from(Some(cutoff))
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].item.id(), late.id());
}

#[tokio::test]
async fn test_semantic_search_filters_by_time_range_to() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-time-to").await;

    let early = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("early item".to_owned())
                .build(),
        )
        .await
        .expect("insert early");
    insert_embedding(&mut txn, early.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let late = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("late item".to_owned())
                .build(),
        )
        .await
        .expect("insert late");
    insert_embedding(&mut txn, late.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    // Push "late" 10 seconds into the future so we can filter by upper bound.
    let cutoff = late.created_at() + chrono::Duration::seconds(5);
    let forwarded = late.created_at() + chrono::Duration::seconds(10);
    set_timestamp(
        &mut txn,
        "knowledge_items",
        "created_at",
        *late.id().inner(),
        forwarded,
    )
    .await;

    // Only items before cutoff (excludes the forwarded late item).
    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .time_range_to(Some(cutoff))
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].item.id(), early.id());
}

#[tokio::test]
async fn test_semantic_search_excludes_superseded_items() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-superseded").await;

    let target = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("superseded item".to_owned())
                .build(),
        )
        .await
        .expect("insert target");
    insert_embedding(&mut txn, target.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let source = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("superseding item".to_owned())
                .build(),
        )
        .await
        .expect("insert source");
    insert_embedding(&mut txn, source.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    insert_supersedes_relation(&mut txn, source.id(), target.id(), principal_id, project_id).await;

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    let ids: Vec<KnowledgeItemId> = response.results.iter().map(|r| r.item.id()).collect();
    assert!(ids.contains(&source.id()));
    assert!(
        !ids.contains(&target.id()),
        "superseded item should be excluded"
    );
}

#[tokio::test]
async fn test_semantic_search_includes_superseded_when_flag_set() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-include-sup").await;

    let target = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("superseded item".to_owned())
                .build(),
        )
        .await
        .expect("insert target");
    insert_embedding(&mut txn, target.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let source = repo
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content("superseding item".to_owned())
                .build(),
        )
        .await
        .expect("insert source");
    insert_embedding(&mut txn, source.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    insert_supersedes_relation(&mut txn, source.id(), target.id(), principal_id, project_id).await;

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .include_superseded(true)
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    let ids: Vec<KnowledgeItemId> = response.results.iter().map(|r| r.item.id()).collect();
    assert!(ids.contains(&source.id()));
    assert!(
        ids.contains(&target.id()),
        "superseded item should be included when flag is set"
    );
}

#[tokio::test]
async fn test_semantic_search_cursor_pagination() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-cursor").await;

    // Insert 5 items with distinct similarities to the query.
    for i in 0..5 {
        let new = a_new_knowledge_item()
            .project_id(project_id)
            .principal_id(principal_id)
            .content(format!("cursor item {i}"))
            .build();
        let item = repo.insert(&mut txn, &new).await.expect("insert");
        insert_embedding(&mut txn, item.id(), EMBEDDING_MODEL, make_embedding(i)).await;
    }

    // Query embedding that favours indices in descending order.
    let mut query_emb = vec![0.0f32; 768];
    query_emb[0] = 0.9;
    query_emb[1] = 0.7;
    query_emb[2] = 0.5;
    query_emb[3] = 0.3;
    query_emb[4] = 0.1;

    // First page: limit 2.
    let params = SemanticSearchParams::builder()
        .query_embedding(query_emb.clone())
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .limit(2)
        .build();

    let page1 = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("page 1");

    assert_eq!(page1.results.len(), 2);
    assert!(page1.next_cursor.is_some());

    // Collect page 1 IDs before moving next_cursor.
    let page1_ids: Vec<KnowledgeItemId> = page1.results.iter().map(|r| r.item.id()).collect();

    // Second page using cursor.
    let params2 = SemanticSearchParams::builder()
        .query_embedding(query_emb)
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .cursor(page1.next_cursor)
        .limit(2)
        .build();

    let page2 = repo
        .semantic_search(&mut txn, &params2)
        .await
        .expect("page 2");

    assert_eq!(page2.results.len(), 2);

    // Pages should return different items.
    for r in &page2.results {
        assert!(
            !page1_ids.contains(&r.item.id()),
            "page 2 should not contain items from page 1"
        );
    }
}

#[tokio::test]
async fn test_semantic_search_no_next_cursor_when_no_more_results() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-no-cursor").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .build();
    let item = repo.insert(&mut txn, &new).await.expect("insert");
    insert_embedding(&mut txn, item.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .limit(10)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 1);
    assert!(response.next_cursor.is_none());
}

#[tokio::test]
async fn test_semantic_search_no_next_cursor_when_total_equals_limit() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-total-eq-limit").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .build();
    let item = repo.insert(&mut txn, &new).await.expect("insert");
    insert_embedding(&mut txn, item.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    // Limit exactly matches the number of items — no more pages exist.
    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .limit(1)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert_eq!(response.results.len(), 1);
    assert!(
        response.next_cursor.is_none(),
        "no more rows exist, cursor should be None"
    );
}

#[tokio::test]
async fn test_semantic_search_invalid_cursor_returns_error() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .cursor(Some("not-a-valid-cursor".to_owned()))
        .limit(10)
        .build();

    let result = repo.semantic_search(&mut txn, &params).await;

    assert!(
        matches!(result, Err(DbError::InvalidCursor { .. })),
        "expected InvalidCursor, got: {result:?}"
    );
}

#[tokio::test]
async fn test_semantic_search_exact_true_when_enough_results() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-exact-true").await;

    for i in 0..3 {
        let new = a_new_knowledge_item()
            .project_id(project_id)
            .principal_id(principal_id)
            .content(format!("exact item {i}"))
            .build();
        let item = repo.insert(&mut txn, &new).await.expect("insert");
        insert_embedding(&mut txn, item.id(), EMBEDDING_MODEL, make_embedding(0)).await;
    }

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .limit(2)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert!(response.exact);
    assert_eq!(response.results.len(), 2);
}

#[tokio::test]
async fn test_semantic_search_exact_false_when_insufficient_results() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgKnowledgeItemRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "ss-exact-false").await;

    let new = a_new_knowledge_item()
        .project_id(project_id)
        .principal_id(principal_id)
        .build();
    let item = repo.insert(&mut txn, &new).await.expect("insert");
    insert_embedding(&mut txn, item.id(), EMBEDDING_MODEL, make_embedding(0)).await;

    let params = SemanticSearchParams::builder()
        .query_embedding(make_embedding(0))
        .embedding_model(EMBEDDING_MODEL.to_owned())
        .limit(5)
        .build();

    let response = repo
        .semantic_search(&mut txn, &params)
        .await
        .expect("search");

    assert!(!response.exact);
    assert_eq!(response.results.len(), 1);
}
