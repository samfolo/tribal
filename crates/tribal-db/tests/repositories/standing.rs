use tribal_db::{
    ItemObservationRepository, KnowledgeItemRepository, PgItemObservationRepository,
    PgKnowledgeItemRepository, PgPrincipalRepository, PgProjectRepository, PgRelationRepository,
    PgStandingRepository, PrincipalRepository, ProjectRepository, RelationRepository,
    StandingRepository,
};
use tribal_domain::{
    EpisodeId, GitRemote, KnowledgeItemId, PrincipalId, ProjectId, RelationBatchId, RelationKind,
};
use tribal_test_utils::{
    TestDb, a_new_item_observation, a_new_knowledge_item, a_new_knowledge_item_relation,
    a_new_principal, a_new_project, commit_relation_batch, shift_relations_timestamp_by_batch,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inserts a principal and project, returning their IDs.
async fn setup_prerequisites(
    txn: &mut sqlx::PgConnection,
    suffix: &str,
) -> (PrincipalId, ProjectId) {
    let principal = PgPrincipalRepository
        .insert(
            txn,
            &a_new_principal()
                .principal_key(format!("user:standing-{suffix}"))
                .build(),
        )
        .await
        .expect("insert principal");

    let project = PgProjectRepository
        .insert_git(
            txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/standing-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project");

    (principal.id(), project.id())
}

/// Inserts a knowledge item and returns its ID.
async fn setup_item(
    txn: &mut sqlx::PgConnection,
    project_id: ProjectId,
    principal_id: PrincipalId,
) -> KnowledgeItemId {
    PgKnowledgeItemRepository
        .insert(
            txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert knowledge item")
        .id()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compute_returns_standings_in_input_order() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "order").await;
    let item_a_id = setup_item(&mut txn, project_id, principal_id).await;
    let item_b_id = setup_item(&mut txn, project_id, principal_id).await;
    let item_c_id = setup_item(&mut txn, project_id, principal_id).await;

    // A supports B (committed).
    let batch_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a_id)
                .target_id(item_b_id)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build()],
        )
        .await
        .expect("batch_insert");
    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    // Add an observation on C.
    PgItemObservationRepository
        .insert(
            &mut txn,
            &a_new_item_observation()
                .knowledge_item_id(item_c_id)
                .principal_id(principal_id)
                .build(),
        )
        .await
        .expect("insert observation");

    let standings = repo
        .compute(&mut txn, &[item_c_id, item_b_id, item_a_id])
        .await
        .expect("compute");

    assert_eq!(standings.len(), 3);
    // C has 1 observation, 0 support.
    assert_eq!(standings[0].observation_count(), 1);
    assert_eq!(standings[0].supporting_count(), 0);
    // B has 1 support.
    assert_eq!(standings[1].supporting_count(), 1);
    // A has nothing.
    assert_eq!(standings[2].supporting_count(), 0);
    assert_eq!(standings[2].observation_count(), 0);
}

#[tokio::test]
async fn test_compute_empty_graph_returns_zero_standings() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "empty-graph").await;
    let item_id = setup_item(&mut txn, project_id, principal_id).await;

    let standings = repo.compute(&mut txn, &[item_id]).await.expect("compute");

    assert_eq!(standings.len(), 1);
    let s = &standings[0];
    assert_eq!(s.supporting_count(), 0);
    assert_eq!(s.contradicting_count(), 0);
    assert!(s.superseded_by().is_none());
    assert_eq!(s.observation_count(), 0);
    assert!(s.newest_supporting_id().is_none());
    assert!(s.newest_contradicting_id().is_none());
    assert_eq!(s.supporting_episode_count(), 0);
    assert_eq!(s.supporting_project_count(), 0);
}

#[tokio::test]
async fn test_compute_excludes_derived_from_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "derived-from").await;
    let source_id = setup_item(&mut txn, project_id, principal_id).await;
    let target_id = setup_item(&mut txn, project_id, principal_id).await;

    let batch_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(source_id)
                .target_id(target_id)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build()],
        )
        .await
        .expect("batch_insert");
    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let standings = repo.compute(&mut txn, &[target_id]).await.expect("compute");

    assert_eq!(standings[0].supporting_count(), 0);
    assert_eq!(standings[0].contradicting_count(), 0);
}

#[tokio::test]
async fn test_compute_excludes_uncommitted_batch_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "uncommitted").await;
    let source_id = setup_item(&mut txn, project_id, principal_id).await;
    let target_id = setup_item(&mut txn, project_id, principal_id).await;

    // Insert but do NOT commit the batch.
    let batch_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(source_id)
                .target_id(target_id)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build()],
        )
        .await
        .expect("batch_insert");

    let standings = repo.compute(&mut txn, &[target_id]).await.expect("compute");

    assert_eq!(standings[0].supporting_count(), 0);
}

#[tokio::test]
async fn test_compute_counts_observations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "obs-count").await;
    let item_id = setup_item(&mut txn, project_id, principal_id).await;

    for _ in 0..3 {
        PgItemObservationRepository
            .insert(
                &mut txn,
                &a_new_item_observation()
                    .knowledge_item_id(item_id)
                    .principal_id(principal_id)
                    .build(),
            )
            .await
            .expect("insert observation");
    }

    let standings = repo.compute(&mut txn, &[item_id]).await.expect("compute");

    assert_eq!(standings[0].observation_count(), 3);
}

#[tokio::test]
async fn test_compute_diversity_metrics() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id_1) = setup_prerequisites(&mut txn, "diversity-1").await;

    // Second project (same principal is reused for all items below).
    let project_id_2 = PgProjectRepository
        .insert_git(
            &mut txn,
            &a_new_project()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    "test/standing-diversity-2",
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project 2")
        .id();

    let target_id = setup_item(&mut txn, project_id_1, principal_id).await;

    // Supporting item from project 1, episode A.
    let episode_alpha_id = EpisodeId::new();
    let supporter_1_id = PgKnowledgeItemRepository
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id_1)
                .principal_id(principal_id)
                .episode_id(Some(episode_alpha_id))
                .build(),
        )
        .await
        .expect("insert supporter 1")
        .id();

    // Supporting item from project 2, episode B.
    let episode_beta_id = EpisodeId::new();
    let supporter_2_id = PgKnowledgeItemRepository
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id_2)
                .principal_id(principal_id)
                .episode_id(Some(episode_beta_id))
                .build(),
        )
        .await
        .expect("insert supporter 2")
        .id();

    // Supporting item from project 1, episode C.
    let episode_gamma_id = EpisodeId::new();
    let supporter_3_id = PgKnowledgeItemRepository
        .insert(
            &mut txn,
            &a_new_knowledge_item()
                .project_id(project_id_1)
                .principal_id(principal_id)
                .episode_id(Some(episode_gamma_id))
                .build(),
        )
        .await
        .expect("insert supporter 3")
        .id();

    let batch_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_id)
                    .source_id(supporter_1_id)
                    .target_id(target_id)
                    .relation_type(RelationKind::Supports)
                    .principal_id(principal_id)
                    .build(),
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_id)
                    .source_id(supporter_2_id)
                    .target_id(target_id)
                    .relation_type(RelationKind::Supports)
                    .principal_id(principal_id)
                    .build(),
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_id)
                    .source_id(supporter_3_id)
                    .target_id(target_id)
                    .relation_type(RelationKind::Supports)
                    .principal_id(principal_id)
                    .build(),
            ],
        )
        .await
        .expect("batch_insert");
    commit_relation_batch(&mut txn, project_id_1, principal_id, batch_id).await;

    let standings = repo.compute(&mut txn, &[target_id]).await.expect("compute");

    assert_eq!(standings[0].supporting_count(), 3);
    assert_eq!(standings[0].supporting_episode_count(), 3);
    assert_eq!(standings[0].supporting_project_count(), 2);
}

#[tokio::test]
async fn test_compute_empty_slice_returns_empty_vec() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let standings = repo.compute(&mut txn, &[]).await.expect("compute");

    assert!(standings.is_empty());
}

#[tokio::test]
async fn test_compute_unknown_ids_return_zero_standings() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let unknown_id = KnowledgeItemId::new();
    let standings = repo
        .compute(&mut txn, &[unknown_id])
        .await
        .expect("compute");

    assert_eq!(standings.len(), 1);
    let s = &standings[0];
    assert_eq!(s.supporting_count(), 0);
    assert_eq!(s.contradicting_count(), 0);
    assert!(s.superseded_by().is_none());
    assert_eq!(s.observation_count(), 0);
    assert!(s.newest_supporting_id().is_none());
    assert!(s.newest_contradicting_id().is_none());
    assert_eq!(s.supporting_episode_count(), 0);
    assert_eq!(s.supporting_project_count(), 0);
}

#[tokio::test]
async fn test_compute_superseded_by() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "superseded").await;
    let old_item_id = setup_item(&mut txn, project_id, principal_id).await;
    let new_item_id = setup_item(&mut txn, project_id, principal_id).await;

    let batch_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(new_item_id)
                .target_id(old_item_id)
                .relation_type(RelationKind::Supersedes)
                .principal_id(principal_id)
                .build()],
        )
        .await
        .expect("batch_insert");
    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let standings = repo
        .compute(&mut txn, &[old_item_id])
        .await
        .expect("compute");

    assert_eq!(standings[0].superseded_by(), Some(new_item_id));
}

#[tokio::test]
async fn test_compute_newest_supporting_and_contradicting() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "newest-ids").await;
    let target_id = setup_item(&mut txn, project_id, principal_id).await;
    let older_supporter_id = setup_item(&mut txn, project_id, principal_id).await;
    let newer_supporter_id = setup_item(&mut txn, project_id, principal_id).await;
    let older_contradictor_id = setup_item(&mut txn, project_id, principal_id).await;
    let newer_contradictor_id = setup_item(&mut txn, project_id, principal_id).await;

    // First batch — older relations.
    let batch_1_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_1_id)
                    .source_id(older_supporter_id)
                    .target_id(target_id)
                    .relation_type(RelationKind::Supports)
                    .principal_id(principal_id)
                    .build(),
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_1_id)
                    .source_id(older_contradictor_id)
                    .target_id(target_id)
                    .relation_type(RelationKind::Contradicts)
                    .principal_id(principal_id)
                    .build(),
            ],
        )
        .await
        .expect("batch_insert 1");
    commit_relation_batch(&mut txn, project_id, principal_id, batch_1_id).await;

    // Backdate the older relations so they are strictly older.
    shift_relations_timestamp_by_batch(&mut txn, batch_1_id, chrono::Duration::hours(-1)).await;

    // Second batch — newer relations.
    let batch_2_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_2_id)
                    .source_id(newer_supporter_id)
                    .target_id(target_id)
                    .relation_type(RelationKind::Supports)
                    .principal_id(principal_id)
                    .build(),
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_2_id)
                    .source_id(newer_contradictor_id)
                    .target_id(target_id)
                    .relation_type(RelationKind::Contradicts)
                    .principal_id(principal_id)
                    .build(),
            ],
        )
        .await
        .expect("batch_insert 2");
    commit_relation_batch(&mut txn, project_id, principal_id, batch_2_id).await;

    let standings = repo.compute(&mut txn, &[target_id]).await.expect("compute");

    assert_eq!(
        standings[0].newest_supporting_id(),
        Some(newer_supporter_id)
    );
    assert_eq!(
        standings[0].newest_contradicting_id(),
        Some(newer_contradictor_id)
    );
}

#[tokio::test]
async fn test_compute_counts_distinct_source_items() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgStandingRepository;

    let (principal_id, project_id) = setup_prerequisites(&mut txn, "distinct-count").await;
    let target_id = setup_item(&mut txn, project_id, principal_id).await;
    let source_id = setup_item(&mut txn, project_id, principal_id).await;

    // Two relation rows from the same source to the same target.
    let batch_1_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[a_new_knowledge_item_relation()
                .relation_batch_id(batch_1_id)
                .source_id(source_id)
                .target_id(target_id)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build()],
        )
        .await
        .expect("batch_insert 1");
    commit_relation_batch(&mut txn, project_id, principal_id, batch_1_id).await;

    let batch_2_id = RelationBatchId::new();
    PgRelationRepository
        .batch_insert(
            &mut txn,
            &[a_new_knowledge_item_relation()
                .relation_batch_id(batch_2_id)
                .source_id(source_id)
                .target_id(target_id)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build()],
        )
        .await
        .expect("batch_insert 2");
    commit_relation_batch(&mut txn, project_id, principal_id, batch_2_id).await;

    let standings = repo.compute(&mut txn, &[target_id]).await.expect("compute");

    assert_eq!(standings[0].supporting_count(), 1);
}
