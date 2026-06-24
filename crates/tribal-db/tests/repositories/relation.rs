use tribal_db::{
    KnowledgeItemRepository, PgKnowledgeItemRepository, PgPrincipalRepository, PgProjectRepository,
    PgRelationRepository, PrincipalRepository, ProjectRepository, RelationRepository,
    TraversalDirection,
};
use tribal_domain::{
    Direction, GitRemote, KnowledgeItemId, PrincipalId, ProjectId, RelationBatchId, RelationKind,
};
use tribal_test_utils::{
    TestDb, a_new_knowledge_item, a_new_knowledge_item_relation, a_new_principal, a_new_project,
    commit_relation_batch,
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
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/{suffix}"),
                    None,
                ))
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

/// Inserts a single additional knowledge item.
async fn setup_item(
    txn: &mut sqlx::PgConnection,
    project_id: ProjectId,
    principal_id: PrincipalId,
    content: &str,
) -> KnowledgeItemId {
    PgKnowledgeItemRepository
        .insert(
            txn,
            &a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal_id)
                .content(content.to_owned())
                .build(),
        )
        .await
        .expect("insert knowledge item")
        .id()
}

// ---------------------------------------------------------------------------
// batch_insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_batch_insert_returns_populated_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, item_a) = setup_prerequisites(&mut txn, "rel-batch-pop").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;

    let batch_id = RelationBatchId::new();
    let results = repo
        .batch_insert(
            &mut txn,
            &[
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_id)
                    .source_id(item_a)
                    .target_id(item_b)
                    .relation_type(RelationKind::Supports)
                    .principal_id(principal_id)
                    .build(),
                a_new_knowledge_item_relation()
                    .relation_batch_id(batch_id)
                    .source_id(item_b)
                    .target_id(item_c)
                    .relation_type(RelationKind::DerivedFrom)
                    .principal_id(principal_id)
                    .build(),
            ],
        )
        .await
        .expect("batch_insert");

    assert_eq!(results.len(), 2);

    let r0 = &results[0];
    assert!(
        r0.id().to_string().starts_with("rel_"),
        "expected rel_ prefix, got: {}",
        r0.id()
    );
    assert_eq!(r0.relation_batch_id(), batch_id);
    assert_eq!(r0.source_id(), item_a);
    assert_eq!(r0.target_id(), item_b);
    assert_eq!(r0.relation_type(), RelationKind::Supports);
    assert_eq!(r0.principal_id(), principal_id);

    let r1 = &results[1];
    assert!(
        r1.id().to_string().starts_with("rel_"),
        "expected rel_ prefix, got: {}",
        r1.id()
    );
    assert_eq!(r1.relation_batch_id(), batch_id);
    assert_eq!(r1.source_id(), item_b);
    assert_eq!(r1.target_id(), item_c);
    assert_eq!(r1.relation_type(), RelationKind::DerivedFrom);
    assert_eq!(r1.principal_id(), principal_id);
}

#[tokio::test]
async fn test_batch_insert_empty_batch_returns_empty_vec() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let results = repo
        .batch_insert(&mut txn, &[])
        .await
        .expect("batch_insert");
    assert!(results.is_empty());
}

// ---------------------------------------------------------------------------
// find_inbound
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_inbound_returns_committed_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-in-committed").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            // Inbound to B: A → B.
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(item_b)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            // Outbound from B (noise): B → C — should NOT appear in inbound query for B.
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(item_c)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let found = repo
        .find_inbound(&mut txn, item_b, None)
        .await
        .expect("find_inbound");

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].source_id(), item_a);
    assert_eq!(found[0].target_id(), item_b);
    assert_eq!(found[0].relation_type(), RelationKind::Supports);
}

#[tokio::test]
async fn test_find_inbound_with_type_filter() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, anchor) = setup_prerequisites(&mut txn, "rel-in-filter").await;
    let item_a = setup_item(&mut txn, project_id, principal_id, "item A").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(anchor)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(anchor)
                .relation_type(RelationKind::Contradicts)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let found = repo
        .find_inbound(&mut txn, anchor, Some(&[RelationKind::Supports]))
        .await
        .expect("find_inbound");

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].relation_type(), RelationKind::Supports);
}

#[tokio::test]
async fn test_find_inbound_returns_empty_when_no_committed_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-in-uncommitted").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;

    // Insert but do NOT commit the batch.
    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[a_new_knowledge_item_relation()
            .relation_batch_id(batch_id)
            .source_id(item_a)
            .target_id(item_b)
            .principal_id(principal_id)
            .build()],
    )
    .await
    .expect("batch_insert");

    let found = repo
        .find_inbound(&mut txn, item_b, None)
        .await
        .expect("find_inbound");

    assert!(found.is_empty());
}

// ---------------------------------------------------------------------------
// find_outbound
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_find_outbound_returns_committed_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-out-committed").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            // Outbound from A: A → B.
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(item_b)
                .relation_type(RelationKind::Contradicts)
                .principal_id(principal_id)
                .build(),
            // Inbound to A (noise): C → A — should NOT appear in outbound query for A.
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_c)
                .target_id(item_a)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let found = repo
        .find_outbound(&mut txn, item_a, None)
        .await
        .expect("find_outbound");

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].source_id(), item_a);
    assert_eq!(found[0].target_id(), item_b);
    assert_eq!(found[0].relation_type(), RelationKind::Contradicts);
}

#[tokio::test]
async fn test_find_outbound_with_type_filter() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, anchor) = setup_prerequisites(&mut txn, "rel-out-filter").await;
    let item_a = setup_item(&mut txn, project_id, principal_id, "item A").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(anchor)
                .target_id(item_a)
                .relation_type(RelationKind::Supersedes)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(anchor)
                .target_id(item_b)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let found = repo
        .find_outbound(&mut txn, anchor, Some(&[RelationKind::DerivedFrom]))
        .await
        .expect("find_outbound");

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].relation_type(), RelationKind::DerivedFrom);
}

#[tokio::test]
async fn test_find_outbound_returns_empty_when_no_committed_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-out-uncommitted").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[a_new_knowledge_item_relation()
            .relation_batch_id(batch_id)
            .source_id(item_a)
            .target_id(item_b)
            .principal_id(principal_id)
            .build()],
    )
    .await
    .expect("batch_insert");

    let found = repo
        .find_outbound(&mut txn, item_a, None)
        .await
        .expect("find_outbound");

    assert!(found.is_empty());
}

// ---------------------------------------------------------------------------
// traverse — uncommitted exclusion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_traverse_returns_empty_when_no_committed_relations() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-trav-uncommitted").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;

    // Insert but do NOT commit the batch.
    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[a_new_knowledge_item_relation()
            .relation_batch_id(batch_id)
            .source_id(item_a)
            .target_id(item_b)
            .principal_id(principal_id)
            .build()],
    )
    .await
    .expect("batch_insert");

    let response = repo
        .traverse(&mut txn, item_b, Direction::Inbound, 2, 10, None)
        .await
        .expect("traverse");

    assert!(response.nodes.is_empty());
    assert!(response.exact);
}

// ---------------------------------------------------------------------------
// traverse — inbound
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_traverse_inbound_multi_depth() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    // Graph: C → B → A (inbound from A's perspective).
    // Noise: A → D (outbound from A — should NOT appear in inbound traversal).
    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-trav-in-multi").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;
    let item_d = setup_item(&mut txn, project_id, principal_id, "item D (noise)").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(item_a)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_c)
                .target_id(item_b)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(item_d)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(&mut txn, item_a, Direction::Inbound, 2, 10, None)
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 2);
    assert!(response.exact);

    // Depth 1: B → A (discovered item is B).
    assert_eq!(response.nodes[0].item.id(), item_b);
    assert_eq!(response.nodes[0].depth, 1);

    // Depth 2: C → B (discovered item is C).
    assert_eq!(response.nodes[1].item.id(), item_c);
    assert_eq!(response.nodes[1].depth, 2);
}

// ---------------------------------------------------------------------------
// traverse — outbound
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_traverse_outbound_multi_depth() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    // Graph: A → B → C (outbound from A's perspective).
    // Noise: D → A (inbound to A — should NOT appear in outbound traversal).
    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-trav-out-multi").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;
    let item_d = setup_item(&mut txn, project_id, principal_id, "item D (noise)").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(item_b)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(item_c)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_d)
                .target_id(item_a)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(&mut txn, item_a, Direction::Outbound, 2, 10, None)
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 2);
    assert!(response.exact);

    // Depth 1: A → B (discovered item is B).
    assert_eq!(response.nodes[0].item.id(), item_b);
    assert_eq!(response.nodes[0].depth, 1);

    // Depth 2: B → C (discovered item is C).
    assert_eq!(response.nodes[1].item.id(), item_c);
    assert_eq!(response.nodes[1].depth, 2);
}

// ---------------------------------------------------------------------------
// traverse — both directions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_traverse_both_merges_directions_multi_depth() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    // Graph: W → X → A → Y → Z (A is anchor; X,W inbound; Y,Z outbound).
    let (principal_id, project_id, item_a) = setup_prerequisites(&mut txn, "rel-trav-both").await;
    let item_w = setup_item(&mut txn, project_id, principal_id, "item W").await;
    let item_x = setup_item(&mut txn, project_id, principal_id, "item X").await;
    let item_y = setup_item(&mut txn, project_id, principal_id, "item Y").await;
    let item_z = setup_item(&mut txn, project_id, principal_id, "item Z").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_w)
                .target_id(item_x)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_x)
                .target_id(item_a)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(item_y)
                .relation_type(RelationKind::Contradicts)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_y)
                .target_id(item_z)
                .relation_type(RelationKind::Contradicts)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(&mut txn, item_a, Direction::Both, 2, 10, None)
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 4);
    assert!(response.exact);

    // All four items discovered at depths 1 and 2.
    let item_ids: Vec<KnowledgeItemId> = response.nodes.iter().map(|n| n.item.id()).collect();
    assert!(item_ids.contains(&item_x));
    assert!(item_ids.contains(&item_y));
    assert!(item_ids.contains(&item_w));
    assert!(item_ids.contains(&item_z));

    // Depth 1 items first, depth 2 items second.
    let depth_1: Vec<KnowledgeItemId> = response
        .nodes
        .iter()
        .filter(|n| n.depth == 1)
        .map(|n| n.item.id())
        .collect();
    assert_eq!(depth_1.len(), 2);
    assert!(depth_1.contains(&item_x));
    assert!(depth_1.contains(&item_y));
}

#[tokio::test]
async fn test_traverse_both_tags_directions_at_multiple_depths() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    // Graph: C → B → A → D → E (A is anchor).
    // Inbound chain: B at depth 1, C at depth 2.
    // Outbound chain: D at depth 1, E at depth 2.
    let (principal_id, project_id, item_a) =
        setup_prerequisites(&mut txn, "rel-trav-both-dir").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;
    let item_d = setup_item(&mut txn, project_id, principal_id, "item D").await;
    let item_e = setup_item(&mut txn, project_id, principal_id, "item E").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(item_a)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_c)
                .target_id(item_b)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(item_d)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_d)
                .target_id(item_e)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(&mut txn, item_a, Direction::Both, 2, 10, None)
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 4);

    // Depth-1 inbound: B asserts something about A.
    let node_b = response
        .nodes
        .iter()
        .find(|n| n.item.id() == item_b)
        .expect("B present");
    assert_eq!(node_b.depth, 1);
    assert_eq!(node_b.traversal_direction, TraversalDirection::Inbound);

    // Depth-2 inbound: C asserts something about B.
    let node_c = response
        .nodes
        .iter()
        .find(|n| n.item.id() == item_c)
        .expect("C present");
    assert_eq!(node_c.depth, 2);
    assert_eq!(node_c.traversal_direction, TraversalDirection::Inbound);

    // Depth-1 outbound: A asserts something about D.
    let node_d = response
        .nodes
        .iter()
        .find(|n| n.item.id() == item_d)
        .expect("D present");
    assert_eq!(node_d.depth, 1);
    assert_eq!(node_d.traversal_direction, TraversalDirection::Outbound);

    // Depth-2 outbound: D asserts something about E.
    let node_e = response
        .nodes
        .iter()
        .find(|n| n.item.id() == item_e)
        .expect("E present");
    assert_eq!(node_e.depth, 2);
    assert_eq!(node_e.traversal_direction, TraversalDirection::Outbound);
}

#[tokio::test]
async fn test_traverse_cycle_terminates() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    // Cycle: A → B → C → D → A.
    let (principal_id, project_id, item_a) = setup_prerequisites(&mut txn, "rel-trav-cycle").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;
    let item_d = setup_item(&mut txn, project_id, principal_id, "item D").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(item_b)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(item_c)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_c)
                .target_id(item_d)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_d)
                .target_id(item_a)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    // Outbound from A with generous depth — should not loop forever.
    let response = repo
        .traverse(&mut txn, item_a, Direction::Outbound, 10, 100, None)
        .await
        .expect("traverse");

    // B at depth 1, C at depth 2, D at depth 3. The D→A edge is not
    // followed because A is the anchor (already visited).
    assert_eq!(response.nodes.len(), 3);
    assert!(response.exact);

    assert_eq!(response.nodes[0].item.id(), item_b);
    assert_eq!(response.nodes[0].depth, 1);
    assert_eq!(response.nodes[1].item.id(), item_c);
    assert_eq!(response.nodes[1].depth, 2);
    assert_eq!(response.nodes[2].item.id(), item_d);
    assert_eq!(response.nodes[2].depth, 3);
}

// ---------------------------------------------------------------------------
// traverse — depth limit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_traverse_respects_depth_limit() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    // Graph: C → B → A. Traverse inbound from A with depth=1.
    let (principal_id, project_id, item_a) = setup_prerequisites(&mut txn, "rel-trav-depth").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(item_a)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_c)
                .target_id(item_b)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(&mut txn, item_a, Direction::Inbound, 1, 10, None)
        .await
        .expect("traverse");

    // Only B is returned (depth 1); C is at depth 2 and excluded.
    assert_eq!(response.nodes.len(), 1);
    assert_eq!(response.nodes[0].item.id(), item_b);
    assert_eq!(response.nodes[0].depth, 1);
    assert!(response.exact);
}

// ---------------------------------------------------------------------------
// traverse — exact flag and limit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_traverse_sets_exact_false_when_limit_reached() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    // Create 4 items that all point inbound to the anchor.
    let (principal_id, project_id, anchor) =
        setup_prerequisites(&mut txn, "rel-trav-exact-false").await;

    let batch_id = RelationBatchId::new();
    let mut relations = Vec::new();
    for i in 0..4 {
        let item = setup_item(&mut txn, project_id, principal_id, &format!("item {i}")).await;
        relations.push(
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item)
                .target_id(anchor)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
        );
    }

    repo.batch_insert(&mut txn, &relations)
        .await
        .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(&mut txn, anchor, Direction::Inbound, 1, 2, None)
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 2);
    assert!(!response.exact);
}

#[tokio::test]
async fn test_traverse_sets_exact_true_when_all_returned() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, anchor) =
        setup_prerequisites(&mut txn, "rel-trav-exact-true").await;
    let item_a = setup_item(&mut txn, project_id, principal_id, "item A").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[a_new_knowledge_item_relation()
            .relation_batch_id(batch_id)
            .source_id(item_a)
            .target_id(anchor)
            .relation_type(RelationKind::Supports)
            .principal_id(principal_id)
            .build()],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(&mut txn, anchor, Direction::Inbound, 1, 10, None)
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 1);
    assert!(response.exact);
}

// ---------------------------------------------------------------------------
// traverse — relation type filter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_traverse_with_type_filter() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, anchor) =
        setup_prerequisites(&mut txn, "rel-trav-type-filter").await;
    let item_a = setup_item(&mut txn, project_id, principal_id, "item A").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(anchor)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(anchor)
                .relation_type(RelationKind::Contradicts)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    let response = repo
        .traverse(
            &mut txn,
            anchor,
            Direction::Inbound,
            1,
            10,
            Some(&[RelationKind::Contradicts]),
        )
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 1);
    assert_eq!(response.nodes[0].item.id(), item_b);
    assert_eq!(response.nodes[0].relation_type, RelationKind::Contradicts);
}

#[tokio::test]
async fn test_traverse_with_multiple_type_filter_uses_or() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgRelationRepository;

    let (principal_id, project_id, anchor) =
        setup_prerequisites(&mut txn, "rel-trav-multi-filter").await;
    let item_a = setup_item(&mut txn, project_id, principal_id, "item A").await;
    let item_b = setup_item(&mut txn, project_id, principal_id, "item B").await;
    let item_c = setup_item(&mut txn, project_id, principal_id, "item C").await;

    let batch_id = RelationBatchId::new();
    repo.batch_insert(
        &mut txn,
        &[
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_a)
                .target_id(anchor)
                .relation_type(RelationKind::Supports)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_b)
                .target_id(anchor)
                .relation_type(RelationKind::Contradicts)
                .principal_id(principal_id)
                .build(),
            a_new_knowledge_item_relation()
                .relation_batch_id(batch_id)
                .source_id(item_c)
                .target_id(anchor)
                .relation_type(RelationKind::DerivedFrom)
                .principal_id(principal_id)
                .build(),
        ],
    )
    .await
    .expect("batch_insert");

    commit_relation_batch(&mut txn, project_id, principal_id, batch_id).await;

    // Filter to Supports OR Contradicts — DerivedFrom should be excluded.
    let response = repo
        .traverse(
            &mut txn,
            anchor,
            Direction::Inbound,
            1,
            10,
            Some(&[RelationKind::Supports, RelationKind::Contradicts]),
        )
        .await
        .expect("traverse");

    assert_eq!(response.nodes.len(), 2);
    let types: Vec<RelationKind> = response.nodes.iter().map(|n| n.relation_type).collect();
    assert!(types.contains(&RelationKind::Supports));
    assert!(types.contains(&RelationKind::Contradicts));
}
