//! Integration tests for the declarative seed infrastructure.
//!
//! Each test starts a pgvector container via [`TestContext`] (shared per
//! binary), opens a transaction that rolls back on drop, and exercises
//! the [`Seed`] builder against the real database through the repository
//! layer.

use chrono::Duration;
use tribal_db::{
    EmbeddingRepository, ItemObservationRepository, JobRepository, KnowledgeItemRepository,
    PgEmbeddingRepository, PgItemObservationRepository, PgJobRepository, PgKnowledgeItemRepository,
    PgReferenceRepository, PgRelationRepository, PgTagRegistryRepository, ReferenceRepository,
    RelationRepository, TagRegistryRepository,
};
use tribal_domain::{
    KnowledgeKind::{Fact, Heuristic},
    ReferenceKind::{FilePath, Url},
    RelationKind::{Supersedes, Supports},
    SourceType,
};
use tribal_test_utils::{Seed, item, scenarios, test_context};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    dot / (norm_a * norm_b)
}

// ---------------------------------------------------------------------------
// Core functionality
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_basic_seed_inserts_project_and_principal() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("item-1", item(Fact, "test content").skip_embed());
        })
        .execute(&mut txn)
        .await;

    let proj_id = result.project_id("proj");
    let principal_id = result.principal_id("user");
    let ki_id = result.item_id("item-1");

    // Verify IDs are valid by fetching from the database.
    let item = PgKnowledgeItemRepository
        .find_by_id(&mut txn, ki_id)
        .await
        .expect("item should exist");
    assert_eq!(item.id(), ki_id);
    assert_eq!(item.content(), "test content");

    // project_id and principal_id are non-zero UUIDs.
    assert_ne!(*proj_id.inner(), uuid::Uuid::nil());
    assert_ne!(*principal_id.inner(), uuid::Uuid::nil());
}

#[tokio::test]
async fn test_seed_with_items_and_embeddings() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").embedding_group("grp"))
                .add_item("b", item(Heuristic, "beta").embedding_group("grp"));
        })
        .execute(&mut txn)
        .await;

    // Both items should have embeddings.
    let emb_a = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("a"), "test-model")
        .await
        .expect("query")
        .expect("embedding for 'a' should exist");

    let emb_b = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("b"), "test-model")
        .await
        .expect("query")
        .expect("embedding for 'b' should exist");

    assert_eq!(emb_a.dimensions(), 768);
    assert_eq!(emb_b.dimensions(), 768);
    assert_eq!(emb_a.embedding().len(), 768);
}

#[tokio::test]
async fn test_seed_with_tags_auto_registers_in_registry() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item(
                "a",
                item(Fact, "tagged item")
                    .tags(&["alpha", "beta"])
                    .skip_embed(),
            );
        })
        .execute(&mut txn)
        .await;

    let all_tags = PgTagRegistryRepository
        .find_all(&mut txn)
        .await
        .expect("find_all tags");

    let tag_names: Vec<&str> = all_tags.iter().map(|t| t.tag()).collect();
    assert!(tag_names.contains(&"alpha"), "alpha should be registered");
    assert!(tag_names.contains(&"beta"), "beta should be registered");
}

#[tokio::test]
async fn test_seed_virtual_clock_backdating() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("early", item(Fact, "early item").skip_embed());
        })
        .advance(Duration::hours(6))
        .for_project("proj", |store| {
            store.add_item("late", item(Fact, "late item").skip_embed());
        })
        .execute(&mut txn)
        .await;

    let early = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("early"))
        .await
        .expect("find early");
    let late = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("late"))
        .await
        .expect("find late");

    assert!(
        late.created_at() > early.created_at(),
        "late item ({:?}) should have a later timestamp than early item ({:?})",
        late.created_at(),
        early.created_at()
    );
}

#[tokio::test]
async fn test_seed_commit_relations_creates_scaffolding() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").skip_embed())
                .add_item("b", item(Fact, "beta").skip_embed());
        })
        .relate("a", Supports, "b")
        .commit_relations("batch-1")
        .execute(&mut txn)
        .await;

    let job_id = result.batch_job_id("batch-1");
    let job = PgJobRepository
        .find_by_id(&mut txn, job_id)
        .await
        .expect("job should exist");

    assert!(
        job.committed_batch_id().is_some(),
        "job should have a committed_batch_id"
    );

    let relation_ids = result.relation_ids("batch-1");
    assert_eq!(relation_ids.len(), 1);
}

#[tokio::test]
async fn test_seed_uncommitted_relations_no_scaffolding() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").skip_embed())
                .add_item("b", item(Fact, "beta").skip_embed());
        })
        .relate("a", Supports, "b")
        .execute(&mut txn)
        .await;

    assert_eq!(result.uncommitted_relation_ids().len(), 1);
    assert_eq!(result.batch_count(), 0);
}

#[tokio::test]
async fn test_seed_observations_backdated() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").skip_embed())
                .observe("a", SourceType::AgentMediated);
        })
        .execute(&mut txn)
        .await;

    let obs_ids = result.observation_ids("a");
    assert_eq!(obs_ids.len(), 1);

    let observations = PgItemObservationRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("a"))
        .await
        .expect("find observations");
    assert_eq!(observations.len(), 1);

    let item = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("a"))
        .await
        .expect("find item");

    // Observation and item should share the same virtual time.
    assert_eq!(observations[0].observed_at(), item.created_at());
}

#[tokio::test]
async fn test_seed_references_attached_to_items() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").skip_embed())
                .add_reference("a", FilePath, "//src/main.rs")
                .add_reference("a", Url, "https://example.com");
        })
        .execute(&mut txn)
        .await;

    let ref_ids = result.reference_ids("a");
    assert_eq!(ref_ids.len(), 2);

    let refs = PgReferenceRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("a"))
        .await
        .expect("find references");
    assert_eq!(refs.len(), 2);
}

#[tokio::test]
async fn test_seed_embedding_groups_produce_similar_vectors() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").embedding_group("same"))
                .add_item("b", item(Fact, "beta").embedding_group("same"))
                .add_item("c", item(Fact, "gamma").embedding_group("different"));
        })
        .execute(&mut txn)
        .await;

    let emb_a = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("a"), "test-model")
        .await
        .expect("query")
        .expect("embedding a");
    let emb_b = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("b"), "test-model")
        .await
        .expect("query")
        .expect("embedding b");
    let emb_c = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("c"), "test-model")
        .await
        .expect("query")
        .expect("embedding c");

    let sim_ab = cosine_similarity(emb_a.embedding(), emb_b.embedding());
    let sim_ac = cosine_similarity(emb_a.embedding(), emb_c.embedding());

    assert!(
        sim_ab > sim_ac,
        "same-group similarity ({sim_ab}) should exceed cross-group ({sim_ac})"
    );
}

#[tokio::test]
async fn test_seed_skip_embed_item_has_no_embedding() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("skipped", item(Fact, "no vector").skip_embed());
        })
        .execute(&mut txn)
        .await;

    let emb = PgEmbeddingRepository
        .find_by_knowledge_item_id(&mut txn, result.item_id("skipped"), "test-model")
        .await
        .expect("query");

    assert!(emb.is_none(), "skip_embed item should have no embedding");
}

#[tokio::test]
async fn test_seed_episode_label_shares_episode_id() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").episode("ep-1").skip_embed())
                .add_item("b", item(Fact, "beta").episode("ep-1").skip_embed())
                .add_item("c", item(Fact, "gamma").episode("ep-2").skip_embed());
        })
        .execute(&mut txn)
        .await;

    let item_a = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("a"))
        .await
        .expect("find a");
    let item_b = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("b"))
        .await
        .expect("find b");
    let item_c = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("c"))
        .await
        .expect("find c");

    assert_eq!(
        item_a.episode_id(),
        item_b.episode_id(),
        "same episode label should produce same episode_id"
    );
    assert_ne!(
        item_a.episode_id(),
        item_c.episode_id(),
        "different episode labels should produce different episode_ids"
    );
}

#[tokio::test]
async fn test_seed_cross_project_relations() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj-a", "git@example.com:a.git")
        .define_project("proj-b", "git@example.com:b.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj-a", |store| {
            store.add_item("item-a", item(Fact, "from project A").skip_embed());
        })
        .for_project("proj-b", |store| {
            store.add_item("item-b", item(Fact, "from project B").skip_embed());
        })
        .relate("item-a", Supports, "item-b")
        .commit_relations("cross-proj")
        .execute(&mut txn)
        .await;

    assert_eq!(result.relation_ids("cross-proj").len(), 1);
}

#[tokio::test]
async fn test_seed_cross_project_observations() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj-a", "git@example.com:a.git")
        .define_project("proj-b", "git@example.com:b.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj-a", |store| {
            store.add_item("item-a", item(Fact, "from project A").skip_embed());
        })
        .for_project("proj-b", |store| {
            // Observe an item from project A within project B's scope.
            store.observe("item-a", SourceType::ManualCapture);
        })
        .execute(&mut txn)
        .await;

    let obs = result.observation_ids("item-a");
    assert_eq!(obs.len(), 1, "cross-project observation should be recorded");
}

#[tokio::test]
async fn test_seed_as_principal_within_scope() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("alice", "user:alice")
        .define_principal("bob", "user:bob")
        .set_embedding_model("test-model", 768)
        .as_principal("alice")
        .for_project("proj", |store| {
            store
                .add_item("by-alice", item(Fact, "alice's item").skip_embed())
                .as_principal("bob")
                .add_item("by-bob", item(Fact, "bob's item").skip_embed());
        })
        .execute(&mut txn)
        .await;

    let alice_item = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("by-alice"))
        .await
        .expect("find alice item");
    let bob_item = PgKnowledgeItemRepository
        .find_by_id(&mut txn, result.item_id("by-bob"))
        .await
        .expect("find bob item");

    assert_eq!(alice_item.principal_id(), result.principal_id("alice"));
    assert_eq!(bob_item.principal_id(), result.principal_id("bob"));
}

#[tokio::test]
async fn test_seed_forward_reference_within_scope() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    // observe and add_reference declared BEFORE add_item — should still
    // work because of the two-bucket partition.
    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .observe("x", SourceType::AgentMediated)
                .add_reference("x", FilePath, "//src/lib.rs")
                .add_item("x", item(Fact, "forward-referenced item").skip_embed());
        })
        .execute(&mut txn)
        .await;

    assert_eq!(result.observation_ids("x").len(), 1);
    assert_eq!(result.reference_ids("x").len(), 1);
}

// ---------------------------------------------------------------------------
// Pre-built scenarios
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_basic_knowledge_graph_scenario() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = scenarios::basic_knowledge_graph().execute(&mut txn).await;

    // 1 project, 1 principal.
    let _proj_id = result.project_id("tribal");
    let _principal_id = result.principal_id("sam");

    // 5 items.
    let labels = result.item_labels();
    assert_eq!(labels.len(), 5);

    // 5 embeddings (one per item).
    for label in &labels {
        let _ = result.embedding_id(label);
    }

    // 5 references (one per item).
    let total_refs: usize = labels.iter().map(|l| result.reference_ids(l).len()).sum();
    assert_eq!(total_refs, 5);

    // 3 relations in 1 committed batch.
    assert_eq!(result.batch_count(), 1);
    assert_eq!(result.relation_ids("initial-relations").len(), 3);
}

#[tokio::test]
async fn test_supersession_scenario() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = scenarios::supersession_scenario().execute(&mut txn).await;

    // 1 project, 1 principal, 5 items, 5 embeddings.
    let _proj_id = result.project_id("tribal");
    let _principal_id = result.principal_id("sam");
    assert_eq!(result.item_labels().len(), 5);

    // 3 committed batches.
    assert_eq!(result.batch_count(), 3);

    // original-support: 1 relation
    assert_eq!(result.relation_ids("original-support").len(), 1);
    // contradiction: 1 relation
    assert_eq!(result.relation_ids("contradiction").len(), 1);
    // supersession: 3 relations
    assert_eq!(result.relation_ids("supersession").len(), 3);

    // Total: 5 relations.
    let total = result.relation_ids("original-support").len()
        + result.relation_ids("contradiction").len()
        + result.relation_ids("supersession").len();
    assert_eq!(total, 5);

    // Verify 2 superseded items: original-fact and original-heuristic have
    // inbound Supersedes relations.
    let original_fact_id = result.item_id("original-fact");
    let original_heuristic_id = result.item_id("original-heuristic");

    let inbound_fact = PgRelationRepository
        .find_inbound(&mut txn, original_fact_id, Some(&[Supersedes]))
        .await
        .expect("find inbound supersedes for original-fact");
    assert_eq!(
        inbound_fact.len(),
        1,
        "original-fact should have 1 inbound Supersedes relation"
    );

    let inbound_heuristic = PgRelationRepository
        .find_inbound(&mut txn, original_heuristic_id, Some(&[Supersedes]))
        .await
        .expect("find inbound supersedes for original-heuristic");
    assert_eq!(
        inbound_heuristic.len(),
        1,
        "original-heuristic should have 1 inbound Supersedes relation"
    );
}

// ---------------------------------------------------------------------------
// Composability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_seed_composability() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = scenarios::basic_knowledge_graph()
        .for_project("tribal", |store| {
            store.add_item(
                "extra-item",
                item(Fact, "extended item").embedding_group("caching"),
            );
        })
        .relate("extra-item", Supports, "heuristic-1")
        .commit_relations("extended-batch")
        .execute(&mut txn)
        .await;

    // Original 5 + 1 new = 6 items.
    assert_eq!(result.item_labels().len(), 6);

    // Original batch + new batch = 2 committed batches.
    assert_eq!(result.batch_count(), 2);
    assert_eq!(result.relation_ids("extended-batch").len(), 1);
}

// ---------------------------------------------------------------------------
// Validation panics
// ---------------------------------------------------------------------------

#[tokio::test]
#[should_panic(expected = "duplicate project label")]
async fn test_duplicate_project_label_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:a.git")
        .define_project("proj", "git@example.com:b.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "duplicate principal label")]
async fn test_duplicate_principal_label_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:a")
        .define_principal("user", "user:b")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "duplicate item label")]
async fn test_duplicate_item_label_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("x", item(Fact, "first").skip_embed())
                .add_item("x", item(Fact, "second").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "duplicate batch label")]
async fn test_duplicate_batch_label_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store
                .add_item("a", item(Fact, "alpha").skip_embed())
                .add_item("b", item(Fact, "beta").skip_embed())
                .add_item("c", item(Fact, "gamma").skip_embed());
        })
        .relate("a", Supports, "b")
        .commit_relations("batch")
        .relate("b", Supports, "c")
        .commit_relations("batch")
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "relation source")]
async fn test_unknown_relate_source_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("a", item(Fact, "alpha").skip_embed());
        })
        .relate("unknown", Supports, "a")
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "relation target")]
async fn test_unknown_relate_target_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("a", item(Fact, "alpha").skip_embed());
        })
        .relate("a", Supports, "unknown")
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "observe target")]
async fn test_unknown_observe_target_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.observe("unknown", SourceType::AgentMediated);
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "reference target")]
async fn test_unknown_reference_target_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_reference("unknown", FilePath, "//src/lib.rs");
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "unknown principal")]
async fn test_unknown_principal_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("ghost")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "unknown project")]
async fn test_unknown_project_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("ghost", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "no embedding model set")]
async fn test_no_embedding_model_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "embeddable item"));
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "non-negative duration")]
async fn test_negative_advance_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .advance(Duration::hours(-1))
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "no projects were defined")]
async fn test_no_projects_defined_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_principal("user", "user:key")
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "no principals were defined")]
async fn test_no_principals_defined_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "self-relation not permitted")]
async fn test_self_relation_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .relate("x", Supports, "x")
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "conflicting values")]
async fn test_conflicting_embedding_model_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("model-a", 768)
        .set_embedding_model("model-b", 512)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "requires an active principal")]
async fn test_no_principal_for_add_item_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        // Note: no as_principal() call.
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "requires an active principal")]
async fn test_no_principal_for_observe_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    // The observe is declared before as_principal, so it captures None.
    // The add_item is declared after, so it captures "user".
    // The two-bucket partition processes items (bucket 1) first, creating
    // "x" successfully, then processes observes (bucket 2) which panics
    // on the missing principal.
    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .for_project("proj", |store| {
            store
                .observe("x", SourceType::AgentMediated)
                .as_principal("user")
                .add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

#[tokio::test]
#[should_panic(expected = "embedding dimensions must be greater than zero")]
async fn test_zero_dimensions_panics() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 0)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .execute(&mut txn)
        .await;
}

// ---------------------------------------------------------------------------
// No-op behaviours
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_commit_relations_with_no_pending_is_noop() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x").skip_embed());
        })
        .commit_relations("empty-batch")
        .execute(&mut txn)
        .await;

    assert_eq!(result.batch_count(), 0);
}

#[tokio::test]
async fn test_set_embedding_model_identical_is_noop() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin txn");

    // Setting the same model twice should not panic.
    let result = Seed::new()
        .define_project("proj", "git@example.com:test.git")
        .define_principal("user", "user:key")
        .set_embedding_model("test-model", 768)
        .set_embedding_model("test-model", 768)
        .as_principal("user")
        .for_project("proj", |store| {
            store.add_item("x", item(Fact, "x"));
        })
        .execute(&mut txn)
        .await;

    let _ = result.embedding_id("x");
}
