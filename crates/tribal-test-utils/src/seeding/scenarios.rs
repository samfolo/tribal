//! Pre-built seed scenarios for common test graph shapes.

use chrono::Duration;
use tribal_domain::{
    KnowledgeKind::{DecisionRecord, Fact, Heuristic},
    ReferenceKind::{Concept, FilePath, Symbol, Url},
    RelationKind::{Contradicts, DerivedFrom, Supersedes, Supports},
};

use super::types::{Seed, item};

/// Returns a seed describing a basic knowledge graph.
///
/// Entities: 1 project (`"tribal"`), 1 principal (`"sam"`), 5 items
/// across 3 embedding groups, 5 references, 3 committed relations in
/// 1 batch (`"initial-relations"`).
pub fn basic_knowledge_graph() -> Seed {
    Seed::new()
        .define_project("tribal", "git@github.com:test/tribal.git")
        .define_principal("sam", "user:sam")
        .set_embedding_model("test-model", 768)
        .as_principal("sam")
        .for_project("tribal", |store| {
            store
                .add_item(
                    "heuristic-1",
                    item(Heuristic, "Cache invalidation is hard")
                        .tags(&["caching"])
                        .embedding_group("caching"),
                )
                .add_item(
                    "heuristic-2",
                    item(Heuristic, "Prefer composition over inheritance")
                        .tags(&["architecture"])
                        .embedding_group("design"),
                )
                .add_item(
                    "fact-1",
                    item(Fact, "Redis TTL defaults to no expiry")
                        .tags(&["redis", "caching"])
                        .embedding_group("caching"),
                )
                .add_item(
                    "fact-2",
                    item(Fact, "Postgres JSONB supports GIN indexing")
                        .tags(&["postgres"])
                        .embedding_group("databases"),
                )
                .add_item(
                    "decision-1",
                    item(DecisionRecord, "Chose Redis over Memcached")
                        .tags(&["redis", "architecture"])
                        .embedding_group("caching"),
                )
                .add_reference("heuristic-1", FilePath, "//src/cache.rs")
                .add_reference("heuristic-2", Url, "https://example.com/composition")
                .add_reference("fact-1", Concept, "redis-ttl")
                .add_reference("fact-2", Symbol, "jsonb_path_query")
                .add_reference("decision-1", FilePath, "//docs/adr/001-cache-backend.md");
        })
        .relate("fact-1", Supports, "heuristic-1")
        .relate("fact-2", Contradicts, "heuristic-1")
        .relate("decision-1", DerivedFrom, "heuristic-1")
        .commit_relations("initial-relations")
}

/// Returns a seed describing a supersession chain with temporal ordering.
///
/// Entities: 1 project (`"tribal"`), 1 principal (`"sam"`), 5 items,
/// 5 relations across 3 committed batches (`"original-support"`,
/// `"contradiction"`, `"supersession"`). Two items are superseded.
pub fn supersession_scenario() -> Seed {
    Seed::new()
        .define_project("tribal", "git@github.com:test/tribal.git")
        .define_principal("sam", "user:sam")
        .set_embedding_model("test-model", 768)
        .as_principal("sam")
        // Original items
        .for_project("tribal", |store| {
            store
                .add_item(
                    "original-fact",
                    item(Fact, "Library X uses approach A").embedding_group("library-x"),
                )
                .add_item(
                    "original-heuristic",
                    item(Heuristic, "Always use approach A").embedding_group("approach-a"),
                );
        })
        .relate("original-fact", Supports, "original-heuristic")
        .commit_relations("original-support")
        .advance(Duration::hours(6))
        // Contradicting evidence
        .for_project("tribal", |store| {
            store.add_item(
                "contradicting-fact",
                item(Fact, "Library X v2 deprecated approach A").embedding_group("library-x"),
            );
        })
        .relate("contradicting-fact", Contradicts, "original-heuristic")
        .commit_relations("contradiction")
        .advance(Duration::hours(6))
        // Supersession
        .for_project("tribal", |store| {
            store
                .add_item(
                    "superseding-fact",
                    item(Fact, "Library X v3 uses approach B").embedding_group("library-x"),
                )
                .add_item(
                    "revised-heuristic",
                    item(Heuristic, "Use approach B for Library X").embedding_group("approach-b"),
                );
        })
        .relate("superseding-fact", Supersedes, "original-fact")
        .relate("revised-heuristic", Supersedes, "original-heuristic")
        .relate("superseding-fact", Supports, "revised-heuristic")
        .commit_relations("supersession")
}
