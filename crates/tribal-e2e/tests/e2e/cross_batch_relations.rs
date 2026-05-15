use serde_json::json;
use tribal_config::ProviderKind;
use tribal_domain::KnowledgeKind;
use tribal_test_utils::item;

use crate::harness::{
    assertions::assert_success,
    fixtures::{
        ExtractionFixture, ReferenceSpec, RelationFixture, SimilarItemSpec, candidate, hint, novel,
        relate,
    },
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies that new content can be related to pre-existing knowledge
/// items via `relate()` relation edges, and that the resulting
/// graph is traversable via `tribal_explore`.
///
/// Theme: Canopy's event sourcing architecture — a new performance
/// fact supports the original architecture decision, and a companion
/// monitoring heuristic is extracted alongside it.
#[tokio::test]
async fn test_cross_batch_relations() {
    let mut harness = TestHarness::init(|setup| {
        // Anthropic triage exercises the Anthropic envelope abstraction.
        setup.config(|c| {
            c.inference.triage.provider = ProviderKind::Anthropic;
            c.inference.triage.api_key =
                Some("sk-ant-e2e-000000".parse().expect("test fixture is valid"));
        });

        setup.graph(|g| {
            g.as_principal("default")
                .for_project("test-project", |store| {
                    store.add_item(
                        "decision",
                        item(
                            KnowledgeKind::DecisionRecord,
                            "Meridian chose event sourcing for Canopy's document \
                             history service to support arbitrary undo depth and \
                             audit compliance",
                        ),
                    );
                })
        });
    })
    .await;

    let decision_id = harness.label("decision");

    // -- Mount mocks ----------------------------------------------------------

    // Two candidates: a fact about storage savings (with a reference) and a
    // monitoring heuristic. The extraction includes a relation hint linking them.
    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "fact",
                            "Event sourcing in the document service reduced storage \
                             costs by 40% through snapshot compaction",
                        )
                        .tags(&["event-sourcing", "storage", "performance"])
                        .reference(ReferenceSpec {
                            kind: "file_path",
                            value: "services/doc-history/src/compaction.rs",
                            description: Some("Snapshot compaction implementation"),
                        }),
                    )
                    .candidate(
                        candidate(
                            "heuristic",
                            "Monitor the snapshot compaction lag metric; if it exceeds \
                             the replay window, increase the compaction worker count",
                        )
                        .tags(&["event-sourcing", "monitoring"]),
                    )
                    .relation_hint(hint(1, "derived_from", 0))
                    .build(),
            );
        })
        .await;

    // Both candidates are novel. Triage recognises the seeded architecture
    // decision as a similar item to the storage cost fact.
    harness
        .mount_triage(|m| {
            m.on_content_repeat_last(
                "storage costs",
                &[novel()
                    .similar_item(SimilarItemSpec {
                        item_id: decision_id,
                        suggested_relation: "supports",
                        justification: "Storage savings validate the event sourcing decision",
                    })
                    .build()
                    .into()],
            );
            m.on_content_repeat_last("compaction lag", &[novel().build().into()]);
        })
        .await;

    // Relation stage: the storage fact supports the seeded decision (cross-batch),
    // and the monitoring heuristic is derived from the storage fact (intra-batch).
    harness
        .mount_relation(|m| {
            m.respond(
                RelationFixture::builder()
                    .edge(relate(0, "supports", 2).justification(
                        "Storage cost reduction validates the event sourcing \
                             architecture decision",
                    ))
                    .edge(relate(1, "derived_from", 0))
                    .build(),
            );
        })
        .await;

    // -- Ingest and wait for pipeline completion ------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "Event sourcing reduced storage costs by 40% through \
                            snapshot compaction. Monitor the compaction lag metric; \
                            if it exceeds the replay window, increase compaction workers."
            }),
        )
        .await;
    assert_success!(result);

    let ingest_json = tool_result_json(&result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id").to_owned();

    harness.expect_completion(&job_id).await;

    // -- Verify the cross-batch relation via explore --------------------------

    // The storage cost fact should have an outbound "supports" edge to the
    // seeded decision record.
    let discover_result = harness
        .call_tool(
            "tribal_discover",
            json!({ "query": "event sourcing storage costs" }),
        )
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"].as_array().expect("items array");

    let storage_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("storage costs"))
        })
        .expect("storage cost fact should appear in discover results");
    let storage_id = storage_item["item"]["id"]
        .as_str()
        .expect("item id")
        .to_owned();

    // Explore outbound from the storage fact.
    let explore_result = harness
        .call_tool(
            "tribal_explore",
            json!({
                "item_id": &storage_id,
                "direction": "outbound",
                "depth": 1,
            }),
        )
        .await;
    assert_success!(explore_result);

    let explore_json = tool_result_json(&explore_result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let related_ids: Vec<&str> = related
        .iter()
        .filter_map(|r| r["item"]["id"].as_str())
        .collect();
    assert!(
        related_ids.contains(&decision_id),
        "storage fact should have an outbound relation to the seeded decision; \
         got {related_ids:?}",
    );

    // Verify the relation type is "supports".
    let decision_edge = related
        .iter()
        .find(|r| r["item"]["id"].as_str() == Some(decision_id))
        .expect("decision edge");
    assert_eq!(
        decision_edge["relation_type"].as_str(),
        Some("supports"),
        "cross-batch edge should be 'supports'",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
