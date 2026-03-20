use serde_json::json;
use tribal_domain::KnowledgeKind;
use tribal_test_utils::item;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, intra_batch, novel},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies the full ingest pipeline: extraction produces two novel
/// candidates, triage classifies both as novel, relations are
/// committed, and both items are discoverable via semantic search
/// and retrievable via `tribal_get_item`.
///
/// This is the happy-path pipeline test. Duplicate handling is
/// covered by `test_duplicate_only_batch`; content-matched triage
/// mocks with mixed outcomes are avoided here to prevent wiremock
/// concurrent matching flakes.
///
/// Theme: Canopy's deployment infrastructure — a canary analysis
/// fact and a rollback procedure, related by a "supports" edge.
#[tokio::test]
async fn test_ingest_pipeline_end_to_end() {
    let mut harness = TestHarness::init(|setup| {
        setup.graph(|g| {
            g.as_principal("default")
                .for_project("test-project", |store| {
                    store.add_item(
                        "existing",
                        item(
                            KnowledgeKind::Fact,
                            "Canopy uses blue-green deployments to achieve \
                             zero-downtime releases for the collaboration service",
                        ),
                    );
                })
        });
    })
    .await;

    // -- Mount mocks ----------------------------------------------------------

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "fact",
                            "The deployment pipeline runs canary analysis for 15 \
                             minutes before promoting to full traffic",
                        )
                        .tags(&["deployment", "canary"]),
                    )
                    .candidate(
                        candidate(
                            "procedure",
                            "To roll back a failed Canopy deployment, revert the \
                             target group weight to the previous stable version",
                        )
                        .tags(&["deployment", "rollback"]),
                    )
                    .build(),
            );
        })
        .await;

    harness
        .mount_triage(|m| {
            m.respond(novel().build());
        })
        .await;

    harness
        .mount_relation(|m| {
            m.respond(
                RelationFixture::builder()
                    .edge(intra_batch(0, "supports", 1))
                    .build(),
            );
        })
        .await;

    // -- Ingest ---------------------------------------------------------------

    let ingest_result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "The deployment pipeline runs canary analysis before \
                            promoting. To roll back, revert the target group weight."
            }),
        )
        .await;
    assert_success!(ingest_result);

    let ingest_json = tool_result_json(&ingest_result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id in response");

    // -- Wait for pipeline completion -----------------------------------------

    harness.expect_completion(job_id).await;

    // -- Verify via discover --------------------------------------------------

    let discover_result = harness
        .call_tool(
            "tribal_discover",
            json!({ "query": "deployment canary rollback" }),
        )
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"].as_array().expect("items array");

    // Both novel candidates + the seeded item should appear.
    assert!(
        items.len() >= 2,
        "expected at least 2 items from the pipeline, got {}",
        items.len(),
    );

    let canary_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("canary analysis"))
        })
        .expect("canary item should appear in discover results");
    let canary_id = canary_item["item"]["id"].as_str().expect("canary item id");

    // -- Verify via get_item --------------------------------------------------

    let get_result = harness
        .call_tool("tribal_get_item", json!({ "item_ids": [canary_id] }))
        .await;
    assert_success!(get_result);

    let get_json = tool_result_json(&get_result);
    let fetched_content = get_json["items"][canary_id]["item"]["content"]
        .as_str()
        .expect("content field in get_item response");
    assert!(
        fetched_content.contains("canary analysis"),
        "expected content to contain 'canary analysis', got: {fetched_content}",
    );

    // -- Verify relation via explore ------------------------------------------

    let explore_result = harness
        .call_tool(
            "tribal_explore",
            json!({ "item_id": canary_id, "direction": "outbound", "depth": 1 }),
        )
        .await;
    assert_success!(explore_result);

    let explore_json = tool_result_json(&explore_result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    assert!(
        related.iter().any(|r| r["item"]["content"]
            .as_str()
            .is_some_and(|c| c.contains("roll back"))),
        "canary item should have an outbound relation to the rollback procedure",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
