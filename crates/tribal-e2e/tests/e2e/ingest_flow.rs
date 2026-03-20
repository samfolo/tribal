use serde_json::json;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, intra_batch, novel},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies the full ingest pipeline: extraction produces two novel
/// candidates, triage classifies both as novel, relations are
/// committed, and both items are discoverable via semantic search,
/// retrievable via `tribal_get_item`, and connected via explore.
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
    let mut harness = TestHarness::init(|_setup| {}).await;

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

    // Both novel candidates should be discoverable.
    let canary_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("canary analysis"))
        })
        .expect("canary item should appear in discover results");
    let canary_id = canary_item["item"]["id"]
        .as_str()
        .expect("canary item id")
        .to_owned();

    let rollback_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("roll back"))
        })
        .expect("rollback item should appear in discover results");
    let rollback_id = rollback_item["item"]["id"]
        .as_str()
        .expect("rollback item id")
        .to_owned();

    // -- Verify via get_item --------------------------------------------------

    let get_result = harness
        .call_tool(
            "tribal_get_item",
            json!({ "item_ids": [&canary_id, &rollback_id] }),
        )
        .await;
    assert_success!(get_result);

    let get_json = tool_result_json(&get_result);
    assert!(
        get_json["items"][&canary_id]["item"]["content"]
            .as_str()
            .is_some_and(|c| c.contains("canary analysis")),
        "get_item should return the canary item",
    );
    assert!(
        get_json["items"][&rollback_id]["item"]["content"]
            .as_str()
            .is_some_and(|c| c.contains("roll back")),
        "get_item should return the rollback item",
    );

    // -- Verify relation via explore ------------------------------------------

    let explore_result = harness
        .call_tool(
            "tribal_explore",
            json!({ "item_id": &canary_id, "direction": "outbound", "depth": 1 }),
        )
        .await;
    assert_success!(explore_result);

    let explore_json = tool_result_json(&explore_result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let rollback_edge = related
        .iter()
        .find(|r| {
            r["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("roll back"))
        })
        .expect("canary item should have an outbound relation to the rollback procedure");
    assert_eq!(
        rollback_edge["relation_type"].as_str(),
        Some("supports"),
        "edge to rollback procedure should be 'supports'",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
