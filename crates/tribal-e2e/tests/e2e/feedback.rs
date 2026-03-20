use serde_json::json;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, candidate, novel},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies the discover → feedback loop: ingest knowledge, discover
/// it by query, then submit retrieval feedback with the trace ID from
/// the discover response.
///
/// Theme: an engineer discovers a latency debugging heuristic for
/// Canopy's event replay system and rates the retrieval positively.
#[tokio::test]
async fn test_feedback_after_discovery() {
    let mut harness = TestHarness::init(|_setup| {}).await;

    // -- Mount mocks ----------------------------------------------------------

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "heuristic",
                            "When debugging Canopy latency spikes, check the event \
                             replay snapshot cache hit rate before scaling up replay \
                             workers",
                        )
                        .tags(&["debugging", "latency", "event-replay"]),
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

    // Single candidate, no intra-batch relations needed.
    // Relation stage receives an empty batch — mount an empty fixture.
    harness
        .mount_relation(|m| {
            m.respond(RelationFixture::builder().build());
        })
        .await;

    // -- Ingest and wait for pipeline completion ------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "When debugging Canopy latency spikes, check the event \
                            replay snapshot cache hit rate before scaling up workers."
            }),
        )
        .await;
    assert_success!(result);

    let job_id = tool_result_json(&result)["job_id"]
        .as_str()
        .expect("job_id")
        .to_owned();

    harness.expect_completion(&job_id).await;

    // -- Discover the ingested knowledge --------------------------------------

    let discover_result = harness
        .call_tool("tribal_discover", json!({ "query": "latency debugging" }))
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let trace_id = discover_json["trace_id"]
        .as_str()
        .expect("trace_id in discover response");

    let items = discover_json["items"].as_array().expect("items array");
    assert!(!items.is_empty(), "discover should return the ingested item");

    let returned_ids: Vec<&str> = items
        .iter()
        .filter_map(|i| i["item"]["id"].as_str())
        .collect();

    // -- Submit feedback ------------------------------------------------------

    let feedback_result = harness
        .call_tool(
            "tribal_feedback",
            json!({
                "trace_id": trace_id,
                "query_text": "latency debugging",
                "returned_item_ids": returned_ids,
                "rating": "positive",
                "notes": "Exactly the heuristic I needed for the replay investigation",
            }),
        )
        .await;
    assert_success!(feedback_result);

    let feedback_json = tool_result_json(&feedback_result);
    assert!(
        feedback_json["feedback_id"].as_str().is_some(),
        "feedback response should include a feedback_id",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
