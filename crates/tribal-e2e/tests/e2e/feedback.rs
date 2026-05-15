use serde_json::json;
use tribal_config::ProviderKind;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, novel},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies the full retrieval session lifecycle: ingest knowledge,
/// discover it by query, explore a result's graph neighbourhood,
/// then submit feedback that threads the entire session together
/// via trace ID.
///
/// Theme: an engineer discovers a latency debugging heuristic for
/// Canopy's event replay system, explores its relations, and rates
/// the retrieval session positively.
#[tokio::test]
async fn test_feedback_after_discovery() {
    let mut harness = TestHarness::init(|setup| {
        // OpenAI embedding exercises the OpenAI embed envelope abstraction.
        setup.config(|c| {
            c.embedding.provider = ProviderKind::OpenAi;
            c.embedding.api_key = Some("sk-e2e-000000".parse().expect("test fixture is valid"));
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

    let ingest_json = tool_result_json(&result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id").to_owned();

    harness.expect_completion(&job_id).await;

    // -- Discover the ingested knowledge --------------------------------------

    let discover_result = harness
        .call_tool("tribal_discover", json!({ "query": "latency debugging" }))
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let trace_id = discover_json["trace_id"]
        .as_str()
        .expect("trace_id in discover response")
        .to_owned();

    let items = discover_json["items"].as_array().expect("items array");
    assert!(
        !items.is_empty(),
        "discover should return the ingested item"
    );

    let anchor_id = items[0]["item"]["id"].as_str().expect("item id").to_owned();

    let returned_ids: Vec<&str> = items
        .iter()
        .filter_map(|i| i["item"]["id"].as_str())
        .collect();

    // -- Explore the discovered item ------------------------------------------

    let explore_result = harness
        .call_tool(
            "tribal_explore",
            json!({
                "item_id": &anchor_id,
                "session_trace_id": &trace_id,
                "direction": "both",
            }),
        )
        .await;
    assert_success!(explore_result);

    let explore_json = tool_result_json(&explore_result);
    let explore_trace = explore_json["trace_id"]
        .as_str()
        .expect("trace_id in explore response");
    assert_eq!(
        explore_trace, &trace_id,
        "explore with session_trace_id should return the same trace ID",
    );

    // -- Submit feedback for the full retrieval session ------------------------

    let feedback_result = harness
        .call_tool(
            "tribal_feedback",
            json!({
                "trace_id": &trace_id,
                "query_text": "latency debugging",
                "returned_item_ids": returned_ids,
                "explored_anchor_ids": [&anchor_id],
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
