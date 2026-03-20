use serde_json::json;
use tribal_config::ProviderKind;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, novel},
    server::{DEFAULT_PRINCIPAL_KEY, TestHarness},
    tool_call::tool_result_json,
};

/// Verifies system stability under concurrent ingestion of identical
/// content from two independent clients.
///
/// Both pipelines run to completion without errors. Because triage
/// only sees committed items, both classify the candidate as novel —
/// producing two knowledge items rather than one. This documents a
/// known race window in triage-based deduplication: there is no
/// DB-level content uniqueness constraint, so concurrent triage
/// calls that both see an empty knowledge base will both classify
/// as novel.
///
/// Theme: two Meridian engineers independently capture the same
/// Canopy observability insight.
#[tokio::test]
async fn test_concurrent_identical_ingests() {
    let mut harness = TestHarness::init(|setup| {
        // OpenAI relation exercises the OpenAI envelope abstraction.
        setup.config(|c| {
            c.inference.relation.provider = ProviderKind::OpenAi;
            c.inference.relation.api_key = Some("sk-e2e-000000".to_owned());
        });
    })
    .await;

    // -- Mount mocks ----------------------------------------------------------

    // Both pipelines hit the same extraction mock — persistent, returns
    // the same candidate each time.
    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "heuristic",
                            "When Canopy's WebSocket connection count exceeds 10,000 \
                             per node, check the event loop saturation metric before \
                             scaling horizontally",
                        )
                        .tags(&["observability", "websockets", "scaling"]),
                    )
                    .build(),
            );
        })
        .await;

    // Triage responds novel for all requests. The deduplication happens
    // at the knowledge item insertion layer (content + project uniqueness),
    // not at the triage mock level.
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

    // -- Set up second client with the same project context -------------------

    let client_2 = harness.connect_client(DEFAULT_PRINCIPAL_KEY).await;

    // Discover from the primary client to learn the resolved project ID.
    let discover_result = harness
        .call_tool("tribal_discover", json!({ "query": "anything" }))
        .await;
    let discover_json = tool_result_json(&discover_result);
    let project_id = discover_json["applied_project_id"]
        .as_str()
        .expect("applied_project_id")
        .to_owned();

    let result = client_2
        .call_tool("tribal_set_context", json!({ "project_id": &project_id }))
        .await;
    assert_success!(result);

    // -- Ingest from both clients simultaneously ------------------------------

    let content = "When Canopy's WebSocket connection count exceeds 10,000 per node, \
                   check event loop saturation before scaling horizontally.";

    let (result_1, result_2) = tokio::join!(
        harness.call_tool("tribal_ingest", json!({ "content": content })),
        client_2.call_tool("tribal_ingest", json!({ "content": content })),
    );
    assert_success!(result_1);
    assert_success!(result_2);

    let json_1 = tool_result_json(&result_1);
    let json_2 = tool_result_json(&result_2);
    let job_id_1 = json_1["job_id"].as_str().expect("job_id_1").to_owned();
    let job_id_2 = json_2["job_id"].as_str().expect("job_id_2").to_owned();

    // -- Wait for both pipelines to complete ----------------------------------

    harness.expect_completion(&job_id_1).await;
    harness.expect_completion(&job_id_2).await;

    // -- Verify deduplication -------------------------------------------------

    let result = harness
        .call_tool(
            "tribal_discover",
            json!({ "query": "websocket connection scaling" }),
        )
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    let items = discover_json["items"].as_array().expect("items array");

    // Both triage calls see no existing items (race window), so both
    // classify as novel. This produces 2 items with identical content.
    let matching: Vec<_> = items
        .iter()
        .filter(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("WebSocket connection count"))
        })
        .collect();
    assert_eq!(
        matching.len(),
        2,
        "concurrent triage race: both jobs create an item; got {} matching items",
        matching.len(),
    );

    // Both jobs should report 1 item created (no observations, since
    // triage classified both as novel independently).
    let status_1 = tool_result_json(
        &harness
            .call_tool("tribal_job_status", json!({ "job_id": &job_id_1 }))
            .await,
    );
    let status_2 = tool_result_json(
        &harness
            .call_tool("tribal_job_status", json!({ "job_id": &job_id_2 }))
            .await,
    );

    assert_eq!(
        status_1["items_created"].as_u64(),
        Some(1),
        "job 1 should create 1 item",
    );
    assert_eq!(
        status_2["items_created"].as_u64(),
        Some(1),
        "job 2 should create 1 item",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
