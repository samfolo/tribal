use serde_json::json;
use tribal_domain::JobOutcome;

use crate::harness::{
    assertions::assert_success, fixtures::ExtractionFixture, server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies that ingesting trivial content which yields no knowledge
/// candidates completes the job with an `empty` outcome and creates
/// no knowledge items.
#[tokio::test]
async fn test_zero_candidate_extraction() {
    let mut harness = TestHarness::init(|_setup| {}).await;

    // -- Mount mocks ----------------------------------------------------------

    // Extraction returns zero candidates — nothing knowledge-worthy.
    harness
        .mount_extraction(|m| {
            m.respond(ExtractionFixture::builder().build());
        })
        .await;

    // Triage and relation stages should not be called (no candidates).

    // -- Ingest ---------------------------------------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "Had a quick sync about the next sprint. \
                            Alice is covering on-call for Bob next week."
            }),
        )
        .await;
    assert_success!(result);

    let ingest_json = tool_result_json(&result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id").to_owned();

    // -- Wait for pipeline completion -----------------------------------------

    harness.expect_outcome(&job_id, JobOutcome::Empty).await;

    // -- Verify ---------------------------------------------------------------

    let status_json = tool_result_json(
        &harness
            .call_tool("tribal_job_status", json!({ "job_id": &job_id }))
            .await,
    );
    assert_eq!(
        status_json["items_created"].as_u64(),
        Some(0),
        "no items should be created for zero-candidate extraction",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
}
