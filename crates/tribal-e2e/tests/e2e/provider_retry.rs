use serde_json::json;
use tribal_domain::ProviderKind;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, novel, relate},
    mocks::{ContentMatcher, error},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies that a transient provider error during extraction is
/// retried by the worker and the pipeline completes successfully.
///
/// The extraction mock returns a 503 on the first call, then the
/// real fixture on retry. Triage and relation proceed normally.
///
/// Theme: extracting knowledge about Canopy's API gateway rate
/// limiting, but the extraction provider is briefly unavailable.
#[tokio::test]
async fn test_provider_failure_and_retry() {
    let mut harness = TestHarness::init(|setup| {
        // OpenAI extraction exercises the OpenAI envelope abstraction.
        setup.config(|c| {
            c.inference.extraction.provider = ProviderKind::OpenAi;
            c.inference.extraction.api_key =
                Some("sk-e2e-000000".parse().expect("test fixture is valid"));
        });
    })
    .await;

    // -- Mount mocks ----------------------------------------------------------

    let extraction_fixture = ExtractionFixture::builder()
        .candidate(
            candidate(
                "fact",
                "The Canopy API gateway enforces tenant-level rate limiting \
                 at 1000 requests per second",
            )
            .tags(&["api-gateway", "rate-limiting"]),
        )
        .candidate(
            candidate(
                "heuristic",
                "When a tenant hits the rate limit, back off exponentially \
                 rather than retrying immediately to avoid thundering herd",
            )
            .tags(&["rate-limiting", "resilience"]),
        )
        .build();

    // First extraction call → 503; retry → fixture.
    harness
        .mount_extraction(|m| {
            m.on_content_repeat_last(
                ContentMatcher::Any,
                &[error(503), extraction_fixture.into()],
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
                    .edge(relate(0, "supports", 1))
                    .build(),
            );
        })
        .await;

    // -- Ingest and wait for pipeline completion ------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "The Canopy API gateway enforces tenant-level rate limiting. \
                            When a tenant hits the limit, back off exponentially."
            }),
        )
        .await;
    assert_success!(result);

    let job_id = tool_result_json(&result)["job_id"]
        .as_str()
        .expect("job_id")
        .to_owned();

    // Despite the transient 503, the worker retries and succeeds.
    harness.expect_completion(&job_id).await;

    // -- Verify ---------------------------------------------------------------

    let status_json = tool_result_json(
        &harness
            .call_tool("tribal_job_status", json!({ "job_id": &job_id }))
            .await,
    );
    assert_eq!(
        status_json["items_created"].as_u64(),
        Some(2),
        "both candidates should be created after successful retry",
    );

    // The retry is genuine, not vacuous: the extraction endpoint saw two
    // content-bearing calls, the transient 503 and the successful
    // re-attempt. The startup probe carries the probe input rather than the
    // ingest content, so filtering on the content isolates the real calls.
    let extraction_calls = harness
        .extraction_server()
        .received_requests()
        .await
        .expect("recorded extraction requests")
        .iter()
        .filter(|r| String::from_utf8_lossy(&r.body).contains("Canopy API gateway"))
        .count();
    assert_eq!(
        extraction_calls, 2,
        "the 503 and its successful retry are two real extraction calls",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
}
