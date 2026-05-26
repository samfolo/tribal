use serde_json::json;
use tribal_domain::{JobOutcome, KnowledgeKind, ProviderKind};
use tribal_test_utils::item;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, duplicate},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies that when every extracted candidate is classified as a
/// duplicate of an existing item, the job completes with an `empty`
/// outcome, no new knowledge items are created, and exactly one
/// observation is recorded against the matched item.
///
/// Theme: re-ingesting Canopy's notification architecture when it is
/// already captured in the knowledge base.
#[tokio::test]
async fn test_duplicate_only_batch() {
    let mut harness = TestHarness::init(|setup| {
        // Mixed providers: OpenAI embedding, Anthropic extraction,
        // Ollama triage/relation (default). Exercises heterogeneous
        // envelope handling across all three provider implementations.
        setup.config(|c| {
            c.embedding.provider = ProviderKind::OpenAi;
            c.embedding.api_key = Some("sk-e2e-000000".parse().expect("test fixture is valid"));
            c.inference.extraction.provider = ProviderKind::Anthropic;
            c.inference.extraction.api_key =
                Some("sk-ant-e2e-000000".parse().expect("test fixture is valid"));
        });

        setup.graph(|g| {
            g.as_principal("default")
                .for_project("test-project", |store| {
                    store.add_item(
                        "notification_item",
                        item(
                            KnowledgeKind::Fact,
                            "Canopy's notification service uses a fan-out-on-write \
                             pattern to pre-compute per-user notification feeds",
                        ),
                    );
                })
        });
    })
    .await;

    // -- Mount mocks ----------------------------------------------------------

    // Extraction produces a single candidate that restates the existing knowledge.
    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "fact",
                            "The notification system fans out writes to pre-computed \
                             feeds for each user",
                        )
                        .tags(&["notifications", "fan-out"]),
                    )
                    .build(),
            );
        })
        .await;

    // Triage classifies the candidate as a duplicate of the existing item,
    // the sole search hit, at index 0.
    harness
        .mount_triage(|m| {
            m.respond(duplicate(0).build());
        })
        .await;

    // Relation stage still runs (computes outcome from triage results)
    // but produces no relations.
    harness
        .mount_relation(|m| {
            m.respond(RelationFixture::builder().build());
        })
        .await;

    // -- Ingest ---------------------------------------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "The notification system fans out writes to \
                            pre-computed feeds for each user."
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
        "no items should be created when all candidates are duplicates",
    );
    assert_eq!(
        status_json["observations_created"].as_u64(),
        Some(1),
        "exactly one observation should be recorded for the duplicate encounter",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
