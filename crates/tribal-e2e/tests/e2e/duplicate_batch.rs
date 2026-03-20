use serde_json::json;
use tribal_db::{KnowledgeItemRepository, PgKnowledgeItemRepository};
use tribal_test_utils::a_new_knowledge_item;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, duplicate},
    polling::expect_condition,
    server::{TestHarness, seed},
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
        seed!(setup, |seed| {
            let project_id = seed.project_id();
            let principal_id = seed.principal_id();
            let existing = PgKnowledgeItemRepository
                .insert(
                    seed.conn(),
                    &a_new_knowledge_item()
                        .project_id(project_id)
                        .principal_id(principal_id)
                        .content(
                            "Canopy's notification service uses a fan-out-on-write pattern \
                             to pre-compute per-user notification feeds"
                                .to_owned(),
                        )
                        .build(),
                )
                .await
                .expect("insert existing notification item");
            seed.label("notification_item", existing.id());
        });
    })
    .await;

    let existing_id = harness.label("notification_item");

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

    // Triage classifies the candidate as a duplicate of the existing item.
    harness
        .mount_triage(|m| {
            m.respond(duplicate(existing_id).build());
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

    let job_id = tool_result_json(&result)["job_id"]
        .as_str()
        .expect("job_id")
        .to_owned();

    // -- Wait for pipeline completion -----------------------------------------

    // All duplicates → "empty" outcome (no novel items created).
    expect_condition!("job completes with empty outcome", wait: 30_000, {
        let status_result = harness
            .call_tool(
                "tribal_job_status",
                json!({ "job_id": &job_id, "wait_seconds": 2 }),
            )
            .await;
        let json = tool_result_json(&status_result);
        json["status"].as_str() == Some("completed")
            && json["outcome"].as_str() == Some("empty")
    });

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
