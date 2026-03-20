use serde_json::json;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, intra_batch, novel},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies that relation normalisation gracefully handles invalid
/// edges without failing the pipeline.
///
/// The relation mock returns a mix of valid and invalid edges:
/// - A valid intra-batch edge (0 supports 1)
/// - A supersedes edge (always dropped by normalisation step 1)
/// - An edge with an out-of-range batch index (unresolvable, step 3)
/// - A self-edge (dropped after resolution, step 4)
/// - An exact duplicate of the valid edge (deduplicated, step 5)
///
/// The normalisation layer drops all four invalid edges and commits
/// only the valid one. The job completes with `success` outcome.
///
/// Theme: Canopy's authentication subsystem — the LLM hallucinates
/// some relation edges that don't correspond to real candidates.
#[tokio::test]
async fn test_relation_normalisation_drops_invalid_edges() {
    let mut harness = TestHarness::init(|_setup| {}).await;

    // -- Mount mocks ----------------------------------------------------------

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "fact",
                            "Canopy's authentication service issues JWT tokens with a \
                             15-minute expiry and a 7-day refresh window",
                        )
                        .tags(&["auth", "jwt"]),
                    )
                    .candidate(
                        candidate(
                            "heuristic",
                            "When debugging authentication failures, check the clock \
                             skew between the auth service and the API gateway before \
                             investigating token contents",
                        )
                        .tags(&["auth", "debugging"]),
                    )
                    .candidate(
                        candidate(
                            "procedure",
                            "To rotate Canopy's JWT signing key, deploy the new key to \
                             all API gateway nodes before revoking the old one",
                        )
                        .tags(&["auth", "key-rotation"]),
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

    // Mix of valid and invalid edges — each invalid edge uses a
    // different relation type to exercise all normalisation rules.
    harness
        .mount_relation(|m| {
            m.respond(
                RelationFixture::builder()
                    // Valid: candidate 0 supports candidate 1.
                    .edge(intra_batch(0, "supports", 1))
                    // Dropped (step 1): supersedes edges are always removed.
                    // Uses a distinct source/target (0→2) so it can't be
                    // masked by deduplication with the valid edge above.
                    .edge(intra_batch(0, "supersedes", 2))
                    // Dropped (step 3): batch index 99 does not exist.
                    .edge(intra_batch(99, "contradicts", 0))
                    // Dropped (step 4): self-edge after resolution.
                    .edge(intra_batch(0, "derived_from", 0))
                    // Dropped (step 5): exact duplicate of the valid edge.
                    .edge(intra_batch(0, "supports", 1))
                    .build(),
            );
        })
        .await;

    // -- Ingest and wait for pipeline completion ------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "Canopy issues JWTs with 15-minute expiry. When debugging \
                            auth failures, check clock skew first. To rotate the \
                            signing key, deploy before revoking."
            }),
        )
        .await;
    assert_success!(result);

    let ingest_json = tool_result_json(&result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id").to_owned();

    // Pipeline should complete despite the invalid edges.
    harness.expect_completion(&job_id).await;

    // -- Verify items were created --------------------------------------------

    let status_json = tool_result_json(
        &harness
            .call_tool("tribal_job_status", json!({ "job_id": &job_id }))
            .await,
    );
    assert_eq!(
        status_json["items_created"].as_u64(),
        Some(3),
        "all three candidates should be created despite invalid relation edges",
    );

    // -- Verify the valid edge survived via explore ---------------------------

    let discover_result = harness
        .call_tool("tribal_discover", json!({ "query": "JWT authentication" }))
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"].as_array().expect("items array");

    let jwt_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("JWT tokens"))
        })
        .expect("JWT fact should appear in discover results");
    let jwt_id = jwt_item["item"]["id"].as_str().expect("item id").to_owned();

    let explore_result = harness
        .call_tool(
            "tribal_explore",
            json!({
                "item_id": &jwt_id,
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

    // The valid edge (0 supports 1) should be the only surviving relation.
    assert_eq!(
        related.len(),
        1,
        "only the valid intra-batch edge should survive normalisation; \
         got {} relations",
        related.len(),
    );
    assert_eq!(
        related[0]["relation_type"].as_str(),
        Some("supports"),
        "surviving edge should be 'supports'",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
