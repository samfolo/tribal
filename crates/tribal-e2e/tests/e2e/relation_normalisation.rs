use serde_json::json;
use tribal_config::ProviderKind;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, novel, relate},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies that relation normalisation gracefully handles invalid
/// edges without failing the pipeline.
///
/// The relation mock returns a mix of valid and invalid edges:
/// - A valid intra-batch edge (0 supports 1)
/// - An edge with an unknown relation type (skipped during lenient parsing)
/// - An edge with an out-of-range batch index (unresolvable, step 2)
/// - A self-edge (dropped after resolution, step 3)
/// - An exact duplicate of the valid edge (deduplicated, step 4)
///
/// The lenient parser and normalisation layer handle all four invalid
/// edges and commit only the valid one. The job completes with
/// `success` outcome.
///
/// Theme: Canopy's authentication subsystem — the LLM hallucinates
/// some relation edges that don't correspond to real candidates.
#[tokio::test]
async fn test_relation_normalisation_drops_invalid_edges() {
    let mut harness = TestHarness::init(|setup| {
        // OpenAI triage exercises the OpenAI envelope abstraction.
        setup.config(|c| {
            c.inference.triage.provider = ProviderKind::OpenAi;
            c.inference.triage.api_key =
                Some("sk-e2e-000000".parse().expect("test fixture is valid"));
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
                    .edge(relate(0, "supports", 1))
                    // Skipped during lenient parsing: unknown relation type.
                    .edge(relate(0, "unexpected", 2))
                    // Dropped (step 2): batch index 99 does not exist.
                    .edge(relate(99, "contradicts", 0))
                    // Dropped (step 3): self-edge after resolution.
                    .edge(relate(0, "derived_from", 0))
                    // Dropped (step 4): exact duplicate of the valid edge.
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
