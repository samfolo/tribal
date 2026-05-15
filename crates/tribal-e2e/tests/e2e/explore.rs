use serde_json::json;
use tribal_config::ProviderKind;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, novel, relate},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies graph traversal via `tribal_explore`: depth control,
/// direction filtering, and relation type filtering.
///
/// Theme: Canopy's caching layer — a write-through cache fact
/// supports a debugging heuristic, which contradicts a cache
/// invalidation procedure.
#[tokio::test]
async fn test_explore_graph_traversal() {
    let mut harness = TestHarness::init(|setup| {
        // Anthropic extraction exercises the Anthropic envelope abstraction.
        setup.config(|c| {
            c.inference.extraction.provider = ProviderKind::Anthropic;
            c.inference.extraction.api_key =
                Some("sk-ant-e2e-000000".parse().expect("test fixture is valid"));
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
                            "Canopy's session store uses a write-through cache to \
                             reduce database load during peak collaboration sessions",
                        )
                        .tags(&["caching", "session-store"]),
                    )
                    .candidate(
                        candidate(
                            "heuristic",
                            "When cache hit rate drops below 90%, investigate \
                             connection pool saturation before adding cache nodes",
                        )
                        .tags(&["caching", "debugging"]),
                    )
                    .candidate(
                        candidate(
                            "procedure",
                            "To invalidate the session cache, publish a cache-bust \
                             event to the shared message bus rather than clearing \
                             directly",
                        )
                        .tags(&["caching", "invalidation"]),
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
                    .edge(relate(0, "supports", 1))
                    .edge(relate(1, "contradicts", 2))
                    .build(),
            );
        })
        .await;

    // -- Ingest and wait for pipeline completion ------------------------------

    let ingest_result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "Canopy's session store uses a write-through cache. \
                            When cache hit rate drops, investigate pool saturation. \
                            To invalidate, publish a cache-bust event."
            }),
        )
        .await;
    assert_success!(ingest_result);

    let job_id = tool_result_json(&ingest_result)["job_id"]
        .as_str()
        .expect("job_id")
        .to_owned();

    harness.expect_completion(&job_id).await;

    // -- Discover items to get their IDs --------------------------------------

    let discover_result = harness
        .call_tool(
            "tribal_discover",
            json!({ "query": "session cache invalidation" }),
        )
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"].as_array().expect("items array");
    assert!(
        items.len() >= 3,
        "expected at least 3 items, got {}",
        items.len()
    );

    let find_id = |substring: &str| -> String {
        items
            .iter()
            .find(|i| {
                i["item"]["content"]
                    .as_str()
                    .is_some_and(|c| c.to_lowercase().contains(substring))
            })
            .unwrap_or_else(|| panic!("no item containing '{substring}'"))["item"]["id"]
            .as_str()
            .expect("item id")
            .to_owned()
    };

    let id_a = find_id("write-through cache");
    let id_b = find_id("cache hit rate");
    let id_c = find_id("cache-bust event");

    // -- Explore depth 1 from A → should find B ------------------------------

    let result = harness
        .call_tool(
            "tribal_explore",
            json!({ "item_id": &id_a, "depth": 1, "direction": "both" }),
        )
        .await;
    assert_success!(result);

    let explore_json = tool_result_json(&result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let related_ids: Vec<&str> = related
        .iter()
        .filter_map(|r| r["item"]["id"].as_str())
        .collect();
    assert!(
        related_ids.contains(&id_b.as_str()),
        "depth-1 from A should include B; got {related_ids:?}",
    );

    // -- Explore depth 2 from A → should find B and C ------------------------

    let result = harness
        .call_tool(
            "tribal_explore",
            json!({ "item_id": &id_a, "depth": 2, "direction": "both" }),
        )
        .await;
    assert_success!(result);

    let explore_json = tool_result_json(&result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let related_ids: Vec<&str> = related
        .iter()
        .filter_map(|r| r["item"]["id"].as_str())
        .collect();
    assert!(
        related_ids.contains(&id_b.as_str()),
        "depth-2 from A should include B; got {related_ids:?}",
    );
    assert!(
        related_ids.contains(&id_c.as_str()),
        "depth-2 from A should include C; got {related_ids:?}",
    );

    // -- Explore with direction filter (outbound from A) ----------------------

    let result = harness
        .call_tool(
            "tribal_explore",
            json!({ "item_id": &id_a, "direction": "outbound", "depth": 2 }),
        )
        .await;
    assert_success!(result);

    let explore_json = tool_result_json(&result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let outbound_ids: Vec<&str> = related
        .iter()
        .filter_map(|r| r["item"]["id"].as_str())
        .collect();
    assert!(
        outbound_ids.contains(&id_b.as_str()),
        "outbound from A should include B; got {outbound_ids:?}",
    );

    // -- Explore with relation_types filter -----------------------------------

    let result = harness
        .call_tool(
            "tribal_explore",
            json!({
                "item_id": &id_a,
                "relation_types": ["supports"],
                "depth": 2,
                "direction": "both",
            }),
        )
        .await;
    assert_success!(result);

    let explore_json = tool_result_json(&result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let supports_ids: Vec<&str> = related
        .iter()
        .filter_map(|r| r["item"]["id"].as_str())
        .collect();
    assert!(
        supports_ids.contains(&id_b.as_str()),
        "supports filter should include B; got {supports_ids:?}",
    );
    assert!(
        !supports_ids.contains(&id_c.as_str()),
        "supports filter should exclude C (contradicts edge); got {supports_ids:?}",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
