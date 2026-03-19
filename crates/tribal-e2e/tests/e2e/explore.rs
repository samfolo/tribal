use serde_json::json;

use crate::harness::assertions::assert_success;
use crate::harness::fixtures::{
    ExtractionFixture, RelationFixture, candidate, intra_batch, novel,
};
use crate::harness::server::TestHarness;
use crate::harness::tool_call::tool_result_json;

#[tokio::test]
async fn test_explore_graph_traversal() {
    let harness = TestHarness::init(|setup| {
        setup.principal_key("e2e-explore-principal");
    })
    .await;

    // -- Mount mocks ----------------------------------------------------------

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate("fact", "Ownership rules prevent data races in Rust")
                            .tags(&["rust", "ownership"]),
                    )
                    .candidate(
                        candidate("heuristic", "Borrow checker enforces aliasing discipline")
                            .tags(&["rust", "borrowing"]),
                    )
                    .candidate(
                        candidate("procedure", "Use Arc for shared ownership across threads")
                            .tags(&["rust", "concurrency"]),
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
                    .edge(intra_batch(0, 1, "supports"))
                    .edge(intra_batch(1, 2, "contradicts"))
                    .build(),
            );
        })
        .await;

    // -- Ingest and wait for pipeline completion ------------------------------

    let ingest_result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "Ownership rules prevent data races. \
                            Borrow checker enforces aliasing. \
                            Use Arc for shared ownership."
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
            json!({ "query": "ownership borrowing concurrency" }),
        )
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"]
        .as_array()
        .expect("items array");
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

    let id_a = find_id("ownership rules");
    let id_b = find_id("borrow checker");
    let id_c = find_id("arc for shared");

    // -- Explore depth 1 from A → should find B ------------------------------

    let result = harness
        .call_tool(
            "tribal_explore",
            json!({ "item_id": &id_a, "depth": 1, "direction": "both" }),
        )
        .await;
    assert_success!(result);

    let related = tool_result_json(&result)["related_items"]
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

    let related = tool_result_json(&result)["related_items"]
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

    let related = tool_result_json(&result)["related_items"]
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

    let related = tool_result_json(&result)["related_items"]
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
