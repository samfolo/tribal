use serde_json::json;
use tribal_domain::{KnowledgeKind, ProviderKind, RelationKind};
use tribal_test_utils::item;

use crate::harness::{
    assertions::assert_success, server::TestHarness, tool_call::tool_result_json,
};

/// Verifies that discover and explore correctly reflect standing
/// scores and superseded-item exclusion.
///
/// Theme: Canopy's event replay snapshot subsystem evolved through
/// several iterations — the knowledge graph captures the full arc:
///
/// - **A** (fact): local disk snapshots for sub-millisecond access
/// - **B** (fact, supersedes A): migrated to S3 after the February incident
/// - **C** (fact, supports B): S3 snapshots reduced P99 from 2.1s to 340ms
/// - **D** (heuristic, contradicts C): cold-start latency increased 3x
///
/// Discover should exclude A (superseded) by default, include it
/// with `include_superseded`, and report correct standing counts
/// on each item.
#[expect(
    clippy::too_many_lines,
    reason = "end-to-end scenario reads as one linear narrative"
)]
#[tokio::test]
async fn test_standing_and_supersession() {
    let mut harness = TestHarness::init(|setup| {
        // Anthropic relation exercises the Anthropic envelope abstraction.
        setup.config(|c| {
            c.inference.relation.provider = ProviderKind::Anthropic;
            c.inference.relation.api_key =
                Some("sk-ant-e2e-000000".parse().expect("test fixture is valid"));
        });

        setup.graph(|g| {
            g.as_principal("default")
                .for_project("test-project", |store| {
                    store
                        .add_item(
                            "a",
                            item(
                                KnowledgeKind::Fact,
                                "Canopy stores event replay snapshots on local disk for \
                                 sub-millisecond access during document reconstruction",
                            ),
                        )
                        .add_item(
                            "b",
                            item(
                                KnowledgeKind::Fact,
                                "After the February incident, snapshot storage was \
                                 migrated from local disk to S3 to survive instance \
                                 termination",
                            ),
                        )
                        .add_item(
                            "c",
                            item(
                                KnowledgeKind::Fact,
                                "S3-backed snapshots reduced event replay P99 from 2.1s \
                                 to 340ms, validating the migration decision",
                            ),
                        )
                        .add_item(
                            "d",
                            item(
                                KnowledgeKind::Heuristic,
                                "Cold-start latency increased by 3x with S3 snapshots \
                                 because the first replay must fetch over the network",
                            ),
                        );
                })
                .relate("b", RelationKind::Supersedes, "a")
                .relate("c", RelationKind::Supports, "b")
                .relate("d", RelationKind::Contradicts, "c")
                .commit_relations("snapshot-arc")
        });
    })
    .await;

    let id_a = harness.label("a").to_owned();
    let id_b = harness.label("b").to_owned();
    let id_c = harness.label("c").to_owned();
    let id_d = harness.label("d").to_owned();

    // -- Discover (default: exclude superseded) -------------------------------

    let result = harness
        .call_tool(
            "tribal_discover",
            json!({
                "query": "event replay snapshots",
                "include_standing": true,
            }),
        )
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    let items = discover_json["items"].as_array().expect("items array");

    let item_ids: Vec<&str> = items
        .iter()
        .filter_map(|i| i["item"]["id"].as_str())
        .collect();

    assert!(
        !item_ids.contains(&id_a.as_str()),
        "superseded item A should be excluded by default; got {item_ids:?}",
    );
    assert_eq!(
        item_ids.len(),
        3,
        "should return B, C, D (excluding superseded A); got {item_ids:?}",
    );

    // Check B's standing: 1 supporting (C), 0 contradicting.
    let item_b_entry = items
        .iter()
        .find(|i| i["item"]["id"].as_str() == Some(&id_b))
        .expect("item B in results");
    assert_eq!(
        item_b_entry["standing"]["supporting_count"].as_u64(),
        Some(1),
        "B should have 1 supporting item (C)",
    );
    assert_eq!(
        item_b_entry["standing"]["contradicting_count"].as_u64(),
        Some(0),
        "B should have 0 contradicting items",
    );

    // Check C's standing: 0 supporting, 1 contradicting (D).
    let item_c_entry = items
        .iter()
        .find(|i| i["item"]["id"].as_str() == Some(&id_c))
        .expect("item C in results");
    assert_eq!(
        item_c_entry["standing"]["contradicting_count"].as_u64(),
        Some(1),
        "C should have 1 contradicting item (D)",
    );

    // -- Discover with include_superseded -------------------------------------

    let result = harness
        .call_tool(
            "tribal_discover",
            json!({
                "query": "event replay snapshots",
                "include_standing": true,
                "include_superseded": true,
            }),
        )
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    let items = discover_json["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        4,
        "with include_superseded, all four items should appear",
    );

    // Check A's standing: superseded_by should be B.
    let item_a_entry = items
        .iter()
        .find(|i| i["item"]["id"].as_str() == Some(&id_a))
        .expect("item A in results with include_superseded");
    assert_eq!(
        item_a_entry["standing"]["superseded_by"].as_str(),
        Some(id_b.as_str()),
        "A's standing should show superseded_by = B",
    );

    // -- Explore from C (both directions) -------------------------------------

    let result = harness
        .call_tool(
            "tribal_explore",
            json!({
                "item_id": &id_c,
                "direction": "both",
                "depth": 1,
            }),
        )
        .await;
    assert_success!(result);

    let explore_json = tool_result_json(&result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");

    // C supports B → outbound edge to B.
    let edge_to_b = related
        .iter()
        .find(|r| r["item"]["id"].as_str() == Some(&id_b))
        .expect("B should appear in C's neighbourhood");
    assert_eq!(edge_to_b["relation_type"].as_str(), Some("supports"),);
    assert_eq!(edge_to_b["relation_direction"].as_str(), Some("outbound"),);

    // D contradicts C → inbound edge from D.
    let edge_from_d = related
        .iter()
        .find(|r| r["item"]["id"].as_str() == Some(&id_d))
        .expect("D should appear in C's neighbourhood");
    assert_eq!(edge_from_d["relation_type"].as_str(), Some("contradicts"),);
    assert_eq!(edge_from_d["relation_direction"].as_str(), Some("inbound"),);

    // -- Verify via get_item --------------------------------------------------

    let result = harness
        .call_tool(
            "tribal_get_item",
            json!({
                "item_ids": [&id_a, &id_b],
                "include_standing": true,
            }),
        )
        .await;
    assert_success!(result);

    let get_json = tool_result_json(&result);
    assert_eq!(
        get_json["items"][&id_a]["standing"]["superseded_by"].as_str(),
        Some(id_b.as_str()),
        "get_item should confirm A is superseded by B",
    );
    assert_eq!(
        get_json["items"][&id_b]["standing"]["supporting_count"].as_u64(),
        Some(1),
        "get_item should confirm B has 1 supporting item",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
}
