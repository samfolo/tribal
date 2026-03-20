use serde_json::json;
use tribal_domain::KnowledgeKind;
use tribal_test_utils::item;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, duplicate, intra_batch, novel},
    server::TestHarness,
    tool_call::tool_result_json,
};

#[tokio::test]
async fn test_ingest_pipeline_end_to_end() {
    let mut harness = TestHarness::init(|setup| {
        setup.graph(|g| {
            g.as_principal("default")
                .for_project("test-project", |store| {
                    store.add_item(
                        "existing",
                        item(
                            KnowledgeKind::Fact,
                            "Pre-existing item for duplicate matching",
                        ),
                    );
                })
        });
    })
    .await;

    let existing_id = harness.label("existing");

    // -- Mount mocks ----------------------------------------------------------

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "fact",
                            "Rust ensures memory safety without a garbage collector",
                        )
                        .tags(&["rust", "memory-safety"]),
                    )
                    .candidate(
                        candidate(
                            "heuristic",
                            "Use lifetimes to express ownership relationships",
                        )
                        .tags(&["rust", "lifetimes"]),
                    )
                    .build(),
            );
        })
        .await;

    harness
        .mount_triage(|m| {
            m.on_content_repeat_last("memory safety", &[novel().build().into()]);
            m.on_content_repeat_last("lifetimes", &[duplicate(existing_id).build().into()]);
        })
        .await;

    harness
        .mount_relation(|m| {
            m.respond(
                RelationFixture::builder()
                    .edge(intra_batch(0, "supports", 1))
                    .build(),
            );
        })
        .await;

    // -- Ingest ---------------------------------------------------------------

    let ingest_result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "Rust ensures memory safety without a garbage collector. \
                            Use lifetimes to express ownership relationships."
            }),
        )
        .await;
    assert_success!(ingest_result);

    let ingest_json = tool_result_json(&ingest_result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id in response");

    // -- Wait for pipeline completion -----------------------------------------

    harness.expect_completion(job_id).await;

    // -- Verify via discover --------------------------------------------------

    let discover_result = harness
        .call_tool("tribal_discover", json!({ "query": "memory safety" }))
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"].as_array().expect("items array");

    let novel_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("memory safety"))
        })
        .expect("novel item should appear in discover results");
    let novel_item_id = novel_item["item"]["id"].as_str().expect("novel item id");

    // -- Verify via get_item --------------------------------------------------

    let get_result = harness
        .call_tool("tribal_get_item", json!({ "item_ids": [novel_item_id] }))
        .await;
    assert_success!(get_result);

    let get_json = tool_result_json(&get_result);
    let fetched_content = get_json["items"][novel_item_id]["item"]["content"]
        .as_str()
        .expect("content field in get_item response");
    assert!(
        fetched_content.contains("memory safety"),
        "expected content to contain 'memory safety', got: {fetched_content}",
    );

    // -- Verify duplicate was not inserted as a new item ----------------------

    let duplicate_items: Vec<_> = items
        .iter()
        .filter(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("lifetimes"))
        })
        .collect();
    assert!(
        duplicate_items.is_empty(),
        "duplicate candidate should not appear as a new knowledge item",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
