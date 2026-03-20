use serde_json::json;
use tribal_domain::KnowledgeKind;
use tribal_test_utils::item;

use crate::harness::{
    assertions::assert_success,
    fixtures::{ExtractionFixture, RelationFixture, candidate, duplicate, intra_batch, novel},
    server::TestHarness,
    tool_call::tool_result_json,
};

/// Verifies the full ingest pipeline: extraction produces two
/// candidates, triage classifies one as novel and one as a duplicate
/// of a seeded item, relations are committed, and the novel item is
/// discoverable while the duplicate is not inserted as a new item.
///
/// Theme: Canopy's deployment infrastructure — a canary analysis
/// fact is novel, while a blue-green deployment fact duplicates
/// existing knowledge.
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
                            "Canopy uses blue-green deployments to achieve \
                             zero-downtime releases for the collaboration service",
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
                            "The deployment pipeline runs canary analysis for 15 \
                             minutes before promoting to full traffic",
                        )
                        .tags(&["deployment", "canary"]),
                    )
                    .candidate(
                        candidate(
                            "fact",
                            "Blue-green deployment strategy enables zero-downtime \
                             releases for the collaboration service",
                        )
                        .tags(&["deployment", "blue-green"]),
                    )
                    .build(),
            );
        })
        .await;

    harness
        .mount_triage(|m| {
            m.on_content_repeat_last("canary analysis for 15 minutes", &[novel().build().into()]);
            m.on_content_repeat_last(
                "Blue-green deployment strategy",
                &[duplicate(existing_id).build().into()],
            );
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
                "content": "The deployment pipeline runs canary analysis before \
                            promoting. Blue-green deployments enable zero-downtime."
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
        .call_tool("tribal_discover", json!({ "query": "deployment canary" }))
        .await;
    assert_success!(discover_result);

    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"].as_array().expect("items array");

    let novel_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("canary analysis"))
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
        fetched_content.contains("canary analysis"),
        "expected content to contain 'canary analysis', got: {fetched_content}",
    );

    // -- Verify duplicate was not inserted as a new item ----------------------

    let has_duplicate = items.iter().any(|i| {
        i["item"]["content"]
            .as_str()
            .is_some_and(|c| c.contains("Blue-green deployment strategy"))
    });
    if has_duplicate {
        let all_contents: Vec<_> = items
            .iter()
            .filter_map(|i| i["item"]["content"].as_str())
            .collect();
        let ctx = harness.diagnostic_context();
        let diagnostic = ctx.format_failure(job_id, "completed", "success").await;
        panic!(
            "duplicate candidate should not appear as a new knowledge item\n\n\
             Discover returned {} items:\n{:#?}\n\n{diagnostic}",
            items.len(),
            all_contents,
        );
    }

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
