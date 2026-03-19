use serde_json::json;
use tribal_db::{
    PgPrincipalRepository, PgProjectRepository, PrincipalRepository, ProjectRepository,
};
use tribal_test_utils::{a_new_principal, a_new_project, serial_lock, test_context};

use crate::harness::{
    config::test_config,
    server::build_harness,
    tool_call::{assert_success, call_tool, tool_result_json},
    wiremock_setup::{
        EMBEDDING_MODEL, EXTRACTION_MODEL, SMALL_MODEL, extraction_response, mount_chat_mock,
        mount_chat_sequence, mount_embed_mock, mount_tags_mock, novel_triage_response,
        relation_response,
    },
};

#[tokio::test]
async fn test_explore_graph_traversal() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let pool = ctx.create_pool().await.expect("create per-test pool");
    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    // -- Clean slate ---------------------------------------------------------
    let mut conn = pool.acquire().await.expect("acquire connection");
    tribal_test_utils::truncate_all_tables(&mut conn).await;
    drop(conn);

    // -- Wiremock servers (one per provider role) -----------------------------
    let embedding_server = wiremock::MockServer::start().await;
    let extraction_server = wiremock::MockServer::start().await;
    let triage_server = wiremock::MockServer::start().await;
    let relation_server = wiremock::MockServer::start().await;

    // -- Infrastructure scaffolding ------------------------------------------
    let mut conn = pool.acquire().await.expect("acquire connection");

    let project = PgProjectRepository
        .insert(&mut conn, &a_new_project().build())
        .await
        .expect("insert project");

    PgPrincipalRepository
        .insert(
            &mut conn,
            &a_new_principal()
                .principal_key("e2e-explore-principal".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");

    drop(conn);

    // -- Mount wiremock mocks ------------------------------------------------
    // Extraction: 3 candidates (A, B, C)
    mount_tags_mock(&embedding_server, EMBEDDING_MODEL).await;
    mount_embed_mock(&embedding_server).await;

    mount_tags_mock(&extraction_server, EXTRACTION_MODEL).await;
    mount_chat_mock(
        &extraction_server,
        &extraction_response(
            &[
                (
                    "fact",
                    "Ownership rules prevent data races in Rust",
                    &["rust", "ownership"],
                ),
                (
                    "heuristic",
                    "Borrow checker enforces aliasing discipline",
                    &["rust", "borrowing"],
                ),
                (
                    "procedure",
                    "Use Arc for shared ownership across threads",
                    &["rust", "concurrency"],
                ),
            ],
            &[],
        ),
    )
    .await;

    // Triage: all Novel (3 calls)
    mount_tags_mock(&triage_server, SMALL_MODEL).await;
    mount_chat_sequence(
        &triage_server,
        &[
            novel_triage_response(),
            novel_triage_response(),
            novel_triage_response(),
        ],
    )
    .await;

    // Relation: A→B (supports), B→C (contradicts)
    mount_tags_mock(&relation_server, SMALL_MODEL).await;
    mount_chat_mock(
        &relation_server,
        &relation_response(&[(0, 1, "supports"), (1, 2, "contradicts")]),
    )
    .await;

    // -- Start server and connect client -------------------------------------
    let config = test_config(
        ctx.database_url(),
        &embedding_server.uri(),
        &extraction_server.uri(),
        &triage_server.uri(),
        &relation_server.uri(),
        prompts_dir.path().to_str().expect("prompts dir to str"),
    );

    let mut harness = build_harness(
        config,
        prompts_dir,
        Some(project.id().to_string()),
        "e2e-explore-principal",
        pool,
    )
    .await;

    // -- Ingest and wait for pipeline completion -----------------------------
    let ingest_result = call_tool(
        &harness.client,
        "tribal_ingest",
        json!({ "content": "Ownership rules prevent data races. Borrow checker enforces aliasing. Use Arc for shared ownership." }),
    )
    .await;
    assert_success(&ingest_result);
    let job_id = tool_result_json(&ingest_result)["job_id"]
        .as_str()
        .expect("job_id")
        .to_owned();

    let mut status = String::new();
    for _ in 0..60 {
        let result = call_tool(
            &harness.client,
            "tribal_job_status",
            json!({ "job_id": &job_id, "wait_seconds": 2 }),
        )
        .await;
        assert_success(&result);
        status = tool_result_json(&result)["status"]
            .as_str()
            .expect("status")
            .to_owned();
        if status == "completed" || status == "failed" {
            break;
        }
    }
    assert_eq!(status, "completed", "job did not complete");

    // -- Discover items to get their IDs -------------------------------------
    let discover_json = poll_discover(&harness, "ownership borrowing concurrency", 3).await;
    let items = discover_json["items"].as_array().expect("items array");
    assert!(
        items.len() >= 3,
        "expected at least 3 items, got {}",
        items.len()
    );

    // Map content substrings to item IDs.
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

    // -- Explore depth 1 from A (both directions) → should find B ------------
    let result = call_tool(
        &harness.client,
        "tribal_explore",
        json!({ "item_id": &id_a, "depth": 1, "direction": "both" }),
    )
    .await;
    assert_success(&result);
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

    // -- Explore depth 2 from A (both directions) → should find B and C ------
    let result = call_tool(
        &harness.client,
        "tribal_explore",
        json!({ "item_id": &id_a, "depth": 2, "direction": "both" }),
    )
    .await;
    assert_success(&result);
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

    // -- Explore with direction filter (outbound from A) ---------------------
    let result = call_tool(
        &harness.client,
        "tribal_explore",
        json!({ "item_id": &id_a, "direction": "outbound", "depth": 2 }),
    )
    .await;
    assert_success(&result);
    let explore_json = tool_result_json(&result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let outbound_ids: Vec<&str> = related
        .iter()
        .filter_map(|r| r["item"]["id"].as_str())
        .collect();
    // A→B is outbound from A, B→C is outbound from B (reachable at depth 2).
    assert!(
        outbound_ids.contains(&id_b.as_str()),
        "outbound from A should include B; got {outbound_ids:?}",
    );

    // -- Explore with relation_types filter ----------------------------------
    let result = call_tool(
        &harness.client,
        "tribal_explore",
        json!({ "item_id": &id_a, "relation_types": ["supports"], "depth": 2, "direction": "both" }),
    )
    .await;
    assert_success(&result);
    let explore_json = tool_result_json(&result);
    let related = explore_json["related_items"]
        .as_array()
        .expect("related_items");
    let supports_ids: Vec<&str> = related
        .iter()
        .filter_map(|r| r["item"]["id"].as_str())
        .collect();
    // Only A→B is "supports"; B→C is "contradicts" so C should not appear.
    assert!(
        supports_ids.contains(&id_b.as_str()),
        "supports filter should include B; got {supports_ids:?}",
    );
    assert!(
        !supports_ids.contains(&id_c.as_str()),
        "supports filter should exclude C (contradicts edge); got {supports_ids:?}",
    );

    // -- Cleanup (teardown before shutdown — pool closes with the server) ----
    harness.teardown().await;
    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Polls `tribal_discover` until at least `min_items` results appear.
async fn poll_discover(
    harness: &crate::harness::server::TestHarness,
    query: &str,
    min_items: usize,
) -> serde_json::Value {
    for attempt in 0..10 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let result = call_tool(
            &harness.client,
            "tribal_discover",
            json!({ "query": query }),
        )
        .await;
        assert_success(&result);
        let json = tool_result_json(&result);
        if let Some(items) = json["items"].as_array()
            && items.len() >= min_items
        {
            return json;
        }
    }
    panic!(
        "discover returned fewer than {min_items} items after 10 attempts for query: {query}",
    );
}
