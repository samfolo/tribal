mod harness;

use serde_json::json;
use tribal_db::{
    KnowledgeItemRepository, PgKnowledgeItemRepository, PgPrincipalRepository, PgProjectRepository,
    PrincipalRepository, ProjectRepository,
};
use tribal_test_utils::{
    a_new_knowledge_item, a_new_principal, a_new_project, serial_lock, test_context,
};

use harness::{
    config::test_config,
    server::build_harness,
    tool_call::{assert_success, call_tool, tool_result_json},
    wiremock_setup::{
        EMBEDDING_MODEL, EXTRACTION_MODEL, SMALL_MODEL, duplicate_triage_response,
        extraction_response, mount_chat_mock, mount_chat_sequence, mount_embed_mock,
        mount_tags_mock, novel_triage_response, relation_response,
    },
};

#[tokio::test]
async fn test_ingest_pipeline_end_to_end() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    // -- Wiremock servers (one per provider role) -----------------------------
    let embedding_server = wiremock::MockServer::start().await;
    let extraction_server = wiremock::MockServer::start().await;
    let triage_server = wiremock::MockServer::start().await;
    let relation_server = wiremock::MockServer::start().await;

    // -- Clean slate ---------------------------------------------------------
    let mut conn = ctx.pool().acquire().await.expect("acquire connection");
    tribal_test_utils::truncate_all_tables(&mut conn).await;
    drop(conn);

    // -- Infrastructure scaffolding ------------------------------------------
    let mut conn = ctx.pool().acquire().await.expect("acquire connection");

    let project = PgProjectRepository
        .insert(&mut conn, &a_new_project().build())
        .await
        .expect("insert project");

    let principal = PgPrincipalRepository
        .insert(
            &mut conn,
            &a_new_principal()
                .principal_key("e2e-test-principal".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");

    // Pre-insert a knowledge item as the duplicate match target.
    // The triage Duplicate response references this item's ID, and
    // commit_duplicate inserts an ItemObservation with a FK to it.
    let existing_item = PgKnowledgeItemRepository
        .insert(
            &mut conn,
            &a_new_knowledge_item()
                .project_id(project.id())
                .principal_id(principal.id())
                .content("Pre-existing item for duplicate matching".to_owned())
                .build(),
        )
        .await
        .expect("insert existing knowledge item");

    drop(conn);

    // -- Mount wiremock mocks ------------------------------------------------
    mount_tags_mock(&embedding_server, EMBEDDING_MODEL).await;
    mount_embed_mock(&embedding_server).await;

    mount_tags_mock(&extraction_server, EXTRACTION_MODEL).await;
    mount_chat_mock(
        &extraction_server,
        &extraction_response(
            &[
                ("fact", "Rust ensures memory safety without a garbage collector", &["rust", "memory-safety"]),
                ("heuristic", "Use lifetimes to express ownership relationships", &["rust", "lifetimes"]),
            ],
            &[],
        ),
    )
    .await;

    mount_tags_mock(&triage_server, SMALL_MODEL).await;
    mount_chat_sequence(
        &triage_server,
        &[
            novel_triage_response(),
            duplicate_triage_response(&existing_item.id().to_string()),
        ],
    )
    .await;

    mount_tags_mock(&relation_server, SMALL_MODEL).await;
    mount_chat_mock(
        &relation_server,
        &relation_response(&[(0, 1, "supports")]),
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
        "e2e-test-principal",
    )
    .await;

    // -- Ingest --------------------------------------------------------------
    let ingest_result = call_tool(
        &harness.client,
        "tribal_ingest",
        json!({ "content": "Rust ensures memory safety without a garbage collector. Use lifetimes to express ownership relationships." }),
    )
    .await;
    assert_success(&ingest_result);
    let ingest_json = tool_result_json(&ingest_result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id in response");

    // -- Poll until completed ------------------------------------------------
    let mut status = String::new();
    for _ in 0..60 {
        let status_result = call_tool(
            &harness.client,
            "tribal_job_status",
            json!({ "job_id": job_id, "wait_seconds": 2 }),
        )
        .await;
        assert_success(&status_result);
        let status_json = tool_result_json(&status_result);
        status = status_json["status"]
            .as_str()
            .expect("status field")
            .to_owned();
        if status == "completed" || status == "failed" {
            break;
        }
    }
    assert_eq!(status, "completed", "job did not complete");

    // -- Verify via discover -------------------------------------------------
    let discover_result = call_tool(
        &harness.client,
        "tribal_discover",
        json!({ "query": "memory safety" }),
    )
    .await;
    assert_success(&discover_result);
    let discover_json = tool_result_json(&discover_result);
    let items = discover_json["items"].as_array().expect("items array");

    // The novel candidate should be discoverable; the duplicate should not
    // have been inserted as a new knowledge item.
    let novel_item = items
        .iter()
        .find(|i| {
            i["item"]["content"]
                .as_str()
                .is_some_and(|c| c.contains("memory safety"))
        })
        .expect("novel item should appear in discover results");
    let novel_item_id = novel_item["item"]["id"]
        .as_str()
        .expect("novel item id");

    // -- Verify via get_item -------------------------------------------------
    let get_result = call_tool(
        &harness.client,
        "tribal_get_item",
        json!({ "item_ids": [novel_item_id] }),
    )
    .await;
    assert_success(&get_result);
    let get_json = tool_result_json(&get_result);
    let fetched_content = get_json["items"][novel_item_id]["item"]["content"]
        .as_str()
        .expect("content field in get_item response");
    assert!(
        fetched_content.contains("memory safety"),
        "expected content to contain 'memory safety', got: {fetched_content}",
    );

    // -- Verify duplicate was not inserted as a new item ---------------------
    // The duplicate candidate should have created an observation against the
    // existing item, not a new knowledge item.
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

    // -- Shutdown ------------------------------------------------------------
    harness.shutdown().await;
    harness.teardown().await;
}
