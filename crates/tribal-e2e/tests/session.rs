mod harness;

use serde_json::json;
use tribal_db::{PgPrincipalRepository, PgProjectRepository, PrincipalRepository, ProjectRepository};
use tribal_domain::GitRemote;
use tribal_test_utils::{a_new_principal, a_new_project, serial_lock, test_context};

use harness::{
    config::test_config,
    server::build_harness,
    tool_call::{assert_error, assert_success, call_tool, tool_result_json},
    wiremock_setup::{
        EMBEDDING_MODEL, EXTRACTION_MODEL, SMALL_MODEL, mount_embed_mock, mount_tags_mock,
    },
};

#[tokio::test]
async fn test_session_context_lifecycle() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let prompts_dir = tempfile::tempdir().expect("create prompts tempdir");

    // -- Clean slate ---------------------------------------------------------
    let mut conn = ctx.pool().acquire().await.expect("acquire connection");
    tribal_test_utils::truncate_all_tables(&mut conn).await;
    drop(conn);

    // -- Single wiremock server (no pipeline work) ---------------------------
    let mock_server = wiremock::MockServer::start().await;
    mount_tags_mock(&mock_server, EMBEDDING_MODEL).await;
    mount_tags_mock(&mock_server, EXTRACTION_MODEL).await;
    mount_tags_mock(&mock_server, SMALL_MODEL).await;
    mount_embed_mock(&mock_server).await;

    // -- Infrastructure scaffolding ------------------------------------------
    let mut conn = ctx.pool().acquire().await.expect("acquire connection");

    let project_1 = PgProjectRepository
        .insert(
            &mut conn,
            &a_new_project()
                .name("project-one".to_owned())
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    "test/project-one",
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project 1");

    let project_2 = PgProjectRepository
        .insert(
            &mut conn,
            &a_new_project()
                .name("project-two".to_owned())
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    "test/project-two",
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert project 2");

    PgPrincipalRepository
        .insert(
            &mut conn,
            &a_new_principal()
                .principal_key("e2e-session-principal".to_owned())
                .build(),
        )
        .await
        .expect("insert principal");

    drop(conn);

    // -- Start server without a project (session starts empty) ---------------
    let uri = mock_server.uri();
    let config = test_config(
        ctx.database_url(),
        &uri,
        &uri,
        &uri,
        &uri,
        prompts_dir.path().to_str().expect("prompts dir to str"),
    );

    let mut harness = build_harness(config, prompts_dir, None, "e2e-session-principal").await;

    let project_1_id = project_1.id().to_string();
    let project_2_id = project_2.id().to_string();

    // -- Step 1: ingest without a project → error ----------------------------
    let result = call_tool(
        &harness.client,
        "tribal_ingest",
        json!({ "content": "test content" }),
    )
    .await;
    assert_error(&result);

    // -- Step 2: set context to project 1 ------------------------------------
    let result = call_tool(
        &harness.client,
        "tribal_set_context",
        json!({ "project_id": &project_1_id }),
    )
    .await;
    assert_success(&result);
    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_1_id.as_str()),
        "set_context should reflect project 1",
    );

    // -- Step 3: ingest now succeeds (session project active) ----------------
    let result = call_tool(
        &harness.client,
        "tribal_ingest",
        json!({ "content": "test content" }),
    )
    .await;
    assert_success(&result);

    // -- Step 4: discover reflects project 1 ---------------------------------
    let result = call_tool(
        &harness.client,
        "tribal_discover",
        json!({ "query": "test" }),
    )
    .await;
    assert_success(&result);
    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_1_id.as_str()),
        "discover should scope to project 1",
    );

    // -- Step 5: switch to project 2 -----------------------------------------
    let result = call_tool(
        &harness.client,
        "tribal_set_context",
        json!({ "project_id": &project_2_id }),
    )
    .await;
    assert_success(&result);
    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_2_id.as_str()),
        "set_context should reflect project 2",
    );

    // -- Step 6: discover now scoped to project 2 ----------------------------
    let result = call_tool(
        &harness.client,
        "tribal_discover",
        json!({ "query": "test" }),
    )
    .await;
    assert_success(&result);
    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_2_id.as_str()),
        "discover should scope to project 2 after context switch",
    );

    // -- Shutdown ------------------------------------------------------------
    harness.shutdown().await;
    harness.teardown().await;
}
