use serde_json::json;
use tribal_db::{PgProjectRepository, ProjectRepository};
use tribal_domain::GitRemote;
use tribal_test_utils::a_new_project;

use crate::harness::assertions::{assert_error, assert_success};
use crate::harness::macros::seed;
use crate::harness::server::TestHarness;
use crate::harness::tool_call::tool_result_json;

#[tokio::test]
async fn test_session_context_lifecycle() {
    let harness = TestHarness::init(|setup| {
        setup.principal_key("e2e-session-principal");
        setup.no_project();

        seed!(setup, |seed| {
            let project_1 = PgProjectRepository
                .insert(
                    seed.conn(),
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
                    seed.conn(),
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

            seed.label("project_1", project_1.id());
            seed.label("project_2", project_2.id());
        });
    })
    .await;

    let project_1_id = harness.label("project_1");
    let project_2_id = harness.label("project_2");

    // -- Step 1: ingest without a project → error -----------------------------

    let result = harness
        .call_tool("tribal_ingest", json!({ "content": "test content" }))
        .await;
    assert_error!(result);

    // -- Step 2: set context to project 1 -------------------------------------

    let result = harness
        .call_tool("tribal_set_context", json!({ "project_id": project_1_id }))
        .await;
    assert_success!(result);

    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_1_id),
        "set_context should reflect project 1",
    );

    // -- Step 3: ingest now succeeds (session project active) -----------------

    let result = harness
        .call_tool("tribal_ingest", json!({ "content": "test content" }))
        .await;
    assert_success!(result);

    // -- Step 4: discover reflects project 1 ----------------------------------

    let result = harness
        .call_tool("tribal_discover", json!({ "query": "test" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_1_id),
        "discover should scope to project 1",
    );

    // -- Step 5: switch to project 2 ------------------------------------------

    let result = harness
        .call_tool("tribal_set_context", json!({ "project_id": project_2_id }))
        .await;
    assert_success!(result);

    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_2_id),
        "set_context should reflect project 2",
    );

    // -- Step 6: discover now scoped to project 2 -----------------------------

    let result = harness
        .call_tool("tribal_discover", json!({ "query": "test" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_2_id),
        "discover should scope to project 2 after context switch",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
