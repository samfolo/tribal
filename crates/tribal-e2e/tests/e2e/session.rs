use serde_json::json;
use tribal_db::{PgProjectRepository, ProjectRepository};
use tribal_domain::GitRemote;
use tribal_test_utils::a_new_project;

use crate::harness::{
    assertions::{assert_error, assert_success},
    server::{TestHarness, seed},
    tool_call::tool_result_json,
};

/// Verifies the session context lifecycle: a fresh client cannot
/// ingest without a project, setting a project enables ingestion,
/// and switching projects updates the discover scope.
///
/// Theme: two Canopy teams — API and dashboard — operating in
/// separate project contexts.
#[tokio::test]
async fn test_session_context_lifecycle() {
    let mut harness = TestHarness::init(|setup| {
        setup.no_project();

        seed!(setup, |seed| {
            let project_1 = PgProjectRepository
                .insert(
                    seed.conn(),
                    &a_new_project()
                        .name("canopy-api".to_owned())
                        .git_remote(GitRemote::from_parts(
                            "github.com",
                            "meridian/canopy-api",
                            None,
                        ))
                        .build(),
                )
                .await
                .expect("insert canopy-api project");

            let project_2 = PgProjectRepository
                .insert(
                    seed.conn(),
                    &a_new_project()
                        .name("canopy-dashboard".to_owned())
                        .git_remote(GitRemote::from_parts(
                            "github.com",
                            "meridian/canopy-dashboard",
                            None,
                        ))
                        .build(),
                )
                .await
                .expect("insert canopy-dashboard project");

            seed.label("project_1", project_1.id());
            seed.label("project_2", project_2.id());
        });
    })
    .await;

    let project_1_id = harness.label("project_1");
    let project_2_id = harness.label("project_2");

    // -- Step 1: ingest without a project → error -----------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({ "content": "Canopy API rate limiting configuration" }),
        )
        .await;
    assert_error!(result);

    // -- Step 2: set context to canopy-api ------------------------------------

    let result = harness
        .call_tool("tribal_set_context", json!({ "project_id": project_1_id }))
        .await;
    assert_success!(result);

    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_1_id),
        "set_context should reflect canopy-api",
    );

    // -- Step 3: ingest now succeeds (session project active) -----------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({ "content": "Canopy API rate limiting configuration" }),
        )
        .await;
    assert_success!(result);

    // -- Step 4: discover reflects canopy-api ----------------------------------

    let result = harness
        .call_tool("tribal_discover", json!({ "query": "rate limiting" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_1_id),
        "discover should scope to canopy-api",
    );

    // -- Step 5: switch to canopy-dashboard -----------------------------------

    let result = harness
        .call_tool("tribal_set_context", json!({ "project_id": project_2_id }))
        .await;
    assert_success!(result);

    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_2_id),
        "set_context should reflect canopy-dashboard",
    );

    // -- Step 6: discover now scoped to canopy-dashboard ----------------------

    let result = harness
        .call_tool("tribal_discover", json!({ "query": "rate limiting" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_2_id),
        "discover should scope to canopy-dashboard after context switch",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
