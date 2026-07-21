use serde_json::json;
use tribal_db::{JobRepository, PgJobRepository, PgProjectRepository, ProjectRepository};
use tribal_domain::{GitRemote, JobId};
use tribal_test_utils::a_new_project;

use crate::harness::{
    assertions::assert_success,
    server::{TestHarness, seed},
    tool_call::tool_result_json,
};

/// Verifies the session context lifecycle: a fresh client ingests into
/// System, setting a project changes the ingestion default,
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
                .insert_git(
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
                .insert_git(
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

            seed.label("project_1", &project_1.id());
            seed.label("project_2", &project_2.id());
        });
    })
    .await;

    let project_1_id = harness.label("project_1");
    let project_2_id = harness.label("project_2");

    // -- An omitted target falls back to System -------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({ "content": "Canopy API rate limiting configuration" }),
        )
        .await;
    assert_success!(result);
    let job_id: JobId = tool_result_json(&result)["job_id"]
        .as_str()
        .expect("ingest response job id")
        .parse()
        .expect("valid job id");
    let mut connection = harness
        .pool
        .acquire()
        .await
        .expect("acquire assertion connection");
    let job = PgJobRepository
        .find_by_id(&mut connection, job_id)
        .await
        .expect("find System-targeted job");
    let system = PgProjectRepository
        .find_system(&mut connection)
        .await
        .expect("find System project");
    assert_eq!(job.project_id(), system.id());

    // -- The session binds to canopy-api --------------------------------------

    let result = harness
        .call_tool("tribal_set_context", json!({ "project_id": project_1_id }))
        .await;
    assert_success!(result);

    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_1_id),
        "set_context reflects canopy-api",
    );

    // -- Ingest uses the active session project -------------------------------

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({ "content": "Canopy API rate limiting configuration" }),
        )
        .await;
    assert_success!(result);

    // -- Discovery reflects canopy-api ----------------------------------------

    let result = harness
        .call_tool("tribal_discover", json!({ "query": "rate limiting" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_1_id),
        "discover scopes to canopy-api",
    );

    // -- The session switches to canopy-dashboard -----------------------------

    let result = harness
        .call_tool("tribal_set_context", json!({ "project_id": project_2_id }))
        .await;
    assert_success!(result);

    let ctx_json = tool_result_json(&result);
    assert_eq!(
        ctx_json["project"]["id"].as_str(),
        Some(project_2_id),
        "set_context reflects canopy-dashboard",
    );

    // -- Discovery follows the context switch ---------------------------------

    let result = harness
        .call_tool("tribal_discover", json!({ "query": "rate limiting" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    assert_eq!(
        discover_json["applied_project_id"].as_str(),
        Some(project_2_id),
        "discover scopes to canopy-dashboard after the context switch",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
}
