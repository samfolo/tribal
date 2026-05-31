use serde_json::json;
use tribal_config::{DEFAULT_EMBEDDING_DIMENSIONS, DEFAULT_EMBEDDING_MODEL};
use tribal_db::{
    EmbeddingRepository, KnowledgeItemRepository, PgEmbeddingRepository, PgKnowledgeItemRepository,
    PgProjectRepository, ProjectRepository,
};
use tribal_domain::GitRemote;
use tribal_test_utils::{
    a_new_embedding, a_new_knowledge_item, a_new_project, ensure_genesis_profile,
};

use crate::harness::{
    assertions::{assert_error, assert_success},
    fixtures::{ExtractionFixture, RelationFixture, candidate, novel},
    mocks::fixed_embedding_vector,
    server::{DEFAULT_PRINCIPAL_KEY, TestHarness, seed},
    tool_call::tool_result_json,
};

/// Verifies that two MCP clients connected to the same server have
/// fully independent session contexts, and that the three-way
/// `project_id` semantics on discover work correctly:
///
/// - absent → session project
/// - explicit ID → filter to that project
/// - null → global (all projects)
///
/// A third project ("canopy-platform") is pre-seeded with knowledge
/// so that the null/global query returns items from multiple projects.
///
/// Theme: three Meridian teams — backend, frontend, platform — using
/// the same Tribal instance without cross-contamination.
#[tokio::test]
async fn test_multi_session_isolation() {
    let mut harness = TestHarness::init(|setup| {
        setup.no_project();

        seed!(setup, |seed| {
            let backend = PgProjectRepository
                .insert(
                    seed.conn(),
                    &a_new_project()
                        .name("canopy-backend".to_owned())
                        .git_remote(GitRemote::from_parts(
                            "github.com",
                            "meridian/canopy-backend",
                            None,
                        ))
                        .build(),
                )
                .await
                .expect("insert backend project");

            let frontend = PgProjectRepository
                .insert(
                    seed.conn(),
                    &a_new_project()
                        .name("canopy-frontend".to_owned())
                        .git_remote(GitRemote::from_parts(
                            "github.com",
                            "meridian/canopy-frontend",
                            None,
                        ))
                        .build(),
                )
                .await
                .expect("insert frontend project");

            let platform = PgProjectRepository
                .insert(
                    seed.conn(),
                    &a_new_project()
                        .name("canopy-platform".to_owned())
                        .git_remote(GitRemote::from_parts(
                            "github.com",
                            "meridian/canopy-platform",
                            None,
                        ))
                        .build(),
                )
                .await
                .expect("insert platform project");

            // Pre-seed a knowledge item in the platform project so the
            // global query returns items from more than one project.
            let principal_id = seed.principal_id();
            let platform_item = PgKnowledgeItemRepository
                .insert(
                    seed.conn(),
                    &a_new_knowledge_item()
                        .project_id(platform.id())
                        .principal_id(principal_id)
                        .content(
                            "The canopy-platform shared library provides a circuit \
                             breaker implementation used by all backend services"
                                .to_owned(),
                        )
                        .build(),
                )
                .await
                .expect("insert platform knowledge item");

            let profile_id = ensure_genesis_profile(
                seed.conn(),
                DEFAULT_EMBEDDING_MODEL,
                DEFAULT_EMBEDDING_DIMENSIONS,
            )
            .await
            .id();
            PgEmbeddingRepository
                .insert(
                    seed.conn(),
                    &a_new_embedding()
                        .knowledge_item_id(platform_item.id())
                        .embedding_profile_id(profile_id)
                        .model(DEFAULT_EMBEDDING_MODEL.to_owned())
                        .embedding(fixed_embedding_vector(DEFAULT_EMBEDDING_DIMENSIONS))
                        .build(),
                )
                .await
                .expect("insert platform item embedding");

            seed.label("backend", backend.id());
            seed.label("frontend", frontend.id());
        });
    })
    .await;

    let backend_id = harness.label("backend").to_owned();
    let frontend_id = harness.label("frontend").to_owned();

    // -- Mount mocks ----------------------------------------------------------

    harness
        .mount_extraction(|m| {
            m.respond(
                ExtractionFixture::builder()
                    .candidate(
                        candidate(
                            "fact",
                            "The canopy-backend service processes document mutations \
                             through an event sourcing pipeline with at-least-once \
                             delivery guarantees",
                        )
                        .tags(&["backend", "event-sourcing"]),
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
            m.respond(RelationFixture::builder().build());
        })
        .await;

    // -- Primary client: set context to backend, ingest -----------------------

    let result = harness
        .call_tool("tribal_set_context", json!({ "project_id": &backend_id }))
        .await;
    assert_success!(result);

    let result = harness
        .call_tool(
            "tribal_ingest",
            json!({
                "content": "The canopy-backend service processes document mutations \
                            through an event sourcing pipeline."
            }),
        )
        .await;
    assert_success!(result);

    let ingest_json = tool_result_json(&result);
    let job_id = ingest_json["job_id"].as_str().expect("job_id").to_owned();

    harness.expect_completion(&job_id).await;

    // Primary client discovers the item via session context (backend).
    let result = harness
        .call_tool("tribal_discover", json!({ "query": "canopy services" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    let items = discover_json["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        1,
        "primary client should discover exactly the backend item via session context",
    );

    // -- Second client: independent session -----------------------------------

    let client_2 = harness.connect_client(DEFAULT_PRINCIPAL_KEY).await;

    // Step 1: without a project context, ingest should fail.
    let result = client_2
        .call_tool(
            "tribal_ingest",
            json!({ "content": "this should fail without a project" }),
        )
        .await;
    assert_error!(result);

    // Step 2: set context to frontend project.
    let result = client_2
        .call_tool("tribal_set_context", json!({ "project_id": &frontend_id }))
        .await;
    assert_success!(result);

    // Step 3: session-scoped discover (frontend) — nothing ingested there.
    let result = client_2
        .call_tool("tribal_discover", json!({ "query": "canopy services" }))
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    let items = discover_json["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        0,
        "frontend session should not see backend or platform knowledge",
    );

    // Step 4: explicit project_id (backend) — should find the ingested item.
    let result = client_2
        .call_tool(
            "tribal_discover",
            json!({
                "query": "canopy services",
                "project_id": &backend_id,
            }),
        )
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    let items = discover_json["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        1,
        "explicit backend project_id should return exactly the backend item",
    );

    // Step 5: null project_id (global) — should find both backend and platform.
    let result = client_2
        .call_tool(
            "tribal_discover",
            json!({
                "query": "canopy services",
                "project_id": null,
            }),
        )
        .await;
    assert_success!(result);

    let discover_json = tool_result_json(&result);
    let items = discover_json["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        2,
        "null project_id should return items from all projects (backend + platform)",
    );

    // -- Cleanup --------------------------------------------------------------

    harness.shutdown().await;
    harness.teardown().await;
}
