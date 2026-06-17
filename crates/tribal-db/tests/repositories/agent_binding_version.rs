use tribal_db::{AgentBindingVersionRepository, DbError, PgAgentBindingVersionRepository};
use tribal_domain::{
    AgentBindingVersionId, ProjectScope, StageExecutorKind, TaskType, ToolDescriptor,
    ToolExecutionMode, ToolSafetyTier,
};
use tribal_test_utils::{an_agent_definition, test_context};

/// A binding row written before tool reach was hashed stored each tool as a
/// bare descriptor. Reading it back must load through the compatibility
/// deserialiser, taking the fenced reach, rather than failing the definition
/// read.
#[tokio::test]
async fn test_find_by_id_loads_a_legacy_bare_tool_binding_with_fenced_scope() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    let descriptor = ToolDescriptor::builder()
        .name("read_knowledge_item".to_owned())
        .description("read one item".to_owned())
        .input_schema(serde_json::json!({"type": "object"}))
        .response_size_bound(8_192)
        .safety_tier(ToolSafetyTier::Pure)
        .execution_mode(ToolExecutionMode::Immediate)
        .build();

    // A current definition, rewritten so its one tool carries the pre-reach
    // bare-descriptor shape instead of the `{descriptor, project_scope}` one.
    let mut definition = serde_json::to_value(
        an_agent_definition()
            .pipeline_stage(TaskType::Relation)
            .executor(StageExecutorKind::BuiltInLoop)
            .build(),
    )
    .expect("serialise definition");
    definition["tools"] =
        serde_json::json!([serde_json::to_value(&descriptor).expect("serialise descriptor")]);

    let id = AgentBindingVersionId::new();
    sqlx::query(
        "INSERT INTO agent_binding_versions (id, hash, pipeline_stage, definition) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(id.inner())
    .bind("c".repeat(64))
    .bind(TaskType::Relation.as_str())
    .bind(&definition)
    .execute(&mut *txn)
    .await
    .expect("insert legacy row");

    let loaded = PgAgentBindingVersionRepository
        .find_by_id(&mut txn, id)
        .await
        .expect("the query succeeds")
        .expect("the row is present");
    assert_eq!(loaded.definition().tools.len(), 1);
    assert_eq!(loaded.definition().tools[0].descriptor, descriptor);
    assert_eq!(
        loaded.definition().tools[0].project_scope,
        ProjectScope::Fenced,
        "a bare descriptor takes the fenced reach",
    );
}

/// A row whose stored definition no longer matches the current shape fails
/// the read closed with context rather than panicking the caller.
#[tokio::test]
async fn test_find_by_id_fails_closed_on_a_malformed_definition() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");

    let id = AgentBindingVersionId::new();
    sqlx::query(
        "INSERT INTO agent_binding_versions (id, hash, pipeline_stage, definition) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(id.inner())
    .bind("d".repeat(64))
    .bind(TaskType::Relation.as_str())
    .bind(serde_json::json!({"unexpected": "shape"}))
    .execute(&mut *txn)
    .await
    .expect("insert malformed row");

    let result = PgAgentBindingVersionRepository
        .find_by_id(&mut txn, id)
        .await;
    assert!(
        matches!(result, Err(DbError::QueryFailed { .. })),
        "a malformed definition returns an error instead of panicking",
    );
}
