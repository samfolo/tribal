use tribal_db::{AgentBindingVersionRepository, DbError, PgAgentBindingVersionRepository};
use tribal_domain::{AgentBindingVersionId, TaskType};
use tribal_test_utils::TestDb;

/// A row whose stored definition no longer matches the current shape fails
/// the read closed with context rather than panicking the caller.
#[tokio::test]
async fn test_find_by_id_fails_closed_on_a_malformed_definition() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");

    let id = AgentBindingVersionId::new();
    PgAgentBindingVersionRepository
        .insert_raw_for_test(
            &mut txn,
            id,
            &"d".repeat(64),
            TaskType::Relation,
            serde_json::json!({"unexpected": "shape"}),
        )
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
