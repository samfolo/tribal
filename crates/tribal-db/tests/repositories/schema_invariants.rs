//! Direct probes of constraints that production reaches only through repositories.

use tribal_test_utils::TestDb;

#[tokio::test]
async fn test_project_origin_constraint_rejects_malformed_and_extra_key_shapes() {
    let ctx = TestDb::new().await;
    let malformed = [
        serde_json::Value::Null,
        serde_json::json!({}),
        serde_json::json!({"kind": "system", "extra": true}),
        serde_json::json!({"kind": "git", "default_branch": "main"}),
        serde_json::json!({
            "kind": "git",
            "remote": {"kind": "canonical", "value": ""},
            "default_branch": "main",
        }),
        serde_json::json!({
            "kind": "git",
            "remote": {"kind": "unknown", "value": "github.com/acme/repo"},
            "default_branch": "main",
        }),
        serde_json::json!({
            "kind": "git",
            "remote": {"kind": "legacy", "value": "raw", "extra": true},
            "default_branch": "main",
        }),
        serde_json::json!({
            "kind": "git",
            "remote": {"kind": "legacy", "value": "raw"},
            "default_branch": 1,
        }),
        serde_json::json!({
            "kind": "git",
            "remote": {"kind": "legacy", "value": "raw"},
            "default_branch": "main",
            "extra": true,
        }),
    ];

    for origin in malformed {
        let result = sqlx::query(
            "INSERT INTO projects (origin, name, schema_version, settings) \
             VALUES ($1, 'invalid', 1, '{}'::jsonb)",
        )
        .bind(origin.clone())
        .execute(ctx.pool())
        .await;
        assert!(result.is_err(), "malformed origin was accepted: {origin}");
    }
}

#[tokio::test]
async fn test_system_project_origin_is_a_database_singleton() {
    let ctx = TestDb::new().await;
    let duplicate = sqlx::query(
        "INSERT INTO projects (origin, name, schema_version, settings) \
         VALUES ('{\"kind\":\"system\"}'::jsonb, 'Duplicate', 1, '{}'::jsonb)",
    )
    .execute(ctx.pool())
    .await;

    assert!(duplicate.is_err());
}
