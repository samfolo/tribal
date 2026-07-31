//! Direct probes of constraints that production reaches only through repositories.

use std::borrow::Cow;

use sqlx::migrate::Migrator;
use tribal_db::{DbError, GraphIdentityRepository, MIGRATOR, PgGraphIdentityRepository};
use tribal_test_utils::TestDb;

const INSERT_FLAT_REMOTE_PROJECT: &str = include_str!("sql/insert_flat_remote_project.sql");
const INSERT_OWNING_PRINCIPAL: &str = include_str!("sql/insert_owning_principal.sql");
const INSERT_PROJECT_OWNED_KNOWLEDGE_ITEM: &str =
    include_str!("sql/insert_project_owned_knowledge_item.sql");
const INSERT_PROJECT_OWNED_EXTERNAL_REFERENCE: &str =
    include_str!("sql/insert_project_owned_external_reference.sql");
const INSERT_PROMPT_VERSION: &str = include_str!("sql/insert_prompt_version.sql");
const INSERT_SYSTEM_FINGERPRINT: &str = include_str!("sql/insert_system_fingerprint.sql");
const INSERT_PROJECT_OWNED_JOB: &str = include_str!("sql/insert_project_owned_job.sql");
const INSERT_PROJECT_WITH_ORIGIN: &str = include_str!("sql/insert_project_with_origin.sql");
const INSERT_SECOND_SYSTEM_PROJECT: &str = include_str!("sql/insert_second_system_project.sql");
const SELECT_PROJECT_ORIGIN_BY_ID: &str = include_str!("sql/select_project_origin_by_id.sql");
const SELECT_KNOWLEDGE_ITEM_PROJECT_BY_ID: &str =
    include_str!("sql/select_knowledge_item_project_by_id.sql");
const SELECT_EXTERNAL_REFERENCE_PROJECT_BY_ID: &str =
    include_str!("sql/select_external_reference_project_by_id.sql");
const SELECT_JOB_PROJECT_BY_ID: &str = include_str!("sql/select_job_project_by_id.sql");
const SELECT_SYSTEM_PROJECT_COUNT: &str = include_str!("sql/select_system_project_count.sql");
const INSERT_GRAPH_IDENTITY_ROW: &str = include_str!("sql/insert_graph_identity_row.sql");
const DELETE_GRAPH_IDENTITY_ROW: &str = include_str!("sql/delete_graph_identity_row.sql");
const SELECT_GRAPH_IDENTITY_COUNT: &str = include_str!("sql/select_graph_identity_count.sql");

const PRIOR_PROJECT_HEAD: i64 = 20_260_718_224_843;
const PRIOR_GRAPH_IDENTITY_HEAD: i64 = 20_260_721_102_914;

async fn migrate_to_head(ctx: &TestDb, head: i64) {
    let prior = Migrator {
        migrations: Cow::Owned(
            MIGRATOR
                .iter()
                .filter(|migration| migration.version <= head)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    prior.run(ctx.pool()).await.expect("migrate to prior head");
}

async fn migrate_to_prior_project_head(ctx: &TestDb) {
    migrate_to_head(ctx, PRIOR_PROJECT_HEAD).await;
}

async fn seed_flat_remote_project(
    ctx: &TestDb,
    index: usize,
    remote: &str,
    branch: &str,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    sqlx::query(INSERT_FLAT_REMOTE_PROJECT)
        .bind(id)
        .bind(remote)
        .bind(format!("flat-remote-{index}"))
        .bind(branch)
        .execute(ctx.pool())
        .await
        .expect("seed flat-remote project");
    id
}

struct DirectProjectReferences {
    knowledge_item: uuid::Uuid,
    external_reference: uuid::Uuid,
    job: uuid::Uuid,
}

async fn seed_direct_project_references(
    ctx: &TestDb,
    project_ids: &[uuid::Uuid],
) -> DirectProjectReferences {
    let principal_id = uuid::Uuid::new_v4();
    let item_id = uuid::Uuid::new_v4();
    sqlx::query(INSERT_OWNING_PRINCIPAL)
        .bind(principal_id)
        .execute(ctx.pool())
        .await
        .expect("seed owning principal");
    sqlx::query(INSERT_PROJECT_OWNED_KNOWLEDGE_ITEM)
        .bind(item_id)
        .bind(project_ids[2])
        .bind(principal_id)
        .execute(ctx.pool())
        .await
        .expect("seed project-owned row");

    let reference_id = uuid::Uuid::new_v4();
    sqlx::query(INSERT_PROJECT_OWNED_EXTERNAL_REFERENCE)
        .bind(reference_id)
        .bind(item_id)
        .bind(project_ids[3])
        .execute(ctx.pool())
        .await
        .expect("seed project-owned external reference");

    let prompt_ids: Vec<uuid::Uuid> = (0_u8..6).map(|_| uuid::Uuid::new_v4()).collect();
    for (index, ((stage, role), id)) in [
        ("extraction", "system"),
        ("extraction", "user"),
        ("triage", "system"),
        ("triage", "user"),
        ("relation", "system"),
        ("relation", "user"),
    ]
    .iter()
    .zip(&prompt_ids)
    .enumerate()
    {
        sqlx::query(INSERT_PROMPT_VERSION)
            .bind(id)
            .bind(stage)
            .bind(role)
            .bind(format!("{index:064x}"))
            .execute(ctx.pool())
            .await
            .expect("seed prompt version");
    }
    let fingerprint_hash = "f".repeat(64);
    sqlx::query(INSERT_SYSTEM_FINGERPRINT)
        .bind(&fingerprint_hash)
        .bind("a".repeat(64))
        .execute(ctx.pool())
        .await
        .expect("seed system fingerprint");
    let job_id = uuid::Uuid::new_v4();
    sqlx::query(INSERT_PROJECT_OWNED_JOB)
        .bind(job_id)
        .bind(project_ids[4])
        .bind(principal_id)
        .bind(prompt_ids[0])
        .bind(prompt_ids[1])
        .bind(prompt_ids[2])
        .bind(prompt_ids[3])
        .bind(prompt_ids[4])
        .bind(prompt_ids[5])
        .bind(&fingerprint_hash)
        .execute(ctx.pool())
        .await
        .expect("seed project-owned job");

    DirectProjectReferences {
        knowledge_item: item_id,
        external_reference: reference_id,
        job: job_id,
    }
}

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
            "remote": {"kind": "wrapped", "value": "raw", "extra": true},
            "default_branch": "main",
        }),
        serde_json::json!({
            "kind": "git",
            "remote": {"kind": "wrapped", "value": "raw"},
            "default_branch": 1,
        }),
        serde_json::json!({
            "kind": "git",
            "remote": {"kind": "wrapped", "value": "raw"},
            "default_branch": "main",
            "extra": true,
        }),
    ];

    for origin in malformed {
        let result = sqlx::query(INSERT_PROJECT_WITH_ORIGIN)
            .bind(origin.clone())
            .execute(ctx.pool())
            .await;
        assert!(result.is_err(), "malformed origin was accepted: {origin}");
    }
}

#[tokio::test]
async fn test_system_project_origin_is_a_database_singleton() {
    let ctx = TestDb::new().await;
    let duplicate = sqlx::query(INSERT_SECOND_SYSTEM_PROJECT)
        .execute(ctx.pool())
        .await;

    assert!(duplicate.is_err());
}

#[tokio::test]
async fn test_project_origin_migration_preserves_canonical_identity_and_references() {
    let ctx = TestDb::new_unmigrated().await;
    migrate_to_prior_project_head(&ctx).await;

    let fixtures = [
        ("github.com/acme/canonical", "main"),
        ("gitlab.com/acme/space", "develop"),
        ("github.com/acme/repository", "trunk"),
        ("codeberg.org/acme/portable", "main"),
        ("github.com/acme/branchless", ""),
    ];
    let mut ids = Vec::new();
    for (index, (remote, branch)) in fixtures.iter().enumerate() {
        ids.push(seed_flat_remote_project(&ctx, index, remote, branch).await);
    }
    let references = seed_direct_project_references(&ctx, &ids).await;

    MIGRATOR
        .run(ctx.pool())
        .await
        .expect("migrate to current head");

    for ((remote, branch), id) in fixtures.iter().zip(&ids) {
        let origin = sqlx::query_scalar::<_, serde_json::Value>(SELECT_PROJECT_ORIGIN_BY_ID)
            .bind(id)
            .fetch_one(ctx.pool())
            .await
            .expect("read migrated origin");
        assert_eq!(
            origin,
            serde_json::json!({
                "kind": "git",
                "remote": remote,
                "default_branch": branch,
            })
        );
    }

    let referenced_project =
        sqlx::query_scalar::<_, uuid::Uuid>(SELECT_KNOWLEDGE_ITEM_PROJECT_BY_ID)
            .bind(references.knowledge_item)
            .fetch_one(ctx.pool())
            .await
            .expect("read migrated reference");
    assert_eq!(referenced_project, ids[2]);
    let external_reference_project =
        sqlx::query_scalar::<_, uuid::Uuid>(SELECT_EXTERNAL_REFERENCE_PROJECT_BY_ID)
            .bind(references.external_reference)
            .fetch_one(ctx.pool())
            .await
            .expect("read migrated external reference");
    assert_eq!(external_reference_project, ids[3]);
    let job_project = sqlx::query_scalar::<_, uuid::Uuid>(SELECT_JOB_PROJECT_BY_ID)
        .bind(references.job)
        .fetch_one(ctx.pool())
        .await
        .expect("read migrated job");
    assert_eq!(job_project, ids[4]);
    let system_count = sqlx::query_scalar::<_, i64>(SELECT_SYSTEM_PROJECT_COUNT)
        .fetch_one(ctx.pool())
        .await
        .expect("count System projects");
    assert_eq!(system_count, 1);
}

#[tokio::test]
async fn test_graph_identity_refuses_a_second_row() {
    let ctx = TestDb::new().await;

    let duplicate = sqlx::query(INSERT_GRAPH_IDENTITY_ROW)
        .bind(true)
        .execute(ctx.pool())
        .await;
    assert!(
        duplicate.is_err(),
        "a second only_row=TRUE row must violate the primary key"
    );

    let escaped = sqlx::query(INSERT_GRAPH_IDENTITY_ROW)
        .bind(false)
        .execute(ctx.pool())
        .await;
    assert!(
        escaped.is_err(),
        "an only_row=FALSE row must violate the check constraint"
    );

    let count: i64 = sqlx::query_scalar(SELECT_GRAPH_IDENTITY_COUNT)
        .fetch_one(ctx.pool())
        .await
        .expect("count graph identity rows");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_graph_identity_upgrade_seeds_exactly_one_row() {
    let ctx = TestDb::new_unmigrated().await;
    migrate_to_head(&ctx, PRIOR_GRAPH_IDENTITY_HEAD).await;

    let table_exists: bool = sqlx::query_scalar("SELECT to_regclass('graph_identity') IS NOT NULL")
        .fetch_one(ctx.pool())
        .await
        .expect("probe for graph_identity before the migration");
    assert!(!table_exists, "the prior head must predate graph_identity");

    MIGRATOR
        .run(ctx.pool())
        .await
        .expect("migrate to current head");

    let count: i64 = sqlx::query_scalar(SELECT_GRAPH_IDENTITY_COUNT)
        .fetch_one(ctx.pool())
        .await
        .expect("count graph identity rows");
    assert_eq!(count, 1, "the upgrade seeds exactly one identity");

    let mut conn = ctx.pool().acquire().await.expect("acquire");
    PgGraphIdentityRepository
        .get(&mut conn)
        .await
        .expect("the seeded identity is readable through the repository");
}

#[tokio::test]
async fn test_graph_identity_missing_row_reports_invalid_state() {
    let ctx = TestDb::new().await;

    sqlx::query(DELETE_GRAPH_IDENTITY_ROW)
        .execute(ctx.pool())
        .await
        .expect("remove the seeded identity");

    let mut conn = ctx.pool().acquire().await.expect("acquire");
    let err = PgGraphIdentityRepository
        .get(&mut conn)
        .await
        .expect_err("a missing row is invalid graph state, not a default");
    assert!(matches!(err, DbError::GraphIdentityMissing));
}
