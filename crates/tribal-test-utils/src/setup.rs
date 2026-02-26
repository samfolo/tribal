//! Test setup helpers for entities that lack a direct production
//! repository path.
//!
//! Functions in this module either delegate to an existing repository
//! (prompt versions) or use raw SQL for entities with no production
//! insertion path (embeddings, committed relations).

use sqlx::PgConnection;
use tribal_db::{NewPromptVersion, PgPromptVersionRepository, PromptVersionRepository};
use tribal_domain::{KnowledgeItemId, PrincipalId, PromptVersionId, RelationBatchId, RelationKind};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EMBEDDING_DIMENSIONS_EXCEED_I32: &str = "embedding dimensions exceed i32";

// ---------------------------------------------------------------------------
// insert_prompt_version
// ---------------------------------------------------------------------------

/// Inserts a prompt version via the production repository upsert.
///
/// Callers use the existing `a_new_prompt_version()` factory to build
/// the input.  The upsert is idempotent — repeated calls with the same
/// `(stage, content_hash)` return the existing row.
///
/// # Panics
///
/// Panics if the database operation fails.
pub async fn insert_prompt_version(
    conn: &mut PgConnection,
    new: &NewPromptVersion,
) -> PromptVersionId {
    PgPromptVersionRepository
        .upsert(conn, new)
        .await
        .expect("setup: insert prompt version")
        .id()
}

// ---------------------------------------------------------------------------
// insert_embedding
// ---------------------------------------------------------------------------

/// Inserts a test embedding for a knowledge item.
///
/// No production repository exposes direct embedding insertion — this
/// uses raw SQL.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn insert_embedding(
    conn: &mut PgConnection,
    knowledge_item_id: KnowledgeItemId,
    model: &str,
    vector: Vec<f32>,
) {
    let dimensions = i32::try_from(vector.len()).expect(EMBEDDING_DIMENSIONS_EXCEED_I32);
    let pgvec = pgvector::Vector::from(vector);
    sqlx::query(
        "INSERT INTO embeddings (knowledge_item_id, model, dimensions, embedding) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(knowledge_item_id.inner())
    .bind(model)
    .bind(dimensions)
    .bind(pgvec)
    .execute(&mut *conn)
    .await
    .expect("setup: insert embedding");
}

// ---------------------------------------------------------------------------
// insert_committed_relation
// ---------------------------------------------------------------------------

/// Inserts a committed knowledge item relation.
///
/// No production repository exposes direct relation insertion with a
/// pre-assigned batch — this uses raw SQL.
///
/// # Panics
///
/// Panics if the database query fails.
pub async fn insert_committed_relation(
    conn: &mut PgConnection,
    batch_id: RelationBatchId,
    source_id: KnowledgeItemId,
    target_id: KnowledgeItemId,
    relation_type: RelationKind,
    principal_id: PrincipalId,
) {
    sqlx::query(
        "INSERT INTO knowledge_item_relations \
             (relation_batch_id, source_id, target_id, \
              relation_type, principal_id) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(batch_id.inner())
    .bind(source_id.inner())
    .bind(target_id.inner())
    .bind(relation_type.as_str())
    .bind(principal_id.inner())
    .execute(&mut *conn)
    .await
    .expect("setup: insert committed relation");
}
