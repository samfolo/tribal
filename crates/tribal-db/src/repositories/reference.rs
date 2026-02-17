//! Reference repository: trait definition and Postgres implementation.
//!
//! References are lightweight pointers attached to knowledge items.  They
//! are append-only — no UPDATE or DELETE operations.  The repository
//! provides insert and lookup operations for the `item_external_references`
//! table.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::{KnowledgeItemId, ProjectId, Reference, ReferenceId, ReferenceKind};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_KIND_IN_DB: &str = "unrecognised reference kind in database — schema mismatch";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new external reference.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.
#[derive(Debug, TypedBuilder)]
pub struct NewReference {
    /// The knowledge item this reference is attached to.
    pub knowledge_item_id: KnowledgeItemId,
    /// Classification of the reference target.
    pub kind: ReferenceKind,
    /// The pointer value (e.g. `"//src/auth/middleware.rs"`, a URL).
    pub value: String,
    /// What was at the location and why it mattered.
    #[builder(default)]
    pub description: Option<String>,
    /// Which project context this reference belongs to.
    pub project_id: ProjectId,
    /// Git commit SHA when the reference was known valid.
    #[builder(default)]
    pub commit: Option<String>,
    /// Branch name (informational only).
    #[builder(default)]
    pub branch: Option<String>,
}

// ---------------------------------------------------------------------------
// Row mapping helper
// ---------------------------------------------------------------------------

/// Builds a [`Reference`] from a query result row.
///
/// References have 8 fields, so this helper avoids repeating the builder
/// chain across the three repository methods.  Panics if `kind` contains
/// an unrecognised value, which indicates a schema mismatch (the CHECK
/// constraint should prevent this).
fn build_reference(
    id: uuid::Uuid,
    knowledge_item_id: uuid::Uuid,
    kind: &str,
    value: String,
    description: Option<String>,
    project_id: uuid::Uuid,
    commit: Option<String>,
    branch: Option<String>,
) -> Reference {
    Reference::builder()
        .id(ReferenceId::from(id))
        .knowledge_item_id(KnowledgeItemId::from(knowledge_item_id))
        .kind(kind.parse::<ReferenceKind>().expect(UNKNOWN_KIND_IN_DB))
        .value(value)
        .description(description)
        .project_id(ProjectId::from(project_id))
        .commit(commit)
        .branch(branch)
        .build()
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for external references.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait ReferenceRepository {
    /// Inserts a new reference and returns the fully populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewReference,
    ) -> Result<Reference, DbError>;

    /// Finds all references for a knowledge item.
    ///
    /// Returns an empty vec if no references exist.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_knowledge_item_id(
        &self,
        conn: &mut PgConnection,
        knowledge_item_id: KnowledgeItemId,
    ) -> Result<Vec<Reference>, DbError>;

    /// Finds all references for multiple knowledge items.
    ///
    /// Returns an empty vec if no references exist or if the input
    /// is empty.  Silently omits IDs that have no references.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_knowledge_item_ids(
        &self,
        conn: &mut PgConnection,
        knowledge_item_ids: &[KnowledgeItemId],
    ) -> Result<Vec<Reference>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`ReferenceRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgReferenceRepository;

#[async_trait]
impl ReferenceRepository for PgReferenceRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewReference,
    ) -> Result<Reference, DbError> {
        let kind_str = new.kind.as_str();

        let r = sqlx::query!(
            r#"
            INSERT INTO item_external_references
                (knowledge_item_id, kind, value, description, project_id, commit, branch)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, knowledge_item_id, kind, value, description, project_id, commit, branch
            "#,
            new.knowledge_item_id.inner(),
            kind_str,
            new.value,
            new.description,
            new.project_id.inner(),
            new.commit,
            new.branch,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "inserting reference".to_owned(),
            source: e,
        })?;

        Ok(build_reference(
            r.id,
            r.knowledge_item_id,
            &r.kind,
            r.value,
            r.description,
            r.project_id,
            r.commit,
            r.branch,
        ))
    }

    async fn find_by_knowledge_item_id(
        &self,
        conn: &mut PgConnection,
        knowledge_item_id: KnowledgeItemId,
    ) -> Result<Vec<Reference>, DbError> {
        let rows = sqlx::query!(
            r#"
            SELECT id, knowledge_item_id, kind, value, description, project_id, commit, branch
            FROM item_external_references
            WHERE knowledge_item_id = $1
            "#,
            knowledge_item_id.inner(),
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding references by knowledge item id {knowledge_item_id}"),
            source: e,
        })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                build_reference(
                    r.id,
                    r.knowledge_item_id,
                    &r.kind,
                    r.value,
                    r.description,
                    r.project_id,
                    r.commit,
                    r.branch,
                )
            })
            .collect())
    }

    async fn find_by_knowledge_item_ids(
        &self,
        conn: &mut PgConnection,
        knowledge_item_ids: &[KnowledgeItemId],
    ) -> Result<Vec<Reference>, DbError> {
        if knowledge_item_ids.is_empty() {
            return Ok(Vec::new());
        }

        let raw_ids: Vec<uuid::Uuid> = knowledge_item_ids.iter().map(|id| *id.inner()).collect();

        let rows = sqlx::query!(
            r#"
            SELECT id, knowledge_item_id, kind, value, description, project_id, commit, branch
            FROM item_external_references
            WHERE knowledge_item_id = ANY($1)
            "#,
            &raw_ids,
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "finding references by {} knowledge item ids",
                knowledge_item_ids.len()
            ),
            source: e,
        })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                build_reference(
                    r.id,
                    r.knowledge_item_id,
                    &r.kind,
                    r.value,
                    r.description,
                    r.project_id,
                    r.commit,
                    r.branch,
                )
            })
            .collect())
    }
}
