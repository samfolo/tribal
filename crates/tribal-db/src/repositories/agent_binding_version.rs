//! Agent binding version repository: content-addressed, append-only.
//!
//! A version is keyed by its hash; recording the same definition twice
//! converges on one row, so resolution is idempotent across processes and
//! restarts. Rows are never updated or deleted. Uses runtime
//! `sqlx::query()` because rows carry TEXT-encoded domain enums.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{AgentBinding, AgentBindingVersionId, AgentDefinition, TaskType};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&["id", "hash", "pipeline_stage", "definition", "created_at"]);

const UNKNOWN_STAGE_IN_DB: &str = "unrecognised pipeline stage in database: schema mismatch";
const MALFORMED_DEFINITION_IN_DB: &str = "malformed binding definition in database: format drift";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for recording a binding version.
///
/// `id` and `created_at` are server-defaulted. The caller computes `hash`
/// over the canonically serialised definition, tool descriptors included.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewAgentBindingVersion {
    /// The content address (64 lowercase hex characters).
    pub hash: String,
    /// The stage this binding serves.
    pub pipeline_stage: TaskType,
    /// The pinned definition.
    pub definition: AgentDefinition,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for agent binding versions.
#[async_trait]
pub trait AgentBindingVersionRepository {
    /// Records a version idempotently, keyed by hash: recording an
    /// already-known definition returns the existing row.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn record(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentBindingVersion,
    ) -> Result<AgentBinding, DbError>;

    /// Finds a version by id.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AgentBindingVersionId,
    ) -> Result<Option<AgentBinding>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`AgentBindingVersionRepository`].
pub struct PgAgentBindingVersionRepository;

#[async_trait]
impl AgentBindingVersionRepository for PgAgentBindingVersionRepository {
    async fn record(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentBindingVersion,
    ) -> Result<AgentBinding, DbError> {
        let definition =
            serde_json::to_value(&new.definition).map_err(|e| DbError::QueryFailed {
                context: format!("serialising binding definition {}", new.hash),
                source: sqlx::Error::Encode(Box::new(e)),
            })?;

        // The no-op DO UPDATE makes the statement always RETURN the row,
        // inserted or pre-existing, in one round trip.
        let sql = format!(
            "INSERT INTO agent_binding_versions (hash, pipeline_stage, definition) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (hash) DO UPDATE SET hash = EXCLUDED.hash \
             RETURNING {COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(&new.hash)
            .bind(new.pipeline_stage.as_str())
            .bind(definition)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("recording binding version {}", new.hash),
                source: e,
            })?;

        map_agent_binding_row(&row)
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AgentBindingVersionId,
    ) -> Result<Option<AgentBinding>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM agent_binding_versions WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding binding version {id}"),
                source: e,
            })?;

        row.as_ref().map(map_agent_binding_row).transpose()
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a row, failing closed rather than panicking on definition drift: a
/// row whose stored stage or definition no longer matches the current shape
/// (the tool-reach reshape is one such drift) fails the affected thread with
/// context instead of taking the worker down.
fn map_agent_binding_row(r: &sqlx::postgres::PgRow) -> Result<AgentBinding, DbError> {
    let hash = r.get::<String, _>("hash");
    let pipeline_stage = r
        .get::<String, _>("pipeline_stage")
        .parse::<TaskType>()
        .map_err(|_| decode_failure(&hash, UNKNOWN_STAGE_IN_DB.to_owned()))?;
    let definition =
        serde_json::from_value::<AgentDefinition>(r.get::<serde_json::Value, _>("definition"))
            .map_err(|source| {
                decode_failure(&hash, format!("{MALFORMED_DEFINITION_IN_DB}: {source}"))
            })?;
    Ok(AgentBinding::builder()
        .id(AgentBindingVersionId::from(r.get::<uuid::Uuid, _>("id")))
        .hash(hash)
        .pipeline_stage(pipeline_stage)
        .definition(definition)
        .created_at(r.get("created_at"))
        .build())
}

/// Wraps a row-decode failure as a non-retryable [`DbError::QueryFailed`],
/// matching the encode-side error the record path already raises.
fn decode_failure(hash: &str, detail: String) -> DbError {
    DbError::QueryFailed {
        context: format!("decoding binding version {hash}"),
        source: sqlx::Error::Decode(detail.into()),
    }
}
