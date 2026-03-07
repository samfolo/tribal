//! Token usage repository: trait definition and Postgres implementation.
//!
//! Token usage records are append-only and written immediately after
//! each LLM or embedding call.  The `tokens_total` field is derived
//! in SQL from `tokens_input + tokens_output`, never copied from a
//! provider's total field.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{
    EmbeddingPurpose, JobId, PipelineStage, PromptVersionId, TaskId, TokenUsage, TokenUsageId,
    TokenUsageStage,
};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "job_id",
    "task_id",
    "attempt",
    "stage",
    "purpose",
    "provider",
    "model",
    "tokens_input",
    "tokens_output",
    "tokens_cache_read",
    "tokens_cache_write",
    "tokens_total",
    "latency_ms",
    "system_prompt_version_id",
    "user_prompt_version_id",
    "trace_id",
    "created_at",
]);

const UNKNOWN_PIPELINE_STAGE_IN_DB: &str =
    "unrecognised pipeline stage in database — schema mismatch";
const UNKNOWN_EMBEDDING_PURPOSE_IN_DB: &str =
    "unrecognised embedding purpose in database — schema mismatch";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new token usage record.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.  `tokens_total` is excluded
/// — derived in SQL as `tokens_input + tokens_output`.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewTokenUsage {
    /// The job this usage belongs to (null for read-path calls).
    #[builder(default)]
    pub job_id: Option<JobId>,
    /// The task this usage belongs to (null for read-path calls).
    #[builder(default)]
    pub task_id: Option<TaskId>,
    /// Snapshot of the task's retry count at call time.
    pub attempt: i32,
    /// The pipeline stage and optional purpose.
    pub stage: TokenUsageStage,
    /// The LLM or embedding provider name.
    pub provider: String,
    /// The model identifier.
    pub model: String,
    /// Number of input tokens consumed.
    pub tokens_input: i32,
    /// Number of output tokens produced.
    pub tokens_output: i32,
    /// Input tokens served from cache.
    #[builder(default)]
    pub tokens_cache_read: i32,
    /// Tokens written to cache.
    #[builder(default)]
    pub tokens_cache_write: i32,
    /// End-to-end latency in milliseconds.
    pub latency_ms: i32,
    /// The system prompt version used (null for embedding calls).
    #[builder(default)]
    pub system_prompt_version_id: Option<PromptVersionId>,
    /// The user prompt version used (null for embedding calls).
    #[builder(default)]
    pub user_prompt_version_id: Option<PromptVersionId>,
    /// Optional trace identifier for read-path correlation.
    #[builder(default)]
    pub trace_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for token usage records.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.  Token usage is append-only.
#[async_trait]
pub trait TokenUsageRepository {
    /// Inserts a token usage record and returns the populated domain type.
    ///
    /// `tokens_total` is computed in SQL as `tokens_input + tokens_output`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewTokenUsage,
    ) -> Result<TokenUsage, DbError>;

    /// Finds all token usage records for a given job, ordered by
    /// `created_at ASC`.
    ///
    /// Returns an empty vec when no records exist.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<TokenUsage>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`TokenUsageRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgTokenUsageRepository;

#[async_trait]
impl TokenUsageRepository for PgTokenUsageRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewTokenUsage,
    ) -> Result<TokenUsage, DbError> {
        let stage_str = new.stage.pipeline_stage().as_str();
        let purpose_str = new.stage.purpose().map(|p| p.as_str());

        let sql = format!(
            "INSERT INTO token_usage \
                 (job_id, task_id, attempt, stage, purpose, provider, model, \
                  tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, \
                  tokens_total, latency_ms, system_prompt_version_id, user_prompt_version_id, \
                  trace_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $8 + $9, $12, $13, $14, $15) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new.job_id.map(|id| *id.inner()))
            .bind(new.task_id.map(|id| *id.inner()))
            .bind(new.attempt)
            .bind(stage_str)
            .bind(purpose_str)
            .bind(&new.provider)
            .bind(&new.model)
            .bind(new.tokens_input)
            .bind(new.tokens_output)
            .bind(new.tokens_cache_read)
            .bind(new.tokens_cache_write)
            .bind(new.latency_ms)
            .bind(new.system_prompt_version_id.map(|id| *id.inner()))
            .bind(new.user_prompt_version_id.map(|id| *id.inner()))
            .bind(&new.trace_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "inserting token usage".to_owned(),
                source: e,
            })?;

        Ok(map_token_usage_row(&row))
    }

    async fn find_by_job_id(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<TokenUsage>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM token_usage \
             WHERE job_id = $1 \
             ORDER BY created_at ASC",
        );

        let rows = sqlx::query(&sql)
            .bind(job_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding token usage for job {job_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_token_usage_row).collect())
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

#[cfg(feature = "test-helpers")]
impl PgTokenUsageRepository {
    /// Returns all token usage records, ordered by `created_at ASC`.
    ///
    /// Intended for integration tests that need to inspect records
    /// without filtering by job (e.g. backfill records with no job).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    pub async fn find_all_for_test(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Vec<TokenUsage>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM token_usage \
             ORDER BY created_at ASC",
        );

        let rows =
            sqlx::query(&sql)
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| DbError::QueryFailed {
                    context: "finding all token usage records".to_owned(),
                    source: e,
                })?;

        Ok(rows.iter().map(map_token_usage_row).collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a token usage query into a
/// [`TokenUsage`].
fn map_token_usage_row(r: &sqlx::postgres::PgRow) -> TokenUsage {
    TokenUsage::builder()
        .id(TokenUsageId::from(r.get::<uuid::Uuid, _>("id")))
        .job_id(r.get::<Option<uuid::Uuid>, _>("job_id").map(JobId::from))
        .task_id(r.get::<Option<uuid::Uuid>, _>("task_id").map(TaskId::from))
        .attempt(r.get("attempt"))
        .stage(
            r.get::<String, _>("stage")
                .parse::<PipelineStage>()
                .expect(UNKNOWN_PIPELINE_STAGE_IN_DB),
        )
        .purpose(r.get::<Option<String>, _>("purpose").map(|s| {
            s.parse::<EmbeddingPurpose>()
                .expect(UNKNOWN_EMBEDDING_PURPOSE_IN_DB)
        }))
        .provider(r.get("provider"))
        .model(r.get("model"))
        .tokens_input(r.get("tokens_input"))
        .tokens_output(r.get("tokens_output"))
        .tokens_cache_read(r.get("tokens_cache_read"))
        .tokens_cache_write(r.get("tokens_cache_write"))
        .tokens_total(r.get("tokens_total"))
        .latency_ms(r.get("latency_ms"))
        .system_prompt_version_id(
            r.get::<Option<uuid::Uuid>, _>("system_prompt_version_id")
                .map(PromptVersionId::from),
        )
        .user_prompt_version_id(
            r.get::<Option<uuid::Uuid>, _>("user_prompt_version_id")
                .map(PromptVersionId::from),
        )
        .trace_id(r.get("trace_id"))
        .created_at(r.get("created_at"))
        .build()
}
