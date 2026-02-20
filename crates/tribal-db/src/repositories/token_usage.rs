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
};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_PIPELINE_STAGE_IN_DB: &str =
    "unrecognised pipeline stage in database — schema mismatch";
const UNKNOWN_EMBEDDING_PURPOSE_IN_DB: &str =
    "unrecognised embedding purpose in database — schema mismatch";

// ---------------------------------------------------------------------------
// Type-safe stage encoding
// ---------------------------------------------------------------------------

/// Encodes the `purpose_stage_check` constraint at the type level.
///
/// Embedding usage requires a purpose qualifier; non-embedding stages
/// forbid it.  This enum makes the invalid state unrepresentable in
/// [`NewTokenUsage`].
#[derive(Debug, Clone, Copy)]
pub enum TokenUsageStage {
    /// The extraction pipeline stage.
    Extraction,
    /// The triage pipeline stage.
    Triage,
    /// The relation pipeline stage.
    Relation,
    /// The embedding pipeline stage with a required purpose.
    Embedding {
        /// Whether the embedding was for indexing or querying.
        purpose: EmbeddingPurpose,
    },
}

impl TokenUsageStage {
    /// Returns the pipeline stage.
    #[must_use]
    pub fn pipeline_stage(&self) -> PipelineStage {
        match self {
            Self::Extraction => PipelineStage::Extraction,
            Self::Triage => PipelineStage::Triage,
            Self::Relation => PipelineStage::Relation,
            Self::Embedding { .. } => PipelineStage::Embedding,
        }
    }

    /// Returns the embedding purpose, if applicable.
    #[must_use]
    pub fn purpose(&self) -> Option<EmbeddingPurpose> {
        match self {
            Self::Embedding { purpose } => Some(*purpose),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new token usage record.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.  `tokens_total` is excluded
/// — derived in SQL as `tokens_input + tokens_output`.
#[derive(Debug, TypedBuilder)]
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
    /// The prompt version used (null for embedding calls).
    #[builder(default)]
    pub prompt_version_id: Option<PromptVersionId>,
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

const COLUMNS: &str = "id, job_id, task_id, attempt, stage, purpose, provider, model, \
                        tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, \
                        tokens_total, latency_ms, prompt_version_id, trace_id, created_at";

#[async_trait]
impl TokenUsageRepository for PgTokenUsageRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewTokenUsage,
    ) -> Result<TokenUsage, DbError> {
        let stage_str = new.stage.pipeline_stage().as_str();
        let purpose_str = new.stage.purpose().map(|p| p.as_str().to_owned());

        let sql = format!(
            "INSERT INTO token_usage \
                 (job_id, task_id, attempt, stage, purpose, provider, model, \
                  tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, \
                  tokens_total, latency_ms, prompt_version_id, trace_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $8 + $9, $12, $13, $14) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new.job_id.map(|id| *id.inner()))
            .bind(new.task_id.map(|id| *id.inner()))
            .bind(new.attempt)
            .bind(stage_str)
            .bind(&purpose_str)
            .bind(&new.provider)
            .bind(&new.model)
            .bind(new.tokens_input)
            .bind(new.tokens_output)
            .bind(new.tokens_cache_read)
            .bind(new.tokens_cache_write)
            .bind(new.latency_ms)
            .bind(new.prompt_version_id.map(|id| *id.inner()))
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
        .prompt_version_id(
            r.get::<Option<uuid::Uuid>, _>("prompt_version_id")
                .map(PromptVersionId::from),
        )
        .trace_id(r.get("trace_id"))
        .created_at(r.get("created_at"))
        .build()
}
