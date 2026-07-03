//! Token usage repository: trait definition and Postgres implementation.
//!
//! Token usage records are append-only and written immediately after
//! each LLM or embedding call.  The `tokens_total` field is derived
//! in SQL from `tokens_input + tokens_output`, never copied from a
//! provider's total field.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use strum::IntoEnumIterator;
use tribal_domain::{
    AgentThreadId, AgentThreadRecordId, EmbeddingPurpose, ExecutionLocus, JobId, PipelineStage,
    PrincipalId, PromptVersionId, ReindexRunId, TaskId, TaskType, TokenUsage, TokenUsageId,
    TokenUsageStage, UsageTotals,
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
    "principal_id",
    "execution_locus",
    "reindex_run_id",
    "agent_thread_id",
    "agent_thread_record_id",
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
const UNKNOWN_EXECUTION_LOCUS_IN_DB: &str =
    "unrecognised execution locus in database — schema mismatch";

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
    /// The contributing principal, when the spend is attributed to one (null
    /// for system work and calls made before a principal binds).
    #[builder(default)]
    pub principal_id: Option<PrincipalId>,
    /// Where the metered work ran (defaults to the edge runtime — local work).
    #[builder(default)]
    pub execution_locus: ExecutionLocus,
    /// The reindex run this usage belongs to (set only for reindex backfill
    /// and catch-up embedding, null otherwise).
    #[builder(default)]
    pub reindex_run_id: Option<ReindexRunId>,
    /// The agent thread this usage belongs to (null for non-thread calls).
    #[builder(default)]
    pub agent_thread_id: Option<AgentThreadId>,
    /// The committed record this usage produced (null until the terminal
    /// record commits, and forever for calls whose record never commits).
    #[builder(default)]
    pub agent_thread_record_id: Option<AgentThreadRecordId>,
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

    /// Links one attempt's completion row to the assistant record its
    /// turn committed, inside the caller's terminal transaction. The row
    /// is scoped by thread, task, attempt, and the stage's completion
    /// class — embedding rows attribute to the thread alone, having
    /// produced no record. Returns the affected row count: zero means
    /// the sink's best-effort write never landed, which stays
    /// best-effort here too.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn link_completion_to_record(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        task_id: TaskId,
        attempt: i32,
        record_id: AgentThreadRecordId,
    ) -> Result<u64, DbError>;

    /// Links the single most-recent unlinked completion-class row at
    /// `(thread, attempt)` to the given record: the driver-driven
    /// thread's analogue of [`Self::link_completion_to_record`], for
    /// executions with no stage task to scope by. Returns the affected
    /// row count; zero stays best-effort.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn link_thread_completion_to_record(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        attempt: i32,
        record_id: AgentThreadRecordId,
    ) -> Result<u64, DbError>;

    /// Sums a thread's ledger-side spend: input, output, and cache-write
    /// tokens across every request the gateway made for the thread,
    /// including requests whose records never committed. Cache-read is not
    /// counted; no provider populates it today, so the exclusion is a no-op
    /// pending a provider-independent cache-accounting reconciliation. This
    /// is the admission check's number.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn sum_thread_spend(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<u64, DbError>;

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

    /// Totals one platform user's attributed spend, grouped by execution
    /// locus. Joins the ledger to `principals` through the platform linkage, so
    /// a binary bound to a platform user reads its own per-user cost. Returns
    /// one [`UsageTotals`] per locus the user has spend at (empty for a user
    /// with none).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn totals_by_platform_user(
        &self,
        conn: &mut PgConnection,
        platform_user_id: &str,
    ) -> Result<Vec<UsageTotals>, DbError>;

    /// Totals an account's attributed spend across every principal bound to
    /// it, grouped by execution locus — the account-wide view of the same
    /// ledger. Returns one [`UsageTotals`] per locus the account has spend at.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn totals_by_account_reference(
        &self,
        conn: &mut PgConnection,
        account_reference: &str,
    ) -> Result<Vec<UsageTotals>, DbError>;
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
    async fn link_completion_to_record(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        task_id: TaskId,
        attempt: i32,
        record_id: AgentThreadRecordId,
    ) -> Result<u64, DbError> {
        // The completion class derives from the task vocabulary, so a
        // future stage joins this predicate by construction.
        let completion_stages: Vec<&str> = TaskType::iter()
            .map(|t| TokenUsageStage::from(t).pipeline_stage().as_str())
            .collect();

        // Constrained to the single most-recent matching row: attempt
        // values repeat once mid-thread progress resets the retry count,
        // and a bulk sweep would attach an earlier claim's orphaned row
        // to the wrong record. The outer `IS NULL` guard repeats the
        // subquery's so the update only ever claims a still-unlinked row:
        // callers already serialise on the claim CAS and the thread lock,
        // but this keeps the statement correct in isolation rather than by
        // caller convention. A zero-row result stays best-effort.
        let result = sqlx::query(
            "UPDATE token_usage \
             SET agent_thread_record_id = $5 \
             WHERE id = ( \
                 SELECT id FROM token_usage \
                 WHERE agent_thread_id = $1 \
                   AND task_id = $2 \
                   AND attempt = $3 \
                   AND stage = ANY($4::text[]) \
                   AND agent_thread_record_id IS NULL \
                 ORDER BY created_at DESC, id DESC \
                 LIMIT 1 \
             ) \
               AND agent_thread_record_id IS NULL",
        )
        .bind(thread_id.inner())
        .bind(task_id.inner())
        .bind(attempt)
        .bind(&completion_stages)
        .bind(record_id.inner())
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("linking completion spend to record {record_id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn link_thread_completion_to_record(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        attempt: i32,
        record_id: AgentThreadRecordId,
    ) -> Result<u64, DbError> {
        let completion_stages: Vec<&str> = TaskType::iter()
            .map(|t| TokenUsageStage::from(t).pipeline_stage().as_str())
            .collect();

        // The outer `IS NULL` guard mirrors the subquery's so the update
        // only ever claims a still-unlinked row, keeping the statement
        // correct in isolation rather than by the caller's serialisation.
        let result = sqlx::query(
            "UPDATE token_usage \
             SET agent_thread_record_id = $4 \
             WHERE id = ( \
                 SELECT id FROM token_usage \
                 WHERE agent_thread_id = $1 \
                   AND attempt = $2 \
                   AND stage = ANY($3::text[]) \
                   AND agent_thread_record_id IS NULL \
                 ORDER BY created_at DESC, id DESC \
                 LIMIT 1 \
             ) \
               AND agent_thread_record_id IS NULL",
        )
        .bind(thread_id.inner())
        .bind(attempt)
        .bind(&completion_stages)
        .bind(record_id.inner())
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("linking thread completion spend to record {record_id}"),
            source: e,
        })?;

        Ok(result.rows_affected())
    }

    async fn sum_thread_spend(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<u64, DbError> {
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(tokens_input + tokens_output + tokens_cache_write), 0) \
             FROM token_usage WHERE agent_thread_id = $1",
        )
        .bind(thread_id.inner())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("summing thread {thread_id} spend"),
            source: e,
        })?;

        Ok(u64::try_from(total).unwrap_or(0))
    }

    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewTokenUsage,
    ) -> Result<TokenUsage, DbError> {
        let stage_str = new.stage.pipeline_stage().as_str();
        let purpose_str = new.stage.purpose().map(|p| p.as_str());

        let sql = format!(
            "INSERT INTO token_usage \
                 (job_id, task_id, reindex_run_id, agent_thread_id, agent_thread_record_id, \
                  attempt, stage, purpose, provider, model, \
                  tokens_input, tokens_output, tokens_cache_read, tokens_cache_write, \
                  tokens_total, latency_ms, system_prompt_version_id, user_prompt_version_id, \
                  trace_id, principal_id, execution_locus) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $11 + $12, \
                     $15, $16, $17, $18, $19, $20) \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(new.job_id.map(|id| *id.inner()))
            .bind(new.task_id.map(|id| *id.inner()))
            .bind(new.reindex_run_id.map(|id| *id.inner()))
            .bind(new.agent_thread_id.map(|id| *id.inner()))
            .bind(new.agent_thread_record_id.map(|id| *id.inner()))
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
            .bind(new.principal_id.map(|id| *id.inner()))
            .bind(new.execution_locus.as_str())
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

    async fn totals_by_platform_user(
        &self,
        conn: &mut PgConnection,
        platform_user_id: &str,
    ) -> Result<Vec<UsageTotals>, DbError> {
        let rows = sqlx::query(&usage_totals_sql("platform_user_id"))
            .bind(platform_user_id)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("totalling spend for platform user {platform_user_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_usage_totals_row).collect())
    }

    async fn totals_by_account_reference(
        &self,
        conn: &mut PgConnection,
        account_reference: &str,
    ) -> Result<Vec<UsageTotals>, DbError> {
        let rows = sqlx::query(&usage_totals_sql("account_reference"))
            .bind(account_reference)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("totalling spend for account {account_reference}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_usage_totals_row).collect())
    }
}

/// Builds the per-locus usage-totals query, filtered on one `principals`
/// linkage column. A column name cannot be a bind parameter and the filter
/// column varies per caller (`platform_user_id` or `account_reference`), so the
/// compile-time macros cannot carry one query for both; the interpolated column
/// is a fixed in-crate literal, never caller input, so it carries no injection.
fn usage_totals_sql(principal_filter_column: &str) -> String {
    format!(
        "SELECT tu.execution_locus AS execution_locus, \
                COUNT(*) AS records, \
                COALESCE(SUM(tu.tokens_input), 0) AS tokens_input, \
                COALESCE(SUM(tu.tokens_output), 0) AS tokens_output, \
                COALESCE(SUM(tu.tokens_cache_read), 0) AS tokens_cache_read, \
                COALESCE(SUM(tu.tokens_cache_write), 0) AS tokens_cache_write \
         FROM token_usage tu \
         JOIN principals p ON p.id = tu.principal_id \
         WHERE p.{principal_filter_column} = $1 \
         GROUP BY tu.execution_locus",
    )
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
        .principal_id(
            r.get::<Option<uuid::Uuid>, _>("principal_id")
                .map(PrincipalId::from),
        )
        .execution_locus(
            r.get::<String, _>("execution_locus")
                .parse::<ExecutionLocus>()
                .expect(UNKNOWN_EXECUTION_LOCUS_IN_DB),
        )
        .reindex_run_id(
            r.get::<Option<uuid::Uuid>, _>("reindex_run_id")
                .map(ReindexRunId::from),
        )
        .agent_thread_id(
            r.get::<Option<uuid::Uuid>, _>("agent_thread_id")
                .map(AgentThreadId::from),
        )
        .agent_thread_record_id(
            r.get::<Option<uuid::Uuid>, _>("agent_thread_record_id")
                .map(AgentThreadRecordId::from),
        )
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

/// Maps a grouped-totals row into a [`UsageTotals`].
fn map_usage_totals_row(r: &sqlx::postgres::PgRow) -> UsageTotals {
    UsageTotals {
        execution_locus: r
            .get::<String, _>("execution_locus")
            .parse::<ExecutionLocus>()
            .expect(UNKNOWN_EXECUTION_LOCUS_IN_DB),
        records: r.get("records"),
        tokens_input: r.get("tokens_input"),
        tokens_output: r.get("tokens_output"),
        tokens_cache_read: r.get("tokens_cache_read"),
        tokens_cache_write: r.get("tokens_cache_write"),
    }
}
