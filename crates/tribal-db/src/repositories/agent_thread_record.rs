//! Agent thread record repository: the append-only log.
//!
//! Appends derive their `seq` from the same locked read snapshot that
//! selected the work: callers lock the thread row first, ask
//! [`AgentThreadRecordRepository::next_seq`], and insert within the same
//! transaction. A conflict loser surfaces as
//! [`DbError::UniqueViolation`] on the `(thread_id, seq)` key or the
//! tool-result fence, re-reads the tail, and reconciles with what is now
//! committed. Records are immutable: there is no update path. Uses runtime
//! `sqlx::query()` because rows carry TEXT-encoded domain enums.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{
    AgentThreadId, AgentThreadRecord, AgentThreadRecordId, AgentThreadRecordKind,
    AgentThreadRecordSeq, JobId, TaskType,
};
use typed_builder::TypedBuilder;

use super::common::{columns::Columns, constraint::try_into_unique_violation};
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "thread_id",
    "seq",
    "kind",
    "requesting_seq",
    "tool_call_id",
    "content",
    "usage",
    "trace_id",
    "span_id",
    "committed_at",
]);

const UNKNOWN_KIND_IN_DB: &str = "unrecognised record kind in database: schema mismatch";
const NEGATIVE_COUNT: &str = "negative count in database: data corruption";
const BATCH_INDEX_OVERFLOW: &str = "negative batch_index in database — data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for appending one record.
///
/// `id` and `committed_at` are server-defaulted. The caller derives `seq`
/// under the thread row lock; the fence columns are set exactly for
/// executed tool results, which the table CHECK enforces.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewAgentThreadRecord {
    /// The thread appended to.
    pub thread_id: AgentThreadId,
    /// The position derived under the thread row lock.
    pub seq: AgentThreadRecordSeq,
    /// What kind of event this record is.
    pub kind: AgentThreadRecordKind,
    /// The requesting assistant message, for executed tool results.
    #[builder(default)]
    pub requesting_seq: Option<AgentThreadRecordSeq>,
    /// The answered call id, for executed tool results.
    #[builder(default)]
    pub tool_call_id: Option<String>,
    /// The event payload, shaped per the thread's format version.
    pub content: serde_json::Value,
    /// Token usage observed for the call this record captures.
    #[builder(default)]
    pub usage: Option<serde_json::Value>,
    /// The observing trace id.
    #[builder(default)]
    pub trace_id: Option<String>,
    /// The observing span id.
    #[builder(default)]
    pub span_id: Option<String>,
}

/// One agentic triage thread's submission, keyed by the candidate's batch
/// index — the relation stage's handoff lookup. Only agentic threads
/// commit submission records, so a one-shot job yields none.
#[derive(Debug, Clone)]
pub struct JobTriageSubmission {
    /// The candidate's position in the extraction batch.
    pub batch_index: u32,
    /// The submission record's content (the validated payload and its
    /// provenance).
    pub content: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for agent thread records.
#[async_trait]
pub trait AgentThreadRecordRepository {
    /// Returns the next free position in a thread's log. Call under the
    /// thread row lock, in the transaction that will insert.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn next_seq(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<AgentThreadRecordSeq, DbError>;

    /// Appends one record.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] when the seq key or the
    /// tool-result fence rejects the append (the conflict-loser signal),
    /// or [`DbError::QueryFailed`] on other database errors.
    async fn append(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentThreadRecord,
    ) -> Result<AgentThreadRecord, DbError>;

    /// Lists a thread's records in log order.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_thread(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<Vec<AgentThreadRecord>, DbError>;

    /// Lists a thread's records in log order from `from_seq` (inclusive),
    /// at most `limit` of them — the keyset page behind paged reads.
    /// The first page starts at [`AgentThreadRecordSeq::FIRST`]; each
    /// further page starts at the previous page's last seq's `next()`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_thread_from(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        from_seq: AgentThreadRecordSeq,
        limit: u32,
    ) -> Result<Vec<AgentThreadRecord>, DbError>;

    /// Returns a thread's last committed record, if any.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn last_record(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<Option<AgentThreadRecord>, DbError>;

    /// Counts a thread's executed tool results under one requesting
    /// message — the resolve transaction's completeness input.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn count_tool_results(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        requesting_seq: AgentThreadRecordSeq,
    ) -> Result<u64, DbError>;

    /// Lists the submission records of a job's triage threads, each with
    /// its candidate's batch index — the relation stage's handoff lookup.
    /// Only agentic threads commit submission records, so a one-shot job
    /// yields an empty list and the caller gates the call on the
    /// configured executor so the default path issues no query at all.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_triage_submissions_by_job(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<JobTriageSubmission>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`AgentThreadRecordRepository`].
pub struct PgAgentThreadRecordRepository;

#[async_trait]
impl AgentThreadRecordRepository for PgAgentThreadRecordRepository {
    async fn next_seq(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<AgentThreadRecordSeq, DbError> {
        let row = sqlx::query(
            "SELECT COALESCE(MAX(seq) + 1, 0) AS next FROM agent_thread_records \
             WHERE thread_id = $1",
        )
        .bind(thread_id.inner())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("deriving next seq for thread {thread_id}"),
            source: e,
        })?;

        Ok(AgentThreadRecordSeq::new(row.get::<i64, _>("next")))
    }

    async fn append(
        &self,
        conn: &mut PgConnection,
        new: &NewAgentThreadRecord,
    ) -> Result<AgentThreadRecord, DbError> {
        let sql = format!(
            "INSERT INTO agent_thread_records \
             (thread_id, seq, kind, requesting_seq, tool_call_id, content, usage, trace_id, span_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             RETURNING {COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(new.thread_id.inner())
            .bind(new.seq.inner())
            .bind(new.kind.as_str())
            .bind(new.requesting_seq.map(AgentThreadRecordSeq::inner))
            .bind(new.tool_call_id.as_deref())
            .bind(&new.content)
            .bind(new.usage.as_ref())
            .bind(new.trace_id.as_deref())
            .bind(new.span_id.as_deref())
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| {
                try_into_unique_violation(&e).unwrap_or_else(|| DbError::QueryFailed {
                    context: format!(
                        "appending {} record at seq {} to thread {}",
                        new.kind.as_str(),
                        new.seq,
                        new.thread_id,
                    ),
                    source: e,
                })
            })?;

        Ok(map_agent_thread_record_row(&row))
    }

    async fn find_by_thread(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<Vec<AgentThreadRecord>, DbError> {
        let sql =
            format!("SELECT {COLUMNS} FROM agent_thread_records WHERE thread_id = $1 ORDER BY seq");
        let rows = sqlx::query(&sql)
            .bind(thread_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("listing records for thread {thread_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_thread_record_row).collect())
    }

    async fn find_by_thread_from(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        from_seq: AgentThreadRecordSeq,
        limit: u32,
    ) -> Result<Vec<AgentThreadRecord>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM agent_thread_records \
             WHERE thread_id = $1 AND seq >= $2 ORDER BY seq LIMIT $3"
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id.inner())
            .bind(from_seq.inner())
            .bind(i64::from(limit))
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("listing a record page for thread {thread_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_agent_thread_record_row).collect())
    }

    async fn last_record(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
    ) -> Result<Option<AgentThreadRecord>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM agent_thread_records WHERE thread_id = $1 \
             ORDER BY seq DESC LIMIT 1"
        );
        let row = sqlx::query(&sql)
            .bind(thread_id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("reading the last record of thread {thread_id}"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_agent_thread_record_row))
    }

    async fn count_tool_results(
        &self,
        conn: &mut PgConnection,
        thread_id: AgentThreadId,
        requesting_seq: AgentThreadRecordSeq,
    ) -> Result<u64, DbError> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS count FROM agent_thread_records \
             WHERE thread_id = $1 AND requesting_seq = $2 AND kind = 'tool_result'",
        )
        .bind(thread_id.inner())
        .bind(requesting_seq.inner())
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "counting tool results for thread {thread_id} request {requesting_seq}"
            ),
            source: e,
        })?;

        Ok(u64::try_from(row.get::<i64, _>("count")).expect(NEGATIVE_COUNT))
    }

    async fn find_triage_submissions_by_job(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Vec<JobTriageSubmission>, DbError> {
        let rows = sqlx::query(
            "SELECT tasks.batch_index, r.content FROM agent_thread_records r \
             JOIN agent_threads t ON t.id = r.thread_id \
             JOIN tasks ON tasks.id = t.stage_task_id \
             WHERE tasks.job_id = $1 AND t.pipeline_stage = $2 AND r.kind = $3 \
             ORDER BY tasks.batch_index",
        )
        .bind(job_id.inner())
        .bind(TaskType::Triage.as_str())
        .bind(AgentThreadRecordKind::Submission.as_str())
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("listing the triage submissions of job {job_id}"),
            source: e,
        })?;

        Ok(rows
            .iter()
            .map(|r| JobTriageSubmission {
                batch_index: u32::try_from(r.get::<i32, _>("batch_index"))
                    .expect(BATCH_INDEX_OVERFLOW),
                content: r.get::<serde_json::Value, _>("content"),
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

fn map_agent_thread_record_row(r: &sqlx::postgres::PgRow) -> AgentThreadRecord {
    AgentThreadRecord::builder()
        .id(AgentThreadRecordId::from(r.get::<uuid::Uuid, _>("id")))
        .thread_id(AgentThreadId::from(r.get::<uuid::Uuid, _>("thread_id")))
        .seq(AgentThreadRecordSeq::new(r.get::<i64, _>("seq")))
        .kind(
            r.get::<String, _>("kind")
                .parse::<AgentThreadRecordKind>()
                .expect(UNKNOWN_KIND_IN_DB),
        )
        .requesting_seq(
            r.get::<Option<i64>, _>("requesting_seq")
                .map(AgentThreadRecordSeq::new),
        )
        .tool_call_id(r.get::<Option<String>, _>("tool_call_id"))
        .content(r.get::<serde_json::Value, _>("content"))
        .usage(r.get::<Option<serde_json::Value>, _>("usage"))
        .trace_id(r.get::<Option<String>, _>("trace_id"))
        .span_id(r.get::<Option<String>, _>("span_id"))
        .committed_at(r.get("committed_at"))
        .build()
}
