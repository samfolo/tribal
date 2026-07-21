//! The tools every agentic stage shares: the job's own context and the
//! record logs of its other threads.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use tribal_agent_runtime::{StageTool, ToolOutcome};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, ExtractionResultRepository,
    PgAgentThreadRecordRepository, PgAgentThreadRepository, PgExtractionResultRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadId, AgentThreadRecordSeq, Candidate, JobId, KnowledgeKind,
    RecoverableToolFailure, ToolDescriptor, ToolExecutionMode, ToolFailure, ToolSafetyTier,
};

use super::{db_failure, parse_arguments, serialise_outcome};

// ---------------------------------------------------------------------------
// read_job_context
// ---------------------------------------------------------------------------

const JOB_CONTEXT_NAME: &str = "read_job_context";
const JOB_CONTEXT_RESPONSE_SIZE_BOUND: u32 = 16_384;
const JOB_CONTEXT_EXPECTED: &str = "no arguments";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobContextArguments {}

#[derive(Debug, Serialize)]
struct CandidateView {
    batch_index: u32,
    kind: KnowledgeKind,
    content: String,
    suggested_tags: Vec<String>,
}

#[derive(Debug, Serialize)]
struct JobContextView {
    job_id: String,
    current_batch_index: u32,
    candidates: Vec<CandidateView>,
}

/// Reads the job's extraction batch: every candidate it produced, with
/// the position under triage named so the model can place itself.
pub(crate) struct ReadJobContextTool {
    descriptor: ToolDescriptor,
    job_id: JobId,
    batch_index: u32,
}

impl ReadJobContextTool {
    /// The tool's declared contract: the binding-hash input the
    /// lockstep definition derivation reads without constructing the
    /// tool.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(JOB_CONTEXT_NAME.to_owned())
            .description(
                "Read this ingestion job's extraction batch: every candidate it produced, \
                 with the batch index under triage named, to spot near-duplicates arriving \
                 together in the same ingest. Optional: reach for it only when a sibling \
                 candidate looks like the same claim."
                    .to_owned(),
            )
            .input_schema(serde_json::json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }))
            .response_size_bound(JOB_CONTEXT_RESPONSE_SIZE_BOUND)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build()
    }

    /// Creates the job context tool for one stage execution.
    pub(crate) fn new(job_id: JobId, batch_index: u32) -> Self {
        let descriptor = Self::describe();
        Self {
            descriptor,
            job_id,
            batch_index,
        }
    }
}

#[async_trait]
impl StageTool for ReadJobContextTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn execute(
        &self,
        conn: &mut PgConnection,
        _prepared: serde_json::Value,
        arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        let _: JobContextArguments =
            parse_arguments(JOB_CONTEXT_NAME, arguments, JOB_CONTEXT_EXPECTED)?;
        let extraction = PgExtractionResultRepository
            .find_by_job_id(conn, self.job_id)
            .await
            .map_err(|source| db_failure("reading the job's extraction result", &source))?
            .ok_or_else(|| ToolFailure::System {
                context: format!(
                    "no extraction result exists for job {}: triage runs after extraction",
                    self.job_id,
                ),
            })?;
        let candidates: Vec<Candidate> = serde_json::from_value(extraction.candidates().clone())
            .map_err(|source| ToolFailure::System {
                context: format!("deserialising the job's extraction candidates: {source}"),
            })?;

        let view = JobContextView {
            job_id: self.job_id.to_string(),
            current_batch_index: self.batch_index,
            candidates: candidates
                .iter()
                .enumerate()
                .map(|(index, candidate)| CandidateView {
                    batch_index: u32::try_from(index).unwrap_or(u32::MAX),
                    kind: candidate.kind(),
                    content: candidate.content().to_owned(),
                    suggested_tags: candidate.suggested_tags().to_vec(),
                })
                .collect(),
        };
        serialise_outcome("serialising the job context", &view)
    }
}

// ---------------------------------------------------------------------------
// read_sibling_threads
// ---------------------------------------------------------------------------

const SIBLING_THREADS_NAME: &str = "read_sibling_threads";
const SIBLING_THREADS_RESPONSE_SIZE_BOUND: u32 = 32_768;
const SIBLING_THREADS_EXPECTED: &str =
    r#"{} to list the roster, or {"thread_id": "<a roster thread id>", "from_seq": <integer>}"#;

/// Records returned per page of a sibling thread's log.
pub(crate) const SIBLING_RECORDS_PAGE_SIZE: u32 = 20;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SiblingThreadsArguments {
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    from_seq: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ThreadRosterEntry {
    thread_id: String,
    stage: &'static str,
    status: &'static str,
    created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct ThreadRosterView {
    threads: Vec<ThreadRosterEntry>,
}

#[derive(Debug, Serialize)]
struct RecordView {
    seq: i64,
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    requesting_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    content: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct RecordPageView {
    thread_id: String,
    records: Vec<RecordView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_from_seq: Option<i64>,
}

/// Lists the job's other threads, or pages one thread's record log.
///
/// The roster covers the job's stage threads; the reading thread itself
/// is excluded, and a page request for anything else renders as a miss.
pub(crate) struct ReadSiblingThreadsTool {
    descriptor: ToolDescriptor,
    job_id: JobId,
    thread_id: AgentThreadId,
}

impl ReadSiblingThreadsTool {
    /// The tool's declared contract: the binding-hash input the
    /// lockstep definition derivation reads without constructing the
    /// tool.
    pub(crate) fn describe() -> ToolDescriptor {
        ToolDescriptor::builder()
            .name(SIBLING_THREADS_NAME.to_owned())
            .description(
                "List this job's other agent threads, or read one thread's record log a page \
                 at a time by passing its thread_id (resume with the returned next_from_seq), \
                 to see what earlier stages observed and decided. Optional and rarely needed."
                    .to_owned(),
            )
            .input_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "thread_id": {
                        "type": "string",
                        "description": "A thread id from the roster; omit to list the roster."
                    },
                    "from_seq": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "The record position to resume from; copy next_from_seq \
                                        from the previous page."
                    }
                },
                "additionalProperties": false
            }))
            .response_size_bound(SIBLING_THREADS_RESPONSE_SIZE_BOUND)
            .safety_tier(ToolSafetyTier::Pure)
            .execution_mode(ToolExecutionMode::Immediate)
            .build()
    }

    /// Creates the sibling reader for one stage execution: the job whose
    /// roster it serves and the reading thread it excludes from it.
    pub(crate) fn new(job_id: JobId, thread_id: AgentThreadId) -> Self {
        let descriptor = Self::describe();
        Self {
            descriptor,
            job_id,
            thread_id,
        }
    }

    /// Renders the job's thread roster, the reading thread excluded.
    fn roster_view(&self, threads: &[AgentThread]) -> ThreadRosterView {
        ThreadRosterView {
            threads: threads
                .iter()
                .filter(|thread| thread.id() != self.thread_id)
                .map(|thread| ThreadRosterEntry {
                    thread_id: thread.id().to_string(),
                    stage: thread.stage().as_str(),
                    status: thread.status().as_str(),
                    created_at: thread.created_at(),
                    completed_at: thread.completed_at(),
                })
                .collect(),
        }
    }
}

#[async_trait]
impl StageTool for ReadSiblingThreadsTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    async fn execute(
        &self,
        conn: &mut PgConnection,
        _prepared: serde_json::Value,
        arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        let args: SiblingThreadsArguments =
            parse_arguments(SIBLING_THREADS_NAME, arguments, SIBLING_THREADS_EXPECTED)?;

        let threads = PgAgentThreadRepository
            .find_by_job_id(conn, self.job_id)
            .await
            .map_err(|source| db_failure("listing the job's threads", &source))?;

        let Some(raw_thread_id) = args.thread_id else {
            if args.from_seq.is_some() {
                return Err(ToolFailure::Recoverable(
                    RecoverableToolFailure::InvalidArguments {
                        tool: SIBLING_THREADS_NAME.to_owned(),
                        detail: "'from_seq' only applies when paging one thread; pass \
                                 'thread_id' with it"
                            .to_owned(),
                    },
                ));
            }
            return serialise_outcome("serialising the thread roster", &self.roster_view(&threads));
        };

        let sibling_id: AgentThreadId = raw_thread_id.parse().map_err(|_| {
            ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments {
                tool: SIBLING_THREADS_NAME.to_owned(),
                detail: format!(
                    "'{raw_thread_id}' is not a thread id; copy ids exactly as the roster \
                     renders them (the '{}_' prefix followed by a UUID)",
                    AgentThreadId::PREFIX,
                ),
            })
        })?;
        let from_seq = match args.from_seq {
            Some(position) if position < 0 => {
                return Err(ToolFailure::Recoverable(
                    RecoverableToolFailure::InvalidArguments {
                        tool: SIBLING_THREADS_NAME.to_owned(),
                        detail: "'from_seq' must be non-negative".to_owned(),
                    },
                ));
            }
            Some(position) => AgentThreadRecordSeq::new(position),
            None => AgentThreadRecordSeq::FIRST,
        };
        let is_sibling =
            sibling_id != self.thread_id && threads.iter().any(|t| t.id() == sibling_id);
        if !is_sibling {
            return Err(ToolFailure::Recoverable(
                RecoverableToolFailure::EmptyResult {
                    tool: SIBLING_THREADS_NAME.to_owned(),
                    detail: format!(
                        "no sibling thread with id {raw_thread_id} exists for this job"
                    ),
                },
            ));
        }

        // One extra row decides whether a further page exists without a
        // second query.
        let mut records = PgAgentThreadRecordRepository
            .find_by_thread_id_from(conn, sibling_id, from_seq, SIBLING_RECORDS_PAGE_SIZE + 1)
            .await
            .map_err(|source| db_failure("paging a sibling thread's records", &source))?;
        let more = records.len() > SIBLING_RECORDS_PAGE_SIZE as usize;
        records.truncate(SIBLING_RECORDS_PAGE_SIZE as usize);
        let next_from_seq = more
            .then(|| records.last().map(|record| record.seq().next().inner()))
            .flatten();

        let view = RecordPageView {
            thread_id: raw_thread_id,
            records: records
                .iter()
                .map(|record| RecordView {
                    seq: record.seq().inner(),
                    kind: record.kind().as_str(),
                    requesting_seq: record.requesting_seq().map(AgentThreadRecordSeq::inner),
                    tool_call_id: record.tool_call_id().map(str::to_owned),
                    content: record.content().clone(),
                })
                .collect(),
            next_from_seq,
        };
        serialise_outcome("serialising a sibling thread's record page", &view)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_db::{
        AgentBindingVersionRepository, DrivingTaskRef, ExtractionResultRepository, JobRepository,
        NewAgentBindingVersion, NewAgentThread, NewAgentThreadRecord,
        PgAgentBindingVersionRepository, PgJobRepository, PgPrincipalRepository,
        PgProjectRepository, PgTaskRepository, PrincipalRepository, ProjectRepository,
        TaskRepository,
    };
    use tribal_domain::{
        AGENT_THREAD_FORMAT_VERSION, AgentThreadRecordKind, AgentThreadStage, GitRemote,
        PrincipalId, TaskType,
    };
    use tribal_test_utils::{
        TestDb, a_candidate, a_new_extraction_result, a_new_job, a_new_principal, a_new_project,
        a_new_prompt_version, a_new_system_fingerprint, a_new_task, an_agent_definition,
        candidates_json, insert_prompt_version, upsert_system_fingerprint,
    };

    use super::*;

    struct SeededJob {
        job: JobId,
        principal: PrincipalId,
    }

    async fn seed_job(conn: &mut sqlx::PgConnection, suffix: &str) -> SeededJob {
        let principal = PgPrincipalRepository
            .insert(
                conn,
                &a_new_principal()
                    .principal_key(format!("user:tools-common-{suffix}"))
                    .build(),
            )
            .await
            .expect("insert principal");
        let project = PgProjectRepository
            .insert_git(
                conn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        &format!("test/tools-common-{suffix}"),
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("insert project");
        let pv_id = insert_prompt_version(conn, &a_new_prompt_version().build()).await;
        let fingerprint_hash =
            upsert_system_fingerprint(conn, &a_new_system_fingerprint().build()).await;
        let job = PgJobRepository
            .insert(
                conn,
                &a_new_job()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .extraction_system_prompt_version_id(pv_id)
                    .extraction_user_prompt_version_id(pv_id)
                    .triage_system_prompt_version_id(pv_id)
                    .triage_user_prompt_version_id(pv_id)
                    .relation_system_prompt_version_id(pv_id)
                    .relation_user_prompt_version_id(pv_id)
                    .system_fingerprint_hash(fingerprint_hash)
                    .build(),
            )
            .await
            .expect("insert job");
        SeededJob {
            job: job.id(),
            principal: principal.id(),
        }
    }

    async fn seed_stage_thread(
        conn: &mut sqlx::PgConnection,
        job: &SeededJob,
        stage: TaskType,
        binding_hash: &str,
        batch_index: Option<u32>,
    ) -> AgentThread {
        let task = PgTaskRepository
            .insert(
                conn,
                &a_new_task()
                    .job_id(job.job)
                    .task_type(stage)
                    .batch_index(batch_index)
                    .build(),
            )
            .await
            .expect("insert task");
        let binding = PgAgentBindingVersionRepository
            .record(
                conn,
                &NewAgentBindingVersion::builder()
                    .hash(binding_hash.to_owned())
                    .pipeline_stage(stage)
                    .definition(an_agent_definition().pipeline_stage(stage).build())
                    .build(),
            )
            .await
            .expect("record binding");
        PgAgentThreadRepository
            .insert(
                conn,
                &NewAgentThread::builder()
                    .stage(AgentThreadStage::Product(stage))
                    .binding_version_id(Some(binding.id()))
                    .driving_task(DrivingTaskRef::Stage(task.id()))
                    .principal_id(Some(job.principal))
                    .format_version(AGENT_THREAD_FORMAT_VERSION)
                    .build(),
            )
            .await
            .expect("insert thread")
    }

    fn parsed(outcome: &ToolOutcome) -> serde_json::Value {
        serde_json::from_str(&outcome.content).expect("tool outcomes are JSON")
    }

    #[tokio::test]
    async fn test_read_job_context_renders_the_extraction_batch() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let job = seed_job(&mut txn, "job-context").await;

        let candidates = [
            a_candidate().content("the first claim".to_owned()).build(),
            a_candidate().content("the second claim".to_owned()).build(),
        ];
        PgExtractionResultRepository
            .insert(
                &mut txn,
                &a_new_extraction_result()
                    .job_id(job.job)
                    .candidates(candidates_json(&candidates))
                    .build(),
            )
            .await
            .expect("insert extraction result");

        let tool = ReadJobContextTool::new(job.job, 1);
        let outcome = tool
            .execute(&mut txn, serde_json::Value::Null, &serde_json::json!({}))
            .await
            .expect("job context read succeeds");
        let view = parsed(&outcome);

        assert_eq!(view["job_id"], job.job.to_string());
        assert_eq!(view["current_batch_index"], 1);
        let rendered = view["candidates"].as_array().expect("candidates array");
        assert_eq!(rendered.len(), 2);
        assert_eq!(rendered[0]["batch_index"], 0);
        assert_eq!(rendered[0]["content"], "the first claim");
        assert_eq!(rendered[1]["batch_index"], 1);
        assert_eq!(rendered[1]["content"], "the second claim");
        assert!(rendered[0]["suggested_tags"].is_array());

        // A job without an extraction result is a broken pipeline
        // ordering, never a model-facing diagnostic.
        let bare_job = seed_job(&mut txn, "job-context-bare").await;
        let err = ReadJobContextTool::new(bare_job.job, 0)
            .execute(&mut txn, serde_json::Value::Null, &serde_json::json!({}))
            .await
            .expect_err("a missing extraction result is a system failure");
        assert!(matches!(err, ToolFailure::System { .. }));
    }

    #[tokio::test]
    async fn test_read_sibling_threads_lists_the_roster_excluding_self() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let job = seed_job(&mut txn, "roster").await;
        let own =
            seed_stage_thread(&mut txn, &job, TaskType::Triage, &"c".repeat(64), Some(0)).await;
        let sibling =
            seed_stage_thread(&mut txn, &job, TaskType::Extraction, &"d".repeat(64), None).await;

        let tool = ReadSiblingThreadsTool::new(job.job, own.id());
        let outcome = tool
            .execute(&mut txn, serde_json::Value::Null, &serde_json::json!({}))
            .await
            .expect("roster read succeeds");
        let view = parsed(&outcome);
        let threads = view["threads"].as_array().expect("threads array");
        assert_eq!(threads.len(), 1, "the reading thread must not list itself");
        assert_eq!(threads[0]["thread_id"], sibling.id().to_string());
        assert_eq!(threads[0]["stage"], "extraction");
        assert_eq!(threads[0]["status"], "queued");

        // Paging the reading thread itself or an unknown thread renders
        // as the same miss.
        for raw_id in [own.id().to_string(), AgentThreadId::new().to_string()] {
            let err = tool
                .execute(
                    &mut txn,
                    serde_json::Value::Null,
                    &serde_json::json!({"thread_id": raw_id}),
                )
                .await
                .expect_err("only the job's other threads are readable");
            assert!(matches!(
                err,
                ToolFailure::Recoverable(RecoverableToolFailure::EmptyResult { .. })
            ));
        }

        let err = tool
            .execute(
                &mut txn,
                serde_json::Value::Null,
                &serde_json::json!({"from_seq": 3}),
            )
            .await
            .expect_err("a cursor without a thread is an argument fault");
        assert!(matches!(
            err,
            ToolFailure::Recoverable(RecoverableToolFailure::InvalidArguments { .. })
        ));
    }

    #[tokio::test]
    async fn test_read_sibling_threads_pages_the_record_log() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let job = seed_job(&mut txn, "paging").await;
        let own =
            seed_stage_thread(&mut txn, &job, TaskType::Triage, &"e".repeat(64), Some(0)).await;
        let sibling =
            seed_stage_thread(&mut txn, &job, TaskType::Extraction, &"f".repeat(64), None).await;

        let total = i64::from(SIBLING_RECORDS_PAGE_SIZE) + 5;
        for position in 0..total {
            PgAgentThreadRecordRepository
                .append(
                    &mut txn,
                    &NewAgentThreadRecord::builder()
                        .thread_id(sibling.id())
                        .seq(AgentThreadRecordSeq::new(position))
                        .kind(AgentThreadRecordKind::Input)
                        .content(serde_json::json!({"position": position}))
                        .build(),
                )
                .await
                .expect("append record");
        }

        let tool = ReadSiblingThreadsTool::new(job.job, own.id());
        let first = parsed(
            &tool
                .execute(
                    &mut txn,
                    serde_json::Value::Null,
                    &serde_json::json!({"thread_id": sibling.id().to_string()}),
                )
                .await
                .expect("first page"),
        );
        let records = first["records"].as_array().expect("records array");
        assert_eq!(records.len(), SIBLING_RECORDS_PAGE_SIZE as usize);
        assert_eq!(records[0]["seq"], 0);
        assert_eq!(records[0]["kind"], "input");
        assert_eq!(records[0]["content"]["position"], 0);
        let next = first["next_from_seq"]
            .as_i64()
            .expect("a further page is signposted");
        assert_eq!(next, i64::from(SIBLING_RECORDS_PAGE_SIZE));

        // The cursor the model copies back walks the log without gaps
        // or repeats, and the final page carries no signpost.
        let second = parsed(
            &tool
                .execute(
                    &mut txn,
                    serde_json::Value::Null,
                    &serde_json::json!({
                        "thread_id": sibling.id().to_string(),
                        "from_seq": next,
                    }),
                )
                .await
                .expect("second page"),
        );
        let records = second["records"].as_array().expect("records array");
        assert_eq!(records.len(), 5);
        assert_eq!(records[0]["seq"], next);
        assert_eq!(records[4]["seq"], total - 1);
        assert!(
            second.get("next_from_seq").is_none(),
            "the last page must not signpost a further one",
        );

        // A page that ends exactly at the log's tail is also final.
        let exact = parsed(
            &tool
                .execute(
                    &mut txn,
                    serde_json::Value::Null,
                    &serde_json::json!({
                        "thread_id": sibling.id().to_string(),
                        "from_seq": 5,
                    }),
                )
                .await
                .expect("exact-boundary page"),
        );
        assert_eq!(
            exact["records"].as_array().expect("records array").len(),
            SIBLING_RECORDS_PAGE_SIZE as usize,
        );
        assert!(exact.get("next_from_seq").is_none());
    }
}
