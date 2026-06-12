//! Shared utilities and types for pipeline stage implementations.

use tribal_agent_runtime::{
    RecordedMessage, RenderedConversation, StageThread, begin_turn, commit_noop_terminal,
    commit_one_shot_terminal,
};
use tribal_common::clamp_to_i32;
use tribal_db::{
    EmbeddingProfileRepository, NewExtractionResult, NewItemObservation, NewKnowledgeItem, NewTask,
    NewTriageSimilarItemDecision, PgEmbeddingProfileRepository, PgPromptVersionRepository,
    PgTagRegistryRepository, PgTokenUsageRepository, PromptVersionRepository,
    TagRegistryRepository, TokenUsageRepository,
};
use tribal_domain::{
    AgentThread, CompletionResponse, EmbeddingProfile, EmbeddingProfileId, Job, ProjectId,
    PromptVersion, PromptVersionId, SuggestedReference, TagRegistryEntry, Task, TaskType,
    UsageOwner, span_attrs,
};
use tribal_inference::{
    CompletionRequest, EmbeddingTarget, InferenceError, Message, Role, UsageAttribution,
};

use crate::{
    error::StageError,
    tag_resolution::NewTagWithEmbedding,
    worker::{Worker, map_runtime_error},
};

// ---------------------------------------------------------------------------
// StageCommit
// ---------------------------------------------------------------------------

/// Domain effects produced by a stage, committed transactionally after
/// the stage completes.
pub(crate) enum StageCommit {
    /// Extraction stage effects.
    Extraction {
        /// The extraction result to insert.
        extraction_result: NewExtractionResult,
        /// Triage tasks to create (one per candidate in the batch).
        triage_tasks: Vec<NewTask>,
        /// Capped candidate count.
        batch_size: u32,
        /// Pre-cap candidate count.
        original_count: u32,
    },
    /// Triage stage effects.
    Triage {
        /// The project this triage belongs to.
        project_id: ProjectId,
        /// The triage decision with associated data.
        decision: TriageCommitDecision,
        /// Per-similar-item decisions for audit persistence.
        similar_item_decisions: Vec<NewTriageSimilarItemDecision>,
    },
    /// Relation stage effects.
    Relation {
        /// The relation decision with associated commit data.
        decision: super::relation::RelationCommitDecision,
    },
}

// ---------------------------------------------------------------------------
// StageRun
// ---------------------------------------------------------------------------

/// One stage execution's full outcome: the domain effects to commit, the
/// thread that ran it, and the model response when a turn actually ran
/// (`None` for idempotency no-ops). The commit path composes all three
/// into one transaction.
pub(crate) struct StageRun {
    /// The thread the stage executed under.
    pub(crate) thread: AgentThread,
    /// The domain effects to commit.
    pub(crate) commit: StageCommit,
    /// The model response, when a turn ran.
    pub(crate) response: Option<CompletionResponse>,
}

// ---------------------------------------------------------------------------
// TriageCommitDecision
// ---------------------------------------------------------------------------

/// The triage decision variant with associated commit data.
///
/// Carries raw components rather than pre-constructed DB types because
/// fields like `knowledge_item_id` are only available after INSERT
/// RETURNING inside the commit transaction.
pub(crate) enum TriageCommitDecision {
    /// Novel candidate: create a new knowledge item with embedding and references.
    Novel {
        /// The knowledge item to insert.
        knowledge_item: Box<NewKnowledgeItem>,
        /// The candidate's embedding vector.
        embedding_vector: Vec<f32>,
        /// The embedding model used.
        embedding_model: String,
        /// The active profile the candidate was embedded against. The commit
        /// flip-check compares it to the active read under the cutover lock, so
        /// a cutover between the pre-embed and the write re-embeds rather than
        /// storing an old-space vector under the new profile.
        embedding_profile_id: EmbeddingProfileId,
        /// References suggested by the extraction stage.
        suggested_references: Vec<SuggestedReference>,
        /// New tags with pre-computed embedding vectors for storage.
        new_tags: Vec<NewTagWithEmbedding>,
        /// Tags resolved to existing entries, for `usage_count` increment.
        resolved_tags: Vec<String>,
    },
    /// Duplicate candidate: record an observation against the matched item.
    Duplicate {
        /// The observation to insert.
        observation: NewItemObservation,
    },
    /// Idempotency skip: result already exists for this `(job_id, batch_index)`.
    NoOp,
}

// ---------------------------------------------------------------------------
// Active embedding profile
// ---------------------------------------------------------------------------

/// Loads the active embedding profile against the given connection.
///
/// Reads and writes embed against the active profile, so the read site, the
/// commit transaction, and tag resolution all resolve it through here. Taking
/// an explicit connection lets the commit path resolve it inside the
/// transaction's snapshot. First-boot provisioning completes a genesis profile
/// before the worker serves any task, so its absence is a consistency fault,
/// not an expected outcome.
///
/// # Errors
///
/// Returns [`StageError::Database`] on query failure or when no profile is
/// active.
pub(crate) async fn load_active_embedding_profile(
    conn: &mut sqlx::PgConnection,
    stage: &str,
) -> Result<EmbeddingProfile, StageError> {
    PgEmbeddingProfileRepository
        .find_active(conn)
        .await
        .map_err(|e| StageError::Database {
            stage: stage.into(),
            context: "loading active embedding profile".into(),
            source: e,
        })?
        .ok_or_else(|| StageError::Database {
            stage: stage.into(),
            context: "no active embedding profile".into(),
            source: tribal_db::DbError::NotFound {
                entity: "embedding_profile",
                id: "active".to_owned(),
            },
        })
}

// ---------------------------------------------------------------------------
// Span recording
// ---------------------------------------------------------------------------

/// Records the system and user prompt version IDs on the current span.
pub(crate) fn record_prompt_version_ids(
    system_pv_id: PromptVersionId,
    user_pv_id: PromptVersionId,
) {
    let span = tracing::Span::current();
    span.record(
        span_attrs::SYSTEM_PROMPT_VERSION_ID,
        tracing::field::display(system_pv_id),
    );
    span.record(
        span_attrs::USER_PROMPT_VERSION_ID,
        tracing::field::display(user_pv_id),
    );
}

// ---------------------------------------------------------------------------
// Shared accessors
// ---------------------------------------------------------------------------

impl Worker {
    /// Resolves the active embedding profile on a freshly acquired
    /// connection, so a live read or write embeds in the space it targets
    /// rather than a boot-time snapshot's.
    ///
    /// The returned profile id is the producing identity carried to the
    /// commit, whose flip-check compares it against the active read under
    /// the cutover lock; its provider resolves through the gateway's
    /// per-profile cache, so the common no-flip path is a cache hit.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] if the active profile cannot be
    /// read, or [`StageError::Provider`] if its provider cannot be built
    /// or its credential resolved.
    pub(crate) async fn resolve_active_embedding(
        &self,
        stage: &str,
    ) -> Result<EmbeddingProfile, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for the active embedding profile".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let active = load_active_embedding_profile(&mut conn, stage).await?;

        // A misconfigured profile fails here, before any stage work, rather
        // than mid-embed with a context that no longer names the profile.
        self.gateway()
            .prepare_embedding_target(&EmbeddingTarget::from(&active))
            .map_err(|source| StageError::Provider {
                context: "resolving the active profile's embedding provider".into(),
                source,
            })?;
        Ok(active)
    }
}

// ---------------------------------------------------------------------------
// Attribution and error mapping
// ---------------------------------------------------------------------------

/// Builds the ledger attribution for one stage execution: the pipeline
/// owner, the stage's prompt version pair, and the job's trace identity.
pub(crate) fn stage_attribution(job: &Job, task: &Task, thread: &AgentThread) -> UsageAttribution {
    let (system_pv_id, user_pv_id) = prompt_version_ids_for_task(job, task);
    UsageAttribution {
        owner: UsageOwner::Pipeline {
            job_id: job.id(),
            task_id: task.id(),
            thread_id: thread.id(),
            record_id: None,
            attempt: clamp_to_i32(task.retry_count()),
        },
        system_prompt_version_id: Some(system_pv_id),
        user_prompt_version_id: Some(user_pv_id),
        trace_id: job
            .trace_context()
            .and_then(tribal_telemetry::trace_id_from_traceparent)
            .or_else(tribal_telemetry::current_trace_id),
    }
}

/// Returns the `(system, user)` prompt version pair for the task's stage.
pub(crate) fn prompt_version_ids_for_task(
    job: &Job,
    task: &Task,
) -> (PromptVersionId, PromptVersionId) {
    match task.task_type() {
        TaskType::Extraction => (
            job.extraction_system_prompt_version_id(),
            job.extraction_user_prompt_version_id(),
        ),
        TaskType::Triage => (
            job.triage_system_prompt_version_id(),
            job.triage_user_prompt_version_id(),
        ),
        TaskType::Relation => (
            job.relation_system_prompt_version_id(),
            job.relation_user_prompt_version_id(),
        ),
    }
}

/// Maps a gateway error onto the stage error taxonomy: an exhausted permit
/// wait is the stage's semaphore timeout, anything else a provider
/// failure under the given context.
pub(crate) fn map_gateway_error(context: &str, error: InferenceError) -> StageError {
    match error {
        InferenceError::PermitTimeout { provider_key, .. } => {
            StageError::SemaphoreTimeout { provider_key }
        }
        source => StageError::Provider {
            context: context.to_owned(),
            source,
        },
    }
}

// ---------------------------------------------------------------------------
// Shared loaders
// ---------------------------------------------------------------------------

impl Worker {
    /// Loads the full tag registry from the database.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] on pool or query failure.
    pub(crate) async fn load_tag_registry(
        &self,
        stage: &str,
    ) -> Result<Vec<TagRegistryEntry>, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for tag registry".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        PgTagRegistryRepository
            .find_all(&mut conn)
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "loading tag registry".into(),
                source: e,
            })
    }

    /// Loads a prompt version by ID from the database.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::Database`] on pool or query failure.
    pub(crate) async fn load_prompt_version(
        &self,
        stage: &str,
        id: PromptVersionId,
    ) -> Result<PromptVersion, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for prompt version".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        PgPromptVersionRepository
            .find_by_id(&mut conn, id)
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "loading prompt version".into(),
                source: e,
            })
    }
}

// ---------------------------------------------------------------------------
// The one-shot bracket
// ---------------------------------------------------------------------------

impl Worker {
    /// Brackets a stage's single completion call with the thread log:
    /// commits the input record (first attempt) or adopts the committed
    /// one (resume), and returns the turn to send — the committed
    /// conversation verbatim, with this attempt's runtime parameters,
    /// plus the recorded resolution context the caller's positional
    /// references must resolve against.
    ///
    /// Parameters (temperature, token caps, response format) ride the
    /// fresh request: they are binding-pinned behaviour, not conversation
    /// content, so the log stays byte-stable while parameters follow the
    /// thread's recorded binding.
    pub(crate) async fn bracket_one_shot(
        &self,
        stage: &str,
        stage_thread: &StageThread,
        job: &Job,
        task: &Task,
        request: CompletionRequest,
        resolution_context: Option<serde_json::Value>,
    ) -> Result<BracketedTurn, StageError> {
        let (system_pv_id, user_pv_id) = prompt_version_ids_for_task(job, task);
        let rendered = RenderedConversation {
            system: request.system.clone(),
            messages: request
                .messages
                .iter()
                .map(|m| RecordedMessage {
                    role: m.role().as_str().to_owned(),
                    content: m.content().to_owned(),
                })
                .collect(),
            system_prompt_version_id: Some(system_pv_id),
            user_prompt_version_id: Some(user_pv_id),
            resolution_context,
        };

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: stage.into(),
                context: "acquiring connection for the input record".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let Some(claim_token) = task.claim_token() else {
            return Err(StageError::OwnershipLost);
        };
        let begun = begin_turn(
            &mut conn,
            &stage_thread.thread,
            task.id(),
            claim_token,
            stage_thread.input.as_ref(),
            rendered,
        )
        .await
        .map_err(|source| map_runtime_error(stage, "committing the input record", source))?;

        Ok(BracketedTurn {
            resolution_context: begun.conversation.resolution_context.clone(),
            request: CompletionRequest {
                system: begun.conversation.system,
                messages: begun
                    .conversation
                    .messages
                    .into_iter()
                    .map(|m| {
                        // The one-shot bracket only ever records the two
                        // launched wire roles; an unrecognised string (a
                        // future format's role) downgrades to User rather
                        // than dropping the message, keeping resume total.
                        if m.role.as_str() == Role::Assistant.as_str() {
                            Message::Assistant {
                                content: m.content,
                                tool_calls: vec![],
                            }
                        } else {
                            Message::User { content: m.content }
                        }
                    })
                    .collect(),
                tools: vec![],
                temperature: request.temperature,
                max_tokens: request.max_tokens,
                response_format: request.response_format,
            },
        })
    }
}

/// The one-shot bracket's product: the request to send — the committed
/// conversation on resume — and the resolution context recorded with it.
pub(crate) struct BracketedTurn {
    /// The request to send.
    pub request: CompletionRequest,
    /// The recorded context the response's positional references resolve
    /// against; `None` for stages with no positional references, or for
    /// records written before the context was recorded.
    pub resolution_context: Option<serde_json::Value>,
}

/// Commits a stage thread's terminal inside the caller's transaction:
/// the assistant record and completed status when a turn ran, the bare
/// completed status for a no-op. The completion's ledger row gains its
/// record attribution in the same commit; zero linked rows means the
/// sink's best-effort write never landed, which stays best-effort here.
pub(crate) async fn finish_thread(
    txn: &mut sqlx::PgConnection,
    stage: &str,
    thread: &AgentThread,
    task: &Task,
    response: Option<&CompletionResponse>,
) -> Result<(), StageError> {
    match response {
        Some(response) => {
            let outcome = commit_one_shot_terminal(txn, thread, response)
                .await
                .map_err(|source| {
                    map_runtime_error(stage, "committing the thread terminal", source)
                })?;
            PgTokenUsageRepository
                .link_completion_to_record(
                    txn,
                    thread.id(),
                    task.id(),
                    clamp_to_i32(task.retry_count()),
                    outcome.assistant_record.id(),
                )
                .await
                .map_err(|source| StageError::Database {
                    stage: stage.into(),
                    context: "linking the completion spend to its record".into(),
                    source,
                })?;
            Ok(())
        }
        None => commit_noop_terminal(txn, thread)
            .await
            .map_err(|source| map_runtime_error(stage, "committing the thread terminal", source)),
    }
}
