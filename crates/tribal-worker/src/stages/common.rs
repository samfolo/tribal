//! Shared utilities and types for pipeline stage implementations.

use tribal_agent_runtime::{
    AcceptedSubmission, AdmissionDecision, DrivingClaim, RecheckPolicy, RecordedMessage,
    RenderedConversation, StageThread, SuspendOutcome, begin_turn, carried_rechecks,
    commit_loop_terminal, commit_noop_terminal, commit_one_shot_terminal, decide_admission,
    suspend_stage_thread,
};
use tribal_common::clamp_to_i32;
use tribal_config::{DEFAULT_AGENTIC_RECHECK_BOUND, DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS};
use tribal_db::{
    AgentThreadRepository, EmbeddingProfileRepository, NewExtractionResult, NewItemObservation,
    NewKnowledgeItem, NewTask, NewTriageSimilarItemDecision, PgAgentThreadRepository,
    PgEmbeddingProfileRepository, PgPromptVersionRepository, PgTagRegistryRepository,
    PgTokenUsageRepository, PromptVersionRepository, TagRegistryRepository, TokenUsageRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadSuspension, CompletionResponse, EmbeddingProfile, EmbeddingProfileId,
    InferenceIdentity, Job, ProjectId, PromptVersion, PromptVersionId, SuggestedReference,
    TagRegistryEntry, Task, TaskType, UsageOwner, span_attrs,
};
use tribal_inference::{
    CompletionRequest, EmbeddingTarget, InferenceError, Message, Role, UsageAttribution,
};

use crate::{
    definition::current_stage_budgets,
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
    Extraction {
        extraction_result: NewExtractionResult,
        /// The loaded binding the stage actually ran under, committed
        /// onto the job's source context before fan-out.
        extraction_identity: InferenceIdentity,
        /// One triage task per candidate in the batch.
        triage_tasks: Vec<NewTask>,
        /// Capped candidate count.
        batch_size: u32,
        /// Pre-cap candidate count.
        original_count: u32,
    },
    Triage {
        project_id: ProjectId,
        decision: TriageCommitDecision,
        similar_item_decisions: Vec<NewTriageSimilarItemDecision>,
    },
    Relation {
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
    pub(crate) thread: AgentThread,
    pub(crate) commit: StageCommit,
    pub(crate) terminal: StageTerminal,
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
        knowledge_item: Box<NewKnowledgeItem>,
        embedding_vector: Vec<f32>,
        embedding_model: String,
        /// The active profile the candidate was embedded against. The commit
        /// flip-check compares it to the active read under the cutover lock, so
        /// a cutover between the pre-embed and the write re-embeds rather than
        /// storing an old-space vector under the new profile.
        embedding_profile_id: EmbeddingProfileId,
        suggested_references: Vec<SuggestedReference>,
        new_tags: Vec<NewTagWithEmbedding>,
        /// Tags resolved to existing entries, for `usage_count` increment.
        resolved_tags: Vec<String>,
    },
    /// Duplicate candidate: record an observation against the matched item.
    Duplicate { observation: NewItemObservation },
    /// Idempotency skip: result already exists for this `(job_id, batch_index)`.
    NoOp,
}

// ---------------------------------------------------------------------------
// Active embedding profile
// ---------------------------------------------------------------------------

/// Loads the active embedding profile against the given connection. Taking
/// an explicit connection lets the commit path resolve it inside the
/// transaction's snapshot. First-boot provisioning completes a genesis
/// profile before the worker serves any task, so its absence is a
/// consistency fault, not an expected outcome.
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
    /// rather than a boot-time snapshot's. The returned profile id is the
    /// producing identity carried to the commit, whose flip-check compares
    /// it against the active read under the cutover lock.
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
    attribution_with_prompts(job, task, thread, system_pv_id, user_pv_id)
}

/// Builds the ledger attribution with an explicit prompt version pair:
/// the loop path attributes its calls to the binding's loop prompts,
/// not the job's stamped one-shot pair.
pub(crate) fn attribution_with_prompts(
    job: &Job,
    task: &Task,
    thread: &AgentThread,
    system_pv_id: PromptVersionId,
    user_pv_id: PromptVersionId,
) -> UsageAttribution {
    UsageAttribution {
        owner: UsageOwner::Pipeline {
            job_id: job.id(),
            task_id: task.id(),
            thread_id: thread.id(),
            record_id: None,
            attempt: clamp_to_i32(task.retry_count()),
        },
        principal_id: Some(job.principal_id()),
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
    /// one (resume), enforces the binding's token budget, and returns the
    /// turn to send: the committed conversation verbatim with this
    /// attempt's runtime parameters, plus the recorded resolution context
    /// the caller's positional references must resolve against.
    ///
    /// Parameters (temperature, token caps, response format) ride the fresh
    /// request rather than the log: they are binding-pinned behaviour, not
    /// conversation content, so the log stays byte-stable while parameters
    /// follow the thread's recorded binding.
    ///
    /// Returns `None` when no turn runs this claim: the budget admission
    /// suspended the thread, its bounded re-checks ran dry, or a
    /// cancellation intervened.
    pub(crate) async fn bracket_one_shot(
        &self,
        stage: &str,
        stage_thread: &StageThread,
        job: &Job,
        task: &Task,
        request: CompletionRequest,
        resolution_context: Option<serde_json::Value>,
    ) -> Result<Option<BracketedTurn>, StageError> {
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
            response_schema: None,
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
            DrivingClaim::stage(task.id(), claim_token),
            stage_thread.input.as_ref(),
            rendered,
        )
        .await
        .map_err(|source| map_runtime_error(stage, "committing the input record", source))?;

        if self
            .enforce_token_budget(stage, stage_thread, task, claim_token, &mut conn)
            .await?
        {
            return Ok(None);
        }

        Ok(Some(BracketedTurn {
            resolution_context: begun.conversation.resolution_context.clone(),
            request: CompletionRequest {
                system: begun.conversation.system,
                messages: begun
                    .conversation
                    .messages
                    .into_iter()
                    .map(|m| {
                        // The one-shot bracket only ever records the two
                        // the recorded wire roles; an unrecognised string
                        // downgrades to User rather than dropping the
                        // message, keeping resume total.
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
        }))
    }

    /// Enforces the binding's token budget before the stage's call,
    /// returning `true` when no turn may run this claim. Exhaustion
    /// suspends with the bounded-recheck accounting; a re-check that
    /// runs dry fails the thread through the claim-time disposal
    /// machinery, as does a cancellation intervening at the suspend.
    async fn enforce_token_budget(
        &self,
        stage: &str,
        stage_thread: &StageThread,
        task: &Task,
        claim_token: uuid::Uuid,
        conn: &mut sqlx::PgConnection,
    ) -> Result<bool, StageError> {
        // Enforcement reads the current configuration, never the recorded
        // binding: headroom can return through a configuration change.
        let budgets = current_stage_budgets(
            task.task_type(),
            stage_thread.binding.definition().executor,
            self.agents(),
        );
        if budgets.max_total_tokens.is_none() {
            return Ok(false);
        }
        let thread = &stage_thread.thread;
        let carried = carried_rechecks(conn, thread)
            .await
            .map_err(|source| map_runtime_error(stage, "reading the carried re-checks", source))?;
        let recheck = RecheckPolicy {
            delay_seconds: DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS,
            bound: DEFAULT_AGENTIC_RECHECK_BOUND,
        };
        let decision = decide_admission(conn, thread, &budgets, recheck, carried)
            .await
            .map_err(|source| map_runtime_error(stage, "admitting the call", source))?;
        self.metrics()
            .record_agent_budget_admission(decision.label());
        match decision {
            AdmissionDecision::Proceed => Ok(false),
            AdmissionDecision::Suspend { unchanged_rechecks } => {
                let wake_at = chrono::Utc::now()
                    + chrono::Duration::seconds(i64::from(recheck.delay_seconds));
                let suspension = AgentThreadSuspension::BudgetExhaustion { unchanged_rechecks };
                let outcome = suspend_stage_thread(
                    conn,
                    thread,
                    task.id(),
                    claim_token,
                    &suspension,
                    Some(wake_at),
                )
                .await
                .map_err(|source| map_runtime_error(stage, "suspending on the budget", source))?;
                match outcome {
                    SuspendOutcome::Suspended => {
                        self.metrics()
                            .record_agent_suspension(suspension.reason_label());
                    }
                    SuspendOutcome::CancelIntervened => {
                        self.dispose_intervened(task, thread).await?;
                    }
                }
                Ok(true)
            }
            AdmissionDecision::Exhausted(cause) => {
                // The bounded re-checks ran dry: the thread fails with
                // its task disposed, exactly as a terminal stage error
                // would, in the claim-time disposal shape.
                tracing::warn!(
                    thread_id = %thread.id(),
                    task_id = %task.id(),
                    ?cause,
                    "budget re-checks exhausted; failing the thread",
                );
                Err(StageError::BudgetExhausted {
                    stage: stage.to_owned(),
                    context: cause.to_string(),
                })
            }
        }
    }

    /// Re-reads the thread and runs the claim-time disposal for an
    /// intent that intervened mid-claim.
    pub(crate) async fn dispose_intervened(
        &self,
        task: &Task,
        thread: &AgentThread,
    ) -> Result<(), StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: task.task_type().as_str().into(),
                context: "acquiring connection for the intervened disposal".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let fresh = PgAgentThreadRepository
            .find_by_id(&mut conn, thread.id())
            .await
            .map_err(|e| StageError::Database {
                stage: task.task_type().as_str().into(),
                context: "re-reading the intervened thread".into(),
                source: e,
            })?;
        drop(conn);
        if let Some(fresh) = fresh {
            self.dispose_for_thread_state(task, &fresh).await?;
        }
        Ok(())
    }
}

/// The one-shot bracket's product: the request to send (the committed
/// conversation on resume) and the resolution context recorded with it.
pub(crate) struct BracketedTurn {
    pub request: CompletionRequest,
    /// The recorded context the response's positional references resolve
    /// against; `None` for stages with no positional references, or for
    /// records written before the context was recorded.
    pub resolution_context: Option<serde_json::Value>,
}

/// How a stage's thread reaches its terminal: the one-shot completion
/// (with or without a turn) or the loop's accepted submission.
pub(crate) enum StageTerminal {
    /// A one-shot turn's response, `None` for an idempotency no-op.
    Completion(Option<CompletionResponse>),
    /// The loop's accepted submission; records and spend committed per
    /// turn, the terminal contributes the submission record and the CAS.
    Submission(AcceptedSubmission),
}

/// Commits a stage thread's terminal inside the caller's transaction:
/// the assistant record and completed status when a one-shot turn ran,
/// the bare completed status for a no-op, or the typed submission record
/// and completed status for a loop. The one-shot completion's ledger row
/// gains its record attribution in the same commit (a loop's rows were
/// linked per turn); zero linked rows means the sink's best-effort write
/// never landed, which stays best-effort here.
pub(crate) async fn finish_thread(
    txn: &mut sqlx::PgConnection,
    stage: &str,
    thread: &AgentThread,
    task: &Task,
    terminal: &StageTerminal,
) -> Result<(), StageError> {
    match terminal {
        StageTerminal::Completion(Some(response)) => {
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
        StageTerminal::Completion(None) => commit_noop_terminal(txn, thread)
            .await
            .map_err(|source| map_runtime_error(stage, "committing the thread terminal", source)),
        StageTerminal::Submission(accepted) => {
            commit_loop_terminal(txn, thread, accepted)
                .await
                .map_err(|source| {
                    map_runtime_error(stage, "committing the loop terminal", source)
                })?;
            Ok(())
        }
    }
}
