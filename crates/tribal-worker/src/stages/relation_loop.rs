//! The agentic relation stage: the loop executor's wiring.
//!
//! Context assembly is the one-shot's — the batch's committed items, the
//! intra-batch hints, the triage handoffs and similar-item decisions —
//! rendered through the loop templates the thread's recorded binding pins.
//! The runtime drives the turns; this module supplies the cross-project
//! tools, the submission pipeline, and the mapping from an accepted
//! submission onto the existing relation commit. The committed shape (the
//! batch-sealed relation rows and the job's terminal outcome) is the
//! one-shot's, so the two executors cannot drift on what they write.

use std::{collections::HashSet, sync::Arc};

use tracing::Instrument;
use tribal_agent_runtime::{
    LoopOutcome, RecheckPolicy, RecordedMessage, RenderedConversation, StageThread, ToolRegistry,
    TurnLoopDeps, recorded_conversation, run_turn_loop,
};
use tribal_config::{DEFAULT_AGENTIC_RECHECK_BOUND, DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS};
use tribal_db::{
    KnowledgeItemRepository, NewKnowledgeItemRelation, PgKnowledgeItemRepository,
    PgPromptVersionRepository, PromptVersionRepository,
};
use tribal_domain::{
    AgentBinding, Job, KnowledgeItemId, PrincipalId, PromptClass, PromptRole, PromptStage,
    PromptVersion, RelationBatchId, RelationKind, Task, TaskType, span_attrs,
};
use tribal_inference::UsageAttribution;

use super::{
    RelationCommitDecision, StageCommit, StageTerminal,
    common::attribution_with_prompts,
    relation::{RelationContext, build_prompt_context, compute_outcome},
};
use crate::{
    common::PARSE_PREVIEW_LENGTH,
    error::{STAGE_RELATION, StageError},
    parsing::{RelationSubmission, RelationSubmissionEdge},
    prompt::assemble_relation_loop_opening,
    stages::RelationSubmissionPipeline,
    tools::{RelationToolset, build_relation_registry, submit_relations_descriptor},
    worker::{Worker, heartbeat::WorkerHeartbeatPump, map_runtime_error},
};

/// The loop's recorded prompt pair, resolved from the binding's hashes.
struct RecordedLoopPrompts {
    system: PromptVersion,
    user: PromptVersion,
}

impl Worker {
    /// Runs the relation loop for a job: assembles the opening from the
    /// batch, registers the cross-project tools, drives the runtime's turn
    /// loop, and maps the outcome onto the stage contract.
    pub(super) async fn run_relation_loop(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
        pump: &WorkerHeartbeatPump,
    ) -> Result<Option<(StageCommit, StageTerminal)>, StageError> {
        let span = tracing::info_span!(
            "tribal.task.relation",
            { span_attrs::TASK_ID } = %task.id(),
            { span_attrs::STAGE } = "relation",
            { span_attrs::RETRY_COUNT } = task.retry_count(),
            { span_attrs::SYSTEM_PROMPT_VERSION_ID } = tracing::field::Empty,
            { span_attrs::USER_PROMPT_VERSION_ID } = tracing::field::Empty,
        );

        async {
            // Idempotency guard: a prior attempt already sealed the batch.
            if job.committed_batch_id().is_some() {
                return Ok(Some((
                    StageCommit::Relation {
                        decision: RelationCommitDecision::NoOp,
                    },
                    StageTerminal::Completion(None),
                )));
            }

            let ctx = self.load_relation_data(job).await?;

            // The loop prompts follow the thread's recorded binding, not the
            // currently active set: a resumed thread renders and attributes
            // exactly what it was admitted under.
            let prompts = self
                .recorded_relation_loop_prompts(&stage_thread.binding)
                .await?;
            tracing::Span::current().record(
                span_attrs::SYSTEM_PROMPT_VERSION_ID,
                tracing::field::display(prompts.system.id()),
            );
            tracing::Span::current().record(
                span_attrs::USER_PROMPT_VERSION_ID,
                tracing::field::display(prompts.user.id()),
            );

            let active_profile = self.resolve_active_embedding(STAGE_RELATION).await?;
            let attribution = attribution_with_prompts(
                job,
                task,
                &stage_thread.thread,
                prompts.system.id(),
                prompts.user.id(),
            );

            let opening_rendered = self
                .relation_opening_or_replay(&stage_thread.thread, &ctx, &prompts)
                .await?;

            let registry =
                self.relation_tool_registry(job, &active_profile, attribution.clone(), deadline)?;
            let pipeline = RelationSubmissionPipeline;
            let submit_descriptor = submit_relations_descriptor();
            let parameters = &stage_thread.binding.definition().parameters;

            let outcome = run_turn_loop(TurnLoopDeps {
                pool: self.pool(),
                gateway: self.gateway(),
                stage: TaskType::Relation,
                thread: &stage_thread.thread,
                task_id: task.id(),
                claim_token: task.claim_token().ok_or(StageError::OwnershipLost)?,
                attempt: tribal_common::clamp_to_i32(task.retry_count()),
                attribution: &attribution,
                registry: &registry,
                submit_descriptor: &submit_descriptor,
                pipeline: &pipeline,
                pump,
                rendered: opening_rendered,
                budgets: crate::definition::current_stage_budgets(
                    TaskType::Relation,
                    stage_thread.binding.definition().executor,
                    self.agents(),
                ),
                recheck: RecheckPolicy {
                    delay_seconds: DEFAULT_AGENTIC_RECHECK_DELAY_SECONDS,
                    bound: DEFAULT_AGENTIC_RECHECK_BOUND,
                },
                temperature: crate::prompt::narrow_temperature(parameters.temperature),
                max_tokens: parameters.max_tokens,
                permit_deadline: deadline,
            })
            .await
            .map_err(|source| {
                map_runtime_error(STAGE_RELATION, "running the relation loop", source)
            })?;

            match outcome {
                LoopOutcome::Submitted(accepted) => {
                    let submission: RelationSubmission =
                        serde_json::from_value(accepted.payload.clone()).map_err(|_| {
                            let raw: String = accepted
                                .payload
                                .to_string()
                                .chars()
                                .take(PARSE_PREVIEW_LENGTH)
                                .collect();
                            StageError::Parse {
                                context: "re-reading the accepted submission".to_owned(),
                                raw_response: Some(raw),
                            }
                        })?;
                    let commit = self.relation_submission_commit(&ctx, &submission).await?;
                    Ok(Some((commit, StageTerminal::Submission(accepted))))
                }
                LoopOutcome::Suspended => Ok(None),
                LoopOutcome::CancelIntent => {
                    self.dispose_intervened(task, &stage_thread.thread).await?;
                    Ok(None)
                }
                LoopOutcome::BudgetFailed { cause } => Err(StageError::BudgetExhausted {
                    stage: STAGE_RELATION.into(),
                    context: cause.to_string(),
                }),
            }
        }
        .instrument(span)
        .await
    }

    /// The rendered opening for the batch: a fresh claim builds it, a resume
    /// replays the committed one.
    ///
    /// The resume signal is the committed record log itself, not a recorded
    /// resolution context: the model references items by their committed
    /// ids, which are stable across the resume, so there is no positional
    /// lookup to preserve.
    async fn relation_opening_or_replay(
        &self,
        thread: &tribal_domain::AgentThread,
        ctx: &RelationContext<'_>,
        prompts: &RecordedLoopPrompts,
    ) -> Result<RenderedConversation, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_RELATION.into(),
                context: "acquiring connection to read the recorded opening".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let recorded = recorded_conversation(&mut conn, thread)
            .await
            .map_err(|source| {
                map_runtime_error(STAGE_RELATION, "reading the recorded opening", source)
            })?;
        drop(conn);
        match recorded {
            Some(rendered) => Ok(rendered),
            None => Self::relation_loop_opening(ctx, prompts),
        }
    }

    /// Assembles the loop's opening: the batch's committed items, the
    /// intra-batch hints, and the triage cross-references rendered through
    /// the binding's loop templates. The committed item ids are the model's
    /// reference handles, so they ride the rendered context.
    fn relation_loop_opening(
        ctx: &RelationContext<'_>,
        prompts: &RecordedLoopPrompts,
    ) -> Result<RenderedConversation, StageError> {
        let prompt_context = build_prompt_context(ctx, ctx.batch_size)?;
        let (rendered_system, rendered_user) = assemble_relation_loop_opening(
            prompts.system.content(),
            prompts.user.content(),
            &prompt_context,
        )?;
        Ok(RenderedConversation {
            system: Some(rendered_system),
            messages: vec![RecordedMessage {
                role: "user".to_owned(),
                content: rendered_user,
            }],
            system_prompt_version_id: Some(prompts.system.id()),
            user_prompt_version_id: Some(prompts.user.id()),
            resolution_context: None,
            response_schema: None,
        })
    }

    /// Maps an accepted submission onto the relation commit: every edge's
    /// endpoints resolved and rechecked for existence, self-edges and
    /// duplicates dropped, the batch sealed with the one-shot's computed
    /// outcome. The recheck at the commit boundary is what lets a claim that
    /// vanished between the model's read and its submission drop the edge
    /// rather than fail the insert.
    async fn relation_submission_commit(
        &self,
        ctx: &RelationContext<'_>,
        submission: &RelationSubmission,
    ) -> Result<StageCommit, StageError> {
        let existing = self.existing_endpoint_ids(submission).await?;
        let batch_id = RelationBatchId::new();
        let (relations, skipped) = normalise_submission_edges(
            &submission.relations,
            &existing,
            batch_id,
            ctx.job.principal_id(),
        );
        let outcome = compute_outcome(&ctx.triage_results, ctx.batch_size);

        Ok(StageCommit::Relation {
            decision: RelationCommitDecision::Relate {
                relations,
                batch_id,
                outcome,
                skipped,
            },
        })
    }

    /// The subset of the submission's cited ids that still resolve to a live
    /// knowledge item. A parse failure cannot reach here: the submission
    /// pipeline already rejected any unparseable id.
    async fn existing_endpoint_ids(
        &self,
        submission: &RelationSubmission,
    ) -> Result<HashSet<KnowledgeItemId>, StageError> {
        let cited: Vec<KnowledgeItemId> = submission
            .relations
            .iter()
            .flat_map(|edge| [edge.source_id.as_str(), edge.target_id.as_str()])
            .filter_map(|raw| raw.parse::<KnowledgeItemId>().ok())
            .collect();
        if cited.is_empty() {
            return Ok(HashSet::new());
        }

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_RELATION.into(),
                context: "acquiring connection to recheck endpoint existence".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let items = PgKnowledgeItemRepository
            .find_by_ids(&mut conn, &cited)
            .await
            .map_err(|source| StageError::Database {
                stage: STAGE_RELATION.into(),
                context: "rechecking endpoint existence".into(),
                source,
            })?;
        Ok(items.into_iter().map(|item| item.id()).collect())
    }

    /// The relation stage's cross-project tool registry. Delegates to the
    /// shared builder the lockstep test pins against the hashed surface.
    fn relation_tool_registry(
        &self,
        job: &Job,
        active_profile: &tribal_domain::EmbeddingProfile,
        attribution: UsageAttribution,
        deadline: tokio::time::Instant,
    ) -> Result<ToolRegistry, StageError> {
        build_relation_registry(&RelationToolset {
            profile: active_profile.clone(),
            gateway: Arc::clone(self.gateway()),
            // The agentic search cap; the relation search reuses the triage
            // result limit rather than carry a second knob.
            search_limit: self.config().triage_search_limit,
            deadline,
            attribution,
            // Jobs are single-project, so the batch spans one project; the
            // neighbourhood read stays inside it by default.
            batch_projects: HashSet::from([job.project_id()]),
        })
        .map_err(|source| StageError::BindingDerivation {
            stage: STAGE_RELATION.into(),
            context: source.to_string(),
        })
    }

    /// Resolves the loop templates the binding's recorded hashes pin —
    /// execution truth, hot-reload-proof, resume-stable.
    async fn recorded_relation_loop_prompts(
        &self,
        binding: &AgentBinding,
    ) -> Result<RecordedLoopPrompts, StageError> {
        let hashes = &binding.definition().prompt_hashes;
        let [system_hash, user_hash] = hashes.as_slice() else {
            return Err(StageError::BindingDerivation {
                stage: STAGE_RELATION.into(),
                context: format!(
                    "the loop binding carries {} prompt hashes where two were recorded",
                    hashes.len(),
                ),
            });
        };

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| StageError::Database {
                stage: STAGE_RELATION.into(),
                context: "acquiring connection for the loop prompts".into(),
                source: tribal_db::DbError::QueryFailed {
                    context: "pool acquire".into(),
                    source: e,
                },
            })?;
        let system =
            resolve_relation_loop_prompt(&mut conn, PromptRole::System, system_hash).await?;
        let user = resolve_relation_loop_prompt(&mut conn, PromptRole::User, user_hash).await?;
        Ok(RecordedLoopPrompts { system, user })
    }
}

/// Resolves one recorded relation loop prompt by its slot and content hash.
async fn resolve_relation_loop_prompt(
    conn: &mut sqlx::PgConnection,
    role: PromptRole,
    hash: &str,
) -> Result<PromptVersion, StageError> {
    PgPromptVersionRepository
        .find_by_slot_and_hash(conn, PromptStage::Relation, PromptClass::Loop, role, hash)
        .await
        .map_err(|source| StageError::Database {
            stage: STAGE_RELATION.into(),
            context: "resolving a recorded loop prompt".into(),
            source,
        })?
        .ok_or_else(|| StageError::BindingDerivation {
            stage: STAGE_RELATION.into(),
            context: format!(
                "no stored relation loop {} prompt matches the binding's hash",
                role.as_str(),
            ),
        })
}

/// Resolves the submission's id-based edges into relation rows, sealed with
/// the batch id. Drops, with a skip count, every edge whose endpoint no
/// longer exists, every self-edge, and every duplicate `(source, target,
/// type)` triple — the same normalisation the one-shot path applies to its
/// index-resolved edges.
fn normalise_submission_edges(
    edges: &[RelationSubmissionEdge],
    existing: &HashSet<KnowledgeItemId>,
    batch_id: RelationBatchId,
    principal_id: PrincipalId,
) -> (Vec<NewKnowledgeItemRelation>, usize) {
    let mut seen = HashSet::new();
    let mut rows = Vec::with_capacity(edges.len());
    let mut skipped: usize = 0;

    for edge in edges {
        let (Ok(source_id), Ok(target_id)) = (
            edge.source_id.parse::<KnowledgeItemId>(),
            edge.target_id.parse::<KnowledgeItemId>(),
        ) else {
            skipped += 1;
            continue;
        };
        if !existing.contains(&source_id) || !existing.contains(&target_id) {
            skipped += 1;
            continue;
        }
        if source_id == target_id {
            skipped += 1;
            continue;
        }
        let relation_type: RelationKind = edge.relation_type.into();
        if !seen.insert((source_id, target_id, relation_type)) {
            skipped += 1;
            continue;
        }
        rows.push(
            NewKnowledgeItemRelation::builder()
                .relation_batch_id(batch_id)
                .source_id(source_id)
                .target_id(target_id)
                .relation_type(relation_type)
                .principal_id(principal_id)
                .justification(edge.justification.clone())
                .build(),
        );
    }

    (rows, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::IngestionRelationKind;

    fn ki(suffix: &str) -> KnowledgeItemId {
        format!("ki_{suffix}0000-0000-0000-0000-000000000000")
            .parse()
            .unwrap()
    }

    fn edge(source: KnowledgeItemId, target: KnowledgeItemId) -> RelationSubmissionEdge {
        RelationSubmissionEdge {
            source_id: source.to_string(),
            target_id: target.to_string(),
            relation_type: IngestionRelationKind::Supports,
            justification: None,
        }
    }

    #[test]
    fn test_normalise_keeps_a_live_distinct_edge() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        let existing = HashSet::from([a, b]);
        let (rows, skipped) = normalise_submission_edges(
            &[edge(a, b)],
            &existing,
            RelationBatchId::new(),
            PrincipalId::new(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_id, a);
        assert_eq!(rows[0].target_id, b);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_normalise_drops_a_vanished_endpoint() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        // Only `a` still exists; the edge to the vanished `b` drops.
        let existing = HashSet::from([a]);
        let (rows, skipped) = normalise_submission_edges(
            &[edge(a, b)],
            &existing,
            RelationBatchId::new(),
            PrincipalId::new(),
        );
        assert!(rows.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_drops_self_edges_and_duplicates() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        let existing = HashSet::from([a, b]);
        let (rows, skipped) = normalise_submission_edges(
            &[edge(a, a), edge(a, b), edge(a, b)],
            &existing,
            RelationBatchId::new(),
            PrincipalId::new(),
        );
        assert_eq!(rows.len(), 1, "the self-edge and the duplicate both drop");
        assert_eq!(skipped, 2);
    }

    #[test]
    fn test_normalise_preserves_inverse_edges() {
        let a = ki("aaaa");
        let b = ki("bbbb");
        let existing = HashSet::from([a, b]);
        let (rows, skipped) = normalise_submission_edges(
            &[edge(a, b), edge(b, a)],
            &existing,
            RelationBatchId::new(),
            PrincipalId::new(),
        );
        assert_eq!(rows.len(), 2, "an inverse edge is a distinct triple");
        assert_eq!(skipped, 0);
    }
}
