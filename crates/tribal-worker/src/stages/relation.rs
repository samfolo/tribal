//! Relation stage: LLM-based relation extraction and commit.

use std::collections::{HashMap, HashSet};

use tracing::Instrument;
use tribal_agent_runtime::{StageThread, SubmissionContent};
use tribal_common::clamp_to_u32;
use tribal_db::{
    AgentThreadRecordRepository, ExtractionResultRepository, JobTriageSubmission,
    KnowledgeItemRepository, NewKnowledgeItemRelation, PgAgentThreadRecordRepository,
    PgExtractionResultRepository, PgKnowledgeItemRepository, PgTriageResultRepository,
    PgTriageSimilarItemDecisionRepository, TriageResultRepository,
    TriageSimilarItemDecisionRepository,
};
use tribal_domain::{
    Candidate, Job, JobId, JobOutcome, KnowledgeItemId, PrincipalId, RelationBatchId, RelationHint,
    RelationKind, Task, TaskType, TriageOutcome, TriageResult, TriageSimilarItemDecision,
    span_attrs,
};
use tribal_inference::PermitWait;

use super::{
    StageCommit, StageTerminal, map_gateway_error, record_prompt_version_ids, stage_attribution,
};
use crate::{
    common::PARSE_PREVIEW_LENGTH,
    error::{STAGE_RELATION, StageError},
    parsing::{RelationEdge, RelationTarget, TriageSubmission, parse_relation_response},
    prompt::{
        CandidateOutcome, RelationPromptContext, SimilarItemDecisionContext, SimilarityBand,
        assemble_relation_prompt,
    },
    worker::Worker,
};

// ---------------------------------------------------------------------------
// RelationContext
// ---------------------------------------------------------------------------

/// Context assembled before running the relation stage: the complete
/// episode picture the relation agent reasons over.
pub(crate) struct RelationContext<'a> {
    pub job: &'a Job,
    pub batch_size: u32,
    pub candidates: Vec<Candidate>,
    pub relation_hints: Vec<RelationHint>,
    pub triage_results: Vec<TriageResult>,
    pub similar_item_decision_contexts: Vec<SimilarItemDecisionContext>,
    /// The notes agentic triage handed downstream, keyed by candidate
    /// batch index. Empty for the one-shot path, which commits no
    /// submission records.
    pub handoffs: HashMap<u32, String>,
}

// ---------------------------------------------------------------------------
// Commit decision
// ---------------------------------------------------------------------------

/// The relation stage's commit decision.
pub(crate) enum RelationCommitDecision {
    /// Relations to commit with the job's terminal status.
    Relate {
        relations: Vec<NewKnowledgeItemRelation>,
        batch_id: RelationBatchId,
        outcome: JobOutcome,
        /// Number of edges dropped during normalisation.
        skipped: usize,
    },
    /// Idempotency skip: `committed_batch_id` already set.
    NoOp,
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

impl Worker {
    /// Loads all relation stage inputs from the database using a single
    /// pooled connection and returns the assembled context.
    pub(super) async fn load_relation_data<'a>(
        &self,
        job: &'a Job,
    ) -> Result<RelationContext<'a>, StageError> {
        let batch_size = job.batch_size().ok_or_else(|| StageError::Database {
            stage: STAGE_RELATION.into(),
            context: format!("job {} has no batch_size set", job.id()),
            source: tribal_db::DbError::NotFound {
                entity: "job.batch_size",
                id: job.id().to_string(),
            },
        })?;

        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| relation_sqlx_error("acquiring connection", e))?;

        let (candidates, relation_hints) = load_extraction_data(&mut conn, job).await?;
        let triage_results = load_triage_results(&mut conn, job).await?;
        let similar_item_decisions = load_similar_item_decisions(&mut conn, job).await?;
        let similar_item_decision_contexts =
            build_similar_item_decision_contexts(&mut conn, &similar_item_decisions, batch_size)
                .await?;
        // Handoffs are durable triage submission records, the truth of what
        // this job's triage committed; a one-shot job has none, so the read
        // returns empty without depending on the current executor config
        // (which a mid-job change would make a false witness).
        let handoffs = load_triage_handoffs(&mut conn, job.id()).await?;

        drop(conn);

        Ok(RelationContext {
            job,
            batch_size,
            candidates,
            relation_hints,
            triage_results,
            similar_item_decision_contexts,
            handoffs,
        })
    }
}

/// Reads the job's agentic triage handoffs, keyed by candidate batch
/// index. Each submission record is parsed individually and a malformed
/// one skipped (the handoff is best-effort enrichment, never a stage
/// failure); a submission with no handoff contributes no entry.
async fn load_triage_handoffs(
    conn: &mut sqlx::PgConnection,
    job_id: JobId,
) -> Result<HashMap<u32, String>, StageError> {
    let submissions = PgAgentThreadRecordRepository
        .find_triage_submissions_by_job_id(conn, job_id)
        .await
        .map_err(|source| relation_db_error("loading triage handoffs", source))?;
    Ok(handoffs_by_batch_index(submissions))
}

/// Extracts each submission's downstream handoff, keyed by candidate batch
/// index. Each submission is parsed individually and a malformed one
/// skipped (best-effort enrichment, never a stage failure); a submission
/// that carried no handoff contributes no entry.
fn handoffs_by_batch_index(submissions: Vec<JobTriageSubmission>) -> HashMap<u32, String> {
    submissions
        .into_iter()
        .filter_map(|submission| {
            let content: SubmissionContent = serde_json::from_value(submission.content).ok()?;
            let payload: TriageSubmission = serde_json::from_value(content.payload).ok()?;
            payload.handoff.map(|notes| (submission.batch_index, notes))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the relation stage for a task, routed by the thread's recorded
    /// binding: the executor follows the binding, not the current
    /// configuration, so a resumed thread continues the way it started.
    pub(crate) async fn run_relation(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
        pump: &crate::worker::heartbeat::WorkerHeartbeatPump,
    ) -> Result<Option<(StageCommit, StageTerminal)>, StageError> {
        match stage_thread.binding.definition().executor {
            tribal_domain::StageExecutorKind::OneShot => {
                self.run_relation_one_shot(job, task, deadline, stage_thread)
                    .await
            }
            tribal_domain::StageExecutorKind::BuiltInLoop => {
                self.run_relation_loop(job, task, deadline, stage_thread, pump)
                    .await
            }
            tribal_domain::StageExecutorKind::ExternalAgent => Err(StageError::BindingDerivation {
                stage: STAGE_RELATION.into(),
                context: "the external-agent executor has no runner".into(),
            }),
        }
    }

    /// Runs the one-shot relation turn for a job: loads triage results and
    /// extraction hints, calls the LLM, normalises and filters edges, and
    /// returns the stage commit with the response for the thread terminal.
    /// Returns early with a no-op when `committed_batch_id` is already set.
    async fn run_relation_one_shot(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
        stage_thread: &StageThread,
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
            // Idempotency guard.
            if job.committed_batch_id().is_some() {
                return Ok(Some((
                    StageCommit::Relation {
                        decision: RelationCommitDecision::NoOp,
                    },
                    StageTerminal::Completion(None),
                )));
            }

            let include_llm_content = self.include_llm_content();

            let ctx = self.load_relation_data(job).await?;

            let prompt_context = build_prompt_context(&ctx, ctx.batch_size)?;

            let (system_pv, user_pv) = tokio::try_join!(
                self.load_prompt_version(
                    STAGE_RELATION,
                    ctx.job.relation_system_prompt_version_id()
                ),
                self.load_prompt_version(STAGE_RELATION, ctx.job.relation_user_prompt_version_id()),
            )?;

            record_prompt_version_ids(
                ctx.job.relation_system_prompt_version_id(),
                ctx.job.relation_user_prompt_version_id(),
            );

            let request = assemble_relation_prompt(
                system_pv.content(),
                user_pv.content(),
                &prompt_context,
                &stage_thread.binding.definition().parameters,
            )?;

            if include_llm_content {
                tracing::debug!(
                    system_prompt = %request.system.as_deref().unwrap_or(""),
                    user_prompt = %request.messages.first().map_or("", |m| m.content()),
                    "relation prompt assembled",
                );
            }

            // Relation's positional references resolve against the
            // similar-item decisions, which are immutable once the fan-in
            // fires and are read in a deterministic order: a resumed
            // attempt re-derives the identical lookup, so no resolution
            // context needs recording.
            let Some(bracketed) = self
                .bracket_one_shot(STAGE_RELATION, stage_thread, ctx.job, task, request, None)
                .await?
            else {
                return Ok(None);
            };
            let request = bracketed.request;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let response = self
                .gateway()
                .complete(
                    TaskType::Relation,
                    request,
                    PermitWait::Bounded { limit: remaining },
                    &stage_attribution(ctx.job, task, &stage_thread.thread),
                )
                .await
                .map_err(|e| map_gateway_error("relation LLM call", e))?;

            if include_llm_content {
                tracing::debug!(
                    response = %response.text,
                    "relation response received",
                );
            }
            let output = {
                let _parse_span = tracing::info_span!("tribal.relation.parse").entered();
                parse_relation_response(&response).inspect_err(|_| {
                    if include_llm_content {
                        let preview: String =
                            response.text.chars().take(PARSE_PREVIEW_LENGTH).collect();
                        tracing::debug!(preview = %preview, "parse failure: raw LLM response");
                    } else {
                        tracing::debug!(
                            response_length = response.text.len(),
                            "parse failure: response details redacted",
                        );
                    }
                })?
            };

            let lookup = build_unified_lookup(
                &ctx.triage_results,
                &ctx.similar_item_decision_contexts,
                ctx.batch_size,
            );
            let decision = build_commit_decision(
                output.relations,
                &lookup,
                &ctx.triage_results,
                ctx.batch_size,
                ctx.job.principal_id(),
            );

            Ok(Some((
                StageCommit::Relation { decision },
                StageTerminal::Completion(Some(response)),
            )))
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// Private helpers: data loading
// ---------------------------------------------------------------------------

/// Loads candidates and relation hints from the extraction result.
async fn load_extraction_data(
    conn: &mut sqlx::PgConnection,
    job: &Job,
) -> Result<(Vec<Candidate>, Vec<RelationHint>), StageError> {
    let extraction_result = PgExtractionResultRepository
        .find_by_job_id(conn, job.id())
        .await
        .map_err(|e| relation_db_error("loading extraction result", e))?
        .ok_or_else(|| StageError::Database {
            stage: STAGE_RELATION.into(),
            context: format!("no extraction result for job {}", job.id()),
            source: tribal_db::DbError::NotFound {
                entity: "extraction_result",
                id: job.id().to_string(),
            },
        })?;

    let candidates: Vec<Candidate> = serde_json::from_value(extraction_result.candidates().clone())
        .map_err(|_| {
            let raw: String = extraction_result
                .candidates()
                .to_string()
                .chars()
                .take(PARSE_PREVIEW_LENGTH)
                .collect();
            StageError::Parse {
                context: format!("deserialising candidates for job {}", job.id()),
                raw_response: Some(raw),
            }
        })?;

    let relation_hints: Vec<RelationHint> =
        serde_json::from_value(extraction_result.relation_hints().clone()).map_err(|_| {
            let raw: String = extraction_result
                .relation_hints()
                .to_string()
                .chars()
                .take(PARSE_PREVIEW_LENGTH)
                .collect();
            StageError::Parse {
                context: format!("deserialising relation hints for job {}", job.id()),
                raw_response: Some(raw),
            }
        })?;

    Ok((candidates, relation_hints))
}

/// Loads all triage results for the job.
async fn load_triage_results(
    conn: &mut sqlx::PgConnection,
    job: &Job,
) -> Result<Vec<TriageResult>, StageError> {
    PgTriageResultRepository
        .find_by_job_id(conn, job.id())
        .await
        .map_err(|e| relation_db_error("loading triage results", e))
}

/// Loads all triage similar item decisions for the job.
async fn load_similar_item_decisions(
    conn: &mut sqlx::PgConnection,
    job: &Job,
) -> Result<Vec<TriageSimilarItemDecision>, StageError> {
    PgTriageSimilarItemDecisionRepository
        .find_by_job_id(conn, job.id())
        .await
        .map_err(|e| relation_db_error("loading similar item decisions", e))
}

/// Builds `SimilarItemDecisionContext` records by loading matched
/// item content from the knowledge item repository.
async fn build_similar_item_decision_contexts(
    conn: &mut sqlx::PgConnection,
    decisions: &[TriageSimilarItemDecision],
    batch_size: u32,
) -> Result<Vec<SimilarItemDecisionContext>, StageError> {
    if decisions.is_empty() {
        return Ok(vec![]);
    }

    let mut seen_ids = HashSet::new();
    let mut unique_ids = Vec::new();
    for id in decisions
        .iter()
        .map(TriageSimilarItemDecision::matched_item_id)
    {
        if seen_ids.insert(id) {
            unique_ids.push(id);
        }
    }

    let items = PgKnowledgeItemRepository
        .find_by_ids(conn, &unique_ids)
        .await
        .map_err(|e| relation_db_error("loading knowledge items for context", e))?;

    let content_by_id: HashMap<KnowledgeItemId, String> = items
        .into_iter()
        .map(|item| (item.id(), item.content().to_owned()))
        .collect();

    let mut contexts = Vec::with_capacity(decisions.len());

    for d in decisions {
        let Some(content) = content_by_id.get(&d.matched_item_id()) else {
            tracing::warn!(
                matched_item_id = %d.matched_item_id(),
                batch_index = d.batch_index(),
                "dropping stale similar-item decision: matched knowledge item no longer exists",
            );
            continue;
        };

        contexts.push(SimilarItemDecisionContext {
            batch_index: d.batch_index(),
            context_index: batch_size + clamp_to_u32(contexts.len()),
            matched_item_id: d.matched_item_id(),
            matched_content: content.clone(),
            similarity_score: d.similarity_score(),
            similarity_label: SimilarityBand::from(d.similarity_score()).to_string(),
            suggested_relation: d.suggested_relation(),
            justification: d.justification_text().to_owned(),
        });
    }

    Ok(contexts)
}

// ---------------------------------------------------------------------------
// Prompt context assembly
// ---------------------------------------------------------------------------

/// Builds the `RelationPromptContext` from the loaded relation data.
/// Fails when `batch_size` exceeds the candidates array length, which
/// signals corruption between the extraction result and the job's count.
pub(super) fn build_prompt_context<'a>(
    ctx: &'a RelationContext<'_>,
    batch_size: u32,
) -> Result<RelationPromptContext<'a>, StageError> {
    let candidate_outcomes = build_candidate_outcomes(
        &ctx.candidates,
        &ctx.triage_results,
        &ctx.handoffs,
        batch_size,
    )?;

    Ok(RelationPromptContext {
        candidates: candidate_outcomes,
        relation_hints: &ctx.relation_hints,
        similar_item_decisions: &ctx.similar_item_decision_contexts,
    })
}

/// The ids the relation opening offers as citable references up front: each
/// in-batch candidate's committed id (created, or the item a duplicate
/// matched) and the existing claims triage surfaced. The loop's tool results
/// extend this set at submission time; together they are the provenance the
/// submission pipeline grounds endpoints against. Sourced from the loaded
/// context rather than the thread's job id, which a stage thread does not
/// carry.
pub(super) fn relation_citable_seed(ctx: &RelationContext<'_>) -> HashSet<KnowledgeItemId> {
    let mut seed = HashSet::new();
    for result in &ctx.triage_results {
        if result.batch_index() >= ctx.batch_size {
            continue;
        }
        match result.outcome() {
            TriageOutcome::Created { item_id } => {
                seed.insert(*item_id);
            }
            TriageOutcome::Duplicate {
                matched_item_id, ..
            } => {
                seed.insert(*matched_item_id);
            }
            TriageOutcome::Failed { .. } => {}
        }
    }
    for decision in &ctx.similar_item_decision_contexts {
        seed.insert(decision.matched_item_id);
    }
    seed
}

// ---------------------------------------------------------------------------
// Candidate outcomes
// ---------------------------------------------------------------------------

/// Builds the `CandidateOutcome` list by joining candidates with triage
/// results by batch index. Fails when `batch_size` exceeds the candidates
/// array length, which signals corruption between the extraction result
/// and the job's `batch_size` field.
fn build_candidate_outcomes<'a>(
    candidates: &'a [Candidate],
    triage_results: &[TriageResult],
    handoffs: &HashMap<u32, String>,
    batch_size: u32,
) -> Result<Vec<CandidateOutcome<'a>>, StageError> {
    let n_candidates = batch_size as usize;

    if n_candidates > candidates.len() {
        return Err(StageError::Database {
            stage: STAGE_RELATION.into(),
            context: format!(
                "batch_size ({batch_size}) exceeds candidates length ({})",
                candidates.len(),
            ),
            source: tribal_db::DbError::NotFound {
                entity: "candidates",
                id: format!("expected >= {batch_size}, found {}", candidates.len()),
            },
        });
    }

    let triage_by_index: HashMap<u32, &TriageResult> = triage_results
        .iter()
        .map(|r| (r.batch_index(), r))
        .collect();

    Ok(candidates
        .iter()
        .enumerate()
        .take(n_candidates)
        .map(|(i, candidate)| {
            let batch_index = clamp_to_u32(i);
            let (outcome, item_id) = match triage_by_index.get(&batch_index) {
                Some(result) => match result.outcome() {
                    TriageOutcome::Created { item_id } => ("created".into(), Some(*item_id)),
                    TriageOutcome::Duplicate {
                        matched_item_id, ..
                    } => ("duplicate".into(), Some(*matched_item_id)),
                    TriageOutcome::Failed { .. } => ("failed".into(), None),
                },
                None => ("failed".into(), None),
            };

            CandidateOutcome {
                batch_index,
                candidate,
                outcome,
                item_id,
                handoff: handoffs.get(&batch_index).cloned(),
            }
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Commit decision building
// ---------------------------------------------------------------------------

/// Normalises edges, computes the job outcome, and builds the commit
/// decision with `NewKnowledgeItemRelation` rows.
fn build_commit_decision(
    edges: Vec<RelationEdge>,
    lookup: &[Option<KnowledgeItemId>],
    triage_results: &[TriageResult],
    batch_size: u32,
    principal_id: PrincipalId,
) -> RelationCommitDecision {
    let (normalised, skipped) = normalise_edges(edges, lookup);
    let outcome = compute_outcome(triage_results, batch_size);
    let batch_id = RelationBatchId::new();

    let relations: Vec<NewKnowledgeItemRelation> = normalised
        .into_iter()
        .map(|edge| {
            NewKnowledgeItemRelation::builder()
                .relation_batch_id(batch_id)
                .source_id(edge.source_id)
                .target_id(edge.target_id)
                .relation_type(edge.relation_type)
                .principal_id(principal_id)
                .justification(edge.justification)
                .build()
        })
        .collect();

    RelationCommitDecision::Relate {
        relations,
        batch_id,
        outcome,
        skipped,
    }
}

// ---------------------------------------------------------------------------
// Unified lookup table
// ---------------------------------------------------------------------------

/// Builds a unified index-to-ID lookup covering all referenceable items:
/// indices `0..batch_size` resolve through triage outcomes, indices
/// `batch_size..` resolve directly from similar-item decision targets,
/// which are existing knowledge items whose IDs are already known.
fn build_unified_lookup(
    triage_results: &[TriageResult],
    similar_item_decisions: &[SimilarItemDecisionContext],
    batch_size: u32,
) -> Vec<Option<KnowledgeItemId>> {
    let n_candidates = batch_size as usize;
    let total = n_candidates + similar_item_decisions.len();
    let mut lookup = vec![None; total];

    for result in triage_results {
        let idx = result.batch_index() as usize;
        if idx < n_candidates {
            lookup[idx] = match result.outcome() {
                TriageOutcome::Created { item_id } => Some(*item_id),
                TriageOutcome::Duplicate {
                    matched_item_id, ..
                } => Some(*matched_item_id),
                TriageOutcome::Failed { .. } => None,
            };
        }
    }

    for (i, decision) in similar_item_decisions.iter().enumerate() {
        debug_assert_eq!(
            n_candidates + i,
            decision.context_index as usize,
            "context_index mismatch: decisions may have been reordered",
        );
        lookup[n_candidates + i] = Some(decision.matched_item_id);
    }

    lookup
}

// ---------------------------------------------------------------------------
// Edge normalisation
// ---------------------------------------------------------------------------

/// A resolved edge with concrete `KnowledgeItemId` endpoints.
struct ResolvedEdge {
    source_id: KnowledgeItemId,
    target_id: KnowledgeItemId,
    relation_type: RelationKind,
    justification: Option<String>,
}

/// Normalises raw relation edges into resolved, deduplicated edges,
/// dropping any with an unresolvable endpoint, self-edges, and duplicate
/// `(source_id, target_id, relation_type)` triples. `Supersedes` edges
/// cannot reach here: the schema-level [`IngestionRelationKind`] type
/// excludes the variant entirely.
fn normalise_edges(
    edges: Vec<RelationEdge>,
    lookup: &[Option<KnowledgeItemId>],
) -> (Vec<ResolvedEdge>, usize) {
    let mut seen = HashSet::new();
    let mut result = Vec::with_capacity(edges.len());
    let mut skipped: usize = 0;

    for edge in edges {
        let relation_type: RelationKind = edge.relation_type.into();

        let Some(source_id) = resolve_target(&edge.source, lookup) else {
            tracing::debug!(
                ?edge.source, ?edge.target, ?relation_type,
                "dropping edge: unresolvable source",
            );
            skipped += 1;
            continue;
        };
        let Some(target_id) = resolve_target(&edge.target, lookup) else {
            tracing::debug!(
                ?edge.source, ?edge.target, ?relation_type,
                "dropping edge: unresolvable target",
            );
            skipped += 1;
            continue;
        };

        if source_id == target_id {
            tracing::debug!(
                %source_id, ?relation_type,
                "dropping self-edge",
            );
            skipped += 1;
            continue;
        }

        let triple = (source_id, target_id, relation_type);
        if !seen.insert(triple) {
            tracing::debug!(
                %source_id, %target_id, ?relation_type,
                "dropping duplicate edge",
            );
            skipped += 1;
            continue;
        }

        result.push(ResolvedEdge {
            source_id,
            target_id,
            relation_type,
            justification: edge.justification,
        });
    }

    (result, skipped)
}

/// Resolves a single `RelationTarget` to a `KnowledgeItemId`.
fn resolve_target(
    target: &RelationTarget,
    lookup: &[Option<KnowledgeItemId>],
) -> Option<KnowledgeItemId> {
    match target {
        RelationTarget::ContextIndex { context_index } => {
            lookup.get(*context_index as usize).copied().flatten()
        }
    }
}

// ---------------------------------------------------------------------------
// Outcome computation
// ---------------------------------------------------------------------------

/// Computes the job outcome from triage results and batch size.
pub(super) fn compute_outcome(triage_results: &[TriageResult], batch_size: u32) -> JobOutcome {
    let n_created = triage_results
        .iter()
        .filter(|r| matches!(r.outcome(), TriageOutcome::Created { .. }))
        .count();
    let n_dead = batch_size.saturating_sub(clamp_to_u32(triage_results.len()));

    if n_created == 0 {
        JobOutcome::Empty
    } else if n_dead == 0 {
        JobOutcome::Success
    } else {
        JobOutcome::Partial
    }
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn relation_db_error(context: &str, source: tribal_db::DbError) -> StageError {
    StageError::Database {
        stage: STAGE_RELATION.into(),
        context: context.into(),
        source,
    }
}

fn relation_sqlx_error(context: &str, source: sqlx::Error) -> StageError {
    relation_db_error(
        context,
        tribal_db::DbError::QueryFailed {
            context: context.into(),
            source,
        },
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_domain::{
        KnowledgeItemId, RelationSuggestion, TriageOutcome, TriageResult, TriageResultId,
    };

    use super::*;
    use crate::parsing::IngestionRelationKind;

    fn ki(suffix: &str) -> KnowledgeItemId {
        format!("ki_{suffix}0000-0000-0000-0000-000000000000")
            .parse()
            .unwrap()
    }

    fn triage_result(batch_index: u32, outcome: TriageOutcome) -> TriageResult {
        TriageResult::builder()
            .id(TriageResultId::new())
            .job_id(tribal_domain::JobId::new())
            .batch_index(batch_index)
            .created_at(chrono::Utc::now())
            .outcome(outcome)
            .build()
    }

    fn edge(
        source: RelationTarget,
        target: RelationTarget,
        relation_type: IngestionRelationKind,
    ) -> RelationEdge {
        RelationEdge {
            source,
            target,
            relation_type,
            justification: None,
        }
    }

    fn created(batch_index: u32, ki_id: KnowledgeItemId) -> TriageResult {
        triage_result(batch_index, TriageOutcome::Created { item_id: ki_id })
    }

    fn duplicate(batch_index: u32, matched: KnowledgeItemId) -> TriageResult {
        triage_result(
            batch_index,
            TriageOutcome::Duplicate {
                observation_id: tribal_domain::ItemObservationId::new(),
                matched_item_id: matched,
            },
        )
    }

    fn similar_item(context_index: u32, matched_id: KnowledgeItemId) -> SimilarItemDecisionContext {
        SimilarItemDecisionContext {
            batch_index: 0,
            context_index,
            matched_item_id: matched_id,
            matched_content: String::new(),
            similarity_score: 0.5,
            similarity_label: String::new(),
            suggested_relation: RelationSuggestion::Supports,
            justification: String::new(),
        }
    }

    // -- normalise_edges tests --

    #[test]
    fn test_normalise_resolves_created_to_item_id() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let lookup = vec![Some(ki_a), Some(ki_b)];
        let edges = vec![edge(
            RelationTarget::ContextIndex { context_index: 0 },
            RelationTarget::ContextIndex { context_index: 1 },
            IngestionRelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert_eq!(normalised.len(), 1);
        assert_eq!(normalised[0].source_id, ki_a);
        assert_eq!(normalised[0].target_id, ki_b);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_normalise_resolves_duplicate_to_matched_item_id() {
        // Candidate 0 was created as ki_created.
        // Candidate 1 was flagged as a duplicate of ki_existing.
        // An edge from index 0 → index 1 should resolve the
        // duplicate's target to ki_existing (the matched item),
        // not to the candidate itself (which was never created).
        let ki_created = ki("aaaa");
        let ki_existing = ki("cccc");
        let lookup = vec![Some(ki_created), Some(ki_existing)];
        let edges = vec![edge(
            RelationTarget::ContextIndex { context_index: 0 },
            RelationTarget::ContextIndex { context_index: 1 },
            IngestionRelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert_eq!(normalised.len(), 1);
        assert_eq!(normalised[0].source_id, ki_created);
        assert_eq!(normalised[0].target_id, ki_existing);
        assert_eq!(skipped, 0);
    }

    /// The handoff extraction reads each submission's notes by batch
    /// index, drops a submission that carried none, and skips a malformed
    /// one rather than failing the stage.
    #[test]
    fn test_handoffs_by_batch_index_extracts_skips_and_drops() {
        let submission = |batch_index: u32, content: serde_json::Value| JobTriageSubmission {
            batch_index,
            content,
        };
        let with_handoff = submission(
            0,
            serde_json::json!({
                "payload": { "decision": { "decision": "created" }, "handoff": "links auth and billing" },
                "requesting_seq": 1,
                "tool_call_id": "call_submit",
            }),
        );
        let no_handoff = submission(
            1,
            serde_json::json!({
                "payload": { "decision": { "decision": "created" } },
                "requesting_seq": 1,
                "tool_call_id": "call_submit",
            }),
        );
        let malformed = submission(2, serde_json::json!({ "not": "a submission" }));

        let handoffs = handoffs_by_batch_index(vec![with_handoff, no_handoff, malformed]);

        assert_eq!(
            handoffs.len(),
            1,
            "only the submission that carried a handoff contributes"
        );
        assert_eq!(
            handoffs.get(&0).map(String::as_str),
            Some("links auth and billing"),
        );
    }

    #[test]
    fn test_build_candidate_outcomes_attaches_handoffs_by_batch_index() {
        let candidates: Vec<Candidate> = (0..2)
            .map(|i| {
                serde_json::from_value(serde_json::json!({
                    "kind": "fact",
                    "content": format!("candidate {i}"),
                    "suggested_tags": [],
                }))
                .expect("candidate")
            })
            .collect();
        let triage_results = vec![created(0, ki("aaaa")), created(1, ki("bbbb"))];
        let mut handoffs = HashMap::new();
        handoffs.insert(0, "the candidate links auth and billing".to_owned());

        let outcomes = build_candidate_outcomes(&candidates, &triage_results, &handoffs, 2)
            .expect("outcomes build");
        assert_eq!(
            outcomes[0].handoff.as_deref(),
            Some("the candidate links auth and billing"),
            "the handoff rides its candidate by batch index",
        );
        assert_eq!(
            outcomes[1].handoff, None,
            "a candidate with no handoff carries none",
        );
    }

    #[test]
    fn test_normalise_drops_unresolvable_index() {
        let ki_a = ki("aaaa");
        let lookup = vec![Some(ki_a)];
        let edges = vec![edge(
            RelationTarget::ContextIndex { context_index: 0 },
            RelationTarget::ContextIndex { context_index: 5 },
            IngestionRelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert!(normalised.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_drops_failed_candidate() {
        let ki_a = ki("aaaa");
        // Index 1 is None: corresponds to a failed triage outcome.
        let lookup = vec![Some(ki_a), None];
        let edges = vec![edge(
            RelationTarget::ContextIndex { context_index: 0 },
            RelationTarget::ContextIndex { context_index: 1 },
            IngestionRelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert!(normalised.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_drops_self_edges() {
        let ki_a = ki("aaaa");
        let lookup = vec![Some(ki_a)];
        let edges = vec![edge(
            RelationTarget::ContextIndex { context_index: 0 },
            RelationTarget::ContextIndex { context_index: 0 },
            IngestionRelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert!(normalised.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_deduplicates_triples() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let lookup = vec![Some(ki_a), Some(ki_b)];
        let edges = vec![
            edge(
                RelationTarget::ContextIndex { context_index: 0 },
                RelationTarget::ContextIndex { context_index: 1 },
                IngestionRelationKind::Supports,
            ),
            edge(
                RelationTarget::ContextIndex { context_index: 0 },
                RelationTarget::ContextIndex { context_index: 1 },
                IngestionRelationKind::Supports,
            ),
        ];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert_eq!(normalised.len(), 1);
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_preserves_inverse_edges() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let lookup = vec![Some(ki_a), Some(ki_b)];
        let edges = vec![
            edge(
                RelationTarget::ContextIndex { context_index: 0 },
                RelationTarget::ContextIndex { context_index: 1 },
                IngestionRelationKind::Supports,
            ),
            edge(
                RelationTarget::ContextIndex { context_index: 1 },
                RelationTarget::ContextIndex { context_index: 0 },
                IngestionRelationKind::Supports,
            ),
        ];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert_eq!(normalised.len(), 2);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_normalise_resolves_similar_item_index() {
        // 2 candidates + 2 similar items in the unified lookup.
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let ki_similar_1 = ki("cccc");
        let ki_similar_2 = ki("dddd");
        let lookup = vec![
            Some(ki_a),
            Some(ki_b),
            Some(ki_similar_1),
            Some(ki_similar_2),
        ];
        let edges = vec![edge(
            RelationTarget::ContextIndex { context_index: 0 },
            RelationTarget::ContextIndex { context_index: 2 },
            IngestionRelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert_eq!(normalised.len(), 1);
        assert_eq!(normalised[0].source_id, ki_a);
        assert_eq!(normalised[0].target_id, ki_similar_1);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_normalise_mixed_candidate_and_similar_item_edges() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let ki_similar = ki("cccc");
        let lookup = vec![Some(ki_a), Some(ki_b), Some(ki_similar)];
        let edges = vec![
            // Candidate → candidate.
            edge(
                RelationTarget::ContextIndex { context_index: 0 },
                RelationTarget::ContextIndex { context_index: 1 },
                IngestionRelationKind::Supports,
            ),
            // Candidate → similar item.
            edge(
                RelationTarget::ContextIndex { context_index: 1 },
                RelationTarget::ContextIndex { context_index: 2 },
                IngestionRelationKind::Contradicts,
            ),
        ];

        let (normalised, skipped) = normalise_edges(edges, &lookup);
        assert_eq!(normalised.len(), 2);
        assert_eq!(normalised[0].source_id, ki_a);
        assert_eq!(normalised[0].target_id, ki_b);
        assert_eq!(normalised[1].source_id, ki_b);
        assert_eq!(normalised[1].target_id, ki_similar);
        assert_eq!(skipped, 0);
    }

    // -- build_unified_lookup tests --

    #[test]
    fn test_lookup_maps_created_and_duplicate_triage_outcomes() {
        let ki_created = ki("aaaa");
        let ki_matched = ki("bbbb");
        let triage = vec![created(0, ki_created), duplicate(1, ki_matched)];

        let lookup = build_unified_lookup(&triage, &[], 2);

        assert_eq!(lookup.len(), 2);
        assert_eq!(lookup[0], Some(ki_created));
        assert_eq!(lookup[1], Some(ki_matched));
    }

    #[test]
    fn test_lookup_maps_failed_triage_to_none() {
        let triage = vec![triage_result(
            0,
            TriageOutcome::Failed {
                error_message: "test failure".into(),
                retryable: false,
            },
        )];

        let lookup = build_unified_lookup(&triage, &[], 1);

        assert_eq!(lookup.len(), 1);
        assert_eq!(lookup[0], None);
    }

    #[test]
    fn test_lookup_appends_similar_items_after_candidates() {
        let ki_candidate = ki("aaaa");
        let ki_similar_1 = ki("bbbb");
        let ki_similar_2 = ki("cccc");
        let triage = vec![created(0, ki_candidate)];
        let decisions = vec![similar_item(1, ki_similar_1), similar_item(2, ki_similar_2)];

        let lookup = build_unified_lookup(&triage, &decisions, 1);

        assert_eq!(lookup.len(), 3);
        assert_eq!(lookup[0], Some(ki_candidate));
        assert_eq!(lookup[1], Some(ki_similar_1));
        assert_eq!(lookup[2], Some(ki_similar_2));
    }

    #[test]
    fn test_lookup_ignores_triage_result_beyond_batch_size() {
        let ki_a = ki("aaaa");
        let ki_stray = ki("bbbb");
        let triage = vec![created(0, ki_a), created(5, ki_stray)];

        let lookup = build_unified_lookup(&triage, &[], 2);

        assert_eq!(lookup.len(), 2);
        assert_eq!(lookup[0], Some(ki_a));
        assert_eq!(lookup[1], None);
    }

    #[test]
    fn test_lookup_empty_decisions_produces_batch_size_length() {
        let triage = vec![created(0, ki("aaaa"))];

        let lookup = build_unified_lookup(&triage, &[], 3);

        assert_eq!(lookup.len(), 3);
        assert_eq!(lookup[0], Some(ki("aaaa")));
        assert_eq!(lookup[1], None);
        assert_eq!(lookup[2], None);
    }

    // -- compute_outcome tests --

    #[test]
    fn test_outcome_success_when_all_created() {
        let results = vec![created(0, ki("aaaa")), created(1, ki("bbbb"))];
        assert_eq!(compute_outcome(&results, 2), JobOutcome::Success);
    }

    #[test]
    fn test_outcome_empty_when_none_created() {
        let results = vec![duplicate(0, ki("aaaa")), duplicate(1, ki("bbbb"))];
        assert_eq!(compute_outcome(&results, 2), JobOutcome::Empty);
    }

    #[test]
    fn test_outcome_partial_when_some_dead_lettered() {
        // batch_size=2 but only 1 triage result exists: the missing
        // result means the other triage task was dead-lettered (no
        // TriageResult row is written for dead-lettered tasks).
        // With n_created=1 and n_dead=1, the outcome is Partial.
        let results = vec![created(0, ki("aaaa"))];
        assert_eq!(compute_outcome(&results, 2), JobOutcome::Partial);
    }

    #[test]
    fn test_outcome_empty_when_all_duplicate() {
        let results = vec![duplicate(0, ki("aaaa"))];
        assert_eq!(compute_outcome(&results, 1), JobOutcome::Empty);
    }
}
