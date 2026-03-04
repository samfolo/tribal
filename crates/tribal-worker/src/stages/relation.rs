//! Relation stage: LLM-based relation extraction and commit.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::Semaphore;
use tracing::Instrument;
use tribal_db::{
    ExtractionResultRepository, KnowledgeItemRepository, NewKnowledgeItemRelation,
    PgExtractionResultRepository, PgKnowledgeItemRepository, PgTriageResultRepository,
    PgTriageSimilarItemDecisionRepository, TriageResultRepository,
    TriageSimilarItemDecisionRepository,
};
use tribal_domain::{
    Candidate, Job, JobOutcome, KnowledgeItemId, PrincipalId, RelationBatchId, RelationHint,
    RelationKind, Task, TriageOutcome, TriageResult, TriageSimilarItemDecision, span_attrs,
};
use tribal_inference::{InferenceProvider, ProviderKey, Usage};

use super::{StageCommit, StageOutput};
use crate::{
    common::{PARSE_PREVIEW_LENGTH, clamp_to_u32},
    error::{SEMAPHORE_CLOSED, STAGE_RELATION, StageError},
    parsing::{RelationEdge, RelationTarget, parse_relation_response},
    prompt::{
        CandidateOutcome, RelationPromptContext, SimilarItemDecisionContext,
        assemble_relation_prompt,
    },
    worker::Worker,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EXPECT_RELATION_KEY: &str = "relation key registered at startup";

// ---------------------------------------------------------------------------
// RelationContext
// ---------------------------------------------------------------------------

/// Context assembled before running the relation stage.
///
/// This is the richest context of any stage — it carries the complete
/// episode picture for the relation agent.
pub(crate) struct RelationContext<'a> {
    /// The parent job.
    pub job: &'a Job,
    /// Typed candidates, deserialised from the extraction result.
    pub candidates: Vec<Candidate>,
    /// Typed relation hints, deserialised from the extraction result.
    pub relation_hints: Vec<RelationHint>,
    /// All triage results for this job.
    pub triage_results: Vec<TriageResult>,
    /// Enriched similar item decisions with matched item content.
    pub similar_item_decision_contexts: Vec<SimilarItemDecisionContext>,
}

// ---------------------------------------------------------------------------
// Commit decision
// ---------------------------------------------------------------------------

/// The relation stage's commit decision.
pub(crate) enum RelationCommitDecision {
    /// Relations to commit with the job's terminal status.
    Relate {
        /// The relations to batch-insert.
        relations: Vec<NewKnowledgeItemRelation>,
        /// The batch ID sealing this relation commit.
        batch_id: RelationBatchId,
        /// The computed job outcome.
        outcome: JobOutcome,
        /// Number of edges dropped during normalisation.
        skipped: usize,
    },
    /// Idempotency skip — `committed_batch_id` already set.
    NoOp,
}

// ---------------------------------------------------------------------------
// Relation accessors
// ---------------------------------------------------------------------------

impl Worker {
    /// Returns a reference to the relation inference provider.
    pub(crate) fn relation_provider(&self) -> &Arc<dyn InferenceProvider> {
        &self.relation_provider
    }

    /// Returns the relation provider key.
    pub(crate) fn relation_key(&self) -> &ProviderKey {
        &self.relation_key
    }

    /// Returns the relation semaphore from the provider registry.
    ///
    /// # Panics
    ///
    /// Panics if the relation key is not registered in the provider
    /// registry.
    pub(crate) fn relation_semaphore(&self) -> &Arc<Semaphore> {
        self.provider_registry()
            .semaphore(self.relation_key())
            .expect(EXPECT_RELATION_KEY)
    }
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

impl Worker {
    /// Loads all relation stage inputs from the database using a single
    /// pooled connection and returns the assembled context.
    async fn load_relation_data<'a>(
        &self,
        job: &'a Job,
    ) -> Result<RelationContext<'a>, StageError> {
        let mut conn = self
            .pool()
            .acquire()
            .await
            .map_err(|e| relation_sqlx_error("acquiring connection", e))?;

        let (candidates, relation_hints) = load_extraction_data(&mut conn, job).await?;
        let triage_results = load_triage_results(&mut conn, job).await?;
        let similar_item_decisions = load_similar_item_decisions(&mut conn, job).await?;
        let similar_item_decision_contexts =
            build_similar_item_decision_contexts(&mut conn, &similar_item_decisions).await?;

        drop(conn);

        Ok(RelationContext {
            job,
            candidates,
            relation_hints,
            triage_results,
            similar_item_decision_contexts,
        })
    }
}

// ---------------------------------------------------------------------------
// Stage implementation
// ---------------------------------------------------------------------------

impl Worker {
    /// Runs the relation stage for a task.
    ///
    /// Loads triage results and extraction relation hints, calls the
    /// relation LLM, normalises and filters edges, and returns a
    /// [`StageOutput`] ready for atomic commit.
    ///
    /// Returns early with a no-op if `committed_batch_id` is already
    /// set (idempotency guard).
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] variant matching the failure mode:
    /// - [`StageError::Database`] for pool/repository failures.
    /// - [`StageError::SemaphoreTimeout`] if the provider semaphore
    ///   cannot be acquired within the remaining time budget.
    /// - [`StageError::TemplateRender`] if the prompt template is invalid.
    /// - [`StageError::Provider`] if the LLM call fails.
    /// - [`StageError::Parse`] if the LLM response cannot be parsed.
    ///
    /// # Panics
    ///
    /// Panics if the relation provider key is not registered in the
    /// provider registry or if the semaphore is unexpectedly closed.
    pub(crate) async fn run_relation(
        &self,
        job: &Job,
        task: &Task,
        deadline: tokio::time::Instant,
    ) -> Result<StageOutput, StageError> {
        let span = tracing::info_span!(
            "tribal.task.relation",
            { span_attrs::TASK_ID } = %task.id(),
            { span_attrs::LLM_STAGE } = "relation",
            { span_attrs::RETRY_COUNT } = task.retry_count(),
        );

        async {
            // Idempotency guard.
            if job.committed_batch_id().is_some() {
                return Ok(StageOutput {
                    commit: StageCommit::Relation {
                        decision: RelationCommitDecision::NoOp,
                    },
                    usages: vec![],
                });
            }

            let include_llm_content = self.config().include_llm_content;

            let ctx = self.load_relation_data(job).await?;

            let batch_size = ctx.job.batch_size().ok_or_else(|| StageError::Database {
                stage: STAGE_RELATION.into(),
                context: format!("job {} has no batch_size set", ctx.job.id()),
                source: tribal_db::DbError::NotFound {
                    entity: "job.batch_size",
                    id: ctx.job.id().to_string(),
                },
            })?;

            let prompt_context = build_prompt_context(&ctx, batch_size)?;

            let system_pv = self
                .load_prompt_version(STAGE_RELATION, ctx.job.relation_system_prompt_version_id())
                .await?;
            let user_pv = self
                .load_prompt_version(STAGE_RELATION, ctx.job.relation_user_prompt_version_id())
                .await?;

            // Acquire semaphore.
            let semaphore = self.relation_semaphore();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let _permit = tokio::time::timeout(remaining, Arc::clone(semaphore).acquire_owned())
                .await
                .map_err(|_| StageError::SemaphoreTimeout {
                    provider_key: format!("{:?}", self.relation_key()),
                })?
                .expect(SEMAPHORE_CLOSED);

            let request = assemble_relation_prompt(
                system_pv.content(),
                user_pv.content(),
                &prompt_context,
            )?;

            if include_llm_content {
                tracing::debug!(
                    system_prompt = %request.system.as_deref().unwrap_or(""),
                    user_prompt = %request.messages.first().map(|m| m.content.as_str()).unwrap_or(""),
                    "relation prompt assembled",
                );
            }

            let response = self
                .relation_provider()
                .complete(request)
                .await
                .map_err(|e| StageError::Provider {
                    context: "relation LLM call".into(),
                    source: e,
                })?;

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
                        tracing::debug!(preview = %preview, "parse failure — raw LLM response");
                    } else {
                        tracing::debug!(
                            response_length = response.text.len(),
                            "parse failure — response details redacted",
                        );
                    }
                })?
            };

            let decision = build_commit_decision(
                output.relations,
                &ctx.triage_results,
                batch_size,
                ctx.job.principal_id(),
            );

            Ok(StageOutput {
                commit: StageCommit::Relation { decision },
                usages: vec![Usage::Completion(response.usage)],
            })
        }
        .instrument(span)
        .await
    }
}

// ---------------------------------------------------------------------------
// Private helpers — data loading
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

    decisions
        .iter()
        .map(|d| {
            let content = content_by_id.get(&d.matched_item_id()).ok_or_else(|| {
                relation_db_error(
                    "similar item decision refers to missing knowledge item",
                    tribal_db::DbError::NotFound {
                        entity: "knowledge_item",
                        id: d.matched_item_id().to_string(),
                    },
                )
            })?;

            Ok(SimilarItemDecisionContext {
                batch_index: d.batch_index(),
                matched_item_id: d.matched_item_id(),
                matched_content: content.clone(),
                similarity_score: d.similarity_score(),
                suggested_relation: d.suggested_relation(),
                justification: d.justification_text().to_owned(),
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Prompt context assembly
// ---------------------------------------------------------------------------

/// Builds the `RelationPromptContext` from the loaded relation data.
///
/// This is a pure, synchronous transformation — no database access.
/// Borrows from the `RelationContext` to avoid cloning.
///
/// # Errors
///
/// Returns [`StageError::Database`] if `batch_size` exceeds the
/// candidates array length (data corruption).
fn build_prompt_context<'a>(
    ctx: &'a RelationContext<'_>,
    batch_size: u32,
) -> Result<RelationPromptContext<'a>, StageError> {
    let candidate_outcomes = build_candidate_outcomes(&ctx.candidates, &ctx.triage_results, batch_size)?;

    Ok(RelationPromptContext {
        candidates: candidate_outcomes,
        relation_hints: &ctx.relation_hints,
        similar_item_decisions: &ctx.similar_item_decision_contexts,
    })
}

// ---------------------------------------------------------------------------
// Candidate outcomes
// ---------------------------------------------------------------------------

/// Builds the `CandidateOutcome` list by joining candidates with triage
/// results by batch index.
///
/// # Errors
///
/// Returns [`StageError::Database`] if `batch_size` exceeds the
/// candidates array length — this indicates data corruption between
/// the extraction result and the job's `batch_size` field.
fn build_candidate_outcomes<'a>(
    candidates: &'a [Candidate],
    triage_results: &[TriageResult],
    batch_size: u32,
) -> Result<Vec<CandidateOutcome<'a>>, StageError> {
    if (batch_size as usize) > candidates.len() {
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
        .take(batch_size as usize)
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
    triage_results: &[TriageResult],
    batch_size: u32,
    principal_id: PrincipalId,
) -> RelationCommitDecision {
    let (normalised, skipped) = normalise_edges(edges, triage_results);
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
// Edge normalisation
// ---------------------------------------------------------------------------

/// A resolved edge with concrete `KnowledgeItemId` endpoints.
struct ResolvedEdge {
    source_id: KnowledgeItemId,
    target_id: KnowledgeItemId,
    relation_type: RelationKind,
    justification: Option<String>,
}

/// Normalises raw relation edges into resolved, deduplicated edges.
///
/// Steps:
/// 1. Drop `Supersedes` edges.
/// 2. Resolve `BatchIndex` endpoints (sources and targets) to `KnowledgeItemId` via triage results.
/// 3. Drop edges with any unresolvable endpoint.
/// 4. Drop self-edges.
/// 5. Deduplicate `(source_id, target_id, relation_type)` triples.
fn normalise_edges(
    edges: Vec<RelationEdge>,
    triage_results: &[TriageResult],
) -> (Vec<ResolvedEdge>, usize) {
    let triage_by_index: HashMap<u32, &TriageResult> = triage_results
        .iter()
        .map(|r| (r.batch_index(), r))
        .collect();

    let mut seen = HashSet::new();
    let mut result = Vec::with_capacity(edges.len());
    let mut skipped: usize = 0;

    for edge in edges {
        // Step 1: drop Supersedes.
        if edge.relation_type == RelationKind::Supersedes {
            tracing::debug!(
                ?edge.source, ?edge.target,
                "dropping supersedes edge",
            );
            skipped += 1;
            continue;
        }

        // Step 2+3: resolve targets.
        let Some(source_id) = resolve_target(&edge.source, &triage_by_index) else {
            tracing::debug!(
                ?edge.source, ?edge.target, relation_type = ?edge.relation_type,
                "dropping edge — unresolvable source",
            );
            skipped += 1;
            continue;
        };
        let Some(target_id) = resolve_target(&edge.target, &triage_by_index) else {
            tracing::debug!(
                ?edge.source, ?edge.target, relation_type = ?edge.relation_type,
                "dropping edge — unresolvable target",
            );
            skipped += 1;
            continue;
        };

        // Step 4: drop self-edges.
        if source_id == target_id {
            tracing::debug!(
                %source_id, relation_type = ?edge.relation_type,
                "dropping self-edge",
            );
            skipped += 1;
            continue;
        }

        // Step 5: deduplicate.
        let triple = (source_id, target_id, edge.relation_type);
        if !seen.insert(triple) {
            tracing::debug!(
                %source_id, %target_id, relation_type = ?edge.relation_type,
                "dropping duplicate edge",
            );
            skipped += 1;
            continue;
        }

        result.push(ResolvedEdge {
            source_id,
            target_id,
            relation_type: edge.relation_type,
            justification: edge.justification,
        });
    }

    (result, skipped)
}

/// Resolves a single `RelationTarget` to a `KnowledgeItemId`.
fn resolve_target(
    target: &RelationTarget,
    triage_by_index: &HashMap<u32, &TriageResult>,
) -> Option<KnowledgeItemId> {
    match target {
        RelationTarget::ItemId { item_id } => Some(*item_id),
        RelationTarget::BatchIndex { batch_index } => {
            let result = triage_by_index.get(batch_index)?;
            match result.outcome() {
                TriageOutcome::Created { item_id } => Some(*item_id),
                TriageOutcome::Duplicate {
                    matched_item_id, ..
                } => Some(*matched_item_id),
                TriageOutcome::Failed { .. } => None,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Outcome computation
// ---------------------------------------------------------------------------

/// Computes the job outcome from triage results and batch size.
fn compute_outcome(triage_results: &[TriageResult], batch_size: u32) -> JobOutcome {
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
        KnowledgeItemId, RelationKind, TriageOutcome, TriageResult, TriageResultId,
    };

    use super::*;

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
        relation_type: RelationKind,
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

    // -- normalise_edges tests --

    #[test]
    fn test_normalise_drops_supersedes_edges() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let results = vec![created(0, ki_a), created(1, ki_b)];
        let edges = vec![edge(
            RelationTarget::BatchIndex { batch_index: 0 },
            RelationTarget::BatchIndex { batch_index: 1 },
            RelationKind::Supersedes,
        )];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert!(normalised.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_resolves_created_to_item_id() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let results = vec![created(0, ki_a), created(1, ki_b)];
        let edges = vec![edge(
            RelationTarget::BatchIndex { batch_index: 0 },
            RelationTarget::BatchIndex { batch_index: 1 },
            RelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert_eq!(normalised.len(), 1);
        assert_eq!(normalised[0].source_id, ki_a);
        assert_eq!(normalised[0].target_id, ki_b);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_normalise_resolves_duplicate_to_matched_item_id() {
        // Candidate 0 was created as ki_created.
        // Candidate 1 was flagged as a duplicate of ki_existing.
        // An edge from batch 0 → batch 1 should resolve the
        // duplicate's target to ki_existing (the matched item),
        // not to the candidate itself (which was never created).
        let ki_created = ki("aaaa");
        let ki_existing = ki("cccc");
        let results = vec![created(0, ki_created), duplicate(1, ki_existing)];
        let edges = vec![edge(
            RelationTarget::BatchIndex { batch_index: 0 },
            RelationTarget::BatchIndex { batch_index: 1 },
            RelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert_eq!(normalised.len(), 1);
        assert_eq!(normalised[0].source_id, ki_created);
        assert_eq!(normalised[0].target_id, ki_existing);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_normalise_drops_unresolved_batch_index() {
        let ki_a = ki("aaaa");
        let results = vec![created(0, ki_a)];
        let edges = vec![edge(
            RelationTarget::BatchIndex { batch_index: 0 },
            RelationTarget::BatchIndex { batch_index: 5 },
            RelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert!(normalised.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_drops_failed_batch_index() {
        let ki_a = ki("aaaa");
        let results = vec![
            created(0, ki_a),
            triage_result(
                1,
                TriageOutcome::Failed {
                    error_message: "test failure".into(),
                    retryable: false,
                },
            ),
        ];
        let edges = vec![edge(
            RelationTarget::BatchIndex { batch_index: 0 },
            RelationTarget::BatchIndex { batch_index: 1 },
            RelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert!(normalised.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_drops_self_edges() {
        let ki_a = ki("aaaa");
        let results = vec![created(0, ki_a)];
        let edges = vec![edge(
            RelationTarget::BatchIndex { batch_index: 0 },
            RelationTarget::BatchIndex { batch_index: 0 },
            RelationKind::Supports,
        )];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert!(normalised.is_empty());
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_deduplicates_triples() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let results = vec![created(0, ki_a), created(1, ki_b)];
        let edges = vec![
            edge(
                RelationTarget::BatchIndex { batch_index: 0 },
                RelationTarget::BatchIndex { batch_index: 1 },
                RelationKind::Supports,
            ),
            edge(
                RelationTarget::BatchIndex { batch_index: 0 },
                RelationTarget::BatchIndex { batch_index: 1 },
                RelationKind::Supports,
            ),
        ];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert_eq!(normalised.len(), 1);
        assert_eq!(skipped, 1);
    }

    #[test]
    fn test_normalise_preserves_inverse_edges() {
        let ki_a = ki("aaaa");
        let ki_b = ki("bbbb");
        let results = vec![created(0, ki_a), created(1, ki_b)];
        let edges = vec![
            edge(
                RelationTarget::BatchIndex { batch_index: 0 },
                RelationTarget::BatchIndex { batch_index: 1 },
                RelationKind::Supports,
            ),
            edge(
                RelationTarget::BatchIndex { batch_index: 1 },
                RelationTarget::BatchIndex { batch_index: 0 },
                RelationKind::Supports,
            ),
        ];

        let (normalised, skipped) = normalise_edges(edges, &results);
        assert_eq!(normalised.len(), 2);
        assert_eq!(skipped, 0);
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
        // batch_size=2 but only 1 triage result exists — the missing
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
