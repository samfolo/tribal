//! The deterministic validators behind `submit_result`: never a model call.
//!
//! A model-addressable rejection bounces with diagnostics the loop's model
//! can correct against. The graph re-checks run at evaluation time, so the
//! optimistic reads the agent's tools established are re-validated at the
//! commit boundary.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use serde::Deserialize;
use sqlx::PgConnection;
use tribal_agent_runtime::{
    RecordedMessage, RenderedConversation, SeenCorpus, SubmissionOutcome, SubmissionPipeline,
    ToolResultContent, VerifierLaunch, verdict_schema,
};
use tribal_db::{
    AgentThreadRecordRepository, DbError, KnowledgeItemRepository, PgAgentThreadRecordRepository,
    PgKnowledgeItemRepository, PgRelationRepository, RelationRepository,
};
use tribal_domain::{
    AgentBindingVersionId, AgentThread, AgentThreadRecordKind, Candidate, KnowledgeItem,
    KnowledgeItemId, ProjectId, PromptVersionId, RelationKind, RelationSuggestion, TaskType,
    ToolFailure,
};

use crate::{
    parsing::{TriageSubmission, TriageSubmissionDecision},
    prompt::{VerifierConsideredItem, VerifierSubmissionContext, assemble_verifier_input},
};

/// One candidate-search result page from the thread log. The
/// `embedding_profile_id` both selects the page (only a candidate-search
/// record carries it) and pins its scores to one embedding geometry, so a
/// profile cutover does not merge scores across incomparable vector spaces.
#[derive(Deserialize)]
struct CandidateSearchPage {
    results: Vec<CandidateScoreItem>,
    embedding_profile_id: String,
}

#[derive(Deserialize)]
struct CandidateScoreItem {
    item_id: String,
    similarity_score: f64,
}

/// What a thread's candidate searches established: whether at least one
/// search ran, and the highest similarity score recorded per item.
///
/// The two are deliberately distinct. An empty `scores` with `searched` set
/// is a real search that found nothing (the first item into an empty graph),
/// which is allowed to conclude novel; an empty `scores` with `searched`
/// clear is a submission that skipped the search entirely, which is not.
pub(crate) struct CandidateSearchProvenance {
    pub searched: bool,
    pub scores: HashMap<String, f64>,
}

/// Reconstructs the candidate-search provenance from a thread's tool
/// results, reading it from the committed record log so it survives a resume
/// intact. A successful search records a page even when it returned no items,
/// so `searched` tracks the page's presence, not the score count.
///
/// Scores are pinned to a single embedding profile: a profile change drops
/// the superseded pages rather than merging scores across incomparable
/// geometries, so an item the new profile does not surface cannot ground a
/// decision on a stale score.
///
/// # Errors
///
/// Returns [`DbError`] when the records cannot be read.
pub(crate) async fn reconstruct_candidate_scores(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<CandidateSearchProvenance, DbError> {
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(conn, thread.id())
        .await?;
    let mut searched = false;
    let mut profile: Option<String> = None;
    let mut scores: HashMap<String, f64> = HashMap::new();
    for record in records {
        if record.kind() != AgentThreadRecordKind::ToolResult {
            continue;
        }
        let Ok(result) = serde_json::from_value::<ToolResultContent>(record.content().clone())
        else {
            continue;
        };
        if result.is_error {
            continue;
        }
        // Only a candidate-search page carries the embedding_profile_id
        // marker, so any other tool's result legitimately does not parse here.
        let Ok(page) = serde_json::from_str::<CandidateSearchPage>(&result.output) else {
            continue;
        };
        // A candidate-search page parsed: a search ran, whatever it returned.
        searched = true;
        // A profile change supersedes the earlier pages' geometry. Cutover is
        // monotone (the rejected cursor forces a restart under the new
        // profile), so dropping the prior scores leaves the run under the
        // current profile alone.
        if profile.as_deref() != Some(page.embedding_profile_id.as_str()) {
            scores.clear();
            profile = Some(page.embedding_profile_id);
        }
        for item in page.results {
            scores
                .entry(item.item_id)
                .and_modify(|best| {
                    if item.similarity_score > *best {
                        *best = item.similarity_score;
                    }
                })
                .or_insert(item.similarity_score);
        }
    }
    Ok(CandidateSearchProvenance { searched, scores })
}

/// The verifier configuration one stage execution carries: everything the
/// launch needs to render a fresh-context child without reaching back into
/// worker state.
pub(crate) struct VerifierContext {
    pub binding_version_id: AgentBindingVersionId,
    pub system_template: String,
    pub system_prompt_version_id: PromptVersionId,
    pub user_template: String,
    pub user_prompt_version_id: PromptVersionId,
    pub candidate: Candidate,
}

/// The triage stage's submission pipeline, fenced to one project.
pub(crate) struct TriageSubmissionPipeline {
    project_id: ProjectId,
    /// The verifier, when the binding configures one; absent leaves an
    /// accepted submission to commit on the validators alone.
    verifier: Option<VerifierContext>,
}

impl TriageSubmissionPipeline {
    /// Creates the pipeline for one stage execution, with no verifier.
    pub(crate) fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            verifier: None,
        }
    }

    /// Attaches the verifier the binding resolved, when one is configured.
    pub(crate) fn with_verifier(mut self, verifier: Option<VerifierContext>) -> Self {
        self.verifier = verifier;
        self
    }

    /// Resolves one considered claim's content, scoped to this project; a
    /// vanished or foreign id is skipped rather than failing the launch.
    async fn verifier_item(
        &self,
        conn: &mut PgConnection,
        raw_id: &str,
    ) -> Result<Option<KnowledgeItem>, ToolFailure> {
        let Ok(id) = raw_id.parse::<KnowledgeItemId>() else {
            return Ok(None);
        };
        match PgKnowledgeItemRepository.find_by_id(conn, id).await {
            Ok(item) if item.project_id() == self.project_id => Ok(Some(item)),
            // A foreign-project or vanished id is not verifier context; the
            // launch skips it rather than failing.
            Ok(_) | Err(DbError::NotFound { .. }) => Ok(None),
            Err(source) => Err(ToolFailure::System {
                context: format!("loading a considered claim for verification: {source}"),
            }),
        }
    }

    /// Builds the verifier's view of the claims the submission assessed,
    /// resolving each one's content. The duplicated claim is included even
    /// when the submission carried no assessment for it, so the verifier
    /// can judge the wrong-duplicate defect against its content.
    async fn verifier_considered_items(
        &self,
        conn: &mut PgConnection,
        submission: &TriageSubmission,
        matched: Option<&str>,
    ) -> Result<Vec<VerifierConsideredItem>, ToolFailure> {
        let mut items = Vec::new();
        // Deduplicated on the resolved item id, not the raw string, so two
        // spellings of one id cannot render the same claim twice.
        let mut seen: HashSet<KnowledgeItemId> = HashSet::new();
        for assessment in &submission.considered_items {
            if let Some(item) = self.verifier_item(conn, &assessment.item_id).await? {
                seen.insert(item.id());
                items.push(VerifierConsideredItem {
                    item_id: assessment.item_id.clone(),
                    kind: item.kind(),
                    assessment: format!(
                        "suggested relation {}: {}",
                        assessment.suggested_relation.as_str(),
                        assessment.justification,
                    ),
                    content: item.content().to_owned(),
                });
            }
        }
        // The duplicated claim is surfaced even when the submission carried
        // no assessment for it, but never twice.
        if let Some(matched_id) = matched
            && let Some(item) = self.verifier_item(conn, matched_id).await?
            && seen.insert(item.id())
        {
            items.push(VerifierConsideredItem {
                item_id: matched_id.to_owned(),
                kind: item.kind(),
                assessment: "classified as the duplicated claim".to_owned(),
                content: item.content().to_owned(),
            });
        }
        Ok(items)
    }
}

#[async_trait]
impl SubmissionPipeline for TriageSubmissionPipeline {
    async fn evaluate(
        &self,
        conn: &mut PgConnection,
        thread: &AgentThread,
        corpus: &SeenCorpus,
        arguments: &serde_json::Value,
    ) -> Result<SubmissionOutcome, ToolFailure> {
        // 1. Schema parse: free, and the diagnostics teach the shape.
        let submission: TriageSubmission = match serde_json::from_value(arguments.clone()) {
            Ok(submission) => submission,
            Err(source) => {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "the submission does not match its schema: {source}; check the \
                         submit_result schema and resubmit"
                    ),
                });
            }
        };

        // 2. One assessment per item: a repeat resolves to the same id and
        //    collides on the decision table's per-item uniqueness at commit,
        //    which no retry clears, so reject it here.
        let mut assessed: HashSet<KnowledgeItemId> = HashSet::new();
        for item in &submission.considered_items {
            if let Ok(id) = item.item_id.parse::<KnowledgeItemId>()
                && !assessed.insert(id)
            {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "the submission assesses {} more than once; give each examined claim a \
                         single assessment",
                        item.item_id,
                    ),
                });
            }
        }

        // 3. ID membership: every referenced id must appear in something
        //    the model was actually shown.
        let mut referenced: Vec<&str> = submission
            .considered_items
            .iter()
            .map(|item| item.item_id.as_str())
            .collect();
        if let TriageSubmissionDecision::Duplicate { matched_item_id } = &submission.decision {
            referenced.push(matched_item_id.as_str());
        }
        let unseen: Vec<&str> = referenced
            .iter()
            .copied()
            .filter(|id| !corpus.contains(id))
            .collect();
        if !unseen.is_empty() {
            return Ok(SubmissionOutcome::Bounced {
                diagnostics: format!(
                    "these ids do not appear in anything you were shown: {}; copy item ids \
                     exactly as rendered in your context or a tool result",
                    unseen.join(", "),
                ),
            });
        }

        // 3b. The must-search rule: classification must be grounded in
        //     candidate similarity. At least one search must have run (even
        //     one that found nothing, so the first item into an empty graph
        //     can still conclude novel), and every cited id needs a recorded
        //     candidate-search score. Together this closes the
        //     novel-by-laziness gap the removed opening prefetch would
        //     otherwise open.
        let provenance = reconstruct_candidate_scores(conn, thread)
            .await
            .map_err(|source| ToolFailure::System {
                context: format!("reconstructing candidate-search provenance: {source}"),
            })?;
        if !provenance.searched {
            return Ok(SubmissionOutcome::Bounced {
                diagnostics: "you have not searched yet; call search_candidate_similar_items \
                              before classifying, so the decision is grounded in candidate \
                              similarity"
                    .to_owned(),
            });
        }
        let ungrounded: Vec<&str> = referenced
            .iter()
            .copied()
            .filter(|id| !provenance.scores.contains_key(*id))
            .collect();
        if !ungrounded.is_empty() {
            return Ok(SubmissionOutcome::Bounced {
                diagnostics: format!(
                    "these ids carry no candidate-similarity score: {}; cite only items a \
                     candidate search returned (a superseded item, which the search does not \
                     surface, cannot be cited even after reading it)",
                    ungrounded.join(", "),
                ),
            });
        }

        // 4. The duplicate path's graph re-checks: the matched item still
        //    exists in this project and its supersedence state is
        //    unchanged: the optimistic re-check at the submit boundary.
        if let TriageSubmissionDecision::Duplicate { matched_item_id } = &submission.decision {
            let Ok(matched_id) = matched_item_id.parse::<KnowledgeItemId>() else {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "'{matched_item_id}' is not a knowledge item id; copy ids exactly as \
                         rendered (the '{}_' prefix followed by a UUID)",
                        KnowledgeItemId::PREFIX,
                    ),
                });
            };

            let item = match PgKnowledgeItemRepository.find_by_id(conn, matched_id).await {
                Ok(item) => Some(item),
                Err(DbError::NotFound { .. }) => None,
                Err(source) => {
                    return Err(ToolFailure::System {
                        context: format!("re-checking the matched item: {source}"),
                    });
                }
            };
            let in_project = item.filter(|item| item.project_id() == self.project_id);
            if in_project.is_none() {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "no item with id {matched_item_id} exists in this project any more; \
                         re-check with the read tools and re-decide"
                    ),
                });
            }

            let superseding = PgRelationRepository
                .find_inbound(conn, matched_id, Some(&[RelationKind::Supersedes]))
                .await
                .map_err(|source| ToolFailure::System {
                    context: format!("re-checking the matched item's supersedence: {source}"),
                })?;
            if !superseding.is_empty() {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "{matched_item_id} has been superseded since you read it; a retired \
                         claim cannot be a duplicate target; re-check and re-decide"
                    ),
                });
            }

            // 5. Contradiction implies novel: a candidate that contradicts
            //    any examined claim cannot be a duplicate. The graph needs
            //    both perspectives. The disqualifying contradiction need not
            //    be against the matched claim; a duplicate that conflicts
            //    with some other claim would silently discard that conflict.
            let contradicted = submission
                .considered_items
                .iter()
                .find(|item| item.suggested_relation == RelationSuggestion::Contradicts);
            if let Some(item) = contradicted {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "the submission classifies the candidate as a duplicate of \
                         {matched_item_id} while marking {} as a contradiction; a candidate that \
                         contradicts a claim is novel, not a duplicate, so re-decide",
                        item.item_id
                    ),
                });
            }
        }

        let payload = serde_json::to_value(&submission).map_err(|source| ToolFailure::System {
            context: format!("serialising the accepted submission: {source}"),
        })?;
        Ok(SubmissionOutcome::Accepted { payload })
    }

    async fn verifier_launch(
        &self,
        conn: &mut PgConnection,
        _thread: &AgentThread,
        payload: &serde_json::Value,
    ) -> Result<Option<VerifierLaunch>, ToolFailure> {
        let Some(verifier) = &self.verifier else {
            // No verifier configured: the validated submission commits
            // directly.
            return Ok(None);
        };

        // The payload is the submission the validators just accepted, so
        // it parses; a failure here is a system fault, not a bounce.
        let submission: TriageSubmission =
            serde_json::from_value(payload.clone()).map_err(|source| ToolFailure::System {
                context: format!("re-reading the accepted submission for verification: {source}"),
            })?;

        let (decision, matched_item_id) = match &submission.decision {
            TriageSubmissionDecision::Novel => ("novel".to_owned(), None),
            TriageSubmissionDecision::Duplicate { matched_item_id } => {
                ("duplicate".to_owned(), Some(matched_item_id.clone()))
            }
        };
        let submission_ctx = VerifierSubmissionContext {
            decision,
            matched_item_id: matched_item_id.clone(),
            handoff: submission.handoff.clone(),
        };
        let considered = self
            .verifier_considered_items(conn, &submission, matched_item_id.as_deref())
            .await?;

        let (system, user) = assemble_verifier_input(
            &verifier.system_template,
            &verifier.user_template,
            &verifier.candidate,
            &submission_ctx,
            &considered,
        )
        .map_err(|source| ToolFailure::System {
            context: format!("rendering the verifier prompt: {source}"),
        })?;

        Ok(Some(VerifierLaunch {
            binding_version_id: verifier.binding_version_id,
            // The verifier child executes under the parent's stage, so its
            // calls route through the same provider configuration.
            pipeline_stage: TaskType::Triage,
            child_input: RenderedConversation {
                system: Some(system),
                messages: vec![RecordedMessage {
                    role: "user".to_owned(),
                    content: user,
                }],
                system_prompt_version_id: Some(verifier.system_prompt_version_id),
                user_prompt_version_id: Some(verifier.user_prompt_version_id),
                resolution_context: None,
                // The verdict envelope the child's call is constrained to;
                // the driver maps it onto the response format.
                response_schema: Some(verdict_schema()),
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use tribal_db::{
        AgentThreadRepository, DrivingTaskRef, JobRepository, JobStateOverride, NewAgentThread,
        NewAgentThreadRecord, PgAgentThreadRepository, PgJobRepository, PgPrincipalRepository,
        PgProjectRepository, PgTaskRepository, PrincipalRepository, ProjectRepository,
        TaskRepository,
    };
    use tribal_domain::{
        AGENT_THREAD_FORMAT_VERSION, GitRemote, JobOutcome, JobStatus, PrincipalId, RelationBatchId,
    };
    use tribal_test_utils::{
        TestDb, a_new_job, a_new_knowledge_item, a_new_principal, a_new_project,
        a_new_prompt_version, a_new_system_fingerprint, a_new_task, an_agent_definition,
        an_agent_thread, insert_committed_relation, insert_prompt_version,
        upsert_system_fingerprint,
    };

    use super::*;

    fn corpus_with(texts: &[&str]) -> SeenCorpus {
        let mut corpus = SeenCorpus::default();
        for text in texts {
            corpus.push((*text).to_owned());
        }
        corpus
    }

    fn duplicate_of(id: &str) -> serde_json::Value {
        serde_json::json!({
            "decision": {"decision": "duplicate", "matched_item_id": id},
        })
    }

    /// Persists a triage thread whose log carries one candidate-search
    /// result scoring `scored`, so a submission evaluated against it clears
    /// the must-search gate and reaches the graph re-checks under test. The
    /// page mirrors the search tool's output, so the same reconstruction the
    /// production commit runs reads these scores back.
    async fn thread_with_scored_search(
        conn: &mut sqlx::PgConnection,
        principal_id: PrincipalId,
        project_id: ProjectId,
        scored: &[(String, f64)],
    ) -> AgentThread {
        let pv_id = insert_prompt_version(conn, &a_new_prompt_version().build()).await;
        let fingerprint =
            upsert_system_fingerprint(conn, &a_new_system_fingerprint().build()).await;
        let job = PgJobRepository
            .insert(
                conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_system_prompt_version_id(pv_id)
                    .extraction_user_prompt_version_id(pv_id)
                    .triage_system_prompt_version_id(pv_id)
                    .triage_user_prompt_version_id(pv_id)
                    .relation_system_prompt_version_id(pv_id)
                    .relation_user_prompt_version_id(pv_id)
                    .system_fingerprint_hash(fingerprint)
                    .build(),
            )
            .await
            .expect("seed job");
        let task = PgTaskRepository
            .insert(
                conn,
                &a_new_task()
                    .job_id(job.id())
                    .task_type(TaskType::Triage)
                    .batch_index(Some(0))
                    .build(),
            )
            .await
            .expect("seed triage task");
        let binding = tribal_agent_runtime::resolve_binding(
            conn,
            &an_agent_definition()
                .pipeline_stage(TaskType::Triage)
                .build(),
        )
        .await
        .expect("resolve binding");
        let thread = PgAgentThreadRepository
            .insert(
                conn,
                &NewAgentThread::builder()
                    .pipeline_stage(TaskType::Triage)
                    .binding_version_id(binding.id())
                    .driving_task(DrivingTaskRef::Stage(task.id()))
                    .principal_id(principal_id)
                    .format_version(AGENT_THREAD_FORMAT_VERSION)
                    .build(),
            )
            .await
            .expect("insert thread");

        append_candidate_search_page(conn, &thread, "ep_marker", scored).await;
        thread
    }

    /// Appends one candidate-search call/result pair: the assistant call, then
    /// a result page under `profile_id` scoring `scored`, mirroring the search
    /// tool's output so the reconstruction reads these scores back.
    async fn append_candidate_search_page(
        conn: &mut sqlx::PgConnection,
        thread: &AgentThread,
        profile_id: &str,
        scored: &[(String, f64)],
    ) {
        let call_seq = PgAgentThreadRecordRepository
            .next_seq(conn, thread.id())
            .await
            .expect("call seq");
        let call_id = format!("call_{call_seq}");
        PgAgentThreadRecordRepository
            .append(
                conn,
                &NewAgentThreadRecord::builder()
                    .thread_id(thread.id())
                    .seq(call_seq)
                    .kind(AgentThreadRecordKind::AssistantMessage)
                    .content(serde_json::json!({
                        "text": "",
                        "tool_calls": [{
                            "id": call_id,
                            "name": "search_candidate_similar_items",
                            "arguments": {},
                        }],
                    }))
                    .build(),
            )
            .await
            .expect("append search call");
        let page = serde_json::json!({
            "results": scored
                .iter()
                .map(|(id, score)| serde_json::json!({
                    "item_id": id,
                    "similarity_score": score,
                }))
                .collect::<Vec<_>>(),
            "embedding_profile_id": profile_id,
        });
        let result_seq = PgAgentThreadRecordRepository
            .next_seq(conn, thread.id())
            .await
            .expect("result seq");
        PgAgentThreadRecordRepository
            .append(
                conn,
                &NewAgentThreadRecord::builder()
                    .thread_id(thread.id())
                    .seq(result_seq)
                    .kind(AgentThreadRecordKind::ToolResult)
                    .requesting_seq(Some(call_seq))
                    .tool_call_id(Some(call_id))
                    .content(serde_json::json!({
                        "output": serde_json::to_string(&page).expect("serialise page"),
                        "is_error": false,
                    }))
                    .build(),
            )
            .await
            .expect("append search result");
    }

    #[tokio::test]
    async fn test_reconstruct_pins_scores_to_the_latest_embedding_profile() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let principal = PgPrincipalRepository
            .insert(
                &mut txn,
                &a_new_principal()
                    .principal_key("user:profile-cutover".to_owned())
                    .build(),
            )
            .await
            .expect("principal");
        let project = PgProjectRepository
            .insert(
                &mut txn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        "test/profile-cutover",
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("project");

        let superseded = "ki_00000000-0000-0000-0000-0000000000c1".to_owned();
        let carried = "ki_00000000-0000-0000-0000-0000000000d2".to_owned();
        // The pre-cutover search scored both items under the old profile.
        let thread = thread_with_scored_search(
            &mut txn,
            principal.id(),
            project.id(),
            &[(superseded.clone(), 0.95), (carried.clone(), 0.80)],
        )
        .await;
        // A profile cutover restarts the search under a new profile that
        // re-scores only `carried` and never surfaces `superseded`.
        append_candidate_search_page(&mut txn, &thread, "ep_next", &[(carried.clone(), 0.60)])
            .await;

        let provenance = reconstruct_candidate_scores(&mut txn, &thread)
            .await
            .expect("reconstruct");
        assert!(provenance.searched);
        let carried_score = provenance
            .scores
            .get(&carried)
            .copied()
            .expect("the carried item is scored");
        assert!(
            (carried_score - 0.60).abs() < 1e-9,
            "the carried item keeps its current-profile score (0.60), not the superseded 0.80",
        );
        assert!(
            !provenance.scores.contains_key(&superseded),
            "a score only under the superseded profile is dropped, not merged",
        );
    }

    #[tokio::test]
    async fn test_a_malformed_submission_bounces_with_the_schema_diagnostics() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let pipeline = TriageSubmissionPipeline::new(ProjectId::new());
        let thread = an_agent_thread().build();

        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&[]),
                &serde_json::json!({"verdict": "novel"}),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("schema")
        ));
    }

    #[tokio::test]
    async fn test_an_unseen_id_bounces_by_membership() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let pipeline = TriageSubmissionPipeline::new(ProjectId::new());
        let thread = an_agent_thread().build();

        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&["the context shows ki_00000000-0000-0000-0000-000000000001"]),
                &duplicate_of("ki_00000000-0000-0000-0000-00000000beef"),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains("ki_00000000-0000-0000-0000-00000000beef")
                    && diagnostics.contains("copy item ids exactly")
        ));
    }

    #[tokio::test]
    async fn test_a_repeated_assessment_bounces_before_commit() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let pipeline = TriageSubmissionPipeline::new(ProjectId::new());
        let thread = an_agent_thread().build();
        let id = "ki_00000000-0000-0000-0000-0000000000a1";

        // Two assessments of one claim would collide on the decision table's
        // per-item uniqueness at commit; the validator bounces it first.
        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&[]),
                &serde_json::json!({
                    "decision": {"decision": "created"},
                    "considered_items": [
                        {"item_id": id, "suggested_relation": "supports", "justification": "a"},
                        {"item_id": id, "suggested_relation": "unrelated", "justification": "b"},
                    ],
                }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains(id) && diagnostics.contains("more than once")
        ));
    }

    /// The recheck fixture: a live in-project item, a foreign project's
    /// item, and a superseded item behind a committed supersedes edge,
    /// all of them in the seen corpus.
    struct RecheckFixture {
        pipeline: TriageSubmissionPipeline,
        corpus: SeenCorpus,
        /// A thread carrying candidate-search scores for every fixture id,
        /// so each recheck clears the must-search gate ahead of it.
        thread: AgentThread,
        live: KnowledgeItemId,
        foreign: KnowledgeItemId,
        superseded: KnowledgeItemId,
    }

    async fn seed_recheck_fixture(txn: &mut sqlx::PgConnection) -> RecheckFixture {
        let principal = PgPrincipalRepository
            .insert(
                txn,
                &a_new_principal()
                    .principal_key("user:submission-recheck".to_owned())
                    .build(),
            )
            .await
            .expect("principal");
        let project = PgProjectRepository
            .insert(
                txn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts("github.com", "test/submission", None))
                    .build(),
            )
            .await
            .expect("project");
        let foreign_project = PgProjectRepository
            .insert(
                txn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        "test/submission-foreign",
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("foreign project");

        let an_item = |project_id| {
            a_new_knowledge_item()
                .project_id(project_id)
                .principal_id(principal.id())
                .build()
        };
        let live = PgKnowledgeItemRepository
            .insert(txn, &an_item(project.id()))
            .await
            .expect("live item");
        let foreign = PgKnowledgeItemRepository
            .insert(txn, &an_item(foreign_project.id()))
            .await
            .expect("foreign item");
        let superseded = PgKnowledgeItemRepository
            .insert(txn, &an_item(project.id()))
            .await
            .expect("superseded item");
        let successor = PgKnowledgeItemRepository
            .insert(txn, &an_item(project.id()))
            .await
            .expect("successor item");

        // The supersedes edge counts only when committed through a job.
        let batch_id = RelationBatchId::new();
        let pv_id = insert_prompt_version(txn, &a_new_prompt_version().build()).await;
        let fingerprint = upsert_system_fingerprint(txn, &a_new_system_fingerprint().build()).await;
        PgJobRepository
            .insert_for_test(
                txn,
                &a_new_job()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .extraction_system_prompt_version_id(pv_id)
                    .extraction_user_prompt_version_id(pv_id)
                    .triage_system_prompt_version_id(pv_id)
                    .triage_user_prompt_version_id(pv_id)
                    .relation_system_prompt_version_id(pv_id)
                    .relation_user_prompt_version_id(pv_id)
                    .system_fingerprint_hash(fingerprint)
                    .build(),
                &JobStateOverride::builder()
                    .status(JobStatus::Completed)
                    .outcome(Some(JobOutcome::Success))
                    .committed_batch_id(Some(batch_id))
                    .build(),
            )
            .await
            .expect("committed job");
        insert_committed_relation(
            txn,
            batch_id,
            successor.id(),
            superseded.id(),
            RelationKind::Supersedes,
            principal.id(),
        )
        .await;

        let corpus = corpus_with(&[&format!(
            "shown: {} {} {} and ki_00000000-0000-0000-0000-00000000dead",
            live.id(),
            foreign.id(),
            superseded.id(),
        )]);
        // The search that grounds every recheck: the vanished id was scored
        // too, so it clears the gate and bounces on the live-item recheck,
        // not on the must-search rule.
        let scored = vec![
            (live.id().to_string(), 0.95),
            (foreign.id().to_string(), 0.92),
            (superseded.id().to_string(), 0.90),
            ("ki_00000000-0000-0000-0000-00000000dead".to_owned(), 0.88),
        ];
        let thread = thread_with_scored_search(txn, principal.id(), project.id(), &scored).await;
        RecheckFixture {
            pipeline: TriageSubmissionPipeline::new(project.id()),
            corpus,
            thread,
            live: live.id(),
            foreign: foreign.id(),
            superseded: superseded.id(),
        }
    }

    #[tokio::test]
    async fn test_the_duplicate_recheck_requires_a_live_in_project_unsuperseded_match() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");
        let RecheckFixture {
            pipeline,
            corpus,
            thread,
            live,
            foreign,
            superseded,
        } = seed_recheck_fixture(&mut txn).await;

        // A live, in-project, unsuperseded match is accepted.
        let outcome = pipeline
            .evaluate(&mut txn, &thread, &corpus, &duplicate_of(&live.to_string()))
            .await
            .expect("evaluates");
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));

        // A vanished id and a foreign project's id bounce identically.
        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus,
                &duplicate_of("ki_00000000-0000-0000-0000-00000000dead"),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains("exists in this project")
        ));
        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus,
                &duplicate_of(&foreign.to_string()),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains("exists in this project")
        ));

        // A superseded match bounces: the retired claim is no target.
        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus,
                &duplicate_of(&superseded.to_string()),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains("superseded")
        ));
    }

    /// A duplicate decision bounces whenever any examined claim is assessed
    /// as a contradiction, whether or not it is the matched item: a
    /// candidate that contradicts a claim is novel, never a duplicate.
    #[tokio::test]
    async fn test_contradicting_any_examined_claim_bounces_a_duplicate() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");

        let principal = PgPrincipalRepository
            .insert(
                &mut txn,
                &a_new_principal()
                    .principal_key("user:submission-contradiction".to_owned())
                    .build(),
            )
            .await
            .expect("principal");
        let project = PgProjectRepository
            .insert(
                &mut txn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        "test/submission-contradiction",
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("project");
        let matched = PgKnowledgeItemRepository
            .insert(
                &mut txn,
                &a_new_knowledge_item()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .build(),
            )
            .await
            .expect("matched item");
        let other = PgKnowledgeItemRepository
            .insert(
                &mut txn,
                &a_new_knowledge_item()
                    .project_id(project.id())
                    .principal_id(principal.id())
                    .build(),
            )
            .await
            .expect("other item");

        let pipeline = TriageSubmissionPipeline::new(project.id());
        let corpus = corpus_with(&[&format!("shown: {} {}", matched.id(), other.id())]);
        // Both examined claims carry a candidate-search score, so the bounce
        // under test is the contradiction rule, not the must-search gate.
        let thread = thread_with_scored_search(
            &mut txn,
            principal.id(),
            project.id(),
            &[
                (matched.id().to_string(), 0.9),
                (other.id().to_string(), 0.9),
            ],
        )
        .await;

        let submission = |contradicted: &str| {
            serde_json::json!({
                "decision": {"decision": "duplicate", "matched_item_id": matched.id().to_string()},
                "considered_items": [{
                    "item_id": contradicted,
                    "suggested_relation": "contradicts",
                    "justification": "conflicting claims",
                }],
            })
        };

        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus,
                &submission(&matched.id().to_string()),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains("contradicts") || diagnostics.contains("contradiction")
        ));

        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus,
                &submission(&other.id().to_string()),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains("contradicts") || diagnostics.contains("contradiction")
        ));
    }

    /// The verifier launch renders a fresh-context child: the rubric
    /// system prompt, the candidate and submission, every assessed claim's
    /// content, and the duplicated claim surfaced even without a per-claim
    /// assessment: all constrained to the verdict schema.
    #[tokio::test]
    async fn test_the_verifier_launch_renders_the_child_with_the_rubric_and_verdict_schema() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");

        let principal = PgPrincipalRepository
            .insert(
                &mut txn,
                &a_new_principal()
                    .principal_key("user:verifier-launch".to_owned())
                    .build(),
            )
            .await
            .expect("principal");
        let project = PgProjectRepository
            .insert(
                &mut txn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts("github.com", "test/verifier", None))
                    .build(),
            )
            .await
            .expect("project");
        let an_item = |content: &str| {
            a_new_knowledge_item()
                .project_id(project.id())
                .principal_id(principal.id())
                .content(content.to_owned())
                .build()
        };
        let matched = PgKnowledgeItemRepository
            .insert(&mut txn, &an_item("auth tokens expire after an hour"))
            .await
            .expect("matched item");
        let other = PgKnowledgeItemRepository
            .insert(&mut txn, &an_item("caching keys are namespaced by tenant"))
            .await
            .expect("other item");

        let candidate: Candidate = serde_json::from_value(serde_json::json!({
            "kind": "fact",
            "content": "tokens expire after sixty minutes",
            "suggested_tags": ["auth"],
        }))
        .expect("candidate");

        let binding_id = AgentBindingVersionId::new();
        let system_pv_id = PromptVersionId::new();
        let user_pv_id = PromptVersionId::new();
        let verifier = VerifierContext {
            binding_version_id: binding_id,
            system_template: include_str!("../../../../prompts/triage/verifier_system.tera")
                .to_owned(),
            system_prompt_version_id: system_pv_id,
            user_template: include_str!("../../../../prompts/triage/verifier_user.tera").to_owned(),
            user_prompt_version_id: user_pv_id,
            candidate,
        };
        let pipeline = TriageSubmissionPipeline::new(project.id()).with_verifier(Some(verifier));

        // A duplicate of `matched`, with a per-claim assessment only for
        // `other`: the matched claim's content must still surface.
        let payload = serde_json::json!({
            "decision": {"decision": "duplicate", "matched_item_id": matched.id().to_string()},
            "considered_items": [{
                "item_id": other.id().to_string(),
                "suggested_relation": "supports",
                "justification": "shares the caching topic",
            }],
            "handoff": "links auth expiry and caching",
        });
        let thread = an_agent_thread().build();
        let launch = pipeline
            .verifier_launch(&mut txn, &thread, &payload)
            .await
            .expect("launch evaluates")
            .expect("a verifier is configured");

        assert_eq!(launch.pipeline_stage, TaskType::Triage);
        assert_eq!(launch.binding_version_id, binding_id);
        assert_eq!(
            launch.child_input.response_schema,
            Some(verdict_schema()),
            "the child's call is constrained to the verdict schema",
        );
        assert_eq!(
            launch.child_input.system_prompt_version_id,
            Some(system_pv_id),
        );
        assert_eq!(launch.child_input.user_prompt_version_id, Some(user_pv_id));

        let system = launch
            .child_input
            .system
            .as_deref()
            .expect("a system prompt");
        assert!(
            system.contains("verification agent"),
            "the rubric system prompt rides the child: {system}",
        );
        let user = &launch.child_input.messages[0].content;
        assert!(
            user.contains("tokens expire after sixty minutes"),
            "the candidate reaches the verifier: {user}",
        );
        assert!(
            user.contains("caching keys are namespaced by tenant"),
            "the assessed claim's content reaches the verifier: {user}",
        );
        assert!(
            user.contains("auth tokens expire after an hour"),
            "the duplicated claim is surfaced even without a per-claim assessment: {user}",
        );
        assert!(
            user.contains("links auth expiry and caching"),
            "the handoff reaches the verifier: {user}",
        );

        // With no verifier configured, the launch declines and the
        // submission would commit on the validators alone.
        let plain = TriageSubmissionPipeline::new(project.id());
        assert!(
            plain
                .verifier_launch(&mut txn, &thread, &payload)
                .await
                .expect("launch evaluates")
                .is_none(),
            "no verifier configured means no launch",
        );
    }

    /// The must-search gate: a submission bounces when nothing has been
    /// searched, and when a cited id carries no candidate-search score, but
    /// a search that grounds every cited id clears it. This is what keeps a
    /// classification anchored to candidate similarity once the opening
    /// prefetch is gone.
    #[tokio::test]
    async fn test_the_must_search_gate_bounces_unsearched_and_ungrounded_submissions() {
        let ctx = TestDb::new().await;
        let mut txn = ctx.begin().await.expect("begin");

        let principal = PgPrincipalRepository
            .insert(
                &mut txn,
                &a_new_principal()
                    .principal_key("user:must-search".to_owned())
                    .build(),
            )
            .await
            .expect("principal");
        let project = PgProjectRepository
            .insert(
                &mut txn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        "test/must-search",
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("project");
        let pipeline = TriageSubmissionPipeline::new(project.id());

        let scored_id = "ki_00000000-0000-0000-0000-0000000000a1";
        let unscored_id = "ki_00000000-0000-0000-0000-0000000000b2";
        let corpus = corpus_with(&[&format!("shown: {scored_id} {unscored_id}")]);
        let novel_citing = |id: &str| {
            serde_json::json!({
                "decision": {"decision": "created"},
                "considered_items": [{
                    "item_id": id,
                    "suggested_relation": "supports",
                    "justification": "related context",
                }],
            })
        };

        // Nothing searched: the empty log bounces on the must-search rule
        // even though the cited id was shown.
        let unsearched = an_agent_thread().build();
        let outcome = pipeline
            .evaluate(&mut txn, &unsearched, &corpus, &novel_citing(scored_id))
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("searched")
        ));

        // A search that scored only one id: citing the other one bounces as
        // ungrounded, citing the scored one clears the gate.
        let thread = thread_with_scored_search(
            &mut txn,
            principal.id(),
            project.id(),
            &[(scored_id.to_owned(), 0.9)],
        )
        .await;
        let outcome = pipeline
            .evaluate(&mut txn, &thread, &corpus, &novel_citing(unscored_id))
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains("candidate-similarity score")
        ));

        let outcome = pipeline
            .evaluate(&mut txn, &thread, &corpus, &novel_citing(scored_id))
            .await
            .expect("evaluates");
        assert!(
            matches!(outcome, SubmissionOutcome::Accepted { .. }),
            "a grounded novel submission clears the must-search gate",
        );

        // A search that found nothing still counts as having searched: the
        // first item into an empty graph submits novel with no citations.
        let empty_graph =
            thread_with_scored_search(&mut txn, principal.id(), project.id(), &[]).await;
        let outcome = pipeline
            .evaluate(
                &mut txn,
                &empty_graph,
                &corpus,
                &serde_json::json!({"decision": {"decision": "created"}, "considered_items": []}),
            )
            .await
            .expect("evaluates");
        assert!(
            matches!(outcome, SubmissionOutcome::Accepted { .. }),
            "an empty but executed search lets the first item conclude novel",
        );
    }
}
