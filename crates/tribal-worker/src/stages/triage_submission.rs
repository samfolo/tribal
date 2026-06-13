//! The triage submission pipeline: the deterministic validators behind
//! `submit_result`.
//!
//! Schema parse first, then the checks, in order, never a model call.
//! Every model-addressable rejection bounces with diagnostics held to
//! the prompt bar — the loop's model corrects its own output — and the
//! graph re-checks run at evaluation time so the optimistic reads the
//! agent's tools established are re-validated at the commit boundary.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_agent_runtime::{SeenCorpus, SubmissionOutcome, SubmissionPipeline};
use tribal_db::{
    DbError, KnowledgeItemRepository, PgKnowledgeItemRepository, PgRelationRepository,
    RelationRepository,
};
use tribal_domain::{
    AgentThread, KnowledgeItemId, ProjectId, RelationKind, RelationSuggestion, ToolFailure,
};

use crate::parsing::{HANDOFF_MAX_CHARS, TriageSubmission, TriageSubmissionDecision};

/// The triage stage's submission pipeline, fenced to one project.
pub(crate) struct TriageSubmissionPipeline {
    project_id: ProjectId,
}

impl TriageSubmissionPipeline {
    /// Creates the pipeline for one stage execution.
    pub(crate) fn new(project_id: ProjectId) -> Self {
        Self { project_id }
    }
}

#[async_trait]
impl SubmissionPipeline for TriageSubmissionPipeline {
    async fn evaluate(
        &self,
        conn: &mut PgConnection,
        _thread: &AgentThread,
        corpus: &SeenCorpus,
        arguments: &serde_json::Value,
    ) -> Result<SubmissionOutcome, ToolFailure> {
        // 1. Schema parse — free, and the diagnostics teach the shape.
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

        // 2. The handoff bound: curated notes, never a transcript.
        if let Some(handoff) = &submission.handoff
            && handoff.chars().count() > HANDOFF_MAX_CHARS
        {
            return Ok(SubmissionOutcome::Bounced {
                diagnostics: format!(
                    "the handoff is too long ({} characters against a bound of \
                     {HANDOFF_MAX_CHARS}); keep it to the few notes the relation stage needs",
                    handoff.chars().count(),
                ),
            });
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

        // 4. The duplicate path's graph re-checks: the matched item still
        //    exists in this project and its supersedence state is
        //    unchanged — the optimistic re-check at the submit boundary.
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
                         claim cannot be a duplicate target — re-check and re-decide"
                    ),
                });
            }

            // 5. Contradiction implies novel: a candidate that contradicts
            //    the claim it supposedly duplicates cannot be a duplicate —
            //    the graph needs both perspectives.
            let contradicts_match = submission.considered_items.iter().any(|item| {
                item.item_id == *matched_item_id
                    && item.suggested_relation == RelationSuggestion::Contradicts
            });
            if contradicts_match {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "the submission marks {matched_item_id} as both the duplicated claim \
                         and a contradiction; a candidate that contradicts a claim is novel — \
                         re-decide"
                    ),
                });
            }
        }

        let payload = serde_json::to_value(&submission).map_err(|source| ToolFailure::System {
            context: format!("serialising the accepted submission: {source}"),
        })?;
        Ok(SubmissionOutcome::Accepted { payload })
    }
}

#[cfg(test)]
mod tests {
    use tribal_db::{
        JobStateOverride, PgJobRepository, PgPrincipalRepository, PgProjectRepository,
        PrincipalRepository, ProjectRepository,
    };
    use tribal_domain::{GitRemote, JobOutcome, JobStatus, RelationBatchId};
    use tribal_test_utils::{
        a_new_job, a_new_knowledge_item, a_new_principal, a_new_project, a_new_prompt_version,
        a_new_system_fingerprint, an_agent_thread, insert_committed_relation,
        insert_prompt_version, serial_lock, test_context, upsert_system_fingerprint,
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

    #[tokio::test]
    async fn test_a_malformed_submission_bounces_with_the_schema_diagnostics() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
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
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
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

    /// The recheck fixture: a live in-project item, a foreign project's
    /// item, and a superseded item behind a committed supersedes edge,
    /// all of them in the seen corpus.
    struct RecheckFixture {
        pipeline: TriageSubmissionPipeline,
        corpus: SeenCorpus,
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
        RecheckFixture {
            pipeline: TriageSubmissionPipeline::new(project.id()),
            corpus,
            live: live.id(),
            foreign: foreign.id(),
            superseded: superseded.id(),
        }
    }

    #[tokio::test]
    async fn test_the_duplicate_recheck_requires_a_live_in_project_unsuperseded_match() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();
        let RecheckFixture {
            pipeline,
            corpus,
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

    /// A duplicate decision whose matched claim is itself assessed as a
    /// contradiction bounces: the rule is scoped to the matched item, so
    /// contradicting some *other* examined claim stays legal.
    #[tokio::test]
    async fn test_contradicting_the_matched_item_bounces_but_others_do_not() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

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
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn test_an_overlong_handoff_bounces() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let pipeline = TriageSubmissionPipeline::new(ProjectId::new());
        let thread = an_agent_thread().build();

        let outcome = pipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&[]),
                &serde_json::json!({
                    "decision": {"decision": "created"},
                    "handoff": "x".repeat(HANDOFF_MAX_CHARS + 1),
                }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("too long")
        ));
    }
}
