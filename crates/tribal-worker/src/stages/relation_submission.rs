//! The relation submission pipeline: the deterministic validators behind
//! `submit_result` for the relation loop.
//!
//! Schema parse first, then the checks, in order, never a model call.
//! Every rejection bounces with diagnostics held to the prompt bar so the
//! loop's model corrects its own output. Endpoint existence is rechecked at
//! the commit boundary, not here, so a claim that vanished between the
//! model's read and its submission drops the edge rather than failing the
//! stage; this pipeline validates only what the model controls: the shape,
//! the bounds, and that every cited id was actually shown to it.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_agent_runtime::{SeenCorpus, SubmissionOutcome, SubmissionPipeline, VerifierLaunch};
use tribal_domain::{AgentThread, KnowledgeItemId, ToolFailure};

use crate::parsing::{RELATION_JUSTIFICATION_MAX_CHARS, RelationSubmission};

/// The relation stage's submission pipeline. Cross-project by nature, so it
/// carries no project fence; the membership check against the seen corpus
/// is what keeps a citation grounded in what the model actually read.
pub(crate) struct RelationSubmissionPipeline;

#[async_trait]
impl SubmissionPipeline for RelationSubmissionPipeline {
    async fn evaluate(
        &self,
        _conn: &mut PgConnection,
        _thread: &AgentThread,
        corpus: &SeenCorpus,
        arguments: &serde_json::Value,
    ) -> Result<SubmissionOutcome, ToolFailure> {
        // 1. Schema parse — free, and the diagnostics teach the shape.
        let submission: RelationSubmission = match serde_json::from_value(arguments.clone()) {
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

        for edge in &submission.relations {
            // 2. The justification bound: curated reasoning, never a
            //    transcript.
            if let Some(justification) = &edge.justification
                && justification.chars().count() > RELATION_JUSTIFICATION_MAX_CHARS
            {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "an edge justification is too long ({} characters against a bound of \
                         {RELATION_JUSTIFICATION_MAX_CHARS}); keep it to the reasoning for the \
                         edge",
                        justification.chars().count(),
                    ),
                });
            }

            // 3. ID membership: both endpoints must appear in something the
            //    model was actually shown.
            let unseen: Vec<&str> = [edge.source_id.as_str(), edge.target_id.as_str()]
                .into_iter()
                .filter(|id| !corpus.contains(id))
                .collect();
            if !unseen.is_empty() {
                return Ok(SubmissionOutcome::Bounced {
                    diagnostics: format!(
                        "these ids do not appear in anything you were shown: {}; cite only the \
                         batch items and the ids a search returned, copied exactly",
                        unseen.join(", "),
                    ),
                });
            }

            // 4. ID shape: a seen-but-unparseable id is a wiring fault the
            //    model can fix by copying the id exactly.
            for raw in [&edge.source_id, &edge.target_id] {
                if raw.parse::<KnowledgeItemId>().is_err() {
                    return Ok(SubmissionOutcome::Bounced {
                        diagnostics: format!(
                            "'{raw}' is not a knowledge item id; copy ids exactly as rendered \
                             (the '{}_' prefix followed by a UUID)",
                            KnowledgeItemId::PREFIX,
                        ),
                    });
                }
            }
        }

        let payload = serde_json::to_value(&submission).map_err(|source| ToolFailure::System {
            context: format!("serialising the accepted submission: {source}"),
        })?;
        Ok(SubmissionOutcome::Accepted { payload })
    }

    async fn verifier_launch(
        &self,
        _conn: &mut PgConnection,
        _thread: &AgentThread,
        _payload: &serde_json::Value,
    ) -> Result<Option<VerifierLaunch>, ToolFailure> {
        // The relation loop ships without a verifier in this release; an
        // accepted submission commits on the validators alone.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::{an_agent_thread, serial_lock, test_context};

    use super::*;

    fn corpus_with(texts: &[&str]) -> SeenCorpus {
        let mut corpus = SeenCorpus::default();
        for text in texts {
            corpus.push((*text).to_owned());
        }
        corpus
    }

    const ID_A: &str = "ki_00000000-0000-0000-0000-0000000000a1";
    const ID_B: &str = "ki_00000000-0000-0000-0000-0000000000b2";

    fn edge(source: &str, target: &str) -> serde_json::Value {
        serde_json::json!({
            "source_id": source,
            "target_id": target,
            "relation_type": "supports",
        })
    }

    #[tokio::test]
    async fn test_an_edge_grounded_in_seen_ids_is_accepted() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = RelationSubmissionPipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&[&format!("the batch shows {ID_A} and {ID_B}")]),
                &serde_json::json!({ "relations": [edge(ID_A, ID_B)] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn test_an_empty_submission_is_accepted() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = RelationSubmissionPipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&[]),
                &serde_json::json!({ "relations": [] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn test_an_unseen_endpoint_bounces_by_membership() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = RelationSubmissionPipeline
            .evaluate(
                &mut txn,
                &thread,
                // Only the source id was shown; the target was never seen.
                &corpus_with(&[&format!("the batch shows {ID_A}")]),
                &serde_json::json!({ "relations": [edge(ID_A, ID_B)] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics }
                if diagnostics.contains(ID_B) && diagnostics.contains("anything you were shown")
        ));
    }

    #[tokio::test]
    async fn test_a_malformed_submission_bounces_with_the_schema_diagnostics() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = RelationSubmissionPipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&[]),
                &serde_json::json!({ "edges": [] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("schema")
        ));
    }

    #[tokio::test]
    async fn test_an_overlong_justification_bounces() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let mut edge = edge(ID_A, ID_B);
        edge["justification"] = serde_json::json!("x".repeat(RELATION_JUSTIFICATION_MAX_CHARS + 1));
        let outcome = RelationSubmissionPipeline
            .evaluate(
                &mut txn,
                &thread,
                &corpus_with(&[&format!("the batch shows {ID_A} and {ID_B}")]),
                &serde_json::json!({ "relations": [edge] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("too long")
        ));
    }
}
