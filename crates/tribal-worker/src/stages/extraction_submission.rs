//! The extraction submission pipeline: the deterministic validators behind
//! `submit_result` for the extraction loop.
//!
//! The checks here are the predicate-decidable ones: the shape, the
//! candidate cap, and that every relation hint references a candidate that
//! exists. A submission that fails one bounces with diagnostics the model
//! corrects against; only source-grounding (a candidate the raw input does
//! not support) needs a model call, and that is the deferred verifier's
//! job, not this pipeline's.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_agent_runtime::{SeenCorpus, SubmissionOutcome, SubmissionPipeline, VerifierLaunch};
use tribal_domain::{AgentThread, ToolFailure};

use crate::parsing::ExtractionSubmission;

/// The extraction stage's submission pipeline. It reads nothing from the
/// graph: extraction produces claims from the raw input, so the only state
/// it needs is the candidate cap the binding ran under.
pub(crate) struct ExtractionSubmissionPipeline {
    max_candidates: usize,
}

impl ExtractionSubmissionPipeline {
    /// Creates the pipeline with the candidate cap the stage enforces.
    pub(crate) fn new(max_candidates: usize) -> Self {
        Self { max_candidates }
    }
}

#[async_trait]
impl SubmissionPipeline for ExtractionSubmissionPipeline {
    async fn evaluate(
        &self,
        _conn: &mut PgConnection,
        _thread: &AgentThread,
        _corpus: &SeenCorpus,
        arguments: &serde_json::Value,
    ) -> Result<SubmissionOutcome, ToolFailure> {
        // 1. Schema parse: free, and the diagnostics teach the shape.
        let submission: ExtractionSubmission = match serde_json::from_value(arguments.clone()) {
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

        // 2. The candidate cap: a job extracts at most a bounded batch.
        if submission.candidates.len() > self.max_candidates {
            return Ok(SubmissionOutcome::Bounced {
                diagnostics: format!(
                    "you submitted {} candidates against a cap of {}; submit only the most \
                     significant, at most {} of them",
                    submission.candidates.len(),
                    self.max_candidates,
                    self.max_candidates,
                ),
            });
        }

        // 3. Relation-hint indices: every hint must reference a candidate
        //    that exists in this submission.
        let n = tribal_common::clamp_to_u32(submission.candidates.len());
        let out_of_range: Vec<u32> = submission
            .relation_hints
            .iter()
            .flat_map(|hint| [hint.source_index(), hint.target_index()])
            .filter(|index| *index >= n)
            .collect();
        if !out_of_range.is_empty() {
            return Ok(SubmissionOutcome::Bounced {
                diagnostics: format!(
                    "these relation-hint indices reference no candidate (there are {n}): {}; \
                     hints index candidates by their zero-based position in this submission",
                    out_of_range
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                ),
            });
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
        // The source-grounding verifier binding is deferred; the predicate
        // validators above are the whole pipeline in this release, and an
        // accepted submission commits on them alone.
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use tribal_test_utils::{an_agent_thread, serial_lock, test_context};

    use super::*;

    fn corpus() -> SeenCorpus {
        SeenCorpus::default()
    }

    fn candidate(content: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "fact",
            "content": content,
            "suggested_tags": [],
        })
    }

    #[tokio::test]
    async fn test_a_within_cap_submission_is_accepted() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = ExtractionSubmissionPipeline::new(3)
            .evaluate(
                &mut txn,
                &thread,
                &corpus(),
                &serde_json::json!({
                    "candidates": [candidate("a"), candidate("b")],
                    "relation_hints": [{
                        "source_index": 1,
                        "target_index": 0,
                        "hint_type": "derived_from",
                    }],
                }),
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

        let outcome = ExtractionSubmissionPipeline::new(3)
            .evaluate(
                &mut txn,
                &thread,
                &corpus(),
                &serde_json::json!({ "candidates": [] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(outcome, SubmissionOutcome::Accepted { .. }));
    }

    #[tokio::test]
    async fn test_over_cap_bounces() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = ExtractionSubmissionPipeline::new(1)
            .evaluate(
                &mut txn,
                &thread,
                &corpus(),
                &serde_json::json!({ "candidates": [candidate("a"), candidate("b")] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("cap of 1")
        ));
    }

    #[tokio::test]
    async fn test_an_out_of_range_hint_bounces() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = ExtractionSubmissionPipeline::new(3)
            .evaluate(
                &mut txn,
                &thread,
                &corpus(),
                &serde_json::json!({
                    "candidates": [candidate("a")],
                    "relation_hints": [{
                        "source_index": 0,
                        "target_index": 5,
                        "hint_type": "derived_from",
                    }],
                }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("reference no candidate")
        ));
    }

    #[tokio::test]
    async fn test_a_malformed_submission_bounces_with_the_schema_diagnostics() {
        let _guard = serial_lock().await;
        let ctx = test_context().await;
        let mut txn = ctx.begin_test().await.expect("begin_test");
        let thread = an_agent_thread().build();

        let outcome = ExtractionSubmissionPipeline::new(3)
            .evaluate(
                &mut txn,
                &thread,
                &corpus(),
                &serde_json::json!({ "items": [] }),
            )
            .await
            .expect("evaluates");
        assert!(matches!(
            outcome,
            SubmissionOutcome::Bounced { ref diagnostics } if diagnostics.contains("schema")
        ));
    }
}
