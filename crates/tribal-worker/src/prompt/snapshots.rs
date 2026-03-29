//! Golden snapshot tests for rendered prompt templates.
//!
//! Renders every embedded default template with rich, realistic
//! synthetic data and compares the output against committed snapshot
//! files. The snapshots serve as both drift prevention (CI fails on
//! unintentional changes) and human-readable previews of what models
//! receive at each pipeline stage.
//!
//! Regenerate with:
//! ```text
//! UPDATE_SNAPSHOTS=1 cargo test -p tribal-worker test_prompt_golden_snapshot
//! ```

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tribal_domain::{
        Candidate, KnowledgeKind, RelationHint, RelationSuggestion,
    };
    use tribal_test_utils::assert_text_snapshot;

    use crate::prompt::{
        CandidateOutcome, RelationPromptContext, SimilarItemContext, SimilarItemDecisionContext,
        SimilarityBand, extraction_user_context, relation_user_context,
        renderer::PromptRenderer,
        triage_user_context,
        variables::{extraction_system_context, relation_system_context, triage_system_context},
    };

    // ---------------------------------------------------------------------
    // Stable identifiers for deterministic snapshots
    // ---------------------------------------------------------------------

    const KI_AUTH_CACHE: &str = "ki_a1b2c3d4-e5f6-7890-abcd-ef0123456789";
    const KI_RATE_LIMIT: &str = "ki_b2c3d4e5-f6a7-8901-bcde-f01234567890";
    const KI_DEPLOY_APPROVAL: &str = "ki_c3d4e5f6-a7b8-9012-cdef-012345678901";
    const KI_BILLING_BATCH: &str = "ki_d4e5f6a7-b8c9-0123-defa-123456789012";
    const KI_BILLING_RAISED: &str = "ki_e5f6a7b8-c9d0-1234-efab-234567890123";
    const KI_AUTH_DB_LOOKUP: &str = "ki_f6a7b8c9-d0e1-2345-fabc-345678901234";
    const KI_BILLING_HEURISTIC: &str = "ki_a7b8c9d0-e1f2-3456-abcd-456789012345";

    // ---------------------------------------------------------------------
    // Shared fixtures
    // ---------------------------------------------------------------------

    fn sample_tags() -> Vec<&'static str> {
        vec![
            "billing",
            "authentication",
            "postgres",
            "incident response",
            "ci pipeline",
            "api rate limiting",
        ]
    }

    // ---------------------------------------------------------------------
    // Extraction
    // ---------------------------------------------------------------------

    fn extraction_raw_input() -> &'static str {
        concat!(
            "We had an interesting finding during the billing incident last week. ",
            "The rate limiter threshold was changed from 100 to 500 during the ",
            "Black Friday response and nobody reverted it. So production has been ",
            "running at 500 req/client for three months now.\n\n",
            "Also, Sarah from the security team mentioned that the auth service ",
            "token validation was moved to direct DB lookups after the Q3 audit. ",
            "The Redis cache is still deployed but not actually used for auth anymore.\n\n",
            "Oh and one more thing — when investigating billing anomalies in the ",
            "future, always check the rate limiter config first. This is the second ",
            "time it's been changed during an incident and not reverted.",
        )
    }

    #[test]
    fn test_prompt_golden_snapshot_extraction() {
        let renderer = PromptRenderer::for_validation();
        let tags = sample_tags();

        let system_ctx = extraction_system_context();
        let rendered = renderer
            .render(
                include_str!("../../../../prompts/extraction/system.tera"),
                system_ctx,
                "extraction system",
            )
            .unwrap();
        assert_text_snapshot!(&rendered, "src/prompt/snapshots/extraction/system.md");

        let user_ctx = extraction_user_context(extraction_raw_input(), &tags);
        let rendered = renderer
            .render(
                include_str!("../../../../prompts/extraction/user.tera"),
                user_ctx,
                "extraction user",
            )
            .unwrap();
        assert_text_snapshot!(&rendered, "src/prompt/snapshots/extraction/user.md");
    }

    // ---------------------------------------------------------------------
    // Triage
    // ---------------------------------------------------------------------

    fn triage_candidate() -> Candidate {
        serde_json::from_value(json!({
            "kind": "fact",
            "content": "The billing service rate limiter threshold was raised from 100 to 500 requests per client during the Black Friday 2024 incident response and was never reverted, so production currently allows 500 despite documentation stating 100.",
            "suggested_tags": ["billing", "incident response", "api rate limiting"],
        }))
        .expect("valid candidate")
    }

    fn triage_similar_items() -> Vec<SimilarItemContext> {
        vec![
            SimilarItemContext {
                item_id: KI_RATE_LIMIT.parse().unwrap(),
                kind: KnowledgeKind::Fact,
                content: "The billing service rate limiter uses a sliding window of \
                          60 seconds with a threshold of 100 requests per client."
                    .to_owned(),
                similarity_score: 0.89,
                similarity_label: SimilarityBand::from(0.89).to_string(),
                tags: vec!["billing".to_owned(), "api rate limiting".to_owned()],
            },
            SimilarItemContext {
                item_id: KI_AUTH_CACHE.parse().unwrap(),
                kind: KnowledgeKind::Fact,
                content: "The authentication service caches tokens in Redis with a \
                          15-minute TTL to reduce database load during peak hours."
                    .to_owned(),
                similarity_score: 0.54,
                similarity_label: SimilarityBand::from(0.54).to_string(),
                tags: vec!["authentication".to_owned()],
            },
            SimilarItemContext {
                item_id: KI_DEPLOY_APPROVAL.parse().unwrap(),
                kind: KnowledgeKind::Fact,
                content: "Deployments to production require approval from at least \
                          two senior engineers before the merge can proceed."
                    .to_owned(),
                similarity_score: 0.21,
                similarity_label: SimilarityBand::from(0.21).to_string(),
                tags: vec!["ci pipeline".to_owned()],
            },
        ]
    }

    #[test]
    fn test_prompt_golden_snapshot_triage() {
        let renderer = PromptRenderer::for_validation();
        let tags = sample_tags();

        let system_ctx = triage_system_context();
        let rendered = renderer
            .render(
                include_str!("../../../../prompts/triage/system.tera"),
                system_ctx,
                "triage system",
            )
            .unwrap();
        assert_text_snapshot!(&rendered, "src/prompt/snapshots/triage/system.md");

        let candidate = triage_candidate();
        let similar_items = triage_similar_items();
        let user_ctx = triage_user_context(&candidate, &similar_items, &tags);
        let rendered = renderer
            .render(
                include_str!("../../../../prompts/triage/user.tera"),
                user_ctx,
                "triage user",
            )
            .unwrap();
        assert_text_snapshot!(&rendered, "src/prompt/snapshots/triage/user.md");
    }

    // ---------------------------------------------------------------------
    // Relation
    // ---------------------------------------------------------------------

    fn relation_candidates() -> Vec<Candidate> {
        vec![
            serde_json::from_value(json!({
                "kind": "fact",
                "content": "The billing service rate limiter threshold was raised from 100 to 500 requests per client during the Black Friday 2024 incident response and was never reverted.",
                "suggested_tags": ["billing", "incident response"],
            }))
            .expect("valid candidate"),
            serde_json::from_value(json!({
                "kind": "fact",
                "content": "After the Q3 2025 security audit, the authentication service was changed to validate tokens against the database on every request. The Redis cache is still present but no longer consulted for auth decisions.",
                "suggested_tags": ["authentication", "incident response"],
            }))
            .expect("valid candidate"),
            serde_json::from_value(json!({
                "kind": "heuristic",
                "content": "When investigating billing anomalies, check the rate limiter configuration first — it has been changed during incidents in the past and not always reverted.",
                "suggested_tags": ["billing", "incident response"],
            }))
            .expect("valid candidate"),
        ]
    }

    fn relation_hints() -> Vec<RelationHint> {
        vec![
            serde_json::from_value(json!({
                "source_index": 2,
                "target_index": 0,
                "hint_type": "derived_from",
            }))
            .expect("valid hint"),
        ]
    }

    fn relation_similar_item_decisions() -> Vec<SimilarItemDecisionContext> {
        vec![
            SimilarItemDecisionContext {
                batch_index: 0,
                matched_item_id: KI_RATE_LIMIT.parse().unwrap(),
                matched_content: "The billing service rate limiter uses a sliding window of \
                                  60 seconds with a threshold of 100 requests per client."
                    .to_owned(),
                similarity_score: 0.89,
                similarity_label: SimilarityBand::from(0.89_f32).to_string(),
                suggested_relation: RelationSuggestion::Contradicts,
                justification: "The candidate reports the threshold was changed to 500 \
                                and never reverted, which directly contradicts the \
                                existing item's stated threshold of 100."
                    .to_owned(),
            },
            SimilarItemDecisionContext {
                batch_index: 1,
                matched_item_id: KI_AUTH_CACHE.parse().unwrap(),
                matched_content: "The authentication service caches tokens in Redis with a \
                                  15-minute TTL to reduce database load during peak hours."
                    .to_owned(),
                similarity_score: 0.82,
                similarity_label: SimilarityBand::from(0.82_f32).to_string(),
                suggested_relation: RelationSuggestion::Contradicts,
                justification: "The candidate states Redis is no longer consulted for \
                                auth decisions, which contradicts the existing item's \
                                description of Redis-based token caching."
                    .to_owned(),
            },
            SimilarItemDecisionContext {
                batch_index: 0,
                matched_item_id: KI_BILLING_BATCH.parse().unwrap(),
                matched_content: "The settlement batch process runs on a 4-hour cycle \
                                  and uses whichever API key was active at batch initiation."
                    .to_owned(),
                similarity_score: 0.41,
                similarity_label: SimilarityBand::from(0.41_f32).to_string(),
                suggested_relation: RelationSuggestion::Unrelated,
                justification: "Both items relate to the billing service but address \
                                different subsystems: rate limiting versus settlement \
                                batch processing."
                    .to_owned(),
            },
        ]
    }

    #[test]
    fn test_prompt_golden_snapshot_relation() {
        let renderer = PromptRenderer::for_validation();

        let system_ctx = relation_system_context();
        let rendered = renderer
            .render(
                include_str!("../../../../prompts/relation/system.tera"),
                system_ctx,
                "relation system",
            )
            .unwrap();
        assert_text_snapshot!(&rendered, "src/prompt/snapshots/relation/system.md");

        let candidates_data = relation_candidates();
        let hints = relation_hints();
        let decisions = relation_similar_item_decisions();

        let prompt_ctx = RelationPromptContext {
            candidates: vec![
                CandidateOutcome {
                    batch_index: 0,
                    candidate: &candidates_data[0],
                    outcome: "created".to_owned(),
                    item_id: Some(KI_BILLING_RAISED.parse().unwrap()),
                },
                CandidateOutcome {
                    batch_index: 1,
                    candidate: &candidates_data[1],
                    outcome: "created".to_owned(),
                    item_id: Some(KI_AUTH_DB_LOOKUP.parse().unwrap()),
                },
                CandidateOutcome {
                    batch_index: 2,
                    candidate: &candidates_data[2],
                    outcome: "created".to_owned(),
                    item_id: Some(KI_BILLING_HEURISTIC.parse().unwrap()),
                },
            ],
            relation_hints: &hints,
            similar_item_decisions: &decisions,
        };
        let user_ctx = relation_user_context(&prompt_ctx);
        let rendered = renderer
            .render(
                include_str!("../../../../prompts/relation/user.tera"),
                user_ctx,
                "relation user",
            )
            .unwrap();
        assert_text_snapshot!(&rendered, "src/prompt/snapshots/relation/user.md");
    }
}
