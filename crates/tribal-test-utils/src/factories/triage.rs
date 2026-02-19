use chrono::Utc;
use tribal_db::{NewTriageResult, NewTriageSimilarItemDecision};
use tribal_domain::{
    ItemObservationId, JobId, KnowledgeItemId, RelationSuggestion, SimilarItem, TriageOutcome,
    TriageResult, TriageResultId, TriageSimilarItemDecision, TriageSimilarItemDecisionId,
};

// ---------------------------------------------------------------------------
// SimilarItem
// ---------------------------------------------------------------------------

define_factory! {
    /// Factory for [`SimilarItem`] instances.
    pub struct SimilarItemFactory for SimilarItem {
        item_id: KnowledgeItemId = KnowledgeItemId::new(),
        similarity_score: f32 = 0.85,
        suggested_relation: RelationSuggestion = RelationSuggestion::Supports,
    }
}

/// Returns a [`SimilarItemFactory`] with sensible defaults.
pub fn a_similar_item() -> SimilarItemFactory {
    SimilarItemFactory::new()
}

// ---------------------------------------------------------------------------
// TriageSimilarItemDecision
// ---------------------------------------------------------------------------

define_factory! {
    /// Factory for [`TriageSimilarItemDecision`] instances.
    pub struct TriageSimilarItemDecisionFactory for TriageSimilarItemDecision {
        id: TriageSimilarItemDecisionId = TriageSimilarItemDecisionId::new(),
        job_id: JobId = JobId::new(),
        batch_index: u32 = 0,
        matched_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        similarity_score: f32 = 0.85,
        suggested_relation: RelationSuggestion = RelationSuggestion::Supports,
        justification_text: String = "test justification".to_owned(),
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`TriageSimilarItemDecisionFactory`] with sensible defaults.
pub fn a_triage_similar_item_decision() -> TriageSimilarItemDecisionFactory {
    TriageSimilarItemDecisionFactory::new()
}

// ---------------------------------------------------------------------------
// TriageResult
// ---------------------------------------------------------------------------

define_factory! {
    /// Factory for [`TriageResult`] instances.
    ///
    /// Use the outcome-specific entry points instead: [`a_triage_result_created`],
    /// [`a_triage_result_duplicate`], or [`a_triage_result_failed`].
    pub struct TriageResultFactory for TriageResult {
        id: TriageResultId = TriageResultId::new(),
        job_id: JobId = JobId::new(),
        batch_index: u32 = 0,
        similar_items: Vec<SimilarItem> = Vec::new(),
        created_at: chrono::DateTime<Utc> = Utc::now(),
        outcome: TriageOutcome = TriageOutcome::Created { item_id: KnowledgeItemId::new() },
    }
}

/// Returns a [`TriageResultFactory`] with a `Created` outcome.
pub fn a_triage_result_created() -> TriageResultFactory {
    TriageResultFactory::new()
}

/// Returns a [`TriageResultFactory`] with a `Duplicate` outcome.
pub fn a_triage_result_duplicate() -> TriageResultFactory {
    TriageResultFactory::new().outcome(TriageOutcome::Duplicate {
        observation_id: ItemObservationId::new(),
        matched_item_id: KnowledgeItemId::new(),
    })
}

/// Returns a [`TriageResultFactory`] with a `Failed` outcome.
pub fn a_triage_result_failed() -> TriageResultFactory {
    TriageResultFactory::new().outcome(TriageOutcome::Failed {
        error_message: "test failure".to_owned(),
        retryable: false,
    })
}

// ---------------------------------------------------------------------------
// NewTriageResult
// ---------------------------------------------------------------------------

define_factory! {
    /// Factory for [`NewTriageResult`] instances used in repository insert
    /// operations.
    ///
    /// Use the outcome-specific entry points: [`a_new_triage_result_created`],
    /// [`a_new_triage_result_duplicate`], or [`a_new_triage_result_failed`].
    pub struct NewTriageResultFactory for NewTriageResult {
        job_id: JobId = JobId::new(),
        batch_index: u32 = 0,
        outcome: TriageOutcome = TriageOutcome::Created { item_id: KnowledgeItemId::new() },
    }
}

/// Returns a [`NewTriageResultFactory`] with a `Created` outcome.
pub fn a_new_triage_result_created() -> NewTriageResultFactory {
    NewTriageResultFactory::new()
}

/// Returns a [`NewTriageResultFactory`] with a `Duplicate` outcome.
pub fn a_new_triage_result_duplicate() -> NewTriageResultFactory {
    NewTriageResultFactory::new().outcome(TriageOutcome::Duplicate {
        observation_id: ItemObservationId::new(),
        matched_item_id: KnowledgeItemId::new(),
    })
}

/// Returns a [`NewTriageResultFactory`] with a `Failed` outcome.
pub fn a_new_triage_result_failed() -> NewTriageResultFactory {
    NewTriageResultFactory::new().outcome(TriageOutcome::Failed {
        error_message: "test failure".to_owned(),
        retryable: false,
    })
}

// ---------------------------------------------------------------------------
// NewTriageSimilarItemDecision
// ---------------------------------------------------------------------------

define_factory! {
    /// Factory for [`NewTriageSimilarItemDecision`] instances used in
    /// repository batch insert operations.
    pub struct NewTriageSimilarItemDecisionFactory for NewTriageSimilarItemDecision {
        job_id: JobId = JobId::new(),
        batch_index: u32 = 0,
        matched_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        similarity_score: f32 = 0.85,
        suggested_relation: RelationSuggestion = RelationSuggestion::Supports,
        justification_text: String = "test justification".to_owned(),
    }
}

/// Returns a [`NewTriageSimilarItemDecisionFactory`] with sensible defaults.
pub fn a_new_triage_similar_item_decision() -> NewTriageSimilarItemDecisionFactory {
    NewTriageSimilarItemDecisionFactory::new()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_similar_item_builds_with_defaults() {
        let s = a_similar_item().build();
        assert!((s.similarity_score() - 0.85).abs() < f32::EPSILON);
        assert_eq!(s.suggested_relation(), RelationSuggestion::Supports);
    }

    #[test]
    fn test_triage_similar_item_decision_builds_with_defaults() {
        let d = a_triage_similar_item_decision().build();
        assert_eq!(d.suggested_relation(), RelationSuggestion::Supports);
        assert_eq!(d.justification_text(), "test justification");
    }

    #[test]
    fn test_triage_result_created_builds_with_defaults() {
        let r = a_triage_result_created().build();
        assert!(matches!(r.outcome(), TriageOutcome::Created { .. }));
    }

    #[test]
    fn test_triage_result_duplicate_builds_with_defaults() {
        let r = a_triage_result_duplicate().build();
        assert!(matches!(r.outcome(), TriageOutcome::Duplicate { .. }));
    }

    #[test]
    fn test_triage_result_failed_builds_with_defaults() {
        let r = a_triage_result_failed().build();
        assert!(matches!(
            r.outcome(),
            TriageOutcome::Failed {
                retryable: false,
                ..
            }
        ));
    }

    #[test]
    fn test_new_triage_result_created_builds_with_defaults() {
        let new = a_new_triage_result_created().build();
        assert!(matches!(new.outcome, TriageOutcome::Created { .. }));
        assert_eq!(new.batch_index, 0);
    }

    #[test]
    fn test_new_triage_result_duplicate_builds_with_defaults() {
        let new = a_new_triage_result_duplicate().build();
        assert!(matches!(new.outcome, TriageOutcome::Duplicate { .. }));
    }

    #[test]
    fn test_new_triage_result_failed_builds_with_defaults() {
        let new = a_new_triage_result_failed().build();
        assert!(matches!(
            new.outcome,
            TriageOutcome::Failed {
                retryable: false,
                ..
            }
        ));
    }

    #[test]
    fn test_new_triage_similar_item_decision_builds_with_defaults() {
        let new = a_new_triage_similar_item_decision().build();
        assert_eq!(new.suggested_relation.as_str(), "supports");
        assert_eq!(new.justification_text, "test justification");
    }
}
