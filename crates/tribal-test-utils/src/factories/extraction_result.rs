use chrono::Utc;
use tribal_db::NewExtractionResult;
use tribal_domain::{ExtractionResult, ExtractionResultId, JobId};

define_factory! {
    /// Factory for [`ExtractionResult`] instances.
    pub struct ExtractionResultFactory for ExtractionResult {
        id: ExtractionResultId = ExtractionResultId::new(),
        job_id: JobId = JobId::new(),
        candidates: serde_json::Value = serde_json::json!([]),
        relation_hints: serde_json::Value = serde_json::json!([]),
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns an [`ExtractionResultFactory`] with sensible defaults.
pub fn an_extraction_result() -> ExtractionResultFactory {
    ExtractionResultFactory::new()
}

define_factory! {
    /// Factory for [`NewExtractionResult`] instances used in repository
    /// insert operations.
    pub struct NewExtractionResultFactory for NewExtractionResult {
        job_id: JobId = JobId::new(),
        candidates: serde_json::Value = serde_json::json!([]),
        relation_hints: serde_json::Value = serde_json::json!([]),
    }
}

/// Returns a [`NewExtractionResultFactory`] with sensible defaults.
pub fn a_new_extraction_result() -> NewExtractionResultFactory {
    NewExtractionResultFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let result = an_extraction_result().build();
        assert_eq!(result.candidates(), &serde_json::json!([]));
        assert_eq!(result.relation_hints(), &serde_json::json!([]));
    }

    #[test]
    fn test_new_extraction_result_builds_with_defaults() {
        let new = a_new_extraction_result().build();
        assert_eq!(new.candidates, serde_json::json!([]));
        assert_eq!(new.relation_hints, serde_json::json!([]));
    }
}
