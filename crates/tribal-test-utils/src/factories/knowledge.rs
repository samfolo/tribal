use chrono::Utc;
use tribal_domain::{
    Confidence, KnowledgeItem, KnowledgeItemId, KnowledgeKind, PrincipalId, ProjectId,
};

define_factory! {
    /// Factory for [`KnowledgeItem`] instances.
    pub struct KnowledgeItemFactory for KnowledgeItem {
        id: KnowledgeItemId = KnowledgeItemId::new(),
        project_id: ProjectId = ProjectId::new(),
        principal_id: PrincipalId = PrincipalId::new(),
        kind: KnowledgeKind = KnowledgeKind::Heuristic,
        content: String = "test knowledge content".to_owned(),
        tags: Vec<String> = Vec::new(),
        confidence: Confidence = Confidence::Inferred,
        claim_context: Option<serde_json::Value> = None,
        source_context: serde_json::Value = serde_json::json!({}),
        episode_id: Option<tribal_domain::EpisodeId> = None,
        capture_commit: Option<String> = None,
        capture_branch: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`KnowledgeItemFactory`] with sensible defaults.
pub fn a_knowledge_item() -> KnowledgeItemFactory {
    KnowledgeItemFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let item = a_knowledge_item().build();
        assert_eq!(item.kind(), KnowledgeKind::Heuristic);
        assert_eq!(item.content(), "test knowledge content");
        assert_eq!(item.confidence(), Confidence::Inferred);
    }
}
