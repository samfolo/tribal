use chrono::Utc;
use tribal_db::NewKnowledgeItem;
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

define_factory! {
    /// Factory for [`NewKnowledgeItem`] instances used in repository insert operations.
    pub struct NewKnowledgeItemFactory for NewKnowledgeItem {
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
    }
}

/// Returns a [`KnowledgeItemFactory`] with sensible defaults.
pub fn a_knowledge_item() -> KnowledgeItemFactory {
    KnowledgeItemFactory::new()
}

/// Returns a [`NewKnowledgeItemFactory`] with sensible defaults.
pub fn a_new_knowledge_item() -> NewKnowledgeItemFactory {
    NewKnowledgeItemFactory::new()
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

    #[test]
    fn test_new_knowledge_item_builds_with_defaults() {
        let new = a_new_knowledge_item().build();
        assert_eq!(new.kind.as_str(), "heuristic");
        assert_eq!(new.content, "test knowledge content");
        assert_eq!(new.confidence.as_str(), "inferred");
    }
}
