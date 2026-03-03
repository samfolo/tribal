use chrono::Utc;
use tribal_db::NewKnowledgeItemRelation;
use tribal_domain::{
    KnowledgeItemId, KnowledgeItemRelation, PrincipalId, RelationBatchId, RelationId, RelationKind,
};

define_factory! {
    /// Factory for [`KnowledgeItemRelation`] instances.
    pub struct KnowledgeItemRelationFactory for KnowledgeItemRelation {
        id: RelationId = RelationId::new(),
        relation_batch_id: RelationBatchId = RelationBatchId::new(),
        source_id: KnowledgeItemId = KnowledgeItemId::new(),
        target_id: KnowledgeItemId = KnowledgeItemId::new(),
        relation_type: RelationKind = RelationKind::Supports,
        principal_id: PrincipalId = PrincipalId::new(),
        justification: Option<String> = None,
        created_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`KnowledgeItemRelationFactory`] with sensible defaults.
pub fn a_knowledge_item_relation() -> KnowledgeItemRelationFactory {
    KnowledgeItemRelationFactory::new()
}

define_factory! {
    /// Factory for [`NewKnowledgeItemRelation`] instances used in
    /// repository batch insert operations.
    pub struct NewKnowledgeItemRelationFactory for NewKnowledgeItemRelation {
        relation_batch_id: RelationBatchId = RelationBatchId::new(),
        source_id: KnowledgeItemId = KnowledgeItemId::new(),
        target_id: KnowledgeItemId = KnowledgeItemId::new(),
        relation_type: RelationKind = RelationKind::Supports,
        principal_id: PrincipalId = PrincipalId::new(),
        justification: Option<String> = None,
    }
}

/// Returns a [`NewKnowledgeItemRelationFactory`] with sensible defaults.
pub fn a_new_knowledge_item_relation() -> NewKnowledgeItemRelationFactory {
    NewKnowledgeItemRelationFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let r = a_knowledge_item_relation().build();
        assert_eq!(r.relation_type(), RelationKind::Supports);
    }

    #[test]
    fn test_new_knowledge_item_relation_builds_with_defaults() {
        let new = a_new_knowledge_item_relation().build();
        assert_eq!(new.relation_type.as_str(), "supports");
    }
}
