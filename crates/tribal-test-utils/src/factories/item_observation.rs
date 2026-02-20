use chrono::Utc;
use tribal_db::NewItemObservation;
use tribal_domain::{ItemObservation, ItemObservationId, KnowledgeItemId, PrincipalId, SourceType};

define_factory! {
    /// Factory for [`ItemObservation`] instances.
    pub struct ItemObservationFactory for ItemObservation {
        id: ItemObservationId = ItemObservationId::new(),
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        principal_id: PrincipalId = PrincipalId::new(),
        source_type: SourceType = SourceType::AgentMediated,
        observed_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns an [`ItemObservationFactory`] with sensible defaults.
pub fn an_item_observation() -> ItemObservationFactory {
    ItemObservationFactory::new()
}

define_factory! {
    /// Factory for [`NewItemObservation`] instances used in repository
    /// insert operations.
    pub struct NewItemObservationFactory for NewItemObservation {
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        principal_id: PrincipalId = PrincipalId::new(),
        source_type: SourceType = SourceType::AgentMediated,
    }
}

/// Returns a [`NewItemObservationFactory`] with sensible defaults.
pub fn a_new_item_observation() -> NewItemObservationFactory {
    NewItemObservationFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let o = an_item_observation().build();
        assert_eq!(o.source_type(), SourceType::AgentMediated);
    }

    #[test]
    fn test_new_item_observation_builds_with_defaults() {
        let new = a_new_item_observation().build();
        assert_eq!(new.source_type.as_str(), "agent_mediated");
    }
}
