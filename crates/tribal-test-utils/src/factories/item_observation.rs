use chrono::Utc;
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
pub fn a_item_observation() -> ItemObservationFactory {
    ItemObservationFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let o = a_item_observation().build();
        assert_eq!(o.source_type(), SourceType::AgentMediated);
    }
}
