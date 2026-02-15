use tribal_domain::{KnowledgeItemId, Standing};

define_factory! {
    /// Factory for [`Standing`] instances.
    pub struct StandingFactory for Standing {
        supporting_count: u32 = 0,
        contradicting_count: u32 = 0,
        superseded_by: Option<KnowledgeItemId> = None,
        observation_count: u32 = 1,
        newest_supporting_id: Option<KnowledgeItemId> = None,
        newest_contradicting_id: Option<KnowledgeItemId> = None,
        supporting_episode_count: u32 = 0,
        supporting_project_count: u32 = 0,
    }
}

/// Returns a [`StandingFactory`] with sensible defaults.
pub fn a_standing() -> StandingFactory {
    StandingFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let s = a_standing().build();
        assert_eq!(s.supporting_count(), 0);
        assert_eq!(s.observation_count(), 1);
        assert!(s.superseded_by().is_none());
    }
}
