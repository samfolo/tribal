use tribal_db::NewReference;
use tribal_domain::{KnowledgeItemId, ProjectId, Reference, ReferenceId, ReferenceKind};

define_factory! {
    /// Factory for [`Reference`] instances.
    pub struct ReferenceFactory for Reference {
        id: ReferenceId = ReferenceId::new(),
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        kind: ReferenceKind = ReferenceKind::FilePath,
        value: String = "//src/lib.rs".to_owned(),
        description: Option<String> = None,
        project_id: ProjectId = ProjectId::new(),
        commit: Option<String> = None,
        branch: Option<String> = None,
    }
}

/// Returns a [`ReferenceFactory`] with sensible defaults.
pub fn a_reference() -> ReferenceFactory {
    ReferenceFactory::new()
}

define_factory! {
    /// Factory for [`NewReference`] instances used in repository insert operations.
    pub struct NewReferenceFactory for NewReference {
        knowledge_item_id: KnowledgeItemId = KnowledgeItemId::new(),
        kind: ReferenceKind = ReferenceKind::FilePath,
        value: String = "//src/lib.rs".to_owned(),
        description: Option<String> = None,
        project_id: ProjectId = ProjectId::new(),
        commit: Option<String> = None,
        branch: Option<String> = None,
    }
}

/// Returns a [`NewReferenceFactory`] with sensible defaults.
pub fn a_new_reference() -> NewReferenceFactory {
    NewReferenceFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let r = a_reference().build();
        assert_eq!(r.kind(), ReferenceKind::FilePath);
        assert_eq!(r.value(), "//src/lib.rs");
    }

    #[test]
    fn test_new_reference_builds_with_defaults() {
        let new = a_new_reference().build();
        assert_eq!(new.kind.as_str(), "file_path");
        assert_eq!(new.value, "//src/lib.rs");
    }
}
