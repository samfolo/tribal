use chrono::Utc;
use tribal_domain::{Project, ProjectId};

define_factory! {
    /// Factory for [`Project`] instances.
    pub struct ProjectFactory for Project {
        id: ProjectId = ProjectId::new(),
        git_remote: String = "git@github.com:test/test-project.git".to_owned(),
        name: String = "test-project".to_owned(),
        default_branch: String = "main".to_owned(),
        project_type: Option<String> = None,
        schema_version: u32 = 1,
        settings: serde_json::Value = serde_json::json!({}),
        created_at: chrono::DateTime<Utc> = Utc::now(),
        updated_at: chrono::DateTime<Utc> = Utc::now(),
    }
}

/// Returns a [`ProjectFactory`] with sensible defaults.
pub fn a_project() -> ProjectFactory {
    ProjectFactory::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builds_with_defaults() {
        let project = a_project().build();
        assert_eq!(project.name(), "test-project");
        assert_eq!(project.default_branch(), "main");
    }
}
