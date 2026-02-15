use chrono::Utc;
use tribal_db::NewProject;
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

define_factory! {
    /// Factory for [`NewProject`] instances used in repository insert operations.
    pub struct NewProjectFactory for NewProject {
        git_remote: String = "git@github.com:test/test-project.git".to_owned(),
        name: String = "test-project".to_owned(),
        default_branch: String = "main".to_owned(),
        project_type: Option<String> = None,
        schema_version: u32 = 1,
        settings: serde_json::Value = serde_json::json!({}),
    }
}

/// Returns a [`ProjectFactory`] with sensible defaults.
pub fn a_project() -> ProjectFactory {
    ProjectFactory::new()
}

/// Returns a [`NewProjectFactory`] with sensible defaults.
pub fn a_new_project() -> NewProjectFactory {
    NewProjectFactory::new()
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

    #[test]
    fn test_new_project_builds_with_defaults() {
        let new_project = a_new_project().build();
        assert_eq!(new_project.name, "test-project");
        assert_eq!(new_project.default_branch, "main");
    }
}
