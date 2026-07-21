use chrono::Utc;
use tribal_db::NewGitProject;
use tribal_domain::{GitRemote, Project, ProjectId, ProjectOrigin};

/// Factory for Git-origin [`Project`] instances.
#[derive(Debug, Clone)]
#[must_use]
pub struct ProjectFactory {
    id: ProjectId,
    git_remote: GitRemote,
    name: String,
    default_branch: String,
    project_type: Option<String>,
    schema_version: u32,
    settings: serde_json::Value,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

impl ProjectFactory {
    pub fn new() -> Self {
        Self {
            id: ProjectId::new(),
            git_remote: GitRemote::from_parts("github.com", "test/test-project", None),
            name: "test-project".to_owned(),
            default_branch: "main".to_owned(),
            project_type: None,
            schema_version: 1,
            settings: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub fn build(self) -> Project {
        Project::builder()
            .id(self.id)
            .origin(ProjectOrigin::Git {
                remote: self.git_remote,
                default_branch: self.default_branch,
            })
            .name(self.name)
            .project_type(self.project_type)
            .schema_version(self.schema_version)
            .settings(self.settings)
            .created_at(self.created_at)
            .updated_at(self.updated_at)
            .build()
    }

    pub fn id(mut self, value: ProjectId) -> Self {
        self.id = value;
        self
    }

    pub fn git_remote(mut self, value: GitRemote) -> Self {
        self.git_remote = value;
        self
    }

    pub fn name(mut self, value: String) -> Self {
        self.name = value;
        self
    }

    pub fn default_branch(mut self, value: String) -> Self {
        self.default_branch = value;
        self
    }

    pub fn project_type(mut self, value: Option<String>) -> Self {
        self.project_type = value;
        self
    }

    pub fn schema_version(mut self, value: u32) -> Self {
        self.schema_version = value;
        self
    }

    pub fn settings(mut self, value: serde_json::Value) -> Self {
        self.settings = value;
        self
    }
}

impl Default for ProjectFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for [`NewGitProject`] repository inputs.
#[derive(Debug, Clone)]
#[must_use]
pub struct NewProjectFactory {
    git_remote: GitRemote,
    name: String,
    default_branch: String,
    project_type: Option<String>,
    schema_version: u32,
    settings: serde_json::Value,
}

impl NewProjectFactory {
    pub fn new() -> Self {
        Self {
            git_remote: GitRemote::from_parts("github.com", "test/test-project", None),
            name: "test-project".to_owned(),
            default_branch: "main".to_owned(),
            project_type: None,
            schema_version: 1,
            settings: serde_json::json!({}),
        }
    }

    pub fn build(self) -> NewGitProject {
        NewGitProject::builder()
            .remote(self.git_remote)
            .name(self.name)
            .default_branch(self.default_branch)
            .project_type(self.project_type)
            .schema_version(self.schema_version)
            .settings(self.settings)
            .build()
    }

    pub fn git_remote(mut self, value: GitRemote) -> Self {
        self.git_remote = value;
        self
    }

    pub fn name(mut self, value: String) -> Self {
        self.name = value;
        self
    }

    pub fn default_branch(mut self, value: String) -> Self {
        self.default_branch = value;
        self
    }

    pub fn project_type(mut self, value: Option<String>) -> Self {
        self.project_type = value;
        self
    }

    pub fn schema_version(mut self, value: u32) -> Self {
        self.schema_version = value;
        self
    }

    pub fn settings(mut self, value: serde_json::Value) -> Self {
        self.settings = value;
        self
    }
}

impl Default for NewProjectFactory {
    fn default() -> Self {
        Self::new()
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
        assert!(matches!(project.origin(), ProjectOrigin::Git { .. }));
    }

    #[test]
    fn test_new_project_builds_with_defaults() {
        let new_project = a_new_project().build();
        assert_eq!(new_project.name, "test-project");
        assert_eq!(new_project.default_branch, "main");
    }
}
