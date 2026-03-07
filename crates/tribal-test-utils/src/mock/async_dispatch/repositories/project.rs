//! Mock implementation of [`ProjectRepository`].

use tribal_db::{NewProject, ProjectRepository};
use tribal_domain::{Project, ProjectId};

use super::mock_repository;

mock_repository! {
    MockProjectRepository for ProjectRepository {
        insert(NewProject => Project)
            (new_project: &NewProject) { new_project.clone() };
        find_by_id(ProjectId => Project)
            (id: ProjectId) { id };
        find_by_git_remote(String => Option<Project>)
            (git_remote: &str) { git_remote.to_owned() };
        list(() => Vec<Project>)
            () { () }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::SubsecRound;
    use tribal_db::DbError;

    use crate::mock::async_dispatch::core::ExhaustBehaviour;
    use crate::mock::async_dispatch::repositories::error_factories::a_not_found;
    use crate::test_context;

    use super::*;

    fn a_project(name: &str) -> Project {
        Project::builder()
            .id(ProjectId::new())
            .git_remote(format!("git@github.com:user/{name}.git"))
            .name(name.to_owned())
            .default_branch("main".to_owned())
            .project_type(None)
            .schema_version(1)
            .settings(serde_json::json!({}))
            .created_at(chrono::Utc::now().trunc_subsecs(6))
            .updated_at(chrono::Utc::now().trunc_subsecs(6))
            .build()
    }

    #[tokio::test]
    async fn test_sequential_find_by_id_returns_in_order() {
        let p1 = a_project("first");
        let p2 = a_project("second");

        let mock = MockProjectRepository::builder()
            .on_find_by_id(p1.clone(), None)
            .on_find_by_id(p2.clone(), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await;

        let r1 = mock.find_by_id(&mut tx, ProjectId::new()).await.unwrap();
        let r2 = mock.find_by_id(&mut tx, ProjectId::new()).await.unwrap();

        assert_eq!(r1.name, "first");
        assert_eq!(r2.name, "second");
    }

    #[tokio::test]
    async fn test_conditional_find_by_id_matches_by_predicate() {
        let target_id = ProjectId::new();
        let project = a_project("matched");

        let mock = MockProjectRepository::builder()
            .when_find_by_id(move |id| *id == target_id)
            .respond_with(project.clone(), None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await;

        let result = mock.find_by_id(&mut tx, target_id).await.unwrap();
        assert_eq!(result.name, "matched");
    }

    #[tokio::test]
    #[should_panic(expected = "sequential queue exhausted")]
    async fn test_exhaust_panic_on_empty_queue() {
        let mock = MockProjectRepository::builder().build();
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await;
        let _ = mock.find_by_id(&mut tx, ProjectId::new()).await;
    }

    #[tokio::test]
    async fn test_exhaust_repeat_last() {
        let project = a_project("repeated");

        let mock = MockProjectRepository::builder()
            .on_find_by_id(project.clone(), None)
            .on_find_by_id_exhaust(ExhaustBehaviour::RepeatLast)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await;

        let r1 = mock.find_by_id(&mut tx, ProjectId::new()).await.unwrap();
        let r2 = mock.find_by_id(&mut tx, ProjectId::new()).await.unwrap();
        let r3 = mock.find_by_id(&mut tx, ProjectId::new()).await.unwrap();

        assert_eq!(r1.name, "repeated");
        assert_eq!(r2.name, "repeated");
        assert_eq!(r3.name, "repeated");
        assert_eq!(mock.find_by_id_call_count(), 3);
    }

    #[tokio::test]
    async fn test_exhaust_error_returns_factory_output() {
        let mock = MockProjectRepository::builder()
            .on_find_by_id_exhaust(ExhaustBehaviour::Error(a_not_found(
                "project",
                "proj_missing",
            )))
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await;
        let err = mock
            .find_by_id(&mut tx, ProjectId::new())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            DbError::NotFound { entity: "project", ref id }
            if id == "proj_missing"
        ));
    }

    #[tokio::test]
    async fn test_call_history_records_requests() {
        let p = a_project("any");

        let mock = MockProjectRepository::builder()
            .on_find_by_id(p.clone(), None)
            .on_find_by_id(p, None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await;

        let id1 = ProjectId::new();
        let id2 = ProjectId::new();
        let _ = mock.find_by_id(&mut tx, id1).await;
        let _ = mock.find_by_id(&mut tx, id2).await;

        let history = mock.find_by_id_history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0], id1);
        assert_eq!(history[1], id2);
    }

    #[test]
    fn test_send_sync_behind_arc() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MockProjectRepository>();

        let mock = MockProjectRepository::builder().build();
        let _arc: Arc<dyn ProjectRepository + Send + Sync> = Arc::new(mock);
    }

    #[tokio::test]
    async fn test_insert_clones_ref_param() {
        let project = a_project("inserted");

        let mock = MockProjectRepository::builder()
            .on_insert(project, None)
            .build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await;

        let new_project = NewProject::builder()
            .git_remote("git@github.com:user/test.git".to_owned())
            .name("test".to_owned())
            .default_branch("main".to_owned())
            .schema_version(1)
            .settings(serde_json::json!({}))
            .build();

        let _ = mock.insert(&mut tx, &new_project).await.unwrap();

        let history = mock.insert_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].name, "test");
    }
}
