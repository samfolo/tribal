use tribal_db::{DbError, PgProjectRepository, ProjectRepository};
use tribal_domain::ProjectId;
use tribal_test_utils::{a_new_project, test_context};

#[tokio::test]
async fn test_insert_returns_populated_project() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let new = a_new_project()
        .git_remote("git@github.com:test/insert-test.git".to_owned())
        .name("insert-test".to_owned())
        .project_type(Some("cli_tool".to_owned()))
        .settings(serde_json::json!({"key": "value"}))
        .build();

    let project = repo.insert(&mut *txn, &new).await.expect("insert");

    assert_eq!(project.git_remote(), "git@github.com:test/insert-test.git");
    assert_eq!(project.name(), "insert-test");
    assert_eq!(project.default_branch(), "main");
    assert_eq!(project.project_type(), Some("cli_tool"));
    assert_eq!(project.schema_version(), 1);
    assert_eq!(project.settings(), &serde_json::json!({"key": "value"}));
    assert!(project.id().to_string().starts_with("proj_"));
}

#[tokio::test]
async fn test_insert_duplicate_git_remote_returns_unique_violation() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let new = a_new_project()
        .git_remote("git@github.com:test/dup-remote.git".to_owned())
        .build();
    repo.insert(&mut *txn, &new).await.expect("first insert");

    let result = repo.insert(&mut *txn, &new).await;
    assert!(
        matches!(result, Err(DbError::UniqueViolation { .. })),
        "expected UniqueViolation, got: {result:?}"
    );
}

#[tokio::test]
async fn test_find_by_id_returns_project() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let new = a_new_project()
        .git_remote("git@github.com:test/find-id.git".to_owned())
        .build();
    let inserted = repo.insert(&mut *txn, &new).await.expect("insert");

    let found = repo
        .find_by_id(&mut *txn, inserted.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.git_remote(), inserted.git_remote());
}

#[tokio::test]
async fn test_find_by_id_not_found_returns_error() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let result = repo.find_by_id(&mut *txn, ProjectId::new()).await;
    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}

#[tokio::test]
async fn test_find_by_git_remote_returns_project() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let remote = "git@github.com:test/find-remote.git";
    let new = a_new_project().git_remote(remote.to_owned()).build();
    let inserted = repo.insert(&mut *txn, &new).await.expect("insert");

    let found = repo
        .find_by_git_remote(&mut *txn, remote)
        .await
        .expect("find_by_git_remote");

    assert!(found.is_some());
    assert_eq!(found.unwrap().id(), inserted.id());
}

#[tokio::test]
async fn test_find_by_git_remote_not_found_returns_none() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let result = repo
        .find_by_git_remote(&mut *txn, "git@github.com:nonexistent/repo.git")
        .await
        .expect("find_by_git_remote");

    assert!(result.is_none());
}

#[tokio::test]
async fn test_list_returns_all_projects_ordered_by_created_at() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let first = repo
        .insert(
            &mut *txn,
            &a_new_project()
                .git_remote("git@github.com:test/list-first.git".to_owned())
                .build(),
        )
        .await
        .expect("insert first");
    let second = repo
        .insert(
            &mut *txn,
            &a_new_project()
                .git_remote("git@github.com:test/list-second.git".to_owned())
                .build(),
        )
        .await
        .expect("insert second");

    let projects = repo.list(&mut *txn).await.expect("list");

    assert!(projects.len() >= 2);
    let ids: Vec<ProjectId> = projects.iter().map(|p| p.id()).collect();
    let first_pos = ids.iter().position(|id| *id == first.id()).unwrap();
    let second_pos = ids.iter().position(|id| *id == second.id()).unwrap();
    assert!(
        first_pos < second_pos,
        "first inserted should appear before second"
    );
}

#[tokio::test]
async fn test_list_returns_empty_vec_when_no_projects() {
    let ctx = test_context().await;
    let mut txn = ctx.begin_test().await.expect("begin_test");
    let repo = PgProjectRepository;

    let projects = repo.list(&mut *txn).await.expect("list");
    assert!(projects.is_empty());
}
