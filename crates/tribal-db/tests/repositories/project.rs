use tribal_db::{
    DbError, EnsureSystemOutcome, PgProjectRepository, ProjectPageKey, ProjectRepository,
};
use tribal_domain::{GitRemote, Project, ProjectId, ProjectOrigin};
use tribal_test_utils::{TestDb, a_new_project, set_timestamp, truncate_all_tables};

#[tokio::test]
async fn test_insert_with_none_optional_fields_returns_none() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let new = a_new_project()
        .remote(GitRemote::from_parts(
            "github.com",
            "test/null-fields",
            None,
        ))
        .build();
    let project = repo.insert_git(&mut txn, &new).await.expect("insert");

    assert_eq!(project.project_type(), None);
}

#[tokio::test]
async fn test_insert_returns_populated_project() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let new = a_new_project()
        .remote(GitRemote::from_parts(
            "github.com",
            "test/insert-test",
            None,
        ))
        .name("insert-test".to_owned())
        .project_type(Some("cli_tool".to_owned()))
        .settings(serde_json::json!({"key": "value"}))
        .build();

    let project = repo.insert_git(&mut txn, &new).await.expect("insert");

    assert_eq!(
        project.origin(),
        &ProjectOrigin::Git {
            remote: GitRemote::from_parts("github.com", "test/insert-test", None,),
            default_branch: "main".to_owned(),
        }
    );
    assert_eq!(project.name(), "insert-test");
    assert_eq!(project.project_type(), Some("cli_tool"));
    assert_eq!(project.schema_version(), 1);
    assert_eq!(project.settings(), &serde_json::json!({"key": "value"}));
    assert!(project.id().to_string().starts_with("proj_"));
}

#[tokio::test]
async fn test_insert_duplicate_git_remote_returns_unique_violation() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let new = a_new_project()
        .remote(GitRemote::from_parts("github.com", "test/dup-remote", None))
        .build();
    repo.insert_git(&mut txn, &new).await.expect("first insert");

    let result = repo.insert_git(&mut txn, &new).await;
    assert!(
        matches!(result, Err(DbError::UniqueViolation { .. })),
        "expected UniqueViolation, got: {result:?}"
    );
}

#[tokio::test]
async fn test_find_by_id_returns_project() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let new = a_new_project()
        .remote(GitRemote::from_parts("github.com", "test/find-id", None))
        .build();
    let inserted = repo.insert_git(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_id(&mut txn, inserted.id())
        .await
        .expect("find_by_id");

    assert_eq!(found.id(), inserted.id());
    assert_eq!(found.origin(), inserted.origin());
}

#[tokio::test]
async fn test_find_by_id_not_found_returns_error() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let result = repo.find_by_id(&mut txn, ProjectId::new()).await;
    assert!(
        matches!(result, Err(DbError::NotFound { .. })),
        "expected NotFound, got: {result:?}"
    );
}

#[tokio::test]
async fn test_find_by_git_remote_returns_project() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let remote = GitRemote::from_parts("github.com", "test/find-remote", None);
    let new = a_new_project().remote(remote.clone()).build();
    let inserted = repo.insert_git(&mut txn, &new).await.expect("insert");

    let found = repo
        .find_by_git_remote(&mut txn, &remote)
        .await
        .expect("find_by_git_remote");

    assert!(found.is_some());
    assert_eq!(found.unwrap().id(), inserted.id());
}

#[tokio::test]
async fn test_find_by_git_remote_not_found_returns_none() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let result = repo
        .find_by_git_remote(
            &mut txn,
            &GitRemote::from_parts("github.com", "nonexistent/repo", None),
        )
        .await
        .expect("find_by_git_remote");

    assert!(result.is_none());
}

#[tokio::test]
async fn test_list_returns_all_projects_ordered_by_created_at() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let first = repo
        .insert_git(
            &mut txn,
            &a_new_project()
                .remote(GitRemote::from_parts("github.com", "test/list-first", None))
                .build(),
        )
        .await
        .expect("insert first");
    let second = repo
        .insert_git(
            &mut txn,
            &a_new_project()
                .remote(GitRemote::from_parts(
                    "github.com",
                    "test/list-second",
                    None,
                ))
                .build(),
        )
        .await
        .expect("insert second");

    // Both inserts share the same transaction timestamp (now() is
    // transaction_timestamp()).  Shift the second project's created_at
    // backward so it should sort before the first.
    let backdated = first.created_at() - chrono::Duration::seconds(10);
    set_timestamp(
        &mut txn,
        "projects",
        "created_at",
        *second.id().inner(),
        backdated,
    )
    .await;

    let projects = repo.list(&mut txn).await.expect("list");

    assert!(projects.len() >= 2);
    let ids: Vec<ProjectId> = projects.iter().map(Project::id).collect();
    let first_pos = ids.iter().position(|id| *id == first.id()).unwrap();
    let second_pos = ids.iter().position(|id| *id == second.id()).unwrap();
    assert!(
        second_pos < first_pos,
        "second (backdated) should appear before first"
    );
}

#[tokio::test]
async fn test_list_returns_empty_vec_when_no_projects() {
    let ctx = TestDb::new().await;
    let mut txn = ctx.begin().await.expect("begin");
    let repo = PgProjectRepository;

    let projects = repo.list(&mut txn).await.expect("list");
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].origin(), &ProjectOrigin::System);
}

#[tokio::test]
async fn test_keyset_page_keeps_captured_high_water_across_later_insert() {
    let ctx = TestDb::new().await;
    let mut connection = ctx.raw_connection().await.expect("connect");
    let repo = PgProjectRepository;
    for suffix in ["one", "two", "three"] {
        repo.insert_git(
            &mut connection,
            &a_new_project()
                .remote(GitRemote::from_parts(
                    "github.com",
                    &format!("test/page-{suffix}"),
                    None,
                ))
                .build(),
        )
        .await
        .expect("seed project");
    }
    let high_water = repo
        .page_high_water(&mut connection)
        .await
        .expect("high water")
        .expect("seeded inventory");
    let first = repo
        .list_page(&mut connection, high_water, None, 2)
        .await
        .expect("first page");
    assert_eq!(first.len(), 2);

    let inserted_later = repo
        .insert_git(
            &mut connection,
            &a_new_project()
                .remote(GitRemote::from_parts("github.com", "test/page-later", None))
                .build(),
        )
        .await
        .expect("later insert");
    let after = first.last().expect("first page is non-empty");
    let second = repo
        .list_page(
            &mut connection,
            high_water,
            Some(ProjectPageKey {
                created_at: after.created_at(),
                id: after.id(),
            }),
            2,
        )
        .await
        .expect("second page");

    let ids: Vec<ProjectId> = first.iter().chain(&second).map(Project::id).collect();
    assert_eq!(ids.len(), 4);
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        4
    );
    assert!(!ids.contains(&inserted_later.id()));
}

#[tokio::test]
async fn test_ensure_system_reports_existing_project() {
    let ctx = TestDb::new().await;
    let mut connection = ctx.raw_connection().await.expect("connect");
    let repo = PgProjectRepository;

    let outcome = repo
        .ensure_system(&mut connection)
        .await
        .expect("ensure System project");

    let EnsureSystemOutcome::Existing(project) = outcome else {
        panic!("migrated database already contains the System project");
    };
    assert_eq!(project.origin(), &ProjectOrigin::System);
}

#[tokio::test]
async fn test_ensure_system_converges_under_concurrent_callers() {
    let ctx = TestDb::new().await;
    let mut setup = ctx.raw_connection().await.expect("setup connection");
    truncate_all_tables(&mut setup).await;
    drop(setup);

    let mut first = ctx.raw_connection().await.expect("first connection");
    let mut second = ctx.raw_connection().await.expect("second connection");
    let (first, second) = tokio::join!(
        PgProjectRepository.ensure_system(&mut first),
        PgProjectRepository.ensure_system(&mut second),
    );
    let outcomes = [first.expect("first ensure"), second.expect("second ensure")];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, EnsureSystemOutcome::Inserted(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, EnsureSystemOutcome::Existing(_)))
            .count(),
        1
    );
    let mut verify = ctx.raw_connection().await.expect("verification connection");
    let projects = PgProjectRepository
        .list(&mut verify)
        .await
        .expect("list projects");
    assert_eq!(
        projects
            .iter()
            .filter(|project| matches!(project.origin(), ProjectOrigin::System))
            .count(),
        1
    );
}
