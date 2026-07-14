//! Process proof for the runtime-independent manager launch and repair path.

use std::{
    io::{BufRead as _, BufReader, Read as _, Write as _},
    os::unix::net::UnixStream,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use tribal_config::{TransportKind, TribalConfig};
use tribal_db::{
    MigrationHeadStatus, MigrationRepository, NewProject, PgProjectRepository, PrincipalRepository,
    ProjectRepository,
};
use tribal_domain::{GitRemote, LOCAL_PRINCIPAL_KEY};
use tribal_test_utils::duration::POLL_INTERVAL;
use tribal_wire::management::{
    ConfigDigest, ConfigDocument, ConfigFieldPath, ConfigLiteral, ConfigRevision, ConfigSetRequest,
    ConfigWriteOutcome, DatabaseInitialiseOutcome, DatabaseInitialiseRequest,
    DatabaseInitialiseResult, LifecycleSnapshot, MANAGEMENT_CONTRACT_VERSION,
    ManagementBootstrapRequest, ManagementBootstrapResponse, ManagementClientHello,
    ManagementError, ManagementResponseError, ManagerLaunchDisposition, ManagerLaunchRecord,
    PageCursor, PageRequest, PageSize, ProjectList, ProjectListRequest, ProjectRegisterInput,
    ProjectRegisterOutcome, ProjectRegisterRequest, ProjectRegistrationSource, RuntimeIdentity,
    RuntimeStartResult, TokenCreateRequest, TokenCreateResult,
};

/// Upper bound for manager replacement and child-process observations.
const PROCESS_TIMEOUT: Duration = Duration::from_secs(20);

#[test]
fn test_invalid_config_manager_repairs_without_restart() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let mut child = spawn_manager(&config_path, temp.path());
    let announcement = continuing_announcement(read_manager_record(&mut child));

    let mut reader = handshake(&announcement);

    let snapshot: LifecycleSnapshot = call(&mut reader, 1, "manager.snapshot", None);
    assert_eq!(phase(&snapshot), "unconfigured");
    let initial_revision = snapshot.header.revision;
    let document: ConfigDocument = call(&mut reader, 2, "config.getAll", None);
    let ConfigDocument::DurableInvalid { revision } = document else {
        panic!("invalid YAML must remain a durable-invalid document");
    };
    let request = ConfigSetRequest {
        key: ConfigFieldPath::parse("database.url").expect("field path is valid"),
        value: ConfigLiteral::new(serde_json::json!(
            "postgres://user:pass@localhost:5432/tribal"
        )),
        expected_revision: revision,
    };
    let outcome: ConfigWriteOutcome = call(
        &mut reader,
        3,
        "config.set",
        Some(&serde_json::to_value(request).expect("request serialises")),
    );
    assert!(!outcome.revision.as_str().is_empty());

    let mut request_id = 4;
    let repaired = poll_until(|| {
        let snapshot: LifecycleSnapshot = call(&mut reader, request_id, "manager.snapshot", None);
        request_id += 1;
        (snapshot.header.revision > initial_revision && configuration_is_valid(&snapshot))
            .then_some(())
    });
    assert!(
        repaired.is_some(),
        "repaired lifecycle did not adopt valid configuration evidence"
    );
    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut reader, 5, "manager.shutdown", None);
    wait_for_success(&mut child, "manager shutdown");
}

#[test]
fn test_process_authority_fences_same_path_and_keeps_distinct_paths_independent() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let mut manager = spawn_manager(&config_path, temp.path());
    let announcement = continuing_announcement(read_manager_record(&mut manager));

    let contender = run_to_completion(manager_command(&config_path, temp.path()));
    assert!(contender.status.success());
    let contender: ManagerLaunchRecord =
        serde_json::from_slice(&contender.stdout).expect("contender record parses");
    assert!(matches!(
        contender,
        ManagerLaunchRecord::Ready {
            announcement: ref observed,
            disposition: ManagerLaunchDisposition::ContenderExits,
        } if observed.instance_id == announcement.instance_id
    ));

    let direct_serve = run_to_completion({
        let mut command = tribal_command(&config_path, temp.path());
        command.arg("serve");
        command
    });
    assert!(
        !direct_serve.status.success(),
        "standalone serve cannot displace the manager"
    );

    let one_shot = run_to_completion({
        let mut command = tribal_command(&config_path, temp.path());
        command.arg("config").arg("path");
        command
    });
    assert!(one_shot.status.success());
    let observed_path: tribal_wire::management::ConfigFilePath =
        serde_json::from_slice(&one_shot.stdout).expect("config path output parses");
    assert_eq!(observed_path, announcement.config_path);

    let independent_path = temp.path().join("independent.yaml");
    std::fs::write(&independent_path, "database: [").expect("second invalid config writes");
    let mut independent = spawn_manager(&independent_path, temp.path());
    let independent_announcement = continuing_announcement(read_manager_record(&mut independent));
    assert_ne!(
        independent_announcement.instance_id,
        announcement.instance_id
    );
    assert_ne!(
        independent_announcement.socket_path,
        announcement.socket_path
    );

    let mut manager_reader = handshake(&announcement);
    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut manager_reader, 1, "manager.shutdown", None);
    wait_for_success(&mut manager, "manager shutdown");
    let mut independent_reader = handshake(&independent_announcement);
    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut independent_reader, 1, "manager.shutdown", None);
    wait_for_success(&mut independent, "independent manager shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_managed_runtime_survives_competing_and_successive_manager_recovery() {
    let database = tribal_test_utils::TestDb::new().await;
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    let mut config = TribalConfig::minimum_valid(database.database_url());
    config.server.transport = TransportKind::Http;
    config.server.bind_address = Some("127.0.0.1:0".to_owned());
    std::fs::write(
        &config_path,
        serde_yaml::to_string(&config).expect("config serialises"),
    )
    .expect("config writes");

    let mut original = spawn_manager(&config_path, temp.path());
    let original_announcement = continuing_announcement(read_manager_record(&mut original));
    let mut original_client = handshake(&original_announcement);
    wait_for_start_clear(&mut original_client);
    let started: RuntimeStartResult = call(&mut original_client, 1, "runtime.start", None);
    let RuntimeStartResult::Started { snapshot } = started else {
        panic!("managed runtime must start: {started:?}");
    };
    let runtime = runtime_from_snapshot(&LifecycleSnapshot::from(snapshot))
        .expect("started snapshot names its runtime")
        .clone();

    original.kill().expect("original manager is killed");
    let _ = wait_for_exit(&mut original, "original manager death");

    let mut first = spawn_manager(&config_path, temp.path());
    let mut second = spawn_manager(&config_path, temp.path());
    let first_record = read_manager_record(&mut first);
    let second_record = read_manager_record(&mut second);
    let first_announcement = continued_announcement(&first_record);
    let second_announcement = continued_announcement(&second_record);
    assert_ne!(
        first_announcement.is_some(),
        second_announcement.is_some(),
        "exactly one simultaneous successor must continue: {first_record:?}; {second_record:?}"
    );
    let (mut successor, successor_announcement, mut contender, contender_record) =
        match first_announcement {
            Some(announcement) => (first, announcement, second, second_record),
            None => (
                second,
                second_announcement.expect("second successor continues"),
                first,
                first_record,
            ),
        };
    let contender_status = wait_for_exit(&mut contender, "simultaneous recovery contender");
    match contender_record {
        ManagerLaunchRecord::Ready {
            announcement,
            disposition: ManagerLaunchDisposition::ContenderExits,
        } => {
            assert!(contender_status.success());
            assert_eq!(announcement.instance_id, successor_announcement.instance_id);
        }
        ManagerLaunchRecord::Failed { .. } => assert!(!contender_status.success()),
        ManagerLaunchRecord::Ready {
            disposition: ManagerLaunchDisposition::ManagerContinues,
            ..
        } => unreachable!("the contender cannot also continue"),
    }

    let mut successor_client = handshake(&successor_announcement);
    wait_for_runtime(&mut successor_client, &runtime);
    successor.kill().expect("first successor is killed");
    let _ = wait_for_exit(&mut successor, "first successor death");

    let mut second_successor = spawn_manager(&config_path, temp.path());
    let second_successor_announcement =
        continuing_announcement(read_manager_record(&mut second_successor));
    let mut second_successor_client = handshake(&second_successor_announcement);
    wait_for_runtime(&mut second_successor_client, &runtime);
    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut second_successor_client, 1, "manager.shutdown", None);
    wait_for_success(&mut second_successor, "recovered manager shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_database_initialise_negotiates_v3_and_migrates_once_after_revision_check() {
    let database = tribal_test_utils::TestDb::new_unmigrated().await;
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    let config = TribalConfig::minimum_valid(database.database_url());
    std::fs::write(
        &config_path,
        serde_yaml::to_string(&config).expect("config serialises"),
    )
    .expect("config writes");

    let mut manager = spawn_manager(&config_path, temp.path());
    let announcement = continuing_announcement(read_manager_record(&mut manager));

    let (_v2_reader, v2_response) = handshake_version(&announcement, 2);
    assert!(matches!(
        v2_response,
        ManagementBootstrapResponse::VersionMismatch { hello }
            if hello.protocol_version == MANAGEMENT_CONTRACT_VERSION
    ));

    let mut client = handshake(&announcement);
    let document: ConfigDocument = call(&mut client, 1, "config.getAll", None);
    let ConfigDocument::DurableValid { revision, .. } = document else {
        panic!("valid configuration must expose its durable revision");
    };
    let stale = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"stale"));
    let stale_request = DatabaseInitialiseRequest {
        expected_revision: stale.clone(),
    };
    let stale_error = call_error(
        &mut client,
        2,
        "database.initialise",
        Some(&serde_json::to_value(stale_request).expect("request serialises")),
    );
    assert!(matches!(
        stale_error.error,
        ManagementError::ConfigConflict { expected, actual }
            if expected == stale && actual == revision
    ));
    let mut connection = database.raw_connection().await.expect("database connects");
    assert!(
        !tribal_db::PgMigrationRepository
            .has_migrations_table(&mut connection)
            .await
            .expect("migration state reads"),
        "stale refusal must precede database effects"
    );
    drop(connection);

    let request = DatabaseInitialiseRequest {
        expected_revision: revision.clone(),
    };
    let first: DatabaseInitialiseResult = call(
        &mut client,
        3,
        "database.initialise",
        Some(&serde_json::to_value(&request).expect("request serialises")),
    );
    assert_eq!(first.config_revision, revision);
    assert_eq!(first.value, DatabaseInitialiseOutcome::Initialised);

    let mut connection = database
        .raw_connection()
        .await
        .expect("database reconnects");
    let expected_head = tribal_db::MIGRATOR
        .iter()
        .last()
        .expect("compiled migrations are non-empty")
        .version;
    assert_eq!(
        tribal_db::PgMigrationRepository
            .current_head_matches(&mut connection, expected_head)
            .await
            .expect("migration head reads"),
        MigrationHeadStatus::Matches
    );
    assert!(
        tribal_db::PgPrincipalRepository
            .find_by_key(&mut connection, LOCAL_PRINCIPAL_KEY)
            .await
            .expect("principal reads")
            .is_some()
    );
    drop(connection);

    let second: DatabaseInitialiseResult = call(
        &mut client,
        4,
        "database.initialise",
        Some(&serde_json::to_value(request).expect("request serialises")),
    );
    assert_eq!(second.config_revision, revision);
    assert_eq!(second.value, DatabaseInitialiseOutcome::AlreadyInitialised);

    let projected = run_to_completion({
        let mut command = tribal_command(&config_path, temp.path());
        command.arg("database").arg("initialise");
        command
    });
    assert!(projected.status.success(), "{projected:?}");
    let projected: DatabaseInitialiseResult =
        serde_json::from_slice(&projected.stdout).expect("database command result parses");
    assert_eq!(projected.config_revision, revision);
    assert_eq!(
        projected.value,
        DatabaseInitialiseOutcome::AlreadyInitialised
    );

    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut client, 5, "manager.shutdown", None);
    wait_for_success(&mut manager, "manager shutdown");
}

#[tokio::test]
async fn test_direct_runtime_credentials_follow_the_canonical_config_namespace() {
    let database = tribal_test_utils::TestDb::new().await;
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    let mut config = TribalConfig::minimum_valid(database.database_url());
    config.server.transport = TransportKind::Http;
    std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let mut manager = spawn_manager(&config_path, temp.path());
    let announcement = continuing_announcement(read_manager_record(&mut manager));
    let mut client = handshake(&announcement);
    let document: ConfigDocument = call(&mut client, 1, "config.getAll", None);
    let ConfigDocument::DurableValid { revision, .. } = document else {
        panic!("valid configuration must expose its durable revision");
    };
    let created: TokenCreateResult = call(
        &mut client,
        2,
        "token.create",
        Some(
            &serde_json::to_value(TokenCreateRequest {
                expected_revision: revision,
                principal: None,
                ttl_hours: Some(1),
                scopes: Vec::new(),
                persist_as_default: true,
            })
            .unwrap(),
        ),
    );
    let secret = created.value.token.expose_secret().to_owned();
    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut client, 3, "manager.shutdown", None);
    wait_for_success(&mut manager, "manager shutdown after credential issuance");

    let rendered = run_to_completion({
        let mut command = tribal_command(&config_path, temp.path());
        command
            .arg("mcp-config")
            .arg("--transport")
            .arg("http")
            .arg("--static-token");
        command
    });
    assert!(rendered.status.success(), "mcp-config failed: {rendered:?}");
    let document: serde_json::Value = serde_json::from_slice(&rendered.stdout).unwrap();
    assert_eq!(
        document["headers"]["Authorization"],
        format!("Bearer {secret}")
    );
    let checked = run_to_completion({
        let mut command = tribal_command(&config_path, temp.path());
        command.arg("check").arg("--json");
        command
    });
    let report: serde_json::Value = serde_json::from_slice(&checked.stdout).unwrap();
    let token_status = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "valid_token_exists")
        .and_then(|row| row["status"].as_str());
    assert_eq!(token_status, Some("pass"));
    assert!(
        !temp.path().join("tribal/credentials.json").exists(),
        "manager issuance must not create the global credential file"
    );

    let other_config_path = temp.path().join("other.yaml");
    std::fs::write(&other_config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let isolated = run_to_completion({
        let mut command = tribal_command(&other_config_path, temp.path());
        command
            .arg("mcp-config")
            .arg("--transport")
            .arg("http")
            .arg("--static-token");
        command
    });
    assert!(
        !isolated.status.success(),
        "a different canonical config must not consume the credential"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "one process journey keeps pagination evidence in causal order"
)]
async fn test_project_pagination_is_revision_bound_bounded_and_high_water_stable() {
    let database = tribal_test_utils::TestDb::new().await;
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    let config = TribalConfig::minimum_valid(database.database_url());
    std::fs::write(
        &config_path,
        serde_yaml::to_string(&config).expect("config serialises"),
    )
    .expect("config writes");

    let mut manager = spawn_manager(&config_path, temp.path());
    let announcement = continuing_announcement(read_manager_record(&mut manager));
    let mut client = handshake(&announcement);
    let document: ConfigDocument = call(&mut client, 1, "config.getAll", None);
    let ConfigDocument::DurableValid { revision, .. } = document else {
        panic!("valid configuration must expose its durable revision");
    };

    let stale = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"stale"));
    let stale_error = call_error(
        &mut client,
        2,
        "project.register",
        Some(&serde_json::to_value(project_request(&stale, "stale")).expect("request serialises")),
    );
    assert!(matches!(
        stale_error.error,
        ManagementError::ConfigConflict { expected, actual }
            if expected == stale && actual == revision
    ));
    let mut connection = database.raw_connection().await.expect("database connects");
    assert!(
        PgProjectRepository
            .list(&mut connection)
            .await
            .expect("projects list")
            .is_empty()
    );
    drop(connection);

    let mut seeded = Vec::new();
    for (id, suffix) in [(3, "one"), (4, "two"), (5, "three")] {
        let result: tribal_wire::management::ProjectRegisterResult = call(
            &mut client,
            id,
            "project.register",
            Some(
                &serde_json::to_value(project_request(&revision, suffix))
                    .expect("request serialises"),
            ),
        );
        assert_eq!(result.config_revision, revision);
        let ProjectRegisterOutcome::Registered { project } = result.value else {
            panic!("first registration must insert");
        };
        seeded.push(project.id);
    }
    let duplicate: tribal_wire::management::ProjectRegisterResult = call(
        &mut client,
        6,
        "project.register",
        Some(&serde_json::to_value(project_request(&revision, "one")).expect("request serialises")),
    );
    assert!(matches!(
        duplicate.value,
        ProjectRegisterOutcome::AlreadyRegistered { .. }
    ));

    let first: ProjectList = call(
        &mut client,
        7,
        "project.list",
        Some(
            &serde_json::to_value(ProjectListRequest {
                page: PageRequest {
                    size: PageSize::try_from(2).expect("page size is valid"),
                    after: None,
                },
            })
            .expect("request serialises"),
        ),
    );
    assert_eq!(first.config_revision, revision);
    assert_eq!(first.value.items.len(), 2);
    assert!(serde_json::to_vec(&first).expect("page serialises").len() < 64 * 1024);
    let first_cursor = first.value.next.clone().expect("first page continues");

    thread::sleep(Duration::from_millis(2));
    let later: tribal_wire::management::ProjectRegisterResult = call(
        &mut client,
        8,
        "project.register",
        Some(
            &serde_json::to_value(project_request(&revision, "later")).expect("request serialises"),
        ),
    );
    let later_id = match later.value {
        ProjectRegisterOutcome::Registered { project } => project.id,
        ProjectRegisterOutcome::AlreadyRegistered { .. } => unreachable!("remote is new"),
    };
    let second: ProjectList = call(
        &mut client,
        9,
        "project.list",
        Some(
            &serde_json::to_value(ProjectListRequest {
                page: PageRequest {
                    size: PageSize::try_from(2).expect("page size is valid"),
                    after: Some(first_cursor.clone()),
                },
            })
            .expect("request serialises"),
        ),
    );
    assert_eq!(second.value.items.len(), 1);
    assert!(second.value.next.is_none());
    let walked: Vec<_> = first
        .value
        .items
        .iter()
        .chain(&second.value.items)
        .map(|project| project.id)
        .collect();
    assert_eq!(walked.len(), 3);
    assert_eq!(
        walked
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );
    assert!(!walked.contains(&later_id));
    assert!(seeded.iter().all(|id| walked.contains(id)));

    let malformed = PageCursor::try_from("not-base64".to_owned()).expect("cursor is non-empty");
    let malformed_error = call_error(
        &mut client,
        10,
        "project.list",
        Some(
            &serde_json::to_value(ProjectListRequest {
                page: PageRequest {
                    size: PageSize::try_from(2).expect("page size is valid"),
                    after: Some(malformed),
                },
            })
            .expect("request serialises"),
        ),
    );
    assert!(matches!(
        malformed_error.error,
        ManagementError::ConfigurationInvalid { .. }
    ));

    let change: ConfigWriteOutcome = call(
        &mut client,
        11,
        "config.set",
        Some(
            &serde_json::to_value(ConfigSetRequest {
                key: ConfigFieldPath::parse("logging.level").expect("field path is valid"),
                value: ConfigLiteral::new(serde_json::json!("debug")),
                expected_revision: revision.clone(),
            })
            .expect("request serialises"),
        ),
    );
    let stale_cursor_error = call_error(
        &mut client,
        12,
        "project.list",
        Some(
            &serde_json::to_value(ProjectListRequest {
                page: PageRequest {
                    size: PageSize::try_from(2).expect("page size is valid"),
                    after: Some(first_cursor),
                },
            })
            .expect("request serialises"),
        ),
    );
    assert!(matches!(
        stale_cursor_error.error,
        ManagementError::ConfigConflict { expected, actual }
            if expected == revision && actual == change.revision
    ));

    let mut connection = database
        .raw_connection()
        .await
        .expect("database reconnects");
    PgProjectRepository
        .insert(
            &mut connection,
            &NewProject::builder()
                .git_remote(GitRemote::from_parts(
                    "github.com",
                    "cortex/oversized-project",
                    None,
                ))
                .name("x".repeat(70 * 1024))
                .default_branch("main".to_owned())
                .schema_version(1)
                .settings(serde_json::json!({}))
                .build(),
        )
        .await
        .expect("oversized project inserts");
    drop(connection);
    let oversized_error = call_error(
        &mut client,
        13,
        "project.list",
        Some(
            &serde_json::to_value(ProjectListRequest {
                page: PageRequest {
                    size: PageSize::try_from(1).expect("page size is valid"),
                    after: None,
                },
            })
            .expect("request serialises"),
        ),
    );
    assert!(matches!(
        oversized_error.error,
        ManagementError::Administration {
            failure: tribal_wire::management::AdministrationFailure::InventoryItemTooLarge {
                item: tribal_wire::management::InventoryItemRef::Project(_),
            }
        }
    ));

    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut client, 14, "manager.shutdown", None);
    wait_for_success(&mut manager, "manager shutdown");
}

fn project_request(revision: &ConfigRevision, suffix: &str) -> ProjectRegisterRequest {
    ProjectRegisterRequest {
        expected_revision: revision.clone(),
        project: ProjectRegisterInput {
            source: ProjectRegistrationSource::GitRemote {
                remote: GitRemote::from_parts("github.com", &format!("cortex/{suffix}"), None),
            },
            name: None,
            default_branch: None,
        },
    }
}

fn tribal_command(config_path: &std::path::Path, root: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tribal"));
    command
        .arg("--config")
        .arg(config_path)
        .env("HOME", root)
        .env("XDG_CONFIG_HOME", root)
        .env("XDG_RUNTIME_DIR", root)
        .env("XDG_STATE_HOME", root);
    command
}

fn manager_command(config_path: &std::path::Path, root: &std::path::Path) -> Command {
    let mut command = tribal_command(config_path, root);
    command.arg("manage").arg("--announce-json");
    command
}

fn spawn_manager(config_path: &std::path::Path, root: &std::path::Path) -> Child {
    manager_command(config_path, root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("manager spawns")
}

#[track_caller]
fn read_manager_record(child: &mut Child) -> ManagerLaunchRecord {
    let mut stdout = child.stdout.take().expect("manager stdout is piped");
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = stdout.read_to_end(&mut bytes).map(|_| bytes);
        let _ = sender.send(result);
    });
    let bytes = match receiver.recv_timeout(PROCESS_TIMEOUT) {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => panic!("manager launch record reads: {error}"),
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("manager launch record timed out: {error}");
        }
    };
    serde_json::from_slice(&bytes).expect("one bounded manager launch record parses")
}

fn continuing_announcement(
    record: ManagerLaunchRecord,
) -> tribal_wire::management::ManagerAnnouncement {
    let ManagerLaunchRecord::Ready {
        announcement,
        disposition: ManagerLaunchDisposition::ManagerContinues,
    } = record
    else {
        panic!("winning manager must announce that it continues: {record:?}");
    };
    announcement
}

fn continued_announcement(
    record: &ManagerLaunchRecord,
) -> Option<tribal_wire::management::ManagerAnnouncement> {
    match record {
        ManagerLaunchRecord::Ready {
            announcement,
            disposition: ManagerLaunchDisposition::ManagerContinues,
        } => Some(announcement.clone()),
        ManagerLaunchRecord::Ready {
            disposition: ManagerLaunchDisposition::ContenderExits,
            ..
        }
        | ManagerLaunchRecord::Failed { .. } => None,
    }
}

fn run_to_completion(mut command: Command) -> Output {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("command spawns");
    if poll_until(|| child.try_wait().expect("command status reads")).is_none() {
        let _ = child.kill();
        let output = child.wait_with_output().expect("timed-out output reads");
        panic!("command timed out: {output:?}");
    }
    child.wait_with_output().expect("command output reads")
}

fn wait_for_success(child: &mut Child, context: &str) {
    let status = wait_for_exit(child, context);
    assert!(status.success(), "{context} status was {status}");
}

fn wait_for_exit(child: &mut Child, context: &str) -> std::process::ExitStatus {
    let Some(status) = poll_until(|| child.try_wait().expect("manager status reads")) else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("{context} timed out");
    };
    status
}

fn poll_until<T>(mut observe: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        if let Some(value) = observe() {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn handshake(announcement: &tribal_wire::management::ManagerAnnouncement) -> BufReader<UnixStream> {
    let (reader, hello) = handshake_version(announcement, MANAGEMENT_CONTRACT_VERSION);
    assert!(matches!(
        hello,
        ManagementBootstrapResponse::Compatible { ref hello }
            if hello.manager_instance_id == announcement.instance_id
    ));
    reader
}

fn handshake_version(
    announcement: &tribal_wire::management::ManagerAnnouncement,
    protocol_version: u16,
) -> (BufReader<UnixStream>, ManagementBootstrapResponse) {
    let stream = connect_with_retry(&announcement.socket_path);
    let mut reader = BufReader::new(stream);
    write_frame(
        reader.get_mut(),
        &ManagementBootstrapRequest::Handshake {
            hello: ManagementClientHello { protocol_version },
        },
    );
    let hello: ManagementBootstrapResponse = read_frame(&mut reader);
    (reader, hello)
}

fn wait_for_start_clear(reader: &mut BufReader<UnixStream>) {
    let mut id = 10;
    let clear = poll_until(|| {
        let snapshot: LifecycleSnapshot = call(reader, id, "manager.snapshot", None);
        id += 1;
        if matches!(
            snapshot.phase,
            tribal_wire::management::LifecyclePhase::Stopped {
                state: tribal_wire::management::StoppedState::Ready { .. }
            }
        ) {
            return Some(());
        }
        None
    });
    assert!(clear.is_some(), "start readiness did not clear");
}

fn wait_for_runtime(reader: &mut BufReader<UnixStream>, expected: &RuntimeIdentity) {
    let mut id = 20;
    let recovered = poll_until(|| {
        let snapshot: LifecycleSnapshot = call(reader, id, "manager.snapshot", None);
        id += 1;
        if runtime_from_snapshot(&snapshot) == Some(expected) {
            return Some(());
        }
        None
    });
    assert!(
        recovered.is_some(),
        "recovered manager did not publish the expected runtime"
    );
}

fn runtime_from_snapshot(snapshot: &LifecycleSnapshot) -> Option<&RuntimeIdentity> {
    match &snapshot.phase {
        tribal_wire::management::LifecyclePhase::Healthy { runtime, .. }
        | tribal_wire::management::LifecyclePhase::Degraded { runtime, .. }
        | tribal_wire::management::LifecyclePhase::VersionMismatch { runtime, .. }
        | tribal_wire::management::LifecyclePhase::Stopping { runtime }
        | tribal_wire::management::LifecyclePhase::RuntimeUnresponsive { runtime, .. } => {
            Some(runtime)
        }
        tribal_wire::management::LifecyclePhase::Unconfigured { .. }
        | tribal_wire::management::LifecyclePhase::Stopped { .. }
        | tribal_wire::management::LifecyclePhase::Starting
        | tribal_wire::management::LifecyclePhase::CancellingEarlyChild { .. }
        | tribal_wire::management::LifecyclePhase::ManagerTerminating { .. } => None,
    }
}

fn connect_with_retry(path: &str) -> UnixStream {
    let mut last_error = None;
    let stream = poll_until(|| match UnixStream::connect(path) {
        Ok(stream) => Some(stream),
        Err(error) => {
            last_error = Some(error);
            None
        }
    });
    stream.unwrap_or_else(|| {
        panic!(
            "management socket did not accept: {}",
            last_error.expect("connection attempt records its failure")
        )
    })
}

fn call<T: serde::de::DeserializeOwned>(
    reader: &mut BufReader<UnixStream>,
    id: u64,
    method: &str,
    params: Option<&serde_json::Value>,
) -> T {
    write_frame(
        reader.get_mut(),
        &serde_json::json!({"id": id, "method": method, "params": params}),
    );
    loop {
        let response: serde_json::Value = read_frame(reader);
        if response.get("event").is_some() {
            continue;
        }
        assert_eq!(response["id"], id);
        return serde_json::from_value(response["result"].clone()).expect("typed result decodes");
    }
}

fn call_error(
    reader: &mut BufReader<UnixStream>,
    id: u64,
    method: &str,
    params: Option<&serde_json::Value>,
) -> ManagementResponseError {
    write_frame(
        reader.get_mut(),
        &serde_json::json!({"id": id, "method": method, "params": params}),
    );
    loop {
        let response: serde_json::Value = read_frame(reader);
        if response.get("event").is_some() {
            continue;
        }
        assert_eq!(response["id"], id);
        return serde_json::from_value(response["error"].clone()).expect("typed error decodes");
    }
}

fn write_frame(stream: &mut UnixStream, value: &impl serde::Serialize) {
    let mut bytes = serde_json::to_vec(value).expect("frame serialises");
    bytes.push(b'\n');
    stream.write_all(&bytes).expect("frame writes");
}

fn read_frame<T: serde::de::DeserializeOwned>(reader: &mut BufReader<UnixStream>) -> T {
    let mut line = String::new();
    reader.read_line(&mut line).expect("frame reads");
    serde_json::from_str(&line).expect("frame parses")
}

fn phase(snapshot: &LifecycleSnapshot) -> &'static str {
    match snapshot.phase {
        tribal_wire::management::LifecyclePhase::Unconfigured { .. } => "unconfigured",
        tribal_wire::management::LifecyclePhase::Stopped { .. } => "stopped",
        _ => "runtime",
    }
}

fn configuration_is_valid(snapshot: &LifecycleSnapshot) -> bool {
    match &snapshot.phase {
        tribal_wire::management::LifecyclePhase::Stopped { .. } => true,
        tribal_wire::management::LifecyclePhase::Unconfigured { readiness, .. } => [
            tribal_wire::management::CheckName::ConfigParse,
            tribal_wire::management::CheckName::ConfigValidate,
        ]
        .into_iter()
        .all(|expected| {
            readiness.checks.iter().any(|observation| {
                matches!(
                    &observation.result,
                    tribal_wire::management::CheckResult::Pass { name, .. }
                        if *name == expected
                )
            })
        }),
        _ => false,
    }
}
