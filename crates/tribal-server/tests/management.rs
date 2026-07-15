//! Process proof for the runtime-independent manager launch and repair path.

use std::{
    fs::OpenOptions,
    io::{BufRead as _, BufReader, Read as _, Write as _},
    os::unix::{fs::PermissionsExt as _, net::UnixStream},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use fs2::FileExt as _;
use tribal::{ManagementClientError, ManagerConnector, ManagerConnectorError};
use tribal_config::TribalConfig;
use tribal_db::{
    MigrationHeadStatus, MigrationRepository, NewProject, PgProjectRepository, PrincipalRepository,
    ProjectRepository,
};
use tribal_domain::{GitRemote, LOCAL_PRINCIPAL_KEY, TransportKind};
use tribal_test_utils::duration::POLL_INTERVAL;
use tribal_wire::management::{
    ConfigDigest, ConfigDocument, ConfigFieldPath, ConfigLiteral, ConfigRevision, ConfigSetRequest,
    ConfigWriteOutcome, DatabaseInitialiseOutcome, DatabaseInitialiseRequest,
    DatabaseInitialiseResult, LifecycleSnapshot, MANAGEMENT_CONTRACT_VERSION,
    ManagementBootstrapRequest, ManagementBootstrapResponse, ManagementClientHello,
    ManagementError, ManagementResponseError, ManagementServerHello, ManagerAnnouncement,
    ManagerLaunchDisposition, ManagerLaunchFailure, ManagerLaunchRecord, ManagerShutdownCall,
    PageCursor, PageRequest, PageSize, ProjectList, ProjectListRequest, ProjectRegisterInput,
    ProjectRegisterOutcome, ProjectRegisterRequest, ProjectRegistrationSource, RuntimeIdentity,
    RuntimeStartResult, TokenCreateRequest, TokenCreateResult,
};

/// Upper bound for manager replacement and child-process observations.
const PROCESS_TIMEOUT: Duration = Duration::from_secs(20);

#[test]
fn manager_shutdown_projection() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let mut manager = spawn_manager(&config_path, temp.path());
    let announcement = continuing_announcement(read_manager_record(&mut manager));

    let output = run_to_completion({
        let mut command = tribal_command(&config_path, temp.path());
        command.arg("manager").arg("shutdown");
        command
    });
    assert!(
        output.status.success(),
        "shutdown projection failed: {output:?}"
    );
    serde_json::from_slice::<tribal_wire::management::ManagerShutdownResult>(&output.stdout)
        .expect("shutdown result remains typed");
    wait_for_success(&mut manager, "manager shutdown projection");
    assert!(!Path::new(&announcement.socket_path).exists());
}

#[test]
fn config_watcher_lag_rereads_snapshot() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    let mut config = TribalConfig::minimum_valid("postgres://localhost/tribal");
    config.logging.level = "info".to_owned();
    std::fs::write(
        &config_path,
        serde_yaml::to_string(&config).expect("config serialises"),
    )
    .expect("config writes");
    let mut manager = spawn_manager(&config_path, temp.path());
    let announcement = continuing_announcement(read_manager_record(&mut manager));
    let mut lagged = handshake(&announcement);
    let mut writer = handshake(&announcement);

    let initial: ConfigDocument = call(&mut writer, 1, "config.getAll", None);
    let mut revision = config_revision(&initial);
    for sequence in 0..2_048_u64 {
        let level = if sequence % 2 == 0 { "debug" } else { "info" };
        let request = ConfigSetRequest {
            key: ConfigFieldPath::parse("logging.level").expect("config path parses"),
            value: ConfigLiteral::new(serde_json::json!(level)),
            expected_revision: revision,
        };
        let outcome: ConfigWriteOutcome = call(
            &mut writer,
            sequence + 2,
            "config.set",
            Some(&serde_json::to_value(request).expect("request serialises")),
        );
        revision = outcome.revision;
    }

    config.logging.level = "trace".to_owned();
    let edited = serde_yaml::to_string(&config).expect("edited config serialises");
    let edited_revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(edited.as_bytes()));
    std::fs::write(&config_path, edited).expect("raw config edit writes");

    lagged
        .get_ref()
        .set_nonblocking(true)
        .expect("lagged subscriber polling configures");
    drain_until_disconnect(&mut lagged);
    drop(lagged);
    drop(writer);

    let mut reconnected = handshake(&announcement);
    let snapshot = poll_until(|| {
        let document: ConfigDocument = call(&mut reconnected, 10_000, "config.getAll", None);
        (config_revision(&document) == edited_revision).then_some(document)
    })
    .expect("reconnected client observes the raw edit");
    assert_eq!(config_logging_level(&snapshot), Some("trace"));

    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut reconnected, 10_001, "manager.shutdown", None);
    drop(reconnected);
    wait_for_success(&mut manager, "config lag shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_serve_project_mode_process_matrix() {
    let database = tribal_test_utils::TestDb::new().await;
    let mut connection = database.raw_connection().await.expect("database connects");
    let ambient = PgProjectRepository
        .insert(&mut connection, &new_project("ambient", "cortex/ambient"))
        .await
        .expect("ambient project inserts");
    let repository_project = PgProjectRepository
        .insert(
            &mut connection,
            &new_project("repository", "cortex/serve-mode"),
        )
        .await
        .expect("repository project inserts");
    drop(connection);

    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary process root");
    let repository = temp.path().join("repository");
    initialise_git_repository(&repository);

    for transport in [TransportKind::Http, TransportKind::Sse] {
        let root = temp.path().join(format!("managed-{transport}"));
        std::fs::create_dir_all(&root).expect("managed root creates");
        let config_path = root.join("tribal.yaml");
        write_server_config(&config_path, database.database_url(), transport);
        let mut manager = manager_command(&config_path, &root)
            .env("TRIBAL_PROJECT_ID", ambient.id().to_string())
            .current_dir(&repository)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("manager spawns");
        let announcement = continuing_announcement(read_manager_record(&mut manager));
        let mut client = handshake(&announcement);
        wait_for_start_clear(&mut client);
        let started: RuntimeStartResult = call(&mut client, 1, "runtime.start", None);
        assert!(
            matches!(started, RuntimeStartResult::Started { .. }),
            "managed {transport} runtime starts: {started:?}"
        );
        assert!(manager.try_wait().expect("manager status reads").is_none());
        let _: tribal_wire::management::ManagerShutdownResult =
            call(&mut client, 2, "manager.shutdown", None);
        wait_for_success(&mut manager, "managed project-mode shutdown");
    }

    for (name, arguments, ambient_project, expected) in [
        (
            "auto",
            Vec::new(),
            None,
            Some(repository_project.id().to_string()),
        ),
        (
            "unscoped",
            vec!["--unscoped".to_owned()],
            Some(ambient.id().to_string()),
            None,
        ),
        (
            "project",
            vec!["--project".to_owned(), ambient.id().to_string()],
            Some(repository_project.id().to_string()),
            Some(ambient.id().to_string()),
        ),
    ] {
        let root = temp.path().join(format!("direct-{name}"));
        std::fs::create_dir_all(&root).expect("direct root creates");
        let config_path = root.join("tribal.yaml");
        write_server_config(&config_path, database.database_url(), TransportKind::Http);
        let mut command = tribal_command(&config_path, &root);
        command
            .arg("serve")
            .args(arguments)
            .current_dir(&repository)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        match ambient_project {
            Some(project) => {
                command.env("TRIBAL_PROJECT_ID", project);
            }
            None => {
                command.env_remove("TRIBAL_PROJECT_ID");
            }
        }
        let mut server = command.spawn().expect("direct server spawns");
        let _ = expected;
        poll_until(|| server.try_wait().ok().filter(Option::is_none))
            .unwrap_or_else(|| panic!("direct {name} remains live"));
        server.kill().expect("direct server stops");
        let _ = server.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connector_concurrent_first_launch() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let first = connector(&config_path, temp.path()).connect();
    let second = connector(&config_path, temp.path()).connect();
    let (first, second) = tokio::join!(first, second);
    let mut first = first.expect("first connector attaches");
    let mut second = second.expect("second connector attaches");

    assert_eq!(
        first.announcement().instance_id,
        second.announcement().instance_id
    );
    assert!(matches!(
        (first.disposition(), second.disposition()),
        (
            ManagerLaunchDisposition::ManagerContinues,
            ManagerLaunchDisposition::ContenderExits
        ) | (
            ManagerLaunchDisposition::ContenderExits,
            ManagerLaunchDisposition::ManagerContinues
        )
    ));
    let first_snapshot: LifecycleSnapshot = first
        .client_mut()
        .call::<tribal_wire::management::ManagerSnapshotCall>(&())
        .await
        .expect("first connector calls manager")
        .lifecycle;
    let second_snapshot: LifecycleSnapshot = second
        .client_mut()
        .call::<tribal_wire::management::ManagerSnapshotCall>(&())
        .await
        .expect("second connector calls manager")
        .lifecycle;
    assert_eq!(
        first_snapshot.header.manager_instance_id,
        second_snapshot.header.manager_instance_id
    );
    let descriptors = authority_descriptors(temp.path());
    assert_eq!(descriptors.len(), 1, "one authority announcement is live");
    assert_eq!(descriptors[0]["kind"], "manager");

    first
        .client_mut()
        .call::<ManagerShutdownCall>(&())
        .await
        .expect("manager shutdown succeeds");
    assert!(
        poll_until(|| (!Path::new(&first.announcement().socket_path).exists()).then_some(()))
            .is_some(),
        "manager socket must disappear after shutdown"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn connector_standalone_runtime_conflict() {
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
    let mut runtime = tribal_command(&config_path, temp.path())
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("standalone runtime spawns");
    assert!(
        poll_until(|| {
            authority_descriptors(temp.path())
                .iter()
                .any(|descriptor| descriptor["kind"] == "standalone_runtime")
                .then_some(())
        })
        .is_some(),
        "standalone runtime must publish authority"
    );

    let Err(error) = connector(&config_path, temp.path()).connect().await else {
        panic!("connector must refuse standalone runtime authority");
    };
    assert!(matches!(
        error,
        ManagerConnectorError::LaunchRefused {
            failure: ManagerLaunchFailure::DirectRuntimeConflict { .. }
        }
    ));
    runtime.kill().expect("standalone runtime stops");
    let _ = runtime.wait();
}

#[tokio::test(flavor = "multi_thread")]
async fn connector_recovering_authority_conflict() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let lock_path = config_path.with_file_name(".tribal.yaml.authority.lock");
    let ready_path = temp.path().join("lock-ready");
    let mut holder = Command::new(std::env::current_exe().expect("test executable resolves"))
        .arg("--ignored")
        .arg("--exact")
        .arg("connector_recovering_lock_holder")
        .env("CONNECTOR_LOCK_PATH", &lock_path)
        .env("CONNECTOR_READY_PATH", &ready_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("lock holder spawns");
    assert!(
        poll_until(|| ready_path.exists().then_some(())).is_some(),
        "lock holder must acquire authority"
    );

    let Err(error) = connector(&config_path, temp.path()).connect().await else {
        panic!("connector must refuse recovering authority");
    };
    assert!(matches!(
        error,
        ManagerConnectorError::LaunchRefused {
            failure: ManagerLaunchFailure::AuthorityRecovering { .. }
        }
    ));
    holder.kill().expect("lock holder stops");
    let _ = holder.wait();
}

#[test]
#[ignore = "process helper entered only by connector_recovering_authority_conflict"]
fn connector_recovering_lock_holder() {
    let lock_path = PathBuf::from(std::env::var_os("CONNECTOR_LOCK_PATH").expect("lock path set"));
    let ready_path =
        PathBuf::from(std::env::var_os("CONNECTOR_READY_PATH").expect("ready path set"));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .expect("authority lock opens");
    lock.lock_exclusive().expect("authority lock acquires");
    std::fs::write(ready_path, b"ready").expect("ready marker writes");
    loop {
        thread::sleep(Duration::from_mins(1));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connector_incompatible_manager_refusal() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let socket_path = temp.path().join("incompatible.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("fake manager binds");
    let announcement = fake_announcement(&config_path, &socket_path, "incompatible");
    let launcher = write_fake_launcher(temp.path(), &announcement, true);
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

        let (stream, _) = listener.accept().await.expect("fake manager accepts");
        let mut reader = tokio::io::BufReader::new(stream);
        let mut request = Vec::new();
        reader
            .read_until(b'\n', &mut request)
            .await
            .expect("handshake reads");
        let mut stream = reader.into_inner();
        let response = ManagementBootstrapResponse::VersionMismatch {
            hello: ManagementServerHello {
                protocol_version: MANAGEMENT_CONTRACT_VERSION + 1,
                binary_version: "incompatible".to_owned(),
                manager_instance_id: "incompatible".to_owned(),
            },
        };
        let mut bytes = serde_json::to_vec(&response).expect("response serialises");
        bytes.push(b'\n');
        stream.write_all(&bytes).await.expect("response writes");
    });

    let Err(error) = ManagerConnector::with_executable(launcher, &config_path)
        .connect()
        .await
    else {
        panic!("connector must refuse incompatible manager");
    };
    assert!(matches!(
        error,
        ManagerConnectorError::Attach {
            source: ManagementClientError::VersionMismatch
        }
    ));
    server.await.expect("fake manager task joins");
}

#[tokio::test(flavor = "multi_thread")]
async fn connector_manager_disappears_before_attach() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let socket_path = temp.path().join("absent.sock");
    let announcement = fake_announcement(&config_path, &socket_path, "vanished");
    let launcher = write_fake_launcher(temp.path(), &announcement, false);

    let Err(error) = ManagerConnector::with_executable(launcher, &config_path)
        .connect()
        .await
    else {
        panic!("connector must reject vanished manager");
    };
    assert!(matches!(error, ManagerConnectorError::ManagerDisappeared));
}

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
        command.arg("config").arg("path").arg("--json");
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
        command.arg("database").arg("initialise").arg("--json");
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
            .arg("integration")
            .arg("mcp-config")
            .arg("--transport")
            .arg("http")
            .arg("--auth")
            .arg("persisted-bearer")
            .arg("--json");
        command
    });
    assert!(rendered.status.success(), "mcp-config failed: {rendered:?}");
    let document: serde_json::Value = serde_json::from_slice(&rendered.stdout).unwrap();
    assert_eq!(
        document["value"]["data"]["document"]["mcpServers"]["tribal"]["headers"]["Authorization"],
        format!("Bearer {secret}"),
        "unexpected integration receipt: {document}",
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
        .find(|row| row["result"]["name"] == "valid_token_exists")
        .and_then(|row| row["result"]["status"].as_str());
    assert_eq!(
        token_status,
        Some("pass"),
        "unexpected check report: {report}"
    );
    assert!(
        !temp.path().join("tribal/credentials.json").exists(),
        "manager issuance must not create the global credential file"
    );

    let other_config_path = temp.path().join("other.yaml");
    std::fs::write(&other_config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
    let isolated = run_to_completion({
        let mut command = tribal_command(&other_config_path, temp.path());
        command
            .arg("integration")
            .arg("mcp-config")
            .arg("--transport")
            .arg("http")
            .arg("--auth")
            .arg("persisted-bearer")
            .arg("--json");
        command
    });
    assert!(
        !isolated.status.success(),
        "a different canonical config must not consume the credential"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
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
        .env("XDG_STATE_HOME", root)
        .env("TMPDIR", root);
    command
}

fn manager_command(config_path: &std::path::Path, root: &std::path::Path) -> Command {
    let mut command = tribal_command(config_path, root);
    command.arg("manager").arg("run").arg("--announce-json");
    command
}

fn new_project(name: &str, path: &str) -> NewProject {
    NewProject::builder()
        .git_remote(GitRemote::from_parts("github.com", path, None))
        .name(name.to_owned())
        .default_branch("main".to_owned())
        .schema_version(1)
        .settings(serde_json::json!({}))
        .build()
}

fn initialise_git_repository(path: &Path) {
    std::fs::create_dir_all(path).expect("repository directory creates");
    assert!(
        Command::new("git")
            .arg("init")
            .arg("--quiet")
            .arg(path)
            .status()
            .expect("git init runs")
            .success()
    );
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(path)
            .arg("remote")
            .arg("add")
            .arg("origin")
            .arg("https://github.com/cortex/serve-mode.git")
            .status()
            .expect("git remote runs")
            .success()
    );
}

fn write_server_config(path: &Path, database_url: &str, transport: TransportKind) {
    let mut config = TribalConfig::minimum_valid(database_url);
    config.server.transport = transport;
    config.server.bind_address = Some("127.0.0.1:0".to_owned());
    std::fs::write(
        path,
        serde_yaml::to_string(&config).expect("config serialises"),
    )
    .expect("config writes");
}

fn connector(config_path: &Path, root: &Path) -> ManagerConnector {
    [
        "HOME",
        "XDG_CONFIG_HOME",
        "XDG_RUNTIME_DIR",
        "XDG_STATE_HOME",
    ]
    .into_iter()
    .fold(
        ManagerConnector::with_executable(env!("CARGO_BIN_EXE_tribal"), config_path),
        |connector, key| connector.environment(key, root),
    )
}

fn authority_descriptors(root: &Path) -> Vec<serde_json::Value> {
    let management = root.join("tribal/management");
    let Ok(namespaces) = std::fs::read_dir(management) else {
        return Vec::new();
    };
    namespaces
        .filter_map(Result::ok)
        .filter_map(|namespace| std::fs::read(namespace.path().join("authority.json")).ok())
        .filter_map(|bytes| serde_json::from_slice(&bytes).ok())
        .collect()
}

fn fake_announcement(
    config_path: &Path,
    socket_path: &Path,
    instance_id: &str,
) -> ManagerAnnouncement {
    ManagerAnnouncement {
        instance_id: instance_id.to_owned(),
        socket_path: socket_path.to_string_lossy().into_owned(),
        protocol_version: MANAGEMENT_CONTRACT_VERSION,
        binary_version: env!("CARGO_PKG_VERSION").to_owned(),
        config_path: tribal_wire::management::ConfigFilePath {
            path: config_path.to_string_lossy().into_owned(),
        },
        pid: std::process::id(),
    }
}

fn write_fake_launcher(
    root: &Path,
    announcement: &ManagerAnnouncement,
    remain_alive: bool,
) -> PathBuf {
    let path = root.join(if remain_alive {
        "incompatible-manager"
    } else {
        "vanished-manager"
    });
    let record = ManagerLaunchRecord::Ready {
        announcement: announcement.clone(),
        disposition: ManagerLaunchDisposition::ManagerContinues,
    };
    let encoded = serde_json::to_string(&record)
        .expect("launch record serialises")
        .replace('\'', "'\\''");
    let tail = if remain_alive {
        "sleep 2\n"
    } else {
        "exit 0\n"
    };
    std::fs::write(
        &path,
        format!("#!/bin/sh\nprintf '%s' '{encoded}'\nexec 1>&-\n{tail}"),
    )
    .expect("fake launcher writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
        .expect("fake launcher is executable");
    path
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

fn drain_until_disconnect(reader: &mut BufReader<UnixStream>) {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        let mut frame = Vec::new();
        match reader.read_until(b'\n', &mut frame) {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "lagged subscriber did not disconnect before the process deadline"
                );
                thread::sleep(POLL_INTERVAL);
            }
            Err(error) => panic!("lagged subscriber did not disconnect: {error}"),
        }
    }
}

fn config_revision(document: &ConfigDocument) -> ConfigRevision {
    match document {
        ConfigDocument::DurableValid { revision, .. }
        | ConfigDocument::DurableInvalid { revision } => revision.clone(),
        ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => {
            panic!("test requires a stable configuration revision: {document:?}")
        }
    }
}

fn config_logging_level(document: &ConfigDocument) -> Option<&str> {
    match document {
        ConfigDocument::DurableValid { values, .. } => values
            .expose_sensitive()
            .pointer("/logging/level")
            .and_then(serde_json::Value::as_str),
        ConfigDocument::DurableInvalid { .. }
        | ConfigDocument::UncertainValid { .. }
        | ConfigDocument::UncertainInvalid { .. }
        | ConfigDocument::Unreadable { .. } => None,
    }
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
