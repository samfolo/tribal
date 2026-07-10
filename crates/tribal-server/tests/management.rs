//! Process proof for the runtime-independent manager launch and repair path.

use std::{
    io::{BufRead as _, BufReader, Read as _, Write as _},
    os::unix::net::UnixStream,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tribal_wire::management::{
    ConfigDocument, ConfigFieldPath, ConfigLiteral, ConfigSetRequest, ConfigWriteOutcome,
    LifecycleSnapshot, MANAGEMENT_CONTRACT_VERSION, ManagementBootstrapRequest,
    ManagementBootstrapResponse, ManagementClientHello, ManagerLaunchDisposition,
    ManagerLaunchRecord,
};

const PROCESS_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn invalid_config_manager_announces_repairs_and_shuts_down() {
    let temp = tempfile::Builder::new()
        .prefix("tm")
        .tempdir_in("/tmp")
        .expect("temporary manager root");
    let config_path = temp.path().join("tribal.yaml");
    std::fs::write(&config_path, "database: [").expect("invalid config writes");
    let mut child = Command::new(env!("CARGO_BIN_EXE_tribal"))
        .arg("manage")
        .arg("--announce-json")
        .arg("--config")
        .arg(&config_path)
        .env("HOME", temp.path())
        .env("XDG_RUNTIME_DIR", temp.path())
        .env("XDG_STATE_HOME", temp.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("manager spawns");
    let stdout = child.stdout.take().expect("manager stdout is piped");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).expect("launch record reads");
    let record: ManagerLaunchRecord = serde_json::from_str(&line).expect("launch record parses");
    let ManagerLaunchRecord::Ready {
        announcement,
        disposition: ManagerLaunchDisposition::ManagerContinues,
    } = record
    else {
        panic!("winning manager must announce that it continues: {record:?}");
    };
    let mut trailing = Vec::new();
    reader
        .read_to_end(&mut trailing)
        .expect("announcement channel reaches EOF");
    assert!(trailing.is_empty());

    let stream = connect_with_retry(&announcement.socket_path);
    let mut reader = BufReader::new(stream);
    write_frame(
        reader.get_mut(),
        &ManagementBootstrapRequest::Handshake {
            hello: ManagementClientHello {
                protocol_version: MANAGEMENT_CONTRACT_VERSION,
            },
        },
    );
    let hello: ManagementBootstrapResponse = read_frame(&mut reader);
    assert!(matches!(
        hello,
        ManagementBootstrapResponse::Compatible { ref hello }
            if hello.manager_instance_id == announcement.instance_id
    ));

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

    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        let snapshot: LifecycleSnapshot = call(&mut reader, 4, "manager.snapshot", None);
        if snapshot.header.revision > initial_revision && configuration_is_valid(&snapshot) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "repaired lifecycle did not adopt valid configuration evidence"
        );
        thread::sleep(Duration::from_millis(25));
    }
    let _: tribal_wire::management::ManagerShutdownResult =
        call(&mut reader, 5, "manager.shutdown", None);

    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().expect("manager status reads") {
            assert!(status.success(), "manager shutdown status was {status}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "manager did not exit after shutdown"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn connect_with_retry(path: &str) -> UnixStream {
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    loop {
        match UnixStream::connect(path) {
            Ok(stream) => return stream,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => panic!("management socket did not accept: {error}"),
        }
    }
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
