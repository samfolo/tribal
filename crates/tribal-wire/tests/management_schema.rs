//! Golden JSON Schema for the public local-management contract.
#![cfg(feature = "schema")]

mod schema_support;

use std::{fs, path::Path};

use schema_support::legacy_control_schemas;
use schemars::schema_for;
use tribal_wire::management::{
    ConfigFieldPath, HealthDegradedReadinessReport, LifecycleSnapshot, ManagerShutdownResult,
    ReadinessReport, RuntimeRestartResult, RuntimeStartResult, RuntimeStopResult,
    StartBlockedReadinessReport, StartClearReadinessReport,
};

const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";
const GOLDEN: &str = "tests/golden/management_contract.json";

#[test]
fn management_contract_matches_its_golden_schema() {
    let mut schemas = legacy_control_schemas();
    schemas.extend([
        ("ConfigFieldPath", schema_for!(ConfigFieldPath)),
        ("ReadinessReport", schema_for!(ReadinessReport)),
        (
            "StartBlockedReadinessReport",
            schema_for!(StartBlockedReadinessReport),
        ),
        (
            "StartClearReadinessReport",
            schema_for!(StartClearReadinessReport),
        ),
        (
            "HealthDegradedReadinessReport",
            schema_for!(HealthDegradedReadinessReport),
        ),
        ("LifecycleSnapshot", schema_for!(LifecycleSnapshot)),
        ("RuntimeStartResult", schema_for!(RuntimeStartResult)),
        ("RuntimeStopResult", schema_for!(RuntimeStopResult)),
        ("RuntimeRestartResult", schema_for!(RuntimeRestartResult)),
        ("ManagerShutdownResult", schema_for!(ManagerShutdownResult)),
    ]);

    let generated =
        serde_json::to_string_pretty(&schemas).expect("management schemas serialise to JSON");
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN);

    if std::env::var(UPDATE_ENV_VAR).is_ok() {
        fs::create_dir_all(path.parent().expect("golden path has a parent"))
            .expect("create the golden directory");
        fs::write(&path, &generated).expect("write the golden schema");
        return;
    }

    let committed = fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "golden schema missing at {}; run with {UPDATE_ENV_VAR}=1 to create it",
            path.display(),
        )
    });
    assert_eq!(
        committed,
        generated,
        "management schema drifted from {}; run with {UPDATE_ENV_VAR}=1 to update it",
        path.display(),
    );
}
