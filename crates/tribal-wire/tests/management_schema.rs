//! Golden JSON Schema for the public local-management contract.
#![cfg(feature = "schema")]

use std::{collections::BTreeMap, fs, path::Path};

use schemars::{schema::RootSchema, schema_for};
use tribal_wire::management as m;

const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";
const GOLDEN: &str = "tests/golden/management_contract.json";

#[test]
fn management_contract_matches_its_golden_schema() {
    let schemas: BTreeMap<&str, RootSchema> = BTreeMap::from([
        ("ConfigFieldPath", schema_for!(m::ConfigFieldPath)),
        ("LifecycleSnapshot", schema_for!(m::LifecycleSnapshot)),
        ("ManagementEvent", schema_for!(m::ManagementEvent)),
        ("RuntimeStartResult", schema_for!(m::RuntimeStartResult)),
        ("RuntimeStopResult", schema_for!(m::RuntimeStopResult)),
        ("RuntimeRestartResult", schema_for!(m::RuntimeRestartResult)),
        (
            "ManagerShutdownResult",
            schema_for!(m::ManagerShutdownResult),
        ),
        ("ManagerLaunchRecord", schema_for!(m::ManagerLaunchRecord)),
        (
            "ManagementBootstrapRequest",
            schema_for!(m::ManagementBootstrapRequest),
        ),
        (
            "ManagementBootstrapResponse",
            schema_for!(m::ManagementBootstrapResponse),
        ),
        ("ConfigDocument", schema_for!(m::ConfigDocument)),
        ("ConfigValue", schema_for!(m::ConfigValue)),
        ("ConfigSetRequest", schema_for!(m::ConfigSetRequest)),
        ("ConfigPatchRequest", schema_for!(m::ConfigPatchRequest)),
        ("ConfigPatchOutcome", schema_for!(m::ConfigPatchOutcome)),
        ("ModelsCatalogue", schema_for!(m::ModelsCatalogue)),
        (
            "ModelSelectionRequest",
            schema_for!(m::ModelSelectionRequest),
        ),
        (
            "CredentialSourcesRequest",
            schema_for!(m::CredentialSourcesRequest),
        ),
        ("CredentialSources", schema_for!(m::CredentialSources)),
        ("GenesisOptions", schema_for!(m::GenesisOptions)),
        (
            "GenesisConfigurationRequest",
            schema_for!(m::GenesisConfigurationRequest),
        ),
        (
            "GenesisConvergenceRequest",
            schema_for!(m::GenesisConvergenceRequest),
        ),
        (
            "GraphEmbeddingProfile",
            schema_for!(m::GraphEmbeddingProfile),
        ),
        (
            "ManagedRuntimeStatusResult",
            schema_for!(m::ManagedRuntimeStatusResult),
        ),
        (
            "RuntimeLogsTailRequest",
            schema_for!(m::RuntimeLogsTailRequest),
        ),
        (
            "RuntimeLogsTailResult",
            schema_for!(m::RuntimeLogsTailResult),
        ),
        (
            "RuntimeTokenListResult",
            schema_for!(m::RuntimeTokenListResult),
        ),
        ("TokenList", schema_for!(m::TokenList)),
        ("ConfigChangeEvent", schema_for!(m::ConfigChangeEvent)),
        (
            "ManagementResponseError",
            schema_for!(m::ManagementResponseError),
        ),
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
