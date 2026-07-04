//! Golden JSON-Schema test for the control-bridge wire contract.
//!
//! Every top-level control crossing is schema-generated and diffed against a
//! committed golden file. A schema-affecting change to any DTO regenerates the
//! schema, so an unregenerated golden fails here — and in CI, which runs this
//! test with the `schema` feature on. This golden is the single source the
//! desktop client's DTOs generate from. Regenerate after an intended change with
//! `UPDATE_SNAPSHOTS=1 cargo test -p tribal-wire --features schema`.
#![cfg(feature = "schema")]

use std::{collections::BTreeMap, fs, path::Path};

use schemars::{schema::RootSchema, schema_for};
use tribal_wire::control::{
    ClientHello, ConfigDocument, ConfigGetRequest, ConfigPath, ConfigSchema, ConfigSetRequest,
    ConfigValidateRequest, ConfigValidation, ConfigValue, ConfigWriteOutcome, ControlEvent,
    ControlNotification, ControlRequest, ControlResponse, LogLines, LogsTailRequest,
    RestartOutcome, ServerHello, ServerStatus, StopOutcome, TokenList,
};

/// The environment variable that regenerates the golden file instead of
/// comparing against it.
const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";

/// The committed golden schema, relative to the crate manifest.
const GOLDEN: &str = "tests/golden/control_contract.json";

/// The top-level messages that cross the control socket — the JSON-RPC frames,
/// each method's parameters and result, and the server-initiated events. Each
/// key's schema carries its nested types inline, so this map is the whole
/// contract's committed shape.
fn contract_schemas() -> BTreeMap<&'static str, RootSchema> {
    BTreeMap::from([
        ("ControlRequest", schema_for!(ControlRequest)),
        ("ControlResponse", schema_for!(ControlResponse)),
        ("ControlNotification", schema_for!(ControlNotification)),
        ("ClientHello", schema_for!(ClientHello)),
        ("ServerHello", schema_for!(ServerHello)),
        ("ConfigSchema", schema_for!(ConfigSchema)),
        ("ConfigGetRequest", schema_for!(ConfigGetRequest)),
        ("ConfigValue", schema_for!(ConfigValue)),
        ("ConfigDocument", schema_for!(ConfigDocument)),
        ("ConfigSetRequest", schema_for!(ConfigSetRequest)),
        ("ConfigWriteOutcome", schema_for!(ConfigWriteOutcome)),
        ("ConfigValidateRequest", schema_for!(ConfigValidateRequest)),
        ("ConfigValidation", schema_for!(ConfigValidation)),
        ("ConfigPath", schema_for!(ConfigPath)),
        ("ServerStatus", schema_for!(ServerStatus)),
        ("RestartOutcome", schema_for!(RestartOutcome)),
        ("StopOutcome", schema_for!(StopOutcome)),
        ("LogsTailRequest", schema_for!(LogsTailRequest)),
        ("LogLines", schema_for!(LogLines)),
        ("TokenList", schema_for!(TokenList)),
        ("ControlEvent", schema_for!(ControlEvent)),
    ])
}

#[test]
fn control_contract_matches_its_golden_schema() {
    let generated = serde_json::to_string_pretty(&contract_schemas())
        .expect("control schemas serialise to JSON");
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
        "control schema drifted from {}; run with {UPDATE_ENV_VAR}=1 to update it",
        path.display(),
    );
}
