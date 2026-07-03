//! Golden JSON-Schema test for the gateway wire contract.
//!
//! Every top-level gateway crossing is schema-generated and diffed against a
//! committed golden file. A schema-affecting change to any DTO regenerates the
//! schema, so an unregenerated golden fails here — and in CI, which runs this
//! test with the `schema` feature on. Regenerate after an intended change with
//! `UPDATE_SNAPSHOTS=1 cargo test -p tribal-wire --features schema`.
#![cfg(feature = "schema")]

use std::{collections::BTreeMap, fs, path::Path};

use schemars::{schema::RootSchema, schema_for};
use tribal_wire::gateway::{
    Acknowledgement, CapSync, CompletionChunk, CompletionTerminal, GatewayError, GatewayUsage,
    GrantSet, HoldsReport, InferenceRequest, JobEnqueue,
};

/// The environment variable that regenerates the golden file instead of
/// comparing against it.
const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";

/// The committed golden schema, relative to the crate manifest.
const GOLDEN: &str = "tests/golden/gateway_contract.json";

/// The top-level messages that cross the seam. Each key's schema carries its
/// nested types inline, so this map is the whole contract's committed shape.
fn contract_schemas() -> BTreeMap<&'static str, RootSchema> {
    BTreeMap::from([
        ("InferenceRequest", schema_for!(InferenceRequest)),
        ("CompletionChunk", schema_for!(CompletionChunk)),
        ("CompletionTerminal", schema_for!(CompletionTerminal)),
        ("GatewayError", schema_for!(GatewayError)),
        ("GatewayUsage", schema_for!(GatewayUsage)),
        ("Acknowledgement", schema_for!(Acknowledgement)),
        ("HoldsReport", schema_for!(HoldsReport)),
        ("JobEnqueue", schema_for!(JobEnqueue)),
        ("CapSync", schema_for!(CapSync)),
        ("GrantSet", schema_for!(GrantSet)),
    ])
}

#[test]
fn gateway_contract_matches_its_golden_schema() {
    let generated = serde_json::to_string_pretty(&contract_schemas())
        .expect("gateway schemas serialise to JSON");
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
        "gateway schema drifted from {}; run with {UPDATE_ENV_VAR}=1 to update it",
        path.display(),
    );
}
