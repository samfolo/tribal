//! Golden JSON-Schema test for the control-bridge wire contract.
//!
//! Every top-level control crossing is schema-generated and diffed against a
//! committed golden file. A schema-affecting change to any DTO regenerates the
//! schema, so an unregenerated golden fails here — and in CI, which runs this
//! test with the `schema` feature on. This golden is the single source the
//! desktop client's DTOs generate from. Regenerate after an intended change with
//! `UPDATE_SNAPSHOTS=1 cargo test -p tribal-wire --features schema`.
#![cfg(feature = "schema")]

mod schema_support;

use std::{fs, path::Path};

use schema_support::control_schemas;

/// The environment variable that regenerates the golden file instead of
/// comparing against it.
const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";

/// The committed golden schema, relative to the crate manifest.
const GOLDEN: &str = "tests/golden/control_contract.json";

#[test]
fn control_contract_matches_its_golden_schema() {
    let generated = serde_json::to_string_pretty(&control_schemas())
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
