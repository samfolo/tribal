//! Golden drift test for the `config.schema` surface.
//!
//! The structural JSON Schema of `TribalConfig` and its per-leaf metadata
//! overlay are diffed against a committed golden. A schema-affecting change to
//! any config type — a new field, a changed type, a renamed enum variant —
//! redrafts the schema, so an unregenerated golden fails here and in CI, which
//! runs this test with the `schema` feature on. This golden is what the
//! desktop client's settings form is generated and gated against. Regenerate
//! after an intended change with
//! `UPDATE_SNAPSHOTS=1 cargo test -p tribal-config --features schema`.
#![cfg(feature = "schema")]

use std::{fs, path::Path};

/// The environment variable that regenerates the golden file instead of
/// comparing against it.
const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";

/// The committed golden schema, relative to the crate manifest.
const GOLDEN: &str = "tests/golden/config_schema.json";

#[test]
fn config_schema_matches_its_golden() {
    let generated = serde_json::to_string_pretty(&tribal_config::config_schema())
        .expect("the config schema serialises to JSON");
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
        "config schema drifted from {}; run with {UPDATE_ENV_VAR}=1 to update it",
        path.display(),
    );
}
