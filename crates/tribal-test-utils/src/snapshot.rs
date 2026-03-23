//! Golden snapshot assertion for JSON-serialisable values.
//!
//! Compares a value's pretty-printed JSON representation against a
//! file on disk.  When the `UPDATE_SNAPSHOTS` environment variable is
//! set, the file is overwritten instead of compared — use this to
//! regenerate snapshots after intentional changes.
//!
//! # Usage
//!
//! ```ignore
//! assert_json_snapshot!(&value, "src/schemas/golden_snapshot.json");
//! ```
//!
//! The path is relative to the calling crate's `CARGO_MANIFEST_DIR`.
//! Regenerate with `UPDATE_SNAPSHOTS=1 cargo test -p <crate> <test>`.

use std::{fs, path::Path};

use serde::Serialize;

/// Environment variable that triggers snapshot regeneration.
const UPDATE_ENV_VAR: &str = "UPDATE_SNAPSHOTS";

/// Compares a JSON-serialised value against a snapshot file.
///
/// Called by the [`assert_json_snapshot`] macro — not intended for
/// direct use (the macro injects `CARGO_MANIFEST_DIR`).
///
/// # Panics
///
/// Panics if the snapshot file is missing (when not updating), the
/// file cannot be read or written, or the serialised value does not
/// match the existing snapshot.
pub fn assert_json_snapshot_impl(value: &impl Serialize, snapshot_path: &Path) {
    let snapshot =
        serde_json::to_string_pretty(value).expect("snapshot value must be serialisable");

    if std::env::var(UPDATE_ENV_VAR).is_ok() {
        if let Some(parent) = snapshot_path.parent() {
            fs::create_dir_all(parent).expect("create snapshot parent directory");
        }
        fs::write(snapshot_path, &snapshot).expect("write snapshot file");
        return;
    }

    assert!(
        snapshot_path.exists(),
        "Snapshot missing at {path}. Run with {UPDATE_ENV_VAR}=1 to create it.",
        path = snapshot_path.display(),
    );

    let existing = fs::read_to_string(snapshot_path).expect("read snapshot file");
    assert_eq!(
        existing,
        snapshot,
        "Snapshot mismatch at {path}. Run with {UPDATE_ENV_VAR}=1 to update.",
        path = snapshot_path.display(),
    );
}

/// Asserts that a JSON-serialisable value matches a golden snapshot file.
///
/// The `relative_path` is resolved from the calling crate's
/// `CARGO_MANIFEST_DIR`.  When the `UPDATE_SNAPSHOTS` environment
/// variable is set, the file is overwritten instead of compared.
///
/// # Examples
///
/// ```ignore
/// assert_json_snapshot!(&tools, "src/schemas/golden_snapshot.json");
/// ```
#[macro_export]
macro_rules! assert_json_snapshot {
    ($value:expr, $relative_path:expr) => {
        $crate::snapshot::assert_json_snapshot_impl(
            $value,
            &::std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join($relative_path),
        )
    };
}
