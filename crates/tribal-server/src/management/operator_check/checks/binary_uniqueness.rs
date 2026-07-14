//! Outcome constructors and action for the `binary_uniqueness` check.
//!
//! Walks the PATH variable looking for `tribal` (or `tribal.exe` on
//! Windows) binaries.  Multiple resolutions indicate a shadowing risk;
//! none indicate the running binary is not discoverable through PATH.

use std::path::{Path, PathBuf};

use super::{
    state::CheckState,
    types::{CheckDetail, CheckOutcome, CheckRemediation},
};

/// Binary filename to search for on each PATH entry.
#[cfg(windows)]
const BINARY_FILENAME: &str = "tribal.exe";
#[cfg(not(windows))]
const BINARY_FILENAME: &str = "tribal";

impl CheckOutcome {
    pub(in crate::management::operator_check) fn binary_unique(path: PathBuf) -> Self {
        Self::Pass {
            detail: CheckDetail::BinaryUnique { path },
        }
    }

    pub(in crate::management::operator_check) fn binary_duplicate(paths: Vec<PathBuf>) -> Self {
        Self::Warn {
            detail: CheckDetail::BinaryDuplicate {
                paths: paths.clone(),
            },
            remediation: CheckRemediation::PruneDuplicateBinaries { paths },
        }
    }

    pub(in crate::management::operator_check) fn binary_absent() -> Self {
        Self::Warn {
            detail: CheckDetail::BinaryAbsent,
            remediation: CheckRemediation::EnsureTribalOnPath,
        }
    }
}

/// Walks PATH for [`BINARY_FILENAME`].  `exists` decouples filesystem
/// I/O from the search logic so tests can supply a synthetic predicate.
/// Duplicate PATH entries are common (`PATH=/usr/local/bin:/usr/local/bin:...`)
/// and would otherwise produce a false duplicate warning.
pub(in crate::management::operator_check) fn find_tribal_binaries<F>(
    path_var: &str,
    exists: F,
) -> Vec<PathBuf>
where
    F: Fn(&Path) -> bool,
{
    let mut found: Vec<PathBuf> = Vec::new();
    for entry in std::env::split_paths(path_var) {
        let candidate = entry.join(BINARY_FILENAME);
        if exists(&candidate) && !found.contains(&candidate) {
            found.push(candidate);
        }
    }
    found
}

/// On Unix, a non-executable regular file named `tribal` on PATH would
/// never be invoked by a shell — counting it as a duplicate (or as a
/// resolution at all) misleads the operator.  Require at least one
/// execute bit so the count matches what `command -v` would observe.
#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let Ok(meta) = path.metadata() else {
        return false;
    };
    meta.is_file() && meta.permissions().mode() & 0o111 != 0
}

/// On Windows the filename match against `tribal.exe` already implies
/// executability — `is_file` is sufficient.
#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

/// Reads `state.path_var` and reports the outcome.
// PATH lookup is sync, but the step dispatcher requires every action
// to share the `async fn act` signature.
#[allow(clippy::unused_async)]
pub(in crate::management::operator_check) async fn act(state: &mut CheckState) -> CheckOutcome {
    let binaries = find_tribal_binaries(&state.path_var, is_executable_file);
    match binaries.len() {
        0 => CheckOutcome::binary_absent(),
        1 => CheckOutcome::binary_unique(
            binaries
                .into_iter()
                .next()
                .expect("len() == 1 guarantees at least one element"),
        ),
        _ => CheckOutcome::binary_duplicate(binaries),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn predicate_from(paths: &[&str]) -> impl Fn(&Path) -> bool + use<> {
        let set: HashSet<PathBuf> = paths.iter().map(PathBuf::from).collect();
        move |path: &Path| set.contains(path)
    }

    #[test]
    fn test_find_tribal_binaries_returns_empty_when_path_contains_none() {
        let exists = predicate_from(&[]);
        let binaries = find_tribal_binaries("/usr/local/bin:/opt/bin", exists);
        assert!(binaries.is_empty());
    }

    #[test]
    fn test_find_tribal_binaries_returns_one_when_single_entry_matches() {
        let candidate = format!("/usr/local/bin/{BINARY_FILENAME}");
        let exists = predicate_from(&[&candidate]);
        let binaries = find_tribal_binaries("/usr/local/bin:/opt/bin", exists);
        assert_eq!(binaries, vec![PathBuf::from(candidate)]);
    }

    #[test]
    fn test_find_tribal_binaries_returns_all_matches_in_order() {
        let first = format!("/usr/local/bin/{BINARY_FILENAME}");
        let second = format!("/opt/bin/{BINARY_FILENAME}");
        let exists = predicate_from(&[&first, &second]);
        let binaries = find_tribal_binaries("/usr/local/bin:/opt/bin", exists);
        assert_eq!(binaries, vec![PathBuf::from(first), PathBuf::from(second)],);
    }

    #[test]
    fn test_find_tribal_binaries_dedupes_repeated_path_entries() {
        let candidate = format!("/usr/local/bin/{BINARY_FILENAME}");
        let exists = predicate_from(&[&candidate]);
        let binaries = find_tribal_binaries("/usr/local/bin:/usr/local/bin:/usr/local/bin", exists);
        assert_eq!(binaries, vec![PathBuf::from(candidate)]);
    }

    #[test]
    fn test_binary_unique_is_pass() {
        let path = PathBuf::from(format!("/usr/local/bin/{BINARY_FILENAME}"));
        assert!(matches!(
            &CheckOutcome::binary_unique(path.clone()),
            CheckOutcome::Pass {
                detail: CheckDetail::BinaryUnique { path: p },
            } if p == &path,
        ));
    }

    #[test]
    fn test_binary_duplicate_is_warn_with_prune_remediation() {
        let paths = vec![
            PathBuf::from(format!("/usr/local/bin/{BINARY_FILENAME}")),
            PathBuf::from(format!("/opt/bin/{BINARY_FILENAME}")),
        ];
        assert!(matches!(
            &CheckOutcome::binary_duplicate(paths.clone()),
            CheckOutcome::Warn {
                detail: CheckDetail::BinaryDuplicate { paths: detail_paths },
                remediation: CheckRemediation::PruneDuplicateBinaries {
                    paths: rec_paths,
                },
            } if detail_paths == &paths && rec_paths == &paths,
        ));
    }

    #[test]
    fn test_binary_absent_is_warn_with_ensure_on_path_remediation() {
        assert!(matches!(
            &CheckOutcome::binary_absent(),
            CheckOutcome::Warn {
                detail: CheckDetail::BinaryAbsent,
                remediation: CheckRemediation::EnsureTribalOnPath,
            },
        ));
    }

    #[cfg(unix)]
    mod unix_tests {
        use std::{fs, os::unix::fs::PermissionsExt};

        use tempfile::tempdir;

        use super::*;

        #[test]
        fn test_is_executable_file_rejects_non_executable_regular_file() {
            let dir = tempdir().expect("tempdir");
            let path = dir.path().join(BINARY_FILENAME);
            fs::write(&path, b"#!/bin/sh\n").expect("write");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
            assert!(!is_executable_file(&path));
        }

        #[test]
        fn test_is_executable_file_accepts_owner_executable_regular_file() {
            let dir = tempdir().expect("tempdir");
            let path = dir.path().join(BINARY_FILENAME);
            fs::write(&path, b"#!/bin/sh\n").expect("write");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod 0755");
            assert!(is_executable_file(&path));
        }

        #[test]
        fn test_is_executable_file_rejects_missing_path() {
            let dir = tempdir().expect("tempdir");
            assert!(!is_executable_file(&dir.path().join("nonexistent")));
        }
    }
}
