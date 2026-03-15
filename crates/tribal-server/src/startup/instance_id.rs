//! Unique per-process instance identifier.

use std::{process, sync::Arc};

use uuid::Uuid;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Generates a unique instance identifier: `{hostname}~{pid}~{boot_id}`.
///
/// Uses `~` as the separator to avoid ambiguity with hyphens in hostnames
/// and UUIDs.  The boot identifier is a random UUID v4, making the value
/// unique even when the same host restarts with an identical PID.
pub(crate) fn generate_instance_id() -> Arc<str> {
    let hostname = gethostname::gethostname();
    let hostname = hostname.to_string_lossy();
    let pid = process::id();
    let boot_id = Uuid::new_v4();

    Arc::from(format!("{hostname}~{pid}~{boot_id}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_id_format() {
        let id = generate_instance_id();
        let parts: Vec<&str> = id.splitn(3, '~').collect();

        // Exactly 3 segments: hostname, pid, and boot_id.
        assert_eq!(parts.len(), 3, "expected hostname~pid~boot_id, got: {id}");

        // PID segment must be numeric.
        assert!(
            parts[1].parse::<u32>().is_ok(),
            "PID segment is not numeric: {}",
            parts[1],
        );

        // boot_id segment must be a valid UUID.
        assert!(
            Uuid::parse_str(parts[2]).is_ok(),
            "boot_id segment is not a valid UUID: {}",
            parts[2],
        );
    }
}
