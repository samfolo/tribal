//! Where a metered operation ran.
//!
//! The local usage meter records both the work the edge runtime ran on the
//! user's own machine and the managed enrichment the platform ran elsewhere on
//! the account's behalf. [`ExecutionLocus`] is the axis that tells the two
//! apart, so the ledger separates what a self-hoster ran locally from what the
//! managed service ran for them.

use serde::{Deserialize, Serialize};

/// Where a metered operation ran.
///
/// The edge runtime runs in the binary against the user's own keys; the managed
/// runtime runs on the platform and is metered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionLocus {
    /// The edge runtime — in the binary, on the user's machine.
    #[default]
    Edge,
    /// The managed runtime — on the platform, run elsewhere.
    Managed,
}

enum_text_conversions!(ExecutionLocus {
    ExecutionLocus::Edge => "edge",
    ExecutionLocus::Managed => "managed",
});

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_execution_locus_serde_roundtrip, ExecutionLocus {
        ExecutionLocus::Edge => "edge",
        ExecutionLocus::Managed => "managed",
    });

    enum_text_tests!(test_execution_locus_text_roundtrip, ExecutionLocus {
        ExecutionLocus::Edge => "edge",
        ExecutionLocus::Managed => "managed",
    });

    #[test]
    fn test_default_is_edge() {
        assert_eq!(ExecutionLocus::default(), ExecutionLocus::Edge);
    }
}
