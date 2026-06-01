use super::common::define_id;

define_id!(
    /// Unique identifier for a reindex run (one `tribal reindex` invocation).
    ///
    /// The stable correlation key across `reindex_runs`, `token_usage`, and the
    /// reindex telemetry spans.
    ReindexRunId,
    "rrun"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(ReindexRunId, "rrun");
}
