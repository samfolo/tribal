use super::common::define_id;

define_id!(
    /// Unique identifier for one committed record in an agent thread's
    /// append-only log.
    ///
    /// Rows are also addressable by `(thread, seq)`; the id is the stable
    /// key `token_usage` attribution and telemetry carry.
    AgentThreadRecordId,
    "arec"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(AgentThreadRecordId, "arec");
}
