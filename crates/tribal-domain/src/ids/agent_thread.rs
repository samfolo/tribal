use super::common::define_id;

define_id!(
    /// Unique identifier for an agent thread (one durable agent run).
    ///
    /// The stable correlation key across `agent_threads`, the record log,
    /// `token_usage` attribution, and the runtime telemetry spans.
    AgentThreadId,
    "athr"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(AgentThreadId, "athr");
}
