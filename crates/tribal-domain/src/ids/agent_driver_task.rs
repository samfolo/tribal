use super::common::define_id;

define_id!(
    /// Unique identifier for an agent driver task: the queue-family row
    /// that drives a thread with no stage task, or executes deferred work.
    AgentDriverTaskId,
    "adrv"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(AgentDriverTaskId, "adrv");
}
