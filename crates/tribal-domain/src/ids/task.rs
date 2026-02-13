use super::common::define_id;

/// Unique identifier for a task.
define_id!(TaskId, "task");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(TaskId, "task");
}
