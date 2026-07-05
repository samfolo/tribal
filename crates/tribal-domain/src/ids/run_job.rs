use super::common::define_id;

define_id!(
    /// Unique identifier for a managed job in the runtime job plane.
    RunJobId,
    "job"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(RunJobId, "job");
}
