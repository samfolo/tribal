use super::common::define_id;

define_id!(
    /// Unique identifier for a job.
    JobId,
    "job"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(JobId, "job");
}
