use super::common::define_id;

/// Unique identifier for a job.
define_id!(JobId, "job");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(JobId, "job");
}
