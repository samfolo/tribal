use super::common::define_id;

define_id!(
    /// Unique identifier for a single reindex task (one batch or catch-up
    /// singleton within a run).
    ReindexTaskId,
    "rtask"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(ReindexTaskId, "rtask");
}
