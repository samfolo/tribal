use super::common::define_id;

define_id!(
    /// Unique identifier for a relation batch committed by a relation task.
    RelationBatchId,
    "rb"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(RelationBatchId, "rb");
}
