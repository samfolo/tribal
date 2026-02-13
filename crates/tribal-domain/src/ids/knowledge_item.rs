use super::common::define_id;

define_id!(
    /// Unique identifier for a knowledge item.
    KnowledgeItemId,
    "ki"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(KnowledgeItemId, "ki");
}
