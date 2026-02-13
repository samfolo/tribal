use super::common::define_id;

/// Unique identifier for a knowledge item.
define_id!(KnowledgeItemId, "ki");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(KnowledgeItemId, "ki");
}
