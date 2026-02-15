use super::common::define_id;

define_id!(
    /// Unique identifier for a knowledge item relation.
    RelationId,
    "rel"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(RelationId, "rel");
}
