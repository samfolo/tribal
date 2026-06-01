use super::common::define_id;

define_id!(
    /// Unique identifier for an embedding profile (one activation of one
    /// embedding geometry).
    EmbeddingProfileId,
    "eprof"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(EmbeddingProfileId, "eprof");
}
