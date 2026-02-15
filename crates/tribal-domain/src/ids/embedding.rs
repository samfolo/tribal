use super::common::define_id;

define_id!(
    /// Unique identifier for an embedding vector.
    EmbeddingId,
    "emb"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(EmbeddingId, "emb");
}
