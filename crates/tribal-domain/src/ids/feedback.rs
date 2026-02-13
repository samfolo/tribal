use super::common::define_id;

/// Unique identifier for retrieval feedback.
define_id!(RetrievalFeedbackId, "fb");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(RetrievalFeedbackId, "fb");
}
