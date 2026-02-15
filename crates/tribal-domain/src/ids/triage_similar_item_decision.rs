use super::common::define_id;

define_id!(
    /// Unique identifier for a triage similar-item decision.
    TriageSimilarItemDecisionId,
    "tsd"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(TriageSimilarItemDecisionId, "tsd");
}
