use super::common::define_id;

define_id!(
    /// Unique identifier for an episode (a group of items from the same
    /// experience or session).
    EpisodeId,
    "ep"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(EpisodeId, "ep");
}
