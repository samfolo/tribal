use super::common::define_id;

define_id!(
    /// Identity of one manager-held storage transition.
    StorageTransitionId,
    "stx"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(StorageTransitionId, "stx");
}
