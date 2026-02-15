use super::common::define_id;

define_id!(
    /// Unique identifier for an item observation (re-encounter record).
    ItemObservationId,
    "obs"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(ItemObservationId, "obs");
}
