use super::common::define_id;

define_id!(
    /// Unique identifier for a reference.
    ReferenceId,
    "ref"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::common::id_tests;

    id_tests!(ReferenceId, "ref");
}
